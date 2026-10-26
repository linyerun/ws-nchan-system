// Package push 实现“逻辑目标 -> 在线 uid -> 按节点分组 -> Redis 投递”的
// 核心发布流程，是 HTTP 与 gRPC 两个入口共享的业务层。
//
// # 一次发布的完整路径
//
//	调用方 POST /api/push 或 gRPC Publish
//	  -> resolve：把 user / room / broadcast 解析成接收者 uid 列表
//	  -> GetManyPresence：批量查询每个 uid 当前由哪个节点承载
//	  -> 按 node_id 分组
//	  -> 对每个 uid 构造 Envelope，PUBLISH 到 push:node:{nodeID}
//	  -> 目标节点的 bridge 消费后调用本地 Nchan，最终下发到 WebSocket
//
// 这就是“写时扇出（fan-out on write）”：房间/广播的成员展开发生在发布侧，
// Nchan 侧每个用户只需要一个固定频道 u:{uid}，不需要理解房间协议。
//
// # 计数语义
//
//	Online  = 路由表中存在 presence 的接收者数量
//	Offline = 目标 uid 没有 presence（或已过期）
//	Failed  = 有 presence，但目标节点 bridge 不在线（订阅者数为 0）
//
// Online 与 Failed 会重叠：节点刚刚宕机、注册信息尚未过期时，
// 该用户仍被路由表认为在线（计入 Online），但消息实际发不出去（计入 Failed）。
package push

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"ws-nchan-system/internal/auth"
	"ws-nchan-system/internal/model"
	"ws-nchan-system/internal/stats"
	"ws-nchan-system/internal/store"
)

var (
	// ErrBadTarget 表示目标类型/目标 ID 非法，属于调用方参数错误（HTTP 400）。
	ErrBadTarget = errors.New("invalid target")
	// ErrBadPayload 表示 payload 不是 JSON 对象，同样属于参数错误。
	ErrBadPayload = errors.New("payload must be a JSON object")
)

// Service 是发布业务层，依赖 Redis 存储、内存计数器和日志。
// 它本身无状态，可安全并发调用。
type Service struct {
	store  *store.Store
	stats  *stats.Counters
	logger *slog.Logger
}

// New 构造发布服务。各依赖由 main 统一创建并注入。
func New(st *store.Store, sc *stats.Counters, logger *slog.Logger) *Service {
	return &Service{store: st, stats: sc, logger: logger}
}

// Publish 执行一次发布：解析目标、定位节点、逐条投递。
//
// 处理步骤：
//  1. 参数归一化与校验（From 默认 "server"，payload 默认空对象且必须是 JSON 对象）；
//  2. resolve 把逻辑目标展开成 uid 列表；
//  3. 生成一个消息 ID，同一次发布的所有接收者共享，供前端去重；
//  4. 批量读取 presence，按节点分组并统计在线/离线；
//  5. 对每个 uid 构造 Envelope，PUBLISH 到其所属节点的通道；
//  6. 汇总 failed（节点 bridge 不在线）并更新计数器。
//
// 注意投递是串行的：保证同一节点内消息按构造顺序进入 Pub/Sub，
// 从而让同一用户收到的消息大致保序。大规模广播可改为按节点批量/并发投递。
func (s *Service) Publish(ctx context.Context, in model.PublishInput) (model.PublishResult, error) {
	// From 为空时记为 server，方便系统级通知；非空则必须是合法 uid。
	if in.From == "" {
		in.From = "server"
	}
	if !auth.ValidUID(in.From) {
		return model.PublishResult{}, fmt.Errorf("%w: from", ErrBadTarget)
	}
	// payload 必须是一个 JSON 对象。这里只校验“形状”，不解析业务字段，
	// 推送层保持对业务协议的透明。
	if len(in.Payload) == 0 {
		in.Payload = json.RawMessage(`{}`)
	}
	var probe map[string]any
	if err := json.Unmarshal(in.Payload, &probe); err != nil {
		return model.PublishResult{}, ErrBadPayload
	}

	uids, kind, err := s.resolve(ctx, in)
	if err != nil {
		return model.PublishResult{}, err
	}

	msgID := newMsgID()
	now := time.Now().UnixMilli()
	res := model.PublishResult{MsgID: msgID}
	// 一次 Pipeline 批量读取所有接收者的路由信息。
	presences := s.store.GetManyPresence(ctx, uids)

	byNode := map[string][]string{}
	online := 0
	for _, uid := range uids {
		p, ok := presences[uid]
		if !ok {
			// 没有 presence = 离线（或 TTL 已过期），计入 offline。
			continue
		}
		online++
		byNode[p.NodeID] = append(byNode[p.NodeID], uid)
	}
	res.Offline = len(uids) - online
	res.Online = online

	for node, nodeUIDs := range byNode {
		res.Nodes = append(res.Nodes, node)
		for _, uid := range nodeUIDs {
			// 每个接收者单独构造 Envelope：direct 带 To，room 带 Room，
			// broadcast 统一用 To="all"。Payload 原样透传，避免重复序列化业务数据。
			env := model.Envelope{
				ID:      msgID,
				Kind:    kind,
				From:    in.From,
				TS:      now,
				Payload: in.Payload,
			}
			switch kind {
			case "direct":
				env.To = uid
			case "room":
				env.Room = in.TargetID
			case "broadcast":
				env.To = "all"
			}
			body, err := json.Marshal(env)
			if err != nil {
				res.Failed++
				continue
			}
			receivers, err := s.store.PublishToNode(ctx, node, model.BridgeMessage{
				Channel: "u:" + uid,
				Body:    body,
			})
			if err != nil {
				s.logger.Warn("redis fanout failed", "node", node, "uid", uid, "err", err)
				res.Failed++
				continue
			}
			if receivers == 0 {
				// presence 尚未过期，但目标节点的 bridge 已经不在监听：
				// 典型的“幽灵会话窗口”，消息实际被丢弃，计为失败。
				s.logger.Warn("node bridge missing", "node", node, "uid", uid)
				res.Failed++
			}
		}
	}

	s.stats.Published.Add(1)
	s.stats.FanoutTargets.Add(int64(online))
	return res, nil
}

// resolve 把逻辑目标解析为具体的接收者 uid 列表，同时返回消息类型 kind。
//
//   - user：只校验 uid 格式，不要求在线（离线也返回，由调用方计入 offline）；
//   - room：读取房间成员 Set；房间消息会发给所有成员，在线者投递、离线者计数；
//   - broadcast：读取当前全局在线列表。
//
// 房间成员和在线列表理论上都可能有重复 uid（不同存储来源），
// 因此房间路径额外做一次去重；广播路径的数据来自 ZSET，天然唯一。
func (s *Service) resolve(ctx context.Context, in model.PublishInput) ([]string, string, error) {
	switch in.TargetType {
	case "user":
		if !auth.ValidUID(in.TargetID) {
			return nil, "", fmt.Errorf("%w: uid", ErrBadTarget)
		}
		return []string{in.TargetID}, "direct", nil

	case "room":
		if !auth.ValidUID(in.TargetID) {
			return nil, "", fmt.Errorf("%w: room", ErrBadTarget)
		}
		members, err := s.store.RoomMembers(ctx, in.TargetID)
		if err != nil {
			return nil, "", err
		}
		return dedupe(members), "room", nil

	case "broadcast":
		online, err := s.store.Online(ctx)
		if err != nil {
			return nil, "", err
		}
		uids := make([]string, 0, len(online))
		for _, p := range online {
			uids = append(uids, p.UID)
		}
		return uids, "broadcast", nil

	default:
		return nil, "", fmt.Errorf("%w: target_type", ErrBadTarget)
	}
}

// dedupe 保持首次出现顺序去重，避免同一用户收到重复消息。
func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// newMsgID 生成带随机前缀的消息 ID：msg_ + 12 字节随机数的 hex。
// 使用 crypto/rand 避免多节点并发生成时碰撞；极端情况下随机源不可用时
// 退化为纳秒时间戳，保证仍能返回一个可用 ID。
func newMsgID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("msg_%d", time.Now().UnixNano())
	}
	return "msg_" + hex.EncodeToString(b[:])
}
