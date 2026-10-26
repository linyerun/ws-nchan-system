// Package store 封装本系统对 Redis 的全部读写，是“全局 Session”的唯一入口。
//
// # Redis 数据模型
//
//	presence:user:{uid}   hash     uid / node_id / since / last_hb，TTL = PRESENCE_TTL
//	presence:index        zset     uid -> presence 过期时间戳，用于快速列出在线用户
//	node:{id}             hash     节点注册信息，TTL = NODE_TTL
//	node:{id}:users       zset     该节点承载的 uid -> presence 过期时间
//	nodes:index           zset     节点 -> 注册过期时间戳
//	room:{id}:members     set      房间成员（持久，不随离线清理）
//	user:{uid}:rooms      set      用户加入的房间（持久）
//	push:node:{id}        pub/sub  跨节点消息通道（消息不存储，发完即弃）
//
// # 关键不变式
//
//  1. presence 的 TTL 是“在线”的权威来源。socket 断开回调、TCP 半开、
//     进程被 kill 都不可靠，只有 TTL 能避免永久幽灵在线。
//  2. ZSET 索引必须“读时裁剪”。Redis 在 hash 过期时不会自动删除索引成员，
//     所以索引分数统一存“过期时刻”，读取时先用 ZRemRangeByScore 清理。
//  3. 多个相关写入使用 Pipeline，合并网络往返；这里不要求严格事务，
//     因为读路径会清理中间态，最坏只会短暂出现一条悬挂索引。
package store

import (
	"context"
	"encoding/json"

	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"ws-nchan-system/internal/model"
)

const (
	// presenceIndexKey 全量在线用户索引，成员是 uid，分数是 presence 的过期时间戳。
	// 广播和 /api/online 都依赖它，避免 KEYS/SCAN 全库扫描。
	presenceIndexKey = "presence:index"
	// nodesIndexKey 节点注册索引，成员是节点 ID，分数是节点注册的过期时间戳。
	nodesIndexKey = "nodes:index"
)

// Store 是对 Redis 客户端的薄封装。
// 它不持有任何本地状态，因此可以安全地被 HTTP、gRPC、bridge、reaper
// 多个 goroutine 并发使用（go-redis 客户端本身是并发安全的）。
type Store struct {
	rdb *redis.Client
}

// New 创建一个连接到 addr 的 Store。此时尚未真正建立连接，
// 连接由 go-redis 在首次命令时按需建立；main 中的 waitForRedis 会主动 Ping。
func New(addr string) *Store {
	return &Store{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

// Ping 检测 Redis 连通性，用于启动探活。
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// Close 关闭底层连接池，通常在进程退出时 defer 调用。
func (s *Store) Close() error { return s.rdb.Close() }

// 下面几个方法集中管理 Redis key 的拼装，避免散落的字符串拼接导致命名不一致。
// 命名空间用冒号分隔（presence:user:{uid}），与 README 中的数据模型一一对应。

func (s *Store) presenceKey(uid string) string  { return "presence:user:" + uid }
func (s *Store) nodeKey(id string) string       { return "node:" + id }
func (s *Store) nodeUsersKey(id string) string  { return "node:" + id + ":users" }
func (s *Store) roomKey(id string) string       { return "room:" + id + ":members" }
func (s *Store) userRoomsKey(uid string) string { return "user:" + uid + ":rooms" }

// TouchPresence 写入或续期一个用户的在线路由记录。
//
// 参数 ttl 同时作用于三处：
//   - presence hash 的过期时间；
//   - presence:index 中该 uid 的分数（= 过期时刻）；
//   - node:{nodeID}:users 中该 uid 的分数（= 过期时刻）。
//
// since（首次上线时间）在多次心跳之间保持不变：先读旧值，只有记录不存在
// 时才使用当前时间。读 + 写不是一个原子操作，但同一 uid 的写入来自
// 建连鉴权与心跳，冲突窗口极小，且不会破坏路由正确性。
//
// 所有写入通过 Pipeline 一次提交，减少往返；万一执行中途失败，
// 最坏结果是索引与 hash 短暂不一致，Online() 的读时清理会收敛。
func (s *Store) TouchPresence(ctx context.Context, uid, nodeID string, ttl time.Duration) error {
	now := time.Now()
	key := s.presenceKey(uid)

	// 保留原始上线时间；redis.Nil 表示记录不存在（首次建连或已过期）。
	since, err := s.rdb.HGet(ctx, key, "since").Int64()
	if err == redis.Nil {
		since = now.UnixMilli()
	} else if err != nil {
		return err
	}

	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"uid":     uid,
		"node_id": nodeID,
		"since":   since,
		"last_hb": now.UnixMilli(),
	})
	pipe.PExpire(ctx, key, ttl)
	pipe.ZAdd(ctx, presenceIndexKey, redis.Z{
		Score:  float64(now.Add(ttl).UnixMilli()),
		Member: uid,
	})
	// 节点维度的反查索引：节点宕机时，reaper 靠它知道要清理哪些用户。
	pipe.ZAdd(ctx, s.nodeUsersKey(nodeID), redis.Z{
		Score:  float64(now.Add(ttl).UnixMilli()),
		Member: uid,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// GetPresence 读取一个用户的在线记录。
// 第二个返回值表示记录是否存在；键已过期时同样返回 false。
func (s *Store) GetPresence(ctx context.Context, uid string) (model.Presence, bool) {
	m, err := s.rdb.HGetAll(ctx, s.presenceKey(uid)).Result()
	if err != nil || len(m) == 0 {
		return model.Presence{}, false
	}
	return toPresence(uid, m), true
}

// DeletePresence 在客户端主动下线时立即删除在线记录。
// 同时清理两个 ZSET 索引，避免 /api/online 读到悬挂成员；
// 对于崩溃、断网等非优雅退出，仍由 TTL 兜底。
func (s *Store) DeletePresence(ctx context.Context, uid string) error {
	p, ok := s.GetPresence(ctx, uid)
	if !ok {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, s.presenceKey(uid))
	pipe.ZRem(ctx, presenceIndexKey, uid)
	if p.NodeID != "" {
		pipe.ZRem(ctx, s.nodeUsersKey(p.NodeID), uid)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// GetManyPresence 批量读取在线记录，返回“只包含存在记录”的 map。
//
// 用于发布路径：房间成员、广播目标可能很多，逐个 HGETALL 会产生 N 次往返；
// 这里用 Pipeline 合并为一次网络往返。整体读取失败时返回空 map，
// 调用方会把所有目标当作离线处理，行为可预期。
func (s *Store) GetManyPresence(ctx context.Context, uids []string) map[string]model.Presence {
	out := make(map[string]model.Presence, len(uids))
	if len(uids) == 0 {
		return out
	}
	pipe := s.rdb.Pipeline()
	cmds := make(map[string]*redis.MapStringStringCmd, len(uids))
	for _, uid := range uids {
		cmds[uid] = pipe.HGetAll(ctx, s.presenceKey(uid))
	}
	// 单个 key 不存在时 Pipeline 整体会返回 redis.Nil，这是正常情况，不算错误。
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return out
	}
	for uid, cmd := range cmds {
		if m, err := cmd.Result(); err == nil && len(m) > 0 {
			out[uid] = toPresence(uid, m)
		}
	}
	return out
}

// Online 返回当前所有在线用户，按 uid 排序。
//
// 步骤：
//  1. 按分数裁剪 presence:index，移除已过期成员；
//  2. 取出剩余 uid，批量读取 presence hash；
//  3. 如果 hash 已过期但索引还在（TTL 到期时间存在毫秒级差异），
//     把这些“悬挂索引”顺手删掉，保证在线列表不出现幽灵用户。
func (s *Store) Online(ctx context.Context) ([]model.Presence, error) {
	now := time.Now().UnixMilli()
	if err := s.rdb.ZRemRangeByScore(ctx, presenceIndexKey, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	uids, err := s.rdb.ZRange(ctx, presenceIndexKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	presences := s.GetManyPresence(ctx, uids)

	out := make([]model.Presence, 0, len(presences))
	var dangling []any
	for _, uid := range uids {
		if p, ok := presences[uid]; ok {
			out = append(out, p)
		} else {
			// hash 已过期但索引成员尚未被清理，收集起来一次性删除。
			dangling = append(dangling, uid)
		}
	}
	if len(dangling) > 0 {
		_ = s.rdb.ZRem(ctx, presenceIndexKey, dangling...).Err()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UID < out[j].UID })
	return out, nil
}

// TouchNode 写入或续期本节点的注册信息。
// 和 presence 一样，hash 与索引使用相同的 TTL；节点每 NodeTTL/3 调用一次。
func (s *Store) TouchNode(ctx context.Context, info model.NodeInfo, ttl time.Duration) error {
	now := time.Now()
	key := s.nodeKey(info.ID)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"id":         info.ID,
		"addr":       info.Addr,
		"started_at": info.StartedAt,
		"last_hb":    now.UnixMilli(),
	})
	pipe.PExpire(ctx, key, ttl)
	pipe.ZAdd(ctx, nodesIndexKey, redis.Z{
		Score:  float64(now.Add(ttl).UnixMilli()),
		Member: info.ID,
	})
	_, err := pipe.Exec(ctx)
	return err
}

// Nodes 返回当前仍存活的节点列表，并附带每个节点承载的在线用户数。
//
// 先裁剪节点索引，再逐个读取节点 hash；节点数量很少（演示场景通常 1-2 个），
// 这里没有做 Pipeline 优化。读取前会顺手裁剪 node:{id}:users，
// 使 Online 计数不会包含已过期的 uid。
func (s *Store) Nodes(ctx context.Context) ([]model.NodeInfo, error) {
	now := time.Now().UnixMilli()
	if err := s.rdb.ZRemRangeByScore(ctx, nodesIndexKey, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	ids, err := s.rdb.ZRange(ctx, nodesIndexKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	nodes := make([]model.NodeInfo, 0, len(ids))
	for _, id := range ids {
		m, err := s.rdb.HGetAll(ctx, s.nodeKey(id)).Result()
		if err != nil || len(m) == 0 {
			continue
		}
		n := model.NodeInfo{ID: id, Addr: m["addr"], StartedAt: atoi64(m["started_at"]), LastHB: atoi64(m["last_hb"])}
		if err := s.rdb.ZRemRangeByScore(ctx, s.nodeUsersKey(id), "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
			return nil, err
		}
		n.Online = int(s.rdb.ZCard(ctx, s.nodeUsersKey(id)).Val())
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

// DeadNode 描述一个被清理的故障节点：节点 ID 及被一并下线的用户。
type DeadNode struct {
	ID    string
	Users []string
}

// ReapDeadNodes 清理心跳已过期（分数 <= 当前时间）的节点及其在线记录。
//
// 这是“节点宕机自愈”的核心：
//   - 故障节点的 presence 记录在 PRESENCE_TTL（20s）后本会自然过期，
//     但 node:{id}:users 与节点注册信息需要有人主动清理；
//   - 每个存活节点都会定期执行本方法，因此任意节点都能完成清理，无需选主；
//   - 清理前会重新读取 presence 并核对 node_id，避免把已经重连到其他节点的
//     用户误删（该用户可能恰好在两次检查之间切换了节点）。
//
// 跳过 selfID：本节点即使因为 GC/阻塞短暂错过续期，也不应该清理自己，
// 否则会误删本机所有在线用户。
func (s *Store) ReapDeadNodes(ctx context.Context, selfID string) ([]DeadNode, error) {
	now := time.Now().UnixMilli()
	expired, err := s.rdb.ZRangeByScore(ctx, nodesIndexKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil {
		return nil, err
	}

	var dead []DeadNode
	for _, id := range expired {
		if id == selfID {
			continue
		}
		users, _ := s.rdb.ZRange(ctx, s.nodeUsersKey(id), 0, -1).Result()
		removed := make([]string, 0, len(users))
		for _, uid := range users {
			p, ok := s.GetPresence(ctx, uid)
			// 只有 presence 仍指向该故障节点时才删除；若用户已重连到别的节点，
			// 新的 presence 会覆盖 node_id，这里会跳过。
			if ok && p.NodeID == id {
				_ = s.rdb.Del(ctx, s.presenceKey(uid)).Err()
				_ = s.rdb.ZRem(ctx, presenceIndexKey, uid).Err()
				removed = append(removed, uid)
			}
		}
		pipe := s.rdb.Pipeline()
		pipe.Del(ctx, s.nodeKey(id))
		pipe.Del(ctx, s.nodeUsersKey(id))
		pipe.ZRem(ctx, nodesIndexKey, id)
		_, _ = pipe.Exec(ctx)
		dead = append(dead, DeadNode{ID: id, Users: removed})
	}
	return dead, nil
}

// JoinRoom 把用户加入房间，并同步维护“用户 -> 房间”反查。
//
// 两个 Set 互为索引：
//   - room:{id}:members 用于房间消息 fan-out；
//   - user:{uid}:rooms 用于 /api/me 与前端房间列表。
//
// 房间成员关系是持久的，不随离线或登出删除；这是业务语义，
// 与 presence 的短 TTL 生命周期相互独立。
func (s *Store) JoinRoom(ctx context.Context, uid, room string) error {
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, s.roomKey(room), uid)
	pipe.SAdd(ctx, s.userRoomsKey(uid), room)
	_, err := pipe.Exec(ctx)
	return err
}

// LeaveRoom 把用户从房间移除，并清理反查索引。Set 的删除是幂等的。
func (s *Store) LeaveRoom(ctx context.Context, uid, room string) error {
	pipe := s.rdb.Pipeline()
	pipe.SRem(ctx, s.roomKey(room), uid)
	pipe.SRem(ctx, s.userRoomsKey(uid), room)
	_, err := pipe.Exec(ctx)
	return err
}

// RoomMembers 返回房间成员（排序后），包含离线成员。
// 推送时再由 presence 判断谁在线，因此离线成员不会被投递，但会记入 offline 计数。
func (s *Store) RoomMembers(ctx context.Context, room string) ([]string, error) {
	members, err := s.rdb.SMembers(ctx, s.roomKey(room)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(members)
	return members, nil
}

// UserRooms 返回用户加入的所有房间（排序后），用于前端“我的房间”列表。
func (s *Store) UserRooms(ctx context.Context, uid string) ([]string, error) {
	rooms, err := s.rdb.SMembers(ctx, s.userRoomsKey(uid)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(rooms)
	return rooms, nil
}

// PublishToNode 把一条待投递消息写入目标节点的 Pub/Sub 通道。
//
// 返回值是“该通道当前的订阅者数量”：
//   - 1 表示目标节点的 bridge 正在运行，消息已被接收；
//   - 0 表示没有 bridge 在听，消息被丢弃。此时 presence 可能还没过期，
//     这正是节点故障后的“幽灵会话窗口”，调用方会把该接收者计为 failed。
//
// 注意 Redis Pub/Sub 是 fire-and-forget：即使返回 1，也不代表 Nchan
// 一定成功下发，只代表消息已交给目标节点的 bridge。
func (s *Store) PublishToNode(ctx context.Context, nodeID string, msg model.BridgeMessage) (int64, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}
	return s.rdb.Publish(ctx, "push:node:"+nodeID, payload).Result()
}

// SubscribeNode 订阅本节点的消息通道，返回 go-redis 的 PubSub 对象。
// 调用方负责在退出时 Close，并持续消费 Channel() 中的消息。
func (s *Store) SubscribeNode(ctx context.Context, nodeID string) *redis.PubSub {
	return s.rdb.Subscribe(ctx, "push:node:"+nodeID)
}

// toPresence 把 Redis hash 的字符串字段转换为 Presence 结构。
func toPresence(uid string, m map[string]string) model.Presence {
	return model.Presence{
		UID:    uid,
		NodeID: m["node_id"],
		Since:  atoi64(m["since"]),
		LastHB: atoi64(m["last_hb"]),
	}
}

// atoi64 宽容地解析整数：空串或非法值返回 0。
// Redis 中的数据由本服务写入，正常不会失败；这里选择零值而不是报错，
// 避免一条脏数据导致整个在线列表读取失败。
func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
