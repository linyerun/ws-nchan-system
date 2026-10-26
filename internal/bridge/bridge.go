// Package bridge 负责把 Redis 中的跨节点消息“落地”到本机 Nchan。
//
// 它是消息通路上唯一同时接触 Redis Pub/Sub 与 Nchan 的组件：
//
//	node1 发布 -> Redis PUBLISH push:node:node2
//	                       │
//	                       ▼
//	node2 bridge 订阅 push:node:node2
//	                       │  POST /internal/pub/u:{uid}
//	                       ▼
//	node2 nginx/Nchan 广播给本机 WebSocket 订阅者
//
// 这样节点之间没有直接 RPC，新增节点只需启动进程并持续写心跳；
// 每个节点只关心自己的本地通道，扩展模型非常简单。
package bridge

import (
	"context"
	"encoding/json"
	"log/slog"

	"ws-nchan-system/internal/model"
	"ws-nchan-system/internal/nchan"
	"ws-nchan-system/internal/stats"
	"ws-nchan-system/internal/store"
)

// Bridge 是单个节点的消息桥接器。
type Bridge struct {
	nodeID string
	store  *store.Store
	nchan  *nchan.Client
	stats  *stats.Counters
	logger *slog.Logger
}

// New 构造 bridge。nodeID 决定订阅哪个 Redis 通道。
func New(nodeID string, st *store.Store, nc *nchan.Client, sc *stats.Counters, logger *slog.Logger) *Bridge {
	return &Bridge{nodeID: nodeID, store: st, nchan: nc, stats: sc, logger: logger}
}

// Run 启动订阅循环，通常以 `go bridge.Run(ctx)` 的方式运行。
//
// 步骤：
//  1. 订阅 push:node:{nodeID}；
//  2. 等待订阅确认（Receive）：确认后才开始消费，避免漏掉订阅生效前的消息；
//  3. 持续从 Channel() 读取消息并逐条投递，ctx 取消时退出。
//
// 注意：go-redis 的 Pub/Sub 会在后台自动重连；Channel() 关闭表示
// 订阅彻底结束，此时直接返回，进程退出由 main 的统一信号处理负责。
func (b *Bridge) Run(ctx context.Context) {
	pubsub := b.store.SubscribeNode(ctx, b.nodeID)
	defer pubsub.Close()

	// Receive 返回第一个服务端响应（订阅成功确认）。如果 Redis 不可达，
	// 这里会失败；bridge 直接退出，节点仍可服务但跨节点消息无法落地，
	// 由容器重启策略或人工介入恢复。
	if _, err := pubsub.Receive(ctx); err != nil {
		b.logger.Error("bridge subscribe failed", "err", err)
		return
	}
	b.logger.Info("bridge subscribed", "redis_channel", "push:node:"+b.nodeID)

	msgs := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			b.deliver(ctx, msg.Payload)
		}
	}
}

// deliver 解析一条 Redis 消息并发布到本地 Nchan。
//
// 任何失败都只记录日志并累加计数器，不中断订阅循环：
// 一条坏消息不应该拖垮整个节点的消息通路。
// 成功与失败的计数最终通过 /api/stats 暴露，便于定位问题。
func (b *Bridge) deliver(ctx context.Context, payload string) {
	var msg model.BridgeMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		b.logger.Warn("bridge bad payload", "err", err)
		b.stats.BridgeErrors.Add(1)
		return
	}
	// Channel 或 Body 缺失说明消息格式不完整，同样跳过。
	if msg.Channel == "" || len(msg.Body) == 0 {
		b.stats.BridgeErrors.Add(1)
		return
	}
	if err := b.nchan.Publish(ctx, msg.Channel, msg.Body); err != nil {
		b.logger.Warn("nchan publish failed", "channel", msg.Channel, "err", err)
		b.stats.BridgeErrors.Add(1)
		b.stats.NchanErrors.Add(1)
		return
	}
	b.stats.BridgeOK.Add(1)
}
