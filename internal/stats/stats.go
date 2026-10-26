// Package stats 维护每个节点进程内的轻量级运行计数器。
//
// 这些计数不落 Redis、不做聚合，只用于回答“本节点是否健康、消息卡在哪一环”：
//
//	auth_ok / auth_rejected -> 鉴权是否正常
//	published               -> HTTP/gRPC 收到并处理了多少次发布
//	fanout_targets          -> 实际定位到多少个在线接收者
//	bridge_delivered        -> bridge 成功写入本地 Nchan 的次数
//	bridge_errors           -> bridge 解析/发布失败的次数
//	nchan_pub_errors        -> 其中 Nchan 返回非 2xx 的次数
//
// 全部使用 atomic，读写无锁；/api/stats 每次读取时生成快照。
package stats

import (
	"sync/atomic"
	"time"
)

// Counters 是一个节点的内存计数器集合。
// 实例在 main 中创建，注入 push / bridge / httpapi 三个模块共享。
type Counters struct {
	// Started 进程启动时间，用于计算 uptime。
	Started time.Time

	// AuthOK WebSocket 建连鉴权成功次数（由 /internal/auth 子请求累加）。
	// 普通 REST API 的 Bearer 校验不经过这里，因此不计入。
	AuthOK atomic.Int64
	// AuthRejected WebSocket 建连鉴权失败次数（token 非法、过期、uid 不匹配等）。
	AuthRejected atomic.Int64
	// Heartbeats 收到的心跳请求次数。
	Heartbeats atomic.Int64

	// Published 发布请求总数。
	Published atomic.Int64
	// FanoutTargets 累计投递目标数（按在线接收者计数）。
	FanoutTargets atomic.Int64
	// BridgeOK bridge 成功把消息交给本地 Nchan 的次数。
	BridgeOK atomic.Int64
	// BridgeErrors bridge 处理失败次数（JSON 解析失败、字段缺失等）。
	BridgeErrors atomic.Int64
	// NchanErrors 其中由 Nchan 发布接口返回错误导致的失败次数。
	NchanErrors atomic.Int64
}

// New 创建计数器并记录启动时间。
func New() *Counters { return &Counters{Started: time.Now()} }

// Snapshot 生成一份可直接 JSON 序列化的计数快照。
// 各字段独立读取，不保证严格同一时刻，但对监控展示已经足够。
func (c *Counters) Snapshot(node string) map[string]any {
	return map[string]any{
		"node":              node,
		"uptime_seconds":    int64(time.Since(c.Started).Seconds()),
		"auth_ok":           c.AuthOK.Load(),
		"auth_rejected":     c.AuthRejected.Load(),
		"heartbeats":        c.Heartbeats.Load(),
		"published":         c.Published.Load(),
		"fanout_targets":    c.FanoutTargets.Load(),
		"bridge_delivered":  c.BridgeOK.Load(),
		"bridge_errors":     c.BridgeErrors.Load(),
		"nchan_pub_errors":  c.NchanErrors.Load(),
		"server_started_at": c.Started.Unix(),
	}
}
