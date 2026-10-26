// Package model 定义跨模块共享的数据结构。
//
// 这些结构同时承担两个角色：
//   - Redis 中的存储格式（Presence、NodeInfo）；
//   - 网络传输格式（Envelope 下发给浏览器，BridgeMessage 在节点间传递，
//     PublishInput/PublishResult 是 HTTP 与 gRPC 两个入口的公共模型）。
//
// 所有需要序列化的字段都带有 json tag，字段名即接口契约，改动时需同步前端。
package model

import "encoding/json"

// Presence 是一个在线用户的“全局视图”，保存在 Redis 中。
//
// 它是消息路由的唯一事实来源：uid -> 当前持有该用户 WebSocket 的节点。
// 当用户从 node1 重连到 node2 时，同一份记录会被覆盖为新的 NodeID，
// 后续消息就会路由到 node2。
type Presence struct {
	// UID 用户标识，由 token 中的身份决定。
	UID string `json:"uid"`
	// NodeID 当前承载该用户长连接的节点 ID。
	NodeID string `json:"node_id"`
	// Since 首次上线时间（毫秒）。心跳续期时保持不变，便于展示会话时长。
	Since int64 `json:"since"`
	// LastHB 最近一次心跳时间（毫秒）。
	LastHB int64 `json:"last_hb"`
}

// Envelope 是最终通过 Nchan 沿 WebSocket 下发给浏览器的消息信封。
//
// 一条逻辑消息（同一个 ID）可能被复制成多份 Envelope：房间消息发给每个
// 成员时，To/Room 字段不同，但 ID 相同，前端据此做去重。
type Envelope struct {
	// ID 消息唯一 ID。同一次发布的所有接收者共享同一个 ID。
	ID string `json:"id"`
	// Kind 消息类型：direct（定向）/ room（房间）/ broadcast（广播）。
	Kind string `json:"kind"`
	// From 发送者 uid，或业务系统标识（gRPC 调用时由请求带入）。
	From string `json:"from"`
	// To 定向消息的接收者 uid；广播时固定为 "all"；房间消息为空。
	To string `json:"to,omitempty"`
	// Room 房间消息的房间 ID；其他类型为空。
	Room string `json:"room,omitempty"`
	// TS 服务端生成消息时的时间戳（毫秒）。
	TS int64 `json:"ts"`
	// Payload 原始业务负载。这里刻意用 json.RawMessage 透传，
	// 服务端不解析业务字段，避免推送层与业务协议耦合。
	Payload json.RawMessage `json:"payload"`
}

// BridgeMessage 是 Redis Pub/Sub 在节点之间传递的载荷。
//
// 发布节点把“要发到哪个 Nchan channel、发什么内容”打包成该结构，
// 写入 push:node:{id}；目标节点的 bridge 消费后再调用本地 Nchan。
type BridgeMessage struct {
	// Channel Nchan 频道名，例如 u:alice。
	Channel string `json:"channel"`
	// Body 已序列化的 Envelope，bridge 不需要理解其内容。
	Body json.RawMessage `json:"body"`
}

// PublishInput 是 HTTP / gRPC 两个发布入口归一化后的参数。
type PublishInput struct {
	// From 发送者标识，会写入 Envelope.From 展示给接收方。
	From string
	// TargetType 目标类型：user / room / broadcast。
	TargetType string
	// TargetID 目标 ID：user 时为 uid，room 时为房间 ID，broadcast 时忽略。
	TargetID string
	// Payload 业务负载，必须是 JSON 对象（推送层只校验、不解析具体字段）。
	Payload json.RawMessage
}

// PublishResult 是发布接口的返回值，用于告诉调用方本次投递的结果。
type PublishResult struct {
	// MsgID 本次发布的唯一消息 ID。
	MsgID string `json:"msg_id"`
	// Online 路由表中存在 presence 的接收者数量。
	// 注意它会包含 Failed 中的接收者：节点故障但注册尚未过期时，
	// 这些用户仍被视为“在线”，但消息无法送达。
	Online int `json:"online_recipients"`
	// Offline 目标中不在线的接收者数量（例如房间里的离线成员）。
	Offline int `json:"offline_recipients"`
	// Failed 找到 presence、但目标节点 bridge 不在线导致消息被丢弃的数量。
	// 大于 0 通常意味着节点故障后尚未完成清理的“幽灵窗口”。
	Failed int `json:"failed_recipients"`
	// Nodes 本次实际投递到的节点列表（去重前的追加顺序）。
	Nodes []string `json:"nodes,omitempty"`
}

// NodeInfo 是节点注册表里的一条记录，用于集群成员发现与故障清理。
type NodeInfo struct {
	// ID 节点标识，对应 NODE_ID。
	ID string `json:"id"`
	// Addr 节点的 HTTP 监听地址，仅用于展示/排查。
	Addr string `json:"addr"`
	// StartedAt 节点进程启动时间（毫秒）。
	StartedAt int64 `json:"started_at"`
	// LastHB 最近一次心跳时间（毫秒）。
	LastHB int64 `json:"last_hb"`
	// Online 该节点当前承载的在线用户数，读取时由 Redis 实时计算。
	Online int `json:"online"`
}
