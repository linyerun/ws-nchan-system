// Package config 负责从环境变量加载单个推送节点的运行时配置。
//
// 设计原则：
//   - 所有配置都有可用的默认值，本地直接 `go run ./cmd/server` 就能启动；
//   - 环境变量优先于默认值，方便 docker-compose / Kubernetes 注入；
//   - 时间配置支持 Go 的时长写法（"20s"、"1h"），也兼容纯数字（按秒解释），
//     这样即使运维同学写了 `PRESENCE_TTL=20` 也不会让进程启动失败。
package config

import (
	"os"
	"strconv"
	"time"
)

// Config 是一个推送节点的全部运行时参数。
//
// 节点本身不保存业务状态：在线状态、节点注册、房间关系、跨节点消息
// 全部放在 Redis 中。因此同一份二进制只要换一个 NODE_ID 和端口，
// 就能启动成集群里的另一个对等节点。
type Config struct {
	// NodeID 节点唯一标识。写入 Redis 节点注册表，并用于订阅本节点的消息通道
	// push:node:{NodeID}；日志和 /api/stats 里也用它区分节点。
	NodeID string
	// HTTPAddr HTTP 监听地址，提供 REST API 与 nginx auth_request 使用的 /internal/auth。
	HTTPAddr string
	// GRPCAddr gRPC 监听地址，提供面向业务系统的高频发布接口 PushService。
	GRPCAddr string
	// RedisAddr Redis 地址，保存在线状态、节点注册表、房间关系，并承担 Pub/Sub 扩散。
	RedisAddr string
	// NchanPubURL 本机 Nchan 的内部发布地址（由同节点 nginx 的 /internal/pub 提供）。
	// bridge 收到跨节点消息后，POST 到这里把消息交给 nginx 内的 Nchan 广播。
	NchanPubURL string
	// TokenSecret 客户端 token 的 HMAC 密钥。集群内所有节点必须一致，
	// 否则在 node1 签发的 token 到 node2 会校验失败。
	TokenSecret string
	// InternalToken 内部共享密钥，同时用于：
	//   1) gRPC 请求头 x-internal-token 的校验；
	//   2) bridge 调 nginx 发布接口时的 X-Publisher-Token。
	InternalToken string
	// TokenTTL 客户端 token 的有效期。过期后 API 与 WebSocket 鉴权都会失败。
	TokenTTL time.Duration
	// PresenceTTL 在线状态在 Redis 中的过期时间，客户端心跳会不断续期。
	// 它是判断“离线”的权威依据：断开回调不可靠，只有 TTL 能兜底。
	PresenceTTL time.Duration
	// HeartbeatEvery 建议客户端发送心跳的间隔，通过 /api/dev/token 下发给前端；
	// 必须小于 PresenceTTL，否则心跳还没到，在线记录就已经过期。
	HeartbeatEvery time.Duration
	// NodeTTL 节点注册信息的过期时间。节点按 NodeTTL/3 的频率续期，
	// 超过该时间没有心跳的节点会被其他节点的 reaper 判定为宕机。
	NodeTTL time.Duration
	// ReapEvery 后台清理协程的巡检周期，用于回收宕机节点残留的注册与在线记录。
	ReapEvery time.Duration
	// DevMode 是否开放 /api/dev/token。打开后任何人都能自签 token，
	// 仅限本地演示；生产环境必须关闭，改由业务鉴权服务签发。
	DevMode bool
}

// Load 从环境变量构建配置，未设置的项使用默认值。
// 默认值与 README 的“配置项”表格保持一致。
func Load() Config {
	return Config{
		NodeID:         env("NODE_ID", "node1"),
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		GRPCAddr:       env("GRPC_ADDR", ":9090"),
		RedisAddr:      env("REDIS_ADDR", "127.0.0.1:6379"),
		NchanPubURL:    env("NCHAN_PUB_URL", "http://127.0.0.1:8081/internal/pub"),
		TokenSecret:    env("TOKEN_SECRET", "dev-secret-change-me"),
		InternalToken:  env("INTERNAL_TOKEN", "dev-internal-token"),
		TokenTTL:       envDuration("TOKEN_TTL", time.Hour),
		PresenceTTL:    envDuration("PRESENCE_TTL", 20*time.Second),
		HeartbeatEvery: envDuration("HEARTBEAT_INTERVAL", 5*time.Second),
		NodeTTL:        envDuration("NODE_TTL", 15*time.Second),
		ReapEvery:      envDuration("REAP_INTERVAL", 10*time.Second),
		DevMode:        envBool("DEV_MODE", true),
	}
}

// env 读取字符串环境变量。空字符串视为“未设置”，返回 fallback，
// 这样 docker-compose 里误写成 `NODE_ID=` 时仍有可用默认值。
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envDuration 读取时间配置，解析顺序：
//  1. time.ParseDuration：支持 "20s"、"1h"、"500ms" 等写法；
//  2. 纯整数：按“秒”解释（"20" == 20s）；
//  3. 都失败时回退默认值，避免一个笔误导致整个进程无法启动。
func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	return fallback
}

// envBool 读取布尔配置。strconv.ParseBool 接受 1/0、t/f、true/false 等写法；
// 解析失败时回退默认值。
func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
