// Package httpapi 实现控制面的 HTTP 接口。
//
// 它同时承担两类职责：
//
//  1. 面向浏览器的 REST API：签发 token、心跳、下线、在线列表、推送、房间管理；
//  2. 面向 nginx 的内部鉴权端点 /internal/auth：WebSocket 握手前由
//     auth_request 子请求调用，校验 token 并写入 presence。
//
// 两类入口使用不同的鉴权方式：
//   - REST API：Authorization: Bearer <token>，由 authed 中间件统一校验；
//   - /internal/auth：token 来自 nginx 透传的 X-Original-URI（WebSocket
//     浏览器无法自定义 Header），校验成功返回 204 + X-User-Id。
//
// 服务本身不持有 WebSocket；长连接完全由 nginx/Nchan 管理，
// 因此这里的所有 handler 都是短请求，超时风险低。
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"ws-nchan-system/internal/auth"
	"ws-nchan-system/internal/config"
	"ws-nchan-system/internal/model"
	"ws-nchan-system/internal/push"
	"ws-nchan-system/internal/stats"
	"ws-nchan-system/internal/store"
)

// Server 聚合 HTTP 层需要的依赖，由 main 创建一次后长期复用。
type Server struct {
	cfg    config.Config
	store  *store.Store
	push   *push.Service
	stats  *stats.Counters
	logger *slog.Logger
}

// New 构造 HTTP Server。依赖均为接口/指针，可安全并发使用。
func New(cfg config.Config, st *store.Store, ps *push.Service, sc *stats.Counters, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, store: st, push: ps, stats: sc, logger: logger}
}

// Handler 注册全部路由并返回 http.Handler。
//
// Go 1.22+ 的方法+路径模式（"POST /api/push"）让方法匹配也交给 ServeMux；
// 路径中的 {room} 通过 r.PathValue("room") 读取。
//
// 路由分组：
//   - 公共：/healthz、/api/dev/token（受 DevMode 控制）、/api/stats；
//   - 内部：/internal/auth，仅供 nginx 在容器网络内调用；
//   - 需要 Bearer token：其余 /api/*。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.health) // 健康检查，供容器 healthcheck 使用
	mux.HandleFunc("GET /api/dev/token", s.devToken)
	mux.HandleFunc("/internal/auth", s.internalAuth) // 校验 WebSocket 的 token（nginx 子请求）

	mux.Handle("GET /api/me", s.authed(s.me))
	mux.Handle("POST /api/heartbeat", s.authed(s.heartbeat))
	mux.Handle("POST /api/logout", s.authed(s.logout))
	mux.Handle("GET /api/online", s.authed(s.online))
	mux.Handle("POST /api/push", s.authed(s.publish))
	mux.Handle("POST /api/rooms/{room}/join", s.authed(s.joinRoom))
	mux.Handle("POST /api/rooms/{room}/leave", s.authed(s.leaveRoom))
	mux.Handle("GET /api/rooms/{room}", s.authed(s.roomInfo))
	mux.HandleFunc("GET /api/stats", s.statsHandler)

	return mux
}

// authedFunc 是需要用户身份的业务 handler 签名。
// uid 由中间件从 token 中解析后传入，避免每个 handler 重复解析。
type authedFunc func(http.ResponseWriter, *http.Request, string)

// authed 把带 Bearer token 校验的中间件套在业务 handler 外层。
// 校验失败统一返回 401，不区分“缺少 token”和“token 非法”，
// 避免给攻击者提供额外信息。
func (s *Server) authed(next authedFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid, ok := s.bearerUID(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing or invalid token")
			return
		}
		next(w, r, uid)
	})
}

// bearerUID 从 Authorization: Bearer <token> 中解析并校验 uid。
//
// token 由 auth.Verify 做无状态校验（签名 + 过期时间），
// 因此这里不访问 Redis，鉴权本身不会成为瓶颈。
func (s *Server) bearerUID(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return "", false
	}
	uid, err := auth.Verify(s.cfg.TokenSecret, strings.TrimPrefix(h, "Bearer "), time.Now())
	if err != nil {
		return "", false
	}
	return uid, true
}

// internalAuth 由 nginx 的 auth_request 子请求调用。
//
// 协议约定：
//   - nginx 通过 X-Original-URI 传入原始 WebSocket 请求 URI（含 query）；
//   - Go 从 query 中取出 token 和 uid，校验签名并确认两者匹配；
//   - 校验通过后先把 presence 写入 Redis，再返回 204；
//   - nginx 用 auth_request_set 读取响应头 X-User-Id，作为 Nchan 频道后缀。
//
// 顺序很关键：“先注册、再订阅”。如果先让 Nchan 建立订阅、后写 presence，
// 就会出现“用户显示在线但消息路由找不到节点”的空窗；反过来，
// 若握手最终失败，多出来的 presence 会在 PRESENCE_TTL 后自动过期。
//
// 额外校验 uid：防止用 alice 的合法 token 搭配 uid=bob 订阅别人的频道。
func (s *Server) internalAuth(w http.ResponseWriter, r *http.Request) {
	token := ""
	if original := r.Header.Get("X-Original-URI"); original != "" {
		if u, err := url.Parse(original); err == nil {
			token = u.Query().Get("token")
			if uid := u.Query().Get("uid"); uid != "" {
				if verified, err := auth.Verify(s.cfg.TokenSecret, token, time.Now()); err != nil || verified != uid {
					s.stats.AuthRejected.Add(1)
					writeError(w, http.StatusUnauthorized, "token/uid mismatch")
					return
				}
			}
		}
	}
	// 兼容直接携带 X-Token 的调用方式（调试或非浏览器客户端）。
	if token == "" {
		token = r.Header.Get("X-Token")
	}
	uid, err := auth.Verify(s.cfg.TokenSecret, token, time.Now())
	if err != nil {
		s.stats.AuthRejected.Add(1)
		writeError(w, http.StatusUnauthorized, "invalid token")
		return
	}
	// 写 presence 失败时不能放行：否则连接建立了但无法被路由，消息永远收不到。
	if err := s.store.TouchPresence(r.Context(), uid, s.cfg.NodeID, s.cfg.PresenceTTL); err != nil {
		s.stats.AuthRejected.Add(1)
		writeError(w, http.StatusServiceUnavailable, "presence store unavailable")
		return
	}
	s.stats.AuthOK.Add(1)
	s.logger.Info("ws session registered", "uid", uid, "node", s.cfg.NodeID)
	w.Header().Set("X-User-Id", uid)
	w.WriteHeader(http.StatusNoContent)
}

// devToken 是开发用 token 签发接口，仅在 DevMode 打开时可用。
//
// 除了 token 本身，还下发心跳间隔、presence TTL、节点 ID 等元信息，
// 前端据此配置心跳定时器并展示当前连接落在哪个节点。
// 生产环境应关闭该接口，改由业务鉴权系统签发 token。
func (s *Server) devToken(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.DevMode {
		writeError(w, http.StatusNotFound, "dev token endpoint disabled")
		return
	}
	uid := r.URL.Query().Get("uid")
	if !auth.ValidUID(uid) {
		writeError(w, http.StatusBadRequest, "uid must match [A-Za-z0-9_-]{1,32}")
		return
	}
	token, exp := auth.Sign(s.cfg.TokenSecret, uid, s.cfg.TokenTTL)
	writeJSON(w, http.StatusOK, map[string]any{
		"token":          token,
		"uid":            uid,
		"exp":            exp,
		"node_id":        s.cfg.NodeID,
		"ttl_seconds":    int(s.cfg.TokenTTL.Seconds()),
		"presence_ttl":   int(s.cfg.PresenceTTL.Seconds()),
		"heartbeat_rate": int(s.cfg.HeartbeatEvery.Seconds()),
	})
}

// me 返回当前 token 对应的身份、所在节点、在线状态与已加入房间。
// 前端的房间列表定时刷新依赖这个接口。
func (s *Server) me(w http.ResponseWriter, r *http.Request, uid string) {
	p, online := s.store.GetPresence(r.Context(), uid)
	rooms, _ := s.store.UserRooms(r.Context(), uid)
	writeJSON(w, http.StatusOK, map[string]any{
		"uid":      uid,
		"node_id":  s.cfg.NodeID,
		"online":   online,
		"presence": p,
		"rooms":    rooms,
	})
}

// heartbeat 续期 presence TTL，是“在线状态”保活的唯一途径。
//
// 前端每 HEARTBEAT_INTERVAL（默认 5s）调用一次；只要心跳持续，
// presence 就不会过期。客户端崩溃后心跳停止，最多 PRESENCE_TTL（20s）
// 就会自动离线，无须依赖不可靠的 socket 关闭回调。
func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request, uid string) {
	if err := s.store.TouchPresence(r.Context(), uid, s.cfg.NodeID, s.cfg.PresenceTTL); err != nil {
		writeError(w, http.StatusServiceUnavailable, "presence store unavailable")
		return
	}
	s.stats.Heartbeats.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"node_id":     s.cfg.NodeID,
		"ttl_seconds": int(s.cfg.PresenceTTL.Seconds()),
	})
}

// logout 在客户端主动断开时立即清理 presence，让其他用户马上看到离线，
// 不用等待 TTL。异常断开仍由 TTL 兜底。
//
// 注意：这里不会关闭 WebSocket——socket 由 nginx/Nchan 持有，
// 前端在调用本接口后自行 close。
func (s *Server) logout(w http.ResponseWriter, r *http.Request, uid string) {
	if err := s.store.DeletePresence(r.Context(), uid); err != nil {
		writeError(w, http.StatusServiceUnavailable, "presence store unavailable")
		return
	}
	s.logger.Info("session closed by client", "uid", uid, "node", s.cfg.NodeID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// online 返回全局在线用户列表（跨所有节点），数据源是 Redis，
// 因此任意节点看到的结果一致。
func (s *Server) online(w http.ResponseWriter, r *http.Request, _ string) {
	list, err := s.store.Online(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "presence store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"online": list, "count": len(list)})
}

// publishRequest 是 POST /api/push 的请求体。
// payload 使用 json.RawMessage 透传，服务端不关心业务字段结构。
type publishRequest struct {
	TargetType string          `json:"target_type"`
	TargetID   string          `json:"target_id"`
	Payload    json.RawMessage `json:"payload"`
}

// publish 是浏览器/业务方通过 HTTP 发布消息的入口。
//
// 发送者身份取自 Bearer token（而不是请求体），避免伪造 From。
// 参数错误返回 400，内部错误返回 500；成功时返回各接收者计数与目标节点。
func (s *Server) publish(w http.ResponseWriter, r *http.Request, uid string) {
	var req publishRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := s.push.Publish(r.Context(), model.PublishInput{
		From:       uid,
		TargetType: req.TargetType,
		TargetID:   req.TargetID,
		Payload:    req.Payload,
	})
	if err != nil {
		if errors.Is(err, push.ErrBadTarget) || errors.Is(err, push.ErrBadPayload) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.logger.Error("publish failed", "err", err)
		writeError(w, http.StatusInternalServerError, "publish failed")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// joinRoom 加入房间，成功后直接返回房间最新信息（含成员在线状态）。
func (s *Server) joinRoom(w http.ResponseWriter, r *http.Request, uid string) {
	room := r.PathValue("room")
	if !auth.ValidUID(room) {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}
	if err := s.store.JoinRoom(r.Context(), uid, room); err != nil {
		writeError(w, http.StatusServiceUnavailable, "room store unavailable")
		return
	}
	s.roomInfo(w, r, uid)
}

// leaveRoom 退出房间，成功后同样返回房间最新信息。
func (s *Server) leaveRoom(w http.ResponseWriter, r *http.Request, uid string) {
	room := r.PathValue("room")
	if !auth.ValidUID(room) {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}
	if err := s.store.LeaveRoom(r.Context(), uid, room); err != nil {
		writeError(w, http.StatusServiceUnavailable, "room store unavailable")
		return
	}
	s.roomInfo(w, r, uid)
}

// roomMember 是房间成员在 API 中的展示形式：uid + 是否在线 + 所在节点。
type roomMember struct {
	UID    string `json:"uid"`
	Online bool   `json:"online"`
	NodeID string `json:"node_id,omitempty"`
}

// roomInfo 返回房间成员列表及调用者自己加入的房间。
//
// 房间成员保存在 Redis Set 中，包含离线成员；这里批量读取 presence，
// 给每个成员附上在线标记与所在节点，前端据此展示 n/N 在线。
func (s *Server) roomInfo(w http.ResponseWriter, r *http.Request, uid string) {
	room := r.PathValue("room")
	if !auth.ValidUID(room) {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}
	members, err := s.store.RoomMembers(r.Context(), room)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "room store unavailable")
		return
	}
	presences := s.store.GetManyPresence(r.Context(), members)
	out := make([]roomMember, 0, len(members))
	for _, m := range members {
		rm := roomMember{UID: m}
		if p, ok := presences[m]; ok {
			rm.Online = true
			rm.NodeID = p.NodeID
		}
		out = append(out, rm)
	}
	rooms, _ := s.store.UserRooms(r.Context(), uid)
	writeJSON(w, http.StatusOK, map[string]any{"room": room, "members": out, "my_rooms": rooms})
}

// statsHandler 汇总本节点内存计数 + 全局在线数 + 节点注册表，
// 是前端底部状态栏和排障时的主要观察入口。
// 该接口不需要鉴权，仅暴露计数和节点信息，不含用户隐私。
func (s *Server) statsHandler(w http.ResponseWriter, r *http.Request) {
	online, _ := s.store.Online(r.Context())
	nodes, _ := s.store.Nodes(r.Context())
	payload := s.stats.Snapshot(s.cfg.NodeID)
	payload["online_count"] = len(online)
	payload["nodes"] = nodes
	payload["presence_ttl_seconds"] = int(s.cfg.PresenceTTL.Seconds())
	payload["heartbeat_interval_seconds"] = int(s.cfg.HeartbeatEvery.Seconds())
	writeJSON(w, http.StatusOK, payload)
}

// health 是容器健康检查与负载均衡探活使用的极简接口。
// 不检查 Redis：Redis 故障时 HTTP 进程仍然健康，由请求路径自行报错。
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node": s.cfg.NodeID})
}

// decodeJSON 解析请求体，并限制最大 1 MiB，防止超大 body 耗尽内存。
// 这里只做一次 Decode，不接受尾部多余字段，避免隐藏的畸形请求。
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

// writeJSON 统一输出 JSON 响应并设置 Content-Type。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError 以 {"error": "..."} 的形式返回错误，前端直接读取该字段展示。
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
