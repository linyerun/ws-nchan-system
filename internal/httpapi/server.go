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

type Server struct {
	cfg    config.Config
	store  *store.Store
	push   *push.Service
	stats  *stats.Counters
	logger *slog.Logger
}

func New(cfg config.Config, st *store.Store, ps *push.Service, sc *stats.Counters, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, store: st, push: ps, stats: sc, logger: logger}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.health) // 健康检查
	mux.HandleFunc("GET /api/dev/token", s.devToken)
	mux.HandleFunc("/internal/auth", s.internalAuth) // 校验websocket的token

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

type authedFunc func(http.ResponseWriter, *http.Request, string)

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

// internalAuth is called by nginx as an auth_request subrequest. A 204 tells
// nginx the WebSocket upgrade may proceed; X-User-Id is read back with
// auth_request_set and becomes the Nchan channel suffix.
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
	if token == "" {
		token = r.Header.Get("X-Token")
	}
	uid, err := auth.Verify(s.cfg.TokenSecret, token, time.Now())
	if err != nil {
		s.stats.AuthRejected.Add(1)
		writeError(w, http.StatusUnauthorized, "invalid token")
		return
	}
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

func (s *Server) logout(w http.ResponseWriter, r *http.Request, uid string) {
	if err := s.store.DeletePresence(r.Context(), uid); err != nil {
		writeError(w, http.StatusServiceUnavailable, "presence store unavailable")
		return
	}
	s.logger.Info("session closed by client", "uid", uid, "node", s.cfg.NodeID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) online(w http.ResponseWriter, r *http.Request, _ string) {
	list, err := s.store.Online(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "presence store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"online": list, "count": len(list)})
}

type publishRequest struct {
	TargetType string          `json:"target_type"`
	TargetID   string          `json:"target_id"`
	Payload    json.RawMessage `json:"payload"`
}

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

type roomMember struct {
	UID    string `json:"uid"`
	Online bool   `json:"online"`
	NodeID string `json:"node_id,omitempty"`
}

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

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node": s.cfg.NodeID})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
