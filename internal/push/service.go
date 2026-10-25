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
	ErrBadTarget  = errors.New("invalid target")
	ErrBadPayload = errors.New("payload must be a JSON object")
)

type Service struct {
	store  *store.Store
	stats  *stats.Counters
	logger *slog.Logger
}

func New(st *store.Store, sc *stats.Counters, logger *slog.Logger) *Service {
	return &Service{store: st, stats: sc, logger: logger}
}

// Publish resolves a logical target into concrete online uids, groups them by
// the node that owns their socket, and hands each group to Redis Pub/Sub.
func (s *Service) Publish(ctx context.Context, in model.PublishInput) (model.PublishResult, error) {
	if in.From == "" {
		in.From = "server"
	}
	if !auth.ValidUID(in.From) {
		return model.PublishResult{}, fmt.Errorf("%w: from", ErrBadTarget)
	}
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
	presences := s.store.GetManyPresence(ctx, uids)

	byNode := map[string][]string{}
	online := 0
	for _, uid := range uids {
		p, ok := presences[uid]
		if !ok {
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
				// The node's presence has not expired yet but its bridge is
				// gone; this is exactly the ghost-session window.
				s.logger.Warn("node bridge missing", "node", node, "uid", uid)
				res.Failed++
			}
		}
	}

	s.stats.Published.Add(1)
	s.stats.FanoutTargets.Add(int64(online))
	return res, nil
}

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

func newMsgID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("msg_%d", time.Now().UnixNano())
	}
	return "msg_" + hex.EncodeToString(b[:])
}
