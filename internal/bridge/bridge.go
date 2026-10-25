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

// Bridge is the only component that touches Nchan on the message path.
// It listens on this node's Redis channel and re-publishes locally, which is
// what turns a cluster-wide message into frames on sockets owned by this node.
type Bridge struct {
	nodeID string
	store  *store.Store
	nchan  *nchan.Client
	stats  *stats.Counters
	logger *slog.Logger
}

func New(nodeID string, st *store.Store, nc *nchan.Client, sc *stats.Counters, logger *slog.Logger) *Bridge {
	return &Bridge{nodeID: nodeID, store: st, nchan: nc, stats: sc, logger: logger}
}

func (b *Bridge) Run(ctx context.Context) {
	pubsub := b.store.SubscribeNode(ctx, b.nodeID)
	defer pubsub.Close()

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

func (b *Bridge) deliver(ctx context.Context, payload string) {
	var msg model.BridgeMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		b.logger.Warn("bridge bad payload", "err", err)
		b.stats.BridgeErrors.Add(1)
		return
	}
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
