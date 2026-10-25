package stats

import (
	"sync/atomic"
	"time"
)

// Counters are per-node, in-memory and intentionally cheap. They answer
// "is this node healthy" rather than serving as billing-grade metrics.
type Counters struct {
	Started time.Time

	AuthOK       atomic.Int64
	AuthRejected atomic.Int64
	Heartbeats   atomic.Int64

	Published     atomic.Int64
	FanoutTargets atomic.Int64
	BridgeOK      atomic.Int64
	BridgeErrors  atomic.Int64
	NchanErrors   atomic.Int64
}

func New() *Counters { return &Counters{Started: time.Now()} }

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
