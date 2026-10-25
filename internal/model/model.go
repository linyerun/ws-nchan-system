package model

import "encoding/json"

// Presence is the global (Redis-backed) view of one online user.
// It is the routing source of truth: uid -> node that owns the live socket.
type Presence struct {
	UID    string `json:"uid"`
	NodeID string `json:"node_id"`
	Since  int64  `json:"since"`
	LastHB int64  `json:"last_hb"`
}

// Envelope is the message body that Nchan pushes down the socket.
type Envelope struct {
	ID      string          `json:"id"`
	Kind    string          `json:"kind"` // direct | room | broadcast
	From    string          `json:"from"`
	To      string          `json:"to,omitempty"`
	Room    string          `json:"room,omitempty"`
	TS      int64           `json:"ts"`
	Payload json.RawMessage `json:"payload"`
}

// BridgeMessage is the Redis Pub/Sub payload exchanged between nodes.
type BridgeMessage struct {
	Channel string          `json:"channel"`
	Body    json.RawMessage `json:"body"`
}

type PublishInput struct {
	From       string
	TargetType string
	TargetID   string
	Payload    json.RawMessage
}

type PublishResult struct {
	MsgID   string   `json:"msg_id"`
	Online  int      `json:"online_recipients"`
	Offline int      `json:"offline_recipients"`
	Failed  int      `json:"failed_recipients"`
	Nodes   []string `json:"nodes,omitempty"`
}

type NodeInfo struct {
	ID        string `json:"id"`
	Addr      string `json:"addr"`
	StartedAt int64  `json:"started_at"`
	LastHB    int64  `json:"last_hb"`
	Online    int    `json:"online"`
}
