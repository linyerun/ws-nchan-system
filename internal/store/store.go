package store

import (
	"context"
	"encoding/json"

	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"ws-nchan-system/internal/model"
)

const (
	presenceIndexKey = "presence:index"
	nodesIndexKey    = "nodes:index"
)

type Store struct {
	rdb *redis.Client
}

func New(addr string) *Store {
	return &Store{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }
func (s *Store) Close() error                   { return s.rdb.Close() }

func (s *Store) presenceKey(uid string) string  { return "presence:user:" + uid }
func (s *Store) nodeKey(id string) string       { return "node:" + id }
func (s *Store) nodeUsersKey(id string) string  { return "node:" + id + ":users" }
func (s *Store) roomKey(id string) string       { return "room:" + id + ":members" }
func (s *Store) userRoomsKey(uid string) string { return "user:" + uid + ":rooms" }

// TouchPresence writes the user's routing record and renews its TTL.
// TTL expiry is the authoritative "offline" signal, because disconnect
// callbacks from the socket layer are always best-effort.
func (s *Store) TouchPresence(ctx context.Context, uid, nodeID string, ttl time.Duration) error {
	now := time.Now()
	key := s.presenceKey(uid)

	since, err := s.rdb.HGet(ctx, key, "since").Int64()
	if err == redis.Nil {
		since = now.UnixMilli()
	} else if err != nil {
		return err
	}

	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"uid":     uid,
		"node_id": nodeID,
		"since":   since,
		"last_hb": now.UnixMilli(),
	})
	pipe.PExpire(ctx, key, ttl)
	pipe.ZAdd(ctx, presenceIndexKey, redis.Z{
		Score:  float64(now.Add(ttl).UnixMilli()),
		Member: uid,
	})
	pipe.ZAdd(ctx, s.nodeUsersKey(nodeID), redis.Z{
		Score:  float64(now.Add(ttl).UnixMilli()),
		Member: uid,
	})
	_, err = pipe.Exec(ctx)
	return err
}

func (s *Store) GetPresence(ctx context.Context, uid string) (model.Presence, bool) {
	m, err := s.rdb.HGetAll(ctx, s.presenceKey(uid)).Result()
	if err != nil || len(m) == 0 {
		return model.Presence{}, false
	}
	return toPresence(uid, m), true
}

// DeletePresence removes a session immediately on graceful logout. The TTL is
// still the fallback for crashed clients and half-open sockets.
func (s *Store) DeletePresence(ctx context.Context, uid string) error {
	p, ok := s.GetPresence(ctx, uid)
	if !ok {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, s.presenceKey(uid))
	pipe.ZRem(ctx, presenceIndexKey, uid)
	if p.NodeID != "" {
		pipe.ZRem(ctx, s.nodeUsersKey(p.NodeID), uid)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) GetManyPresence(ctx context.Context, uids []string) map[string]model.Presence {
	out := make(map[string]model.Presence, len(uids))
	if len(uids) == 0 {
		return out
	}
	pipe := s.rdb.Pipeline()
	cmds := make(map[string]*redis.MapStringStringCmd, len(uids))
	for _, uid := range uids {
		cmds[uid] = pipe.HGetAll(ctx, s.presenceKey(uid))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return out
	}
	for uid, cmd := range cmds {
		if m, err := cmd.Result(); err == nil && len(m) > 0 {
			out[uid] = toPresence(uid, m)
		}
	}
	return out
}

func (s *Store) Online(ctx context.Context) ([]model.Presence, error) {
	now := time.Now().UnixMilli()
	if err := s.rdb.ZRemRangeByScore(ctx, presenceIndexKey, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	uids, err := s.rdb.ZRange(ctx, presenceIndexKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	presences := s.GetManyPresence(ctx, uids)

	out := make([]model.Presence, 0, len(presences))
	var dangling []any
	for _, uid := range uids {
		if p, ok := presences[uid]; ok {
			out = append(out, p)
		} else {
			// The presence hash expired but the index entry has not been
			// pruned yet; drop it so the online list never shows ghosts.
			dangling = append(dangling, uid)
		}
	}
	if len(dangling) > 0 {
		_ = s.rdb.ZRem(ctx, presenceIndexKey, dangling...).Err()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UID < out[j].UID })
	return out, nil
}

func (s *Store) TouchNode(ctx context.Context, info model.NodeInfo, ttl time.Duration) error {
	now := time.Now()
	key := s.nodeKey(info.ID)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"id":         info.ID,
		"addr":       info.Addr,
		"started_at": info.StartedAt,
		"last_hb":    now.UnixMilli(),
	})
	pipe.PExpire(ctx, key, ttl)
	pipe.ZAdd(ctx, nodesIndexKey, redis.Z{
		Score:  float64(now.Add(ttl).UnixMilli()),
		Member: info.ID,
	})
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) Nodes(ctx context.Context) ([]model.NodeInfo, error) {
	now := time.Now().UnixMilli()
	if err := s.rdb.ZRemRangeByScore(ctx, nodesIndexKey, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	ids, err := s.rdb.ZRange(ctx, nodesIndexKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	nodes := make([]model.NodeInfo, 0, len(ids))
	for _, id := range ids {
		m, err := s.rdb.HGetAll(ctx, s.nodeKey(id)).Result()
		if err != nil || len(m) == 0 {
			continue
		}
		n := model.NodeInfo{ID: id, Addr: m["addr"], StartedAt: atoi64(m["started_at"]), LastHB: atoi64(m["last_hb"])}
		if err := s.rdb.ZRemRangeByScore(ctx, s.nodeUsersKey(id), "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
			return nil, err
		}
		n.Online = int(s.rdb.ZCard(ctx, s.nodeUsersKey(id)).Val())
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

type DeadNode struct {
	ID    string
	Users []string
}

// ReapDeadNodes removes registry and presence records that point to nodes
// whose heartbeat TTL has expired. Presence is re-checked before deletion so
// a user that reconnected to a different node is never dropped by mistake.
func (s *Store) ReapDeadNodes(ctx context.Context, selfID string) ([]DeadNode, error) {
	now := time.Now().UnixMilli()
	expired, err := s.rdb.ZRangeByScore(ctx, nodesIndexKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil {
		return nil, err
	}

	var dead []DeadNode
	for _, id := range expired {
		if id == selfID {
			continue
		}
		users, _ := s.rdb.ZRange(ctx, s.nodeUsersKey(id), 0, -1).Result()
		removed := make([]string, 0, len(users))
		for _, uid := range users {
			p, ok := s.GetPresence(ctx, uid)
			if ok && p.NodeID == id {
				_ = s.rdb.Del(ctx, s.presenceKey(uid)).Err()
				_ = s.rdb.ZRem(ctx, presenceIndexKey, uid).Err()
				removed = append(removed, uid)
			}
		}
		pipe := s.rdb.Pipeline()
		pipe.Del(ctx, s.nodeKey(id))
		pipe.Del(ctx, s.nodeUsersKey(id))
		pipe.ZRem(ctx, nodesIndexKey, id)
		_, _ = pipe.Exec(ctx)
		dead = append(dead, DeadNode{ID: id, Users: removed})
	}
	return dead, nil
}

func (s *Store) JoinRoom(ctx context.Context, uid, room string) error {
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, s.roomKey(room), uid)
	pipe.SAdd(ctx, s.userRoomsKey(uid), room)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) LeaveRoom(ctx context.Context, uid, room string) error {
	pipe := s.rdb.Pipeline()
	pipe.SRem(ctx, s.roomKey(room), uid)
	pipe.SRem(ctx, s.userRoomsKey(uid), room)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) RoomMembers(ctx context.Context, room string) ([]string, error) {
	members, err := s.rdb.SMembers(ctx, s.roomKey(room)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(members)
	return members, nil
}

func (s *Store) UserRooms(ctx context.Context, uid string) ([]string, error) {
	rooms, err := s.rdb.SMembers(ctx, s.userRoomsKey(uid)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(rooms)
	return rooms, nil
}

// PublishToNode routes a message to one node's bridge. The returned count is
// the number of live subscribers on that Redis channel; 0 means the node's
// bridge is not running and the message was dropped.
func (s *Store) PublishToNode(ctx context.Context, nodeID string, msg model.BridgeMessage) (int64, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}
	return s.rdb.Publish(ctx, "push:node:"+nodeID, payload).Result()
}

func (s *Store) SubscribeNode(ctx context.Context, nodeID string) *redis.PubSub {
	return s.rdb.Subscribe(ctx, "push:node:"+nodeID)
}

func toPresence(uid string, m map[string]string) model.Presence {
	return model.Presence{
		UID:    uid,
		NodeID: m["node_id"],
		Since:  atoi64(m["since"]),
		LastHB: atoi64(m["last_hb"]),
	}
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
