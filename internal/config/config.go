package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds every runtime knob of one push node.
type Config struct {
	NodeID         string
	HTTPAddr       string
	GRPCAddr       string
	RedisAddr      string
	NchanPubURL    string
	TokenSecret    string
	InternalToken  string
	TokenTTL       time.Duration
	PresenceTTL    time.Duration
	HeartbeatEvery time.Duration
	NodeTTL        time.Duration
	ReapEvery      time.Duration
	DevMode        bool
}

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

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

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
