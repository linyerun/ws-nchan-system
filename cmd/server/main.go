package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"ws-nchan-system/internal/bridge"
	"ws-nchan-system/internal/config"
	"ws-nchan-system/internal/grpcapi"
	"ws-nchan-system/internal/httpapi"
	"ws-nchan-system/internal/model"
	"ws-nchan-system/internal/nchan"
	"ws-nchan-system/internal/pb"
	"ws-nchan-system/internal/push"
	"ws-nchan-system/internal/stats"
	"ws-nchan-system/internal/store"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st := store.New(cfg.RedisAddr)
	if err := waitForRedis(ctx, st, logger); err != nil {
		logger.Error("redis unavailable", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	defer st.Close()

	sc := stats.New()
	pushSvc := push.New(st, sc, logger)
	nchanClient := nchan.New(cfg.NchanPubURL, cfg.InternalToken)
	bridgeWorker := bridge.New(cfg.NodeID, st, nchanClient, sc, logger)
	go bridgeWorker.Run(ctx)

	nodeInfo := model.NodeInfo{ID: cfg.NodeID, Addr: cfg.HTTPAddr, StartedAt: time.Now().UnixMilli()}
	go heartbeatLoop(ctx, st, nodeInfo, cfg.NodeTTL, logger)
	go reapLoop(ctx, st, cfg, logger)

	api := httpapi.New(cfg, st, pushSvc, sc, logger)
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterPushServiceServer(grpcSrv, grpcapi.New(pushSvc, cfg.InternalToken))
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("grpc listen failed", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr, "node", cfg.NodeID)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		logger.Info("grpc listening", "addr", cfg.GRPCAddr, "node", cfg.NodeID)
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	logger.Info("push node started",
		"node", cfg.NodeID,
		"redis", cfg.RedisAddr,
		"nchan", cfg.NchanPubURL,
		"presence_ttl", cfg.PresenceTTL,
		"heartbeat", cfg.HeartbeatEvery,
	)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("server error", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
}

func waitForRedis(ctx context.Context, st *store.Store, logger *slog.Logger) error {
	var err error
	for i := 0; i < 30; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		err = st.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if i == 0 {
			logger.Info("waiting for redis")
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

func heartbeatLoop(ctx context.Context, st *store.Store, info model.NodeInfo, ttl time.Duration, logger *slog.Logger) {
	tick := time.NewTicker(ttl / 3)
	defer tick.Stop()
	for {
		if err := st.TouchNode(ctx, info, ttl); err != nil {
			logger.Warn("node heartbeat failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func reapLoop(ctx context.Context, st *store.Store, cfg config.Config, logger *slog.Logger) {
	tick := time.NewTicker(cfg.ReapEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		dead, err := st.ReapDeadNodes(ctx, cfg.NodeID)
		if err != nil {
			logger.Warn("reaper failed", "err", err)
			continue
		}
		for _, d := range dead {
			logger.Warn("reaped dead node", "node", d.ID, "offline_users", d.Users)
		}
	}
}
