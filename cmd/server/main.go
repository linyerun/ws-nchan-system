// Command server 是推送节点的进程入口，负责把所有组件装配起来：
//
//	Redis store ──┬── push.Service ── HTTP API / gRPC API
//	              ├── bridge（Redis Pub/Sub -> 本地 Nchan）
//	              ├── 节点心跳协程（续期节点注册）
//	              └── reaper 协程（清理宕机节点残留）
//
// 启动顺序是有意为之：
//  1. 加载配置、创建日志、监听退出信号；
//  2. 等待 Redis 就绪（最多约 15s），否则直接退出，交给容器重启；
//  3. 启动 bridge、节点心跳、reaper 三个后台 goroutine；
//  4. 注册 HTTP 与 gRPC 路由并开始监听；
//  5. 收到 SIGINT/SIGTERM 或任一 server 报错时，优雅关闭。
//
// 注意：WebSocket 由 nginx/Nchan 持有，不经过本进程。
// 因此重启 Go 进程不会断开在线连接；新进程会重新写节点注册，
// 并在自己的 bridge 上重新订阅跨节点消息。
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
	// 1) 配置与日志。slog 的默认 logger 是全局的，
	// 各模块通过依赖注入拿到同一个实例，输出格式统一。
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// signal.NotifyContext 把 SIGINT/SIGTERM 转换成 ctx.Done()，
	// 后续所有 goroutine 都监听这个 ctx，实现统一退出。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 2) Redis 是全局状态中心，必须先可用；启动阶段做有限次重试，
	// 避免 docker compose 下应用比 Redis 先就绪导致崩溃循环。
	st := store.New(cfg.RedisAddr)
	if err := waitForRedis(ctx, st, logger); err != nil {
		logger.Error("redis unavailable", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// 3) 组装业务组件。stats 是进程内计数器，被 push / bridge / httpapi 共享。
	sc := stats.New()
	pushSvc := push.New(st, sc, logger)
	nchanClient := nchan.New(cfg.NchanPubURL, cfg.InternalToken)
	bridgeWorker := bridge.New(cfg.NodeID, st, nchanClient, sc, logger)
	go bridgeWorker.Run(ctx)

	// 节点注册心跳：每 NODE_TTL/3 续期一次，让其他节点知道本节点存活。
	// StartedAt 只在这里设置一次，重启后会被新的时间覆盖。
	nodeInfo := model.NodeInfo{ID: cfg.NodeID, Addr: cfg.HTTPAddr, StartedAt: time.Now().UnixMilli()}
	go heartbeatLoop(ctx, st, nodeInfo, cfg.NodeTTL, logger)
	// 故障清理：定期扫描其他已过期节点，回收其注册与在线记录。
	go reapLoop(ctx, st, cfg, logger)

	// 4) HTTP 服务：REST API + nginx auth_request 端点。
	// ReadHeaderTimeout 防慢速 header 攻击；请求体大小在 handler 内单独限制。
	api := httpapi.New(cfg, st, pushSvc, sc, logger)
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// gRPC 服务：复用同一个 pushSvc，只是协议与鉴权方式不同。
	grpcSrv := grpc.NewServer()
	pb.RegisterPushServiceServer(grpcSrv, grpcapi.New(pushSvc, cfg.InternalToken))
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("grpc listen failed", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}

	// 两个 server 各自在 goroutine 中运行；errCh 容量为 2，
	// 保证任一失败都能非阻塞上报（即使另一个随后也失败）。
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

	// 阻塞等待：要么收到退出信号，要么某个 server 异常退出。
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("server error", "err", err)
	}

	// 优雅关闭：HTTP 最多等 5s 处理完在途请求；gRPC 等待已开始的 RPC 结束。
	// 长连接由 nginx 持有，这里不需要处理 WebSocket 连接迁移。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
}

// waitForRedis 在启动阶段最多尝试 30 次（每次约 1s 超时 + 0.5s 间隔），
// 直到 Ping 成功或 ctx 被取消。这样容器编排下无需严格保证启动顺序，
// 又不会因为 Redis 永久不可用而无限挂起。
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

// heartbeatLoop 周期性续期节点注册，让本节点在 nodes:index 中保持存活。
//
// 第一次循环立即执行（循环体在 select 之前），避免刚启动时出现
// “节点尚未注册”的空窗；之后按 ttl/3 的节奏续期，给网络抖动留出余量。
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

// reapLoop 周期性清理心跳过期的其他节点。
//
// 每个存活节点都会执行同样的清理逻辑，因此任意节点宕机，
// 其余节点都能在 REAP_INTERVAL 内回收其注册与在线记录，
// 不需要选主或中心化协调者。清理结果只用于日志观察。
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
