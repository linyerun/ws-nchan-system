# =============================================================================
# 开发与演示快捷命令
# =============================================================================
# 常用流程：
#   make up          # 编译二进制 + 构建镜像 + 启动单节点
#   make up-cluster  # 额外启动 node2，观察跨节点扩散
#   make e2e         # 单节点 12 项端到端校验
#   make e2e-cluster # 双节点 6 项端到端校验
#   make down        # 停止并删除容器
# =============================================================================

SHELL := /bin/sh

# 宿主机架构归一化：Go 使用 amd64/arm64，而 uname -m 在 x86 上给 x86_64、
# 在 Apple Silicon 上给 arm64。这里做一次映射，用于交叉编译 Linux 二进制。
ARCH ?= $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
GO ?= go
# 交叉编译输出路径；app 镜像会 COPY 这个文件。
BIN := bin/server-linux

.PHONY: build build-linux up up-cluster down logs e2e e2e-cluster loadtest pushcli proto clean

# 仅做本机编译检查（当前平台），不生成 Linux 二进制。
build:
	$(GO) build ./...

# 交叉编译服务端为 Linux 二进制，供 Docker 镜像使用。
#   CGO_ENABLED=0 -> 纯静态链接，Alpine 里无需 glibc；
#   -trimpath     -> 去掉本机绝对路径，构建可复现；
#   -s -w         -> 去掉符号表和调试信息，减小体积。
build-linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) $(GO) build -trimpath -ldflags "-s -w" -o $(BIN) ./cmd/server

# 单节点启动：必须先 build-linux，再构建并启动 compose 服务。
up: build-linux
	docker compose up -d --build

# 双节点启动：--profile cluster 会额外创建 node2-app / node2-nginx。
up-cluster: build-linux
	docker compose --profile cluster up -d --build

# 停止并删除容器。带上 cluster profile，保证 node2 也被清理。
down:
	docker compose --profile cluster down

# 跟踪全部服务日志（含 cluster profile）。
logs:
	docker compose --profile cluster logs -f --tail=100

# 单节点端到端测试：鉴权、定向、房间、广播、离线计数、心跳、TTL 回收等。
e2e:
	node scripts/e2e.mjs

# 双节点端到端测试：跨节点路由与节点注册表。
e2e-cluster:
	node scripts/e2e-cluster.mjs

# 压测脚本；连接数通过 CONNECTIONS 环境变量控制，例如：
#   CONNECTIONS=300 make loadtest
loadtest:
	node scripts/loadtest.mjs

# 用 gRPC CLI 发布一条测试消息，参数通过 ARGS 透传，例如：
#   make pushcli ARGS="-grpc 127.0.0.1:19090 -type user -id bob -text hello"
pushcli:
	$(GO) run ./cmd/pushcli $(ARGS)

# 重新生成 gRPC 代码（需要本机安装 protoc 与 Go 插件）。
# 生成结果已提交到 internal/pb，日常构建不必执行此目标。
proto:
	protoc -I api --go_out=. --go_opt=module=ws-nchan-system \
		--go-grpc_out=. --go-grpc_opt=module=ws-nchan-system api/push.proto

# 彻底清理：删除容器、网络、卷以及本地编译产物。
# 注意 -v 会删除 compose 创建的卷；本项目 Redis 未持久化，不会丢业务数据。
clean:
	docker compose --profile cluster down -v
	rm -rf bin
