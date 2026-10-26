# =============================================================================
# Go 应用镜像
# =============================================================================
# 这是一个“只负责搬运二进制”的镜像：
#   - 不在镜像内编译，构建前由 Makefile 的 build-linux 目标交叉编译出
#     bin/server-linux（Linux/arm64 或 amd64，取决于宿主机架构）；
#   - 运行阶段使用极小的 Alpine，只包含二进制本身；
#   - 以非 root 用户运行，降低容器被攻破后的影响面。
#
# 因为 COPY 依赖 bin/server-linux 已存在，直接执行 docker compose up
# 而没有先 make build-linux 时，会在构建阶段失败。
# =============================================================================

FROM alpine:3.20

# 创建无登录权限的系统用户（UID 10001），后续进程以它运行。
RUN adduser -D -H -u 10001 app
WORKDIR /app
# 静态链接的 Linux 二进制（构建时 CGO_ENABLED=0），无需额外运行库。
COPY bin/server-linux /app/server

USER app
# 8080 = HTTP API；9090 = gRPC 发布接口。
# 这两个端口只在容器网络内使用，由宿主机端口映射决定是否对外开放。
EXPOSE 8080 9090
ENTRYPOINT ["/app/server"]
