# =============================================================================
# Nginx + Nchan 网关镜像
# =============================================================================
# 不依赖机器上预置的 nchan 镜像：
#   - 基础镜像使用官方 debian:trixie-slim；
#   - 通过 Debian 软件包安装 nginx 与 libnginx-mod-nchan 动态模块；
#   - nginx.conf.template 由 docker-compose 的启动命令用 sed 渲染。
#
# 构建需要访问 Debian 软件源，默认使用国内镜像；可这样覆盖：
#   docker build --build-arg DEBIAN_MIRROR=deb.debian.org \
#     -f deploy/nginx.Dockerfile -t ws-nchan-nginx:local .
# 或在 make up 时设置环境变量 DEBIAN_MIRROR。
# =============================================================================

FROM debian:trixie-slim

# 软件源镜像地址参数（仅构建期有效，不会进入最终镜像）。
ARG DEBIAN_MIRROR=mirrors.aliyun.com

# 替换 debian.sources 中的域名后安装：
#   nginx                 -> 网关本体
#   libnginx-mod-nchan    -> Nchan 动态模块（提供 nchan_subscriber 等指令）
#   ca-certificates       -> 访问 HTTPS 上游所需
# 安装完立即清理 apt 列表，减小镜像体积。
RUN sed -i "s|deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends nginx libnginx-mod-nchan ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 把访问/错误日志直接接到容器标准输出，方便 docker logs 观察。
RUN ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log

# Docker 默认用 SIGTERM 停止容器；nginx 对 SIGQUIT 才会优雅退出
# （处理完在途请求再关闭 worker），因此显式指定 STOPSIGNAL。
STOPSIGNAL SIGQUIT
# daemon off 让 nginx 留在前台，容器主进程不会立刻退出。
# 实际启动命令由 docker-compose 覆盖：先渲染配置模板，再 exec nginx。
CMD ["nginx", "-g", "daemon off;"]
