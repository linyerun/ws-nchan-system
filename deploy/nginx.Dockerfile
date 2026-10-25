FROM debian:trixie-slim

# 默认使用国内可达的 Debian 镜像；如需官方源可覆盖：
#   docker build --build-arg DEBIAN_MIRROR=deb.debian.org ...
ARG DEBIAN_MIRROR=mirrors.aliyun.com

RUN sed -i "s|deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends nginx libnginx-mod-nchan ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log

STOPSIGNAL SIGQUIT
CMD ["nginx", "-g", "daemon off;"]
