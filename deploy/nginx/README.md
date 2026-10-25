# Nginx + Nchan 接入层配置说明

本文详细解释 [`deploy/nginx/nginx.conf.template`](./nginx.conf.template) 的每一段配置，以及它和 Go 应用、浏览器之间的协作契约。

---

## 1. 这个文件是什么

它不是一份可以直接运行的 `nginx.conf`，而是一份**配置模板**。里面有两个占位符：

| 占位符 | 替换来源 | 最终效果 |
|---|---|---|
| `@NODE@` | 容器的 `NODE` 环境变量（`node1` / `node2`） | upstream 指向 `node1-app:8080` / `node2-app:8080` |
| `@INTERNAL_TOKEN@` | 容器的 `INTERNAL_TOKEN` 环境变量 | 校验内部发布请求的共享密钥 |

容器启动时，`docker-compose.yml` 里的 command 会执行：

```sh
sed -e "s/@NODE@/$NODE/g" \
    -e "s/@INTERNAL_TOKEN@/$INTERNAL_TOKEN/g" \
    /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf
exec nginx -g 'daemon off;'
```

几个细节：

- **为什么用 sed 而不是 envsubst**：官方/精简 nginx 镜像通常不带 `gettext` 工具，sed 一定存在。
- **为什么 compose 里写成 `$$NODE`**：`$$` 是 Compose 的转义，表示“不要把 `$NODE` 在这里替换掉”，最终传给 shell 的仍是 `$NODE`，由容器内 shell 展开。
- **为什么 `exec`**：让 nginx 成为 PID 1，`docker stop` 时能收到信号并优雅退出；`daemon off;` 保证进程留在前台，容器不会启动即退出。
- 模板是**只读挂载**的（`./deploy/nginx/nginx.conf.template:/etc/nginx/nginx.conf.template:ro`），改完宿主机文件后**重启容器**即可生效，不需要重新构建镜像。
- 生成后的配置写在容器内的 `/etc/nginx/nginx.conf`，可以用 `nginx -T` 查看最终结果。

---

## 2. 它在整体架构中的位置

每个节点都是「Go 应用 + Nginx(nchan)」的一对组合：

```
浏览器
  │
  │  GET /            静态页面
  │  GET /api/...     REST 接口（Authorization: Bearer <token>）
  │  WS  /ws?token=   WebSocket 订阅
  ▼
┌──────────────────── node1-nginx (本文件) ────────────────────┐
│  /          → 直接返回挂载进来的 ./web 静态文件               │
│  /healthz   → 反代到 node1-app:8080/healthz                  │
│  /_auth     → 内部子请求，反代到 node1-app:8080/internal/auth │
│  /ws        → auth_request 通过后，由 nchan 接管长连接        │
│  /internal/pub/{channel} → 仅内网 + 密钥，nchan 发布消息      │
│  /api/      → 反代到 node1-app:8080                          │
└──────────────────────────────────────────────────────────────┘
                              ▲
                              │ POST /internal/pub/u:alice
                              │ （X-Publisher-Token）
                        node1-app 的 bridge
                              ▲
                              │ 订阅 Redis 频道 push:node:node1
                            Redis
```

核心设计：

- **WebSocket 连接由 nginx 的 nchan 模块直接持有**，Go 只参与建连前的鉴权和往 Redis 写消息。
- **消息下发不经过 Go**：Go 的 bridge 从 Redis 收到消息后，用 HTTP POST 到本机 nginx 的 `/internal/pub/{channel}`，nchan 再分发给该 channel 的所有订阅者。
- 因此 Go 重启不会断开已建立的 WebSocket；节点也可以横向扩展，任意节点写入都能经 Redis 广播到所有节点。

---

## 3. 配置逐段详解

### 3.1 全局段（main context）

```nginx
load_module modules/ngx_nchan_module.so;

worker_processes auto;
worker_rlimit_nofile 65536;
pid /run/nginx.pid;
error_log /var/log/nginx/error.log warn;
```

| 指令 | 说明 |
|---|---|
| `load_module` | 动态加载 nchan 模块（Debian 包 `libnginx-mod-nchan` 提供，见 [`deploy/nginx.Dockerfile`](../nginx.Dockerfile)）。没有这行，下面所有 `nchan_*` 指令都会报 unknown directive |
| `worker_processes auto` | worker 数量跟随 CPU 核数 |
| `worker_rlimit_nofile 65536` | 每个 worker 的文件描述符上限。**每条 WebSocket 占一个 fd**，必须和 compose 的 `ulimits.nofile` 一起调大，否则连接一多就报 `too many open files` |
| `error_log ... warn` | 只记录 warn 及以上级别。镜像里该文件已软链到 stderr，所以 `docker logs` 能看到 |

### 3.2 events

```nginx
events {
    worker_connections 10240;
    multi_accept on;
}
```

- `worker_connections 10240`：单个 worker 最多同时处理 10240 条连接。总容量 ≈ worker 数 × 10240。
- `multi_accept on`：一次事件循环尽量多地 accept 新连接，减少高并发建连时的延迟。

> 注意：`nofile` 限制是“每个 worker”的，`worker_connections` 不能超过它。当前 10240 < 65536，安全。

### 3.3 http 公共设置

```nginx
include /etc/nginx/mime.types;
default_type application/octet-stream;

log_format demo '$remote_addr [$time_local] "$request_method $uri" '
                '$status $body_bytes_sent rt=$request_time';
access_log /var/log/nginx/access.log demo;

sendfile on;
tcp_nopush on;
keepalive_timeout 65;
client_max_body_size 1m;

resolver 127.0.0.11 valid=10s ipv6=off;
nchan_shared_memory_size 64m;
```

| 指令 | 说明 |
|---|---|
| `log_format demo` | 自定义访问日志。关键点是用了 **`$uri` 而不是 `$request_uri`**：`$uri` 不含查询串，而 WebSocket 的 token 就放在 URL 查询串里（`/ws?token=...`）。这样 token 不会落到访问日志里 |
| `sendfile` / `tcp_nopush` | 静态文件走内核零拷贝；`tcp_nopush` 配合 sendfile 合并响应包头和文件内容，减少小包 |
| `keepalive_timeout 65` | 客户端空闲连接保持 65 秒 |
| `client_max_body_size 1m` | 请求体上限 1MB。对发布接口和 `/api/` 都生效，超限返回 413 |
| `resolver 127.0.0.11` | Docker 内置 DNS。容器重建后 IP 会变，`valid=10s` 表示最多缓存 10 秒就重新解析 |
| `nchan_shared_memory_size 64m` | nchan 的共享内存大小，所有 worker 共享。channel 的订阅者表、消息缓冲区都放在这里。**这是单机资源**，多个 nginx 节点之间不共享 |

> 关于 `resolver` 的准确理解：`upstream` 里的主机名是在 nginx 启动/重载时解析的；`resolver` 主要服务于运行时需要动态解析的场景（例如以后把 `proxy_pass` 改写成带变量的形式）。所以 **app 容器被重建、IP 变化后，最稳妥的做法是 `docker compose restart node1-nginx`**。

### 3.4 upstream

```nginx
upstream app {
    server @NODE@-app:8080;
    keepalive 32;
}
```

- 模板替换后变成 `server node1-app:8080;`，靠 Docker 网络的服务名解析到容器 IP。
- `keepalive 32`：在这个 upstream 上保持 32 条空闲长连接。**必须配合**下面的 `proxy_http_version 1.1` + `proxy_set_header Connection ""` 才真正生效，否则每个请求都会重新建立 TCP 连接。

### 3.5 server 与 location 总览

```nginx
server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;
    ...
}
```

- `listen 80`：容器内监听 80，宿主机通过 compose 的 `18080:80`（node1）/ `18081:80`（node2）访问。
- `server_name _`：通配，不校验域名。
- `root /usr/share/nginx/html`：对应 compose 里挂载的 `./web:/usr/share/nginx/html:ro`。

各 location 一览：

| location | 匹配方式 | 去向 | 用途 |
|---|---|---|---|
| `/` | 前缀 | 本地静态文件 | 前端页面（SPA 回落 `index.html`） |
| `= /healthz` | 精确 | Go `/healthz` | 存活探针 |
| `= /_auth` | 精确 + internal | Go `/internal/auth` | 鉴权子请求 |
| `= /ws` | 精确 | nchan | WebSocket 订阅 |
| `~ ^/internal/pub/(.+)$` | 正则 | nchan | 内部发布消息 |
| `/api/` | 前缀 | Go `/api/...` | REST 接口 |

**location 匹配优先级**（简化版）：

1. `=` 精确匹配最先命中；
2. 其次是最长前缀匹配（带 `^~` 则不再尝试正则）；
3. 然后按出现顺序尝试正则 `~` / `~*`，第一个命中即用；
4. 最后回落到最长前缀匹配。

所以 `= /ws`、`= /_auth` 不会被其他规则抢走；`/internal/pub/...` 也不会误入 `/api/`。

---

## 4. 各 location 详解

### 4.1 `location /` —— 静态页面

```nginx
location / {
    try_files $uri $uri/ /index.html;
}
```

按顺序尝试：

1. `$uri`：请求路径对应的真实文件（如 `/app.js`）；
2. `$uri/`：对应目录下的 `index.html`；
3. 都不存在则返回 `/index.html`。

第 3 步是单页应用（SPA）的标准做法：前端路由（如 `/room/1`）刷新时也能正常加载页面，由前端 JS 决定渲染什么。

### 4.2 `location = /healthz` —— 健康检查

```nginx
location = /healthz {
    proxy_pass http://app/healthz;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
}
```

- 这是一次**普通的 HTTP GET**，不是 WebSocket 长连接。Go 侧的实现只是：

  ```go
  func (s *Server) health(w http.ResponseWriter, r *http.Request) {
      writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node": s.cfg.NodeID})
  }
  ```

  请求进来立刻返回 JSON 并关闭连接，不查 Redis、不碰 nchan。
- 它只能证明“Go 进程活着、能响应 HTTP”。**不能**证明 Redis、nchan、WebSocket 推送链路正常。
- compose 里的容器健康检查其实是**直连 Go 的 8080**（`wget http://127.0.0.1:8080/healthz`），没有经过 nginx。这里的 `/healthz` 是留给外部/手动检查的入口，例如：

  ```sh
  curl -s http://localhost:18080/healthz
  # {"node":"node1","status":"ok"}
  ```

- `proxy_http_version 1.1` + `Connection ""`：清空 `Connection: close`，让请求能复用 upstream 的 keepalive 连接。

### 4.3 `location = /_auth` —— 鉴权子请求

```nginx
location = /_auth {
    internal;
    proxy_pass http://app/internal/auth;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_pass_request_body off;
    proxy_set_header Content-Length "";
    proxy_set_header X-Original-URI $request_uri;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
```

这是 `auth_request` 机制的被调方：nginx 在处理 `/ws` 时，**内部再发一个子请求**到这里，由 Go 决定“放行还是拒绝”。

请求流程：

```
浏览器 GET /ws?token=xxx
   │
   ├─ nginx 发起子请求 /_auth
   │     ├─ 转发到 Go GET /internal/auth
   │     ├─ 携带 X-Original-URI: /ws?token=xxx
   │     └─ Go 校验 token → 写 Redis 在线状态 → 204 + X-User-Id: alice
   │
   ├─ 子请求 2xx  → 放行，继续处理 /ws
   └─ 子请求非 2xx → 外层请求直接失败（401 / 403 / 503）
```

逐条说明：

| 配置 | 作用 |
|---|---|
| `internal;` | **只允许 nginx 内部调用**。外部访问 `/_auth` 会得到 404，防止绕过或滥用 |
| `proxy_pass http://app/internal/auth;` | 子请求转发给 Go 的内部鉴权接口 |
| `proxy_pass_request_body off;` + `Content-Length ""` | 不转发请求体。子请求必须没有 body，否则长度头和实际内容不一致会出错 |
| `X-Original-URI $request_uri` | 把**原始 URI（含查询串）**传给 Go。Go 就是从这里解析 `?token=...`（以及可选的 `?uid=...`）的 |
| `X-Real-IP` / `X-Forwarded-For` | 透传真实客户端 IP，便于日志和风控 |

Go 侧的契约（见 [`internal/httpapi/server.go`](../../internal/httpapi/server.go) 的 `internalAuth`）：

- 成功：返回 **204 No Content**，并带响应头 `X-User-Id: <uid>`；
- 失败：返回 401（token 无效/过期/uid 不匹配）或 503（Redis 写在线状态失败）；
- 成功时还会 `TouchPresence(uid, nodeID, PRESENCE_TTL)`，即**建连本身就是一次在线登记**，后续靠客户端心跳续期。

### 4.4 `location = /ws` —— WebSocket 订阅

```nginx
location = /ws {
    if ($http_upgrade != "websocket") {
        return 426;
    }

    auth_request /_auth;
    auth_request_set $auth_user $upstream_http_x_user_id;
    auth_request_set $auth_error $upstream_status;

    nchan_subscriber;
    nchan_channel_id "u:$auth_user";
    nchan_message_buffer_length 50;
    nchan_message_timeout 60s;
}
```

执行顺序：

1. **协议检查**：没有 `Upgrade: websocket` 头就返回 `426 Upgrade Required`。这样普通 GET 不会误占一个 nchan 订阅槽位。
2. **鉴权**：`auth_request /_auth` 触发上面那个子请求。
3. **取身份**：`auth_request_set $auth_user $upstream_http_x_user_id` 把子请求响应里的 `X-User-Id` 存进变量。`$auth_error` 存的是子请求状态码，目前**没有实际使用**，只是留作排查/后续日志扩展。
4. **交给 nchan**：`nchan_subscriber` 表示这条连接由 nchan 接管并完成 WebSocket 握手。

关键指令：

| 指令 | 说明 |
|---|---|
| `nchan_subscriber` | 当前 location 作为**订阅端**。nchan 自己处理 Upgrade 握手，**不需要** `proxy_set_header Upgrade`（因为根本没有反代到 Go） |
| `nchan_channel_id "u:$auth_user"` | 频道名固定为 `u:<用户ID>`，**一个用户一个频道**。Go 推送时用的也是 `"u:" + uid`（[`internal/push/service.go`](../../internal/push/service.go)），两边必须一致 |
| `nchan_message_buffer_length 50` | 每个频道最多缓存 50 条消息。晚到的订阅者能补收最近的若干条 |
| `nchan_message_timeout 60s` | 缓冲消息最多保留 60 秒，超时丢弃，防止离线用户堆积内存 |

从握手完成那一刻起，**连接和消息都不再经过 Go**。这也是 Go 重启不影响在线连接、以及能水平扩展的根本原因。

> 注意：频道 `u:<uid>` 只是“个人频道”。如果以后增加房间/广播频道，发布端往 `/internal/pub/room:xxx` POST 即可，nginx 侧不用改——`$1` 会原样作为频道名。

### 4.5 `location ~ ^/internal/pub/(.+)$` —— 内部发布

```nginx
location ~ ^/internal/pub/(.+)$ {
    allow 127.0.0.1;
    allow 10.0.0.0/8;
    allow 172.16.0.0/12;
    allow 192.168.0.0/16;
    deny all;

    if ($http_x_publisher_token != "@INTERNAL_TOKEN@") {
        return 403;
    }

    nchan_publisher;
    nchan_channel_id $1;
}
```

这是 Go 的 bridge 把 Redis 消息“灌进”本机 nchan 的入口：

```
Redis 频道 push:node:node1
   │
   ▼
node1-app bridge
   │  POST http://node1-nginx/internal/pub/u:alice
   │  X-Publisher-Token: <INTERNAL_TOKEN>
   ▼
nginx nchan_publisher → 频道 u:alice → 推给该用户的所有 WebSocket
```

逐条说明：

| 配置 | 作用 |
|---|---|
| `~ ^/internal/pub/(.+)$` | 区分大小写的正则；`(.+)` 捕获完整频道名，存入 `$1` |
| `allow` / `deny all` | **按顺序匹配，第一条命中即生效**，最后的 `deny all` 兜底。只允许回环地址和 Docker 私有网段 |
| `if ($http_x_publisher_token != "...")` | 共享密钥校验，对应 compose 里的 `INTERNAL_TOKEN`。**网络隔离 + 密钥，两层防护** |
| `nchan_publisher` | 当前 location 作为**发布端**，请求体就是消息内容 |
| `nchan_channel_id $1` | 频道名来自 URL，例如 `/internal/pub/u:alice` → 频道 `u:alice` |

两条重要的实践提醒：

1. **IP 白名单在 Docker 端口映射下并不严密**。宿主机/外部客户端访问 `18080` 时，容器看到的源地址通常是 Docker 网关（`172.x.x.1`），落在 `172.16.0.0/12` 里，会通过 IP 检查。真正兜底的是 **token 校验**，不要只依赖网段。
2. **密钥要三处一致**：compose 中 app 的 `INTERNAL_TOKEN`、nginx 的 `INTERNAL_TOKEN`、以及 Go 发起 POST 时带的 `X-Publisher-Token`。不一致的典型症状是 nchan 收不到消息、Go 日志出现 `nchan publish ... status=403`。

> `if` 在 nginx 里以“容易踩坑”著称，但这里只包含 `return`，属于官方认可的少数安全用法。

### 4.6 `location /api/` —— REST 接口

```nginx
location /api/ {
    proxy_pass http://app;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
```

- `proxy_pass http://app;` **没有写路径部分**，所以 nginx 会把原始 URI 原样转发：`/api/me` → `node1-app:8080/api/me`。如果写成 `proxy_pass http://app/;`，路径就会被重写，接口会 404。
- `proxy_http_version 1.1` + `Connection ""`：复用 upstream 的 keepalive 连接。默认的 HTTP/1.0 是短连接。
- `Host $host`：保留客户端访问的域名。
- `X-Real-IP` / `X-Forwarded-For`：把真实客户端 IP 传给 Go。
- 鉴权不在这里做：REST 接口用 `Authorization: Bearer <token>`，由 Go 的 `authed` 中间件校验；只有 `/ws` 需要 nginx 层鉴权，因为浏览器 WebSocket 无法自定义请求头。

---

## 5. 和 Go 应用的接口契约

| 方向 | 请求 | 关键头 / 参数 | 期望结果 |
|---|---|---|---|
| 浏览器 → nginx | `GET /` | — | 返回静态页面 |
| 浏览器 → nginx → Go | `GET /healthz` | — | 200 + `{"status":"ok","node":"node1"}` |
| 浏览器 → nginx → Go | `GET/POST /api/...` | `Authorization: Bearer <token>` | 业务响应；token 无效返回 401 |
| 浏览器 → nginx | `GET /ws?token=<token>`（Upgrade） | `Upgrade: websocket` | 101 切换协议，之后由 nchan 推送 |
| nginx → Go（子请求） | `GET /internal/auth` | `X-Original-URI`（含 token） | 204 + `X-User-Id`；否则 401/503 |
| Go → nginx | `POST /internal/pub/<channel>` | `X-Publisher-Token` | 2xx 表示已交给 nchan；403 表示密钥不对 |

**频道命名约定**：

- 用户个人频道：`u:<uid>`，由 [`internal/push/service.go`](../../internal/push/service.go) 生成，nginx 端拼成 `"u:$auth_user"`。
- 发布端的频道名完全由 URL 决定，Go 发什么 nchan 就建什么频道，因此新增频道类型不需要改 nginx 配置。

**在线状态**：不是由 nginx 维护的。`/_auth` 子请求成功时 Go 写一次 Redis presence，之后由：

- 客户端定时调用 `/api/heartbeat` 续期（`HEARTBEAT_INTERVAL=5s`）；
- Redis TTL 过期（`PRESENCE_TTL=20s`）；
- 后台 reaper 定期清理（`REAP_INTERVAL=10s`）

共同保证“连接关了以后，在线列表最终会变干净”。

---

## 6. 安全设计小结

| 风险 | 对应措施 |
|---|---|
| 伪造内部发布 | `/internal/pub` 限制私网来源 + `X-Publisher-Token` 共享密钥 |
| 绕过鉴权直连 Go | Go 监听 8080 只在容器网络内，不经 nginx 也能被其他容器访问，但浏览器侧无入口；gRPC 9090 才映射到宿主机，且带 `INTERNAL_TOKEN` 校验 |
| token 写进日志 | access log 用 `$uri`，不含查询串；但仍建议生产环境改用一次性 ticket |
| 外部探测 `/_auth` | `internal;` 使外部访问返回 404 |
| 非 WebSocket 请求占坑 | `/ws` 检查 `Upgrade` 头，不满足直接 426 |
| 消息体过大 | `client_max_body_size 1m` |
| 连接数耗尽 fd | `worker_rlimit_nofile` + compose `ulimits.nofile` 同时调到 65536 |

> 生产环境建议：把 WebSocket 的 token 改成“先用 HTTP 换一次性短时 ticket，再用 ticket 建连”，避免长期 token 出现在 URL 上；同时给 nginx 配置 TLS（`wss://`），否则 token 在网络上是明文。

---

## 7. 如何修改与调试

### 修改配置

1. 编辑 `deploy/nginx/nginx.conf.template`（改的是模板，不是生成后的文件）；
2. **重启容器**让 sed 重新生成配置：

   ```sh
   docker compose restart node1-nginx
   # 集群模式两个节点都会重启：
   docker compose --profile cluster restart node1-nginx node2-nginx
   ```

   注意：只执行 `nginx -s reload` 没用，因为磁盘上的 `/etc/nginx/nginx.conf` 还是旧的。

### 检查语法与最终配置

```sh
# 语法检查
docker compose exec node1-nginx nginx -t

# 查看替换后的完整配置（确认 @NODE@ / @INTERNAL_TOKEN@ 已替换）
docker compose exec node1-nginx nginx -T | head -40
```

### 常用排查命令

```sh
# 静态页面 / 健康检查
curl -i http://localhost:18080/
curl -i http://localhost:18080/healthz

# /_auth 是 internal，外部访问应当 404（这是预期行为）
curl -i http://localhost:18080/_auth

# 不带 Upgrade 头访问 /ws，应当返回 426
curl -i "http://localhost:18080/ws?token=whatever"

# 手动往某频道发布（注意：需要密钥；频道名与 Go 保持一致）
curl -i -X POST \
  -H "X-Publisher-Token: dev-internal-token" \
  -H "Content-Type: application/json" \
  -d '{"type":"ping"}' \
  http://localhost:18080/internal/pub/u:alice

# 看访问日志（只有 path，没有 query）
docker compose logs -f node1-nginx
```

真正的端到端验证（建连 → 发消息 → 收到消息）请用项目脚本：

```sh
make e2e           # 单节点
make e2e-cluster   # 双节点，跨节点推送
```

---

## 8. 常见问题

| 现象 | 可能原因 | 排查方向 |
|---|---|---|
| `unknown directive "nchan_subscriber"` | 没加载 nchan 模块，或镜像里没装 `libnginx-mod-nchan` | 检查 `load_module` 是否在文件顶部；执行 `make up` 重新构建 `ws-nchan-nginx:local` |
| `host not found in upstream "node1-app"` | 启动时 app 容器还没进 DNS / 服务名写错 | 确认 compose 中服务名；先启动 redis/app 再启动 nginx |
| `/ws` 一直 426 | 客户端没发 `Upgrade: websocket`（比如用普通 curl/HTTP 客户端） | 用浏览器、`wscat` 或项目 e2e 脚本 |
| `/ws` 返回 401 | token 过期（默认 1h）、签名不对，或 `?uid=` 与 token 内容不一致 | 重新调用 `/api/dev/token` 取新 token |
| `/ws` 返回 503 | Go 写 Redis 在线状态失败 | 看 app 日志与 Redis 健康状态 |
| 发布返回 403 | nginx 与 app 的 `INTERNAL_TOKEN` 不一致 | 两处都改，然后重启 app 和 nginx |
| Go 日志 `nchan publish ... status=403` | 同上，bridge 的密钥被 nginx 拒绝 | 检查 compose 中两处 `INTERNAL_TOKEN` 是否一致 |
| 消息发出但浏览器收不到 | 频道名不一致；或订阅者在另一个 nginx 节点且消息未跨节点广播 | 检查是否都用 `u:<uid>`；确认 bridge 订阅的是 `push:node:<nodeID>`；查 Redis Pub/Sub |
| 晚订阅收不到历史消息 | 消息超过 `nchan_message_timeout 60s`，或超过 `buffer_length 50` 条 | 属预期行为；需要持久历史请落库 |
| 连接数上不去 / `too many open files` | fd 上限没调大 | 同时检查 compose `ulimits.nofile` 与 `worker_rlimit_nofile` |
| 改了配置没生效 | 改的是模板但只 reload 了 nginx | 必须重启容器让 sed 重新生成 `/etc/nginx/nginx.conf` |

---

## 9. 已知限制与扩展方向

1. **nchan 共享内存是单机资源**：`64m` 的 channel 缓冲区只在单个 nginx 进程内共享，跨节点不共享。这也是为什么本项目用“Redis Pub/Sub + 各节点本地发布”，而不是直接让 nchan 跨机通信。
2. **消息不持久化**：Redis 关闭了 RDB/AOF，nchan 缓冲在内存且 60 秒过期。适合演示和实时推送，不适合需要历史回溯的场景。
3. **没有 TLS**：容器内只 `listen 80`。生产需要加 443/证书，并把 `/ws` 升级为 `wss://`。
4. **token 在 URL 上**：浏览器 WebSocket 无法自定义 Header，这是浏览器的限制。可行改进是“先 HTTP 换短时 ticket，再建连”。
5. **单 worker 事件模型**：nchan 在 C 层处理订阅与广播，性能很好，但所有状态都在本机内存；要跨机共享 channel 状态可启用 nchan 的 Redis 引擎，或继续沿用当前的 bridge 模式。
6. **`$auth_error` 未使用**：目前只是保存子请求状态码。若需要更细的观测，可以把它加入自定义日志格式，或在 nchan 回调中区分 401/503。
