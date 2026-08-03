# Flowie 公网 edge Compose

该目录保留既有安装路径，但 Compose 现在明确拆分两个长期服务：

- `flowie-nginx`：负责 `80/443`、ACME HTTP-01、HTTPS TLS termination，并通过已验证的内部 TLS
  转发 Control/Dashboard。
- `flowie-haproxy`：负责 `8883` MQTTS TLS termination、MQTT CONNECT 校验、Client ID consistent
  hash，以及到 Flowie cluster edge 的 PROXY protocol v1 转发。

两个代理都不持有用户、ACL、MQTT session、shard ownership 或数据库凭据。Nginx 继续执行 Certbot
renew；HAProxy 只读挂载同一证书目录，并在证书内容变化后先校验配置再平滑 reload。

## 附加 HTTP vhost

`FLOWIE_HTTP_CONF_DIR` 可指定宿主机上的附加 HTTP vhost 目录；Compose 将它只读挂载到
`/etc/flowie-nginx/http.d`。该入口只扩展 Nginx `http` context，不能改写 HAProxy MQTT 配置。

`FLOWIE_HTTP_ASSET_DIR` 可把附加 vhost 的静态文件目录只读挂载到 `/srv/http`。附加配置负责自己的
域名、证书、upstream 和安全策略。配置生效前必须执行 `nginx -t`；不得把 Flowie service token、
管理密码或 MQTT credential 写入代理配置。

## 内部 listener

- Embedded Control：`127.0.0.1:8443`，TLS identity `flowie-control.internal`。
- 单机 HAProxy：Flowie MQTT 使用 `127.0.0.1:18883` plaintext TCP。
- 多节点 HAProxy：每个 MQTT listener 绑定私网地址，并只允许 HAProxy 节点访问。

公网 HTTPS/MQTTS listener 都不请求客户端证书。Dashboard 使用登录 session；MQTT 使用 Flowie 配置的
Auth/ACL provider。HAProxy 解密 MQTTS 后，Flowie 到 Auth/ACL 的认证语义不变。

`.dockerignore` 和 `.gitignore` 会排除 `.env`、私钥及 `state/`；不得把证书私钥放进镜像层或 Git。

## 首次签发与启动

复制 `.env.example` 为 `.env`，设置实际域名、邮件地址、内部 Control CA 以及 MQTT cluster backend
列表。确保 DNS 已指向该主机，并且签发期间 port 80 未被其他进程占用：

```sh
set -a
. ./.env
set +a
./bootstrap-cert.sh
docker compose -f compose.yml up -d --build
```

`bootstrap-cert.sh` 使用一次性容器签发证书。长期 Nginx 容器以 webroot 续期；HAProxy 默认每 300 秒
检测同一证书内容，更新成功后通过 master-worker `SIGUSR2` 平滑 reload。

## Client ID 路由

`FLOWIE_MQTT_BACKENDS` 是逗号分隔、最多 32 个节点的 `host:port` 列表：

```text
FLOWIE_MQTT_BACKENDS=10.20.0.11:18883,10.20.0.12:18883,10.20.0.13:18883
```

HAProxy 只接受首包为有效 MQTT CONNECT 且 Client ID 非空的连接。完整 CONNECT 必须在
`FLOWIE_MQTT_INSPECT_DELAY_MS` 内装入 `FLOWIE_MQTT_INSPECTION_BUFFER_BYTES`；否则 fail closed，不回退
到 source hash 或 round-robin。HAProxy 对 Client ID 使用 consistent hash，但 Flowie 会从同一原始 CONNECT
独立解析 Client ID，并通过 PostgreSQL ownership/fencing 决定 shard owner；路由命中只是一项性能提示。

## 源地址与信任边界

HAProxy backend `send-proxy` 固定发送文本 PROXY v1。对应 Flowie endpoint 使用 plaintext TCP，并显式
要求可信代理 header：

```yaml
adapters:
  mqtt.endpoint:
    kind: flowie_endpoint
    config:
      transport: tcp
      host: 127.0.0.1
      port: 18883
      trusted_proxy_cidrs: '127.0.0.1/32, ::1/128'
      proxy_header_max_bytes: 256
      proxy_header_timeout_ms: 1000
```

该 listener 不能直接暴露公网，也不能与不发送 PROXY header 的客户端混用。若 HAProxy 和 Flowie 不在
同一主机，`host` 必须是受防火墙保护的私网地址，`trusted_proxy_cidrs` 必须收窄到实际 HAProxy 地址，
不得信任整个应用网段。缺失、畸形、超限、超时或来自未信任 peer 的 header 都在 MQTT 认证前拒绝。

PROXY v1 只传递源/目标地址和端口，不携带 Client ID 或 TLS TLV。`remote_address` 可用于审计、限流和
策略上下文；`transport_peer_address` 保留直接 HAProxy peer。两者都不是 MQTT ownership/fencing 事实。
