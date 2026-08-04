# Flowie HAProxy MQTT edge

该组件在公网 `8883` 终止 MQTTS，从首个 MQTT 3.1/3.1.1/5.0 CONNECT 提取非空 Client ID，按 Client ID
执行 consistent hash，并用 PROXY protocol v1 把客户端源/目标地址传给 Flowie plaintext TCP listener。
HAProxy 只处理 MQTT Client ID 路由，不读取、不认证、不改写 User Name 或 Password；User Name 由 Flowie
作为 `principal_id` 认证，Password 由 Flowie 作为 MQTT credential 校验。

`FLOWIE_MQTT_BACKENDS` 是逗号分隔的 `host:port` 列表，最多 32 个节点，例如：

```text
FLOWIE_MQTT_BACKENDS=10.20.0.11:18883,10.20.0.12:18883,10.20.0.13:18883
```

每个 backend 必须是 cluster connection edge 的私网 listener，并配置仅信任 HAProxy transport CIDR 的
`trusted_proxy_cidrs`。HAProxy 不传递 Client ID 自报字段；Flowie 仍从原始 CONNECT 独立解析规范 Client ID，
并以 PostgreSQL ownership/fencing 为事实源。hash 命中只减少跨节点转发。

CONNECT 无效、Client ID 为空、检查缓冲区耗尽或 `FLOWIE_MQTT_INSPECT_DELAY_MS` 超时都会在连接进入
Flowie 前拒绝。`FLOWIE_MQTT_INSPECTION_BUFFER_BYTES` 默认 32768，必须按公网允许的 CONNECT 上限和
连接容量评估，不能无界增大。

证书从共享 Let's Encrypt 目录的 `fullchain.pem` 和 `privkey.pem` 分别加载。容器每
`FLOWIE_HAPROXY_CERT_POLL_SECONDS` 秒检查证书内容；更新后先执行 `haproxy -c`，再向 master-worker
发送 `SIGUSR2` 平滑 reload。backend 列表变化需要重启容器并重新执行配置校验。
