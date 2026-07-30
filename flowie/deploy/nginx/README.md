# Flowie 公网 TLS 代理

该部署目录是 `flowie_server` 的正式配套组件。单个长期运行的 `flowie-nginx` 容器负责：

- `443`：终止公网 Let's Encrypt HTTPS，并以已验证的内部 TLS 转发到 Control/Dashboard。
- `8883`：终止公网 Let's Encrypt MQTT TLS，并以已验证的内部 TLS 转发到 MQTT listener。
- `80`：只提供 ACME HTTP-01 challenge 和 HTTPS redirect。
- 定期运行 Certbot renew；仅在证书实际更新后 reload Nginx。

Nginx/Certbot 不持有用户、ACL、MQTT session 或其他业务事实。它们属于部署边界，不进入
`flowie_server` C 进程的领域状态或生命周期。

## 附加 HTTP vhost

`FLOWIE_HTTP_CONF_DIR` 可指定宿主机上的附加 HTTP vhost 目录；Compose 将它只读挂载到
`/etc/flowie-nginx/http.d`。该入口只扩展 Nginx `http` context，不进入 `stream` context，也不能
改写 Flowie 自动生成的 Control/MQTT 配置。

`FLOWIE_HTTP_ASSET_DIR` 可把附加 vhost 的静态文件目录只读挂载到 `/srv/http`。Flowie 不生成、
修改或解释其中内容；多个应用共享该目录时必须使用独立子目录，避免互相覆盖。

附加配置负责自己的域名、证书、upstream 和安全策略。配置生效前必须执行 `nginx -t`；宿主机
upstream 只能使用明确监听地址，不得把 Flowie 的 service token、管理密码或 MQTT credential 写入
Nginx 配置。

## 内部 listener

- Embedded Control：`127.0.0.1:8443`，TLS identity `flowie-control.internal`。
- MQTT：`127.0.0.1:18883`，TLS identity `flowie-mqtt.internal`。
- 两张内部证书必须由 `FLOWIE_INTERNAL_CA_FILE` 指定的 CA 签发。
- 公网 HTTPS/MQTT listener 都不请求客户端证书。Dashboard 使用登录 session；MQTT 使用配置的
  Auth/ACL provider。mTLS 只能保留为显式的 service-to-service 第二因子，不得加在公网 listener。

`.env.example` 中的 `FLOWIE_INTERNAL_CA_FILE` 是宿主机路径，Compose 会把它只读挂载到容器内固定路径。
`.dockerignore` 和 `.gitignore` 会排除 `.env`、私钥及 `state/`；不得把证书私钥放进镜像层或 Git。

## 首次签发与启动

复制 `.env.example` 为 `.env`，设置实际域名、邮件地址、内部 CA 文件和上游 TLS identity。确保 DNS
已指向该主机，并且签发期间 port 80 未被其他进程占用：

```sh
set -a
. ./.env
set +a
./bootstrap-cert.sh
docker compose -f compose.yml up -d --build
```

`bootstrap-cert.sh` 先构建并复用同一个 `flowie-nginx:local` 镜像，以一次性容器执行 Certbot；不会引入
第二个长期 Certbot 容器。长期容器使用 webroot 完成续期。`/` 重定向到
`/v1/management/dashboard`，其他 HTTPS path 原样转发给 embedded Control。

## 源地址边界

Flowie 当前不解析 PROXY protocol。MQTT Broker 因此只观察到 Nginx 的上游地址，不能把
`remote_address` 用作该拓扑中的授权事实。用户名、credential、Client ID、Root Group 和本地 ACL
才是授权事实源。在 Flowie 增加显式 trusted-proxy 配置和防欺骗测试前，禁止在 Nginx 中启用
`proxy_protocol`；直接启用会破坏 MQTT framing。
