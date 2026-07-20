# Flowie release gate

本 gate 是 Flowie MQTT protocol/client、managed endpoint、产品 host 与持久化组合的最低可复验条件，
不扩大 [ARCHITECTURE.md](ARCHITECTURE.md) 声明的产品边界。

## 运行

本机必须已有 `127.0.0.1:6379` Redis，并允许测试创建、扫描及删除随机 namespace 下的记录。
public MQTT suite 会访问其测试文件中声明的 HiveMQ 与 EMQX 公共端点；发布环境必须允许对应的
TCP、TLS、WS 与 WSS 出站连接。

```powershell
cmake --preset win-release-user -DTURBO_FLOW_REDIS_LIVE_TESTS=ON -DFLOWIE_MQTT_PUBLIC_LIVE_TESTS=ON
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user -N -L flowie-release
ctest --preset win-release-user -L flowie-release --output-on-failure
ctest --preset win-release-user --output-on-failure
```

`-N -L flowie-release` 必须列出 `test_flowie_mqtt_client_live`、
`test_flowie_transport`、`flowie_server_check_redis_session_store` 和
`flowie_server_check_https_auth_provider`，且这些测试都不得显示
`Disabled`。Gate 与全量回归必须零失败；不能用未启用 live suite 的结果替代。

## 固定门槛

- MQTT protocol/client：MQTT 3.1/3.1.1/5 编解码、QoS 0/1/2、订阅、取消订阅、PING、断线与有界
  command queue 必须通过；public live suite 必须覆盖 TCP/TLS/WS/WSS。
- Server transport：MQTT 3.1、MQTT 3.1.1 与 MQTT 5 必须分别在 TCP/TLS/WS/WSS/Pipe 完成真实 CONNECT 与
  PING 往返；TCP/TLS/WS/WSS 还必须完成公开 client 的 DISCONNECT。TLS/WSS 使用验证 CA 和证书，
  不能用配置解析代替握手。Managed endpoint 还必须验证 MQTT 3.1/3.1.1/5 fan-out 重编码。
- Session/authorization：CONNECT、QoS1/QoS2、重复包、session takeover、retained message、shared
  subscription、Will/Will delay、session expiry、Topic Alias、Subscription Identifier、Assigned Client
  Identifier、Receive Maximum send window、Keep Alive、Enhanced AUTH/re-auth、default-deny ACL 与
  未授权 publish/subscribe 均须通过。
- Backpressure/resource：per-connection 与 aggregate send HWM、慢订阅者隔离、最大连接数、session、
  subscription、inflight 和 retained 容量都必须有确定的拒绝与终态，不允许 silent drop。
- Persistence：SQLite 与真实 Redis 的 `flowie_server --check` 必须创建并扫描选定 record store；
  endpoint 重建、binary retained key、pending Will 和 incompatible record 的测试必须通过。请求持久化
  时不得回退到进程内状态。
- Authentication provider：`profiles.<name>.auth_provider` 必须精确选择唯一注册的 `https` backend；
  `http://`、userinfo、query、fragment、数据库字段、literal token、未知字段和无效 secret reference
  必须在 listener 启动前 fail fast。运行时 TLS、超时、状态码、Content-Type、协议版本和 principal
  校验失败必须拒绝认证，不得 fallback。
- Product host：provider preflight、独立 YAML/Graph 解析、Queue/RuleSet/socket 装配、supervisor
  lifecycle、输出上限及 `--check` 必须通过。

## 明确边界

本 gate 证明库级 security binding/本地 ACL 行为及 bundled `flowie_server` 的配置驱动 provider 装配。
Flowie 只访问 HTTPS 认证服务，不接受 Redis/SQLite/PostgreSQL 或其他身份库连接配置；数据库网络 ACL
必须只允许认证服务访问。认证服务 token 只能通过 key-provider reference 注入。TLS 始终校验服务主机名；
私有 CA 与 mTLS 客户端证书可按 provider 注入，私钥密码只能通过 key-provider reference 获取。生产网络还必须限制 Flowie 仅能出站到认证服务地址，
认证数据库不得暴露到 Flowie 网段或公网。内置 HTTPS provider 当前把 MQTT 5 Authentication Data
作为一次性 HTTPS credential 完成认证；需要多轮 challenge 的部署必须注入实现 enhanced provider ABI
的认证模块。真实 mTLS gate 必须证明服务端以 `SSL_VERIFY_FAIL_IF_NO_PEER_CERT` 要求证书并得到
`X509_V_OK`；仅成功解析配置不算传输证据。多轮认证边界关闭或被产品契约明确排除前，不应把 bundled
server 宣称为通用、完整 MQTT broker。

PostgreSQL live test 不属于 Flowie gate；全量回归允许它在未提供测试数据库时保持 Disabled，但发布
记录必须明确这一点。
