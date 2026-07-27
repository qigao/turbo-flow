# Flowie Control 部署与配置指南

`flowie-control` 是独立于 MQTT 数据面的管理进程。它组合本地授权事实源、JSON-RPC、HTMX
Dashboard，以及可选的 `/v3/authenticate` 与 `/v3/acl` 服务。Auth 可选择本地 Repository verifier，
或只通过 HTTPS 调用一个第三方认证系统；Flowie Broker 不直连身份/ACL 数据库或目录。
控制进程只监听一个显式 HTTPS/mTLS 地址。当前仍缺少首位管理员
bootstrap、限流、HA/migration 和完整安全发布 gate，因此不能据此宣称整个控制面已达到生产就绪。

## 构建与预检

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target flowie-control

$env:FLOWIE_CONTROL_KEY_PASSWORD = "private-key-password"
build\Msvc-Release\bin\flowie-control.exe --check `
  --config flowie\examples\flowie-control.yml
```

真实 listener/mTLS/Auth/ACL gate 使用临时证书、SQLite 和子进程，覆盖本地 credential 成功/拒绝、
完整 ACL bundle、版本不存在、service token、统计 RPC 和 mTLS 身份边界；测试不修改源码树中的部署文件：

```powershell
ctest --preset win-release-user -R test_flowie_control_https_integration --output-on-failure
```

PostgreSQL focused gate 使用相同的 provider-neutral contract 验证 Repository 事务、生产本地 Auth
service 与 ACL generation 的组合，以及 JSON-RPC 下层 management service 的账户/Group/Role/ACL/audit
操作和权限边界；它只有在显式打开 live tests 并提供专用测试连接时才运行。远程打包、旧测试容器复用、
数据清理和证据下载流程见
[`LINUX_REMOTE_TEST_RUNBOOK.md`](LINUX_REMOTE_TEST_RUNBOOK.md)。该测试连接可以使用隔离环境，
产品 runtime 仍固定要求 `sslmode=verify-full` 和实际 TLS session。

也可使用环境变量，优先级固定为 CLI、进程环境、显式 DotEnv：

```powershell
$env:FLOWIE_CONTROL_CONFIG = "C:\flowie\flowie-control.yml"
build\Msvc-Release\bin\flowie-control.exe --check
```

仅在本地开发时使用 `--env-file/-E`。程序不会隐式读取当前目录 `.env`。`--check` 解析完整 schema，
并用 CoroNet/OpenSSL 实际加载 server chain、private key、client CA，以及启用的第三方 HTTPS client
CA/certificate/private key；它不打开 listener、不连接第三方服务或控制数据库，也不执行 schema
migration。选择 PostgreSQL 时，它还校验非秘密 conninfo 与 password secret reference。证书、私钥、
CA、conninfo 或 secret reference 无法加载时立即失败，不回退到 HTTP、SQLite 或本地 credential。

## Auth 来源选择

本文中的“HTTP 认证”一律表示使用 HTTPS 的版本化 JSON 契约。企业/第三方集成只允许以下链路：

```text
MQTT Broker -> HTTPS /v3/authenticate -> flowie-control
                                         |
                                         +-> HTTPS third-party assertion service
                                         +-> local authorization Repository
```

Broker v3 请求中的 `remote_address` 只来自 CoroNet 直接 socket peer；TCP/TLS/WS/WSS 使用数值
`IP:port`，Pipe 使用 `local`。当前不读取 PROXY protocol、`X-Forwarded-For` 或其他代理 header，
因此经代理接入时记录的是代理地址。`peer_certificate_sha256` 仅在 MQTT TLS/WSS endpoint 配置
`tls_client_ca_file`、CoroNet 已验证客户端证书后出现；否则是空字符串。该 MQTT 客户证书与
Broker 调用 `flowie-control` 时用于 Root Group binding 的 mTLS 服务证书是两条独立信任链。

第三方 HTTPS assertion contract 为严格 version 2，并接收相同的直接 peer address 与可选 MQTT 客户
证书指纹。第三方系统可据此组合自身账户认证，但返回的 external groups/claims 仍必须经过本地
principal 映射；ACL 始终只来自 control Repository。

各数据边界固定如下：

| 数据 | 唯一事实源 | Flowie 的行为 |
| --- | --- | --- |
| 第三方 credential、token、目录账户状态 | 第三方 HTTPS 服务 | 发送一次有界请求，只消费 allowlist assertion |
| 本地 user enabled、Root Group、Role/Group、ACL | control Repository | 在第三方认证成功后执行本地授权映射 |
| MQTT session、retained、inflight | FlowStore/session store | 不参与身份认证 |
| 第三方 Redis/PostgreSQL/LDAP/AD/OIDC/RADIUS | 第三方服务内部实现 | Broker 与 control 领域核心不加载其 SDK |
| control SQLite/PostgreSQL | control Repository | 只由 `flowie-control` 访问；Broker 不接收连接配置 |

同一请求不会同时查询两个认证来源。第三方拒绝、超时、TLS/协议错误或服务不可用时直接 fail closed，
不会验证本地密码、切换 FlowStore/database backend 或匿名放行。若未来需要第三方 profile/目录查询，
也必须增加独立 HTTPS 契约，不能新增进程内 directory/database adapter。

## 配置 schema

根节点只接受 `version`、`listener`、`storage`、`management`、`dashboard` 和 `auth`；未知字段、错误类型、
重复证书指纹与越界值都会使启动失败。完整示例见 [flowie-control.yml](examples/flowie-control.yml)。

| 路径 | 约束 |
| --- | --- |
| `version` | 当前只接受 `1` |
| `listener.host` | 默认 `127.0.0.1`；生产应显式配置 loopback 或管理网地址 |
| `listener.port` | `1..65535`，默认 `8443` |
| `listener.tls.cert_file` | 必填，PEM server certificate chain |
| `listener.tls.key_file` | 必填，PEM private key |
| `listener.tls.key_password_ref` | 可选，只接受 `env://UPPER_CASE_NAME`，不接受 literal |
| `listener.tls.client_ca_file` | 必填，验证管理客户端证书的 CA bundle |
| `listener.limits.*` | Iris header、URL、JSON、body 与 header count 的有界配额 |
| `storage.control_store` | `sqlite` 或 `postgresql`；缺失时为兼容旧配置选择 SQLite |
| `storage.sqlite.*` | SQLite path 与 busy timeout；选择 SQLite 时必填，不能出现 PostgreSQL block |
| `storage.postgresql.conninfo` | 非秘密 libpq conninfo；必须显式 `sslmode=verify-full`，拒绝 password/passfile/sslpassword/service/servicefile |
| `storage.postgresql.password_ref` | 必填且只接受 `env://...`；密码作为独立 libpq 参数传入并在 provider 销毁时清零 |
| `storage.postgresql.schema_name` | 默认 `flowie_control`；只接受安全 PostgreSQL identifier |
| `storage.postgresql.*_timeout*` | connect `1..60s`；statement/lock/acquire `1..60000ms` |
| `storage.postgresql.pool_capacity` | `1..64` 个独占连接，默认 `4` |
| `storage.postgresql.schema_mode` | `validate`（默认，无 DDL）或显式 `migrate` |
| `management.rpc_path` | 静态绝对 path，默认 `/v1/management/rpc` |
| `management.certificate_bindings` | mTLS SHA-256 指纹到 `(root_group, principal)` 的唯一绑定 |
| `dashboard.enabled` | 是否注册固定 HTMX Dashboard 路由 |
| `auth.enabled` | 是否注册 `/v3/authenticate` 与 `/v3/acl`；默认关闭 |
| `auth.local_executor.workers` | 本地 Auth 同步 verifier worker 数，`1..64`，默认 `4` |
| `auth.local_executor.queue_capacity` | 本地 Auth 等待队列容量，`1..4096`，默认 `128`；满载返回 429 |
| `auth.local_executor.deadline_ms` | 本地 Auth HTTP 等待上限，`1..60000`，默认 `10000`；到期返回 503 |
| `auth.external_https` | 可选；缺失时使用本地 Auth，出现时由第三方 HTTPS Auth 替换本地 verifier |
| `auth.external_https.url` | 必须为带明确 path 的 HTTPS URL；userinfo、query、fragment 均拒绝 |
| `auth.external_https.service_token_ref` | 访问第三方服务的 bearer token，只接受独立 `env://...` reference |
| `auth.external_https.trusted_issuer` | 第三方断言必须精确匹配的 issuer |
| `auth.external_https.subject_type` | 第三方断言必须精确匹配的 subject type |
| `auth.external_https.timeout_ms` | `1..30000`，默认 `3000` |
| `auth.external_https.max_response_size` | `1024..65536`，默认 `16384` |
| `auth.external_https.max_in_flight` | `1..1024`，默认 `64`；满载时在读取 token 和发起网络请求前返回 busy |
| `auth.external_https.tls.*` | 可选私有 CA；client certificate/key 必须成对出现，密码只接受 `env://...` |

当 `auth.enabled: true` 时还必须设置 `listener_id`、`method`、`service_token_ref` 和
`root_bindings`。service token 同样只接受 `env://...`；auth cache 容量最大 4096，TTL 最大 60 秒，
同一组容量/TTL 限制同时约束 positive credential cache 与 principal snapshot cache。principal cache
命中仍会复核 user/credential revision、全局 store revision 与 policy version，任一事实源不可用时
fail closed。
认证服务还会在 Argon2id/KDF 前执行双层 token bucket：默认每个已验证 mTLS caller 为
`100 requests/s`、burst `200`，每个 `(caller, root_group, principal)` 为 `5 requests/s`、burst `10`。
连续失败消耗 identity bucket；成功凭据只清除该 identity 的失败 bucket，不能重置 caller 总量。
桶有界且只保留 keyed digest，不保存 identity、证书指纹或 secret 明文。
本地 Auth 的 Argon2id、SQLite 与同步 PostgreSQL 调用只进入专用 executor。Iris request/response/socket
不跨线程；deadline 只结束 HTTP 等待，不会强行取消正在执行的同步 KDF/SQL。迟到结果被丢弃，任务自行
擦除 secret；endpoint shutdown 停止接单并 drain。显式 `local_executor` 与 `external_https` 互斥，
外部 HTTPS Auth 始终留在 CoroNet coroutine I/O 路径。
YAML 不保存 ACL rule body、用户 credential、service token 或私钥内容。

控制事实源二选一示例：

```yaml
storage:
  control_store: postgresql
  postgresql:
    conninfo: host=control-db.internal dbname=flowie user=flowie_control sslmode=verify-full sslrootcert=certs/control-db-ca.pem
    password_ref: env://FLOWIE_CONTROL_PG_PASSWORD
    schema_name: flowie_control
    connect_timeout_seconds: 5
    statement_timeout_ms: 5000
    lock_timeout_ms: 5000
    pool_capacity: 4
    acquire_timeout_ms: 5000
    schema_mode: validate
```

不要同时保留 `sqlite` 与 `postgresql` block。生产实例通常以 `schema_mode: validate` 启动；迁移任务
必须显式改为 `migrate`，成功后再恢复 `validate`。当前尚未完成 HA fencing、PITR 与回滚门禁，
因此多实例迁移/写入策略仍不是已发布保证。

未配置 `external_https` 时，`/v3/authenticate` 使用 Repository 中的本地 credential verifier；配置
`external_https` 时，它严格替换本地 verifier，失败不会回退本地密码。不在 `flowie-control` 进程内
加载 OIDC、LDAP/AD、RADIUS 或第三方数据库 SDK；它们全部由第三方 HTTPS 服务内部处理。

本地 Auth 的最小配置如下：

```yaml
auth:
  enabled: true
  listener_id: flowie-control-auth
  method: bearer
  service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
  local_executor:
    workers: 4
    queue_capacity: 128
    deadline_ms: 10000
  root_bindings:
    - peer_certificate_sha256: sha256:<64-lowercase-hex>
      root_group: root-a
```

第三方 Auth 在同一块增加：

```yaml
auth:
  enabled: true
  listener_id: flowie-control-auth
  method: bearer
  service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
  root_bindings:
    - peer_certificate_sha256: sha256:<64-lowercase-hex>
      root_group: root-a
  external_https:
    url: https://third-party-auth.internal/v2/assert
    service_token_ref: env://FLOWIE_THIRD_PARTY_AUTH_TOKEN
    trusted_issuer: https://identity.internal
    subject_type: device
    timeout_ms: 3000
    max_response_size: 16384
    max_in_flight: 64
    tls:
      ca_file: certs/third-party-auth-ca.pem
      client_cert_file: certs/flowie-auth-client-chain.pem
      client_key_file: certs/flowie-auth-client-key.pem
      client_key_password_ref: env://FLOWIE_THIRD_PARTY_AUTH_KEY_PASSWORD
```

外层 `auth.service_token_ref` 保护 Broker 到 `/v3/authenticate` 和 `/v3/acl` 的请求；内层
`auth.external_https.service_token_ref` 保护 `flowie-control` 到第三方断言服务的请求。两个 token
具有不同的信任方向、权限和轮换周期，不能复用。

`GET /v3/acl` 只接受受信 mTLS caller 与外层 bearer token。Root Group 由
`(listener_id, peer certificate SHA-256)` 绑定解析，客户端不能通过 path、query 或 header 指定。
可选 `X-TurboFlow-Policy-Version` 请求精确版本；响应始终是完整 v3 bundle，而不是逐条规则查询。

第三方成功断言的 `issuer` 和 `subject_type` 必须精确匹配配置，稳定 `subject` 被解释为当前 Root Group
中的本地 `principal_id`。Repository 随后重新检查该 principal 存在且 enabled，并只从本地事实源加载
Role/Group；第三方 groups 只是有界映射输入，不会自动获得本地权限。第三方拒绝、超时、TLS/协议错误、
主体不存在或本地用户禁用都 fail closed，且不回退到本地密码。并发达到 `max_in_flight` 时返回 busy，
不会先读取 service token，也不会建立额外连接。

service token 每次请求都从 secret provider 重新获取，可在不重启 Flowie 的情况下轮换。CA、client
certificate 与 private key 在 provider 创建时加载，当前不支持原地热重载；证书文件替换后应通过受控
进程重启重建 provider，健康检查通过后再切流，失败则回滚到仍持有旧证书的实例。

HTTPS adapter 内部提供不含 identity、credential、token、URL 或响应内容的统计快照，字段包括
`started_requests`、`in_flight`、`succeeded`、`denied`、`local_overload`、`remote_overload`、
`remote_server_failures`、`transport_failures`、`protocol_failures` 和 `local_failures`。应分别对本地
舱壁、远端 429、远端 5xx、传输失败和协议失败计算窗口 rate；不要把认证拒绝率直接当作服务可用性故障。
快照为逐字段无锁采样，并发读取时不保证跨字段事务一致。不得改为逐请求记录 token、identity 或请求/
响应正文。

具有 `security_admin` 角色且通过管理 listener mTLS 身份解析的调用方，可从现有
`management.rpc_path` 查询全局聚合快照：

```json
{"jsonrpc":"2.0","method":"flowie.auth.external_https.stats","params":{},"id":1}
```

启用第三方 HTTPS 认证时，结果包含 `enabled: true` 和上述十个计数；未启用时只返回
`{"enabled":false}`。该方法不接受其他参数。计数跨 Root Group 聚合，因此 root-scoped `viewer`、
`user_admin` 和 `policy_admin` 均无权读取；未认证返回 `-32001`，无权限返回 `-32003`，未知参数返回
JSON-RPC `-32602`。响应继承管理 RPC 的 `Cache-Control: no-store`、禁用 batch/notification 和请求配额。

该 RPC 已提供安全的诊断采集入口，但不会计算窗口 rate、SLO 或告警。生产发布前仍需配置外部 collector、
阈值基线和 runbook；不应另外开放匿名 metrics listener。

`principal_ttl_seconds` 同时定义已连接 Broker session 的撤销传播上界。principal 到期时，即使连接完全
空闲且没有 `recv_timeout_ms`/keepalive，Broker 也会主动 fail closed：MQTT 5 先发送
`DISCONNECT 0x87` 再关闭，MQTT 3.x 直接关闭。MQTT 5 客户端必须在到期前发起 Enhanced AUTH
re-authentication；成功提交的新 principal 会原子替换旧 expiry deadline。禁用用户、轮换或撤销
credential 仍不会建立控制面到 Broker 的即时 push 通道，最坏传播时间由当前 principal TTL 决定。

## mTLS 身份与权限

管理请求的 actor 不能来自 header、JSON-RPC params 或 Dashboard form。listener 完成客户端证书链验证后，
runtime 的全局 mTLS middleware 才会将 Iris `Req.security.authenticated` 标记为 true；随后 management
resolver 从 `Req` 取得规范的 `sha256:<64 lowercase hex>` 指纹，并执行：

```text
verified certificate fingerprint
  -> configured certificate binding
  -> enabled principal in the selected control Repository and one root group
  -> current effective roles
  -> viewer/user_admin/policy_admin/security_admin permission bits
```

任一步失败都返回未授权。保留角色是精确字符串；其他业务角色不会获得管理权限。绑定的 principal 必须在
所选 Repository 中已存在、启用且至少拥有一个保留角色，否则正式启动失败。当前仓库尚未交付首位管理员 bootstrap
命令，不能通过直接编辑 SQLite 绕过领域事务、revision 与审计不变量。

生成证书时应分别维护 server CA 与 management client CA，客户端证书包含 `clientAuth` EKU，server 证书包含
监听 DNS/IP 的 SAN。私钥文件只允许服务账户读取。得到客户端证书指纹的 OpenSSL 示例：

```powershell
openssl x509 -in admin-client.pem -noout -fingerprint -sha256
```

将输出去掉冒号并转换为小写，再加 `sha256:` 前缀写入 binding。证书轮换期间可并列配置新旧两个唯一
fingerprint，完成客户端切换后删除旧 binding 并重启；不能把证书 subject/CN 当作稳定身份。

## 启动与关闭

```powershell
build\Msvc-Release\bin\flowie-control.exe `
  --config C:\flowie\flowie-control.yml
```

正常启动只调用显式 host 的 `iris_app_listen_tls_on()`，并固定
`TURBO_TLS_CLIENT_AUTH_REQUIRED`。Iris 收到 `SIGINT`/`SIGTERM` 后停止 event loop；runtime 随后先解除
Auth、Dashboard 和 RPC 绑定，再销毁 app、caller-owned RPC context、service 与所选 Repository。
PostgreSQL provider 会先拒绝新 lease 并检查所有 lease 已归还；关闭失败会返回进程错误而不是静默退出。

Dashboard 固定路径为 `/v1/management/dashboard`，RPC 使用配置的 `management.rpc_path`，认证服务固定为
`POST /v3/authenticate`。不要经由会终止并替换客户端证书身份的反向代理暴露这些路由，除非代理到
controller 的连接仍提供受信、唯一且经过审核的 mTLS 身份映射。
