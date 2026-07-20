# Flowie Control 部署与配置指南

`flowie-control` 是独立于 MQTT 数据面的管理进程。它组合 SQLite 控制事实源、JSON-RPC、HTMX
Dashboard 和可选的 `/v2/authenticate` 服务，只监听一个显式 HTTPS/mTLS 地址。当前仍缺少首位管理员
bootstrap、限流、HA/migration 和完整安全发布 gate，因此不能据此宣称整个控制面已达到生产就绪。

## 构建与预检

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target flowie-control

$env:FLOWIE_CONTROL_KEY_PASSWORD = "private-key-password"
build\Msvc-Release\bin\flowie-control.exe --check `
  --config flowie\examples\flowie-control.yml
```

真实 listener/mTLS/ACL gate 使用临时证书、SQLite 和子进程，不修改源码树中的部署文件：

```powershell
ctest --preset win-release-user -R test_flowie_control_https_integration --output-on-failure
```

也可使用环境变量，优先级固定为 CLI、进程环境、显式 DotEnv：

```powershell
$env:FLOWIE_CONTROL_CONFIG = "C:\flowie\flowie-control.yml"
build\Msvc-Release\bin\flowie-control.exe --check
```

仅在本地开发时使用 `--env-file/-E`。程序不会隐式读取当前目录 `.env`。`--check` 解析完整 schema，
并用 CoroNet/OpenSSL 实际加载 server chain、private key 和 client CA；它不打开 listener，也不创建或修改
SQLite。证书、私钥或 CA 无法加载时立即失败，不回退到 HTTP。

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
| `storage.sqlite.path` | 控制事实源；不是 Flowie provider 数据库 |
| `management.rpc_path` | 静态绝对 path，默认 `/v1/management/rpc` |
| `management.certificate_bindings` | mTLS SHA-256 指纹到 `(root_group, principal)` 的唯一绑定 |
| `dashboard.enabled` | 是否注册固定 HTMX Dashboard 路由 |
| `auth.enabled` | 是否注册 `/v2/authenticate`；默认关闭 |

当 `auth.enabled: true` 时还必须设置 `listener_id`、`method`、`service_token_ref` 和
`root_bindings`。service token 同样只接受 `env://...`；auth cache 容量最大 4096，TTL 最大 60 秒，
同一组容量/TTL 限制同时约束 positive credential cache 与 principal snapshot cache。principal cache
命中仍会复核 user/credential revision、全局 store revision 与 policy version，任一事实源不可用时
fail closed。
认证服务还会在 Argon2id/KDF 前执行双层 token bucket：默认每个已验证 mTLS caller 为
`100 requests/s`、burst `200`，每个 `(caller, root_group, principal)` 为 `5 requests/s`、burst `10`。
连续失败消耗 identity bucket；成功凭据只清除该 identity 的失败 bucket，不能重置 caller 总量。
桶有界且只保留 keyed digest，不保存 identity、证书指纹或 secret 明文。
YAML 不保存 ACL rule body、用户 credential、service token 或私钥内容。

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
  -> enabled SQLite principal in one root group
  -> current effective roles
  -> viewer/user_admin/policy_admin/security_admin permission bits
```

任一步失败都返回未授权。保留角色是精确字符串；其他业务角色不会获得管理权限。绑定的 principal 必须在
SQLite 中已存在、启用且至少拥有一个保留角色，否则正式启动失败。当前仓库尚未交付首位管理员 bootstrap
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
Auth、Dashboard 和 RPC 绑定，再销毁 app、caller-owned RPC context、service 与 store。

Dashboard 固定路径为 `/v1/management/dashboard`，RPC 使用配置的 `management.rpc_path`，认证服务固定为
`POST /v2/authenticate`。不要经由会终止并替换客户端证书身份的反向代理暴露这些路由，除非代理到
controller 的连接仍提供受信、唯一且经过审核的 mTLS 身份映射。
