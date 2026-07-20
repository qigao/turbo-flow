# Flowie 服务端使用指南

本文面向使用仓库内置 `flowie_server` 或 `flowie_supervisor` 部署 MQTT 服务的运维人员和应用开发者。
Flowie 服务端支持 MQTT 3.1、3.1.1 与 5，监听 transport 支持 TCP、TLS、WS、WSS 和 Pipe。

## 1. 服务端的两个输入

Flowie 将部署事实与数据流拓扑分开：

- YAML 保存 endpoint、Queue、RuleSet、认证/ACL provider、容量和超时等部署配置。
- `.flow` 保存 source、stage、adapter、operation 和边的关系。

可直接使用仓库中的完整示例：

- [flowie.yml](examples/flowie.yml)
- [flowie.flow](examples/flowie.flow)

服务端不会从 YAML 中读取 ACL rule body，也不会连接用户认证数据库。用户认证数据库只能由独立认证
服务访问；Flowie 只访问 HTTPS auth/ACL 服务，或在单机部署中使用本地 SQLite ACL bundle。

## 2. 构建

Windows Release：

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target flowie_server flowie_supervisor
```

产物默认位于 `build/Msvc-Release/bin/`。安装后，示例位于
`share/turboflow/examples/flowie/`。

## 3. 启动前检查

每次修改 YAML、Graph、证书路径或 provider 后，先运行完整预检：

```powershell
build\Msvc-Release\bin\flowie_server.exe --check `
  --profile flowie `
  flowie\examples\flowie.yml `
  flowie\examples\flowie.flow
```

成功输出：

```text
flowie_server: configuration and graph are valid
```

`--check` 会解析配置、解析 Graph、创建所选 provider、装配资源并编译 Graph，但不会绑定 listener。
字段类型错误、未知 backend、缺少 secret reference、Graph 引用不存在或 provider 无法初始化都会直接失败，
不会回退到不安全模式。

## 4. 启动与监管

直接运行 worker：

```powershell
build\Msvc-Release\bin\flowie_server.exe `
  --profile flowie `
  flowie\examples\flowie.yml `
  flowie\examples\flowie.flow
```

通过 supervisor 运行：

```powershell
build\Msvc-Release\bin\flowie_supervisor.exe `
  --profile flowie `
  --worker build\Msvc-Release\bin\flowie_server.exe `
  --capture-output 1048576 `
  flowie\examples\flowie.yml `
  flowie\examples\flowie.flow
```

`--capture-output` 是 supervisor 可保留的 worker 输出上限。生产服务应由操作系统 service manager 管理，
并为进程配置 CPU、内存、句柄和日志配额。

## 5. Endpoint 与 Graph

最小 profile 必须能解析到 endpoint、Queue sink/source、RuleSet 和 output。示例 Graph 的数据流是：

```text
MQTT endpoint -> accepted Queue -> RuleSet -> MQTT fan-out
                                      `----> application socket output
```

完整定义见 [flowie.flow](examples/flowie.flow)。修改 settlement 语义时必须同步修改 Queue 与 Graph：

- `received`：收到并验证 packet 后确认。
- `accepted`：有界 Queue 接管后确认。
- `processed`：Graph attempt 完成后确认。
- `durable`：SQLite transaction 或 Redis XADD 提交后确认。

这些边界不能互相模拟。切换 Queue backend 或 settlement 时，应停止 endpoint、排空流量、同时部署 YAML
与 Graph，再执行 `--check` 后启动。

## 6. TCP、TLS、WS、WSS 与 Pipe

在 `adapters.<name>.config.transport` 中选择 `tcp`、`tls`、`ws`、`wss` 或 `pipe`。

TLS/WSS listener 从 CoroNet 进程环境读取证书：

```powershell
$env:TURBONET_TLS_CERT_FILE = "C:\certs\server-chain.pem"
$env:TURBONET_TLS_KEY_FILE = "C:\certs\server-key.pem"
```

然后将 endpoint transport 设置为 `tls` 或 `wss`。WSS endpoint 应显式配置与客户端一致的 path：

```yaml
config:
  transport: wss
  path: /mqtt
```

证书缺失或无法加载时启动失败。私钥文件应只允许服务账户读取，不得写入 YAML、日志或镜像的公共层。

WS/WSS 使用 WebSocket subprotocol `mqtt`。公开 Flowie client 的默认 path 是 `/mqtt`；服务端与反向代理
应保持 path 和 subprotocol 一致。

## 7. HTTPS 认证、ACL 与 mTLS

安全 profile 同时选择 auth provider 和 security realm。完整结构见
[flowie_server_https_secure.yml](app/tests/flowie_server_https_secure.yml)。生产配置可为 auth 与 ACL 服务分别
配置 mTLS：

```yaml
profiles:
  flowie:
    auth_provider: mqtt.auth-service

channels:
  mqtt.auth-service:
    kind: auth_provider
    config:
      backend: https
      url: https://auth.internal.example/v2/authenticate
      method: password
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      timeout_ms: 3000
      max_secret_size: 4096
      tls:
        ca_file: C:/certs/auth-service-ca.pem
        client_cert_file: C:/certs/flowie-client.pem
        client_key_file: C:/certs/flowie-client-key.pem
        client_key_password_ref: env://FLOWIE_AUTH_TLS_KEY_PASSWORD
```

该片段需要合并到完整配置中，并由 endpoint 的 `security_realm`、`auth_method` 和 realm 的
`policy_source` 引用。安全规则如下：

- URL 必须是带明确 path 的 HTTPS URL；禁止 userinfo、query、fragment 和 redirect。
- `client_cert_file` 与 `client_key_file` 必须同时存在。
- 私钥密码只允许使用 key-provider reference，不允许 YAML literal。
- service token 每次请求重新从 key provider 获取，以支持轮换。
- 认证失败、ACL bundle 过期、证书失败和 provider 网络错误全部 fail closed。
- Flowie 进程网络 ACL 只允许访问认证服务；认证数据库不得暴露给 Flowie 网段或公网。
- auth service 返回的 principal 必须带短 TTL。到期时即使连接完全空闲，MQTT 5 也会返回
  `DISCONNECT 0x87` 后关闭，MQTT 3.x 直接关闭；MQTT 5 必须在到期前完成 Enhanced AUTH
  re-authentication，成功的新 principal 会替换旧 expiry deadline。
- 安全 endpoint 会在接受 CONNECT 前按 PUBLISH ACL 校验 Will Topic；只有 CONNECT 权限而没有对应 topic
  PUBLISH 权限的主体不能注册 Will。
- 用户禁用和 credential 轮换/撤销不会即时 push 到 Broker；最坏撤销传播时间由当前 principal TTL
  决定。`recv_timeout_ms` 和 keepalive 仍用于失活连接回收，但不再承担 principal 到期强制断开的职责。

## 8. enhanced provider 是什么

普通 auth provider 是一次调用：输入 identity/method/credential，返回 principal 或拒绝。

enhanced provider 是 MQTT 5 多轮认证状态机，接口包含：

- `begin`：接收 CONNECT Authentication Method/Data，创建 exchange，可返回 challenge。
- `continue_exchange`：处理客户端后续 AUTH 数据，继续 challenge 或返回最终 principal。
- `cancel`：连接断开、超时或协议失败时释放 exchange。

典型用途是 SCRAM、外部 challenge-response、硬件令牌或需要多轮交互的企业身份协议。MQTT 5 使用
AUTH reason `0x18` 继续认证，已连接会话使用 `0x19` 发起 re-authentication。

内置 HTTPS provider 只支持一次 HTTPS credential 验证：它的 `begin` 可以直接成功，但
`continue_exchange` 返回 `TURBO_ENOTSUP`。需要真正多轮认证时，应由自定义 product composition root
注入实现完整 enhanced provider ABI 的模块，不能依靠 YAML 将一次性 HTTPS provider 自动升级为多轮协议。

MQTT 3.1/3.1.1 没有 MQTT 5 AUTH exchange，使用普通认证结果和各自版本可表达的 CONNACK 错误。

## 9. Session、retained 与持久化

`manage_sessions: true` 启用受限 session/retained 状态。未配置 `session_store` 时状态只在进程内有效。
持久化时使用 YAML 中独立的 `record_store` channel：

- SQLite：单机、文件权限可控的部署。
- Redis：多实例共享或外部持久化部署。

Redis/SQLite 在这里是 provider，由配置选择，不是写死在 Flowie 领域代码中的认证数据库。认证用户数据仍
只能由 HTTPS 认证服务管理。

## 10. 发布前检查表

1. 运行 `flowie_server --check`。
2. 运行 `ctest --preset win-release-user -L flowie-release --output-on-failure`。
3. 验证证书链、主机名、过期时间和 mTLS 客户端身份。
4. 验证 auth/ACL service token 与私钥密码 reference 可解析且没有进入日志。
5. 验证连接、session、subscription、inflight、retained、Queue 和输出容量上限。
6. 验证慢订阅者策略和 settlement 终态。
7. 若使用 Redis，执行启用 Redis live gate 的部署测试。

协议与尚未声明的产品边界见 [RELEASE_GATE.md](RELEASE_GATE.md)。
