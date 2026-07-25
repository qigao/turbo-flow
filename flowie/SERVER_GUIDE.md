# Flowie 服务端使用指南

本文面向使用仓库内置 `flowie_server` 或 `flowie_supervisor` 部署 MQTT 服务的运维人员和应用开发者。
Flowie 服务端支持 MQTT 3.1、3.1.1 与 5，监听 transport 支持 TCP、TLS、WS、WSS 和 Pipe。
本文中的 provider、backend、adapter、data source、data sink 和 session store 均采用
[配置式 Broker 概念与术语](CONFIGURED_BROKER_CONCEPTS.md)中的定义。
网络与用户态 buffer 参数见
[CoroNet Buffer Tuning Contract](../io/common/CORONET_BUFFER_TUNING.md)。

## 1. 服务端的两个输入

Flowie 将部署事实与数据流拓扑分开：

- YAML 保存 endpoint、Queue、可选 RuleSet、认证/ACL provider、容量和超时等部署配置。
- `.flow` 保存 source、stage、adapter、operation 和边的关系。

这里的完整 `flowie_server` 配置式 broker 才是基于 TurboFlow 的典型应用。协议库本身没有
Graph；单独注册的 Flowie endpoint 只是可复用组件，除非调用方进一步提供完整产品装配。

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

StorageBackend 也在该预检边界装配：产品宿主注册同级的 `tf_local_storage`、`tf_redis`、
`tf_pgsql` shared library，并通过 `io/common/storage` 的 registry/owner ABI 创建 service。
外部模块使用 `--storage-backend-plugin` 加载；`--record-store-plugin` 仅是兼容别名。Flowie
只消费 provider-neutral 的 FlowStore facade，不调用具体 backend 的 record/hash/index/log/state
函数；插件 function table 只公开 `open()` 和 `close()`。

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

Flowie 的 MQTT broker pipeline 分成协议 owner 阶段和应用 graph 阶段。协议 owner 在连接 lane
完成 framing、协议/大小校验、CONNECT 认证、操作 ACL 和 session/inflight admission；只有通过这些
边界的 PUBLISH 才会被 materialize 为拥有 `mem_buffer_t` 的 `turbo_flow_msg_t`。因此未经认证的
数据不会进入 TurboFlow Policy 或任意用户 stage。

Graph 可以按部署需要组合以下阶段：

```text
MQTT endpoint
  -> protocol/auth/ACL/inflight owner boundary
  -> turbo_flow_msg_t
  -> optional TurboFlow Policy filter/route/transform
  -> optional business data sink: Redis / PostgreSQL / HTTP / socket
  -> optional after-process stage
  -> MQTT fan-out / socket / HTTP / Redis output
  -> settlement -> protocol ACK
```

最小 profile 必须能解析到 endpoint；只有 Graph 引用 `rules.apply` 时
才需要 `rule_set`。业务 data source/data sink 不放入 profile，而是由 Graph 直接引用对应 YAML adapter。仓库示例采用
`endpoint -> RuleSet -> [MQTT fan-out, socket output]`，完整定义见
[flowie.flow](examples/flowie.flow)。`store` 可以放在 RuleSet 前后，但必须在 `.flow` 中显式
连接；前者保存原始 admitted packet，后者保存过滤/变换后的消息，二者不是同一种语义。

`session_store` 不是业务 data sink，也不是用户 Graph 节点。它只由 MQTT session owner 调用，
保存 session、subscription、inflight、Will 和 retained 等协议事实；普通 PUBLISH 业务正文只有
在 Graph 显式连接到 data sink 时才会成为外部业务事实。

settlement 是协议 owner 的 ACK prerequisite，不是普通 stage 返回值：

- `received`：收到并验证 packet，兼容旧行为；不要求 graph 成功。
- `accepted`：所选 graph admission stage 显式确认已接管消息。
- `processed`：本次同步 graph publication 的全部已选择分支完成；任一已选择分支失败都不会 ACK。
- `durable`：显式 durable store 成功提交，例如 Redis Stream `XADD` 或 record-store commit。

TurboFlow graph 可以处理任意 provider 转换出的 `turbo_flow_msg_t`；当前 Flowie endpoint 只将
admitted PUBLISH 暴露给 Graph，不能把 CONNECT/AUTH 等仍由协议 owner 管理的控制事务
直接变成用户 stage。TurboFlow Policy 也不能伪造 MQTT ACK 或推进 session state。若 after-process 不应增加
ACK 延迟，应先在显式 accepted/durable handoff 完成 settlement，再从独立消费路径执行；不能在
同一次同步 publication 中静默忽略 branch failure。切换 save/store 位置、
store backend 或 settlement 时，应停止 endpoint、排空或显式处置 inflight work，同时部署 YAML
与 Graph，再执行 `--check` 后启动；这些字段不支持热重载。

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

WS/WSS 的 path 是精确匹配，不做前缀或大小写归一化。客户端必须在 Upgrade 请求中提供
`Sec-WebSocket-Protocol: mqtt`；缺失或不包含 `mqtt` token 的请求会在 MQTT handler 和 session admission
之前关闭。公开 Flowie client 的默认 path 是 `/mqtt`，反向代理转发时不得改写 path 或移除 subprotocol。

MQTT packet 只能放在 WebSocket binary data frame 中；text data frame 会以 close code 1003 拒绝。单帧及
分片重组后的累计 payload 都受 endpoint `max_packet_size` 限制，超限会以 close code 1009 拒绝。非法
control/close frame 会关闭连接。这些拒绝不会创建 MQTT session，也不会使 listener 退出；后续合法客户端
仍可连接。反向代理的 frame/message 上限应不高于 Flowie 的上限，避免代理层积累 Flowie 必然拒绝的数据。

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

`manage_sessions: true` 启用受限 session/retained 状态。未配置 `session_store` 时，Flowie 使用
`tf_local_storage` DLL 提供的 local Record backend；它是进程内 volatile，关闭或进程退出后状态
不可恢复。持久化时使用 YAML 中独立的 `record_store` channel：

- Redis：多实例共享或外部持久化部署。
- PostgreSQL：SMB 部署中的事务型 session/retained 持久化。

Redis/PostgreSQL 在这里是 `session_store` 的 durable record-store backend，由配置选择，不是
Graph data source/sink，也不是写死在 Flowie 领域代码中的认证数据库。认证用户数据仍只能由
HTTPS 认证服务管理。显式 `session_store` 选择失败时直接报错，不回退到 local。

可直接交付的组合示例位于 `examples/products/`：`flowie-dev.*` 使用 local volatile Record；
`flowie-smb.*` 使用 PostgreSQL record store 保存 session/retained，并通过 PostgreSQL outbox
保存业务 PUBLISH。SMB 的 QoS 1/2 ACK 只在 outbox INSERT 事务 COMMIT 后生成；独立 source
回放记录，Graph 成功后删行，失败则保留并在后续重试，因此交付语义为 at-least-once。

## 10. 发布前检查表

1. 运行 `flowie_server --check`。
2. 运行 `ctest --preset win-release-user -L flowie-release --output-on-failure`。
3. 验证证书链、主机名、过期时间和 mTLS 客户端身份。
4. 验证 auth/ACL service token 与私钥密码 reference 可解析且没有进入日志。
5. 验证连接、session、subscription、inflight、retained、Queue 和输出容量上限。
6. 验证慢订阅者策略和 settlement 终态。
7. 若使用 Redis，执行启用 Redis live gate 的部署测试。
8. 若使用 PostgreSQL，设置 `TURBO_FLOW_PGSQL_TEST_CONNINFO` 并执行 PostgreSQL live gate。

协议与尚未声明的产品边界见 [RELEASE_GATE.md](RELEASE_GATE.md)。
