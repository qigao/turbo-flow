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
Graph；`flowie_endpoint_core_*` 直连 endpoint 不创建 Graph。只有调用方显式注册 Graph adapter
并提供拓扑时，它才成为 TurboFlow composition 的一部分。

可直接使用仓库中的完整示例：

- [flowie.yml](examples/flowie.yml)
- [flowie.flow](examples/flowie.flow)

服务端不会从 Broker YAML 中读取 ACL rule body，也不会让 MQTT worker 连接用户认证数据库。生产入口
使用 `flowie_server --control-config <flowie-control.yml>` 在同一应用生命周期内启动 Control runtime；
MQTT worker 仍只通过 loopback HTTPS `/v4/authenticate` 和 `/v4/acl` 访问它，不跨层调用 Repository。
Control 可把本地 Auth/ACL/管理事实存入 SQLite 或 PostgreSQL；数据库连接与权限不会进入 MQTT worker。
独立 `flowie-control` 可执行文件保留为兼容、诊断入口，不是推荐的生产组合入口。

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
  --require-security `
  --profile flowie `
  --control-config flowie\examples\flowie-control.yml `
  flowie\app\tests\flowie_server_https_secure.yml `
  flowie\examples\flowie.flow
```

成功输出：

```text
flowie_server: configuration and graph are valid
```

`--check` 会解析 Broker/Control 配置、解析 Graph、创建所选 Broker provider、装配资源并编译 Graph，
但不会绑定 listener、打开 Control 数据库或执行 bootstrap。
字段类型错误、未知 backend、缺少 secret reference、Graph 引用不存在或 provider 无法初始化都会直接失败，
不会回退到不安全模式。

生产预检和启动必须使用 `--require-security`。该开关要求 endpoint 同时组合 `auth_method`、HTTPS Auth
provider、`security_realm` 与 HTTPS ACL policy provider；任一环节缺失或初始化失败都会拒绝启动。省略该
开关只用于兼容仓库内现有开发与匿名测试 profile，不应作为生产部署方式。

StorageBackend 也在该预检边界装配：产品宿主注册同级的 `tf_local_storage`、
`tf_sqlite_storage`、`tf_redis`、`tf_pgsql` shared library，并通过 `io/common/storage` 的
registry/owner ABI 创建 service。
外部模块使用 `--storage-backend-plugin` 加载；`--record-store-plugin` 仅是兼容别名。Flowie
协议路径只消费 ProtocolStore facade，业务路径消费 FlowStore facade，不调用具体 backend 的
record/hash/index/log/state
函数；插件 function table 只公开 `open()` 和 `close()`。

Bundled Server 固定包含 Socket、Redis、PostgreSQL、HTTP client/server 与 HTTPS Auth/ACL 能力，不按
编译开关生成不同语义的二进制。YAML 与 Flow 是实例的唯一组合来源：只有被 profile、channel、adapter
或 Graph 引用的 provider/backend 才会创建连接和运行时状态。

## 4. 启动与监管

直接运行 worker：

```powershell
build\Msvc-Release\bin\flowie_server.exe `
  --require-security `
  --profile flowie `
  --control-config C:\flowie\flowie-control.yml `
  flowie\app\tests\flowie_server_https_secure.yml `
  flowie\examples\flowie.flow
```

通过 supervisor 运行：

```powershell
build\Msvc-Release\bin\flowie_supervisor.exe `
  --require-security `
  --profile flowie `
  --worker build\Msvc-Release\bin\flowie_server.exe `
  --capture-output 1048576 `
  --control-config C:\flowie\flowie-control.yml `
  flowie\app\tests\flowie_server_https_secure.yml `
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

`protocol_store` 不是业务 data sink，也不是用户 Graph 节点。它只由 MQTT protocol owner 调用，
保存 session、subscription、inflight、Will 和 retained 等协议事实；普通 PUBLISH 业务正文只有
在 Graph 显式连接到 BusinessStore/data sink 时才会成为外部业务事实。旧 `session_store` 仅是
互斥的配置兼容名，不代表第二存储层。

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
  tls_client_ca_file: C:/certs/mqtt-client-ca.pem
```

`tls_client_ca_file` 仅允许用于 TLS/WSS；配置后客户端证书变为必需，并且 endpoint 必须绑定
`security_realm`。证书链验证成功后，Flowie 才把规范 SHA-256 指纹传给 Auth provider。省略该字段时
保持 server-auth-only TLS，Auth 请求中的客户端证书字段为空。证书或 CA 缺失、无法加载或客户端验证
失败时启动/握手 fail closed。私钥文件应只允许服务账户读取，不得写入 YAML、日志或镜像的公共层。

WS/WSS 的 path 是精确匹配，不做前缀或大小写归一化。客户端必须在 Upgrade 请求中提供
`Sec-WebSocket-Protocol: mqtt`；缺失或不包含 `mqtt` token 的请求会在 MQTT handler 和 session admission
之前关闭。公开 Flowie client 的默认 path 是 `/mqtt`，反向代理转发时不得改写 path 或移除 subprotocol。

MQTT packet 只能放在 WebSocket binary data frame 中；text data frame 会以 close code 1003 拒绝。单帧及
分片重组后的累计 payload 都受 endpoint `max_packet_size` 限制，超限会以 close code 1009 拒绝。非法
control/close frame 会关闭连接。这些拒绝不会创建 MQTT session，也不会使 listener 退出；后续合法客户端
仍可连接。反向代理的 frame/message 上限应不高于 Flowie 的上限，避免代理层积累 Flowie 必然拒绝的数据。

## 7. HTTPS 认证、ACL 与服务凭证

### 公网 TLS 部署组件

`flowie/deploy/nginx/` 是 `flowie_server` 的版本化 edge Compose，并随安装产物复制到
`share/turboflow/deploy/flowie-nginx`；MQTT 专用组件同时安装到
`share/turboflow/deploy/flowie-haproxy`。Compose 中 Nginx 只负责公网 `80/443`、ACME challenge、
HTTPS TLS termination 和 Certbot；HAProxy 负责 `8883` MQTTS TLS termination、CONNECT Client ID
consistent hash 及 backend PROXY protocol v1。二者都不是 Auth/ACL 服务，也不持有用户、规则、
MQTT session、shard ownership 或数据库凭据。

推荐单机拓扑为 Nginx 到 loopback Control TLS `127.0.0.1:8443`，HAProxy 到 loopback plaintext MQTT
`127.0.0.1:18883`。多节点部署把 `FLOWIE_MQTT_BACKENDS` 指向各 Flowie edge 的受保护私网 listener。
公网浏览器和 MQTT 客户端只校验公共证书，不提供客户端证书；Dashboard 仍使用登录 session，Broker
仍通过 HTTPS service token 访问 Auth/ACL。MQTT endpoint 必须启用只信任 HAProxy transport peer 的
PROXY 策略。完整 `.env`、首次签发、Compose 启动、Client ID 路由和源地址边界见
[deploy/nginx/README.md](deploy/nginx/README.md)。

Nginx 和 HAProxy 都在启动前校验配置；HAProxy 会监测续期证书并使用 master-worker 平滑 reload。
部署主机必须显式放行 `80/443/8883`，不能同时运行另一个证书续期 timer 或占用这些端口的代理。

安全 profile 同时选择 auth provider 和 security realm。完整结构见
[flowie_server_https_secure.yml](app/tests/flowie_server_https_secure.yml)。默认使用普通 TLS server
authentication 加有作用域的 service token；以下 `client_cert_file/client_key_file` 只是在隔离服务网络中
显式启用的第二因子，不得成为浏览器或公网 MQTT 客户端的要求：

```yaml
profiles:
  flowie:
    auth_provider: mqtt.auth-service

channels:
  mqtt.auth-service:
    kind: auth_provider
    config:
      backend: https
      url: https://auth.internal.example/v4/authenticate
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
- Auth v3 的 `remote_address` 来自 endpoint 已验证的 transport provenance：默认是直接 socket peer；
  显式启用 trusted PROXY v1/v2 后是 header 中的源地址，同时保留 direct transport peer。Flowie 不读取
  `X-Forwarded-For`，且未信任 peer 不能提供源地址。
- Auth v3 的 `peer_certificate_sha256` 只来自已启用 `tls_client_ca_file` 的 TLS/WSS listener，并与
  Broker 调用 Auth 服务时使用的 mTLS client certificate 相互独立。
- 认证失败、ACL bundle 过期、证书失败和 provider 网络错误全部 fail closed。
- bundled 产品只注册 `https` auth backend；FlowStore、session store、Graph adapter、本地
  SQLite/Redis/PostgreSQL 或进程内 OIDC/LDAP/RADIUS 模块都不能成为认证来源。
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
`continue_exchange` 返回 `TURBO_ENOTSUP`。底层 ABI 允许程序化宿主实现 exchange，但 bundled 产品不把
自定义进程内模块作为第二认证来源。若未来支持真正多轮认证，必须定义版本化 HTTPS exchange 契约并
完成取消、超时和重认证测试；当前部署不能依靠 YAML 把一次性 HTTPS provider 自动升级为多轮协议。

MQTT 3.1/3.1.1 没有 MQTT 5 AUTH exchange，使用普通认证结果和各自版本可表达的 CONNACK 错误。

## 9. Session、retained 与持久化

`manage_sessions: true` 启用受限 session/retained 状态。未配置 `protocol_store` 时，standalone
Flowie 使用独占连接的 SQLite `:memory:` Record backend；打开、schema、CAS 或容量失败会中止启动。
显式配置可使用 YAML 中独立的 `record_store` channel，但 `flowie-server` 仍要求 `backend: sqlite`
及 `database_path: ':memory:'`，只允许调整 namespace 和容量：

- SQLite `:memory:`：standalone 协议事实，进程停止后清空。
- Redis：cluster 协议事实的目标 backend；切换前必须通过 epoch/failover durability gate。

PostgreSQL继续保存 cluster ownership、owner epoch 和 fencing；当前 cluster session facts 在
Redis cutover 完成前仍使用既有 PostgreSQL 实现。协议 Store 和业务 FlowStore 即使选择同一种
引擎，也使用独立 namespace、连接 owner、权限、容量和 migration。显式 `protocol_store` 失败时
直接报错，不回退到 SQLite、Redis、PostgreSQL 或 local。

standalone 重启后 Client 必须重新连接和订阅，Session Present 不从 BusinessStore 恢复。需要长期保存
的消息、设备业务状态、索引或 outbox 由 Graph 写入独立 BusinessStore，可选择 SQLite、Redis 或
PostgreSQL；它与 ProtocolStore 没有 fallback、恢复或双写关系。

可直接交付的组合示例位于 `examples/products/`。旧 product 配置中的 `session_store` 仍可解析，
但应迁移为 `protocol_store`；业务 PUBLISH 可继续通过 PostgreSQL outbox 保存。SMB 的 QoS 1/2
ACK 只在 outbox INSERT 事务 COMMIT 后生成；独立 source
回放记录，Graph 成功后删行，失败则保留并在后续重试，因此交付语义为 at-least-once。

## 10. 发布前检查表

1. 对生产配置运行 `flowie_server --check --require-security`。
2. 运行 `ctest --preset win-release-user -L flowie-release --output-on-failure`。
3. 验证公网/内部证书链、主机名与过期时间；仅在显式启用时验证服务端 mTLS 第二因子。
4. 验证 auth/ACL service token 与私钥密码 reference 可解析且没有进入日志。
5. 验证连接、session、subscription、inflight、retained、Queue 和输出容量上限。
6. 验证慢订阅者策略和 settlement 终态。
7. 若使用 Redis，执行启用 Redis live gate 的部署测试。
8. 若使用 PostgreSQL，设置 `TURBO_FLOW_PGSQL_TEST_CONNINFO` 并执行 PostgreSQL live gate。

协议与尚未声明的产品边界见 [RELEASE_GATE.md](RELEASE_GATE.md)。
