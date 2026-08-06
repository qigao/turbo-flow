# ADR：Broker 通过 HTTPS 使用 Auth 与 ACL Decision Service

## 状态

已采纳。适用于 bundled `flowie_server` 与 `flowie-control`。

本文中的 HTTP 均指 HTTPS。明文 `http://`、Broker 直连认证数据库、Broker 直读 ACL 表和配置固定
业务 Domain 都不属于产品组合。

## 背景

Flowie Broker 只处理 MQTT identity、connection 和 topic，不拥有第三方系统的用户目录、Domain、
Group、Role 或 ACL policy。`flowie-control` 是这些控制面事实的所有者，并通过版本化 HTTPS
endpoint 向 Broker 返回认证结果和单次授权判定。

Domain 是第三方软件系统在 Control 中的接入 ID。系统可以存在大量并行 Domain；Broker 不选择
Domain，也不配置唯一业务 Domain。客户端通过 username/password 认证后，Auth service 从唯一匹配的
Control principal 返回其 Domain，后续 ACL decision 使用该 Domain 的已发布 policy。

旧设计使用静态 `auth.service_bindings` 把 Broker token 绑定到一个业务 Domain，并让 Broker 通过
`GET /v4/acl` 下载完整 ACL bundle。该模型存在三个问题：

- 静态配置把 service credential 与业务 Domain 混在一起，容易把 Broker 错误限制为单 Domain。
- token 生命周期不在 Control Repository 和管理审计中，无法通过统一命令生成、轮换和撤销。
- Broker 下载、解析和持有完整 ACL policy，复制了 Control 的授权事实和 parser/runtime 职责。

## 决策

系统边界固定如下：

```text
MQTT client
    |
    | username / password / client ID
    v
flowie_server
    |-- POST /v4/authenticate --> flowie-control --> Auth Repository
    |
    `-- POST /v4/acl/check ----> flowie-control --> published policy + ACL evaluator

third-party management backend
    `-- HTTPS login session --> /v2/control/rpc --> user/password/group/role/ACL commands
```

- Broker 只依赖 HTTPS Auth/ACL 契约，不接收数据库连接信息或完整 ACL rule body。
- `flowie-control` 拥有 Domain、principal、password/credential、Role、Group、ACL draft、published
  policy 和 audit。
- SQLite 与 PostgreSQL 是同一 Repository port 的可替换实现，不改变 Broker 契约。
- Auth 与 ACL endpoint 的 caller 使用 Repository 生成的 service credential，不再使用静态 binding。
- ACL 是逐请求 decision：CONNECT、SUBSCRIBE 和 PUBLISH 都由 Control 返回 allow/deny。
- FlowStore、ProtocolStore、session store 和 Graph adapter 不是 Auth/ACL 事实源。

## 三类身份

### Management principal

第三方管理后端先用 `Domain + principal + password` 调 `/v2/control/login`，取得有界 Management
session，再以该 session bearer 调 `/v2/control/rpc`。Management session 只用于增删改查 Control
数据，不能调用 Broker-facing endpoint，也不能作为 MQTT password。

Domain-scoped 管理账号只能操作自己的 Domain。只有在 `system` Domain 中有效的 `system_admin`
可以创建 Domain 和显式跨 Domain 管理。完整契约见
[MANAGEMENT_RPC_API.md](MANAGEMENT_RPC_API.md)。

### Service principal

Broker 或其他受信后端在 Repository 中表示为 `principal_type: service`。它使用
`control.credential.generate/rotate` 产生的 token，并按最小权限分配以下精确 Role：

| Role | Endpoint 权限 |
| --- | --- |
| `flowie_auth_client` | 调用 `/v4/authenticate` |
| `flowie_acl_client` | 调用 `/v4/acl/check` |

每次请求必须同时发送：

```http
Authorization: Bearer <service-token>
X-Flowie-Service-Id: broker-main
X-Flowie-Service-Domain: platform-services
```

`X-Flowie-Service-Domain` 只定位 service principal 所属的 credential namespace。它不是被认证
MQTT 用户的 Domain，不限制 Broker 支持的业务 Domain，也不会加入 topic。缺 header、重复 header、
token 错误、principal disabled、credential revoked、ID/Domain 不匹配或缺少 endpoint Role 均 fail
closed。

这两个 Role 授予 Broker 级信任，不是普通业务 Role。尤其 `/v4/acl/check` 的 caller 可以为 Auth
返回的任意业务 Domain 请求判定，因此 service credential 只能交给受控 Broker/backend。

### MQTT principal

MQTT client 只持有 Broker 地址、username/password、client ID 和最终 topic 字符串，不需要 Domain
或 service token。username 对应 Control `principal_id`；client ID 是独立的 MQTT session、takeover
和 routing key，不是凭据。password 只用于 CONNECT，不能参与 topic 构造。

本地 Auth 按 username 跨 Domain 解析 credential：

- 恰好一个 enabled principal 和有效 credential 匹配时继续认证；
- 没有匹配或相同 username 出现在多个 Domain 时拒绝；
- 模糊和不存在路径执行等价的 secret verification 工作，避免暴露可枚举差异；
- 成功响应中的 Domain 来自 Repository，客户端不能通过 topic 或 header 自行声明。

因此部署必须保证 enabled username 全局唯一。建议第三方使用带系统前缀的 username，例如
`warehouse-device-202`。

## Auth HTTPS 契约

Endpoint 是 `POST /v4/authenticate`，payload protocol version 是 `3`。URL major 与 payload version
分别版本化，客户端必须同时使用这里规定的值。

请求 body 必须精确包含以下字段，unknown 或缺失字段均拒绝：

```json
{
  "version": 3,
  "identity": "warehouse-device-202",
  "method": "password",
  "secret_base64": "Y2xpZW50LXBhc3N3b3Jk",
  "protocol": "mqtt5",
  "remote_address": "192.0.2.10:43120",
  "peer_certificate_sha256": ""
}
```

`secret_base64` 是 MQTT password bytes 的 canonical Base64 表示，不是 service token。
`remote_address` 必须来自 Broker 已验证的 transport provenance。启用 MQTT mTLS 时，
`peer_certificate_sha256` 是 listener 已验证证书的规范小写 `sha256:` 指纹；未启用时为空字符串。

成功响应是严格 JSON：

```json
{
  "version": 3,
  "authenticated": true,
  "principal": {
    "id": "warehouse-device-202",
    "type": "device",
    "domain": "warehouse",
    "auth_method": "password",
    "scope": "domain",
    "roles": ["publisher"],
    "groups": ["operators", "east"],
    "expires_at": 1780000000,
    "policy_version": 42
  }
}
```

principal、effective Role/Group 和当前正数 `policy_version` 来自同一 Repository 状态。认证失败不
返回部分 principal。Broker 必须在 `expires_at` 后拒绝继续使用该 principal。

Argon2id 和同步 Repository 工作运行在专用有界 executor，不占用 CoroNet owner lane。队列满返回
HTTP 429，deadline 到期返回 HTTP 503；认证拒绝或 service caller 无权限返回 HTTP 403。deadline
不会强杀已接收的同步 KDF，任务仍负责最终擦除 secret，endpoint 关闭时 drain 已接收任务。

当前 composition root 只支持 Repository-backed local Auth。`auth.external_https` 仍有配置结构，但
runtime create 会以 `TURBO_ENOTSUP` fail fast；不能把它作为已发布能力，也不存在失败后回退本地
Auth 的路径。

## ACL Decision HTTPS 契约

Endpoint 是 `POST /v4/acl/check`，payload protocol version 是 `4`。Broker 每次发送 Auth 返回的
principal snapshot、当前操作和 MQTT context：

```json
{
  "version": 4,
  "access": "write",
  "topic": "warehouse/groups/china/east/operators/devices/warehouse-device-202/event",
  "username": "warehouse-device-202",
  "client_id": "edge-gateway-17",
  "principal": {
    "id": "warehouse-device-202",
    "type": "device",
    "domain": "warehouse",
    "expires_at": 1780000000,
    "policy_version": 42,
    "roles": ["publisher"],
    "groups": ["operators", "east"]
  }
}
```

`access` 只接受：

| 值 | 安全操作 |
| --- | --- |
| `connect` | MQTT CONNECT |
| `read` | MQTT SUBSCRIBE |
| `write` | MQTT PUBLISH |

成功 decision response 精确包含：

```json
{
  "version": 4,
  "allowed": true,
  "reason": "allow_rule",
  "policy_version": 42
}
```

`reason` 是 `allow_rule`、`deny_rule`、`default_deny`、`domain_mismatch`、
`principal_expired` 或 `policy_version_mismatch`。`allowed` 是授权结果，不应用 HTTP status 代替。
格式错误返回 HTTP 400，service caller 无权限返回 HTTP 403，Repository/runtime 故障返回 HTTP 503。

Control 使用 `principal.domain` 和 `principal.policy_version` 加载精确已发布 policy，以与 MQTT
matcher 相同的 parser/runtime 进行判定。不存在 policy、版本不匹配、policy/principal 过期、Domain
不一致、没有 allow 或 endpoint 不可用都 fail closed。显式 deny 优先于 allow。

ACL draft 仍按稳定 ordinal 存储，但每个 enabled user 在一个 Domain 中只能保存一份 canonical ACL
文档。发布事务重新校验完整 draft、主体、Domain、Group parent chain、grammar 和容量，随后原子生成
不可变 `policy_version`。Broker 不读取 draft，也不接收完整 bundle。文法见
[ACL_GRAMMAR.md](ACL_GRAMMAR.md)。

### 热路径权衡

逐请求 decision 把 Control 网络、Repository policy 读取和 matcher 执行放入 CONNECT、SUBSCRIBE、
PUBLISH 热路径。优点是 parser/evaluator 和授权事实只有一个所有者，policy 更新无需向 Broker 分发完整
快照；代价是 Control latency/availability 直接影响消息授权。

当前实现按请求加载精确 bundle 并创建 matcher/realm，尚未提供有界的 Domain/policy-version compiled
cache。因此生产容量必须以压力测试结果为准，并为 HTTP concurrency、timeout、Repository pool 和
Control 实例配置硬上限。未来缓存只能是从 published policy 重建的派生数据，必须以
`(domain, policy_version)` 为 key，容量有界且不能改变 fail-closed 语义。

## Broker 配置

Bundled `flowie_server` 只注册 HTTPS Auth/ACL provider：

```yaml
channels:
  mqtt.auth-service:
    kind: auth_provider
    config:
      backend: https
      url: https://flowie-control.internal/v4/authenticate
      method: password
      service_id: broker-main
      service_domain: platform-services
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      timeout_ms: 3000
      max_secret_size: 4096
      tls:
        ca_file: C:/certs/control-ca.pem

  mqtt.acl-service:
    kind: acl_provider
    config:
      backend: https
      url: https://flowie-control.internal/v4/acl/check
      service_id: broker-main
      service_domain: platform-services
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      timeout_ms: 3000
      max_response_size: 65536
      tls:
        ca_file: C:/certs/control-ca.pem
```

配置中没有业务 `domain`。`service_id`、`service_domain` 和 `service_token_ref` 只是 Broker 调
Control 的身份。URL 必须使用 HTTPS 并包含明确 path；userinfo、query、fragment、redirect 和隐式
retry 均拒绝。token 和 client key password 只能通过 key provider reference 注入，不能写入 YAML。

Auth 和 ACL 可以使用同一 service principal/token，但服务端分别验证精确 Role。也可以为两个 endpoint
使用不同的最小权限 principal。

## `flowie-control` 配置

Control 只配置 endpoint 行为和 Repository，不配置静态 service token：

```yaml
auth:
  enabled: true
  listener_id: flowie-control-auth
  method: password
  local_executor:
    workers: 4
    queue_capacity: 128
    deadline_ms: 10000
```

service principal、Role assignment 和 credential 都通过 Dashboard/Management RPC 写入 Repository。
第三方系统的完整 onboarding 和轮换步骤见
[THIRD_PARTY_INTEGRATION.md](THIRD_PARTY_INTEGRATION.md)。

`workers` 范围为 `1..64`，`queue_capacity` 为 `1..4096`，`deadline_ms` 为 `1..60000`。配置、
secret reference、TLS 文件或 Repository 前置条件无效时启动直接失败，不做 fallback。

## Repository 与一致性

`flowie_control_repository_t` 是内部、版本化 persistence port。Auth service、ACL endpoint、
Management service 和 Dashboard 只依赖该 port，不直接解释 SQLite/PostgreSQL SQL。

- 每个运行实例只选择一个 Repository；禁止 SQLite/PostgreSQL 双写或故障后切换事实源。
- username credential resolution 在 Repository 内完成唯一 Domain 判定；0 或多条匹配都拒绝。
- service credential resolution 使用 `(service_domain, service_id, token)` 并重新读取 enabled、
  credential revision 和 effective endpoint Role。
- ACL decision 读取请求指定的不可变 published policy version，不读取 draft。
- credential 明文只在 generate/rotate 成功响应返回一次；Repository 只保存 verifier。
- Management command 负责 revision、引用约束、幂等、事务和 audit；第三方不得直接改数据库。

PostgreSQL principal snapshot 使用只读可重复读事务。credential KDF 不在持有数据库租约时执行，KDF
后重新读取 generation 以拒绝并发 rotate/revoke。写事务 `COMMIT` 结果不确定时，通过同一命令的
audit 完整身份确认；无法确认则 fail closed，不猜测成功。

## TLS 与 secret 处理

- 所有链路必须验证服务端证书链和 hostname。
- 高安全部署可以要求 Broker client certificate，但 service bearer token 仍是必需身份因子。
- MQTT client certificate fingerprint 与 Broker 调 Control 的 service TLS certificate 是不同身份。
- Broker 出站只允许 Control endpoint 和必要 DNS，不得访问 Control 数据库。
- Authorization、MQTT password、生成 token、Base64 临时值和 KDF 数据在生命周期结束时清零。
- password、service token、Management session 不得进入 URL、配置明文、日志或错误 telemetry。

## 兼容性与迁移

这是公开行为和配置格式的有意变更：

| 旧模型 | 新模型 |
| --- | --- |
| `auth.service_bindings[]` 静态 token/Domain | Repository service principal + Role + generated credential |
| Broker 配置隐含/固定业务 Domain | 无业务 Domain；Auth principal 返回 Domain |
| `GET /v4/acl` 下载 version 3 bundle | `POST /v4/acl/check` 返回 version 4 decision |
| Broker ACL `max_rules` | 删除；request/response 由 `max_response_size` 限制 |
| Broker 编译并缓存完整 policy | Control 对每次操作执行 parser/runtime decision |

升级顺序：

1. 在 Control Repository 创建 Broker service principal，分配 `flowie_auth_client` 和
   `flowie_acl_client`，生成 token 并写入 secret provider。
2. 部署支持数据库 service credential 和 `/v4/acl/check` 的 `flowie-control`。
3. 同一维护窗口更新 Broker channel 的 `service_id`、`service_domain`、ACL URL，并删除
   `max_rules`。
4. 验证多 Domain username 登录、CONNECT/read/write allow/deny、错误 token、rotate/revoke 和 Control
   不可用时 fail closed。
5. 删除旧 `service_bindings` 配置和不再使用的静态 secret。

旧 Broker 与新 ACL endpoint 不兼容，协议不自动降级。回滚必须成组恢复旧 Broker/Control 二进制和
旧配置；不得让新旧进程同时写不同授权事实。

## 验证范围与剩余风险

当前自动化覆盖：

- SQLite/PostgreSQL repository-backed service credential resolution；
- service endpoint Role 最小权限、错误 token/Domain/ID 和 revoke/disable 拒绝；
- username 唯一 Domain 解析与跨 Domain 重名 fail closed；
- Auth v3 严格 request/response、principal 清零、限流和 bounded executor；
- ACL v4 request/decision 编解码、HTTPS headers、mTLS 和 allow/deny reason；
- canonical ACL parser、Group tree、`%u`/`%c`、单 subject 文档和 publish validation。

生产开放前仍需完成：

- 双进程真实 TLS/MQTT 多 Domain 回归和 Docker 部署 gate；
- ACL decision compiled cache 或无缓存容量结论及 P50/P95/P99 压力数据；
- Control HA、Repository pool、连接/请求限流、证书轮换、备份/PITR 和故障注入；
- ASan/UBSan、threat model 与 service credential 泄露响应 runbook；
- external HTTPS Auth verifier 的明确实现决策；在此之前保持 `TURBO_ENOTSUP`。
