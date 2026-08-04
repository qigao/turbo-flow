# ADR：Broker 通过 HTTPS 组合 Auth 与 ACL

## 状态

已采纳。适用于 bundled `flowie_server` 与 `flowie-control`。

本文中的 HTTP 均指 HTTPS。明文 `http://`、Broker 直连认证数据库和 Broker 直读 ACL 数据库都不属于
产品组合。

## 决策摘要

系统边界固定如下：

```text
MQTT client
    |
    v
flowie_server
    |-- POST /v4/authenticate --> flowie-control --> local Auth Repository
    |                                             或 third-party HTTPS Auth
    |
    `-- GET  /v4/acl ---------> flowie-control --> local ACL Repository
              |
              `-> Broker 编译并原子替换本地不可变 ACL snapshot

third-party management system
    `-- HTTPS login session --> flowie-control --> user/credential/role/group/ACL commands

empty-store operator bootstrap
    `-- fixed system/admin credential --> flowie-control --> same repository commands/audit
```

- Broker 只依赖 HTTPS Auth/ACL 契约，不接收数据库连接信息。
- `flowie-control` 是本地账户映射、Role、Group、ACL 草稿、已发布 bundle 和审计的领域所有者。
- Auth 有且仅有一个 verifier：本地 Auth，或第三方 HTTPS Auth。
- ACL 只有本地 Repository 这一事实源；不存在第三方 ACL 热路径和 Broker 数据库 ACL provider。
- SQLite 与 PostgreSQL 是 `flowie-control` Repository 的可替换持久化实现，不改变 Broker 契约。
- FlowStore、session store 和 Graph data adapter 只承载 MQTT/业务状态，不能作为 Auth/ACL 事实源。
- 空 Repository 自动以固定公开初始凭据建立首位 `system_admin`；首次登录只允许改密，之后全部写入走管理 RPC。

TLS 身份边界也固定：

- Dashboard 和面向用户的管理 RPC 使用登录 session，浏览器不请求客户端证书。
- Broker 默认使用服务端 TLS 校验加 `service_bindings[].token_ref`；每个 token 只绑定一个
  `(service_id, domain)`，最多 32 个 binding。
- 高安全部署可把 `listener.tls.client_auth` 设为 `required`，并在 binding 上增加
  `peer_certificate_sha256`。由于当前 CoroNet listener 不支持“可选请求客户端证书”，该模式必须关闭
  Dashboard，不能与浏览器入口混用；它不改变 bearer token 仍为必需条件。

## 为什么这样划分

如果 Broker 直接访问 SQLite、PostgreSQL、LDAP 或第三方用户库，数据库 schema、凭据、KDF、连接池和
迁移策略都会进入数据面。Broker 被攻破后也将获得身份库网络权限。若每次 PUBLISH/SUBSCRIBE 都远程
查询 ACL，则网络延迟和控制面故障会进入消息热路径。

当前划分让 Broker 只处理两个稳定协议：

- CONNECT 时调用一次认证接口，得到有界、版本化 principal。
- policy version 缺失或变化时拉取完整 ACL bundle，随后只在本地不可变 snapshot 上授权。

因此控制面可以动态变化，而正常消息授权不执行 HTTP、SQL 或动态分配。

## Auth 边界

### 本地 Auth

当 `auth.enabled: true` 且未配置 `auth.external_https` 时，`flowie-control` 使用本地 Repository 中的
credential verifier。账户启用状态、credential revision、Role、Group 和 policy version 都来自同一个
Repository 一致性快照。

本地 credential 由管理命令生成、轮换和撤销；明文只返回一次，不写入配置、日志或审计。KDF、缓存、
限流和 secret wipe 仍适用。

本地 verifier 的 Argon2id、SQLite 和同步 PostgreSQL 调用运行在专用有界 executor，不占用 CoroNet
owner lane。Iris `Req`、`Res` 和 socket 永不跨线程；owner lane 先完成 Bearer token
解析、可选证书第二因子校验和严格 JSON 解码，再把作用域服务身份及有所有权的字段副本提交给 worker。
队列满立即返回 HTTP 429；
deadline 到期返回 HTTP 503，并丢弃迟到结果。同步 KDF/数据库调用不能安全抢占，因此 deadline 不强杀
已接收任务；任务继续持有并最终擦除自己的 secret，关闭 endpoint 时停止接单并 drain 全部已接收任务。

### 第三方 HTTPS Auth

当配置 `auth.external_https` 时，它替换本地 credential verifier。第三方系统可以在其服务内部访问
OIDC、LDAP/AD、RADIUS、Redis、PostgreSQL 或专用身份库；这些 SDK、数据库连接和原始 claims 不进入
Flowie 或 `flowie-control` 的领域核心。

第三方服务只返回 version 2 类型化断言：issuer、稳定 subject、subject type、认证方式、强度、时效、
账户状态、revision 和有界 external groups。`flowie-control` 随后把稳定 subject 映射为当前 Domain
中的本地 principal，并再次从本地 Repository 检查 user enabled、Role、Group 和 ACL policy version。
external groups 只是映射输入，不自动获得本地 ACL 权限。请求中的
`peer_certificate_sha256` 是 Flowie MQTT listener 已验证的客户端证书指纹；未启用 MQTT mTLS 时该字段
为空。第三方服务不得把它与 Broker 调用 `flowie-control` 时可选的服务证书第二因子混为一谈。

第三方 HTTPS authenticator 不进入上述 worker executor；它继续在 CoroNet coroutine 上执行异步
TLS/HTTP I/O，并由自己的 `max_in_flight` 与 `timeout_ms` 形成独立舱壁。

### 二选一与失败语义

选择规则由配置决定：

| `auth.enabled` | `auth.external_https` | 行为 |
|---|---|---|
| `false` | 不允许启用 | 不注册 Broker Auth/ACL 服务 |
| `true` | 未配置 | 本地 Auth |
| `true` | 已配置 | 第三方 HTTPS Auth |

同一次认证不会并行或级联两个 verifier。第三方拒绝、超时、TLS/协议错误或依赖不可用时直接 fail
closed，不回退到本地密码。本地 Auth 失败时也不会尝试第三方服务。

## ACL 边界

ACL 规则由 `flowie-control` 的管理命令定义。规则在 Repository 中按稳定 ordinal 逐行存储，但发布是
一个事务命令：

1. 校验完整 draft、主体引用、Domain、规则语法、MQTT filter 和容量；
2. 冻结完整规范规则行集合；
3. 推进单调 `policy_version`；
4. 写入审计并一次提交。

Broker 不读取这些表。`GET /v4/acl` 从 Repository 的只读事务快照返回一个完整 bundle：

```json
{
  "version": 3,
  "policy_version": 42,
  "expires_at": 4102444800,
  "rules": [
    "allow|role|mqtt-user|root-a|publish,subscribe|mqtt_topic|adapter|root-a/#"
  ]
}
```

Domain 不由 query/header 提供，而是由 Bearer token 命中的 `service_bindings` 唯一解析。可选
证书指纹只是该 binding 的第二因子。可选 `X-TurboFlow-Policy-Version` 请求一个精确正版本；
不存在时返回 404，不静默返回其他版本。

Broker 使用公共严格 parser 把完整 bundle 编译为两层不可变索引：

- 通用结构索引：
  `domain -> action -> resource_type -> subject_kind/subject -> pattern`；
- MQTT adapter 索引：按 topic filter 编译的 trie/有界候选结构。

数据库中“逐行存储”不等于 Broker “逐条查询”。Broker 每次只接受完整 bundle，并原子替换 snapshot；
读者只会看到完整旧版本或完整新版本。稳定版本的 PUBLISH/SUBSCRIBE 不访问 HTTPS 或数据库。

## Broker HTTPS 契约

Bundled `flowie_server` 只注册 HTTPS Auth/ACL factory：

```yaml
channels:
  mqtt.auth-service:
    kind: auth_provider
    config:
      backend: https
      url: https://flowie-control.internal/v4/authenticate
      method: password
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      tls:
        ca_file: C:/certs/control-ca.pem

  mqtt.acl-service:
    kind: acl_provider
    config:
      backend: https
      url: https://flowie-control.internal/v4/acl
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      max_response_size: 16777216
      max_rules: 4096
      tls:
        ca_file: C:/certs/control-ca.pem
```

URL 必须使用 HTTPS 并包含明确 path；userinfo、query、fragment、redirect 和自动 retry 被拒绝。service
token 和加密私钥密码只能通过 key provider reference 注入。

认证使用 `POST /v4/authenticate` version 3 JSON。请求精确包含 `version`、`identity`、`method`、
`secret_base64`、`protocol`、`remote_address` 和 `peer_certificate_sha256`。ACL 使用
`GET /v4/acl` version 3 JSON。两者可以共享
同一作用域服务凭证和轮换机制，但服务端分别执行最小权限检查；ACL 接口不接收客户端 credential，Auth 接口
不返回 ACL rule body。

`remote_address` 来自 endpoint 验证过的 transport provenance：TCP/TLS/WS/WSS 默认使用直接 socket
peer 的数值 `IP:port`，Pipe 使用 `local`。TLS/WSS listener 可显式要求 trusted PROXY v1/v2；只有直接
peer 命中配置的数值 CIDR 时才在 TLS 前消费 header，并用其中的源地址覆盖该请求字段，同时单独保留
transport peer 用于诊断。缺失、畸形、超限、超时或未信任输入全部 fail closed。Flowie 不解析
`X-Forwarded-For` 或其他 HTTP 代理 header。

MQTT TLS/WSS endpoint 只有在配置 `tls_client_ca_file` 时才要求客户端证书。CoroNet 完成证书链验证后，
Flowie 才读取规范小写 `sha256:` 指纹并传给 auth provider；验证失败在认证请求发出前 fail closed。
该字段由安全 ABI 追加字段承载，provider 必须按请求 `size` 判定是否可读。旧尺寸调用方仍可被接受，
但不会携带证书上下文。

## `flowie-control` 配置

公共部分：

```yaml
auth:
  enabled: true
  listener_id: flowie-control-auth
  method: password
  local_executor:
    workers: 4
    queue_capacity: 128
    deadline_ms: 10000
  service_bindings:
    - service_id: broker-main
      token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      domain: root-a
```

上面即为本地 Auth 配置。`workers` 范围为 `1..64`，`queue_capacity` 为 `1..4096`，
`deadline_ms` 为 `1..60000`；缺失整个 block 时使用上面的默认值。需要第三方 Auth 时，删除显式
`local_executor` block，并在同一块增加：

```yaml
  external_https:
    url: https://third-party-auth.internal/v2/assert
    service_token_ref: env://FLOWIE_THIRD_PARTY_AUTH_TOKEN
    trusted_issuer: https://identity.internal
    subject_type: device
    timeout_ms: 3000
    max_response_size: 16384
    max_in_flight: 64
    tls:
      ca_file: C:/certs/third-party-ca.pem
      client_cert_file: C:/certs/flowie-control-client.pem
      client_key_file: C:/certs/flowie-control-client-key.pem
```

外层 token 保护 Broker 到 `flowie-control` 的 `/v4/authenticate` 与 `/v4/acl`；内层 token 保护
`flowie-control` 到第三方 assertion 服务。它们信任方向和权限不同，不能复用。

## Repository 与数据库

`flowie_control_repository_t` 是内部、版本化 persistence port。Auth service、ACL endpoint、management
service 和 Dashboard 只依赖该 port，不执行或解释具体数据库 SQL。

- SQLite provider：当前完整实现，是默认控制事实源。
- PostgreSQL provider：使用专用关系 schema，不使用 FlowStore Record 模型。schema/migration/TLS
  与有界连接池、完整 query/command operation table 和公开配置 factory 已实现。池中
  每个 `PGconn` 只允许一个独占租约，满载限时等待；关闭先拒绝新租约并唤醒等待者，再等所有租约
  归还。归还时清理未结束事务，连接只有重新建立 TLS/session 并验证 schema 后才能再次进入可用
  容量。principal snapshot 使用只读可重复读事务；credential 验证不在持有数据库租约时执行 KDF，
  并在 KDF 后重新读取 generation 以拒绝并发 rotate/revoke。写事务的 `COMMIT` 响应丢失时，
  provider 从新租约读取同一事务提交的 audit，按完整命令身份和 revision 确认结果；不能确认时
  fail closed，不把未知状态猜成成功。明确提交成功后的连接回收故障不会覆盖业务成功。
- 每个运行实例只选择一个 Repository。禁止 SQLite/PostgreSQL 双写，也禁止数据库失败后切换事实源。

第三方管理系统不得直接修改控制数据库。它先通过 HTTPS 登录取得有界 session，再通过 session bearer
或 Secure/HttpOnly/SameSite cookie 调用受领域 RBAC 保护的 JSON-RPC 操作
用户、credential、Role、Group 和 ACL；这样 revision、引用校验、发布原子性与审计不会被绕过。

空 Repository 的首次启动是唯一例外入口，但不绕过领域边界：身份固定为 `system/admin`，公开初始密码
固定为 `Flowie@ChangeMe!`。`flowie-control` 使用 Repository command/audit 依次创建 Domain、用户、
credential、`system_admin`/`password_change_required` Role 和 assignment。固定 request ID/revision 允许
中断后幂等重放；Repository 含无关状态时 fail closed。首次认证期间有效权限只包含改密；改密事务移除
限制角色并撤销当前 session，之后 `system_admin` 才成为有效权限。

## 并发、缓存与失效

- Auth 缓存只保存 keyed digest 与不可变 principal snapshot，并复核 user/credential/store revision 与
  policy version。
- 本地管理登录的 Argon2id/Repository 读取运行在独立有界 executor；owner lane 只解析请求并接收结果。
  队列满或 deadline 到期显式失败，超时后才完成的登录会撤销其刚签发的 session，不能留下孤儿凭据。
- 第三方 HTTPS Auth 使用有界 `max_in_flight`，满载时在读取 token和网络 I/O 前返回 busy。
- SecurityRealm 在锁外拉取和编译 bundle，只在替换 snapshot 时短暂持锁。
- 没有 snapshot、版本不匹配、bundle 过期、服务不可达或响应无效时全部 fail closed。
- 远端失败可以继续使用“版本匹配且未过期”的现有 snapshot；过期后不得继续授权。

## 安全与部署

- Broker 出站 ACL 只允许 `flowie-control` 和必要 DNS；不得访问控制数据库或第三方身份库。
- `flowie-control` 到第三方 Auth 的出站权限只在启用外部模式时开放。
- 所有链路必须使用 TLS 并验证主机名与服务端证书链。Broker-facing listener 默认不请求客户端证书；
  高安全部署可显式要求客户端证书，但作用域 bearer 仍是必需身份因子。
- token、credential、Base64 request 和临时 KDF 数据在生命周期结束前清零。
- timeout、header、body、rule count、group/role 数量和 cache/in-flight 容量都有硬上限。
- 固定默认账号和公开初始密码只允许进入首次改密流程；不提供匿名 fallback、可配置默认身份或手工改库
  bootstrap。完成 bootstrap 的 Repository 重启时只验证结构，绝不恢复默认密码。

## 兼容性与迁移

这是产品组合的有意收敛：

- 删除 bundled Broker 的 SQLite ACL factory 与链接依赖；独立 SQLite security 模块只保留内部迁移和
  contract test 用途。
- Broker Auth 收敛为严格 v3，ACL 保持严格 v3，第三方 assertion bridge 收敛为严格 v2；旧 Auth v2、
  assertion v1、旧 tenant、旧 bundle 版本和 YAML 内嵌 rules 均不解析。
- 恢复 `flowie-control` 本地 Auth 为正式可选 verifier；配置 `external_https` 时仍严格替换本地路径，
  没有失败 fallback。
- 配置 version 1 不再接受旧 `bootstrap` block；相同 `system/admin` 数据可重放校验，其他旧 bootstrap
  身份必须在升级前通过旧版本管理 RPC 迁移，否则新版本 fail closed。

迁移时先部署支持 Auth v3 和 assertion v2 的 `flowie-control`/第三方 bridge，再以同一维护窗口升级
Broker，并在控制面 Repository 发布完整 v3 bundle。协议不自动降级；回滚必须成组恢复旧
Broker/control/bridge 进程和旧数据库备份。当前 Broker 不会重新接受 SQLite ACL 配置。

## 已验证与未完成

已验证：

- 本地与第三方 Auth 配置选择；
- SQLite 固定管理员 bootstrap、首次改密权限门、幂等重放、不恢复默认密码、非空库拒绝及启动顺序；
- PostgreSQL `verify-full` 一次性管理员 bootstrap live gate；
- Repository bundle 当前/精确版本读取及不存在版本；
- 真实 TLS listener 上不带客户端证书的 `/v4/authenticate`、`/v4/acl` 成功，错误 token 403、
  版本不存在 404；另有证书第二因子匹配/拒绝单元测试；
- Broker HTTPS-only 产品构建、secure config check，以及注入 MQTT matcher 后真实 MQTT 5/TLS
  Auth/ACL allow/deny smoke gate；
- 第三方 Auth 的严格 JSON、动态 token、mTLS、超时、限流和无 fallback。

生产开放仍需要：

- SQLite/PostgreSQL 全量共享 contract、并发/live/failure gates；
- 自动化 bundled 双进程回归、连接/请求级限流、证书轮换、备份/PITR/HA 和单写者策略；
- Auth/ACL 压力测试、故障注入、ASan/UBSan、threat model 与运维 runbook。
