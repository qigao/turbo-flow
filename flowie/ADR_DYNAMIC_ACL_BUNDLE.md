# ADR：动态 ACL bundle 与本地授权快照

## 状态

已被 [ADR_HTTPS_AUTH_SERVICE.md](ADR_HTTPS_AUTH_SERVICE.md) 取代。以下内容只记录旧 bundle
方案，不是当前 Broker/Control 接口契约；现行实现使用 `POST /v4/acl/check` 返回逐请求 decision。

## 背景

用户、角色与 ACL 会动态变化。若把 ACL 规则写入部署 YAML，每次变更都需要修改配置并重启，且同一
策略会在配置、内存和外部管理系统中形成多个事实源。若 Flowie 在每次授权时直接查询 HTTP 或数据库，
则网络、数据库故障和延迟会进入 MQTT 消息热路径，数据库凭据与 schema 也会穿透 broker 边界。

本决策把身份认证、ACL 控制面和 ACL 执行面分开。认证服务管理用户与凭据；ACL provider 提供版本化
规则；SecurityRealm 只执行经过验证的本地不可变快照。

## 候选方案

1. ACL 存在 YAML：部署简单，但不支持安全的动态管理，YAML 会成为第二事实源。
2. 每次授权远程查询 HTTP/数据库：规则始终新鲜，但把外部 I/O 放入消息热路径，故障面和吞吐代价最大。
3. provider 拉取版本化 bundle，realm 原子替换本地快照：控制面可动态更新，数据面保持本地判定。

选择方案 3。YAML 中的 `rules` 和 `policy_version` 被拒绝，不提供兼容 fallback。

## 架构与状态归属

```text
credential -> HTTPS auth service -> principal(policy_version)
                                      |
                                      v
Flowie -> SecurityRealm -> local immutable ACL snapshot -> allow/deny
                |
                +-> exact policy_source -> HTTPS ACL provider
```

- 用户凭据、账户状态和身份映射只属于 HTTPS 认证服务，Flowie 不保存用户密码。
- 每个 `security_realm` 只声明资源身份和 `policy_source`；它不拥有可写规则。
- bundled Broker 的 `policy_source` 只接受 HTTPS provider。ACL 事实源位于 `flowie-control`
  Repository，Broker 不读取其数据库。
- realm 中的快照是事实源的有界派生缓存。bundle 包含单调递增的 `policy_version`、可选
  `expires_at` 和完整规范规则行数组；加载时由 re2c parser 编译为不可变查询索引。
- 查询索引按 `domain -> action -> resource_type -> subject_kind/subject -> pattern` 编译。
  Domain、subject 与 exact pattern 使用哈希查找；prefix 按请求资源的每个前缀探测哈希叶，复杂度
  受资源长度约束而不随同 subject 的 prefix 规则数增长。协议 adapter 只扫描已命中 subject 叶内的
  有界候选，不扫描其它 Domain、action、resource type 或 subject。显式 deny 仍跨叶优先，
  `matched_rule` 保持原始规则行序号。
- `flowie-control` 在一个 Repository 事务内发布单个 Domain 的规则与版本；读取者只能看到旧
  bundle 或完整新 bundle。Repository 可以由 SQLite 或 PostgreSQL 实现，不改变 HTTPS 契约。
- HTTPS provider 只读取 ACL bundle，不接收客户端 credential。认证与 ACL 可以由同一控制面产品管理，
  但必须使用独立、最小权限的认证接口和策略读取接口。

## 配置契约

Broker 配置：

```yaml
channels:
  acl.main:
    kind: acl_provider
    config:
      backend: https
      url: https://flowie-control.internal/v4/acl
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      timeout_ms: 3000
      max_response_size: 4194304
      max_rules: 4096
```

HTTPS URL 必须有明确 path，禁止 HTTP、userinfo、query、fragment、redirect 和自动 retry。service
token 只能通过 key provider 引用，不能以明文写入 YAML。Broker 配置不接受数据库路径、conninfo 或
schema。

HTTPS 成功响应是严格 JSON：

```json
{
  "version": 3,
  "policy_version": 42,
  "expires_at": 0,
  "rules": [
    "allow|role|mqtt-user|root-a|publish,subscribe|mqtt_topic|adapter|root-a/#"
  ]
}
```

每一行格式为 `effect|subject_kind|subject|domain|actions|resource_type|match_kind|pattern`。
规则行最多 2047 字节；`\\`、`\|`、`\xHH` 可用于字段转义。未知 token、重复 action、非法枚举、
越界字符串、空规则集、超过容量、版本为零或整数溢出均为协议错误。版本 1/2 和 `tenant_id` 字段
不解析，也没有兼容 fallback。

## 刷新、并发与失败语义

- principal 的 policy version 与当前快照不一致，或快照已过期时，realm 才向 provider 请求精确版本。
- provider fetch 在锁外执行。realm 只在获取当前快照引用和原子替换指针时短暂持有 mutex；规则匹配和
  外部 I/O 均不持锁。
- 同一版本的策略内容不可变。已过期 bundle 不能用相同版本替换，管理端必须发布更高版本。
- 没有快照、版本不匹配、bundle 过期、provider 不可达、429、认证失败或响应无效时全部 fail closed。
- provider 是精确选择，不做数据库或 YAML fallback，也不匿名放行。
- realm 销毁必须在 endpoint 停止、并发授权完成之后；provider 生命周期必须长于绑定它的 realm。

该模型不会在每条消息上远程查询。稳定版本的 publish/subscribe 只付出本地快照引用和规则匹配成本；
真实吞吐影响仍需由 Flowie benchmark 持续验证。

## 管理、迁移与回滚

动态管理 API 应发布完整 bundle，而不是逐条修改 realm 缓存。`flowie-control` Repository 的
`policy.publish` 提供事务化命令，再通过只读 `/v4/acl` endpoint 发布结果。

迁移步骤：

1. 从 YAML 删除 `rules` 和 `policy_version`。
2. 创建一个 `acl_provider`，并为 realm 设置唯一 `policy_source`。
3. 在接收生产流量前发布初始 bundle；确认认证 principal 携带相同非零 policy version。
4. 验证允许、显式拒绝、版本升级、过期、provider 不可达和畸形 bundle 均符合预期。
5. 撤销 Flowie 对任何用户数据库的网络权限和数据库凭据。

回滚策略规则时，发布一个更高版本、内容等同于旧策略的新 bundle；禁止降低版本。切换 provider 或 URL
属于部署配置变更，需要停止 endpoint、校验配置并重启，不是热更新。

## 兼容性与验证

- **HIGH**：YAML ACL body 被有意移除；旧配置会启动失败，必须先完成上述迁移。
- **HIGH**：Security C ABI 与 ACL wire/storage bundle 均为唯一 v3，使用规范规则行和必填 Domain。
  旧表保留但 Broker 不读取，必须经控制面重新发布完整 bundle。
- C ABI 追加了 `policy_source`，旧尺寸的程序化静态 realm config 仍被接受；新产品配置应使用 provider。
- SQLite Repository 测试覆盖单调发布、事务快照、Domain 隔离和当前/精确版本读取。
- HTTP 测试覆盖严格配置、非 coroutine 拒绝、bundle 边界和整数溢出；共享 HTTPS transport 的真实
  mTLS 测试要求并验证客户端证书。发布验证仍需覆盖部署证书链、主机名、超时、状态码和 token 轮换。
- Flowie endpoint 测试覆盖 provider 绑定后的 connect/publish/subscribe 授权和默认拒绝。
