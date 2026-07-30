# ADR：Broker 调用方与 Root Group 绑定

## 状态

已被 [`ADR_HTTPS_AUTH_SERVICE.md`](ADR_HTTPS_AUTH_SERVICE.md) 替代。

本文只记录历史决策迁移，不再定义当前配置或安全契约。旧实现曾把 Broker 客户端证书指纹作为
Root Group 的唯一绑定键；当前实现改为作用域 service credential，mTLS 只保留为可选第二因子。

## 保留的不变量

- MQTT 请求 body、query、普通 header 和 MQTT identity 都不能声明或覆盖 `root_group_id`。
- 同一 MQTT identity 可以存在于不同 Root Group，认证查询必须先取得受信 Root Group 范围。
- 未知调用方、重复绑定、作用域冲突、凭据读取失败和认证失败全部 fail closed。
- Broker 不直连账户或 ACL 数据库；认证结果和 ACL bundle 仍通过版本化 HTTPS 契约取得。

## 当前决策

`auth.service_bindings[]` 为每个 Broker 或内部服务定义：

- 唯一 `service_id`；
- 只接受 secret reference 的 `token_ref`；
- 唯一 `root_group_id`；
- 可选的 `peer_certificate_sha256` 第二因子。

请求必须提供 bearer token。`flowie-control` 每次请求从 secret provider 取得当前 token，使用 keyed
digest 匹配唯一 binding，并构造 `flowie_control_verified_caller_t`。Root Group 只来自命中的 binding，
不能来自请求 body。当前 token 发生重复时请求失败，防止轮换期间一个 token 获得多个 Root Group。

默认 listener 使用单向 TLS，只验证服务端身份。若部署显式设置
`listener.tls.client_auth: required`，binding 可同时要求已验证客户端证书指纹；证书不能替代 bearer。
由于当前 listener 不支持“可选请求客户端证书”，该高安全模式只用于关闭 Dashboard 的 service-only
部署，不能作为浏览器或面向用户管理 RPC 的认证方式。

## 管理面边界

Dashboard 和面向用户的 JSON-RPC 不使用上述 service binding，也不从客户端证书映射管理员。
管理员以 Repository 中的账户凭据登录，取得有界服务端 session；后续请求使用
`Secure; HttpOnly; SameSite=Strict` cookie 或同一不透明 token 的 bearer 表示该 session。每次请求重新
检查 user enabled 与当前保留角色，禁用账户或撤销管理角色会立即失权。

## 迁移与回滚

迁移时为每个 Broker 配置独立 token secret reference 和精确 Root Group。需要证书第二因子时，再配置
客户端 CA、证书指纹及 `client_auth: required`；不能复用用户登录凭据或第三方 Auth 上游 token。

回滚必须成组恢复旧 controller、Broker 配置与旧密钥，不允许同时启用证书唯一绑定和 service-token
绑定两套事实源。ACL bundle 与本地账户 Repository 不需要因绑定方式变化而迁移。

## 验证

当前 focused tests 覆盖 token 唯一匹配、动态轮换、重复 token 冲突、错误 token、Root Group 隔离和
可选证书第二因子。真实 HTTPS 集成覆盖无客户端证书的 Broker Auth/ACL、错误 token 拒绝、管理登录
session，以及“客户端证书本身不能授权管理 RPC”。
