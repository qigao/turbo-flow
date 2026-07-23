# ADR：Flowie 仅通过 HTTPS 认证服务建立身份

## 状态

已采纳，适用于 bundled `flowie_server` 的认证边界。

## 背景

认证数据可能来自 Redis、SQLite、PostgreSQL 或任意专用数据库。若 Flowie 为每种数据库注册认证
provider，数据库地址、凭据、schema、KDF 与连接生命周期会穿透产品边界，并容易与 TurboFlow 的
Graph data adapter 或 session record-store backend 混淆。它还会扩大网络可达面：broker 一旦
被攻破，就可能直接访问身份数据库。

本决策只处理 CONNECT 阶段的身份认证。SecurityRealm 使用本地不可变 ACL 快照执行授权；快照的
动态加载与发布由 `ADR_DYNAMIC_ACL_BUNDLE.md` 规定，不属于 credential 认证接口。

## 候选方案

1. Flowie 直接访问每种认证数据库：延迟较低，但数据库协议、schema、秘密与迁移逻辑进入 broker，
   攻击面和部署耦合最大。
2. 可插拔数据库认证 provider：隔离部分代码，但仍允许数据库对 Flowie 网络可达，配置名称也容易与
   Graph data adapter 或 session record-store backend 混淆。
3. Flowie 只访问 HTTPS 认证服务：服务内部自行选择数据库；Flowie 只依赖一个版本化网络契约。

选择方案 3。

## 架构与状态归属

- 认证服务拥有凭据、密码散列、账户状态、身份映射与数据库事务，是认证事实源。
- Flowie 仅在 CONNECT coroutine 中发送一次有界认证请求，并接收 principal、roles、groups、scope、
  expiry 与 policy version。
- ACL provider 拥有版本化 policy 事实源，SecurityRealm 拥有其本地不可变快照。认证完成后的
  connect/publish/subscribe 授权只读取 principal 和本地快照，不在消息热路径访问认证服务或数据库。
- Redis/PostgreSQL 可通过各自 Graph adapter 处理业务数据，Redis/PostgreSQL 可作为 session
  record-store backend；这些用途都与认证服务内部数据库完全分离。

```text
MQTT client -> Flowie CONNECT coroutine -> HTTPS auth service -> private credential database
                         |
                         +-> validated principal -> local SecurityRealm ACL snapshot
```

## 接口契约

配置只接受：

```yaml
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

不接受数据库 host、port、database、schema、key 或 literal service token。URL 必须是 `https`，包含
明确 path，且不允许 userinfo、query、fragment。认证请求使用 `POST application/json`，协议版本为
2，二进制 credential 使用 Base64 字段传输。成功响应必须为 `200 application/json`，包含版本 2、
`authenticated: true` 和完整 principal；未知字段、越界数组、错误 method、零 policy version 或无效
scope 均视为协议错误。principal 使用必填 `root_group` 和 `scope: root_group`，effective `groups` 必须
包含该 Root Group；版本 1、`tenant` 和 `scope: tenant` 均作为协议错误拒绝。

401/403 表示拒绝，429 表示服务繁忙，其他状态、解析错误和网络错误均 fail closed。Flowie 不重试、
不跟随重定向、不切换 backend，也不匿名放行。

## 异步与并发

认证 ABI 保持同步返回错误码，但只允许从运行中的 CoroNet coroutine 调用。TurboHTTP 的 DNS、connect、
TLS、send 和 receive 在等待时 yield，因此不会阻塞 owner-lane 线程中的其他 coroutine。每次认证创建
独立 HTTP client，避免共享 client 的可变认证 header、cookie、重试或连接池状态跨请求污染。

该实现不把请求送入额外线程池：数据库/KDF 已在认证服务侧隔离，再增加本地 worker queue 会引入
连接生命周期、取消与 use-after-free 状态机，而不能减少远程 TLS/服务延迟。

## 安全要求

- 认证数据库只能被认证服务访问，不得暴露到 Flowie 网段或公网。
- Flowie 出站网络 ACL 只允许认证服务目的地址和必要 DNS；认证服务入口只允许 Flowie 身份/网段。
- TLS 始终校验配置主机名和证书链。未配置 `tls.ca_file` 时使用系统信任库；私有 CA 可按 provider
  注入。`client_cert_file` 与 `client_key_file` 必须成对配置，缺失任一项会在 listener 启动前失败。
  加密私钥密码只能通过 `client_key_password_ref` 从 key provider 获取；不得作为 YAML literal。
- service token 通过 key provider 获取，每次请求重新获取以支持轮换；不得写入 YAML、日志、状态文档
  或 graph message。token、Base64 credential 与序列化 request 在释放前清零。
- timeout、secret、response body 和 response header 均有硬上限；禁止 redirect 和 retry。
- 认证服务应限制请求速率、记录不含 credential 的审计事件，并对重复失败实施服务端防爆破策略。

## 兼容性与迁移

这是有意的配置/API 收缩：`backend: redis` 认证 channel、Redis auth factory、Redis verifier-record API
被移除，没有 fallback。普通 Redis data/blob/record/session provider 不受影响。

Security ABI 已升级为唯一 v3；HTTPS authentication body contract 仍为唯一 v2，旧响应不会被自动转换。Flowie
持久 session record 同步升级为唯一 v3，避免把旧 tenant 字段误解释为 Root Group。

HTTP auth/ACL provider C ABI v2 在结构体尾部追加 `tls`；精确使用 v1 尺寸的旧调用方仍可运行，但不能
配置 per-provider CA 或客户端身份。新配置解析严格拒绝未知、非字符串和不完整的 TLS 字段。

迁移步骤：

1. 将现有 credential schema、KDF 与账户规则迁到独立认证服务。
2. 只在认证服务网络内开放数据库，并撤销 Flowie 的数据库凭据和访问规则。
3. 为 Flowie 配置 HTTPS URL 与 service-token reference，安装私有 CA 到系统信任库。
4. 验证 HTTPS 成功/拒绝、证书错误、超时、429、畸形响应、服务不可达和 token 轮换。
5. 最后删除旧数据库认证配置。

回滚只能回到“不启用远程认证的旧产品版本/部署”；当前版本不会恢复数据库直连或自动 fallback。

## 验证范围

- 配置测试：HTTPS 配置通过；HTTP、数据库字段、未知字段和无效 secret reference 被拒绝。
- 生命周期测试：factory/owner 创建销毁、token lease 获取/释放、非 coroutine 调用拒绝。
- 产品检查：`flowie_server_check_https_auth_provider` 验证 composition root 只注册 HTTPS backend。
- 发布前仍需在受控环境做真实 TLS 认证服务集成测试，包括证书链、主机名、超时与失败状态。配置测试
  不能替代真实握手和服务契约测试。
