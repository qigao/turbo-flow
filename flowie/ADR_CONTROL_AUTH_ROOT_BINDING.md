# ADR：认证服务以受信 TLS 调用方绑定 Root Group

## 状态

已接受。内部服务核心、CoroNet/Iris 的显式服务端 mTLS 配置、已验证 peer certificate fingerprint 查询、
Flowie 的薄 Iris 身份适配层和严格 `/v2/authenticate` JSON handler 均已实现；独立控制面进程尚未开放。

## 背景

Flowie 允许不同 Root Group 使用相同 `principal_id`。Broker 的 HTTPS `/v2/authenticate` 请求当前只包含
MQTT identity、method、secret、remote address 和 protocol，不能仅凭 identity 在控制数据库中全局搜索用户。
让 MQTT 客户端在请求中声明 Root Group 会把隔离根交给不可信输入，无法满足 fail-closed 边界。

现有 Broker HTTPS provider 支持验证服务端证书并携带客户端证书。CoroNet/Iris 现已提供版本化服务端 TLS 配置、
强制客户端证书验证和请求级 verified peer certificate SHA-256 查询；Flowie adapter 只从该传输接口构造
`flowie_control_verified_caller_t`。控制面网络入口仍由 feature flag 隐藏，直至严格 JSON handler、进程生命周期、
限流和真实网络集成 gate 完成；不得使用 HTTP header、Bearer token 或 JSON 字段冒充 mTLS 身份。

## 候选方案

1. MQTT 请求携带 `root_group`：实现简单，但客户端可控，拒绝。
2. 认证服务按 identity 全局搜索：同名主体存在歧义，也可能形成跨 Root Group 探测，拒绝。
3. 由受信 listener 和已验证客户端证书身份精确绑定 Root Group：选择。

## 决策

- 认证服务内部使用 `(listener_id, peer_certificate_sha256)` 的不可变精确映射确定唯一
  `root_group_id`。
- certificate fingerprint 必须是规范化的小写 `sha256:` 加 64 位十六进制文本；不接受 wildcard、subject
  substring、大小写自动修复或默认 Root Group。
- `flowie_control_verified_caller_t` 只能由 TLS listener adapter 在证书链验证成功后构造。HTTP header 和
  request body 永远不是该结构的输入来源。
- 未验证证书、未知 listener/fingerprint、重复绑定和方法不匹配全部 fail closed。
- 认证成功后，user、credential revisions、roles 和 effective groups 在一个 SQLite read transaction 中生成一致
  snapshot。credential cache 仍只缓存正向 KDF 结果，不保存明文 secret。
- `policy_version` 由注入的只读 provider 提供；返回零或 provider 失败时不签发 principal。
- principal expiry 有界，默认 300 秒、最大 3600 秒。该 TTL 不替代已连接 session 的撤销策略。

## 状态归属与失败语义

- Root binding 配置是认证服务启动时复制的不可变状态；更新需要构建新实例并切换，不做双向同步。
- SQLite control store 是 user、credential、roles 和 groups 的唯一事实源。
- ACL publisher 是 `policy_version` 的事实源；认证服务只读取，不自行推进版本。
- credential cache 和 principal response 都是派生状态，任何事实源读取失败均不返回部分 principal。
- authenticate 输出由调用方持有；服务不保留请求 secret 或输出 principal。

## 架构与兼容性影响

- Broker `/v2/authenticate` JSON body 不增加 Root Group 字段，现有 v2 request contract 保持不变。
- Flowie 新实现仅位于不安装的 `flowie_control_core` 与 `flowie_control_iris_adapter`，没有 executable、listener、
  YAML 或已安装公开头文件变化。
- CoroNet/Iris 提供版本化 server mTLS 配置和 verified peer identity API；Flowie 薄 adapter 只依赖该公开传输
  契约，不直接访问 OpenSSL 内部对象。JSON endpoint 只通过显式注册进入 Iris app，网络开放仍需独立控制面
  进程、限流和真实集成 gate。
- 迁移时为每个受信 Broker client certificate 配置精确 binding；证书轮换期间可并列配置新旧 fingerprint，完成
  切流后删除旧 binding。
- 回滚只需停止未开放的 control service，Broker 恢复原认证服务；不会修改现有 MQTT session 或 ACL bundle。

## 验证范围

- 两个 Root Group 中相同 MQTT identity 使用不同 credential，不发生跨 Root Group 认证。
- verified/unknown/unverified certificate、重复 binding 和非规范 fingerprint 拒绝。
- roles/effective groups/root/policy version/expiry 组装正确。
- credential cache 命中、credential revoke、policy provider 失败和零版本 fail closed。
- CoroNet 已覆盖真实双向 TLS 握手和双方证书 fingerprint；Iris/Flowie 已覆盖非 TLS 请求、严格 JSON、Bearer
  token、原始 body/Authorization wipe 和 endpoint bind/unbind fail-closed。证书轮换、缺少客户端证书的 Iris
  端到端拒绝、限流和 `/v2/authenticate` 真实网络测试仍属于 release gate。
