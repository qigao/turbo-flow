# ADR：控制面 Credential 的生成、验证与轮换

## 状态

已采纳，当前适用于 `flowie/control` 内部 SQLite 事实源。Credential 生命周期已通过内部 management RPC
暴露为 `flowie.credential.generate/rotate/revoke`。未配置 `auth.external_https` 时，这些 credential
是本地 Auth 的 verifier 事实；配置外部 Auth 时不参与认证。`flowie-control` 不增加 OIDC listener
或第三方数据库 auth backend。

## 背景

控制面需要动态创建和轮换机器 credential，但数据库不能保存可直接用于登录的明文，也不能使用适合
普通数据校验的快速 hash。密码 KDF 消耗较高，若在 SQLite 写事务内执行，会长时间占用单写者锁；若在
事务外执行后不重新校验状态，又可能覆盖并发的禁用、撤销或轮换。

本决策只定义 credential 事实的存储和内部命令语义。内部认证缓存和受信 Domain 绑定见本文后续章节及
`ADR_HTTPS_AUTH_SERVICE.md`；HTTPS adapter、防爆破、管理权限和已连接 session 的处置仍由后续
阶段完成。部署 parser/runtime 以 `auth.external_https` 是否出现选择 verifier：缺失时使用本地
credential 与正向 cache；出现时二者不参与该请求，第三方 HTTPS 失败不得回退到本地 credential。

## 候选方案

1. 保存明文或可逆密文：便于重放 secret，但数据库或密钥泄漏会直接暴露全部 credential，拒绝。
2. SHA-2 等快速 hash：实现简单，但离线猜测成本过低，拒绝。
3. OpenSSL PBKDF2：依赖已存在且适用于部分合规环境，但默认内存硬度不足。
4. 复用仓库 Monocypher Argon2id，并用 TurboUtils CSPRNG 生成 Token entropy 和 salt：不新增依赖，提供内存
   硬度，并保持成熟密码实现边界。

选择方案 4。若未来需要 FIPS profile，应新增明确、版本化的 KDF algorithm，而不是静默切换现有记录。

## 数据与所有权

SQLite `flowie_control_credential` 是 verifier 事实源，并以 `(domain_id, principal_id)` 为主键关联用户。
每条记录分别保存：

- KDF algorithm、memory blocks、passes 和 lanes；
- 16-byte salt 和 32-byte verifier；
- enabled、revision 和时间戳。

数据库不保存生成的明文 Token。机器 Token 由 32-byte entropy 编码为
`flw_mqtt_v1_<Base64URL-no-padding>`；`generate` 和 `rotate` 仅在事务提交成功后把 Token 返回给调用者一次。
幂等重放返回 `TURBO_EALREADY` 和空 Token，因为系统无法也不应恢复原明文。调用方拥有返回 buffer，必须
在持久化到受保护的 secret store 后调用 `flowie_control_generated_credential_wipe()`。

默认参数为 Argon2id、19 MiB、2 passes、1 lane。读取记录时严格校验 algorithm、参数范围和 BLOB 长度；
损坏或未知记录返回协议错误，不做弱 KDF fallback。参数下限参考 OWASP Password Storage Cheat Sheet，
算法和参数语义参考 [RFC 9106](https://www.rfc-editor.org/rfc/rfc9106.html) 与
[OWASP Password Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)。

## 写入与并发

`generate`/`rotate` 分为三个阶段：

1. 只读预检 request replay、expected revision、用户状态和 credential 存在性；
2. 在数据库事务外使用 CSPRNG 生成可打印 Token，并用 Argon2id 生成 salt 和 verifier；
3. `BEGIN IMMEDIATE` 后重新检查 replay、revision、用户和 credential 状态，再原子写入 revision、audit 和
   verifier。

因此昂贵 KDF 不持有 SQLite writer lock；并发状态变化会在第二次校验时失败，不会覆盖新事实。失败路径
回滚事务并清零所有临时 entropy、Token、salt、verifier 和 KDF work area。`revoke` 在单事务内 tombstone 当前
credential；`rotate` 可用新 verifier 显式重新启用已撤销 credential。

## 验证与错误语义

- secret 长度必须在 `1..4096` bytes，其他外部标识继续使用 Security ABI 的有界长度。
- verifier 比较使用 constant-time primitive。
- 不存在的用户或 credential 仍执行一次默认 Argon2id dummy derivation，最终统一返回 `TURBO_EPERM`，以
  减少明显的主体枚举时序差异；这不是完整的恒定时间保证。
- KDF 完成后重新读取用户和 credential revision。若期间发生禁用、撤销或轮换，验证 fail closed。
- 数据库错误、损坏 KDF 字段和随机源失败向上传播，不自动生成弱 secret、不切换算法。

内部 store API 不记录 credential，也不把 Token 写入 audit detail。management RPC 仅在成功的 generate/rotate
响应中返回一次 `token`；重放返回专用冲突错误且不恢复明文，RPC 响应设置 `Cache-Control: no-store`，
实际发送后立即清零响应副本。Dashboard 不渲染 credential。内部 auth service 已实现有界
revision-aware credential cache、事务化 principal snapshot 和受信 TLS caller Domain 绑定；外层 HTTPS
adapter 仍必须实现请求限流、失败审计和统一拒绝响应。

## Revision-aware 正向缓存

`flowie/control` 提供内部 positive credential cache，仅用于本地 verifier 的内部测试与迁移工具。
缓存不属于 Broker HTTPS provider，也不能通过 controller 部署 schema 选择；企业第三方路径不读取
该缓存。

- 缓存键是进程启动时由 TurboUtils CSPRNG 生成的 32-byte 随机 key 所派生的 keyed BLAKE2b digest；输入
  包含有长度边界的 domain、principal 和 credential。缓存不保存 credential、Base64 或可恢复明文。
- 缓存只保存成功验证得到的 user/credential revision，不缓存失败结果，不成为用户状态事实源。
- 每次候选命中都通过轻量 SQLite query 读取当前 active 状态和 revision。禁用、撤销或 revision 不一致时
  立即删除候选；revision 不一致时重新执行当前 verifier 的完整 Argon2id 验证。
- 默认容量 256、TTL 5 秒；硬上限为 4096 entries 和 60 秒。达到容量时驱逐最久未使用 digest；过期项
  在访问或容量驱逐时清理，内存不会无界增长。
- TurboUtils mutex 只保护 hash map、LRU sequence 和缓存 entry。SQLite、Argon2id 和外部 I/O 均在锁外
  执行；cache destroy 要求调用方先停止并发认证。

`flowie-control` 的本地 HTTPS Auth composition root 还把 Argon2id、SQLite 与同步 PostgreSQL 调用
放入专用有界 executor。每个任务拥有解码字段和 secret 副本；Iris `Req`/`Res`/socket 不跨线程。
队列满返回 429，HTTP deadline 返回 503。同步工作不做不安全的强制取消：迟到结果丢弃、secret 由任务
结束时擦除，shutdown 停止接单并 drain。第三方 HTTPS verifier 不使用该 executor。

当前缓存只消除重复 Argon2id，不缓存 roles、effective groups 或最终 principal。未来认证服务应在成功
验证后从事实源读取授权身份视图；在 ACL draft/policy version 完成前，不得用全局 control revision 冒充
`policy_version`。

## 迁移与回滚

这是新增内部表和内部 API，不改变现有 Flowie 配置或公开 Broker ABI。已有控制数据库在打开时创建新表；
未生成 credential 的用户保持无凭据状态。

回滚到不含该表的旧二进制时，旧代码不会读取新表，但数据库中 verifier 事实仍保留。若未来修改 KDF
格式，必须增加 algorithm/version 并提供显式迁移；不得原地改变 Argon2id 参数含义。删除表或迁移用户
数据属于破坏性操作，不在自动回滚范围。

## 验证范围与剩余风险

当前 TinyTest 覆盖一次性 Token 生成、Base64URL alphabet、正确/错误/不存在主体验证、幂等重放、轮换使旧
Token 失效、撤销、重新启用、用户禁用和 Token wipe。缓存测试覆盖正向命中、TTL 过期、错误 credential
不入缓存、revision
失效、撤销/禁用 fail closed、LRU 容量驱逐和并发命中。SQLite revision 与 audit 同事务提交。

- **HIGH**：在 HTTPS 认证服务、防爆破和管理权限完成前，不得把内部 store API 直接暴露到公网。
- **MED**：credential 正向缓存、事务化 principal snapshot 和内部 auth service 已完成；最终 principal
  cache、Iris/CoroNet server mTLS identity 接入和已连接 session 的撤销策略尚未实现，不能据此宣称即时
  全局注销。
- **MED**：发布 gate 仍需真实并发轮换/验证压力测试、故障注入和 ASan/UBSan 验证。
- **LOW**：默认 KDF 参数为当前基线；部署前仍应在目标硬件上测量延迟和内存预算，并通过显式控制面配置
  管理后续参数升级。
