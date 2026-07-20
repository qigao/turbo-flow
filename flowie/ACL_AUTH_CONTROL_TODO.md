# Flowie ACL/Auth 控制面交付清单

## 目标与发布边界

目标是交付独立的 `flowie-control` 管理面，统一管理用户、凭据、角色、层级组、ACL 草稿、原子发布和审计。Flowie Broker 仍是数据面，只通过 mTLS HTTPS 调用认证接口并读取版本化 ACL bundle；Broker 不直连认证数据库，Dashboard 和 JSON-RPC 也不得绕过领域命令直接写库。

在本清单的“生产开放门槛”全部通过前，独立进程可以构建、安装并用于 loopback/隔离管理网的受控验证，
但不得暴露到公网或被发布文档声明为生产可用产品。未完成的 bootstrap、限流、HA/migration 等能力不得用
fallback、默认账号或手工改库绕过。

## 状态模型决定

### Group Forest 与 Root Group

- 产品模型不再使用 tenant。每棵 Group 树都有一个不可变 `root_group_id`，它是用户、规则和资源不可绕过的硬隔离根。
- Root Group 无父节点；普通 Group 只有一个显式 `parent_group_id`。节点不能跨 Root Group 引用或移动，不把 `a/b/c` 路径字符串当成继承事实源。
- 当前控制面只允许创建 Root Group 和子 Group，不提供 reparent，因此已发布命令无法制造环。将来若增加 move，必须在同一事务内完成环检测、深度重算、effective-group 容量校验和审计。
- 用户只保存直接 membership；认证服务计算“Root Group + 直接组 + 全部祖先组”的有界闭包。Broker 保持扁平 exact-match 热路径，不在每条消息上遍历树。
- Security ABI v3 最多携带 16 个 effective groups，并要求非 SYSTEM principal 的集合包含自身 `root_group_id`。超过容量必须 fail fast，不得截断。

### 事实源与派生状态

- 控制数据库是用户、credential verifier、角色、group、membership、ACL draft 和审计事件的唯一事实源。
- 已发布 ACL bundle 是从控制数据库事务快照派生的不可变产物；现有 SQLite/HTTPS policy provider 只负责分发和读取。
- Broker 的 principal 与 ACL snapshot 都是有 TTL/version 的派生缓存，不允许反向修改控制数据库。
- 用户/凭据 revision 与 ACL `policy_version` 分开推进。只有授权可见状态变化才推进 `policy_version`；凭据轮换不能伪装成 ACL 版本变化。

## Phase 0：内部领域与 SQLite 事实源

- [x] 建立不安装公开头文件的 `flowie_control_core` 内部 target。
- [x] 用户创建、读取、禁用采用显式 command/query API。
- [x] 每个写命令在一个 SQLite `BEGIN IMMEDIATE` 事务内完成：校验 `expected_revision`、更新事实、推进 revision、追加审计、提交。
- [x] `request_id` 提供幂等重放；相同 ID 的不同命令必须报冲突。
- [x] SQLite 使用 WAL、`synchronous=FULL`、foreign keys、busy timeout 和有界字段。
- [x] TinyTest 覆盖创建/读取、过期 revision、幂等重放、禁用和审计原子性。

验收：内部 target 与测试通过；没有 executable、端口、配置格式或安装产物变化。

## Phase 1：Root Group、Role 与层级 Group

- [x] 增加 Root Group、Group、user-group 事实表，以及 Group create/disable、membership add/remove 领域命令。
- [x] Group 创建在事务中执行同 Root Group、单父节点、自引用和最大深度校验；当前不开放 reparent，从接口上消除环入口。
- [x] 提供有界 effective-group 查询，在 membership 提交前验证完整祖先闭包不超过 16。
- [x] 增加 role、user-role 事实表、Role create/disable、user-role add/remove 领域命令和最多 8 项的有界 effective-role 查询；越界赋权在同一事务回滚。
- [x] 删除采用 disable/tombstone 或显式引用检查，不允许留下悬空 ACL subject。User/Role/Group 已使用 disable/tombstone；Group disable 要求先移除活跃子组和直接 membership，三类主体的 disable 都会在同一写事务内检查 ACL draft 与当前已发布 bundle 的引用。
- [x] ACL draft 引用的 root-group/role/group/principal 在写入及发布事务中重新校验；禁用后遗留的
  draft 会在再次发布时 fail closed，不能生成带悬空 subject 的新 bundle。

已验证：层级继承、跨 Root Group parent/Role 拒绝、自引用拒绝、Group/Role 数量上限、Role/Group tombstone、membership 撤销、Root Group 不可禁用、ACL draft/已发布 bundle 的 subject 引用阻断、幂等命令、revision 原子性，以及并发 writer 的单胜者提交和失败 writer 无部分写入。

## Phase 2：Credential 与认证服务

- [x] 使用 Monocypher Argon2id；algorithm、参数、salt、verifier 分字段存储，不保存或记录明文。参数与并发语义见 `ADR_CONTROL_CREDENTIALS.md`。
- [x] 内部 `generate_credential`/`rotate_credential` 只使用 TurboUtils CSPRNG，明文只返回一次；幂等重放不恢复明文，临时 secret、salt、verifier 和 KDF work area 在释放前清零。
- [x] 实现 `/v2/authenticate` 严格 HTTPS 契约，返回 principal、root group、roles、effective groups、expiry 和 policy version。内部 Iris endpoint 已限制为 `POST`、精确 v2 字段、规范 Base64、Bearer service token、无缓存响应和明确错误映射；endpoint 仍由独立控制面进程显式注册，默认不监听。
- [x] 认证结果缓存使用有界容量、短 TTL 和 revision 失效；缓存键不得包含可恢复的明文 secret。positive credential cache 使用 keyed digest、TTL/LRU 和 credential/user revision 复核；principal snapshot cache 使用 root/principal keyed digest、TTL/LRU，并同时校验 user revision、credential revision、全局 store revision 与 policy version。缓存只保存不可变派生 snapshot，不保存 secret；数据库/revision 读取失败时 fail closed。
- [x] Argon2id/KDF 前执行有界双层 token bucket：按 verified mTLS caller 限制总量，并按 `(caller, root_group, principal)` 限制连续失败；成功只清除 identity failure bucket，不能重置 caller 总量。状态只保存 keyed digest，满容量按 LRU 淘汰，时间回拨/资源错误 fail closed；HTTP endpoint 将 `TURBO_EBUSY` 映射为 429。
- [x] `/v2/authenticate` 的传输身份必须从受信服务身份解析唯一 Root Group；内部服务核心按 `(listener_id, peer certificate SHA-256)` 精确绑定并覆盖跨 Root Group 同名主体测试。CoroNet/Iris 已提供强制 mTLS 和 verified peer identity，Flowie 薄 adapter 只从 `Req` 的已验证传输身份构造 caller；禁止用 header/body 代替。
- [x] 已连接 session 采用 principal TTL 有界传播：到期时完全空闲连接也会主动 fail closed，MQTT 5
  发送 `DISCONNECT 0x87` 后关闭，MQTT 3.x 直接关闭；Enhanced AUTH 必须在到期前完成，成功提交的新
  principal 会替换旧 deadline。当前没有控制面即时 push 撤销，最坏传播时间由当前 principal TTL 决定。
- [ ] LDAP/AD、OIDC/JWT、RADIUS 和 PostgreSQL 只作为认证服务侧 adapter；Flowie Broker 不注册数据库 auth backend。

当前验证：内部 SQLite store 已覆盖正确/错误/不存在主体凭据、禁用、轮换、撤销、幂等重放、revision 二次校验和 secret wipe；positive credential cache 已覆盖命中、TTL 过期、LRU 容量、revision 失效和并发读取；principal cache 已覆盖命中、TTL 过期、LRU 容量、revision 失效和并发读取，auth service 已覆盖组变更导致的 snapshot 失效；rate limiter 已覆盖 caller/identity 双桶、成功 reset、refill、容量和安全配置，以及 auth service 在 KDF 前返回 `TURBO_EBUSY`。内部 auth service 已覆盖受信 listener/certificate Root Group 绑定、跨 Root Group 同名主体隔离、事务化 principal snapshot、policy failure 和 credential revoke。CoroNet 已覆盖真实 mTLS 握手和双方 verified fingerprint；Iris/Flowie adapter 已覆盖非法配置及普通 HTTP 请求 fail-closed；Iris endpoint 已覆盖严格字段、root_group 注入拒绝、非规范 Base64、Bearer token、body/header wipe、principal JSON 和 bind/unbind。Broker 已覆盖 principal 到期时 MQTT 5/3.x 空闲连接主动断开，以及到期前 Enhanced AUTH 替换旧 deadline。证书轮换、管理面 connection limit、真实网络压力 gate 及数据库/网络故障注入仍待完成，因此 Phase 2 尚未整体验收。

验收：正确/错误凭据、禁用、轮换、缓存命中/失效、防爆破、secret wipe、数据库/网络失败 fail-closed 测试通过。

## Phase 3：ACL 草稿、验证与原子发布

- [x] ACL draft 支持 principal/role/group/any subject、root group、action、resource 和 match kind；发布产物
  使用规范规则行。
- [x] 规范规则行由 re2c parser 严格解析，并编译为
  `root_group -> action -> resource_type -> subject_kind/subject -> pattern` 不可变查询索引；exact pattern
  使用直接哈希叶，prefix 按资源前缀探测哈希叶；协议 adapter 在命中的 subject 叶内保留有界候选链。
  授权热路径不分配内存。adapter 的协议编译索引仍待定义 matcher compile/evaluate 生命周期契约。
- [x] `policy.validate` 在只读事务中检查完整 draft、subject/root 引用、规范规则行、MQTT adapter filter
  和规则容量；deny precedence 由不可变 security index 的既有 deny-first 求值语义保证，不维护第二份顺序状态。
- [x] `policy.publish(expected_revision)` 在 `BEGIN IMMEDIATE` 事务内重新验证、连续编号冻结完整 v3 bundle、
  推进独立 `policy_version`/store revision 并追加审计。`request_id` 重放包含 `expires_at` command identity，
  同 ID 不同过期时间按冲突拒绝。
- [x] `turbo_flow_security_sqlite_provider_publish()` 与 HTTPS provider 已切换为 ACL bundle v3 规则行；加载后
  统一编译为不可变索引，不逐条更新 Broker 快照。
- [ ] 远端 OPA/Cedar/GitOps 只能在控制面导入或编译为 bundle，不能进入 PUBLISH/SUBSCRIBE 热路径。

验收：发布前后 snapshot 原子性、旧版本拒绝、过期 bundle、远端失败保持最后有效快照及过期后 fail-closed 测试通过。

## Phase 4：JSON-RPC 管理面

- [x] 使用 caller-owned `rpc_context_t`；显式 app/path binding，server destroy 时逐项 unregister/unbind。
  context 与 management service 必须长于 request handling，关闭顺序仍是先停 app 再 destroy server/context。
- [x] introspection、batch 和全部 notification 已禁用；request body 上限为 64 KiB，方法参数采用精确字段白名单，
  分页最多 100 项。
- [ ] 独立 HTTPS/mTLS listener 和 header/body 配额已落地；仍需完成 connection/rate limit 与真实网络压力 gate，
  不能用 endpoint 单元测试替代。
- [x] 实现 `flowie.system.status`，user get/list/create/disable，credential generate/rotate/revoke，role list/create/disable/assign/remove/effective，
  group list/create/disable/member add/remove/effective，policy status/rule list/put/delete/validate/publish 和 audit list，
  共 27 个方法。credential generate/rotate 仅在成功响应中返回一次 Base64 secret；幂等重放不恢复明文并返回
  `-32010`，Dashboard 不渲染 credential。生产网络仍必须等待独立 HTTPS/mTLS 或受信 OIDC listener gate。
- [x] Iris authenticated 状态只作为第一层检查；`viewer`、`user_admin`、`policy_admin`、`security_admin`
  权限在共享 management service 再次检查，root group 与 actor 由 resolver 注入，body 不能覆盖。
- [x] RPC handler 只使用结构化 JSON parser 做协议映射并调用共享 command/query service，不执行 SQL；审计时间由
  server clock 注入，客户端不能伪造。
- [ ] 首位管理员通过本机 bootstrap 或一次性凭据创建；禁止默认账号和默认密码。

验收：JSON-RPC 2.0 id/error 语义、权限矩阵、通知拒绝、重复命令、请求上限、context 生命周期和并发写测试通过。

## Phase 5：Dashboard

- [x] Iris 使用独立 Mustache 文件渲染 HTML shell 与管理片段；Dashboard 强制使用固定本地版本 HTMX，
  不提供普通表单/PRG fallback，不使用 inline script、运行时 CDN 或 jQuery。模板、CSS、HTMX 与许可证作为
  独立构建资源复制，缺失或模板编译失败时 Dashboard 创建直接失败。
- [x] Dashboard HTTP handler 与 JSON-RPC 共用 command/query service，不解析或回调 JSON-RPC。
- [x] 页面读取 users、roles、groups、policy draft、audit 和 status，并提供 user/group/role 创建与禁用、group
  membership 增删、role assignment 增删、rule put/delete 与 policy publish 表单；全部写入仍经过共享领域命令。
- [x] users/groups/roles/policy/audit 使用各自独立的 keyset cursor；HTMX 分页仅替换 Dashboard 内容片段，
  每个列表翻页时保留其他列表 cursor，支持 More/First 前向导航。
- [x] Dashboard shell、片段、写操作与本地资源都强制 Iris authenticated 状态；片段和写操作额外要求精确的
  `HX-Request: true`。64 字节 CSRF token 常量时间比较、精确表单字段、严格 CSP、no-store、nosniff、
  cross-origin/referrer/permissions policy 和 Mustache 默认 HTML 转义均已启用；HTMX eval、响应脚本执行与
  history cache 已禁用。
- [ ] 当前 Dashboard 使用 verified mTLS fingerprint 映射 caller，并以进程随机 key 派生 CSRF，不签发弱本地
  session；若增加 Cookie/OIDC 会话，仍必须完成 Secure/HttpOnly/SameSite、rotation 与登录限流。
- [x] 客户端交互只使用 vendored HTMX 2.0.9，不引入 jQuery 或自定义客户端状态事实源。

验收：CSRF、XSS、未授权/越权、会话固定、CSP、分页上限和浏览器端关键流程测试通过。

## Phase 6：Broker 身份上下文与企业 adapter

- [ ] 将 CoroNet 验证过的 MQTT TLS/WSS client certificate identity 传给 auth provider；不得信任客户端自报证书字段。
- [ ] 填充可信 `remote_address`，明确代理场景中何时允许使用 PROXY protocol/X-Forwarded 信息。
- [ ] MQTT 5 真正多轮 enhanced auth 支持 SCRAM/challenge/re-auth；内置单次 HTTPS provider 不冒充多轮实现。
- [ ] PostgreSQL control-store adapter 与 SQLite 共享领域事务契约和兼容性测试。
- [ ] OIDC/JWT、LDAP/AD 和 RADIUS adapter 只返回统一 principal，不把第三方类型扩散到 Broker。

验收：mTLS 身份绑定、证书轮换/撤销、源地址欺骗拒绝、enhanced auth 取消/超时以及各 adapter contract tests 通过。

## 生产开放门槛

- [x] 内部 controller startup-options 使用 TurboUtils CMD/DotEnv 选择独立配置文件；优先级固定为
  `CLI > process environment > explicitly selected DotEnv`，不隐式加载当前目录 `.env`，不接受明文 secret。
- [x] 独立 `flowie-control` 进程和 version 1 独立配置已接入；默认绑定 `127.0.0.1:8443`，不与 MQTT listener 共用地址。
- [x] 管理入口只调用显式 host 的 HTTPS listener，并固定要求 mTLS；证书/私钥/client CA 校验失败时不监听且无 HTTP fallback。
- [x] controller schema 的 service token 与私钥密码只接受 `env://...` reference；用户 credential、ACL rule body 与 secret literal 不进入 YAML、RPC 或 Dashboard。
- [ ] 审计包含 actor、request ID、operation、target、revision、结果与时间，且不包含 credential。
- [ ] 明确备份、恢复、schema migration、回滚和 HA 单写者策略。
- [ ] 完成 threat model、权限审查、模糊测试、ASan/UBSan、发布回归与真实 TLS 集成 gate。
- [ ] `CONTROL_GUIDE.md` 与部署 schema 示例已新增；仍需完成其余 release gate 后才能声明生产就绪。

## 兼容性与回滚

- provider 的 YAML 选择方式保持不变，但安全 ABI 与 HTTPS body contract 已有意升级为唯一 v2：`root_group`/`root_group_id` 取代 tenant 字段，不解析 v1。
- SQLite ACL 只读取 `*_v3` 表。旧表不会被删除或读取；必须向 v3 namespace 重新发布完整规则行 bundle，再切换流量。
- Flowie session record 已升级为唯一 v3。旧记录不会被解释为 Root Group 身份；部署升级前必须按发布流程清理或离线迁移旧 session store。
- 任一阶段失败时可停止 `flowie-control` 并恢复原认证/ACL service；Broker 继续使用最后一个仍有效的 snapshot，过期后 fail closed。
- PostgreSQL、OIDC 或层级 group 不通过验收时不得静默回退到弱认证、扁平路径猜测或匿名放行。
