# Flowie ACL/Auth 控制面交付清单

## 目标与发布边界

目标是交付独立的 `flowie-control` 管理面，统一管理本地用户映射、角色、层级组、ACL 草稿、原子发布和
审计。Flowie Broker 仍是数据面，只通过 mTLS HTTPS 调用认证接口并读取版本化 ACL bundle；第三方
credential 只由第三方 HTTPS 服务验证，本地 credential 只由 `flowie-control` Repository 验证。Broker
不直连任何身份/ACL 数据库，`flowie-control` 也不直连第三方身份数据库或目录，
Dashboard 和 JSON-RPC 也不得绕过领域命令直接写本地授权库。

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

- 本地 credential 的唯一事实源是控制 Repository；选择第三方 Auth 时，第三方 credential、token 和
  目录账户状态的唯一事实源是第三方 HTTPS 服务。两个 verifier 不级联、不 fallback。
- 控制 Repository 是本地 user enabled、credential、角色、group、membership、ACL draft 和审计事件的
  唯一事实源；第三方 Auth 模式下它只做本地账户/授权映射。
- 已发布 ACL bundle 是从控制数据库事务快照派生的不可变产物；`flowie-control` 通过 HTTPS 分发，
  Broker 不注册 SQLite/PostgreSQL ACL provider。
- Broker 的 principal 与 ACL snapshot 都是有 TTL/version 的派生缓存，不允许反向修改控制数据库。
- 用户/凭据 revision 与 ACL `policy_version` 分开推进。只有授权可见状态变化才推进 `policy_version`；凭据轮换不能伪装成 ACL 版本变化。
- FlowStore、session store、Graph adapter 和 PostgreSQL control-store provider 都不能被配置成第三方
  认证来源；OIDC、LDAP/AD、RADIUS 和外部数据库访问只允许存在于第三方 HTTPS 服务内部。

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

## Phase 2：HTTPS 认证服务与兼容 credential 能力

本地 verifier/KDF 是正式可选 Auth。`auth.enabled: true` 且缺少 `external_https` 时选择本地 Auth；
出现 `external_https` 时选择第三方 HTTPS Auth。两种模式都 fail closed，第三方失败时不得 fallback
本地 verifier。

- [x] 使用 Monocypher Argon2id；algorithm、参数、salt、verifier 分字段存储，不保存或记录明文。参数与并发语义见 `ADR_CONTROL_CREDENTIALS.md`。
- [x] 内部 `generate_credential`/`rotate_credential` 只使用 TurboUtils CSPRNG，明文只返回一次；幂等重放不恢复明文，临时 secret、salt、verifier 和 KDF work area 在释放前清零。
- [x] 实现 `/v3/authenticate` 严格 HTTPS 契约，返回 principal、root group、roles、effective groups、expiry 和 policy version。内部 Iris endpoint 已限制为 `POST`、精确 v3 字段、规范 Base64、Bearer service token、无缓存响应和明确错误映射；v3 请求新增受信 `remote_address` 与 MQTT client `peer_certificate_sha256`，endpoint 仍由独立控制面进程显式注册，默认不监听。
- [x] 认证结果缓存使用有界容量、短 TTL 和 revision 失效；缓存键不得包含可恢复的明文 secret。positive credential cache 使用 keyed digest、TTL/LRU 和 credential/user revision 复核；principal snapshot cache 使用 root/principal keyed digest、TTL/LRU，并同时校验 user revision、credential revision、全局 store revision 与 policy version。缓存只保存不可变派生 snapshot，不保存 secret；数据库/revision 读取失败时 fail closed。
- [x] Argon2id/KDF 前执行有界双层 token bucket：按 verified mTLS caller 限制总量，并按 `(caller, root_group, principal)` 限制连续失败；成功只清除 identity failure bucket，不能重置 caller 总量。状态只保存 keyed digest，满容量按 LRU 淘汰，时间回拨/资源错误 fail closed；HTTP endpoint 将 `TURBO_EBUSY` 映射为 429。
- [x] `/v3/authenticate` 的 Broker 调用方身份必须从受信服务身份解析唯一 Root Group；内部服务核心按 `(listener_id, peer certificate SHA-256)` 精确绑定并覆盖跨 Root Group 同名主体测试。CoroNet/Iris 已提供强制 mTLS 和 verified peer identity，Flowie 薄 adapter 只从 `Req` 的已验证传输身份构造 caller；它与 v3 body 中由 Broker 转发的 MQTT client certificate 是两个独立身份，禁止用 header/body 代替前者。
- [x] 已连接 session 采用 principal TTL 有界传播：到期时完全空闲连接也会主动 fail closed，MQTT 5
  发送 `DISCONNECT 0x87` 后关闭，MQTT 3.x 直接关闭；Enhanced AUTH 必须在到期前完成，成功提交的新
  principal 会替换旧 deadline。当前没有控制面即时 push 撤销，最坏传播时间由当前 principal TTL 决定。
- [x] auth service 与 management service 已改为依赖版本化、按 user/auth/credential/group/role/policy/audit
  拆分的内部 control repository port；SQLite 通过薄 adapter 保持现有事务和用户可见行为。
- [x] 第三方认证基础端口已落地：versioned authenticator + subject mapper 接收 Root Group、identity、
  method、secret/token、protocol 和 remote address，只返回 issuer/stable subject/account state、
  assurance、expiry/revision 和有界 external groups。映射后通过 Repository v2 的 credential-free
  一致性 snapshot 重新检查本地 user enabled，并只加载本地 Root Group/Role/Group；第三方失败不回退
  本地密码，external groups 不自动获得本地权限。
- [x] 通用 HTTPS 第三方 authenticator adapter 已落地：独立 version 2 assertion 契约使用精确字段集、
  规范整数和有界响应；只允许 HTTPS 明确 path，以动态 service token 和可选私有 CA/mTLS client identity
  访问第三方 bridge；禁止 redirect/retry，网络、TLS、状态码或响应异常均 fail closed。真实 mTLS、
  coroutine、token lease 和畸形响应 contract tests 已覆盖。
- [x] 通用 HTTPS adapter 已接入独立控制面 version 1 schema 与 composition root。`external_https`
  出现即替换本地 credential 路径；可信 issuer/subject type 精确匹配后，稳定 subject 映射为本地
  principal id，并由 Repository 重新检查本地 user/Role/Group。启动预检会加载 service token 与可选
  client CA/certificate/private key，配置错误在监听前失败。
- [x] parser/runtime 已恢复本地 Auth 选择；`external_https` 缺失不再是配置错误，出现时仍严格替换
  本地 verifier。
- [x] HTTPS adapter 已增加实例级 `max_in_flight` 舱壁，超额请求在 secret/network I/O 前返回 busy；
  真实 mTLS 已覆盖成功、401 拒绝、429 过载、畸形 JSON、超时和并发拒绝，所有路径无 retry/fallback。
- [x] 真实 TLS 故障注入已覆盖响应前断连、服务端证书不受信和缺少客户端证书；service token 轮换已
  验证第三方服务器实际收到逐请求重新加载的新 bearer token。
- [x] HTTPS adapter 已提供无敏感字段的原子统计快照，区分成功、拒绝、本地舱壁、远端 429/5xx、传输、
  协议和本地依赖故障；真实 mTLS 与 token-provider 测试覆盖分类。
- [x] 现有 mTLS management JSON-RPC 已增加 `flowie.auth.external_https.stats`，仅 `security_admin` 可读取
  跨 Root Group 聚合统计；未启用 provider 时显式返回 disabled，未知参数 fail closed。
- [ ] 配置外部 collector，定义窗口化 SLO/告警阈值与故障 runbook；不新增匿名 metrics listener。
- [ ] 完成证书文件替换后的受控重启/回滚演练、端到端压力和部署运维 gate。
  OIDC、LDAP/AD、RADIUS 与外部数据库访问统一由第三方 HTTPS 服务内部承担，不再增加进程内原生
  adapter，也不增加 FlowStore/database 认证 backend。

当前验证：内部 SQLite store 已覆盖正确/错误/不存在主体凭据、禁用、轮换、撤销、幂等重放、revision 二次校验和 secret wipe；positive credential cache 已覆盖命中、TTL 过期、LRU 容量、revision 失效和并发读取；principal cache 已覆盖命中、TTL 过期、LRU 容量、revision 失效和并发读取，auth service 已覆盖组变更导致的 snapshot 失效；rate limiter 已覆盖 caller/identity 双桶、成功 reset、refill、容量和安全配置，以及 auth service 在 KDF 前返回 `TURBO_EBUSY`。内部 auth service 已覆盖受信 listener/certificate Root Group 绑定、跨 Root Group 同名主体隔离、事务化 principal snapshot、policy failure 和 credential revoke。第三方 seam 已覆盖 descriptor/capability、断言时效/账户/重复组校验、transport context、subject mapping、无本地 credential 的授权快照、外部组不自动授权、external expiry 收紧 TTL、上游失败无 fallback 和本地 user disable；通用 HTTPS adapter 已覆盖精确 JSON、非规范/溢出数值、动态 token、非 coroutine 拒绝、真实 mTLS 成功/401/429/5xx/畸形 JSON/超时/响应前断连/服务端证书不受信/缺少客户端证书、token 轮换请求捕获、实例并发舱壁、结果统计分类、配置 schema、subject mapper 和启动 TLS/secret 预检。受保护统计 RPC 已覆盖角色和参数边界，真实管理 HTTPS listener 已验证受信 mTLS `security_admin` 可读取 disabled 状态，且无证书和未知证书继续 fail closed。CoroNet 已覆盖真实 mTLS 握手和双方 verified fingerprint；Iris/Flowie adapter 已覆盖非法配置及普通 HTTP 请求 fail-closed；Iris endpoint 已覆盖严格字段、root_group 注入拒绝、非规范 Base64、Bearer token、body/header wipe、principal JSON 和 bind/unbind。Broker 已覆盖 principal 到期时 MQTT 5/3.x 空闲连接主动断开，以及到期前 Enhanced AUTH 替换旧 deadline。外部统计 collector/告警 runbook、证书替换后的受控重启/回滚演练、管理面 connection limit、真实网络压力 gate 及数据库/网络故障注入仍待完成，因此 Phase 2 尚未整体验收。

验收：第三方 HTTPS 的正确/错误凭据、禁用、轮换、缓存命中/失效、防爆破、secret wipe，以及第三方
网络和本地授权 Repository 失败的 fail-closed 测试通过。

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
- [x] Repository 已提供当前/精确版本的完整 bundle 只读快照，`flowie-control` 通过受 mTLS、
  service token 与证书 Root Group 绑定保护的 `GET /v3/acl` 分发；Broker HTTPS provider 加载后统一
  编译为不可变索引，不逐条更新 Broker 快照。
- [x] bundled Broker 已移除 SQLite ACL factory 和链接依赖；单机部署也通过 loopback HTTPS 访问
  `flowie-control`。
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
  以及仅限 `security_admin` 的 `flowie.auth.external_https.stats`，共 28 个方法。credential
  generate/rotate 仅在成功响应中返回一次 Base64 secret；幂等重放不恢复明文并返回
  `-32010`，Dashboard 不渲染 credential。生产网络仍必须等待 management connection/rate limit、压力和
  运维发布 gate。
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

## Phase 6：Broker 身份上下文与 HTTPS 企业集成

- [x] CoroNet 已把验证过的 MQTT TLS/WSS client certificate SHA-256 传给 Flowie；endpoint 仅在
  `tls_client_ca_file` 启用 required client-auth 后读取 verified fingerprint，并通过 size-gated Auth ABI
  追加字段交给 provider。TCP/WS 或未启用 client CA 的 TLS/WSS 显式不伪造证书身份。
- [x] `remote_address` 只取 CoroNet 直接 socket peer，TCP/TLS/WS/WSS 使用数值 `IP:port`，Pipe 使用
  `local`。当前不支持且不信任 PROXY protocol、X-Forwarded-For 或任意 header；代理部署看到的是代理
  地址。未来支持原始源地址必须增加显式 trusted-listener 配置、版本化解析和欺骗拒绝测试。
- [ ] MQTT 5 真正多轮 enhanced auth 支持 SCRAM/challenge/re-auth；内置单次 HTTPS provider 不冒充多轮
  实现。若 bundled 产品将来支持多轮认证，必须扩展为版本化 HTTPS exchange 契约，不能引入第二个
  进程内企业认证来源。
- [x] provider-neutral control repository contract 已建立；FlowStore 只复用 factory/capability/contract-test
  组织方式，不使用面向 MQTT protocol state 的 Record 数据模型承载控制事实或验证 credential。
- [ ] PostgreSQL control-store provider 使用专用关系 schema，并与 SQLite 共享 repository contract、
  领域事务语义和兼容性测试。专用 schema v1、fingerprint/version 校验、事务级 migration lock、
  connect/statement/lock deadline、`verify-full` gate、SQLSTATE 映射和有界独占连接池已落地；
  credential state/verify、principal snapshot、ACL bundle、audit 以及 user/group/role/policy 管理查询
  view 已实现；
  Root Group、user、credential、group/membership、role/assignment 与 policy draft/publish 写命令已实现
  `SERIALIZABLE`、request-id replay、revision CAS、ACL subject 引用检查、secret 一次返回与 audit
  原子事务，并对 effective group/role ABI 容量越界执行整笔回滚；policy publish 会在事务中重新验证
  完整 draft、连续编号替换 bundle 并独立推进 `policy_version`。完整 Repository v2 operation table 和
  内部 provider lifecycle factory 已绑定并通过 live contract；公开 `control_store` 配置、
  `env://` password、verify-full conninfo gate 和 runtime composition factory 已完成。全量
  SQLite/PostgreSQL 已复用 basic transactional contract，覆盖 revision conflict、User、Group、
  Role、local/external Auth snapshot、credential、ACL publish/bundle/replay 与 audit；全量管理分页、
  并发/故障与运维 gate 尚未完成。两种 provider 还复用真实本地 Auth/ACL generation contract：
  通过 Repository 创建账户、层级 Group、Role、credential 与已发布 ACL，调用生产 auth service
  验证密码、缓存命中、principal 的 Group/Role/policy version，以及 credential revoke 后的 revision
  失效和 fail-closed。生产 management service contract 同时复用两种 provider，覆盖账户分页、
  Group/membership、Role/assignment、ACL validate/publish/status、audit 分页，以及四类管理权限；
  JSON-RPC 只负责严格协议解析与调用该服务。认证快照在 PostgreSQL
  `REPEATABLE READ READ ONLY` 事务中组合用户、有效 Group 和 Role；credential KDF 前后复核
  user/credential revision。连接池固定为 1–64 个独立 `PGconn`，容量耗尽按 deadline 返回，关闭会
  拒绝新租约并等待归还，事务残留在归还时回滚，坏连接只有重新连接并验证 schema 后才会恢复可用。
  `COMMIT` 响应不可用时会从新租约按完整 audit 命令身份与 revision 确认是否已提交；确认成功才保留
  业务结果和本次一次性 credential secret，确认不可用则返回数据库错误。明确提交成功后的连接回收
  故障只影响池健康度，不再把已提交命令改报失败。
  该 provider 只承载本地授权/管理事实，不是第三方认证来源。
  2026-07-27 的 Linux focused gate 复用既有 PostgreSQL 17 容器并先清空用户 schema：
  14 个 live 用例、698 个断言全部通过，其中 commit confirmation 覆盖完整 audit identity 匹配、
  请求不存在、命令身份冲突和 revision 不一致；公共 runtime 连接 `ssl=off` 服务端按 `-4017`
  fail closed，PostgreSQL 日志无 ERROR/PANIC/FATAL，测试结束无残留 schema。该证据不替代上述
  网络断连故障注入与生产门槛。
- [x] 本地 Auth 的 Argon2id、SQLite 与同步 PostgreSQL 调用已进入专用有界 TurboUtils executor；
  `auth.local_executor` 定义 `workers`、`queue_capacity` 和 `deadline_ms`，默认 `4/128/10000`，硬上限
  `64/4096/60000`。Iris `Req`/`Res`/socket 不跨线程；owner lane 只提交 mTLS 指纹和有所有权的解码
  副本。队列满立即映射 429，deadline 映射 503。同步 KDF/SQL 不做不安全的强制取消：请求放弃迟到
  结果，任务继续拥有并擦除 secret；endpoint shutdown 停止接单并 drain。显式 local executor 与
  `external_https` 配置互斥，第三方 HTTPS Auth 保持 CoroNet coroutine I/O。Windows ASan 与 Linux
  GCC/ASan focused gate 覆盖真实 Argon2id deadline、1-worker/1-queue overload、drain 和连续竞态回归；
  PostgreSQL live contract 同轮通过且测试结束无残留 schema。
- [x] 第三方 authenticator/subject-mapper 内部契约只输出类型化 allowlist assertion，并合并为统一
  principal；Broker HTTPS v3 contract 不包含第三方 provider 类型或原始 claims，但会透传由 MQTT
  listener 验证的 client certificate fingerprint 和直接 peer address。
- [x] 可选 HTTPS bridge authenticator 已实现内部传输边界、公开配置 schema、运行时 composition root、
  受限 subject mapper、启动预检与真实 mTLS contract test。
- [x] 第三方 HTTPS bridge 已完成逐请求 token 轮换、真实 TLS/HTTP 故障注入、实例舱壁与无敏感数据
  统计快照；不再增加 OIDC/JWT、LDAP/AD 或 RADIUS 进程内原生 adapter。
- [x] 第三方 HTTPS bridge 的实例统计已接入受 mTLS 和 `security_admin` 保护的 management JSON-RPC。
- [ ] 第三方 HTTPS bridge 仍需完成证书受控重启/回滚、collector/告警 runbook、压力与生产运维 gate。

2026-07-27 的 Linux 精确源码 gate 使用 revision
`e84bfa0ffe302444113c30168435bf1d4a15c4057b9b6e7151c7233a4dcf7860`，复用既有
`flowie-pg-20260724T090212Z` PostgreSQL 17 容器且未创建、重启或替换容器。CoroNet
`test_stream`、Flowie/Auth/ACL/PostgreSQL Release focused 11/11、目标 MQTT mTLS/Auth
重复 10/10、GCC ASan focused 7/7 全部通过；测试前后自定义 schema 均为 0，容器保持
`healthy`、restart count 0，PostgreSQL ERROR/PANIC/FATAL 计数为 0。证据包
`flowie-linux-results-20260727T135316Z-auth-context-final4-verified.zip` 的 SHA-256 为
`f805f13153832c000807f382abd8554c441968c007fcec9a21a0ce69f33d90c2`。

重复运行完整 `test_flowie_endpoint` 时，既有
`MQTT-OWNER-003 MQTT-NET-004 fences a pending send and releases it during shutdown`
用例分别出现过一次超时和一次 SIGPIPE；同一 Release focused 单次、目标 mTLS/Auth 10 次及
ASan 全量 endpoint 均通过。该问题不改变本阶段 Auth 身份上下文契约的验收证据，但属于独立的
产品稳定性缺口，必须在生产开放前完成根因修复和独立压力回归，不能通过增加重试掩盖。

验收：mTLS 身份绑定、证书轮换/撤销、源地址欺骗拒绝、enhanced auth 取消/超时以及 HTTPS contract
tests 通过；配置与网络证据证明不存在第二认证来源。

## 生产开放门槛

- [x] 内部 controller startup-options 使用 TurboUtils CMD/DotEnv 选择独立配置文件；优先级固定为
  `CLI > process environment > explicitly selected DotEnv`，不隐式加载当前目录 `.env`，不接受明文 secret。
- [x] 独立 `flowie-control` 进程和 version 1 独立配置已接入；默认绑定 `127.0.0.1:8443`，不与 MQTT listener 共用地址。
- [x] 管理入口只调用显式 host 的 HTTPS listener，并固定要求 mTLS；证书/私钥/client CA 校验失败时不监听且无 HTTP fallback。
- [x] controller schema 的 service token 与私钥密码只接受 `env://...` reference；用户 credential、ACL rule body 与 secret literal 不进入 YAML、RPC 或 Dashboard。
- [x] 配置 parser 与 runtime 双重要求 Auth 公共字段完整；`external_https` 缺失选择本地 Auth，
  出现则选择第三方 Auth，禁止同时执行或失败 fallback。
- [x] `flowie-control` 通过 `storage.control_store` 选择唯一 SQLite/PostgreSQL Repository；缺失保持
  SQLite 兼容，双 block 被拒绝。PostgreSQL password 只接受 `env://`，conninfo 拒绝内嵌 secret/
  service 并强制 `sslmode=verify-full`；运行失败不回退到 SQLite。
- [ ] 企业发布仍需通过受保护统计 RPC、出站网络 ACL 与配置审计证明没有 FlowStore 或数据库认证旁路；
  `enabled: false` 只允许 management-only 部署。
- [ ] 审计包含 actor、request ID、operation、target、revision、结果与时间，且不包含 credential。
- [ ] 明确备份、恢复、schema migration、回滚和 HA 单写者策略。PostgreSQL migration 已使用事务级
  advisory lock 串行化并拒绝未知 version/fingerprint；备份/PITR、应用回滚和多实例写入策略仍待验证。
- [ ] 修复并独立复验重复 full-endpoint gate 中 `MQTT-NET-004` pending-send shutdown 的超时/SIGPIPE；
  生产 gate 不得把该失败归类为可忽略 flake。
- [ ] 完成 threat model、权限审查、模糊测试、ASan/UBSan、发布回归与真实 TLS 集成 gate。
- [ ] `CONTROL_GUIDE.md` 与部署 schema 示例已新增；仍需完成其余 release gate 后才能声明生产就绪。

## 兼容性与回滚

- provider 的 YAML 选择方式保持不变；安全 Auth request ABI 以 `size` 兼容旧基础尺寸，并追加 MQTT
  client certificate 字段。Broker/control HTTPS body contract 已有意升级为唯一 v3，第三方 assertion
  contract 为唯一 v2；不解析旧版本。`flowie_endpoint_config_t` 增加 `tls_client_ca_file`，混用新旧
  二进制不是受支持的滚动升级路径。
- 旧 SQLite ACL 表不会被 Broker 读取；必须在 `flowie-control` Repository 中重新发布完整 v3 bundle，
  再把 Broker `policy_source` 切换到 `/v3/acl`。
- Flowie session record 已升级为唯一 v3。旧记录不会被解释为 Root Group 身份；部署升级前必须按发布流程清理或离线迁移旧 session store。
- 任一阶段失败时可停止 `flowie-control` 并恢复原认证/ACL service；Broker 继续使用最后一个仍有效的 snapshot，过期后 fail closed。
- PostgreSQL、第三方 HTTPS 认证或层级 group 不通过验收时不得静默回退到弱认证、扁平路径猜测或匿名放行。
