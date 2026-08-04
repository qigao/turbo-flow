# ADR：Flowie Control Store 使用专用领域 Repository Provider

## 状态

已采纳；SQLite adapter 已接入。PostgreSQL session/schema migration、连接池、完整 query/command
operation table、repository provider 与公开 composition factory 已接入。SQLite/PostgreSQL 已共享基础
事务、生产本地 Auth/ACL generation 和 management service contract；全量并发、故障与运维门禁尚未完成。

## 背景

`flowie-control` 的用户、credential verifier、Domain、Group、Role、membership、ACL draft、
published bundle 和管理审计需要共享同一事务事实。既有实现把领域命令和 SQLite SQL 放在
`flowie_control_store.c` 中，auth service、management service 与 runtime 因而直接依赖
`flowie_control_store_t`，无法在不复制上层服务的情况下增加外部数据库。

FlowStore Record contract 面向 MQTT protocol state，只提供 namespace 全量 scan 和原子 CAS batch。
FlowStore ADR 也明确排除 ACL/control-plane SQLite。将控制面关系数据编码为通用 Record 会丢失
类型化索引、外键、keyset query 与迁移边界，因此不改变该事实模型分工。

## 决策

控制面增加内部、版本化的 `flowie_control_repository_t` persistence port。接口按职责拆分为：

- user/domain；
- authentication snapshot；
- credential lifecycle；
- group/membership；
- role/assignment；
- policy draft/publish；
- published policy bundle 只读快照；
- audit/revision。

每组 operation table 都少于十个方法。repository 只借用 provider context 与静态 operation table；
composition root 拥有 provider，且必须在停止 auth/management 请求后才能销毁。auth service 和
management service 复制轻量 repository descriptor，不拥有 provider context。

provider 必须声明并实现以下能力：

- durable commit boundary；
- 领域修改、revision 与管理审计的原子命令；
- revision-checked consistent authentication snapshot；
- bounded keyset pagination；
- externally authenticated identity 的 credential-free、local-authorization snapshot。

缺少版本、能力位、context、operation table 或必需 callback 时，服务创建必须 fail fast。运行时只选择
一个控制事实源，不允许 SQLite/PostgreSQL 双写，也不允许数据库失败时回退到 local/volatile store。

## SQLite adapter

现有 SQLite store 继续拥有当前 schema、事务和 KDF 记录。薄 adapter 只做 `void *ctx` 到
`flowie_control_store_t *` 的类型转换和错误原样传播，不复制领域规则。SQLite store 内嵌 repository
descriptor，因此 descriptor 与 store 具有相同生命周期。

version 1 YAML 的 `storage.control_store` 缺失时仍选择 SQLite，保持既有配置行为。显式选择
`postgresql` 时只允许 PostgreSQL block，不能同时保留 SQLite block。该变化不改变 Broker HTTPS
Auth/ACL contract 或公开安装 ABI。

## PostgreSQL provider 边界

PostgreSQL provider 必须使用专用关系 schema 和类型化 query，不直接使用 FlowStore
`turbo_flow_record_store_t`。可以复用现有 StorageBackend 的 factory/capability/contract-test 组织方式，
以及 libpq 的公共连接配置设施，但不能复用 MQTT Record 数据模型。

已完成的基础边界：

- 使用独立 `domain`、user、credential、group、membership、role、assignment、policy、audit、
  revision 表，不编码为通用 Record；
- schema version + fingerprint 双重兼容性检查；
- transaction-scoped advisory migration lock，migration 失败回滚；
- `PQconnectdbParams` 参数化连接，固定 read-write target、application name、空 `search_path`；
- connect、statement、lock、idle-in-transaction deadline；
- 产品默认要求有效 `sslmode=verify-full` 和实际 TLS session；
- serialization/deadlock/lock contention、statement timeout、integrity 和 connection SQLSTATE
  映射到统一错误边界；
- 1–64 个独占 `PGconn` 的有界连接池、acquisition deadline、关闭 drain 和归还清理；
- credential state/verify、local/external principal snapshot、ACL bundle、audit/revision 的类型化
  只读 query view；
- user/group/role 的 get/list/effective、policy draft validate/list/status 管理查询；所有分页均为有界
  keyset，组合身份和 policy validate 使用 `REPEATABLE READ READ ONLY` 一致性事务；
- Domain、user、credential、group/membership、role/assignment 与 policy draft/publish 写命令使用
  `SERIALIZABLE` 事务完成 request-id replay、revision CAS、ACL subject 引用检查、领域修改和 audit
  原子提交。
  group 命令同时约束父组状态、树深度、叶节点禁用与 effective-group ABI 容量；role 命令约束
  Domain、用户/角色启用状态、ACL subject 引用与 effective-role ABI 容量，容量越界时整笔赋权回滚。
  policy rule 写入要求规范规则行、有效主体和合法 MQTT filter；publish 在同一事务重新验证完整非空
  draft、连续编号替换 bundle、独立推进 `policy_version`，并保存可重放的 publish 结果。
  credential KDF 在事务外执行，并在执行前后重新校验 revision、用户与 credential 状态；generate/rotate
  的 secret 只返回一次，重复 request-id 返回 `TURBO_EALREADY`。serialization/deadlock 不在 provider
  内静默重试，而是返回 `TURBO_EBUSY`，由服务边界用同一 request-id 决定重试。若 `COMMIT` 响应不可用，
  provider 会先归还并清理原租约，再从新租约按 `request_id + actor + operation + domain_id +
  target_id + target_detail` 查询事务内 durable audit，并严格比对 revision；匹配才把原命令判定为成功。
  这使 generate/rotate 能在“事务已提交但响应丢失”时仍安全返回本次内存中的一次性 secret。明确收到
  `COMMIT` 成功后，后续连接清理/重连失败只降低连接池健康容量，不得把已提交的业务命令改报失败。
  若新连接也不可用，provider 仍返回数据库错误，不猜测提交结果；调用方只能保留同一 request-id 重试。
- 内部 provider factory 拥有连接池及 immutable query/command view，绑定完整 Repository v2
  operation table；销毁时先关闭新租约并 drain 在途调用，再释放 view 和连接池。
- version 1 配置用 `storage.control_store: sqlite|postgresql` 显式选择唯一 provider；composition root
  只实例化所选 Repository，不双写、不 fallback。PostgreSQL password 只能通过 `env://` reference
  注入独立 libpq 参数；公开 conninfo 拒绝 password/passfile/sslpassword/service/servicefile，并强制
  `sslmode=verify-full`。`--check` 校验选择、conninfo 和 secret reference，但不连接或迁移数据库。

公开生产接入前还必须完成：

- 单写者 fencing 或经过验证的多写者语义；
- SQLite/PostgreSQL 全量管理分页、并发和故障参数化 contract；
- backup/restore、PITR、HA 切换和故障注入 gate。

配置接入已经改变部署 schema；旧配置因默认 SQLite 保持兼容。PostgreSQL 部署不得接受 literal
conninfo password。

## Authenticator 不是 Control Store

OIDC/JWT、LDAP/AD 和 RADIUS 是 credential/token verifier 与 external-subject mapper，不是 Control Store
provider。bundled 产品也不把这些 verifier 做成进程内 adapter：它们只能位于第三方 HTTPS 服务内部。
Repository contract version 2 增加 `external_principal_snapshot`：HTTPS 外部认证成功并完成 subject
mapping 后，它在一个一致性读事务中检查本地 user enabled 并加载本地 Domain、Role/Group；不要求
也不验证本地 credential。外部 assertion revision 只作为该次认证 generation 返回，不能进入本地
password cache。未配置外部 authenticator 时，Repository credential verifier 是正式的本地 Auth；
配置外部 authenticator 后，本地 verifier 不参与该次认证，也不作为失败 fallback。Broker 继续只注册
HTTPS authentication provider；SQLite/PostgreSQL Repository 和 FlowStore 都不能成为 Broker 的直连
认证来源。

## 验证

- repository validation 测试拒绝缺失版本、能力位、table 和 callback；
- SQLite repository contract 覆盖 revision、audit、credential verification、local/external principal
  snapshot、published bundle 当前/精确版本读取和 conflict 原子性；
- 既有 store、auth cache、auth service、management、Dashboard、RPC、runtime 与 HTTPS integration
  测试继续作为行为回归；
- PostgreSQL provider 的 operation table 已通过 repository validation，并增加 live、migration、并发和
  故障测试；公开配置/secret/TLS fail-fast 与 runtime factory 已覆盖 focused test。SQLite 与
  PostgreSQL 已复用同一 basic transactional contract，覆盖 revision conflict、User、Group、Role、
  local/external Auth snapshot、credential、ACL publish/bundle/replay 和 audit；全量管理分页、并发与
  故障参数化 contract 仍是发布门槛。两者还复用同一个本地 Auth/ACL generation contract：通过
  Repository 创建账户、层级 Group、Role、credential 和已发布 ACL，调用生产
  `flowie_control_auth_service` 验证密码、缓存命中、principal 的 Group/Role/policy version，以及
  credential revoke 后的 revision 失效与 fail-closed。两者还复用生产 management service contract，
  验证账户分页、Group/membership、Role/assignment、ACL validate/publish/status、audit 分页，以及
  viewer/user-admin/policy-admin/security-admin 权限边界；JSON-RPC 只负责严格协议解析和调用该服务。
- `test_flowie_control_pgsql_database` 在没有数据库时覆盖配置边界与 SQLSTATE 映射；
- `test_flowie_control_pgsql_database_live` 覆盖 migrate 后 validate、连接池清理、完整 repository
  operation table、管理查询契约、commit-result-unknown 的 durable audit 确认身份，以及
  Domain/user/credential/group/membership/role/assignment/policy 原子命令的 replay、revision、ACL
  引用、层级与容量、规范规则校验、原子 bundle 发布、secret 一次返回、生产本地 Auth/management
  service 组合及 audit 不变量；只有显式打开 `TURBO_FLOW_PGSQL_LIVE_TESTS` 且提供测试 conninfo 才运行。
  2026-07-27 复用既有 PostgreSQL 17 容器的 focused gate 为 14/14 live cases、698 assertions；
  commit confirmation 的匹配、缺失、身份冲突和 revision 冲突均通过。该用例验证恢复读契约，不替代
  在代理/网络层切断 `COMMIT` 响应的故障注入 gate。
