# ADR: 协议数据与业务数据存储分层

## 状态

已接受，2026-08-03，2026-08-04 修订 standalone 生命周期。standalone ProtocolStore SQLite
`:memory:` 已接线；cluster Redis route
projection、MemberDirectory 与周期 reconciliation 已接线，Redis 尚未替换现有 PostgreSQL
authoritative cluster facts。

## 背景

Flowie 同时处理两类生命周期、确认语义和故障边界不同的数据：

- MQTT 协议数据：session、subscription、inflight、retained、Will、presence、route projection。
- 业务数据：Graph 接纳后的业务消息、状态、索引、日志、时间序列和 outbox。

旧设计把 endpoint 的 `session_store` 描述为 FlowStore MQTT fact facade，使业务存储与协议状态看起来
可以互换。后续 standalone 文件型 SQLite 又把进程内协议状态误建模为长期数据。cluster 当前还将
session/retained authoritative facts 放在 PostgreSQL，使 ownership/fencing、协议状态和业务持久化
共享一个实现边界。

## 决策

两个领域使用不同 facade、配置名称、namespace、连接所有权、migration 和错误语义；即使底层
引擎相同也不得共享逻辑实例。

```text
Flowie MQTT endpoint
  -> ProtocolStore
       standalone: SQLite :memory:
       cluster target: Redis

Flowie ClusterCoordinator
  -> PostgreSQL ownership / owner_epoch / fencing

TurboFlow Graph
  -> BusinessStore / FlowStore
       SQLite / Redis / PostgreSQL
```

`flowie_protocol_store_t` 是 Flowie 内部 opaque facade。endpoint 只能通过它执行 scan、revision
CAS 和 atomic batch；不能直接调用 StorageBackend callback。`FlowStore` 保留业务 State、Index、
Log、Series 和业务 Record 的职责，不再是 MQTT 协议事实的领域 owner。

### Standalone

`manage_sessions: true` 时，`flowie-server` 的 ProtocolStore 必须使用独立 SQLite `:memory:` Record
backend。每个 endpoint store owner 独占一个连接；session、subscription、inflight、retained、Will
及其 revision 在该连接中原子提交，owner close 后全部销毁。进程重启后 Client 必须重新连接、订阅并
重建协议状态。打开、schema、CAS 或容量失败会中止 endpoint 注册，不回退到文件、Redis、PostgreSQL
或另一份内存事实。

隐式 store 默认使用 `:memory:`。显式 `protocol_store` 只用于声明独立容量和 namespace，backend 仍
必须为 SQLite 且 `database_path` 必须为 `:memory:`。旧 `session_store` 只作为互斥的配置兼容名称
保留；两者同时出现立即失败。通用 SQLite backend 仍支持文件路径，但只供 BusinessStore 或其他
产品使用，不允许借此改变 standalone ProtocolStore 生命周期。

需要跨进程长期保存的数据由 Graph 写入独立 BusinessStore，可选择 SQLite、Redis 或 PostgreSQL。
BusinessStore 不导入、恢复、补写或替代 ProtocolStore，也不参与 MQTT Session Present、ACK、
takeover、Will 或 subscription 路由判定。

### Cluster

目标形态使用 Redis 保存 session、subscription、inflight、retained、Will、presence 和 route
projection；PostgreSQL只签发 ownership、`owner_epoch` 和 fencing。每个 Redis mutation 必须携带
PostgreSQL签发的 epoch，Redis 原子脚本拒绝旧 epoch。

本阶段不改变现有 PostgreSQL cluster fact source。Redis 方案只有在完成 epoch barrier、
Redis Cluster `MOVED/ASK`、Sentinel failover、复制/AOF durability 和 takeover recovery 门禁后才能
离线 cutover。迁移期间禁止 PostgreSQL/Redis 协议双写，也禁止读失败后跨后端 fallback。

当前 route projection 是过渡期派生索引，不是 MQTT 事实源。membership worker 独占 PostgreSQL
coordinator，从同一次 bounded membership snapshot 同时生成 topology plan 与不可变
MemberDirectory candidate；topology apply 成功后才原子发布目录。所有 shard projector 只通过该目录
复制查询 node/boot/member，不共享 `PGconn`，也不在目录 miss 时回查 PostgreSQL。

每个 membership cycle 由唯一 worker 对共享 Redis route namespace 执行一次 reconciliation，而不是每
shard 重复全量扫描。相同 session `(owner_epoch, fact_revision)` 只允许在 node/boot、connection 和
session identity 不变且 lease deadline 单调前进时刷新 endpoint/lease；member 缺失、OFFLINE 或
EXPIRED 时不续租，让旧 route 按 deadline 自然失效。Redis I/O 失败进入有界 cycle retry，畸形 route
或 member identity 冲突 fail fast；不存在 PostgreSQL route lookup fallback。

### 设备在线状态

- `protocol_presence` 是 MQTT 事实：Client ID、connection/generation、edge、lease、owner epoch。
  它参与 takeover、route、Will 和 fencing。
- `business_device_status` 是业务投影：由协议事件异步、幂等地更新。它不得参与 MQTT ACK、route、
  session 恢复或 fencing。

## 候选方案

1. 继续由 FlowStore 统一所有数据。依赖较少，但无法表达不同 ACK、TTL、恢复和 fencing 语义，拒绝。
2. PostgreSQL作为全部 cluster 状态。事务一致性清晰，但高频 presence/subscription 路由放大数据库
   热点，且业务与协议故障域耦合，保留为过渡实现。
3. Redis保存全部状态并替代 PostgreSQL ownership。延迟低，但异步复制不能单独提供可靠 fencing，
   Redis failover 后可能接受旧 owner，拒绝。
4. 本 ADR 的双领域 facade + PG fencing + Redis protocol facts。边界最清晰，但增加部署、migration
   和跨后端验收成本，采用。

## 影响与权衡

- 架构：Flowie 不再依赖 FlowStore 的 MQTT facade；StorageBackend 只是实现 SPI，不是领域接口。
- 接口：新增 `protocol_store`；旧 `session_store` 兼容但互斥。standalone 仅接受 SQLite `:memory:`。
- 状态归属：协议写先 atomic commit，再交换 endpoint cache；业务 Graph 不能推进协议 revision。
- 错误语义：配置/open/schema/CAS/容量失败原样向上返回，不进行 backend fallback 或静默修复。
- 性能：standalone 使用 memory journal 且不执行磁盘同步，避免协议查询和状态迁移进入文件 I/O；容量
  仍受 RecordStore 上限约束。cluster Redis 切换前必须以 owner lane benchmark 验证吞吐和 P99。
- 资源：相同物理 Redis/PG/SQLite 的 namespace 隔离不等于 CPU、内存、I/O 和故障域隔离；生产可按
  容量把两层部署到独立实例。

## 迁移与回滚

Standalone 升级前先停止 listener 并排空 owner lane。旧文件型协议数据库不导入 `:memory:`，可按运维
保留策略离线归档；新进程启动后由 Client 重连和重订阅重建协议状态。需要长期保存的业务事实应通过
明确 Graph/BusinessStore migration 独立迁移，禁止把旧协议 snapshot 转成业务事实或双写。回滚旧
版本只能在停机窗口执行，不能把新进程的内存协议状态反向同步到旧文件。

Cluster 迁移采用 `PG claim -> Redis epoch barrier -> snapshot import -> recover -> ACTIVE`。回滚在
ACTIVE 前删除未激活的 Redis namespace；ACTIVE 后必须再次停机、提升 fencing epoch 并执行反向离线
snapshot，不能直接让旧 PostgreSQL facts 重新上线。

## 验证

- SQLite RecordStore：binary key/value、CAS conflict、atomic rollback、容量、稳定 scan snapshot、
  namespace 隔离、文件损坏、文件 close/reopen，以及 `:memory:` close/reopen 后为空。
- Standalone composition：隐式和显式 `:memory:` ProtocolStore 在每个 application generation 均以
  `Session Present = 0` 启动；文件路径在配置边界被拒绝。
- Cluster gate：旧 epoch 拒绝、takeover、Will、route、Redis Cluster/Sentinel failover、AOF/replica
  丢失窗口和无双写 cutover。
- Route projection：同版本 settlement 重放、连续 member lease 续期、endpoint 更新、OFFLINE/EXPIRED
  自然过期、resolver/Redis 错误传播、tombstone 跳过、单一 maintenance owner 和关闭期目录生命周期。
