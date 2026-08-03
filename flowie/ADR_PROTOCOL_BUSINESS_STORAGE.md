# ADR: 协议数据与业务数据存储分层

## 状态

已接受，2026-08-03。按阶段实施：standalone ProtocolStore SQLite 已接线；cluster Redis route
projection、MemberDirectory 与周期 reconciliation 已接线，Redis 尚未替换现有 PostgreSQL
authoritative cluster facts。

## 背景

Flowie 同时处理两类生命周期、确认语义和故障边界不同的数据：

- MQTT 协议数据：session、subscription、inflight、retained、Will、presence、route projection。
- 业务数据：Graph 接纳后的业务消息、状态、索引、日志、时间序列和 outbox。

旧设计把 endpoint 的 `session_store` 描述为 FlowStore MQTT fact facade，并允许默认使用 volatile
local Record backend。这会让业务存储与协议恢复看起来可以互换，也使未显式配置的 standalone
broker 在重启后丢失协议事实。cluster 当前又将 session/retained authoritative facts 放在
PostgreSQL，使 ownership/fencing、协议状态和业务持久化共享一个实现边界。

## 决策

两个领域使用不同 facade、配置名称、namespace、连接所有权、migration 和错误语义；即使底层
引擎相同也不得共享逻辑实例。

```text
Flowie MQTT endpoint
  -> ProtocolStore
       standalone: SQLite
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

`manage_sessions: true` 且未配置显式 `protocol_store` 时，composition root 使用独立 SQLite
Record backend。数据库路径由 `flowie_server --protocol-store-path` 指定，默认
`flowie-protocol.sqlite3`。namespace 使用 endpoint 名；SQLite 连接和容量只归 ProtocolStore
owner。打开、schema、容量或恢复失败会中止 endpoint 注册，不回退到 volatile local。

显式 `protocol_store` 可引用 `backend: sqlite` channel。旧 `session_store` 只作为互斥的配置兼容
名称保留；两者同时出现立即失败。它不是第二事实源，也不触发双写。

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
- 接口：新增 `protocol_store`；旧 `session_store` 兼容但互斥。默认 standalone 从 volatile 变为
  durable SQLite。
- 状态归属：协议写先 durable commit，再交换 endpoint cache；业务 Graph 不能推进协议 revision。
- 错误语义：配置/open/schema/CAS/容量失败原样向上返回，不进行 backend fallback 或静默修复。
- 性能：standalone 增加 SQLite FULL synchronous commit 成本；换取可复验的重启恢复。cluster Redis
  切换前必须以 owner lane benchmark 验证吞吐和 P99。
- 资源：相同物理 Redis/PG/SQLite 的 namespace 隔离不等于 CPU、内存、I/O 和故障域隔离；生产可按
  容量把两层部署到独立实例。

## 迁移与回滚

Standalone 先停止 listener 并排空 owner lane，再从旧 durable `session_store` 导出离线 snapshot，
写入目标 SQLite namespace，校验 record count、revision 和 codec 后切换配置。volatile local 没有可
恢复数据。回滚只能在无新写入的停机窗口切回原 durable store；不得双向同步。

Cluster 迁移采用 `PG claim -> Redis epoch barrier -> snapshot import -> recover -> ACTIVE`。回滚在
ACTIVE 前删除未激活的 Redis namespace；ACTIVE 后必须再次停机、提升 fencing epoch 并执行反向离线
snapshot，不能直接让旧 PostgreSQL facts 重新上线。

## 验证

- SQLite RecordStore：binary key/value、CAS conflict、atomic rollback、容量、稳定 scan snapshot、
  namespace 隔离、损坏数据库、close/reopen。
- Standalone composition：隐式 ProtocolStore 跨 application generation 恢复 MQTT Session Present。
- Cluster gate：旧 epoch 拒绝、takeover、Will、route、Redis Cluster/Sentinel failover、AOF/replica
  丢失窗口和无双写 cutover。
- Route projection：同版本 settlement 重放、连续 member lease 续期、endpoint 更新、OFFLINE/EXPIRED
  自然过期、resolver/Redis 错误传播、tombstone 跳过、单一 maintenance owner 和关闭期目录生命周期。
