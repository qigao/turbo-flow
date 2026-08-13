# ADR：FlowStore 统一状态、索引、日志与时间序列

## 状态

已接受，2026-07-23。

## 背景

旧 `FlowQueue` 同时承载队列、SQLite/Redis 适配和部分通用存储职责，旧
`FlowStorage` 又单独提供文件、目录、append-log 与 SQLite sink。这两个模块没有形成统一的
数据所有权、容量和查询契约。新设计删除两个旧模块，不保留其 API、配置或行为兼容性。

FlowStore 独立于外部协议产品。调用方只依赖类型化存储契约，不拥有具体 HashTable、Redis 命令或
CRoaring 实现。

Record 的公共契约位于基础 `turbo_flow/include/turbo_flow_record_store.h`，因为它是
Flow/TurboFlow 的稳定数据边界；它不反向依赖 FlowStore。FlowStore 提供 State/Index/Log/
Series facade，具体 backend 实现则由 `flowstore/backend` 的 `StorageBackend` registry 按
model 单独装配；local 的实现位于同级 `tf_local_storage` shared library，而不是协议产品内部
direct-factory。

协议 session、subscription、inflight、retained、Will、presence 和 route projection 不属于
FlowStore 业务领域。外部协议 owner 可借用 registry 提供的 Record service，但 endpoint 不得
绕过自身状态边界直接推进 Record callback。

### 业务事实与协议事实不变量

FlowStore 只拥有 Graph 显式写入的业务事实。session、subscription、inflight、retained、Will、
presence 和 route projection 是协议事实，由外部协议 owner 管理。owner 内的
session/vector、topic trie、member map 和 retained map 只能作为从 ProtocolStore 重建的单 owner
cache；每次状态迁移必须先完成协议 CAS/atomic batch，再交换 cache owner，失败不得推进 cache。
业务 Store 与 ProtocolStore 即使使用同一种 backend，也必须使用独立 namespace、连接 owner、
容量和 migration，且禁止 fallback 或同步双写。

连接对象、解析缓冲、发送队列、CoroNet lane 状态和 ACL/control-plane SQLite 不属于 MQTT
业务事实：前者是传输运行时状态，后者是独立管理面，均不通过 FlowStore 承载。

## 决策

FlowStore 及其 Record contract 分为五种不能互换的数据模型：

| 模型 | 事实语义 | local backend | Redis backend | SQLite backend | PostgreSQL backend |
|---|---|---|---|---|---|
| Record | 带 revision 的批量 CAS 记录 | volatile linked store（atomic，不 durable） | Hash/Lua | 固定 Record 表 + transaction | 固定 Record 表 + transaction |
| State | 带 revision 的最新键值状态 | TurboUtils Hash Map | Hash | - | - |
| Index | 无序成员集合和映射查询 | TurboUtils Hash Map/Set | Set 或 Sorted Set | - | - |
| Log | 带单调 cursor 的有序事件历史 | 有界分段日志 | Stream | - | - |
| TimeSeries | 按时间排序的标量采样 | 有界有序 chunk | -（未声明 Series capability） | - | - |

CRoaring 不保存事实数据。它封装在有硬容量上限的 `turbo_flow_bitmap_index_t` 中，只允许作为
可从 IndexStore 重建的派生加速索引，并且必须由代表性 benchmark 证明其相对 HashTable 的收益。

`turbo_flow_store_route` 先按数据语义选择上述模型，再比较部署配置给出的本地
`records/bytes/item/write-rate/retention` 上限。local 是由 `tf_local_storage` DLL 提供的
进程内、非 durable 事实源；要求 durable 或跨进程共享时选择 Redis/PostgreSQL，并由 storage
backend capability 明确拒绝不支持的模型，不自动静默退回 local。

`flowstore/backends/{local,redis,sqlite,pgsql}` 是同级 backend 模块，只通过 StorageBackend 的
`open()/close()` function table 暴露 provider-neutral service。宿主启动时先创建 registry、
注册可用 API，再由 owner 创建并持有 service；调用方不直接调用 local 容器或远程数据库的
`create_*`/`destroy_*` 函数。当前 `tf_redis` 不声明 Series capability，TimeSeries 路由必须
在 `open()` 前被拒绝，而不是退回 Stream、Sorted Set 或 local。

### FlowStore：Record 事实源与可选 schema adapter

`FlowStore` 是一个 namespace-bound RecordStore 的唯一业务 facade，不是关系型 ORM。它直接拥有
`put/get/delete/scan/commit` 的二进制 Record 操作；Record 是 `key + revision + binary value`。DataBind
是可选 schema adapter，既不拥有 FlowStore，也不决定 Redis、SQLite 或 PostgreSQL 的 value 格式：

```text
DataBindRecord (optional)
    -- DataBind binary --> FlowStore Record value
Opaque/protobuf/custom binary
    -------------------> FlowStore Record value
```

核心 API 是二进制 `put/delete/scan/commit`；调用者不写 SQL、Redis command 或 backend 分支。`put`
仍要求调用者给出 `expected_revision` 与 `next_revision`，由 RecordStore 保持 CAS 和 atomic batch 的
既有语义。业务 key（如 `order/42`）由调用者决定；FlowStore 和 DataBind 都不推断 key。

FlowStore 接受任意二进制 value，并原样交给 backend。启用 DataBind adapter 时，adapter 固定
schema/type identity，并只对自己写入或读取的 DataBind binary 执行编解码、schema validation 与
QueryVM 字段解析。adapter 不提供 CSV、JSON、YAML 或 XML 的存储导入导出，也不自动转换格式；无法
按该 schema 解码的 value 返回明确协议错误，不能猜测格式、忽略字段或切换 schema。

SQLite 和 PostgreSQL 的物理存储仍使用一张固定的 Record 表：

```text
namespace_name | record_key | revision | value(BLOB/BYTEA)
```

这里的一行只是完整 Record value 的持久化载体，不是 `Order`、`User` 等业务对象的 SQL 行模型。
SQLite/PG provider 内部用固定、参数绑定的 SQL 执行 schema 创建、snapshot scan、CAS put/delete 和
transaction；这些语句不按 DataBind schema 或业务字段动态生成。Redis 则以等价的 Hash/Lua 原子
操作承载同一 Record contract。于是业务层的事实模型始终是 `key -> revision + value`，而不是
“C struct 映射到 SQL 表/列”。

`FlowStoreDataBind::query` 仅接收已经编译并验证的 QueryVM bytecode；它在 FlowStore snapshot 上依次
解码 DataBind binary，并以 canonical top-level schema field 作为 QueryVM operand 解析来源。它不接收
SQL 文本，也不会把 `status == "open"` 翻译为 `WHERE status = ...`。因此其时间复杂度是
`O(candidate_records × predicate_steps)`；QueryVM 的 instruction/operand/regex/step 上限必须在 scan
之前验证。`uint64/int64` 字段必须使用精确整数 operand 比较，禁止降级为 `double`。

需要高效字段查询时，业务必须维护明确、可从 Record 事实源重建的 IndexStore 派生索引，先取得
候选 key，再由 QueryVM 作精确过滤。未来如需 SQLite/PG 的 SQL 下推，只能作为显式、关系后端专用的
查询 provider：它必须定义支持的 schema field、操作符、索引、方言、事务快照与 fallback-free 的
失败语义；不得扩展 RecordStore 或伪装为 Redis 同构功能。

FlowStore 与其可选 DataBind adapter 都不负责 DataBind schema 到表/列/foreign key 的映射、DDL
migration、Join、关联加载、自动 dirty tracking、级联写入或跨实体事务。这些能力属于独立的
relational ORM 设计，不能混入当前跨后端 Record contract。

## 公共协议

### 数据单元

- Record 是 `binary key -> { revision, binary value }`；一个 value 是不可拆分的领域事实单元。
  value 可为 DataBind、protobuf 或应用私有 binary；它不是文本格式、CSV 行或 SQL 列集合。
- State key/value、Index name/member 和 Log payload 是二进制安全字节序列。
- TimeSeries key 是二进制安全字节序列，sample 是 `timestamp_ms + typed scalar`。
- State revision、Log cursor 均从 1 开始，0 表示“未指定/不存在”。

### 所有权

- 所有写 API 使用 copy 语义。成功和失败后输入仍由调用方拥有。
- 所有读 API 返回 retained `mem_buffer_t` 或复制到调用方提供的数组，不暴露容器内部裸指针。
- Store 独占其可变容器；销毁时释放所有 retained buffer。

### 并发

- 本地 backend 是 single-owner、非线程安全实例。
- 跨线程调用必须在 FlowStore 外部通过既有 command/ring/executor 转移到 owner。
- Redis backend 的连接串行化规则由 Redis provider 明确，不能从本地实现推导线程安全保证。

### 容量

每个实例同时配置并检查：

- `max_records`
- `max_bytes`
- `max_item_bytes`
- `retention_ms`
- `full_policy`

所有 `size + delta`、`count + 1` 和容量乘加在修改状态前检查溢出。State/Index 只允许
`REJECT`；Log/TimeSeries 可使用 `REJECT` 或 `TRIM_OLDEST`。超出单项限制返回
`TURBO_EFBIG`，超出总容量返回 `TURBO_ENOSPC`。

### 状态迁移

State 写入：

```text
ABSENT --put(expected=0)--> revision 1
revision N --put(expected=N)--> revision N+1
revision N --remove(expected=N)--> ABSENT
```

revision 不匹配返回 `TURBO_EBUSY`，且不得改变状态。

Log 写入：

```text
reserve capacity -> copy payload -> assign next cursor -> publish record
```

任一步失败都不得消耗 cursor 或留下半记录。已被 trim 的 cursor 返回明确的 stale-cursor
状态，不伪装成空结果。

TimeSeries 默认要求同一 series 的 timestamp 单调不减；重复 timestamp 和乱序策略是显式配置，
不自动修复输入。

### 关闭

关闭顺序固定为：停止接受写入、等待 backend 活动操作归零、释放 retained 结果、销毁索引与
记录、销毁内存域。关闭后的操作返回 `TURBO_ESHUTDOWN`。

### 可观测性

每个实例至少暴露当前/峰值 records、当前/峰值 bytes、拒绝次数、trim 数、冲突数、查询数和
backend 错误数。热路径只更新计数器，不逐条记录 INFO 日志。

## HashTable 所有权边界

TurboUtils Hash Map 拷贝固定大小 key/value。FlowStore 因此使用 owning entry 保存动态 key/value，
Hash Map 只保存稳定 byte-view 到 entry 的映射，并以 map value 保存 entry owner。删除顺序为：

1. 从 Hash Map 移除 view；
2. 释放 key/value buffer；
3. 回收固定 entry。

resize、remove、clear 或 destroy 后，旧内部 view 失效；公共 API 不返回这些 view。

## Bitmap 与时间序列

Bitmap 只适合整数成员集合或离散布尔时间槽。它不承载任意 payload、数值 sample、多事件时间槽
或排序游标。布尔时间序列的事实仍写入 TimeSeries/Log；bitmap 若启用，只是按时间桶重建的
查询索引。

2026-07-23 的 Windows ASan Debug 基线使用 10,000 个 `uint64` 成员并逐样本执行 4,096 次
membership 查询：通用 FlowStore HashMap 为约 2.31M ops/s，FlowStore BitmapIndex/CRoaring64
为约 5.45M ops/s。
因此整数集合的派生查询允许保留 bitmap；调用方通过 FlowStore 的 BitmapIndex API 使用它，不再
直接依赖 CRoaring。任意二进制 key/value、payload 与时间序列事实仍由
HashMap、Log 或 TimeSeries 承载。该数字只用于数据结构分工，不作为 Release 性能承诺。

## Redis 一致性

一个领域状态只能选择 local、Redis 或 PostgreSQL 为事实源，不做双主写入。需要同时修改 Hash 与
Set/ZSet 的 Redis 操作使用相同 hash tag，并由 Lua/事务原子提交。Redis 提交成功后才刷新本地
派生缓存；失败时本地派生索引失效并从 Redis 重建。

StateStore 的 Redis provider 使用二进制安全 Hash/Lua revision backend。点查使用 `HGET`；
`records` Hash 与 capacity metadata Hash 使用相同 cluster hash tag，写入和删除在一个 Lua CAS 中
同时更新 revision、records 和 bytes，因此初始化后的 mutation 为 O(1)，并支持多个独立 writer。
旧 namespace 首次 mutation 会在同一 Lua 临界区执行一次受 `max_records` 约束的 `HGETALL` 以重建
metadata，后续不再扫描。连接、协议、容量、配置不一致或 CAS 失败均原样返回，不降级到 local。

IndexStore 使用 Redis 原生 Set。namespace/index 二进制名编码为共享同一 hash tag 的 key；
membership、count、visit、intersection 分别落到 `SISMEMBER/SCARD/SMEMBERS/SINTER`。
add/remove 的 Lua 同时修改 Set 与 namespace 级 records/bytes 元数据，因此并发 writer 不会各自
推进容量事实。它不使用 Redis Stream 或 String，也不把 bitmap 当成远程事实存储。

LogStore 使用 Redis Stream 保存唯一 payload 副本，以 ZSet 保存可解析的
`cursor:timestamp:size` 有序元数据，以 Hash 保存 next/head/tail、records/bytes 和不可变容量配置；
三者使用同一 cluster hash tag。append 在修改前完成类型、配置、cursor、Stream 长度与 ZSet
基数预检，再由一个 Lua command 提交 prefix trim、XADD、ZADD 与 counters。FULL_REJECT 不消耗
cursor，TRIM_OLDEST/retention 只删除已规划的前缀；已裁剪 cursor 返回 `TURBO_ERANGE`。
同一 namespace 的多个 writer 必须使用相同 limits，不一致的 mutation 返回 `TURBO_EBUSY`。
单次 Lua 扫描和 read copy-out 均由 `max_operation_records` 限制，且该值不得小于
`limits.max_records`。Redis 错误不回退 local。

若未来 provider 声明 Redis TimeSeries capability，启动时缺少所需命令必须返回
`TURBO_ENOTSUP`，不隐式改用 Stream、Sorted Set 或 local。

## 删除与迁移

- 删除 `storage/`、`TurboFlow::FlowStorage`、`TURBO_FLOW_BUILD_STORAGE` 及其文档和测试。
- 删除 `flowqueue/`、`TurboFlow::FlowQueue`、`TURBO_FLOW_BUILD_QUEUE` 及其文档和测试。
- 新增 `flowstore/` 和 `TurboFlow::FlowStore`；FlowStore 属于完整产品构建图，
  不提供独立 feature option。
- 外部调用方按自身事实边界迁移到 Record/Index/Log/Series，不在 FlowStore 内复制协议状态机。

## 验证

Provider-neutral contract tests 覆盖容量边界、revision 冲突、二进制 key/value、Log cursor/trim、
TimeSeries 重复/乱序、关闭和故障注入。Benchmark 以 HashTable 为基线，对比稀疏/密集成员、
交并集、去重和布尔时间桶；仅在整数派生查询上证明收益后启用 bitmap。
