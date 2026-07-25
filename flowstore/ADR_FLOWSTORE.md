# ADR：FlowStore 统一状态、索引、日志与时间序列

## 状态

已接受，2026-07-23。

## 背景

旧 `FlowQueue` 同时承载队列、SQLite/Redis 适配和部分通用存储职责，旧
`FlowStorage` 又单独提供文件、目录、append-log 与 SQLite sink。这两个模块没有形成统一的
数据所有权、容量和查询契约。新设计删除两个旧模块，不保留其 API、配置或行为兼容性。

FlowStore 独立于 Flowie。Flowie 只依赖类型化存储契约，不拥有具体 HashTable、Redis 命令或
CRoaring 实现。

Record 的公共契约位于基础 `turbo_flow/include/turbo_flow_record_store.h`，因为它是
Flow/TurboFlow 的稳定数据边界；它不反向依赖 FlowStore。FlowStore 提供 State/Index/Log/
Series facade，具体 backend 实现则由 `io/common/storage` 的 `StorageBackend` registry 按
model 单独装配；local 的实现位于同级 `tf_local_storage` shared library，而不是 Flowie 内部
direct-factory。

面向 Flowie 的 MQTT 事实访问统一经过 `turbo_flow_mqtt_store_t`（
`include/turbo_flow_mqtt_store.h`）。它只借用 registry 提供的 Record service，封装 scan、
CAS/atomic commit 和容量元数据；Flowie 不得在门面构造后直接调用 Record callback。

### MQTT 事实源不变量

Flowie 的 MQTT 业务事实必须由 FlowStore 管理，不能由 Flowie owner 的容器独立推进。事实
包括 session、subscription、inflight、retained 和 Will；默认使用进程内 `local` Record
backend，显式 `session_store` 才选择 Redis/PostgreSQL。Flowie owner 内的 session/vector、
topic trie、member map 和 retained map 只能作为从 Record 重建的单 owner cache，用于协议调度
和查询加速；每次状态迁移必须先完成 FlowStore CAS/atomic batch，再交换 cache owner，失败
不得推进 cache。

连接对象、解析缓冲、发送队列、CoroNet lane 状态和 ACL/control-plane SQLite 不属于 MQTT
业务事实：前者是传输运行时状态，后者是独立管理面，均不通过 FlowStore 承载。

## 决策

FlowStore 及其 Record contract 分为五种不能互换的数据模型：

| 模型 | 事实语义 | local backend | Redis backend | PostgreSQL backend |
|---|---|---|---|---|
| Record | 带 revision 的批量 CAS 记录 | volatile linked store（atomic，不 durable） | Hash/Lua | transaction table |
| State | 带 revision 的最新键值状态 | TurboUtils Hash Map | Hash | - |
| Index | 无序成员集合和映射查询 | TurboUtils Hash Map/Set | Set 或 Sorted Set | - |
| Log | 带单调 cursor 的有序事件历史 | 有界分段日志 | Stream | - |
| TimeSeries | 按时间排序的标量采样 | 有界有序 chunk | -（未声明 Series capability） | - |

CRoaring 不保存事实数据。它封装在有硬容量上限的 `turbo_flow_bitmap_index_t` 中，只允许作为
可从 IndexStore 重建的派生加速索引，并且必须由代表性 benchmark 证明其相对 HashTable 的收益。

`turbo_flow_store_route` 先按数据语义选择上述模型，再比较部署配置给出的本地
`records/bytes/item/write-rate/retention` 上限。local 是由 `tf_local_storage` DLL 提供的
进程内、非 durable 事实源；要求 durable 或跨进程共享时选择 Redis/PostgreSQL，并由 storage
backend capability 明确拒绝不支持的模型，不自动静默退回 local。

`io/local`、`io/redis` 和 `io/pgsql` 是同级 backend 模块，只通过 StorageBackend 的
`open()/close()` function table 暴露 provider-neutral service。Flowie 启动时先创建 registry、
注册三个可用 API，再由 owner 创建并持有 service；Flowie 不直接调用 local 容器或远程数据库的
`create_*`/`destroy_*` 函数。当前 `tf_redis` 不声明 Series capability，TimeSeries 路由必须
在 `open()` 前被拒绝，而不是退回 Stream、Sorted Set 或 local。

## 公共协议

### 数据单元

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
因此整数集合的派生查询允许保留 bitmap；Flowie 通过 FlowStore 的 BitmapIndex API 使用它，不再
直接依赖 CRoaring。任意二进制 key/value、payload 与时间序列事实仍由
HashMap、Log 或 TimeSeries 承载。该数字只用于数据结构分工，不作为 Release 性能承诺。

## Redis 一致性

一个领域状态只能选择 local、Redis 或 PostgreSQL 为事实源，不做双主写入。需要同时修改 Hash 与
Set/ZSet 的 Redis 操作使用相同 hash tag，并由 Lua/事务原子提交。Redis 提交成功后才刷新本地
派生缓存；失败时本地派生索引失效并从 Redis 重建。

StateStore 的首个 Redis provider 复用既有二进制安全 Hash/Lua revision backend。点查使用
`HGET`，写入先以一次有界 `HGETALL` 快照核算 `max_records/max_bytes`，再执行 Lua CAS。
因此该 namespace 只允许一个 FlowStore mutable writer；revision 冲突仍由 Redis 原子拒绝，但
总字节准入不声称支持多个独立 writer。连接、协议、容量或 CAS 失败均原样返回，不降级到 local。

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
- 新增 `flowstore/`、`TurboFlow::FlowStore` 和 `TURBO_FLOW_BUILD_STORE`。
- Flowie 按 Session/Inflight/Will、subscription index、Retained、PUBLISH Log/Series 的顺序迁移。

## 验证

Provider-neutral contract tests 覆盖容量边界、revision 冲突、二进制 key/value、Log cursor/trim、
TimeSeries 重复/乱序、关闭和故障注入。Benchmark 以 HashTable 为基线，对比稀疏/密集成员、
交并集、去重和布尔时间桶；仅在整数派生查询上证明收益后启用 bitmap。
