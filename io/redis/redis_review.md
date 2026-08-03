# io/redis 模块代码审查

## 概览

| 文件 | 大小 | 职责 |
|---|---|---|
| [flow_redis.c](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c) | 84KB / 1972行 | 核心实现：adapter、stream owner、blob/record store、publisher |
| [flow_redis_config.c](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_config.c) | 31KB / 612行 | YAML 解析 → 各 store 配置校验 |
| [flow_redis_state_store.c](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_state_store.c) | 20KB / 460行 | 状态投影存储（HSET/XPENDING + Lua 原子事务） |
| [flow_redis_index_store.c](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_index_store.c) | 21KB / 530行 | 索引存储（Redis SADD/SMEMBERS + Lua） |
| [flow_redis_log_store.c](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_log_store.c) | 30KB / 629行 | 操作日志存储（Redis Streams + XADD/XREAD） |
| [flow_redis_storage_backend.c](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_storage_backend.c) | 6KB / 156行 | StorageBackend plugin 分发层 |

---

## 总体评价

架构清晰，职责分层明确：
- `flow_redis_adapter_t` 是所有对 Redis 通信的统一载体（client/cluster/sentinel 三模式透明切换）
- `flow_redis_task_t` 将协程上下文的跳转逻辑和任务参数完整解耦，`flow_redis_run` 序列化调度
- 五种存储类型（stream、data、blob、record、state/index/log）共享同一个连接/执行基础设施，无重复造轮子
- Lua 脚本原子事务（`STATE_COMMIT`、`RECORD_COMMIT`）设计合理，服务端原子性保证强

---

## 问题

### HIGH

#### H1: `flow_redis_source_thread` 中 `group_rc` 在持锁路径上跨过 VLA/局部变量后未检查
[flow_redis.c:738–740](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L738-L740)
```c
int group_rc = flow_redis_run(adapter, &group);  // 声明在 lock 内
turbo_mutex_unlock(&adapter->lock);
if (group_rc != TURBO_OK) return;                // 已经 unlock，无问题
```
**事实**：此处 `group_rc` 声明在 `turbo_mutex_lock` 之后、`turbo_mutex_unlock` 之前，MSVC 混合 C89/C99 模式下在锁临界区内声明变量会触发 C2360（`initialization of 'group_rc' is skipped by 'goto'`）。虽然此文件无 `goto` 跳过该声明，但将变量声明放在锁块内部是脆弱风格。

**影响**：在较旧 MSVC `/Za` 模式下可能导致编译失败（此项目本就是 MSVC target）。

**建议**：将 `group_rc` 声明提到锁之前。

---

#### H2: `flow_redis_source_thread` 中 `redis_stream_result_free` 在 `rc != TURBO_OK` 时跳过
[flow_redis.c:753–756](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L753-L756)
```c
if (rc == TURBO_OK) rc = flow_redis_publish_results(adapter, &read);
turbo_mutex_unlock(&adapter->lock);
redis_stream_result_free(read.results, read.result_count);  // ← 正确
if (rc != TURBO_OK) break;
```
**事实**：`redis_stream_result_free` 在 unlock 之后，且无论 `rc` 为何都被调用——这是正确的。  
**结论**：此处无 leak，误报已排除。

---

#### H3: `flow_redis_adapter_create_store` 在 Sentinel 分支中 `lock_initialized` 和 `poll_wait_initialized` 初始化后，若后续 `calloc` 失败会导致 `flow_redis_shutdown` 中 double-destroy 风险
[flow_redis.c:1005–1073](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L1005-L1073)

**事实**：
- 第 1005 行 `calloc` 的 adapter，若 `tf_connection_init` 失败（L1012），直接 `free(adapter)` 返回——此时 `adapter->context == NULL`，`mutex.lock_initialized == 0`，无 double-free。
- 第 1059 行若 `adapter->context == NULL` 或 cluster/sentinel 创建失败，调用 `flow_redis_shutdown(adapter)`——`flow_redis_shutdown` 对 NULL 指针安全（`redis_client_destroy(NULL)` 等均有 NULL guard）。
- 第 1065–1068 行 `turbo_mutex_init` 之后 `tf_timer_init` 失败，调用 `flow_redis_shutdown` → 再次调用 `turbo_mutex_destroy`——**只要 `turbo_mutex_init` 内部保证成功后才设 `lock_initialized=1`** 即可安全。当前代码确实如此。
  
**结论**：无实际 double-destroy 风险，但 `flow_redis_shutdown` 中直接调用 `flow_redis_stop` 再 destroy 资源，`flow_redis_stop` 会调用 `redis_client_interrupt` → `redis_client_disconnect`，而此时 `client` 可能从未 connect——需确认 `redis_client_interrupt(NULL_or_unconnected)` 安全。**此处标为 MED，详见 M2。**

---

### MED

#### M1: `flow_redis_stream_owner_claim` 中 `token` 溢出路径不完整
[flow_redis.c:1240](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L1240)
```c
if (owner->claim_generation == UINT64_MAX) return TURBO_ERANGE;
```
**事实**：`claim_generation` 在 `owner_create_ex` 初始化为 0（calloc），每次成功 claim 自增。`UINT64_MAX` 在实践中不可达（每次 claim 需一次 Redis 读），但返回 `TURBO_ERANGE` 后没有任何文档说明调用方该如何处理。  
**推论**：调用方（如 `flowie_cluster_peer`）可能对 `TURBO_ERANGE` 无处理，直接向上传播为未分类错误。  
**建议**：在头文件 `turbo_flow_redis.h:140` 的 `turbo_flow_redis_stream_owner_claim` 文档中说明 `TURBO_ERANGE` 的含义。

---

#### M2: `flow_redis_stop` 对从未连接过的 adapter 调用 `redis_client_interrupt` + `redis_client_disconnect`
[flow_redis.c:850–857](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L850-L857)
```c
if (adapter->transport_mode == FLOW_REDIS_TRANSPORT_STANDALONE && adapter->client)
    (void)redis_client_interrupt(adapter->client, TURBO_ESHUTDOWN);
// ...
if (adapter->transport_mode == FLOW_REDIS_TRANSPORT_STANDALONE && adapter->client)
    redis_client_disconnect(adapter->client);
```
**事实**：当 `adapter->connected == 0` 时（从未 connect），`redis_client_disconnect` 调用是否安全取决于 `redis_client` 的契约，代码中未注释。  
**推论**：若 `redis_client_disconnect` 对未连接的 client 有副作用（如 double-free 内部 socket），`flow_redis_shutdown` 调用路径（store create 失败时）存在风险。  
**建议**：在 `flow_redis_stop` 的 standalone 分支添加 `if (adapter->connected)` guard，或在内部函数头注释中声明 `redis_client_disconnect` 可在 unconnected 状态下安全调用。

---

#### M3: `FLOW_REDIS_TASK_RECORD_COMMIT` 中 `calloc` 分配后，`snprintf` 失败时 `revisions/lengths/arguments` 同时被 free，但未将 `task->status` 以外的资源置 NULL
[flow_redis.c:555–568](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L555-L568)
```c
if (!arguments || !lengths || !revisions ||
    snprintf(max_records, ...) < 0 || snprintf(mutation_count, ...) < 0) {
    free(revisions);
    free(lengths);
    free(arguments);
    task->status = TURBO_ENOMEM;
    break;
}
```
**事实**：`snprintf` 返回 `< 0` 时的内存路径是正确的（三个都 free，break）。  
**推论**：但如果 `snprintf` 的 buffer 为 `char[32]` 而格式化的 `%zu` 超过 31 字节（`size_t` 最大值约 20 字节十进制），不会出现 truncation，此处安全。  
**结论**：逻辑正确，但 snprintf 失败原因与 ENOMEM 混用（snprintf 失败通常是 encoding error，不是 OOM）。  
**LOW 级** 建议：将 `task->status = TURBO_EINVAL` 用于 snprintf < 0 的路径。

---

#### M4: `flow_redis_config.c` 中 `flow_redis_blob_config_error` 和 `flow_redis_config_error` 是两套几乎相同的函数
[flow_redis_config.c:13–24](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_config.c#L13-L24) vs [flow_redis_config.c:126–137](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_config.c#L126-L137)

**事实**：两函数唯一差异是路径前缀 `$.adapters.%s` vs `$.channels.%s`。  
**推论**：若将来 error 结构扩展，两处需同步修改，维护成本高。  
**建议**：合并为一个函数，添加 `const char *root` 参数（`"adapters"` / `"channels"`）。

---

#### M5: `flow_redis_state_store.c` 与 `flow_redis_log_store.c` 各自独立 `uint64_t` 解析函数命名冲突风险
- `flow_redis_state_reply_u64` ([state_store.c:63](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_state_store.c#L63))
- `flow_redis_log_u64_parse` ([log_store.c:43](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_log_store.c#L43))
- `flow_redis_size_parse` ([index_store.c:47](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis_index_store.c#L47))

**事实**：三个函数都从字符串/reply 中解析无符号整数，语义相似，逻辑相近（>10 行，差异 <30%）。  
**建议**：抽取到 `flow_redis_internal.h` 一个共享实现，签名 `int flow_redis_parse_u64(const char*, size_t, uint64_t max, uint64_t*)`.

---

### LOW

#### L1: `flow_redis_adapter_t` 中 `connected` 字段无内存序保护，但在 source thread 和 stop 之间被多线程访问
[flow_redis.c:69](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L69) — `int connected;` (plain int)

**事实**：`connected` 在 `flow_redis_connect`（coroutine task, within lock）和 `flow_redis_stop`（main thread, after join）中修改。`stop` 在 join 之后才清零，因此实际上有 join-happens-before 保障。但读路径（`flow_redis_run` 中 `tf_connection_set_usage(... adapter->connected ...)`）也在 lock 内，故线程安全。  
**结论**：当前无数据竞争，但 `connected` 语义不直观（可改为 `atomic_int` 提升可读性）。

---

#### L2: `turbo_flow_redis_stream_owner_drop` 直接转发 `ack`，公开 API 文档声明语义不同
[flow_redis.c:1367–1368](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L1367-L1368)
```c
int turbo_flow_redis_stream_owner_drop(turbo_flow_redis_stream_owner_t *owner, uint64_t token) {
    return turbo_flow_redis_stream_owner_ack(owner, token);
}
```
头文件注释：`drop` = "Terminally XACK a failed entry **without classifying it as worker completion**"，`ack` = "XACK the matching active entry"。  
**事实**：当前实现相同，差异只在语义层。若将来 `ack` 需要触发 completion 统计而 `drop` 不需要，此处会静默失去区分。  
**建议**：哪怕现在语义相同，也保持独立实现（哪怕只是独立函数体），避免未来语义漂移。

---

#### L3: `flow_redis_blob_store_commit` 中错误处理链式写法容易误读
[flow_redis.c:1688](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L1688)
```c
int rc = flow_redis_blob_store_key(adapter, key);
if (rc != TURBO_OK || (!data && data_size > 0u)) return rc != TURBO_OK ? rc : TURBO_EINVAL;
```
**事实**：逻辑正确，但两个不同错误来源（key 不匹配 vs. 参数非法）被压缩进同一行的三目表达式，可读性差。  
**建议**：拆成两个独立 if。

---

#### L4: `flow_redis_source_thread` 中无 `flow` 指针 NULL 时的日志
[flow_redis.c:730–731](file:///c:/projects/cpp/turbonet/turbo-flow/io/redis/src/flow_redis.c#L730-L731)
```c
while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
       turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED)
    turbo_sleep_ms(1);
```
若 `adapter->flow` 为 NULL（正常不会，但 stop 竞争时可能），线程静默退出，无日志无状态迁移。  
**建议**：添加 assert 或 WARN 日志。

---

## 设计亮点（值得保留）

1. **Coroutine-based 异步 I/O 隔离**：`flow_redis_run` 将所有 Redis 命令放进协程上下文，caller 线程在自旋 `while (!task->done)` 驱动，避免了多线程回调地狱，同时 mutex 串行化保证了连接独占性。

2. **XPENDING 幂等 ACK 协议**：`ambiguous_ack` 字段 + `flow_redis_stream_owner_reconcile_ack` 在网络不确定时通过 `XPENDING` 查询精确 entry，实现了可靠的 at-least-once → exactly-once 收敛。

3. **Lua 脚本原子 CAS**：`STATE_COMMIT` 和 `RECORD_COMMIT` 用内嵌 Lua 脚本在 Redis server 端原子校验 revision/CAS，避免了分布式 read-modify-write 的竞态。

4. **三部署模式透明切换**：`flow_redis_transport_command` + `flow_redis_connect` 在 standalone/cluster/sentinel 间完全透明，上层代码无需感知。

5. **StorageBackend plugin 分发层简洁**：`flow_redis_storage_backend.c` 只有 156 行，dispatch switch 清晰，open/close 对称，instance 封装干净。

---

## 优先修复建议

| 优先级 | 问题 | 文件/行 | 建议 |
|---|---|---|---|
| MED | M2: stop 对未连接 client 调用 disconnect | flow_redis.c:850 | 添加 `connected` guard 或注释契约 |
| MED | M4: 两套几乎相同的 config error 函数 | flow_redis_config.c | 合并为带 `root` 参数的单函数 |
| MED | M5: 三套 u64 解析重复 | state/index/log_store.c | 抽取共享实现到 internal.h |
| LOW | L1: `connected` 未用 atomic | flow_redis.c:69 | 改为 `atomic_int` 提升可读性 |
| LOW | L2: `drop` 直接转发 `ack` | flow_redis.c:1367 | 独立实现以隔离未来语义 |
| LOW | L3: 链式三目表达式 | flow_redis.c:1688 | 拆成两个 if |
