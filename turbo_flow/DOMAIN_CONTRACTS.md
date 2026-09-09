# Domain 原语与操作契约

## 1. 目的

`turbo_flow_domain.h` 定义 TurboFlow graph 的 domain contract。Host 先注册 primitive
和 operation descriptor，再解析使用 `operation` / `resource` binding 的 DSL。Descriptor
承载可信的类型、作用域和 runtime boundary 要求；stage callback、adapter consume 和资源
实例生命周期仍由现有 owner API 管理。

每个 runtime node 都必须具有完整 operation contract。未显式声明 domain-specific
`operation` 的 node 在 compile 时解析为 `core.source` 或 `core.stage.inline/thread/coro/worker`；
这些 contract 按实际 executor、handoff、capacity、retry/reject 和 authority 生成，并可通过
`turbo_flow_stage_operation_at()` 查询。Core contract 不会隐式创建 Queue、Connection 或 Session。

## 2. Primitive descriptor

`turbo_flow_primitive_descriptor_t` 参数：

| 字段 | 说明 |
|---|---|
| `size` | 必须为 `sizeof(turbo_flow_primitive_descriptor_t)` |
| `name` | DSL `resource` 使用的唯一 binding name；registry 深拷贝 |
| `type_name` | domain 内稳定类型名，用于 operation/resource compatibility |
| `version` | 非零契约版本 |
| `domain` | Data、Execution、IO/Transport、Protocol/Pattern、Buffer/Persistence、Rules 或 Management |
| `kind` | `VALUE` 或 `RESOURCE`；DSL resource binding 只接受 `RESOURCE` |

Primitive registration 返回：

- `SALTS_OK`：注册成功；
- `SALTS_EINVAL`：字段、domain、kind 或 version 无效；
- `SALTS_EALREADY`：binding name 重复；
- `SALTS_EBUSY`：flow 已 compile/start；
- `SALTS_ENOMEM`：复制 descriptor 失败。

## 3. Operation descriptor

`turbo_flow_operation_descriptor_t` 定义：

- operation 自身所属 `domain`；
- input/output domain 与稳定 type name；没有输入或输出时 domain 为 `NONE` 且 type 为
  `NULL`；
- required resource domain/type；stateless operation 两者均为空；
- data、state、lifetime、concurrency、authority 五维 scope；
- source/stage/bridge role；
- 允许的 inline/thread/coro execution mask；worker 是独立的 bounded handoff，CNet/CHTTP 是
  外部 adapter-owned I/O placement，两者都不属于 execution mask；
- handoff、ordering、backpressure、cancellation、error、deadline 和 settlement runtime contract。

跨 domain input/output 必须声明 `TURBO_FLOW_OPERATION_BRIDGE`。`RESOURCE_OWNER` 或
`PROTOCOL_SESSION` state scope 必须声明 required resource。`OWNER_COMMAND` authority 只允许
Management domain，且不能绑定到 payload graph node。

`size` 必须覆盖当前完整 descriptor，registry 校验完整 runtime contract。截断的旧 descriptor
直接返回 `SALTS_EINVAL`，不再隐式补成 direct contract；registry query 返回深拷贝后的当前结构。

Runtime contract 不创建 executor 或 ring。它声明 operation 正确执行所需的边界，compiler
再与 DSL 已选择的 worker/reorder/retry/reject 配置核对。两者不一致时 fail fast。

## 4. 注册与组合示例

```c
#include "turbo_flow.h"

turbo_flow_t *flow = turbo_flow_create();

turbo_flow_primitive_descriptor_t session = {
    .size = sizeof(session),
    .name = "mqtt.session",
    .type_name = "MqttSession",
    .version = 1,
    .domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    .kind = TURBO_FLOW_PRIMITIVE_RESOURCE,
};

turbo_flow_operation_descriptor_t publish_in = {
    .size = sizeof(publish_in),
    .name = "mqtt.publish_in",
    .version = 1,
    .domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    .input_domain = TURBO_FLOW_DOMAIN_NONE,
    .input_type = NULL,
    .output_domain = TURBO_FLOW_DOMAIN_DATA,
    .output_type = "Message",
    .resource_domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    .resource_type = "MqttSession",
    .scope = {
        .data = TURBO_FLOW_DATA_SCOPE_MESSAGE,
        .state = TURBO_FLOW_STATE_SCOPE_PROTOCOL_SESSION,
        .lifetime = TURBO_FLOW_LIFETIME_SESSION_GENERATION,
        .concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT,
        .authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL,
    },
    .flags = TURBO_FLOW_OPERATION_SOURCE | TURBO_FLOW_OPERATION_BRIDGE,
    .execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE,
    .runtime = {
        .handoff = TURBO_FLOW_HANDOFF_DIRECT,
        .ordering = TURBO_FLOW_ORDERING_UNORDERED,
        .backpressure = TURBO_FLOW_BACKPRESSURE_NONE,
        .cancellation = TURBO_FLOW_CANCELLATION_NONE,
        .error_mode = TURBO_FLOW_ERROR_PROPAGATE,
    },
};

int rc = turbo_flow_register_primitive(flow, &session);
if (rc == SALTS_OK) rc = turbo_flow_register_operation(flow, &publish_in);
if (rc != SALTS_OK) {
  turbo_flow_destroy(flow);
  return rc;
}
```

对应 DSL：

```flow
source ingress adapter mqtt.server operation mqtt.publish_in resource mqtt.session
step validate operation data.validate

stage main {
  ingress -> validate
}
```

Host 仍需注册 `data.validate` descriptor、`validate` stage callback 和 `mqtt.server` adapter，
并由 adapter owner 执行 ingress。Source 不使用 stage `exec`；其 owner-context scope 通过
adapter owner 满足。DSL 不能修改 `publish_in` 的 session authority 或 owner-context 约束。

### 4.1 Node-local keyed state

需要按业务 key 保留运行时状态的 1:1 processor 使用
`turbo_flow_register_keyed_operation_provider()`。它不是另一套 DSL；node 仍通过
`operation <name>` 绑定 descriptor。合法 descriptor 必须声明：

- message data、`NODE` state、`RUNTIME_GENERATION` lifetime；
- inline-lane concurrency 和 `DATA_MUTATION` authority；
- direct handoff、inline execution、无 retry/deadline/settlement；
- propagate 或带显式 reject edge 的 reject error mode。

Store 是 caller-owned opaque owner，一个 store 同时只能绑定一个 provider 和一个 runtime
node。`key_selector` 返回 callback 当次借用的二进制 view；runtime 在返回后立即复制 key。
Processor 通过 callback-local `turbo_flow_keyed_state_get/put/delete()` 操作同一个 key。
PUT/DELETE 先暂存，只有 callback 返回 `SALTS_OK` 且 revision 未变化才原子提交；同 key 并发
更新返回 `SALTS_EBUSY`，runtime 不自动重放可能带外部副作用的 callback。

需要 0..N 输出时使用
`turbo_flow_register_keyed_emitting_operation_provider()`。其 input 为 const，processor 在同一
callback 中访问 keyed state 和 bounded emitter。Runtime 依次验证 callback、emitter sticky
status 和 state revision；三者全部成功后才提交 state 并把输出批次交给下游。callback 失败、
output bound 超限或 state conflict 都会丢弃整个输出批次且不提交 state。输出开始进入下游后，
后续 node 的失败不会回滚已经提交的 keyed state。

```c
#include <stdint.h>
#include <string.h>

static int select_client(const turbo_flow_msg_t *msg, vstr *key, void *ctx) {
  (void)ctx;
  *key = tstr_v_from_buf((const char *)&msg->id, sizeof(msg->id));
  return SALTS_OK;
}

static int increment(turbo_flow_msg_t *msg, turbo_flow_keyed_state_t *state, void *ctx) {
  vstr value;
  uint64_t count = 0;
  int rc = turbo_flow_keyed_state_get(state, &value, NULL);
  (void)ctx;
  if (rc == SALTS_OK) {
    if (value.len != sizeof(count)) return SALTS_EPROTO;
    memcpy(&count, value.data, sizeof(count));
  } else if (rc != SALTS_ENOENT) {
    return rc;
  }
  ++count;
  msg->type = (uint32_t)count;
  return turbo_flow_keyed_state_put(
      state, tstr_v_from_buf((const char *)&count, sizeof(count)));
}

turbo_flow_keyed_state_store_config_t config =
    TURBO_FLOW_KEYED_STATE_STORE_CONFIG_INIT;
config.max_entries = 4096;
config.max_key_size = sizeof(uint64_t);
config.max_value_size = sizeof(uint64_t);
config.max_total_bytes = 4096 * sizeof(uint64_t) * 2;

turbo_flow_keyed_state_store_t *store =
    turbo_flow_keyed_state_store_create(&config);
if (!store) return SALTS_ENOMEM;

turbo_flow_keyed_operation_provider_registration_t provider =
    TURBO_FLOW_KEYED_OPERATION_PROVIDER_REGISTRATION_INIT;
provider.operation_name = "data.count_by_client";
provider.key_selector = select_client;
provider.fn = increment;
provider.store = store;

int rc = turbo_flow_register_keyed_operation_provider(flow, &provider);
/* Register/parse/compile/start the matching descriptor and graph here. */
if (rc != SALTS_OK) {
  turbo_flow_keyed_state_store_destroy(store);
  return rc;
}
```

Store 必须晚于 borrowing flow 销毁。删除会在当前 generation 内保留 key 槽位与 revision，
防止 insert-delete ABA 并保证锁内提交不扩容；下一次 runtime generation 提交时统一释放。
因此 `max_entries` 是单 generation 的不同 key 上限，`max_total_bytes` 统计保留 key 与当前
value。通用 keyed store 本身不解释 event time 或 trigger；窗口语义由下述类型化 provider
建立在同一 revision transaction 之上。

Keyed emitting provider 已足够由应用实现确定性的 count-based tumbling aggregate：未达到
计数阈值时只 PUT accumulator，达到阈值时 DELETE/重置 accumulator 并 emit aggregate。
这只是 processor 语义，不等于 event-time window。

### 4.2 Event-time tumbling window

固定 event-time tumbling window 使用
`turbo_flow_register_event_time_window_provider()`。事件时间直接读取消息事实源
`turbo_flow_msg_t.ts_ns`；框架不调用墙钟，也不从到达顺序推断时间。Store config 显式声明
`window_size_ns`、`allowed_lateness_ns`、最大 window 数、应用 key/value 大小和总字节上限。
Runtime 以 `(window_start, application_key)` 隔离 accumulator；内部 window prefix 不占用
`max_key_size` 的应用配额，但仍计入 `max_total_bytes`。

`on_event` 接收 immutable message/window view 和 callback-local keyed state，只能更新当前
accumulator，不直接向下游发出原事件。Source/adapter owner 使用
`turbo_flow_advance_event_time_watermark()` 提交已经合并好的单调 watermark；多 source 的
min/idle-source 规则仍由该 owner 负责。达到 `window.end_ns + allowed_lateness_ns` 时，runtime
按 `(window_start, binary key)` 排序调用 `on_close`，并使用 bounded emitter 产生 0..N 输出。

Watermark 在扫描前提交，因此并发的迟到事件会在 state commit 点返回
`SALTS_ETIMEDOUT`。Close callback、emitter bound 或 revision 校验失败时 accumulator 保留；
caller 可用相同 watermark 重试。Close 成功时先删除 accumulator，再执行 downstream；后续
downstream failure 不恢复窗口，也不会在相同 watermark 上重复输出。`closed_windows` 统计
本次已经删除的窗口，即使其某个 downstream 随后失败。

单次 advance 对 `W` 个 active window 和 `C` 个 closable window 使用 `O(W + C log C)` 时间、
`O(W)` 临时引用；排序不在事件 publish 热路径。若 profiling 证明高频 watermark 扫描成为
瓶颈，再以预留 timer heap/index 替换扫描，并保留当前 revision/失败语义作为对照基线。

该切片只定义 fixed tumbling、event-time、watermark trigger 和 allowed-lateness eviction。
核心仍不读取 processing time，只接受 host 对
`turbo_flow_advance_event_time_watermark()` 的显式调用。本仓库不再提供周期 watermark owner。
外部 CNet/CHTTP adapter 如需自动推进，必须在事件被 `turbo_flow_publish()` 成功接受后更新其
单一事实源，再提交 `max_event_time - max_out_of_orderness`（下溢饱和为 0）。多 source 的
min/idle-source 合并、timer、失败状态、snapshot 和 stop/reset 生命周期均由该外部 owner
负责；缺少 owner 时 fail fast，不回退到 Graph 内部时钟。

尚未包含 sliding/session window、early/late trigger、side output、durable checkpoint 或
exactly-once sink transaction。Store 与 watermark 都属于 runtime generation，stop 后下一次
start 会清空；对应 owner 必须随 generation stop/reset，不能跨 generation 沿用旧观测值。

## 5. Compile validation

Compiler 对显式 operation node 执行：

1. operation 和 resource binding 存在；
2. source/stage role 匹配；
   source operation 无 graph input 且有 output，stage operation 必须声明 input；
3. required resource 是 RESOURCE，且 domain/type 匹配；
4. executable stage 的 selected executor 在 execution mask 内；worker segment 由 bounded
   handoff contract 声明；source execution 属于 adapter owner；
5. owner-context、pool、lock-free snapshot 与 authority 的组合合法；
6. Management/owner-command 不进入 payload graph；
7. 相邻两个显式 operation 的 output/input domain 和 type 完全一致；
8. bounded handoff 只绑定现有 worker Disruptor，且 capacity 必须与 DSL worker capacity 一致；
9. preserve-input worker 必须有 reorder boundary，reject/retry error mode 必须分别有 reject edge
   或 retry policy；
10. runtime 尚无 owner 的能力返回 `SALTS_ENOTSUP`，不只记录 metadata 后继续运行。

跨 domain 转换发生在单个 bridge operation 内。Graph edge 本身不做隐式转换，也不会根据
字符串相似度推断兼容类型。

## 6. 所有权与生命周期

- Registry 深拷贝 descriptor 中所有字符串；caller 可在注册成功后释放输入字符串。
- Query 返回 registry-owned pointer，有效期到 `turbo_flow_reset(flow, 0)` 或 destroy；
  `reset(flow, 1)` 保留 registry。
- Descriptor 不拥有 resource instance，也不提供执行 callback。
- Stage plan 中的 operation/resource name 是 flow-owned view，不得由 caller 释放。
- 注册、query、parse、compile、reset 和 destroy 由 host 串行化；当前 registry 不声明全并发
  mutation/query 安全。

## 7. 当前边界

当前可执行映射：

| Operation contract | 现有 runtime owner |
|---|---|
| direct handoff | 现有同步 edge dispatch，不增加 ring |
| bounded + block | worker-pool stage 的 bounded Disruptor ring |
| preserve-input worker | 现有 reorder boundary |
| cooperative cancellation | thread/coro/worker task executor |
| operation deadline | inline/adapter 返回后检查；thread/coro/worker task 协作取消 |
| reject error | named reject edge |
| retry error + retry settlement | adapter retry policy/callback |

`turbo_flow_segment_count()` 和 `turbo_flow_segment_plan_at()` 在 compile 成功后提供只读 lowering
结果。`WORKER_POOL` segment 的 width/capacity/runtime contract 可用于 tooling 和验证；direct、
fan-out、fan-in plan 是 lowering metadata，不表示每个逻辑 edge 都拥有独立 ring。

worker backpressure 当前支持 `block`、`fail` 和 `drop-newest`；`drop-oldest` 仍明确返回
`SALTS_ENOTSUP`，因为 active sequence 完成前没有可安全回收的旧 entry。generic
complete/requeue/dead-letter settlement、protocol ACK settlement 和 `SETTLE` error mode 只通过
显式 runtime owner callback 执行，不能由 adapter 私下从字符串 option 推断。

内建计算 executor 只有 `inline`、thread pool 和 coroutine pool。TurboFlow Policy expression evaluate
是 inline pure evaluator，不创建独立 pool。Disruptor worker 是 bounded data handoff/consumer
lane，CNet/CHTTP context 是外部 adapter-owned IO placement；两者都不是新的计算 executor 类别。
旧 `socket`、`io` 和 `custom` executor 已删除；扩展行为必须建模为 typed operation、adapter
owner 或公共 executor 上的 stage，不再绕过统一执行语义。

## 8. Runtime pool resource contract

Runtime-owned Disruptor、thread 和 coroutine pool 是首个落地 Management resource contract 的
资源族。`turbo_flow_pool_resource_status_at()` 返回 caller-owned
`turbo_flow_pool_resource_status_t`，其中包含：

- `uid`：稳定格式 `pool:<pool-kind-number>:<stage-name>`；同一 stage/kind 在 restart 或 resize
  后保持不变；
- `owner_name`：拥有该 pool configuration 的 stage name；
- `generation` / `observed_generation`：来自同一个 owner snapshot；
- 既有 `turbo_flow_pool_snapshot_t` load/capacity/counter；
- 从该 snapshot 一次性推导的 READY、ACCEPTING、DRAINED、SATURATED typed conditions。

调用方必须用 `TURBO_FLOW_POOL_RESOURCE_STATUS_INIT` 初始化 status。小于当前结构大小的 caller
返回 `SALTS_EINVAL`；更大的 future structure 可被接受，当前实现写入当前版本结构并将 `size`
报告为当前结构大小，不读取或承诺保留未知尾部。

`runtime_generation` 由 flow runtime owner 单独维护。成功 start/restart、成功 resize，以及
resize 失败后成功恢复旧配置的 rebuild 都会替换底层 pool 实例并推进 generation；stop 只将
最后一代 record 标为 STOPPED，不推进 generation；no-op resize 也不推进 generation。资源记录
只在该代 runtime 全部创建成功后提交为当前 generation。

`turbo_flow_pool_resize_command_t` 必须携带非零 `expected_generation`，且必须等于当前 runtime
generation；否则分别返回 `SALTS_EINVAL` 或 `SALTS_EBUSY`。旧的截断 command 和 unchecked
调用语义已删除。Observe pool reconcile 从同一个 status
读取 load/capacity 和 observed generation，再发送 checked command；Observe 不修改 Status，也
不拥有 controller thread。

通用 `turbo_flow_resource_reconcile_tick()` 只比较 caller 捕获的 immutable metadata、typed
value 和 Conditions，并在 observed generation 已追平时最多发一条 typed command。多 owner
resize 使用 caller-owned `turbo_flow_resize_workflow_state_t`；初始化时深值复制 pointer-free
spec，随后按 quiesce ingress、drain graph、resize pool、resume graph、resume ingress 推进。
失败不回滚其他 owner，不隐藏 retry；state 记录失败 phase、幂等 attempt 和各 owner 最终有效
generation。状态不保存 message、payload、adapter pointer 或任意 callback。

Connection、Queue、Protocol aggregate、Storage、Rule set、Runtime、Segment 和 Pool 均通过
owner metadata 暴露 stable UID/generation。UID 的稳定范围由 owner 生命周期决定，不能把一次
进程内实例的 UID 宣称为跨部署永久标识。

## 9. Schema-backed resource document

Domain-specific Spec/Status/Condition/Event 不继续扩张一个万能 C struct。公共层使用
`turbo_flow_resource_document_t` 作为固定 ABI envelope，只承载：

- domain、resource kind、document kind；
- stable UID、owner、generation、observed generation；
- `turbo_flow_resource_schema_t` schema identity；
- caller-owned immutable `mem_buffer_t` payload。

Schema identity 由 domain、resource kind、document kind、schema name、type name、schema ID、
schema version 和 encoding 联合确定。Schema descriptor 由可信 provider/registry 拥有；document
只借用 descriptor，并拥有 payload 引用。调用方必须以 `TURBO_FLOW_RESOURCE_DOCUMENT_INIT`
初始化，在复用或退出前调用 `turbo_flow_resource_document_cleanup()`。Query 拒绝过小 ABI 和
仍持有 payload 的输出对象，避免覆盖 owned snapshot。

Pool provider 通过 `turbo_flow_pool_status_document_at()` 输出 JSON encoding 的
`TurboFlowResource.PoolStatus` V1 document。`turbo_flow_pool_status_schema()` 暴露其可信静态
schema。测试使用真实 DataBind 从 schema text 创建 codec、校验 schema ID/version、动态绑定
document，并按字段读取 parallelism、capacity 和 accepting。DataBind 只位于 consumer/test
侧；`TurboFlow::Graph` 不公开 `DataBindValue`，因此 core-only build 不增加 DataBind/JIT
依赖。

Pool counters/capacity 是 `uint64_t`。JSON number 无法精确表达全部 64 位整数，因此 V1 schema
将这些字段定义为十进制 `string`；consumer 必须 checked parse，不能经 `double` 转换。枚举/
`uint32_t` 使用 JSON number，predicate 使用 bool。后续 TBE encoding 可以提供紧凑二进制表示，
但必须保持同一 schema version 的字段语义。

HTTP、S3、Database 等 Domain 各自注册 schema/type，例如 `HttpEndpointStatus`、
`S3RequestStatus`、`DatabasePoolStatus`；共享的是 envelope、lifecycle 和 DataBind projection，
不是字段集合。S3 可以同时引用自己的 request status 与底层 HTTP connection resource，但不能
把两者合并成第二事实源。

Core 为九种 canonical domain/resource pair 注册独立的 common-governance DataBind schema：
Connection、Queue、Pool、Runtime、Segment、Protocol、Storage 和 RuleSet。公开
`turbo_flow_resource_governance_schema()` 仅返回匹配 domain/kind 的 Spec、Conditions 或 Event
schema；Status 始终优先使用 owner-native document，Command 仍走强类型 owner dispatcher。
owner 若提供同类文档则其结果优先；只有 callback 缺失或明确返回 `SALTS_ENOTSUP` 时，core 才从
同一次 metadata/snapshot 生成 common document，其他 owner 错误不被 fallback 掩盖。

Common Spec 的 `capacity` 是 owner snapshot 声明的容量契约，使用十进制 string；零表示该资源
没有 common bounded-capacity 约束。Conditions 固定为 READY、ACCEPTING、DRAINED、SATURATED，
status 和 reason 均由同一 snapshot 纯计算。Event 是“最新不可变观察点”，`sequence` 等于
`observed_generation`；`gap=true` 只表示 `observed_generation < generation`，不是丢失历史事件
的计数，也不构成 replay journal。reason 固定区分 `OBSERVATION_CURRENT`、
`OBSERVATION_LAGGING` 和 `OWNER_ERROR`。重复查询不会推进 sequence/generation；需要历史重放的
owner 必须提供自己的 bounded/durable event journal。

Schema document 是 owner 状态的派生 snapshot，不是业务 payload，也不是任意 command bag。
Observe/Rules/exporter 可读取它，但不能修改它或通过字段名直接执行副作用。Command 仍必须在
owner boundary 校验 command kind、schema/type/version、expected generation、deadline、权限和
幂等语义。Credential、token、SQL text、HTTP body、S3 object content 等敏感/业务数据不得进入
management schema。

Observe exporter 继续只从 owner snapshot/document 导出派生指标，不持有或推进 generation，
也不成为 Spec、Status、Condition 或 Event 的第二事实源。

## 10. Opaque payload 与 schema-bound projection

业务消息与 management resource document 使用不同的 schema identity。业务消息使用
`turbo_flow_data_schema_t`，不携带 resource kind、document kind、UID 或 generation；resource
document 继续使用 `turbo_flow_resource_schema_t`。Data schema 同时区分业务 `type_name` 与稳定
`projection_type`，例如 `Order` 与 `Salts.DataBindValue`，consumer 必须校验 provider value
type 后才能强转 projection。两种 schema identity 不能混用，因为生命周期、权限和错误语义
不同。

`turbo_flow_msg_t` 的 payload bytes 始终是事实源，且可在无 schema 时作为 opaque data 处理。
DataBind、协议 parser 或其他可信 provider 可通过 `turbo_flow_msg_bind_projection()` 附加派生
projection。Core 只保存可信 schema identity、opaque projection pointer 和 clone/destroy hooks，
不依赖 DataBind 类型或 JIT。`turbo_flow_msg_clear_projection()` 只删除派生 projection，不修改
原始 payload。

Descriptor 与 projection 的 domain 可以不同：例如 HTTP 或外部协议 descriptor 描述协议入口，DataBind
projection 使用 Data domain 描述解析结果。跨 domain 不代表存在两份 payload identity；只要 descriptor
声明了 schema，projection 的 encoding、schema name、type name 与 version 就必须完全一致。无论先附加
descriptor 还是先绑定 projection，冲突都返回 `SALTS_EPROTO`，失败不接管调用方 projection。

Projection clone 是 provider capability，不是 Core 对未知对象的推测。提供 clone hook 时，
message clone 得到独立 projection；未提供时，clone/retry/fan-out 返回 `SALTS_ENOTSUP`。DataBind
adapter 使用 DataBind owner 模块公开的 `data_bind_value_clone()` 注册 clone hook，因此 cloned
message 拥有独立值树；TurboFlow 不遍历或序列化绕过 DataBind 私有值树。其他 provider 未提供
clone hook 时仍按 generic contract fail fast。

## 11. Content descriptor 与 domain lookup

业务 payload 的格式身份由独立 `turbo_flow_content_descriptor_t` 表达。Message 只借用 immutable
descriptor，不复制也不释放；adapter/host owner 必须覆盖 message 及其 clone 的生命周期。Descriptor
将 normalized media type、派生 encoding、schema name/type/version、domain profile 和非敏感 payload
identity 分开，identity 只用于诊断，不参与 schema lookup。

Content registry 由 host 显式 create/destroy，不使用 singleton。注册项深拷贝 schema identity 与
projection type；`schema_text` 必须为 NULL，schema 文本加载/编译属于可信 provider，而不是运行时
registry。解析键为 `(domain, profile, media type, schema name/type/version)`；未声明 schema 时只允许
保持 opaque 并返回 `SALTS_ENOENT`，不得根据唯一候选猜测 schema。

HTTP、S3、Database 和外部协议 adapter 只负责把协议元数据归一化为 lookup key。未知 media type 且
没有显式 binding 时保持 opaque；binding 不完整、已声明 schema 不匹配或 registry 冲突时 fail
fast。S3 sink 使用配置的 PUT Content-Type，并在外部请求前校验 payload；S3 source 先从
StatObject/HEAD 读取对象实际 Content-Type，再为本次 GetObject message 构造 descriptor，不能用 PUT
配置代替远端 metadata。PostgreSQL sink 识别/校验 parameter payload；query adapter 根据真实
`PGresult` 输出 rowset/command descriptor，并可通过显式 row mapper 附加 schema-bound projection。
MQTT 使用 `MQTT_APPLICATION` / `MQTT_CONTROL` profile 和同一 registry contract，协议实现仍留在
外部 `turbo-mqtt` 仓库。
