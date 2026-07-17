# TurboFlow 原语图架构

## 1. 决策状态

本文定义 TurboFlow 通过 graph 组合数据、协议和资源操作的目标架构。它是迁移契约，
不表示下述接口均已实现。当前行为仍以公开头文件、测试和 `ARCHITECTURE.md` 为准；
实现工作在 `plan.md` 中以 `- [ ]` 跟踪。

核心决策是：

> TurboFlow graph 组合小型原语与操作。每个可变资源只有一个 owner，`disruptor.h`
> 连接有界运行数据段，host-owned control loop 根据只读 Status 将期望 Spec reconcile
> 为发给 owner 的类型化 Command。

这是借鉴 Kubernetes 的管理模型，不引入 Kubernetes API 或分布式控制面依赖。

当前仓库已提供 message envelope、typed resource snapshot、adapter command/ops、
immutable control facts、runtime pool snapshot、host-owned pool reconcile 和 rule
processor。目标模型组织并扩展这些 API，不会未经兼容阶段直接替换它们。

## 2. 四个平面

```text
Management plane
  Spec -> Observe Status/Conditions -> Reconcile -> typed Owner Command
                ^                                      |
                +-------------- next snapshot <--------+

Data plane
  Input -> Disruptor segment -> Processor/Route/Buffer/Batch -> Settlement -> Output

Protocol and message-pattern plane
  MQTT/FMQ/HTTP/SMTP/POP3/Redis parser、session、FSM、correlation、ACK、retry boundary

Transport plane
  CoroNet context 和 TCP/TLS/WS/Pipe/UDP/KCP byte transport
```

依赖只允许向下。管理面观察各平面，但不搬运业务 payload。协议 owner 可以向数据面提交
或接收消息，但 graph processor 不得穿透 adapter 修改 protocol session。Transport 不感知
graph、协议模式或规则语义。

## 3. 谁在干什么

| Actor/owner | 唯一事实源 | 允许执行 | 不得持有 |
|---|---|---|---|
| Graph compiler | immutable topology、schema、stage binding、execution plan | parse、validate、lower、compile | live connection 或 protocol state |
| Runtime owner | lifecycle、publish admission、task、completion | start、submit、yield、cancel、drain、stop、resize | adapter endpoint 或 protocol session |
| Disruptor segment | claimed slot、sequence、active task handoff | claim、publish、wait、consume、release | durable message、配置、session、QoS state |
| Protocol/pattern owner | parser、session registry、FSM、correlation、ACK、reconnect | encode/decode、推进合法状态、协议 settlement | graph topology 或通用 storage policy |
| IO adapter owner | endpoint、connection、admission、adapter inflight | start、quiesce、resume、send、interrupt、replace | graph scheduling 或 observer state |
| Queue/storage owner | buffered/durable record 及交付状态 | enqueue、reserve、ack、requeue、recover | socket connection 或 protocol session |
| Rules engine | immutable compiled rules、evaluation-local facts | evaluate 并返回 typed action/decision | resource mutation、IO、scheduling、protocol ACK |
| Observe | derived counter、event、immutable snapshot | read、aggregate | 任意状态迁移 |
| Host reconciler | desired Spec、policy memory、command sequencing | 比较 Spec/Status、推导 Condition、发 Command | payload 或重复的 live resource state |

“谁管理谁”表示 command authority，不表示共享状态所有权：

- host 通过 typed command 管理 graph runtime 和已注册资源；
- runtime 管理 execution segment 和 task lifecycle；
- adapter 管理 endpoint/connection，protocol owner 管理 session/FSM；
- Observe 和 Rules 只提供事实或决策，不成为 owner；
- Disruptor 协调数据移交，不管理 entry 所代表的 actor。

禁止跨 owner 直接修改状态，禁止 Observe callback 推进状态，禁止 rule action 获取 adapter
内部指针。

## 4. Domain、Primitive、Operation 与 Graph node

Primitive 和 operation 的语义首先由 domain 决定，不能脱离 domain 建立一套万能动词。
四者的关系是：

```text
Domain
  定义 vocabulary、data type、state invariant、error/settlement semantics
    -> Primitive
       domain 内最小、可独立说明所有权和生命周期的 value/resource capability
    -> Operation
       对 domain data 或 primitive 执行的一个有边界动词
          -> Graph node
             operation + resource binding + immutable config + execution policy
```

### 4.1 Primitive 边界

Primitive 是 domain 内的稳定名词，不是任意 callback，也不是 graph stage 的同义词。它分为：

- **Value primitive**：不可变或显式 move/retain 的数据值，例如 `MessageEnvelope`、`Schema`、
  `EndpointSpec`、`Batch`、`FactsSnapshot`、`Decision`；它只有 value ownership，没有独立
  controller。
- **Resource primitive**：拥有可变状态和不变量的 opaque resource，例如 `Connection`、
  `ProtocolSession`、`Queue`、`ExecutorPool`、`DisruptorSegment`、`RuleProgram`；它必须有唯一
  owner、生命周期、typed Status 和允许的 Commands。

Primitive 的最小性以 domain invariant 为界，而不是以代码行数为界。若拆开后两个对象必须
共享锁、共同提交状态或无法独立恢复，它们仍属于同一个 primitive；若一个对象包含两个可
独立替换、独立观察、独立失败的状态机，则应拆分。

### 4.2 Operation 边界

Operation 是 domain 内的稳定动词。一个 operation 必须只有一个主要 effect，并完整声明
input、output、state access、side effect 和 error/settlement。按权限分为：

| Operation 类别 | 状态权限 | 示例 | Graph 中的位置 |
|---|---|---|---|
| Pure data operation | 只读输入，输出新值或 private mutation | decode、validate、filter、map、route | payload graph node |
| Owner-local operation | 由 owner 在其串行化边界内推进状态 | session parse、queue reserve、pool submit | owner 内部或 adapter node |
| Owner command | 跨 owner 的类型化、幂等状态请求 | quiesce、resume、resize、replace endpoint | management workflow，不走 payload edge |
| Observe operation | 只读 snapshot，不产生控制副作用 | snapshot、metrics aggregate、derive Condition | management plane |
| Settlement operation | 将 data attempt result 转为 owner 决策/command | ACK、requeue、retry、dead-letter | data/protocol 或 data/storage bridge |

`Input`、`Processor`、`Route`、`Buffer`、`Batch`、`Output`、`Settlement` 和 `Observe tap`
是 graph operator role，不全是 primitive。例如 Buffer node 是 `enqueue/reserve` operations
绑定一个 Queue/Buffer resource；Batch node 是 `collect/flush` operations 产生 Batch value；
Input/Output 是跨 domain bridge 的方向性角色。

Operation 不拥有 thread/coroutine/ring。内建计算 executor 只有 `inline`、thread pool 和
coroutine pool，由 runtime 绑定到 node。Disruptor worker 是 bounded data handoff/consumer
lane，不是第四种 executor；CoroNet context 是 IO owner placement，也不是计算 executor。
Operation 不能因为被 graph 调度而获得额外状态权限。

### 4.3 Domain catalog

公共 module catalog 位于 domain contract 之上、Graph DSL 之下。它声明 module identity/version、
能力边界、primitive type/operation exports、依赖范围以及 typed provider 归属；它不加载代码、
不创建资源实例，也不接管 native transport。可信 host/module code 在 compile 前调用
`turbo_flow_register_module()`，再以 `turbo_flow_bind_operation_provider_module()` 将既有
`(operation, resource)` provider 绑定到唯一 module owner，或以
`turbo_flow_register_module_adapter()` 原子绑定 native adapter operations。依赖必须按拓扑顺序注册；Graph DSL
仍只引用 `operation`/`resource`，YAML 仍只选择已注册能力，不能声明 native function。

| Domain | 主要 primitives | 主要 operations | 状态 owner |
|---|---|---|---|
| Data | MessageEnvelope、Schema、Batch | decode、validate、transform、filter、route、split、merge | message/batch owner |
| Execution | Task、ExecutorPool、DisruptorSegment | submit、yield、cancel、wait、drain、resize | runtime |
| IO/Transport | EndpointSpec、Listener、Connection、Stream/Datagram | listen、connect、read、write、interrupt、quiesce | adapter/CoroNet context |
| Protocol/Pattern | Frame、Session、Subscription、Correlation、DeliveryState | parse、encode、publish、request、reply、subscribe、settle | protocol owner |
| Buffer/Persistence | Queue、Record、Checkpoint | enqueue、reserve、ack、requeue、recover | queue/storage owner |
| Rules | RuleProgram、FactsSnapshot、Decision | compile、evaluate | rules owner；evaluation 无副作用 |
| Management | ResourceRef、Spec、Status、Condition、Command | observe、diff、reconcile、apply command | host reconciler + target owner |

HTTP、SMTP、MQTT、FMQ、Redis 等协议可以各自形成 protocol subdomain，并按各自实现需要复用
IO/Transport primitives。复用 connection/endpoint 不代表共享协议 FSM、options 或 transport
实现。HTTP 已有 TurboHTTP/Iris native endpoint/adapter，保持该 owner 与 IO 路径，不迁移为
`io/socket` primitive。`io.socket` 将 CoroNet endpoint 注册为 `SocketEndpoint`；
`io.http.client/server` 分别将 native client/endpoint 注册为 `HttpClientConnection` /
`HttpServerEndpoint`；RPC 同理注册自己的 client/server resource。它们共享的是
module-adapter-resource 校验机制，不共享协议 FSM、连接池或 native transport 实现。
RPC client 可由可信 host 通过 versioned binding 显式注入 borrowed/owned `http_client_t`，使其复用既有
TurboHTTP provider 配置；未注入时仍创建私有 client。Borrowed client 必须比 RPC adapter
活得更久，且不能被另一个 adapter 并发驱动。该 host object 不可由 YAML 构造。

`turbo_flow_protocol` 还提供一个不拥有候选对象的 pattern core：role compatibility、fan-out/
round-robin candidate iteration、generation-fenced route matching，以及单 correlation 的同步
exchange 状态。它不解析 wire frame，不保存 topic/filter、peer、payload、queue、socket 或 ACK。
FMQ 将 PUB/SUB、PUSH/PULL、REQ/REP、ROUTER/DEALER 映射到这些机制；Flowie 只复用候选迭代和
route matching，MQTT wildcard/shared-subscription、CRoaring membership、QoS 与 session 状态仍由
Flowie owner 独占。这样共享的是可复验的选择/关联算法，不是协议状态或第二套 runtime。

`MessageEnvelope` 同时承载 schema-bound 和 opaque data。Payload bytes 是唯一内容事实源；
schema-bound 只表示 envelope 上附加了可选派生 projection，不要求所有 ingress 预先知道格式。
Projection 的 schema identity、clone 和 destroy 由可信 provider 定义。Graph operation 可声明
是否需要特定 schema/type；不需要 schema 的 operation 继续处理 opaque bytes。任何 projection
都可清除并从原始 bytes 重建，不能独立推进业务状态。

### 4.4 Operation 作用域

每个 operation descriptor 必须同时声明五个作用域，compiler/runtime 不允许隐式扩大：

| 作用域 | 必须回答的问题 | 典型值 |
|---|---|---|
| Data scope | 一次处理哪些数据，能否保留或复制 | message、batch、stream chunk、snapshot |
| State scope | 可读写哪个事实源 | none/private、node、graph、resource owner、protocol session、adapter owner |
| Lifetime scope | 引用可活多久 | call、dispatch、task、session generation、runtime generation |
| Concurrency scope | 谁串行化，是否可重入/并发 | inline lane、single owner context、pool、lock-free snapshot |
| Authority scope | 能产生何种副作用 | pure、observe-only、data mutation、owner-local、typed command |

除上述五项外，还必须声明 input/output schema、ordering、capacity/backpressure、deadline/
cancellation、error mapping 和 settlement boundary。Descriptor 是 compile-time/runtime
validation metadata，不是包含所有 domain 方法的胖 vtable。

前三个切片已实现公共 descriptor registry、DSL binding、operation runtime contract
和 runtime pool resource status：host 通过
`turbo_flow_register_primitive()` / `turbo_flow_register_operation()` 注册可信契约，DSL 使用
`operation <name>` / `resource <name>` 组合 node。Compiler 已校验 source/stage role、resource
domain/type/version range、execution mask、owner/pool concurrency scope、management authority 和相邻显式
operation 的 input/output domain/type，并把 bounded backpressure、ordering、retry 和 reject 要求核对
到既有 worker Disruptor、reorder、adapter retry 和 reject edge。所有 runtime node 都解析为
完整 operation；未显式绑定的 node 使用可查询的 `core.source` / `core.stage.*` concrete contract，
未显式绑定的 callback/adapter 仍是兼容路径，不能计入 module-level executable proof。对于已进入
module catalog 的 operation，compiler 会校验 typed provider 或 native adapter 已绑定唯一 module
owner；resource-owned adapter operation 还必须匹配注册时固化的 primitive name。Legacy callback
不能冒充 cataloged operation，`ADAPTER_OWNER` 必须由 typed adapter 执行。
Runtime pool 已提供 stable UID、generation、typed Status/Condition 和 checked
resize command；operation execution deadline 已接入 inline、thread/coro pool、worker lane
以及同步 adapter 边界。Worker runtime 已实现 block、fail 和 drop-newest；drop-oldest 因 active
sequence 不能安全回收而返回 `TURBO_ENOTSUP`。Generic complete/requeue/dead-letter/canceled 和
protocol ACK settlement 已通过显式 owner callback 接入全部 compute/handoff path；缺少 owner 或
请求 operation 未声明的 action 时 fail fast。Connection、Queue、Storage 和 protocol owner 已
提供 owner-scoped UID 与 generation-aware snapshot；其中固定 generation 为 `1` 的 immutable
owner identity 不能被解释为跨 rebuild generation。完整的 typed Spec/Event 仍按 resource kind
逐步扩展，不建立万能 resource union。

Data operation 现已补充三种显式 provider 能力：bounded emitter 为 0..N filter/map/flat-map
提供 caller-owned 输出批次；node-local keyed state 为 1:1 processor 提供有界、按 key 隔离的
runtime-generation 状态事务；event-time tumbling provider 以消息 `ts_ns` 和 owner 提交的
monotonic watermark 关闭有界 keyed accumulator。三者都复用现有 `operation` DSL binding，
不引入第二套控制语法。Keyed callback 不持 store lock，成功后以 per-key revision 提交；冲突
返回 `TURBO_EBUSY`，不隐式 retry。Window close callback 同样不持 store lock；watermark 先
单调提交，再按 `(window_start, binary key)` 关闭窗口，从而使并发迟到事件在 commit 点失败。
Source/adapter owner 负责合并多输入 watermark，store 不承担网络/session owner 或持久化
checkpoint。

两种能力也可通过 keyed-emitting provider 在一个 node 内组合：callback 读取/暂存单 key
state 并收集 0..N outputs，runtime 只在 callback、输出边界和 revision commit 全部成功后释放
该批次。这个组合继续承载应用定义的 count-based tumbling aggregate；类型化 event-time
provider 则补齐 fixed tumbling、watermark trigger 和 allowed lateness。两者都不会在
downstream failure 后反向回滚已提交 state。Processing-time timer、sliding/session window、
复杂 trigger、checkpoint 和 exactly-once sink transaction 仍是独立后续契约。

当前 RulesForge 集成再向前推进一个切片：`rules.forge` 是第一个生产 module catalog entry，
导出 `RuleSet` primitive type 与 `rules.apply` operation，并将每个 `(rules.apply, RuleSet instance)`
typed provider 绑定回该 module。`rules.apply` 是 Rules domain 的显式
stage operation，`RuleSet` 是带 owner/resource contract 的 primitive。Host 通过
`turbo_flow_rule_register_data_operation()` 绑定一个规则资源；compiler 按
`(operation, resource)` 选择 executable provider，旧的按 stage callback 注册方式继续
作为兼容路径。这样规则节点可以放在任意满足 Message 类型边的 DAG 位置，而不把规则
程序、资源状态或执行调度塞进 parser/descriptor。无 schema 的 processor 可以直接使用；
带 schema 的 processor 必须通过
`turbo_flow_rule_facts_provider_fn` 显式提供同一字段顺序、字段 ID 和类型的 typed facts。
provider 返回的值在本次 `rules.apply` 调用期间保持只读有效，RulesForge 只负责契约校验、
求值和 action 应用，不能从 opaque payload 中隐式读取。没有 provider 的 schema-backed
规则会在注册时 fail fast，provider 的运行时错误则原样沿 operation error boundary
传播。

Native adapter catalog 已覆盖 HTTP、RPC、FMQ、Flowie MQTT server、Queue 与 Storage。HTTP/RPC
只描述既有 native client/server 边界；FMQ 按每个 messaging pattern 分开 operation；Flowie 只将
application PUBLISH ingress 和 encoded packet egress 暴露给 graph，CONNECT/SUBSCRIBE/QoS FSM
仍由 session owner 消费。Queue/Storage operation 使用 `RESOURCE_OWNER` scope，并通过
`operation_resource_names + primitives` 把 adapter 实现固定到实际 `QueueBuffer` 或
`StorageResource`，而不是将共享/持久化状态误报为 adapter-private state。

例如 MQTT publish decode 的 scope 是：单 packet/message data、session-generation lifetime、
MQTT owner-local state、CoroNet context 串行化；RulesForge route 是 message data、无共享状态、
dispatch lifetime、pure authority；pool resize 是无 payload、runtime resource state、command
deadline lifetime、host 串行化、typed-command authority。

### 4.5 跨 Domain 规则

同一 domain 内可直接组合类型兼容的 operations。跨 domain 必须使用显式 adapter/bridge，
由 bridge 负责类型转换、ownership transfer、error mapping 和 settlement mapping：

```text
MQTT PUBLISH frame --[protocol/data input bridge]--> MessageEnvelope
MessageEnvelope --[data operations]--> DeliveryAttemptResult
DeliveryAttemptResult --[data/protocol settlement bridge]--> MQTT owner ACK command
```

Bridge 不取得两侧状态所有权。它只能调用目标 owner 的公开 operation/command，不能保存
borrowed internals。Control Command、Status 和业务 payload 也不能因“都是 graph edge”而放入
同一个 envelope；payload graph 和 management workflow 保持物理与权限隔离。

## 5. Disruptor 数据契约

`disruptor.h` 是有界并发 segment 的数据面 handoff。逻辑上它贯穿 graph，物理上只在
concurrency、fan-out/fan-in、buffer、batch 或 completion 边界建立 segment。相邻 inline
stage 可直接传递同一 envelope；每对 stage 都插入 ring 只会增加延迟和存储状态。

每个 ring entry 必须标识或包含：

- message/task envelope 和稳定的 graph/runtime generation；
- dispatch 所需的 source/stage/edge identity；
- 只有一个 release path 的 ownership mode；
- 需要顺序保证时的 ordering key/sequence；
- 与 execution task 共享的 cancellation/deadline state；
- 携带 status 和 settlement metadata 的 completion handle。

Producer 在 publish commit 前持有 entry；consumer acquire 后持有 task，并在 release slot 前
通过 completion 归还所有权。Borrowed transport context 不得跨 asynchronous segment，除非
protocol owner 先将其转换为 owned、generation-checked route token。

Capacity 必填，backpressure 显式选择 `block`、`fail` 或 primitive 专属且已文档化的 drop
policy。满 ring 不是 durable queue。Ordering 按 segment/key 声明；unordered worker pool
不能在没有 reorder/settlement boundary 时进入 ordered fan-in。Stop 的顺序是关闭 admission、
interrupt wait、按 deadline drain，并返回可观察的 terminal status。

当前 physical mapping 为 worker-pool stage 创建 bounded Disruptor ring，并为无动态决策的
静态单 source 图创建 broadcast ring；direct edge 保持同步传递，其他 fan-out/fan-in segment
仍是 compiler plan metadata。物理 worker/broadcast entry 都包含统一 header，thread/coro task
也在执行用户 callback 前校验同一 generation、stage/message identity、ownership 和 handle。
Operation runtime contract 是需求，不是资源工厂：bounded capacity 必须与 DSL worker capacity
相等，preserve-input worker 必须绑定 reorder；worker admission 支持 block/fail/drop-newest，
settlement 支持 complete/retryable/requeue/dead-letter/canceled/protocol ACK。满 ring 仍不是持久队列。

## 6. 资源管理模型

所有可管理资源使用以下逻辑契约：

```text
Metadata: stable kind/name/uid、generation、owner reference
Spec:     validated desired config，单 generation 内 immutable
Status:   owner-derived read-only snapshot，包含 observed_generation
Condition: 从同一个 Status snapshot 推导的 typed reason/status
Command:  携带 expected generation 和 deadline 的 idempotent request
Event:    bounded state-transition notification，绝不是事实源
```

资源族包括 graph runtime、stage/executor pool、Disruptor segment、queue/buffer、IO
endpoint/connection、protocol session aggregate、storage 和 rule set。公共 header 保持小型，
公共 metadata envelope 表达 identity/lifecycle/generation，每个 Domain/resource kind 通过
DataBind schema-backed Spec/Status/Condition/Event document 表达具体内容；性能关键或 owner
command 边界可保留小型 native typed API。禁止用无界字符串 map、万能 union 或裸 `void *`
承载核心状态。

Schema document 的 schema identity 至少包含 domain、resource kind、document kind、schema
name/type、ID、version 和 encoding。Document payload 必须是 owned immutable snapshot，不能
借用 owner 内部 struct。DataBind 在 Observe/Rules/exporter 侧按可信 schema 动态绑定；core
只传递 metadata/schema reference/owned bytes，不依赖 `DataBindValue` 或 JIT。HTTP、S3 和
Database 因此可以共享 resource lifecycle/observe capability，却分别维护
`HttpEndpointStatus`、`S3RequestStatus`、`DatabasePoolStatus` 等不同 schema。

Reconcile cycle 由 host 拥有：

1. 捕获同一 generation 的 immutable Status snapshot。
2. 比较 validated Spec 和 Status，推导 typed Conditions。
3. 已收敛时不执行操作。
4. 未收敛时最多向对应 owner 发一条 idempotent Command。
5. 记录 command result，再从后续 snapshot 验证收敛。

Command 不修改 snapshot。Owner 在改变状态前校验 expected generation、当前 state、capacity
和 deadline。多 owner 变更必须显式编排，例如 `quiesce ingress -> drain graph -> resize
pool -> resume ingress`。系统不伪造跨 owner 原子事务；失败时停止 workflow，保留各 owner
最后一个有效状态，并暴露可重试或需人工处理的 Condition。

Core 以 `turbo_flow_resource_reconcile_tick()` 实现通用单资源 tick：输入是 caller-owned
metadata、typed scalar value、Conditions 和一条 typed command。已收敛时不发令；
`observed_generation < generation` 时只返回 OBSERVING；其余情况最多调用一次统一 dispatcher。
`turbo_flow_resize_workflow_*()` 固化 pointer-free spec 和状态，按五个 tick 执行 quiesce ingress、
drain graph、resize pool、resume graph、resume ingress。失败记录精确 phase 和各 owner 最终有效
generation；只有 caller 显式 retry 才以新 idempotency attempt 重试，不创建线程、不复制 payload，
也不声称跨 owner 原子性。

当前 runtime pool 是该模型的第一个可执行切片。其 UID 为
`pool:<pool-kind-number>:<stage-name>`；start/restart、成功 resize 和成功 rollback rebuild 在
替换 pool 实例后推进 generation，stop/no-op resize 不推进。Pool Status 的 READY、ACCEPTING、
DRAINED、SATURATED Conditions 从同一 snapshot 推导；Observe reconcile 使用其
`observed_generation` 发送 checked resize，stale command 返回 `TURBO_EBUSY`。截断 resize
command 和零 generation 均被拒绝。Connection/Queue 已有稳定的 owner-scoped UID 和
generation-aware snapshot，但当前 immutable owner 通常报告固定 generation `1`；它没有 runtime
pool 那种 rebuild 时递增的 generation，因此不得把该 snapshot identity 当作跨 rebuild UID。
Pool 同时提供首个 schema-backed Status document：固定 envelope 携带 generation 和 schema
identity，具体 counters/capacity/predicates 在 `TurboFlowResource.PoolStatus` V1 payload 中。
真实 DataBind 测试验证 schema 动态绑定；现有 native pool Status 保留给 reconcile 热路径。

## 7. IO 是受控 ingress/egress

IO 不只是 socket callback，也不等于 monitoring/control plane。IO 是受控 ingress/egress，
其 owner 必须分离三个接口：

- data：提交/接收 owned message 或 request；
- status/event：提供不含 credential/payload 的 typed endpoint、connection、admission、
  inflight、last-error snapshot/event；
- command：提供该 owner 支持的 idempotent quiesce、resume、drain、interrupt、replace 或
  protocol-specific command。

HTTP/socket/SMTP/POP3/Redis/MQTT 可复用 connection、endpoint、deadline、admission、pool
和 CoroNet execution primitives。协议 options 留在 typed protocol schema，因为它们的状态机
和 settlement 语义不同。Queue 是 Buffer/Settlement resource，不是 IO connection。

PostgreSQL outbox 同样是 Buffer/Persistence primitive，而不是 FMQ、Flowie 或 graph runtime
内部的特殊 queue。`.yml` named channel 独占 `conninfo/outbox_name/capacity/payload bound/poll`
持久化契约，source/sink adapter 只声明同一 channel 与 role。Sink 在 transaction advisory lock
保护的容量检查与 INSERT 完成 COMMIT 后才可上报 `DURABLE`；source 以 session advisory row lock
跨同步 graph publish 排除其他 source，graph 成功后 DELETE，失败则解锁并保留 row。数据库事务
不跨 graph，进程崩溃自动释放 session lock；graph side effect 与 DELETE 之间的 crash window 明确
采用 at-least-once，不伪造跨数据库/graph 原子提交，也不回退到 memory ACK。

## 8. RulesForge/TurboScript 边界

RulesForge/TurboScript 将声明式规则编译为 immutable program，并针对 typed facts 求值。
它是绑定到 graph node 的 inline pure evaluator，不拥有 thread/coroutine pool，也不创建
隐藏 scheduler。若 host 需要并行规则求值，应由 node 显式选择公共 thread/coroutine pool。
schema-backed `rules.apply` 由 host 提供 `turbo_flow_rule_facts_provider_fn`，将当前 message
materialize 为与 processor schema 完全一致的值数组；值只在本次 operation 调用期间借用，
provider 必须自行保证字符串存储和并发安全。对已有的 schema-bound message projection，
标准 `turbo_flow_rule_projection_facts_provider()` 只负责取得 opaque projection 并调用
host-owned materializer；第三方 projection 的字段解释仍留在适配器边界。
必须分开两种 context：

- data rule 检查单条 message，返回 mutate-private、route、drop、batch key、retry
  classification 或 dead-letter 等 data action；
- control condition 检查 immutable resource/traffic/system/business snapshot，返回 typed
  owner Command proposal。

固定 fact snapshot 下求值必须确定，并受 instruction、time、memory quota 约束，且不执行
IO。规则不能保留 borrowed payload，不能调用 adapter、resize pool、ACK MQTT 或修改
Status。data action 不包含 callback/context，经 runtime-owned decision sidecar 执行；facts
provider 只负责外部数据到 typed facts 的显式边界，不能改变规则 program 或直接执行副作用。
control action 只返回 resource command proposal，经 host authority/observed-generation 校验后
才可交给 owner dispatcher。

## 9. MQTT/Flowie 映射示例

```text
CoroNet TCP/TLS/WS
  -> MQTT protocol owner（parse、session、subscription、QoS FSM）
  -> Input/PUBLISH envelope
  -> Disruptor segment
  -> decode -> RulesForge -> route -> batch/processor
  -> Output delivery attempt
  -> Settlement result
  -> MQTT owner command/event（在已声明边界发送 PUBACK/PUBREC/PUBREL/PUBCOMP）
```

Flowie 是独立应用，只选择性迁移并重命名 TurboMQTT 的 parser 与必要协议基础实现；不链接或
复制其 I/O、queue、processor、sink、worker 与 plugin runtime。Flowie protocol owner 仍是
client/session、subscription、retained、will 和 QoS inflight 的唯一事实源。TurboFlow 只拥有
graph execution 和 delivery processing。Flowie 当前按 QoS 配置 `received`、`accepted`、
`processed` 或 `durable` settlement point；默认值保持 `received`。需要延迟 ACK 时，message-owned
protocol settlement envelope 把请求交给 graph/Queue/durable owner，完成结果必须精确匹配请求
point，且 generation-fenced route 防止 stale reply。迁移来源、兼容性边界与回滚策略见
`../flowie/ARCHITECTURE.md`。

## 10. 候选方案比较

| 方案 | 优点 | 主要问题 | 结论 |
|---|---|---|---|
| 万能 primitive/resource 对象 | 表面 API 数量少，可统一注册 | 胖 vtable、字符串 option、状态所有权模糊，协议和 Queue 被错误同构 | 不采用 |
| Queue/Disruptor-centric runtime | 数据路径直观，容易增加 consumer | 把运行 handoff、durability 和业务事实混为一体，ACK/恢复语义失真 | 不采用 |
| Adapter 各自管理且无公共 resource contract | 迁移成本最低 | 无法统一观察、generation、command、reconcile 和权限边界 | 仅作为当前兼容起点 |
| 四平面 + typed owner/resource + segmented Disruptor | 所有权明确，可渐进接入，协议状态保持自治 | API 类型和测试矩阵增加，多 owner workflow 需显式编排 | 采用 |

选择方案四，因为它复用当前 adapter/snapshot/command 基础，同时让 graph composition、
协议状态机和 Kubernetes-like management 各自保有单一职责。其代价是每个 resource kind
需要 typed extension 和契约测试，但这是避免第二事实源和隐式状态迁移所需的复杂度。

## 11. 兼容、迁移与回滚

- 迁移期间保持现有 stage、adapter、message、snapshot、command 和 control DSL API；新增
  resource contract 通过 `size`/generation 做 additive versioning。
- 保持当前 direct inline dispatch。只有完成行为与 benchmark 对比后才扩大 Disruptor segment。
- Protocol-specific state 不迁入 core；adapter 在现有 registration boundary 后逐个迁移。
- Rules 初期只运行 observe/decision-only；完成 validation、authorization、idempotency tests
  后才启用 owner command execution。
- 迁移失败时可选择旧 compiler lowering/adapter implementation，不改变 graph DSL 或 wire
  format。State format 或 ACK 变化必须另立兼容与回滚决策。

验证范围必须覆盖 compile-time ownership rejection、bounded backpressure、ordering、
completion、stop/drain race、snapshot purity、command idempotency、generation conflict、
reconcile convergence/failure、rule quota 和 protocol settlement integration。性能改动必须
提供前后 throughput 与 P50/P95/P99 latency benchmark；架构偏好本身不是性能证据。

## 12. 风险与证据

- **HIGH - protocol settlement compatibility**：将 endpoint 配置从默认 `received` 改为
  `accepted`、`processed` 或 `durable` 会改变 duplicate delivery、latency、inflight retention
  和 reconnect 行为；部署必须显式选择，并保留协议集成测试与回滚配置。
- **HIGH - asynchronous ownership**：borrowed protocol context 若长于 session 生命周期，
  会造成 stale reply、use-after-free 或错误 settlement 新 generation。未转换为 owned、
  generation-checked token 时必须在 compile/admission 阶段拒绝。
- **MED - universal resource abstraction**：单一巨型 Spec/Status/Command union 会耦合全部
  adapter 并破坏 C ABI 演进。公共 header 保持小型，各 resource kind 使用 typed extension。
- **MED - speculative Disruptor expansion**：每个 stage 之间放 ring 会增加容量调优、排队延迟
  和 shutdown 状态。只分段已确认的 concurrency boundary，并先 benchmark 后替换 inline。
- **MED - control oscillation/split authority**：多个 reconciler/rule 管理同一字段会震荡或
  覆盖 intent。一个 Spec 字段只能有一个 manager，command 使用 generation check，并按需
  使用 hysteresis/cooldown 和可观察 Condition。
- **LOW - exporter coupling**：Prometheus/OpenTelemetry SDK type 必须停留在 exporter adapter，
  保持 core snapshot ABI 和 hot path 独立。

当前基线证据：

- `include/turbo_flow.h`：message envelope、resource snapshot、adapter command/ops；
- `include/turbo_flow_domain.h`、`src/flow_domain.c`：module/primitive/operation catalog、dependency
  compatibility 和 typed provider module binding；
- `include/turbo_flow_control.h`：immutable fact provider，禁止 read side effect；
- `src/flow_disruptor.c`：bounded worker/broadcast Disruptor publish path；
- `../observe/include/turbo_flow_observe.h`：graph snapshot 和 host-owned pool reconcile；
- `include/turbo_flow_policy.h`：compiled ordered rule processor；
- 对应 core、Observe、policy、control 和 FMQ tests。
