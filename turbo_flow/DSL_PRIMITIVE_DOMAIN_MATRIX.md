# TurboFlow DSL / Primitive / Domain 能力矩阵

> 盘点日期：2026-07-17
>
> 本文是仓库级索引和核验表，不替代具体模块契约。`事实`来自当前头文件、实现、测试和
> 现有设计文档；`目标`表示后续要达到的架构，不表示已经可用。

## 1. 先给结论

当前实现可以准确描述为：

```text
Graph DSL   -> parser -> compiler validation -> graph plan/lowering -> runtime dispatch
                                                  -> executable binding / executor
Control DSL -> parser -> typed facts evaluation -> resource command / reconcile -> target owner
```

- `graph` 已经是所有 TurboFlow payload/data-plane 流程的实际编排主线；Management
  owner-command 走独立 Control DSL/API，不进入 payload graph。
- Graph DSL 已经能够显式表达 `operation` 和 `resource`，compiler 也会校验 operation 的
  domain/type、source/stage role、resource compatibility、executor scope、worker capacity、
  retry/reject/reorder 和 settlement 边界。
- `module + primitive + operation + executable binding` 作为统一业务契约目前是**部分落地**：
  module catalog/依赖校验、TurboFlow Policy typed provider，以及 HTTP/RPC/Queue/Storage
  typed native adapter 已实现；
  大多数 IO、协议、队列和存储模块仍以 `adapter + resource provider + core.* implicit
  operation` 接入 graph。
- 因此，当前不能宣称“所有业务都已经基于显式 primitive + graph”；可以宣称“所有
  payload/data-plane 业务都经过 graph，且显式 operation contract 已为后续统一提供校验边界”。

这不是否定现有实现，而是区分三个事实：

1. payload graph 是数据执行事实源；
2. Control DSL/API 是 Management command 的入口，不是第二条 payload graph；
3. adapter/resource owner 是外部资源状态事实源；
4. module/primitive/operation registry 目前还没有覆盖全部 domain adapter。

## 2. 七个概念的唯一职责

| 概念 | 应表达什么 | 不应表达什么 | 当前载体 |
|---|---|---|---|
| Domain | 词汇、数据类型、状态不变量、错误和 settlement 语义 | 线程数、socket 指针、万能动词 | `turbo_flow_domain_t`、schema/resource metadata |
| Module | 版本化能力边界、primitive type/operation 导出、模块依赖和 provider 归属 | graph topology、资源实例工厂、替换模块 native runtime | `turbo_flow_module_descriptor_t`；按依赖顺序注册 |
| Primitive | domain 内可独立说明所有权和生命周期的 value/resource capability | 任意 callback、graph stage 别名、执行器 | `turbo_flow_primitive_descriptor_t`；实际 owner 仍由 adapter/provider 持有 |
| Operation | 一个主要 effect 的 typed 动词，声明 input/output、scope、runtime boundary | 隐式状态、多个 owner 的事务、万能 `process()` | `turbo_flow_operation_descriptor_t`；执行实现来自 typed provider 或 module-owned native adapter，legacy callback/adapter 只处理未 catalog 的兼容路径 |
| Graph node | operation + resource binding + immutable config + execution policy | 独立创造资源、隐式跨 domain 转换 | DSL stage/source、compiled stage plan |
| Adapter | 外部 ingress/egress 和协议/资源 owner 的薄边界 | 取代 graph topology 或持有第二份业务状态 | `turbo_flow_adapter_ops_t`、各 IO 模块 adapter |
| Executor/Disruptor | 放置、并发、有界 handoff、顺序和取消 | 业务语义、协议 ACK、持久化事实 | inline/thread/coro、worker/broadcast/fanin segment |

判断一段代码是否应成为 primitive 或 operation，优先看它拥有的不变量，而不是文件大小：

- 需要独立 owner、generation、snapshot、command 或恢复语义的是 resource primitive；
- 可复制、不可变或显式 move/retain 的数据是 value primitive；
- 只有一个主要副作用、输入输出和错误边界清晰的是 operation；
- 仅负责把外部格式/连接接到 owner 的是 adapter 或 bridge。

## 3. DSL 能力矩阵

### 3.1 Graph DSL 已实现语法

| DSL 能力 | 语义 | 编译/运行影响 | 语法状态 |
|---|---|---|---|
| `source name` | graph ingress，必须无 input、有 output | source operation 或 `core.source` | 已实现 |
| `stage/step name` | graph processor/egress node | 显式 operation 或 `core.stage.*` | 已实现 |
| `adapter name` | 绑定已注册 adapter owner | adapter schema、consume、生命周期、settlement | 已实现 |
| `operation name` | 绑定注册 operation contract | 类型、scope、executor、runtime contract 校验 | 已实现，但生产 catalog 覆盖不全 |
| `resource name` | 绑定 primitive resource name | domain/type/kind/version range 必须匹配 operation | 已实现，不能自动创建资源 |
| `worker N capacity M` | 有界 worker data segment | Disruptor worker pool；capacity 为 2 的幂 | 已实现 |
| `exec inline/thread/coro` | 选择计算 executor | execution mask、pool/coro/thread owner 校验 | 已实现 |
| `retry attempts N delay M` | adapter-owned retry boundary | retry callback、retry settlement、attempt 上限 | 已实现 |
| `reorder capacity N timeout M` | ordered fan-in boundary | unordered worker 分支必须先经过 reorder | 已实现 |
| `a -> b`、`[a,b]` | 线性、fan-out、fan-in graph edge | direct/broadcast/fanin lowering | 已实现 |
| `route a -> b when expr` | 成功输出上的条件 edge | BOOL expression；false 只过滤该 edge | 已实现 |
| `reject name a -> b` | 失败边 | reject error mode 必须有显式 reject edge | 已实现 |
| `stage { in/out/use/step }` | 可复用 composite stage | parser 展开为有界 scoped graph | 已实现，非参数化 |
| expression | 只读字段谓词 | 解析、类型检查、MIR interpreter/JIT | 已实现 |

### 3.2 Control DSL 已实现语法

Control DSL 是独立的 Management 命令语言，不经过 graph compiler：

| Control DSL 能力 | 语义 | 执行边界 | 语法状态 |
|---|---|---|---|
| `flow pause/resume` | 暂停或恢复 ingress admission | runtime owner | 已实现 |
| `flow drain timeout N` | 在 deadline 内等待 active publication 排空 | runtime owner | 已实现 |
| `pool name thread/coro/disruptor resize N timeout M` | generation-checked pool resize | stable resource command | 已实现 |
| `adapter name quiesce/resume` | endpoint/adapter owner 生命周期命令 | stable resource command；legacy direct adapter command 已废弃 | 已实现 |
| `adapter name replace host ... port ... path ...` | 原子替换 endpoint 配置 | stable resource command | 已实现 |
| `if/when expr then command` | 对 immutable typed facts snapshot 求值后执行命令 | control expression evaluator + resource command | 已实现 |

Control DSL 的 parser、facts evaluation、idempotency、UID/generation 和 owner command 是管理面
契约；不能因为它不在 payload graph 中，就从 DSL/domain 盘点中省略。

### 3.3 两类 DSL 当前不应承担的能力

- 不在 Graph DSL 的 payload graph 中放 Management owner-command；compiler 会拒绝 management
  operation。Management 命令使用 Control DSL/API。
- 不通过 graph edge 做隐式 domain/type 转换；必须由显式 bridge operation 完成。
- 不用 `adapter` 名称推断 operation 语义；adapter 是 owner/外部边界，operation 是 typed effect。
- 不用 `worker`、`exec` 或 `coro` 关键字创造业务状态；它们只声明 runtime placement/handoff。
- 不把 queue、socket、protocol session 或规则资源的内部指针放入 message 或 graph edge。
- 不在 Graph DSL 增加 `module {}` 资源工厂语法。模块目录由可信 host/module code 注册；YAML
  继续只选择和配置已注册的 adapter/resource/graph binding。

## 4. Graph DSL 的实际 lowering

```text
显式 operation/resource
        │
        ├─ resolve：找注册 operation；未声明时生成 concrete core.* contract
        ├─ validate：module owner / role / resource version / executor / scope / edge / settlement
        ├─ lower：runtime node + edge + segment + executor plan
        └─ dispatch：inline callback、adapter owner、thread pool、coro pool、worker lane
```

当前 segment 类型：

| Segment | 触发条件 | 所有权/顺序含义 |
|---|---|---|
| `DIRECT` | 普通 edge | 不额外创建 ring；沿同步 dispatch 传递 |
| `WORKER_POOL` | `worker N` 或 bounded operation | bounded Disruptor handoff；capacity 与 DSL 一致 |
| `BROADCAST_FANOUT` | 一个 node 有多个 outgoing edge | 广播拓扑；可变 stage 不得破坏共享 entry 语义 |
| `FANIN_GATE` | 一个 node 有多个 incoming edge | 等待可达输入；条件 fan-in 由 direct scheduler 处理 |
| reorder boundary | 显式 `reorder` | 将 unordered branch 转成可验证的 preserve-input 边界 |

`Disruptor` 是 data handoff，不是业务 primitive；CoroNet context 是 IO owner placement，不是
新的业务 executor。当前完整的 operation runtime contract 可查询，但并不自动创建 ring、资源或
owner。

## 5. Domain 目标词汇表

本表定义每个 domain 应拥有的稳定词汇，不表示这些条目已经注册为生产 primitive/operation。
“当前证据摘要”只说明代码载体存在；产品状态必须使用第 6、8 节的固定状态和值级证据。

| Domain | 核心 value primitive | 核心 resource primitive | 典型 operation | 状态 owner | 当前证据摘要 |
|---|---|---|---|---|---|
| Data | Message、ContentDescriptor、Schema、Batch、Decision | keyed state/window store、schema registry | decode、validate、transform、filter、route、emit、keyed update、window close | message/processor/schema owner | graph/core、codec、Policy、keyed/window 已有实现 |
| Execution | Task、ExecutionPlan、Completion、OrderingKey | thread/coro pool、Disruptor segment、runtime | submit、yield、cancel、wait、drain、resize、reorder | flow runtime/executor owner | 已有 executor、segment plan、pool status/resize |
| IO/Transport | EndpointSpec、ConnectionView、StreamChunk | endpoint、connection、CoroNet execution binding | listen、connect、read、write、interrupt、quiesce、resume | adapter/CoroNet context owner | socket、HTTP endpoint 已接入 |
| Protocol/Pattern | Frame、RouteToken、Correlation、Subscription、DeliveryAttempt | ProtocolSession、route/session aggregate、broker pattern state | parse、encode、publish、request/reply、subscribe、settle | 外部协议 owner | HTTP/RPC、email、Redis protocol paths |
| Buffer/Persistence | Record、Claim、Checkpoint、Blob | Queue、SQLite/Redis record/blob store、file/object storage | enqueue、claim/reserve、ack、requeue、drop、recover、commit | queue/storage owner | queue、storage、Redis、PgSQL、S3 已有 owner API |
| Rules | FactsSnapshot、Decision、Action | RuleSet、SecurityRealm | compile、evaluate、authorize proposal | rules/security owner | `rules.apply` 已显式 primitive + operation |
| Management | ResourceRef、Spec、Status、Condition、Command、Event | runtime/pool/segment/connection/queue/protocol/storage/rule resource records | observe、diff、reconcile、apply command | host reconciler + target owner | resource metadata/document、pool status、control/observe 已有实现 |

Domain 不是目录归属。一个模块可以跨多个 domain，但每个状态必须只有一个 owner。例如：

- 外部消息产品可以同时跨 IO/Transport、Protocol/Pattern、Buffer/Persistence；其 socket、pattern、
  队列和 management state 不能合成一个万能 primitive，也不能由 Graph Core 接管。
- Policy 的输入输出属于 Data，但 RuleSet 属于 Rules；`rules.apply` 是显式跨 domain
  operation，不能把规则资源变成 Data 的普通 map。

## 6. 模块接入矩阵：现状与缺口

| 模块 | 主要 domain | 当前 graph/owner 接入 | 显式 primitive/operation catalog | 状态 | 结论 |
|---|---|---|---|---|---|
| `turbo_flow` core | Data / Execution / Management | Graph DSL/compiler/dispatch + Control DSL/resource command | `core.*` 为 compiler 生成的 implicit contract | `implemented` | graph 与 control 基座已成立；core contract 不是 domain catalog |
| `turbo_flow/src/flow_policy.c` | Rules | typed provider + graph + resource owner | `rules.policy` module 导出 `RuleSet` + `rules.apply`，provider 显式绑定 module | `implemented` | module/contract/binding 参考实现 |
| `codec` | Data | line/length/databind/csv adapter | 未发现生产 `register_primitive/operation` 路径 | `adapter-only` | 需要补 Data operation catalog |
| `queue` | Buffer/Persistence | source/sink adapter + claim settlement | `buffer.queue` 导出 `QueueBuffer`、`queue.dequeue/enqueue`；adapter operation 固定绑定实际 Queue primitive | `implemented` | Queue 状态仍由共享 queue owner 独占，不归 adapter |
| `storage` | Buffer/Persistence | file/directory/sqlite source/sink | `buffer.storage` 导出 `StorageResource` 与 file/directory/append/sqlite operations，绑定实际 storage primitive | `implemented` | source/read 与各 sink commit 保持不同 operation |
| `io/socket` | IO/Transport | CoroNet socket adapter | `io.socket` 导出 `SocketEndpoint` 与 `socket.receive/send`，绑定实际 endpoint owner | `implemented` | graph operation 只表达 ingress/egress；connect/listen/close 仍是 CoroNet owner lifecycle |
| `io/http` | IO/Transport + Protocol/Pattern | native TurboHTTP/Iris client/server adapter | `io.http.client` 的 request/poll 绑定 `HttpClientConnection`；`io.http.server` 的 request/reply 绑定 `HttpServerEndpoint` | `implemented` | 保留 native endpoint/adapter；resource catalog 不迁移 I/O |
| `io/rpc` | IO/Transport + Protocol/Pattern | native RPC client/server adapter | call/poll 绑定 `RpcClientConnection`；request/reply 绑定 `RpcServerEndpoint` | `implemented` | 保留 native RPC/Iris owner；RPC resource 不冒充 HTTP/Socket operation |
| `flowstore/backends/redis` | Protocol/Pattern + Buffer/Persistence | Redis data/stream adapter | stream/blob/record owner 有；DSL operation 未统一 | `adapter-only` | claim/settlement 与 blob commit 必须分成两类 operation |
| `flowstore/backends/pgsql`、`io/s3` | Buffer/Persistence | query/object source/sink | content/resource schema 有；显式 operation 未统一 | `adapter-only` | query/result/object 的 content type 要留在 domain schema |
| `io/email` | Protocol/Pattern | SMTP/POP3/MIME adapter/example | MIME schema 有；显式 operation 未统一 | `adapter-only` | parse/encode 与 SMTP/POP3 owner 不应混为一个 primitive |
| `schedule` | Execution + Management（建议归类） | schedule source adapter | 尚未看到统一 domain operation descriptor | `adapter-only` | 需要先固化 timer/trigger owner 语义再注册 |
| `observe` | Management + Execution | observe callback/summary sink | 只读 snapshot/derived metric | `owner-api` | 不应成为 payload mutation operation |
| `security` | Rules + Management | security realm/resource API | Resource metadata 有；不进 payload graph | `owner-api` | 命令授权与数据规则要保持两个边界 |

本表中“未统一”是架构缺口，不代表模块没有可用实现：这些模块已经通过 adapter、resource
provider、schema、测试以及适用的 graph、control 或 owner API 工作；缺的是把它们的业务能力
提升到可被 compiler 或 management command boundary 统一验证的显式 operation/primitive
catalog。

## 7. 重复与边界风险

### MED：显式执行身份尚未覆盖全部 domain adapter

当前 module catalog 已能为 typed operation provider 记录唯一 module owner，也能通过
`turbo_flow_register_module_adapter()` 原子注册 `(module, operation, adapter)` 关联。Cataloged
operation 若实际解析到 legacy stage callback、未绑定 adapter 或错误 module owner，compiler 会
fail fast；resource-owned operation 进一步要求 typed native adapter 固定绑定实际 primitive。
Socket、HTTP/RPC、Queue 和 Storage 已使用该路径；其他 domain adapter 仍需增量接入。

影响：未 catalog 的旧 DSL 仍只能得到 `core.*` contract、adapter schema 与模块自有测试的保证；
它不会被错误地计入 module-level executable proof，但能力发现与跨模块组合仍不完整。

后续按模块增量注册静态 operation catalog；旧 stage callback/adapter consume 继续作为未显式
operation 的 compatibility path，不能把它们计入 explicit operation proof。

### 已解决：resource compatibility 包含显式版本范围

operation descriptor 现可声明 `resource_min_version/resource_max_version`；compiler 在
kind/domain/type 之后校验 inclusive range。`max=0` 表示无上界；V1 descriptor 没有这两个字段，
按 ABI `size` 安全归一化为 `min=max=0`，即保持原来的 any-version 语义。

契约版本、descriptor ABI `size`、resource instance UID 和 runtime generation 仍是四个独立概念，
不能互相替代。新 canonical operation 应显式填写下界；零下界只用于 V1/兼容契约。

### MED：其余 adapter 的能力宣称仍需显式契约证据

TurboFlow Policy、Socket、HTTP、RPC、Queue 和 Storage 已完成 catalog + executable
binding；codec、Redis、PgSQL、S3、Email 与 Schedule 仍主要通过 legacy adapter 路径工作。即使 DSL
手工绑定 operation，只要执行实现没有 typed binding，仍不能计入 operation-level proof。

影响：DSL 看起来已经模块化，但新增一个 adapter 可能绕过统一 domain contract，造成行为只能靠
模块测试保证，不能由 graph compiler 统一拒绝错误组合。

最小修复方向：为每个 domain adapter 提供静态 operation catalog，先在注册阶段完成
`operation descriptor + primitive compatibility` 校验，再逐步让 DSL 显式绑定；保留
`core.*` 作为兼容路径，但必须可观测并限制在无显式 catalog 的迁移阶段。

### MED：通用 management provider 与 primitive instance 仍需完整类型关联

`turbo_flow_register_module_adapter()` 已可原子注册 primitive、management providers 和 adapter，
并将每个 operation 固定到一个 primitive name；Queue/Storage 已使用该路径。通用 management
provider registry 本身仍未声明其 primitive type，因此其他模块若只注册 provider，仍可能漂移。

影响：同名资源可能在 descriptor、adapter owner、management metadata 中出现漂移；编译通过不
等于 runtime 一定找到同一个 owner。

建议：增加明确的 `primitive type -> owner kind/schema -> provider` 关联表，禁止通过字符串相似
度自动绑定；运行时只接受 generation/UID 一致的 owner snapshot。

### MED：相同动词不能跨 domain 复用成万能 operation

`publish`、`send`、`ack`、`retry`、`commit` 在 transport、protocol、queue、storage 中的
事实源和失败语义不同。可复用的是 pattern algorithm 或 bridge 机制，不是一个跨域 vtable。

建议使用 domain namespace，例如 `message.publish`、`queue.claim`、
`redis.stream.ack`、`storage.commit`，并显式声明它们的 settlement owner。

### MED：exactly-once 不能由 graph/Disruptor 单独推出

当前 graph、bounded handoff、retry 和 settlement 能表达 at-most-once/at-least-once 的部分边界，
但 durable input offset、state mutation 和 output commit 尚未形成同一事务事实源。没有这个边界，
不能把“消息只经过一次 callback”或“发送成功”称为 exactly-once。

## 8. 已实现的 module catalog 与 canonical catalog

Module catalog 是注册/校验层，不是 loader、plugin system、资源工厂或新 DSL：

- `turbo_flow_register_module()` 深拷贝 module identity/version/capabilities、导出的 primitive
  `type_name`、已注册 operation 名称和依赖版本/能力范围；依赖按拓扑顺序注册，缺失或不兼容立即失败；
- 一个 operation 只能有一个 module owner；`turbo_flow_bind_operation_provider_module()` 将既有
  `(operation, resource)` typed provider 绑定到该 owner，并校验 resource primitive type 是 module
  的导出；
- `turbo_flow_register_module_adapter()` 原子注册 native adapter、resource providers 和 typed
  operation owner association；resource-owned operation 还会原子注册 primitive instance 并固化
  adapter-operation-resource 三元关系；失败不转移 adapter context，成功后 registry 接管 shutdown；
- `turbo_flow_module_count/at/find()`、`turbo_flow_operation_provider_module()` 和
  `turbo_flow_adapter_operation_module()` 提供只读查询；
  `reset(..., 1)` 保留目录，registry-clearing reset/destroy 释放它；
- 生产 catalog 包括 `rules.policy`、`io.socket`、HTTP/RPC client/server、
  `buffer.queue` 和 `buffer.storage`。HTTP/RPC 仍保留 native endpoint；catalog 不替换运行时。

人写配置仍为 YAML。YAML 选择 named resource/adapter/operation，host 在解析/构图前注册可信 module
catalog；不能从不可信 YAML 动态声明 provider 身份或 native function。

产品装配位于 YAML resolver 与 module/operation catalog 之间，不属于 Graph DSL grammar。
可信 host 以 caller-owned `turbo_flow_product_provider_registry_t` 注入允许的 adapter/resource
factory：先用 `turbo_flow_product_preflight()` 对 resolved adapter kind 做无副作用校验，再 parse
Graph，并用 `turbo_flow_product_assemble_graph()` 只实例化 Graph 实际引用的 source、processor
resource 和 sink。同名引用只装配一次，resource 先于 adapter；callback 失败后 host 必须丢弃该
Flow generation 并清理 provider-owned owner。该 registry 不是 plugin loader、全局 service
locator、协议状态机或新的事实源。

每个 domain module 只维护一份 catalog；文档、compiler、capability 查询和测试都从同一份
descriptor 派生。当前公共 module descriptor 已覆盖 identity、capabilities、primitive type exports、
operation exports、dependency version range、provider/native-adapter owner 与 resource version range；下列 settlement 字段仍由
现有 primitive/operation/resource descriptor 分担并需继续收敛：

| 类别 | 必填字段 |
|---|---|
| identity | domain、kind、name、contract version、descriptor ABI size、schema/type name |
| primitive | value/resource、owner kind、UID/generation 规则 |
| operation | source/stage/bridge、input/output domain+type、primary effect |
| resource requirement | required resource domain/type、exact version 或兼容范围 |
| executable binding | provider/typed adapter identity、实现版本、绑定的 operation/resource identity |
| scope | data/state/lifetime/concurrency/authority |
| runtime | executor mask、handoff、capacity、ordering、backpressure、deadline、cancellation |
| settlement | complete/retry/requeue/dead-letter/protocol-ack/canceled 的 owner 和失败码 |
| lifecycle | start/stop/quiesce/drain/replace/resize、可恢复状态 |
| evidence | 头文件、实现、focused TinyTest、示例 DSL、支持矩阵 |

状态值固定为：

- `implemented`：有生产实现、契约、focused test，以及符合其边界的 Graph DSL、Control DSL 或
  public API 示例；
- `owner-api`：owner/runtime API 已存在并有测试，但尚未进入显式 domain operation catalog；
- `contract-only`：descriptor 或设计文档存在，但 runtime owner 未完成；
- `adapter-only`：能通过 adapter+graph 工作，但还没有显式 domain operation proof；
- `unsupported`：compiler/runtime 明确返回 `TURBO_ENOTSUP`；
- `deprecated`：仅用于兼容，不再新增依赖。

禁止用“代码存在”代替 `implemented`；尤其要检查错误、容量、stop、ownership、retry 和
settlement。

## 9. 分阶段迁移，不改变现有业务行为

1. **盘点阶段**（进行中）：分别盘点 Graph DSL payload operation、Control DSL command 和 owner API；为每个
   adapter/owner 写出 operation catalog，先不改默认行为。
2. **契约阶段**：补 descriptor、required resource version/range 和 compatibility tests；注册失败
   必须 fail fast，不能静默降级。
3. **执行绑定阶段**（公共机制已完成，模块增量接入）：显式 operation 必须绑定 typed operation provider 或 typed adapter；
   legacy stage callback/adapter consume 只保留在 `core.*` compatibility path，并记录 migration
   diagnostic。
4. **资源绑定阶段**：adapter/provider 注册同时关联 owner resource primitive；校验
   name/type/domain/contract-version/owner-kind/UID 关系，解决 primitive 与 management resource
   registry 漂移。
5. **显式阶段**：新 Graph DSL 必须显式 `operation`；旧无 operation 的图继续生成 `core.*`，但
   禁止新 domain 能力只走 implicit path。Management command 继续使用 Control DSL/API，不迁入
   payload graph。
6. **验证阶段**：每个 operation 至少覆盖正常、错误、边界容量、stop during pending wait、
   retry/settlement、ownership 和适用 transport；再用跨 domain negative matrix 验证 compiler
   拒绝错误组合。
7. **声明阶段**：只有完成 catalog + executable binding proof + compiler proof + runtime test +
   owner lifecycle 后，才把
   能力标为 `implemented`。

## 10. 借鉴边界

可借鉴的不是产品名称，而是边界设计：

- Redpanda Connect 把 input、pipeline processor、output 分开，并把 processor 作为消息上的
  可组合函数；这对应 TurboFlow 的 source、Data operation、adapter sink/bridge。见
  [Processing Pipelines](https://docs.redpanda.com/connect/configuration/processing_pipelines/)
  和 [Processors](https://docs.redpanda.com/connect/components/processors/about/)。
- Apache Camel 将 error handler、redelivery 和 dead-letter endpoint 作为可配置的路由边界；
  TurboFlow 应继续把 retry/requeue/dead-letter/protocol ACK 放在 settlement contract，不塞入
  普通 Data transform。见 [Camel Error Handler](https://camel.apache.org/manual/error-handler.html)
  和 [Dead Letter Channel](https://camel.apache.org/components/4.14.x/eips/dead-letter-channel.html)。
- Redpanda 的 exactly-once/atomicity 依赖事务地提交消费位置和输出，而不是依赖单个 processor
  或 ring；TurboFlow 在没有 durable state + input position + output commit 的共同 owner 前，
  只能声明显式的 at-most-once/at-least-once 语义。见
  [Redpanda Transactions](https://docs.redpanda.com/streaming/23.3/develop/transactions/)。

## 11. 事实依据索引

- Graph DSL 语法和拒绝规则：`turbo_flow/parser/GRAMMARS.md`、`flow_grammar.y`、`flow_parser.c`。
- Control DSL/Management command：`control_grammar.y`、`control_lexer.re`、
  `turbo_flow/include/turbo_flow_control.h`、`flow_control.c`、`flow_resource_command.c`、
  `flow_reconcile.c`。
- Domain/module/primitive/operation ABI：`turbo_flow/include/turbo_flow_domain.h`、`flow_domain.c`。
- Operation resolve/validation：`turbo_flow/src/flow_compile.c`。
- Graph lowering：`turbo_flow/src/flow_plan.c`、`flow_internal.h`。
- Runtime execution：`turbo_flow/src/flow_executor.c`、`flow_execution.c`、`flow_dispatch.c`。
- Module/provider binding 与 TurboFlow Policy 显式 catalog：`turbo_flow/src/flow_domain.c`、
  `flow_compile.c`、`flow_policy.c`、`turbo_flow/tests/test_flow_domain.c`、`test_flow_policy.c`。
- Domain 原则和所有权：`turbo_flow/PRIMITIVE_GRAPH_ARCHITECTURE.md`、`DOMAIN_CONTRACTS.md`。
- Domain contract/negative tests：`turbo_flow/tests/test_flow_domain.c`、
  `test_turbo_flow.c`、各 `io/*/tests`。
