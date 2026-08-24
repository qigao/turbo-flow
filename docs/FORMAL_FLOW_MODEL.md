# TurboFlow 核心 Flow 模型形式化规范

## 目标

用 Lean 4 建立一个参考当前 C 控制语义的 TurboFlow 核心可执行抽象模型，并证明该抽象
模型的关键性质：

1. Flow 与 execution task 只发生允许的生命周期迁移；
2. 成功、失败、条件与 reject edge 的选择互斥且符合运行时定义；
3. fan-in 只有在所有潜在前驱都已决议后才会 ready 或 filtered；
4. 一个 fan-in gate 不会重复产生 ready 事件，因而每个 message/node 至多进入 ready queue 一次。

这里的“证明”指 Lean kernel 检查过的抽象模型定理，不等同于对 C 源码、编译器输出或并发内存模型的自动验证。

## 仓库事实与模型映射

| C 事实源 | 抽象对象 | 需要保持的语义 |
| --- | --- | --- |
| `turbo_flow/include/turbo_flow.h:1327` | `FlowState` | NEW/PARSED/COMPILED/STARTED/STOPPED/FAILED 生命周期 |
| `turbo_flow/src/flow_compile.c:1175` | `CompiledGraph` 前提 | runtime graph 无环 |
| `turbo_flow/src/flow_compile.c:216` | edge 唯一性前提 | 同一 from/to/kind 不重复 |
| `turbo_flow/src/flow_completion.c:38` | `edgeActive` 与 `RouteDecision` | 失败/reject/named/unconditional/conditional 的优先级；成功时 reject 不激活，失败时只激活 reject |
| `turbo_flow/src/flow_completion.c:98` | `FanInGate.resolve` | 每条潜在入边决议一次，递减 remaining，累计 activated |
| `turbo_flow/src/flow_completion.c:137` | `GateOutcome` | remaining=0 且 activated>0 才 ready，否则 filtered |
| `turbo_flow/src/flow_completion.c:170` | completion 幂等前提 | done stage 的重复 completion 不再次释放下游 |
| `turbo_flow/src/flow_internal.h:284` | `TaskState` | NEW/ACCEPTED/RUNNING/COMPLETED/CANCELED 生命周期 |
| `turbo_flow/src/flow_execution.c:28,126` | `TaskState.accepted + TaskEvent.complete` | `flow_execution_task_fail()` 可在 task 运行前通过 `flow_execution_task_complete()` 从 ACCEPTED 进入 COMPLETED |
| `turbo_flow/src/flow_execution.c:28` | task terminal 规则 | COMPLETED/CANCELED 不再被 completion 改写 |

## 抽象边界

### 纳入证明

- 单条 message 在一个已编译 DAG 上的控制决策；
- stage callback 的结果被视为 `ok` 或 `failed` 输入；
- conditional predicate 的成功布尔结果；
- fan-in 的 remaining/activated/ready/filtered 状态；
- public Flow lifecycle 与内部 execution task lifecycle 的合法迁移关系。

### 明确不纳入证明

- C11 atomic/mutex/condition variable 的 happens-before 正确性；
- allocator、`vec_t`、Disruptor、thread/coroutine backend 的实现正确性；
- predicate evaluator、用户 callback、adapter 或 settlement owner 自身正确性；
- payload ownership、clone/move、reorder buffer、deadline 时钟与 retry 的端到端正确性；
- Lean 模型与 C 源码之间的自动 refinement proof。

这些边界意味着 Lean 结果能证明“给定模型前提，调度规则满足不变量”，不能证明任意 C 执行都满足前提。

## 前提

- graph 有限，stage identity 在运行时索引范围内；
- graph 已通过 unknown endpoint、source reachability、duplicate edge 与 cycle 校验；
- 对同一 message，每个已选择 stage 只提交一次有效 completion；重复 completion 被 `done` 防护吸收；
- conditional predicate 若求值失败，执行直接返回错误，不把失败伪装成 false；
- fan-in 的初始 potential 等于 origin 可达子图中的潜在入边数；
- 本模型按单 message 的顺序 worklist 解释运行时，不声称覆盖多个 publish 的并行交错。

## Lean 数据模型

### Edge route

`EdgeKind` 包含 unconditional、conditional(predicate result) 与 reject；`StageResult` 包含 ok/failed。
`RouteDecision` 区分没有命名路由与 `.namedRoute matchesCurrentTarget`。`.namedRoute` 由模型外层
保证 route 非空且属于当前 source stage；C 的对应校验在 `flow_data_route_exists()` 与
`flow_apply_completion()`（`turbo_flow/src/flow_completion.c:84-96,170-183`）。在此前提下，
`edgeActive` 直接编码 `flow_route_edge_active()` 的优先级：失败时仅 reject；成功时 reject 不激活，
随后依次处理 named route target match、unconditional 和 conditional predicate。predicate 求值失败或
结果非 BOOL 会返回错误，不被模型伪装为 false。

### Fan-in gate

每个 target gate 记录：

- `potential`：可达潜在前驱数，初始化后不变；
- `processed`：已经决议的前驱数；
- `activated`：其中选择了该 target 的前驱数；
- `outcome`：pending、ready 或 filtered。

`resolve(selected)` 仅在 `processed < potential` 时成功。最后一个前驱决议后，`activated > 0`
产生 ready，否则产生 filtered。`Gate.Valid` 至少保持：

```text
processed <= potential
activated <= processed
pending  -> processed < potential
ready    -> processed = potential and activated > 0
filtered -> processed = potential and activated = 0
```

### Lifecycle

Flow 与 task 分开建模。`FlowEvent.fail` 是一个已选择调用 `flow_set_error()` 并将 Flow 置为
FAILED 的抽象边界，不代表任意 C 错误。使用 `flow_set_error_keep_state()` 的错误保留原 Flow
状态，不产生 `.fail` 事件。`flow_transition_preserves_allowed` 因此证明成功、stop、reset 以及该
显式失败边界的模型迁移性质，不将 keep-state 错误合并到 FAILED。

Task 的 `.complete` 同时建模 RUNNING → COMPLETED 与 ACCEPTED → COMPLETED。后者对应
`flow_execution_task_fail()` 在 task 运行前调用 `flow_execution_task_complete()` 的路径；例如 executor
检查到任务 header 无效时，不会先进入 RUNNING。

## 必须由 Lean 检查的定理

1. `reject_inactive_on_success`：成功时 reject edge 不激活。
2. `only_reject_active_on_failure`：失败时激活的 edge 必为 reject。
3. `resolve_preserves_valid`：一次合法 predecessor resolution 保持 gate 不变量。
4. `ready_requires_all_processed`：ready 蕴含所有潜在前驱已决议且至少一条激活。
5. `filtered_requires_all_processed`：filtered 蕴含所有潜在前驱已决议且没有激活边。
6. `terminal_gate_cannot_resolve`：ready/filtered gate 不能再次 resolve，因此不重复产生 ready。
7. `flow_transition_preserves_allowed`：`nextFlowState` 返回的每个迁移都属于公开允许关系。
8. `task_terminal_is_absorbing`：completed/canceled task 不再迁移。

Supporting theorem `named_route_prioritizes_target_match`、`initial_valid` 与
`accepted_complete_reaches_completed` 分别检查命名路由匹配优先级、正 potential 初始 gate 的不变量，
以及 task 运行前的 ACCEPTED → COMPLETED 路径；三者不计入上述八个 required theorem。

## 审查发现

### HIGH：递归图遍历没有可执行深度上限

- `事实`：compile reachability 使用递归 `mark_reachable_from_stage()`；cycle 校验使用递归
  `dfs_cycle()`；runtime reachability 和 filtered downstream propagation 也递归调用，分别见
  `turbo_flow/src/flow_compile.c:238`、`turbo_flow/src/flow_compile.c:1160`、
  `turbo_flow/src/flow_completion.c:5` 与 `turbo_flow/src/flow_completion.c:98`。
- `事实`：stage/edge 容器以 `SIZE_MAX` 初始化，当前未检索到 graph stage/depth 的显式配置上限。
- `推论`：足够深且合法的外部 Graph DSL 可在 compile 或每条 message 的 reachability/filtered
  propagation 中耗尽 C 栈并使进程崩溃；实际阈值依平台栈大小与编译优化而异。
- `影响`：Graph 编译、同步 publish、异步 ingress 最终都可能受影响；Lean 的逻辑终止性不能消除
  C 调用栈风险。
- `最小修复方向`：将三处 DFS/propagation 改为复用有界 workspace 的显式栈/队列，或在 parser/
  compiler 入口强制并文档化最大 stage 数与最大 graph 深度；加入深链 compile/publish 回归测试。

### MED：形式化前提尚未成为 C/Lean 间的可检查契约

- `事实`：现有测试覆盖 cycle、失败停止、conditional no-match、conditional fan-in 与 task cancel，
  见 `turbo_flow/tests/test_turbo_flow.c:1869`、`:3251`、`:4806`、`:4830`、`:5536`。
- `事实`：仓库在本变更前没有 `.lean` 或 `lakefile.toml`；当前仍没有 C/Lean refinement
  proof 或 trace checker。
- `推论`：即使抽象定理通过，未来 C 调度修改仍可能偏离 Lean 模型而不触发失败。
- `影响`：证明主要防止模型层设计错误，不能单独充当 C 回归测试。
- `最小修复方向`：在 Lean README 中维护逐函数映射；后续可让 C 测试导出小图 trace，再由
  Lean reference evaluator 或共享生成的 truth table 做差分验证。

### LOW：运行时索引的宽度前提没有显式写入公开 graph contract

- `事实`：公开计数使用 `size_t`，runtime node/edge stage index 与 incoming/outgoing count 使用
  `uint32_t`，例如 `turbo_flow/src/flow_internal.h:174` 与 `:181`；plan 构建处存在从 `size_t`
  到 `uint32_t` 的转换。
- `推论`：实际内存通常会先限制 graph 规模，但形式模型仍需把 stage 数可表示性列为前提。
- `影响`：极端 graph 的计数截断/溢出边界缺乏明确契约和针对性测试。
- `最小修复方向`：compile 入口显式拒绝 stage/edge/in-degree 超过运行时索引宽度的 graph，并测试
  错误语义；不依赖“现实中分配不到”作为隐式保护。

## 验证命令

形式化目录完成后使用：

```powershell
lake --dir formal build TurboFlow.ModelProofs
lake --dir formal env lean formal/TurboFlow/ModelProofs.lean
lake --dir formal build
```

Lean 工具链固定为本机已验证版本 `v4.33.1`，不引入 Mathlib 或网络依赖。
