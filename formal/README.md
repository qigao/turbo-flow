# TurboFlow Lean 核心模型

此 Lake package 用 Lean 4 对 TurboFlow 单条 message 的路由、fan-in 与生命周期控制规则建立可执行的抽象模型。Lean kernel 检查的是该模型及其前提；它不是 C 源码、编译器输出或 C11 并发内存模型的 refinement proof。

完整模型边界、前提、C 映射与审查风险见 [形式化模型规范](../docs/FORMAL_FLOW_MODEL.md)；
事件序列解释器的输入、定理与差分边界见 [可执行轨迹模型规范](../docs/FORMAL_FLOW_TRACE_MODEL.md)；
stage-global route mode 的输入域约束见 [路由 mode 修正规范](../docs/FORMAL_FLOW_ROUTE_MODE.md)。

## 模块

- `TurboFlow.Model`、`TurboFlow.FanIn`：单步 lifecycle、路由与 fan-in 抽象；
- `TurboFlow.ModelProofs`：单步模型的 required 与 supporting theorem；
- `TurboFlow.Trace`：通用 `runTrace`、路径证据及 Flow/task/fan-in/route 的可执行 wrapper；
- `TurboFlow.TraceProofs`：通用与领域轨迹定理，以及由 Lean kernel 归约的可计算示例。

## 可复验检查

在仓库根目录执行：

```powershell
lean --version
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean formal/TurboFlow/TraceProofs.lean
lake --dir formal build
rg.exe -n "\b(sorry|admit|axiom)\b" formal -g "*.lean"
```

预期：`lean --version` 显示 `v4.33.1`；最小 `TurboFlow.TraceProofs` target、直接 proof 文件的 kernel check 与完整 Lake package build 均以 exit code 0 完成；最后的扫描无输出且以 exit code 1 完成，表示没有匹配到占位证明。扫描的 exit code 1 是预期成功，不是失败。先构建最小 target，确保干净 checkout 所需的 imported `.olean` 已生成，再直接检查 proof 文件，最后构建完整 package。

Package 提交了无外部依赖的 `lake-manifest.json`，因此执行上述检查不会在干净 checkout 中
新增 lockfile。

## 已检查定理与 C 映射

下列八项是本模型的 required theorem；行号对应当前工作树。

| Lean theorem | C 事实源 | 覆盖的抽象语义 |
| --- | --- | --- |
| `reject_inactive_on_success` | `turbo_flow/src/flow_completion.c:38-51` | 成功 completion 不激活 reject edge。 |
| `only_reject_active_on_failure` | `turbo_flow/src/flow_completion.c:45-51` | 失败 completion 只可能激活 reject edge。 |
| `flow_transition_preserves_allowed` | `turbo_flow/include/turbo_flow.h:1327-1334`；`turbo_flow/src/flow_parser.c:1088-1108`；`turbo_flow/src/flow_compile.c:1195-1249`；`turbo_flow/src/flow_runtime.c:128-173,442-467`；`turbo_flow/src/flow_core.c:17-39,418-429` | `nextFlowState` 的成功、停止、重置与显式失败迁移都属于允许关系；`.fail` 仅表示已选择 `flow_set_error()` 的边界。 |
| `task_terminal_is_absorbing` | `turbo_flow/src/flow_internal.h:284-290`；`turbo_flow/src/flow_execution.c:28-43` | completed/canceled execution task 不会再被 completion 改写。 |
| `resolve_preserves_valid` | `turbo_flow/src/flow_completion.c:98-145` | 每个潜在前驱决议一次时，remaining/activated 的 gate 不变量保持。 |
| `ready_requires_all_processed` | `turbo_flow/src/flow_completion.c:135-140` | 仅在所有潜在前驱已处理且至少一条 active 时入 ready queue。 |
| `filtered_requires_all_processed` | `turbo_flow/src/flow_completion.c:135-146` | 所有潜在前驱已处理且没有 active edge 时，target 被 filtered。 |
| `terminal_gate_cannot_resolve` | `turbo_flow/src/flow_completion.c:112-145` | terminal gate 没有剩余 predecessor，不能再次 resolve 或重复 ready。 |

以下是 supporting theorem，不计入上述八个 required theorem：

| Lean theorem | C 事实源 | 用途 |
| --- | --- | --- |
| `named_route_prioritizes_target_match` | `turbo_flow/src/flow_completion.c:52-58` | 当外层已保证当前 source stage 同源且 route 非空时，named route 按 target name 匹配，优先于 unconditional/conditional。 |
| `initial_valid` | `turbo_flow/src/flow_runtime.c:495-566` | 可达子图的 potential 由 runtime edge 计数初始化；正 potential 的抽象初始 gate 满足不变量。 |
| `accepted_complete_reaches_completed` | `turbo_flow/src/flow_execution.c:28-43,126-128` | `flow_execution_task_fail()` 可在 task 运行前通过 `flow_execution_task_complete()` 从 ACCEPTED 进入 COMPLETED。 |

## 可执行轨迹定理

`TurboFlow.Trace`/`TurboFlow.TraceProofs` 在已有单步事实源上提供以下十二个 trace theorem：

1. `runTrace_append`：分段执行等价于拼接后一次执行；
2. `runTrace_builds_path`：成功执行产生逐步 relation path；
3. `runTrace_preserves_invariant`：单步保持的不变量保持到成功轨迹终点；
4. `task_transition_preserves_allowed`：成功 task 单步属于 `AllowedTaskTransition`；
5. `flow_trace_preserves_allowed_path`：成功 Flow 轨迹仅含允许迁移；
6. `task_trace_preserves_allowed_path`：成功 task 轨迹仅含允许迁移；
7. `gate_trace_preserves_valid`：有效 fan-in gate 的成功多步 resolution 保持有效；
8. `gate_trace_rejects_extra_resolution`：terminal gate 后追加 resolution 被拒绝；
9. `routeMask_length`：route mask 与输入 edge observation 列表长度相同；
10. `routeMask_pointwise`：每个 mask 索引等于将 observation 的全局 mode 与同索引 edge 的 target match 组合后所得的 `edgeActive` 结果；
11. `routeMask_named_pointwise`：named mode 下每个 mask 索引使用同索引 edge 的 target match 构造 named decision；
12. `routeMask_no_named_pointwise`：no-named mode 下每个 mask 索引使用 `.noNamedRoute`，忽略 edge 的 target match。

`runFlowTrace`、`runTaskTrace`、`runGateTrace` 与 `routeMask` 是可执行 reference evaluator：它们可以检查手工或生成的 Lean 事件列表。它们不是 C/Lean refinement proof。C observer exporter、trace 格式和自动差分 runner 仍不在当前证明范围。

## 生命周期抽象边界

`FlowEvent.fail` 仅表示错误边界已选择 `flow_set_error()`，该 C 函数会将 Flow 置为 FAILED。
通过 `flow_set_error_keep_state()` 记录但保留 Flow 状态的错误不产生 `.fail` 事件。

Task 的 `.complete` 既可表示已运行 task 的 RUNNING → COMPLETED，也可表示运行前失败的
ACCEPTED → COMPLETED。后者由 `accepted_complete_reaches_completed` 检查，不改变
`task_terminal_is_absorbing` 对 COMPLETED/CANCELED 的 terminal 性质。

## 路由与完成边界

`RouteDecision.namedRoute` 不试图在 Lean 中表示字符串或图查询。它的构造前提是外层已验证：route 属于当前 source stage，且 route 非空；C 对应的拒绝路径是 `flow_data_route_exists()` 与 `flow_apply_completion()`（`turbo_flow/src/flow_completion.c:84-96,179-183`）。`RouteObservation.mode` 是一次 completion 的全局 mode；每条 `RouteEdgeObservation` 只保存 edge kind 与该 target 的 `matchesCurrentTarget`。因此 named 双 target fan-out 产生 `[true, false]`，而相同两条 unconditional edge 在 no-named mode 下产生 `[true, true]`；对应的 C 回归场景是 `turbo_flow/tests/test_flow_policy.c:354-386`。在此前提下，`edgeActive` 按 C 的优先级求值：失败 → reject；成功 → reject 禁用 → named route target match → unconditional → conditional predicate。predicate 的求值、错误和类型检查仍在模型边界之外（`turbo_flow/src/flow_completion.c:60-80`）。

`FanInGate.resolve` 只表示单个 target 的计数语义。它不覆盖 C 的递归 filtered-downstream propagation、队列容量错误、observer 副作用或并发交错；这些路径仍由 `flow_release_downstream()` 和 `flow_apply_completion()` 管理。
