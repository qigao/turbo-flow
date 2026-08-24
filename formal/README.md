# TurboFlow Lean 核心模型

此 Lake package 用 Lean 4 对 TurboFlow 单条 message 的路由、fan-in 与生命周期控制规则建立可执行的抽象模型。Lean kernel 检查的是该模型及其前提；它不是 C 源码、编译器输出或 C11 并发内存模型的 refinement proof。

完整模型边界、前提、C 映射与审查风险见 [形式化模型规范](../docs/FORMAL_FLOW_MODEL.md)。

## 可复验检查

在仓库根目录执行：

```powershell
lean --version
lake --dir formal build TurboFlow.ModelProofs
lake --dir formal env lean formal/TurboFlow/ModelProofs.lean
lake --dir formal build
rg.exe -n "\bsorry\b|\badmit\b|axiom" formal -g "*.lean"
```

预期：`lean --version` 显示 `v4.33.1`；最小 `TurboFlow.ModelProofs` target、直接 proof 文件的 kernel check 与完整 Lake package build 均以 exit code 0 完成；最后的扫描无输出且以 exit code 1 完成，表示没有匹配到占位证明。扫描的 exit code 1 是预期成功，不是失败。先构建最小 target，确保干净 checkout 所需的 imported `.olean` 已生成，再直接检查 proof 文件，最后构建完整 package。

## 已检查定理与 C 映射

下列八项是本模型的 required theorem；行号对应当前工作树。

| Lean theorem | C 事实源 | 覆盖的抽象语义 |
| --- | --- | --- |
| `reject_inactive_on_success` | `turbo_flow/src/flow_completion.c:38-51` | 成功 completion 不激活 reject edge。 |
| `only_reject_active_on_failure` | `turbo_flow/src/flow_completion.c:45-51` | 失败 completion 只可能激活 reject edge。 |
| `flow_transition_preserves_allowed` | `turbo_flow/include/turbo_flow.h:1327-1334`；`src/flow_parser.c:1088-1108`；`src/flow_compile.c:1195-1249`；`src/flow_runtime.c:128-173,442-467`；`src/flow_core.c:18-27,418-429` | `nextFlowState` 的成功、停止、重置与显式失败迁移都属于允许关系。 |
| `task_terminal_is_absorbing` | `turbo_flow/src/flow_internal.h:284-290`；`src/flow_execution.c:28-43` | completed/canceled execution task 不会再被 completion 改写。 |
| `resolve_preserves_valid` | `turbo_flow/src/flow_completion.c:98-145` | 每个潜在前驱决议一次时，remaining/activated 的 gate 不变量保持。 |
| `ready_requires_all_processed` | `turbo_flow/src/flow_completion.c:135-140` | 仅在所有潜在前驱已处理且至少一条 active 时入 ready queue。 |
| `filtered_requires_all_processed` | `turbo_flow/src/flow_completion.c:135-146` | 所有潜在前驱已处理且没有 active edge 时，target 被 filtered。 |
| `terminal_gate_cannot_resolve` | `turbo_flow/src/flow_completion.c:112-145` | terminal gate 没有剩余 predecessor，不能再次 resolve 或重复 ready。 |

以下是 supporting theorem，不计入上述八个 required theorem：

| Lean theorem | C 事实源 | 用途 |
| --- | --- | --- |
| `named_route_prioritizes_target_match` | `turbo_flow/src/flow_completion.c:52-58` | 当外层已保证当前 source stage 同源且 route 非空时，named route 按 target name 匹配，优先于 unconditional/conditional。 |
| `initial_valid` | `turbo_flow/src/flow_runtime.c:495-566` | 可达子图的 potential 由 runtime edge 计数初始化；正 potential 的抽象初始 gate 满足不变量。 |

## 路由与完成边界

`RouteDecision.namedRoute` 不试图在 Lean 中表示字符串或图查询。它的构造前提是外层已验证：route 属于当前 source stage，且 route 非空；C 对应的拒绝路径是 `flow_data_route_exists()` 与 `flow_apply_completion()`（`turbo_flow/src/flow_completion.c:84-96,170-183`）。在此前提下，`edgeActive` 按 C 的优先级求值：失败 → reject；成功 → reject 禁用 → named route target match → unconditional → conditional predicate。predicate 的求值、错误和类型检查仍在模型边界之外（`turbo_flow/src/flow_completion.c:60-80`）。

`FanInGate.resolve` 只表示单个 target 的计数语义。它不覆盖 C 的递归 filtered-downstream propagation、队列容量错误、observer 副作用或并发交错；这些路径仍由 `flow_release_downstream()` 和 `flow_apply_completion()` 管理。
