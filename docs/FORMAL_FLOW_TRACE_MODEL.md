# TurboFlow 可执行轨迹模型规范

## 目标

在现有单步 Lean 模型之上建立确定性的事件序列解释器，使 Flow lifecycle、execution task
lifecycle、edge route 和 fan-in resolution 都能由同一 reference evaluator 执行，并证明：

1. 成功执行的 Flow 轨迹中，每一步都是 `AllowedFlowTransition`；
2. 成功执行的 task 轨迹中，每一步都是 `AllowedTaskTransition`；
3. 从有效 fan-in gate 开始的任意成功 resolution 轨迹保持 `FanInGate.Valid`；
4. fan-in 到达 ready/filtered 后，追加任意 resolution 都会被拒绝；
5. route evaluator 为每条输入 edge 绑定且只绑定一个 route decision，并逐点产生布尔 mask。

这些定义为后续 C 测试导出 observation trace、再与 Lean reference evaluator 做差分比较提供
稳定边界。本阶段不增加 C trace exporter，不解析 JSON，也不宣称建立 C/Lean refinement proof。

## 事实源

| C 事实源 | 轨迹输入 | 需要保持的语义 |
| --- | --- | --- |
| `turbo_flow/src/flow_completion.c:38-81` | `RouteEdgeObservation` | failure/reject/named/unconditional/conditional 的单 edge 判定顺序 |
| `turbo_flow/tests/test_flow_policy.c:354-386` | 双 target named fan-out | 选择 `selected`，跳过 `skipped`，对应 pointwise mask `[true, false]` |
| `turbo_flow/src/flow_completion.c:98-151` | `List Bool` | 每个潜在前驱产生一次 selected 决议，最后一个决议产生 ready 或 filtered |
| `turbo_flow/src/flow_completion.c:112-145` | terminal gate 前提 | `remaining == 0` 时拒绝重复 resolution；最后一次决议产生 ready 或 filtered |
| `turbo_flow/src/flow_completion.c:190-214,235-240` | stage `done` 前提 | 已完成 stage 不重复释放下游 |
| `turbo_flow/src/flow_execution.c:28-128` | `List TaskEvent` | accept/run/complete/cancel 以及执行前 fail-complete 路径 |
| `turbo_flow/src/flow_parser.c:1088-1108`、`turbo_flow/src/flow_compile.c:1195-1249`、`turbo_flow/src/flow_runtime.c:128-173,442-467`、`turbo_flow/src/flow_core.c:17-39,418-429` | `List FlowEvent` | public Flow lifecycle 与显式 FAILED 边界 |
| `turbo_flow/tests/test_turbo_flow.c:4806,4830,4917,4969,5536` | 可执行示例 | conditional filtered、fan-in、reject route 与 task cancel 的既有回归场景 |

## 数据与接口

### 通用解释器

```lean
def runTrace (step : state → event → Option state) :
    state → List event → Option state
```

解释器从给定状态依次执行事件。任一步返回 `none` 时整条轨迹立即失败；空轨迹返回初始状态。
它不做 fallback，也不跳过非法事件。

`TransitionPath relation initial events final` 保存与事件数相同的状态迁移证据。通用定理
`runTrace_builds_path` 将每个成功单步的 relation 证明提升为整条成功轨迹的 path 证明。
`runTrace_preserves_invariant` 将单步不变量保持证明提升为任意长度轨迹。

### 领域解释器

```lean
def runFlowTrace : FlowState → List FlowEvent → Option FlowState
def runTaskTrace : TaskState → List TaskEvent → Option TaskState
def runGateTrace : FanInGate → List Bool → Option FanInGate
def routeMask : RouteObservation → List Bool
```

这些函数只是给现有单步事实源绑定明确输入类型，不复制或改写 `nextFlowState`、
`nextTaskState`、`FanInGate.resolve`、`edgeActive` 的逻辑。

`RouteObservation` 包含一个 `StageResult`，以及按 C runtime plan 顺序排列的
`List RouteEdgeObservation`；每个元素把一条 `EdgeKind` 与该 edge 自己的 `RouteDecision` 绑定。
这在类型层消除了 edge/decision 两张列表长度不一致的状态。输出 mask 与输入 observations 等长，
第 `i` 个布尔值等于使用第 `i` 条 edge 的 decision 调用 `edgeActive` 的结果，因此 named fan-out
可以表达 `[true, false]`，不会把首条 edge 的 match 结果广播给其他 target。

### Task 合法关系

现有模型新增 `AllowedTaskTransition`，逐一列出：

- NEW → ACCEPTED；
- ACCEPTED → RUNNING；
- ACCEPTED → COMPLETED；
- ACCEPTED → CANCELED；
- RUNNING → COMPLETED；
- RUNNING → CANCELED。

`ACCEPTED → COMPLETED` 保留 `flow_execution_task_fail()` 在用户 callback 开始前完成任务的真实
路径。terminal absorbing 性质不变。

## 必须由 Lean 检查的定理

1. `runTrace_append`：分段执行与一次执行拼接后的事件列表等价。
2. `runTrace_builds_path`：成功执行产生逐步 relation path。
3. `runTrace_preserves_invariant`：单步保持的不变量被任意成功轨迹保持。
4. `task_transition_preserves_allowed`：每个成功 task 单步属于 `AllowedTaskTransition`。
5. `flow_trace_preserves_allowed_path`：成功 Flow 轨迹只含允许迁移。
6. `task_trace_preserves_allowed_path`：成功 task 轨迹只含允许迁移。
7. `gate_trace_preserves_valid`：有效 gate 的成功多步 resolution 保持有效。
8. `gate_trace_rejects_extra_resolution`：terminal gate 后追加决议使整条轨迹返回 `none`。
9. `routeMask_length`：route mask 长度等于输入 edge observation 数量。
10. `routeMask_pointwise`：每个 mask 索引恰好等于同索引 edge 自己的 `edgeActive` 结果。

另外保留五个可计算 example，分别覆盖完整 Flow lifecycle、完整 task lifecycle、两路 fan-in
ready、terminal 后第三次 resolution 被拒绝，以及双 target named fan-out 的 `[true, false]`。
example 由 kernel 归约检查，不使用外部测试框架。

## 抽象边界与兼容性

- 继续限定为单 message、有限事件列表和顺序解释；不覆盖多个 publish 的并行交错。
- 不建立 stage identity、graph adjacency 或 queue order 的完整模型。
- route predicate 的求值错误仍在模型外；只有成功得到的 Bool 进入 `EdgeKind.conditional`。
- named route 的非空、同源与存在性仍由模型外层 fail fast；进入模型后，每条 edge 只携带该
  target 比较所得的 match Bool。
- C observer trace 的格式、序列化、版本号与导出开关不在本阶段定义。
- 不改变 C API、CMake、vcpkg、运行时数据布局或用户可见调度行为。
- 新 theorem 只加强 Lean 层结论；现有八个 required theorem 的名称和语义保持兼容。

## 验证

```powershell
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean formal/TurboFlow/TraceProofs.lean
lake --dir formal build
rg.exe -n "\b(sorry|admit|axiom)\b" formal -g "*.lean"
```

前三条必须以 exit code 0 完成；placeholder scan 必须无匹配并以 exit code 1 完成。
