# TurboFlow Lean Route Mode 修正规范

## 目标

修正 Lean trace reference evaluator 的命名路由输入域，使一次 stage completion 只能使用一个全局 route mode，同时保留逐 edge 的 target-match 结果。

## C 事实源

- `事实`：`turbo_flow/src/flow_completion.c:52-58` 对整次 completion 读取同一个 `msg->data_decision.route`，随后逐 edge 比较该 edge 的 target name。
- `事实`：`turbo_flow/src/flow_completion.c:60-80` 仅在没有适用于当前 stage 的 named route 时才进入 unconditional/conditional 分支。
- `事实`：`turbo_flow/src/flow_completion.c:84-96,179-183` 在 release 前拒绝不存在的 named target。
- `事实`：`turbo_flow/tests/test_flow_policy.c:354-386` 覆盖两个 downstream target，并要求只执行 `selected`、不执行 `skipped`。

## 类型与所有权

route mode 是 `RouteObservation` 的 stage-global 状态；edge observation 不得自行选择 named/no-named mode。

```lean
inductive RouteMode where
  | noNamedRoute
  | namedRoute

structure RouteEdgeObservation where
  kind : EdgeKind
  matchesCurrentTarget : Bool

structure RouteObservation where
  result : StageResult
  mode : RouteMode
  edges : List RouteEdgeObservation
```

`matchesCurrentTarget` 只在 `.namedRoute` mode 下参与计算；`.noNamedRoute` 下由既有 `EdgeKind` 语义决定 active 状态。该冗余布尔值在 no-named mode 下被忽略，但不会使模型产生与 C 不同的 mask。

## 计算边界

使用一个薄转换函数把全局 mode 与逐 edge match 组合为既有单步模型的 `RouteDecision`：

```lean
def RouteMode.decision (mode : RouteMode) (matchesCurrentTarget : Bool) : RouteDecision :=
  match mode with
  | .noNamedRoute => .noNamedRoute
  | .namedRoute => .namedRoute matchesCurrentTarget
```

`routeMask` 必须对所有 edge 使用同一个 `observation.mode`：

```lean
def routeMask (observation : RouteObservation) : List Bool :=
  observation.edges.map fun edge =>
    edgeActive observation.result
      (observation.mode.decision edge.matchesCurrentTarget) edge.kind
```

不得新增 fallback、有效性布尔标志或允许每条 edge 自行声明 route mode 的并行状态。

## 可执行回归与定理

必须保留原有 `[true, false]` 双 target named fan-out example，并新增 no-named 对照 example：相同两条 unconditional edge 即使携带 `true/false` target-match，`.noNamedRoute` 仍得到 `[true, true]`。两者共同捕获“第二条 edge 错误退回 unconditional”的缺陷。

保留并更新：

- `routeMask_length`
- `routeMask_pointwise`

新增：

- `routeMask_named_pointwise`：named mode 下每个 mask 元素使用同索引 edge 的 `matchesCurrentTarget` 构造 `.namedRoute` decision。
- `routeMask_no_named_pointwise`：no-named mode 下每个 mask 元素使用 `.noNamedRoute`，不使用 edge 的 target-match 选择 route mode。

这些定理证明 reference evaluator 的输入结构与计算规则；它们不是 C/Lean refinement proof。

## 兼容性与禁止范围

- `edgeActive`、`RouteDecision`、Flow/task/fan-in 模型和全部 C API 保持不变。
- Lean `RouteObservation` 构造方式发生内部形式化 API 迁移：调用方必须在 observation 上提供一个 `mode`，并在每条 edge 上提供 `matchesCurrentTarget`。
- 不修改 `CMakeLists.txt`、`CMakeUserPresets.json`、`presets/`、`vcpkg.json` 或 `turbo_flow/`。
- 不新增 C trace exporter、序列化契约或自动差分 runner。

## 验证

```powershell
lean --version
lake --dir formal clean
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean --trust=0 formal/TurboFlow/TraceProofs.lean
lake --dir formal build
rg.exe -n "\b(sorry|admit|axiom)\b" formal -g "*.lean"
git diff --check
git diff --name-only 3396512 -- CMakeLists.txt CMakeUserPresets.json presets vcpkg.json turbo_flow
```

预期 Lean 版本为 `4.33.1`；构建与直接 kernel 检查 exit `0`；placeholder scan 无输出并以 exit `1` 完成；受保护路径无输出。
