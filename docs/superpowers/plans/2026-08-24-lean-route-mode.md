# Lean Stage-Global Route Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 Lean route reference evaluator 以一次 completion 共享的 route mode 计算逐 edge mask，排除 C 不可能产生的 mixed named/no-named observation。

**Architecture:** 保留 `Model.lean` 中 `RouteDecision` 与 `edgeActive` 作为单步事实源。在 `Trace.lean` 将 route mode 提升为 `RouteObservation` 的全局字段，每条 edge 只保存 `EdgeKind` 与 target-match；薄转换函数在 map 内组合两者。用两个可计算 example 和三个 pointwise/length theorem 固化输入域与输出语义。

**Tech Stack:** Lean 4.33.1、Lake、TurboFlow C runtime 事实源。

**Spec:** `docs/FORMAL_FLOW_ROUTE_MODE.md`

## Global Constraints

- route mode 必须属于整个 `RouteObservation`；edge observation 不得携带 named/no-named mode。
- `edgeActive`、`RouteDecision`、Flow/task/fan-in 模型和全部 C API 保持不变。
- 必须先获得与缺失 stage-global mode 直接相关的 RED，再修改生产 Lean 定义。
- 不得引入 fallback、有效性布尔标志或平行列表长度契约。
- 不修改 `CMakeLists.txt`、`CMakeUserPresets.json`、`presets/`、`vcpkg.json` 或 `turbo_flow/`。
- 不得声称建立 C/Lean refinement proof。

---

### Task 1: 收紧 route observation 输入域并同步证明

**Files:**
- Modify: `formal/TurboFlow/Trace.lean`
- Modify: `formal/TurboFlow/TraceProofs.lean`
- Modify: `formal/README.md`
- Modify: `docs/FORMAL_FLOW_TRACE_MODEL.md`
- Modify: `docs/superpowers/plans/2026-08-24-lean-flow-traces.md`
- Add: `docs/FORMAL_FLOW_ROUTE_MODE.md`
- Add: `docs/superpowers/plans/2026-08-24-lean-route-mode.md`

**Interfaces:**
- Consumes: `RouteDecision`、`edgeActive`、`EdgeKind`、`StageResult`。
- Produces: `RouteMode`、`RouteMode.decision`、迁移后的 `RouteEdgeObservation`/`RouteObservation`/`routeMask`、`routeMask_named_pointwise`、`routeMask_no_named_pointwise`。

- [ ] **Step 1: 写入期望 API 的两个回归 example 并确认 RED**

先只修改 `formal/TurboFlow/TraceProofs.lean`，写入以下期望形状；此时生产类型还没有 `mode` 与 `matchesCurrentTarget`，因此必须因缺失的新 API 而失败：

```lean
example : routeMask {
    result := .ok
    mode := .namedRoute
    edges := [
      { kind := .unconditional, matchesCurrentTarget := true },
      { kind := .unconditional, matchesCurrentTarget := false }
    ]
  } = [true, false] := by decide

example : routeMask {
    result := .ok
    mode := .noNamedRoute
    edges := [
      { kind := .unconditional, matchesCurrentTarget := true },
      { kind := .unconditional, matchesCurrentTarget := false }
    ]
  } = [true, true] := by decide
```

Run:

```powershell
lake --dir formal build TurboFlow.TraceProofs
```

Expected: exit `1`，错误明确指出 `RouteObservation.mode` 或 `RouteEdgeObservation.matchesCurrentTarget` 不存在，而不是 import、工具链或语法错误。

- [ ] **Step 2: 实现最小 stage-global route mode**

将 `formal/TurboFlow/Trace.lean` 的 route observation 定义改为：

```lean
inductive RouteMode where
  | noNamedRoute
  | namedRoute
  deriving DecidableEq, Repr

def RouteMode.decision (mode : RouteMode) (matchesCurrentTarget : Bool) : RouteDecision :=
  match mode with
  | .noNamedRoute => .noNamedRoute
  | .namedRoute => .namedRoute matchesCurrentTarget

structure RouteEdgeObservation where
  kind : EdgeKind
  matchesCurrentTarget : Bool
  deriving DecidableEq, Repr

structure RouteObservation where
  result : StageResult
  mode : RouteMode
  edges : List RouteEdgeObservation
  deriving DecidableEq, Repr

def routeMask (observation : RouteObservation) : List Bool :=
  observation.edges.map fun edge =>
    edgeActive observation.result
      (observation.mode.decision edge.matchesCurrentTarget) edge.kind
```

不得修改 `formal/TurboFlow/Model.lean` 的 `RouteDecision` 或 `edgeActive`。

- [ ] **Step 3: 确认 GREEN 并加入明确的 pointwise 定理**

Run:

```powershell
lake --dir formal build TurboFlow.TraceProofs
```

Expected: 两个 example 通过，exit `0`。

更新 `routeMask_pointwise` 的右侧为：

```lean
edgeActive observation.result
  (observation.mode.decision edge.matchesCurrentTarget) edge.kind
```

新增：

```lean
theorem routeMask_named_pointwise (result : StageResult)
    (edges : List RouteEdgeObservation) (index : Nat) :
    (routeMask { result := result, mode := .namedRoute, edges := edges })[index]? =
      edges[index]?.map fun edge =>
        edgeActive result (.namedRoute edge.matchesCurrentTarget) edge.kind := by
  simp [routeMask, RouteMode.decision]

theorem routeMask_no_named_pointwise (result : StageResult)
    (edges : List RouteEdgeObservation) (index : Nat) :
    (routeMask { result := result, mode := .noNamedRoute, edges := edges })[index]? =
      edges[index]?.map fun edge =>
        edgeActive result .noNamedRoute edge.kind := by
  simp [routeMask, RouteMode.decision]
```

保留并适配 `routeMask_length`。随后再次运行最小 target，预期 exit `0`。

- [ ] **Step 4: 同步当前规范与 README**

在 `formal/README.md` 和 `docs/FORMAL_FLOW_TRACE_MODEL.md` 中说明：

- `RouteObservation.mode` 是一次 completion 的全局 mode；
- edge 只提供 `matchesCurrentTarget`；
- named example 为 `[true,false]`，no-named 对照为 `[true,true]`；
- theorem 清单增加 `routeMask_named_pointwise` 与 `routeMask_no_named_pointwise`；
- 仍无 C exporter、序列化契约、自动差分 runner 或 refinement proof。

同步旧 trace plan 中的结构、example 和 theorem 片段，使已提交计划不再描述 mixed-mode 输入。

- [ ] **Step 5: 执行干净验证**

Run:

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

Expected: Lean `4.33.1`；版本、clean、focused、direct 与 full build 操作均 exit `0`；placeholder scan 无输出且 exit `1`；diff check exit `0`；protected diff 无输出。

- [ ] **Step 6: 提交**

```powershell
git add formal/TurboFlow/Trace.lean formal/TurboFlow/TraceProofs.lean formal/README.md docs/FORMAL_FLOW_TRACE_MODEL.md docs/FORMAL_FLOW_ROUTE_MODE.md docs/superpowers/plans/2026-08-24-lean-flow-traces.md docs/superpowers/plans/2026-08-24-lean-route-mode.md
git commit -m "fix: enforce stage-global route mode"
```
