# Lean Flow Trace Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 TurboFlow Lean 单步模型上增加可执行事件序列解释器，并证明 Flow、task 与 fan-in 的多步轨迹保持既有语义约束。

**Architecture:** 新增一个无领域假设的 `runTrace` 核心，再用薄 wrapper 绑定现有 `nextFlowState`、`nextTaskState`、`FanInGate.resolve` 与 `edgeActive`。证明分为通用序列定理和 TurboFlow 领域定理；C 代码、CMake 与公开 ABI 保持不变。

**Tech Stack:** Lean 4.33.1、Lake、Lean Std；无 Mathlib、无网络依赖、无 C/C++ 生成代码。

**Spec:** `docs/FORMAL_FLOW_TRACE_MODEL.md`

## Global Constraints

- 工作目录固定为 `C:\projects\cpp\turbonet\turbo-flow-worktrees\lean-flow-model`，分支固定为 `proof/lean-flow-model`。
- 不修改 `CMakeLists.txt`、`CMakeUserPresets.json`、`presets/*.json`、`vcpkg.json` 或任何 C/C++ 文件。
- 现有八个 required theorem 的名称、参数和语义保持不变。
- `runTrace` 任一步失败即返回 `none`，不得跳过事件、自动修复或 fallback。
- 全部 theorem 与 example 必须由 Lean kernel 检查，不得出现 `sorry`、`admit` 或新 axiom。
- 每个任务先运行最小 proof target，再运行直接 Lean 检查；最终任务运行完整 Lake build。

---

### Task 1: 建立通用 trace kernel

**Files:**
- Create: `formal/TurboFlow/Trace.lean`
- Create: `formal/TurboFlow/TraceProofs.lean`

**Interfaces:**
- Consumes: Lean `Option` 与 `List`；本任务不依赖 TurboFlow 领域状态。
- Produces: `runTrace`、`TransitionPath`、`runTrace_append`、`runTrace_builds_path`、`runTrace_preserves_invariant`。

- [ ] **Step 1: 写引用缺失接口的失败 proof**

先创建 `formal/TurboFlow/TraceProofs.lean`：

```lean
import TurboFlow.Trace

namespace TurboFlow

private def incrementUntilTwo (state : Nat) (_ : Unit) : Option Nat :=
  if state < 2 then some (state + 1) else none

example : runTrace incrementUntilTwo 0 [(), ()] = some 2 := by decide
example : runTrace incrementUntilTwo 0 [(), (), ()] = none := by decide

end TurboFlow
```

- [ ] **Step 2: 运行 RED，确认失败原因是缺少 Trace module**

Run: `lake --dir formal build TurboFlow.TraceProofs`

Expected: FAIL，首个相关错误指出 `TurboFlow.Trace` module 不存在；不得接受工具链、路径或语法错误作为 RED。

- [ ] **Step 3: 实现最小通用解释器与 path 证据类型**

创建 `formal/TurboFlow/Trace.lean`：

```lean
namespace TurboFlow

universe u v

def runTrace {State : Type u} {Event : Type v}
    (step : State → Event → Option State) : State → List Event → Option State
  | state, [] => some state
  | state, event :: events =>
      match step state event with
      | none => none
      | some next => runTrace step next events

inductive TransitionPath {State : Type u} {Event : Type v}
    (relation : State → State → Prop) : State → List Event → State → Prop where
  | nil (state : State) : TransitionPath relation state [] state
  | cons {state next final : State} {events : List Event}
      (event : Event) (head : relation state next)
      (tail : TransitionPath relation next events final) :
      TransitionPath relation state (event :: events) final

end TurboFlow
```

Run: `lake --dir formal build TurboFlow.TraceProofs`

Expected: PASS；两个计算 example 均由 `decide` 关闭。

- [ ] **Step 4: 添加并检查三个通用序列定理**

在 `formal/TurboFlow/Trace.lean` 的 namespace 中加入：

```lean
theorem runTrace_append {State : Type u} {Event : Type v}
    (step : State → Event → Option State) (state : State)
    (first second : List Event) :
    runTrace step state (first ++ second) =
      (runTrace step state first).bind (fun middle => runTrace step middle second) := by
  induction first generalizing state with
  | nil => rfl
  | cons event events ih =>
      simp only [List.cons_append, runTrace]
      cases step state event <;> simp [ih, runTrace]

theorem runTrace_builds_path {State : Type u} {Event : Type v}
    (step : State → Event → Option State) (relation : State → State → Prop)
    (stepSound : ∀ state event next, step state event = some next → relation state next)
    {state final : State} {events : List Event}
    (completed : runTrace step state events = some final) :
    TransitionPath relation state events final := by
  induction events generalizing state with
  | nil =>
      simp [runTrace] at completed
      subst final
      exact .nil state
  | cons event events ih =>
      cases transition : step state event with
      | none => simp [runTrace, transition] at completed
      | some next =>
          have tail : runTrace step next events = some final := by
            simpa [runTrace, transition] using completed
          exact .cons event (stepSound state event next transition) (ih tail)

theorem runTrace_preserves_invariant {State : Type u} {Event : Type v}
    (step : State → Event → Option State) (invariant : State → Prop)
    (stepPreserves : ∀ state event next,
      invariant state → step state event = some next → invariant next)
    {state final : State} {events : List Event}
    (valid : invariant state) (completed : runTrace step state events = some final) :
    invariant final := by
  induction events generalizing state with
  | nil =>
      simp [runTrace] at completed
      simpa [completed] using valid
  | cons event events ih =>
      cases transition : step state event with
      | none => simp [runTrace, transition] at completed
      | some next =>
          have nextValid := stepPreserves state event next valid transition
          have tail : runTrace step next events = some final := by
            simpa [runTrace, transition] using completed
          exact ih nextValid tail
```

Run:

```powershell
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean formal/TurboFlow/Trace.lean
```

Expected: 两条命令均 PASS。Lean 4.33.1 若只要求 proof-script 层面的参数显式化，可调整 tactic，不得改变 theorem statement。

- [ ] **Step 5: 提交通用 trace kernel**

```powershell
git add formal/TurboFlow/Trace.lean formal/TurboFlow/TraceProofs.lean
git commit -m "proof: add executable trace kernel"
```

### Task 2: 将 Flow、task、route 与 fan-in 提升到多步轨迹

**Files:**
- Modify: `formal/TurboFlow/Model.lean`
- Modify: `formal/TurboFlow/ModelProofs.lean`
- Modify: `formal/TurboFlow/Trace.lean`
- Modify: `formal/TurboFlow/TraceProofs.lean`
- Modify: `formal/TurboFlow.lean`

**Interfaces:**
- Consumes: Task 1 的 `runTrace`/`TransitionPath` 和既有单步模型。
- Produces: `AllowedTaskTransition`、`RouteObservation`、`routeMask`、三个领域 trace wrapper，以及规范列出的六个领域多步 theorem。

- [ ] **Step 1: 先写四个可计算领域 example 并确认 RED**

将 `formal/TurboFlow/TraceProofs.lean` 的 import 改为：

```lean
import TurboFlow.ModelProofs
import TurboFlow.Trace
```

在 namespace 中加入：

```lean
example : runFlowTrace .new [.parse, .compile, .start, .stop] = some .stopped := by decide
example : runTaskTrace .new [.accept, .run, .complete] = some .completed := by decide
example : runGateTrace (FanInGate.initial 2) [true, false] =
    some { potential := 2, processed := 2, activated := 1, outcome := .ready } := by decide
example : runGateTrace (FanInGate.initial 2) [true, false, true] = none := by decide
```

Run: `lake --dir formal build TurboFlow.TraceProofs`

Expected: FAIL，错误只指出 `runFlowTrace`、`runTaskTrace` 或 `runGateTrace` 尚未定义。

- [ ] **Step 2: 增加 task 合法单步关系和 soundness theorem**

在 `formal/TurboFlow/Model.lean` 的 `nextTaskState` 后加入：

```lean
inductive AllowedTaskTransition : TaskState → TaskState → Prop where
  | acceptNew : AllowedTaskTransition .new .accepted
  | runAccepted : AllowedTaskTransition .accepted .running
  | completeAccepted : AllowedTaskTransition .accepted .completed
  | cancelAccepted : AllowedTaskTransition .accepted .canceled
  | completeRunning : AllowedTaskTransition .running .completed
  | cancelRunning : AllowedTaskTransition .running .canceled
```

在 `formal/TurboFlow/ModelProofs.lean` 中加入：

```lean
theorem task_transition_preserves_allowed (state next : TaskState) (event : TaskEvent)
    (h : nextTaskState state event = some next) :
    AllowedTaskTransition state next := by
  cases state <;> cases event <;> simp [nextTaskState] at h
  all_goals subst next
  all_goals first
    | exact .acceptNew
    | exact .runAccepted
    | exact .completeAccepted
    | exact .cancelAccepted
    | exact .completeRunning
    | exact .cancelRunning
```

Run: `lake --dir formal build TurboFlow.ModelProofs`

Expected: PASS，且原有 theorem 不变。

- [ ] **Step 3: 实现领域 wrapper 与 route reference evaluator**

在 `formal/TurboFlow/Trace.lean` 顶部加入：

```lean
import TurboFlow.Model
import TurboFlow.FanIn
```

在通用定理后加入：

```lean
structure RouteObservation where
  result : StageResult
  decision : RouteDecision
  edges : List EdgeKind
  deriving DecidableEq, Repr

def routeMask (observation : RouteObservation) : List Bool :=
  observation.edges.map (edgeActive observation.result observation.decision)

def runFlowTrace : FlowState → List FlowEvent → Option FlowState :=
  runTrace nextFlowState

def runTaskTrace : TaskState → List TaskEvent → Option TaskState :=
  runTrace nextTaskState

def runGateTrace : FanInGate → List Bool → Option FanInGate :=
  runTrace FanInGate.resolve
```

Run: `lake --dir formal build TurboFlow.TraceProofs`

Expected: 四个领域 example 全部 PASS。

- [ ] **Step 4: 证明领域多步性质**

在 `formal/TurboFlow/TraceProofs.lean` 中加入：

```lean
theorem routeMask_length (observation : RouteObservation) :
    (routeMask observation).length = observation.edges.length := by
  simp [routeMask]

theorem flow_trace_preserves_allowed_path (state final : FlowState)
    (events : List FlowEvent) (completed : runFlowTrace state events = some final) :
    TransitionPath AllowedFlowTransition state events final := by
  exact runTrace_builds_path nextFlowState AllowedFlowTransition
    flow_transition_preserves_allowed completed

theorem task_trace_preserves_allowed_path (state final : TaskState)
    (events : List TaskEvent) (completed : runTaskTrace state events = some final) :
    TransitionPath AllowedTaskTransition state events final := by
  exact runTrace_builds_path nextTaskState AllowedTaskTransition
    task_transition_preserves_allowed completed

theorem gate_trace_preserves_valid (gate final : FanInGate) (events : List Bool)
    (valid : gate.Valid) (completed : runGateTrace gate events = some final) :
    final.Valid := by
  exact runTrace_preserves_invariant FanInGate.resolve FanInGate.Valid
    resolve_preserves_valid valid completed

theorem gate_trace_rejects_extra_resolution (gate final : FanInGate)
    (events : List Bool) (selected : Bool) (valid : final.Valid)
    (completed : runGateTrace gate events = some final)
    (terminal : final.outcome = .ready ∨ final.outcome = .filtered) :
    runGateTrace gate (events ++ [selected]) = none := by
  rw [runGateTrace, runTrace_append, completed]
  simp [runTrace, terminal_gate_cannot_resolve final selected valid terminal]
```

Run:

```powershell
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean formal/TurboFlow/TraceProofs.lean
```

Expected: 两条命令均 PASS；若需要补充 wrapper 展开，只在 proof 中加入 `runFlowTrace`、`runTaskTrace` 或 `runGateTrace` 的显式展开。

- [ ] **Step 5: 将 trace proof 纳入默认 library 并提交**

把 `formal/TurboFlow.lean` 改为：

```lean
import TurboFlow.ModelProofs
import TurboFlow.TraceProofs
```

Run: `lake --dir formal build`

Expected: PASS，输出包含 `TurboFlow.Trace` 与 `TurboFlow.TraceProofs` 的构建任务。

```powershell
git add formal/TurboFlow.lean formal/TurboFlow/Model.lean formal/TurboFlow/ModelProofs.lean formal/TurboFlow/Trace.lean formal/TurboFlow/TraceProofs.lean
git commit -m "proof: lift TurboFlow invariants to execution traces"
```

### Task 3: 更新证明边界与复验入口

**Files:**
- Modify: `docs/FORMAL_FLOW_MODEL.md`
- Add: `docs/FORMAL_FLOW_TRACE_MODEL.md`
- Modify: `formal/README.md`
- Add: `docs/superpowers/plans/2026-08-24-lean-flow-traces.md`

**Interfaces:**
- Consumes: Task 1/2 的全部公开定义、theorem 和可计算 example。
- Produces: trace 模型的 C 映射、验证命令、已缩小但仍存在的 refinement 风险说明。

- [ ] **Step 1: 更新 formal README 的模块与 theorem 清单**

在 `formal/README.md` 中新增 `Trace.lean`/`TraceProofs.lean` 模块说明，列出规范中的九个 trace theorem，并加入：

```powershell
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean formal/TurboFlow/TraceProofs.lean
```

明确写出：reference evaluator 已可执行事件列表，但 C observer exporter、trace 格式和自动差分 runner 仍不在当前证明范围。

- [ ] **Step 2: 更新主规范中的 refinement 风险状态**

将 `docs/FORMAL_FLOW_MODEL.md` 的 MED 发现改为以下事实边界：

```text
事实：Lean 已提供 route/lifecycle/fan-in 的可执行 reference evaluator 和多步保持定理；
当前仍没有版本化 C trace exporter、序列化契约或自动差分 runner。
影响：模型能检查手工/生成的 Lean 输入，但 C 调度变化仍不会自动触发 Lean 差分失败。
下一最小边界：由 C 测试导出不含 payload 的版本化 observation trace，并在 CI 中比较最终状态与 route mask。
```

不得把 reference evaluator 描述成 refinement proof。

- [ ] **Step 3: 执行完整形式化验证**

Run:

```powershell
lean --version
lake --dir formal clean
lake --dir formal build TurboFlow.TraceProofs
lake --dir formal env lean formal/TurboFlow/TraceProofs.lean
lake --dir formal build
rg.exe -n "\b(sorry|admit|axiom)\b" formal -g "*.lean"
git diff --check
```

Expected: Lean `4.33.1`；四个 Lake/Lean 操作以 exit code 0 完成；placeholder scan 无输出并以 exit code 1 完成；`git diff --check` 为 0。

- [ ] **Step 4: 复核禁止修改范围与提交文档**

Run:

```powershell
git diff --name-only 2dc87ba -- CMakeLists.txt CMakeUserPresets.json presets vcpkg.json turbo_flow
```

Expected: 无输出。随后提交：

```powershell
git add docs/FORMAL_FLOW_MODEL.md docs/FORMAL_FLOW_TRACE_MODEL.md docs/superpowers/plans/2026-08-24-lean-flow-traces.md formal/README.md
git commit -m "docs: specify executable Lean flow traces"
```

## Self-Review

- Spec coverage：通用 trace、Flow/task path、fan-in 多步不变量、terminal 拒绝、route mask 和文档边界均有对应任务。
- Placeholder scan：计划没有实现性占位；所有新增接口、theorem statement、验证命令和预期失败原因均已给出。
- Type consistency：三个 wrapper 都采用 `State → List Event → Option State`；`TransitionPath` 保留事件列表索引；`AllowedTaskTransition` 与 `nextTaskState` 的六条成功分支一一对应。
- Compatibility：不修改现有 theorem，不修改 C/CMake/vcpkg，不引入 I/O、JSON、Mathlib 或 C refinement 声明。
