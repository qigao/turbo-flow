# Lean Flow Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 TurboFlow 的生命周期、edge route 与 fan-in completion 语义建立可由 Lean 4 kernel 检查的核心模型和定理。

**Architecture:** 在独立的 `formal/` Lake package 中建立纯函数模型，不修改现有 CMake 或 C ABI。模型按职责拆成 lifecycle/route 基础类型与 fan-in gate；证明文件只依赖模型，并通过 README 把每个定义映射回当前 C 函数和证明边界。

**Tech Stack:** Lean 4.33.1、Lake、Lean Std；无 Mathlib、无新增 C/C++ 依赖。

**Spec:** `docs/FORMAL_FLOW_MODEL.md`

## Global Constraints

- 不修改用户当前已暂存的 `CMakeLists.txt`、`CMakeUserPresets.json`、`presets/AndroidPresets.json`、`turbo_flow/CMakeLists.txt`。
- Lean 证明只宣称覆盖规范中的抽象边界，不宣称验证 C11 并发内存模型或 C refinement。
- 所有 theorem 必须无 `sorry`、`admit`、未声明 axiom 或占位实现。
- 先运行最小 Lean 文件，再运行完整 `lake build`；完成声明前保存实际输出。

---

### Task 1: 建立 Lean package 与 route/lifecycle 模型

**Files:**
- Create: `formal/lean-toolchain`
- Create: `formal/lakefile.toml`
- Create: `formal/TurboFlow/Model.lean`
- Create: `formal/TurboFlow/ModelProofs.lean`

**Interfaces:**
- Consumes: `docs/FORMAL_FLOW_MODEL.md` 中的 C-to-Lean 映射。
- Produces: `TurboFlow.EdgeKind`、`StageResult`、`edgeActive`、`FlowState`、`FlowEvent`、`nextFlowState`、`TaskState`、`TaskEvent`、`nextTaskState`。

- [ ] **Step 1: 写 package 文件和会失败的 proof import**

```text
# formal/lean-toolchain
leanprover/lean4:v4.33.1
```

```toml
# formal/lakefile.toml
name = "turbo_flow_formal"
version = "0.1.0"
defaultTargets = ["TurboFlow"]

[[lean_lib]]
name = "TurboFlow"
```

```lean
-- formal/TurboFlow/ModelProofs.lean
import TurboFlow.Model
```

- [ ] **Step 2: 运行 import 以确认缺少模型时失败**

Run: `lake --dir formal env lean formal/TurboFlow/ModelProofs.lean`

Expected: FAIL，错误指出 `TurboFlow.Model` 不存在。

- [ ] **Step 3: 实现 route 与生命周期的最小纯函数模型**

```lean
namespace TurboFlow

inductive EdgeKind where
  | unconditional
  | conditional (selected : Bool)
  | reject
  deriving DecidableEq, Repr

inductive StageResult where
  | ok
  | failed
  deriving DecidableEq, Repr

def edgeActive : StageResult → EdgeKind → Bool
  | .ok, .unconditional => true
  | .ok, .conditional selected => selected
  | .ok, .reject => false
  | .failed, .reject => true
  | .failed, _ => false

inductive FlowState where
  | new | parsed | compiled | started | stopped | failed
  deriving DecidableEq, Repr

inductive FlowEvent where
  | parse | compile | start | stop | reset | fail
  deriving DecidableEq, Repr

def nextFlowState : FlowState → FlowEvent → Option FlowState
  | .new, .parse => some .parsed
  | .parsed, .parse => some .parsed
  | .stopped, .parse => some .parsed
  | .failed, .parse => some .parsed
  | .parsed, .compile => some .compiled
  | .stopped, .compile => some .compiled
  | .compiled, .start => some .started
  | .stopped, .start => some .started
  | .started, .stop => some .stopped
  | .new, .reset => some .new
  | .parsed, .reset => some .new
  | .compiled, .reset => some .new
  | .stopped, .reset => some .new
  | .failed, .reset => some .new
  | _, .fail => some .failed
  | _, _ => none

inductive AllowedFlowTransition : FlowState → FlowState → Prop where
  | parseNew : AllowedFlowTransition .new .parsed
  | parseParsed : AllowedFlowTransition .parsed .parsed
  | parseStopped : AllowedFlowTransition .stopped .parsed
  | parseFailed : AllowedFlowTransition .failed .parsed
  | compileParsed : AllowedFlowTransition .parsed .compiled
  | compileStopped : AllowedFlowTransition .stopped .compiled
  | startCompiled : AllowedFlowTransition .compiled .started
  | startStopped : AllowedFlowTransition .stopped .started
  | stopStarted : AllowedFlowTransition .started .stopped
  | reset (state) : state ≠ .started → AllowedFlowTransition state .new
  | fail (state) : AllowedFlowTransition state .failed

inductive TaskState where
  | new | accepted | running | completed | canceled
  deriving DecidableEq, Repr

inductive TaskEvent where
  | accept | run | complete | cancel
  deriving DecidableEq, Repr

def nextTaskState : TaskState → TaskEvent → Option TaskState
  | .new, .accept => some .accepted
  | .accepted, .run => some .running
  | .accepted, .cancel => some .canceled
  | .running, .complete => some .completed
  | .running, .cancel => some .canceled
  | _, _ => none

end TurboFlow
```

- [ ] **Step 4: 写 route 与 terminal proof，并确认 kernel 接受**

```lean
namespace TurboFlow

theorem reject_inactive_on_success :
    edgeActive .ok .reject = false := rfl

theorem only_reject_active_on_failure (edge : EdgeKind)
    (h : edgeActive .failed edge = true) : edge = .reject := by
  cases edge <;> simp [edgeActive] at h ⊢

theorem flow_transition_preserves_allowed (state next : FlowState) (event : FlowEvent)
    (h : nextFlowState state event = some next) :
    AllowedFlowTransition state next := by
  cases state <;> cases event <;> simp [nextFlowState] at h
  all_goals subst next
  all_goals first
    | exact .parseNew
    | exact .parseParsed
    | exact .parseStopped
    | exact .parseFailed
    | exact .compileParsed
    | exact .compileStopped
    | exact .startCompiled
    | exact .startStopped
    | exact .stopStarted
    | exact .reset _ (by decide)
    | exact .fail _

theorem task_terminal_is_absorbing (state : TaskState) (event : TaskEvent)
    (h : state = .completed ∨ state = .canceled) :
    nextTaskState state event = none := by
  rcases h with rfl | rfl <;> cases event <;> rfl

end TurboFlow
```

Run: `lake --dir formal env lean formal/TurboFlow/ModelProofs.lean`

Expected: PASS，无 warning、error、`sorry`。

- [ ] **Step 5: 提交本任务**

```powershell
git add formal/lean-toolchain formal/lakefile.toml formal/TurboFlow/Model.lean formal/TurboFlow/ModelProofs.lean
git commit -m "proof: model TurboFlow routing and lifecycles"
```

### Task 2: 建立 fan-in gate 并证明调度不变量

**Files:**
- Create: `formal/TurboFlow/FanIn.lean`
- Modify: `formal/TurboFlow/ModelProofs.lean`

**Interfaces:**
- Consumes: `StageResult`/`edgeActive` 仅作为上层 route 选择来源；gate 核心只消费 `selected : Bool`。
- Produces: `GateOutcome`、`FanInGate`、`FanInGate.Valid`、`FanInGate.resolve` 及五个 gate 定理。

- [ ] **Step 1: 先写定理签名并确认缺少 FanIn module 时失败**

```lean
import TurboFlow.FanIn

namespace TurboFlow

example (gate : FanInGate) (selected : Bool) (next : FanInGate)
    (valid : gate.Valid) (resolved : gate.resolve selected = some next) :
    next.Valid := by
  exact resolve_preserves_valid gate selected next valid resolved

end TurboFlow
```

Run: `lake --dir formal env lean formal/TurboFlow/ModelProofs.lean`

Expected: FAIL，错误指出 `TurboFlow.FanIn` 不存在。

- [ ] **Step 2: 实现与 C remaining/activated 语义同构的 gate**

```lean
import Std.Tactic

namespace TurboFlow

inductive GateOutcome where
  | pending | ready | filtered
  deriving DecidableEq, Repr

structure FanInGate where
  potential : Nat
  processed : Nat
  activated : Nat
  outcome : GateOutcome
  deriving DecidableEq, Repr

namespace FanInGate

def initial (potential : Nat) : FanInGate :=
  { potential, processed := 0, activated := 0, outcome := .pending }

def Valid (gate : FanInGate) : Prop :=
  gate.processed ≤ gate.potential ∧
  gate.activated ≤ gate.processed ∧
  match gate.outcome with
  | .pending => gate.processed < gate.potential
  | .ready => gate.processed = gate.potential ∧ 0 < gate.activated
  | .filtered => gate.processed = gate.potential ∧ gate.activated = 0

def resolve (gate : FanInGate) (selected : Bool) : Option FanInGate :=
  if gate.processed < gate.potential then
    let processed := gate.processed + 1
    let activated := gate.activated + if selected then 1 else 0
    let outcome :=
      if processed = gate.potential then
        if activated > 0 then GateOutcome.ready else GateOutcome.filtered
      else GateOutcome.pending
    some { gate with processed, activated, outcome }
  else
    none

end FanInGate
end TurboFlow
```

- [ ] **Step 3: 证明初始化和单步保持不变量**

在 `formal/TurboFlow/FanIn.lean` 中加入：

```lean
theorem initial_valid (potential : Nat) (positive : 0 < potential) :
    (initial potential).Valid := by
  simp [initial, Valid, positive]

theorem resolve_preserves_valid (gate : FanInGate) (selected : Bool) (next : FanInGate)
    (valid : gate.Valid) (resolved : gate.resolve selected = some next) :
    next.Valid := by
  unfold resolve at resolved
  split at resolved <;> simp_all [Valid]
  split at resolved <;> split at resolved <;> simp_all [Valid] <;> omega
```

Run: `lake --dir formal env lean formal/TurboFlow/FanIn.lean`

Expected: PASS；若 Lean 4.33 的 simplifier 分支形状不同，只允许调整 proof script，不改变 `Valid` 与 `resolve` 的语义。

- [ ] **Step 4: 证明 ready/filtered 时机与 terminal gate 不可重复决议**

```lean
theorem ready_requires_all_processed (gate : FanInGate) (valid : gate.Valid)
    (ready : gate.outcome = .ready) :
    gate.processed = gate.potential ∧ 0 < gate.activated := by
  simpa [Valid, ready] using valid.2.2

theorem filtered_requires_all_processed (gate : FanInGate) (valid : gate.Valid)
    (filtered : gate.outcome = .filtered) :
    gate.processed = gate.potential ∧ gate.activated = 0 := by
  simpa [Valid, filtered] using valid.2.2

theorem terminal_gate_cannot_resolve (gate : FanInGate) (selected : Bool)
    (valid : gate.Valid) (terminal : gate.outcome = .ready ∨ gate.outcome = .filtered) :
    gate.resolve selected = none := by
  rcases terminal with ready | filtered
  · have done := (ready_requires_all_processed gate valid ready).1
    simp [resolve, done]
  · have done := (filtered_requires_all_processed gate valid filtered).1
    simp [resolve, done]
```

Run: `lake --dir formal env lean formal/TurboFlow/ModelProofs.lean`

Expected: PASS，无 `sorry`/axiom。

- [ ] **Step 5: 提交本任务**

```powershell
git add formal/TurboFlow/FanIn.lean formal/TurboFlow/ModelProofs.lean
git commit -m "proof: verify TurboFlow fan-in gate invariants"
```

### Task 3: 建立可复验入口与 C 映射说明

**Files:**
- Create: `formal/README.md`
- Modify: `README.md`
- Modify: `docs/FORMAL_FLOW_MODEL.md`

**Interfaces:**
- Consumes: Task 1/2 的全部定义和 theorem 名称。
- Produces: 开发者可复制执行的证明命令、逐函数映射、证明/未证明范围和审查风险入口。

- [ ] **Step 1: 写 README 中的证明清单和命令**

`formal/README.md` 必须列出八个 theorem 名称、对应 C 函数/行号、下列命令及预期结果：

```powershell
lake --dir formal build
lake --dir formal env lean formal/TurboFlow/ModelProofs.lean
rg.exe -n "\bsorry\b|\badmit\b|axiom" formal -g "*.lean"
```

前两条预期 exit code 0；最后一条预期无输出且 exit code 1（表示未匹配占位证明）。

- [ ] **Step 2: 在根 README 增加最小入口**

在构建边界之后加入“形式化模型”小节，只链接 `docs/FORMAL_FLOW_MODEL.md` 和 `formal/README.md`，并明确其不是 C refinement proof。

- [ ] **Step 3: 跑完整 Lean 验证并记录版本**

Run:

```powershell
lean --version
lake --dir formal build
lake --dir formal env lean formal/TurboFlow/ModelProofs.lean
rg.exe -n "\bsorry\b|\badmit\b|axiom" formal -g "*.lean"
```

Expected: Lean `4.33.1`；两次构建/检查成功；placeholder scan 无匹配。

- [ ] **Step 4: 复核模型映射和残余风险**

逐项核对 `flow_route_edge_active()`、`flow_release_downstream()`、`flow_apply_completion()`、
`nextFlowState` 和 `nextTaskState`。若 C 行号移动，只更新映射；若语义不同，先修改规范，再修改模型和 proof，不以注释掩盖差异。

- [ ] **Step 5: 提交本任务**

```powershell
git add README.md formal/README.md docs/FORMAL_FLOW_MODEL.md
git commit -m "docs: map Lean proofs to TurboFlow runtime"
```

## Self-Review

- Spec coverage：route、fan-in、Flow lifecycle、task lifecycle、证明边界、C 映射和复验命令均有对应任务。
- Placeholder scan：计划不包含实现性 TODO/TBD/FIXME；“后续”仅描述明确排除的 refinement 扩展，不是当前交付占位。
- Type consistency：`FanInGate.resolve : FanInGate → Bool → Option FanInGate`、`Valid` 与 theorem 参数顺序在 Task 2 各步骤一致；Task 3 只引用 Task 1/2 产生的公开名称。
