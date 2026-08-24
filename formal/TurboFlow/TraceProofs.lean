import TurboFlow.ModelProofs
import TurboFlow.Trace

namespace TurboFlow

private def incrementUntilTwo (state : Nat) (_ : Unit) : Option Nat :=
  if state < 2 then some (state + 1) else none

example : runTrace incrementUntilTwo 0 [(), ()] = some 2 := by decide
example : runTrace incrementUntilTwo 0 [(), (), ()] = none := by decide

example : runFlowTrace .new [.parse, .compile, .start, .stop] = some .stopped := by decide
example : runTaskTrace .new [.accept, .run, .complete] = some .completed := by decide
example : runGateTrace (FanInGate.initial 2) [true, false] =
    some { potential := 2, processed := 2, activated := 1, outcome := .ready } := by decide
example : runGateTrace (FanInGate.initial 2) [true, false, true] = none := by decide

theorem routeMask_length (observation : RouteObservation) :
    (routeMask observation).length = observation.edges.length := by
  simp [routeMask]

theorem flow_trace_preserves_allowed_path (state final : FlowState)
    (events : List FlowEvent) (completed : runFlowTrace state events = some final) :
    TransitionPath AllowedFlowTransition state events final := by
  exact runTrace_builds_path nextFlowState AllowedFlowTransition
    (fun state event next => flow_transition_preserves_allowed state next event) completed

theorem task_trace_preserves_allowed_path (state final : TaskState)
    (events : List TaskEvent) (completed : runTaskTrace state events = some final) :
    TransitionPath AllowedTaskTransition state events final := by
  exact runTrace_builds_path nextTaskState AllowedTaskTransition
    (fun state event next => task_transition_preserves_allowed state next event) completed

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
  change runTrace FanInGate.resolve gate events = some final at completed
  rw [runGateTrace, runTrace_append, completed]
  simp [runTrace, terminal_gate_cannot_resolve final selected valid terminal]

end TurboFlow
