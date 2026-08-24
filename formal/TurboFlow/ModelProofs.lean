import TurboFlow.Model
import TurboFlow.FanIn

namespace TurboFlow

theorem reject_inactive_on_success (decision : RouteDecision) :
    edgeActive .ok decision .reject = false := by
  cases decision <;> rfl

theorem only_reject_active_on_failure (decision : RouteDecision) (edge : EdgeKind)
    (h : edgeActive .failed decision edge = true) : edge = .reject := by
  cases decision <;> cases edge <;> simp [edgeActive] at h ⊢

theorem named_route_prioritizes_target_match (matchesCurrentTarget : Bool) (edge : EdgeKind)
    (h : edge ≠ .reject) :
    edgeActive .ok (.namedRoute matchesCurrentTarget) edge = matchesCurrentTarget := by
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

theorem accepted_complete_reaches_completed :
    nextTaskState .accepted .complete = some .completed := by
  rfl

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

example (gate : FanInGate) (selected : Bool) (next : FanInGate)
    (valid : gate.Valid) (resolved : gate.resolve selected = some next) :
    next.Valid := by
  exact resolve_preserves_valid gate selected next valid resolved

end TurboFlow
