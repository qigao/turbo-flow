import TurboFlow.Model

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
