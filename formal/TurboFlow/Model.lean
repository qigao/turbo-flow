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
