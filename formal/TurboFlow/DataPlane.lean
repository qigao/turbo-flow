import Std.Tactic

namespace TurboFlow

inductive PayloadBacking where
  | owned
  | buffer
  deriving DecidableEq, Repr

structure PayloadView where
  backing : Option PayloadBacking
  backingBytes : Nat
  offset : Nat
  length : Nat
  deriving DecidableEq, Repr

namespace PayloadView

def Valid (view : PayloadView) : Prop :=
  match view.backing with
  | none => view.length = 0
  | some _ => view.offset ≤ view.backingBytes ∧
      view.length ≤ view.backingBytes - view.offset

theorem valid_has_backing_or_is_empty (view : PayloadView) (valid : view.Valid) :
    view.length = 0 ∨
      ∃ backing, view.backing = some backing ∧
        view.offset ≤ view.backingBytes ∧
        view.length ≤ view.backingBytes - view.offset := by
  cases backingEq : view.backing with
  | none =>
      left
      simpa [Valid, backingEq] using valid
  | some backing =>
      right
      have bounds := valid
      simp only [Valid, backingEq] at bounds
      exact ⟨backing, rfl, bounds⟩

end PayloadView

structure IngressBudget where
  maxMessageBytes : Nat
  maxInflightBytes : Nat
  inflightBytes : Nat
  deriving DecidableEq, Repr

namespace IngressBudget

def Valid (budget : IngressBudget) : Prop :=
  budget.inflightBytes ≤ budget.maxInflightBytes

def reserve (budget : IngressBudget) (bytes : Nat) : Option IngressBudget :=
  if bytes ≤ budget.maxMessageBytes ∧
      budget.inflightBytes + bytes ≤ budget.maxInflightBytes then
    some { budget with inflightBytes := budget.inflightBytes + bytes }
  else
    none

def release (budget : IngressBudget) (bytes : Nat) : Option IngressBudget :=
  if bytes ≤ budget.inflightBytes then
    some { budget with inflightBytes := budget.inflightBytes - bytes }
  else
    none

theorem reserve_preserves_valid (budget next : IngressBudget) (bytes : Nat)
    (reserved : budget.reserve bytes = some next) : next.Valid := by
  unfold reserve at reserved
  split at reserved
  · simp at reserved
    subst next
    simp_all [Valid]
  · simp at reserved

theorem reserve_enforces_message_limit (budget next : IngressBudget) (bytes : Nat)
    (reserved : budget.reserve bytes = some next) : bytes ≤ budget.maxMessageBytes := by
  unfold reserve at reserved
  split at reserved <;> simp_all

theorem reserve_then_release (budget next : IngressBudget) (bytes : Nat)
    (reserved : budget.reserve bytes = some next) : next.release bytes = some budget := by
  unfold reserve at reserved
  split at reserved <;> simp_all
  subst next
  simp [release]

end IngressBudget

inductive DataAction where
  | route (key : Nat)
  | batchKey (key : Nat)
  | retryClass (key : Nat)
  deriving DecidableEq, Repr

structure DataDecision where
  route : Option Nat := none
  batchKey : Option Nat := none
  retryClass : Option Nat := none
  deriving DecidableEq, Repr

namespace DataDecision

def applyAction (decision : DataDecision) : DataAction → Option DataDecision
  | .route key =>
      if decision.route.isNone then some { decision with route := some key } else none
  | .batchKey key =>
      if decision.batchKey.isNone then some { decision with batchKey := some key } else none
  | .retryClass key =>
      if decision.retryClass.isNone then some { decision with retryClass := some key } else none

def applyActions : DataDecision → List DataAction → Option DataDecision
  | decision, [] => some decision
  | decision, action :: rest =>
      (decision.applyAction action).bind fun next => next.applyActions rest

def applyAtomically (message : Nat) (actions : List DataAction) : Option (Nat × DataDecision) :=
  (applyActions {} actions).map fun decision => (message, decision)

theorem duplicate_route_rejected (first second : Nat) :
    applyActions {} [.route first, .route second] = none := by
  simp [applyActions, applyAction]

theorem duplicate_batch_key_rejected (first second : Nat) :
    applyActions {} [.batchKey first, .batchKey second] = none := by
  simp [applyActions, applyAction]

theorem duplicate_retry_class_rejected (first second : Nat) :
    applyActions {} [.retryClass first, .retryClass second] = none := by
  simp [applyActions, applyAction]

theorem failed_apply_has_no_commit (message : Nat) (actions : List DataAction)
    (failed : applyActions {} actions = none) :
    applyAtomically message actions = none := by
  simp [applyAtomically, failed]

end DataDecision

inductive SettlementState where
  | pending
  | settled (status : Int)
  deriving DecidableEq, Repr

def settle : SettlementState → Int → Option SettlementState
  | .pending, status => some (.settled status)
  | .settled _, _ => none

theorem pending_settles_once (status : Int) :
    settle .pending status = some (.settled status) := rfl

theorem settled_cannot_settle_again (first second : Int) :
    settle (.settled first) second = none := rfl

end TurboFlow
