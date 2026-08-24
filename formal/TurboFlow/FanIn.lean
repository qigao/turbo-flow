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

open FanInGate

theorem initial_valid (potential : Nat) (positive : 0 < potential) :
    (initial potential).Valid := by
  simp [initial, Valid, positive]

theorem resolve_preserves_valid (gate : FanInGate) (selected : Bool) (next : FanInGate)
    (valid : gate.Valid) (resolved : gate.resolve selected = some next) :
    next.Valid := by
  cases selected
  · by_cases remaining : gate.processed < gate.potential
    · simp [resolve, remaining] at resolved
      subst next
      by_cases completed : gate.processed + 1 = gate.potential
      · by_cases active : 0 < gate.activated
        · simp_all [Valid] <;> omega
        · simp_all [Valid] <;> omega
      · simp_all [Valid] <;> omega
    · simp [resolve, remaining] at resolved
  · by_cases remaining : gate.processed < gate.potential
    · simp [resolve, remaining] at resolved
      subst next
      by_cases completed : gate.processed + 1 = gate.potential
      · simp_all [Valid] <;> omega
      · simp_all [Valid] <;> omega
    · simp [resolve, remaining] at resolved

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

end TurboFlow
