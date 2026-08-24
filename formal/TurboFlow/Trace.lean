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

theorem runTrace_append {State : Type u} {Event : Type v}
    (step : State → Event → Option State) (state : State)
    (first second : List Event) :
    runTrace step state (first ++ second) =
      (runTrace step state first).bind (fun middle => runTrace step middle second) := by
  induction first generalizing state with
  | nil => rfl
  | cons event events ih =>
      simp only [List.cons_append, runTrace]
      cases step state event <;> simp [ih]

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

end TurboFlow
