namespace TurboFlow

abbrev Adjacency := Nat → List Nat

def expandReach (adjacent : Adjacency) (reached : List Nat) : List Nat :=
  reached ++ reached.flatMap adjacent

def recursiveReach (adjacent : Adjacency) (source : Nat) : Nat → List Nat
  | 0 => [source]
  | fuel + 1 => expandReach adjacent (recursiveReach adjacent source fuel)

def iterativeReach (adjacent : Adjacency) (source fuel : Nat) : List Nat :=
  (List.range fuel).foldl (fun reached _ => expandReach adjacent reached) [source]

theorem iterative_reachability_equivalent (adjacent : Adjacency) (source fuel : Nat) :
    iterativeReach adjacent source fuel = recursiveReach adjacent source fuel := by
  induction fuel with
  | zero => rfl
  | succ fuel inductionHypothesis =>
      calc
        iterativeReach adjacent source (fuel + 1) =
            expandReach adjacent (iterativeReach adjacent source fuel) := by
              simp [iterativeReach, List.range_succ, List.foldl_append]
        _ = expandReach adjacent (recursiveReach adjacent source fuel) := by
              rw [inductionHypothesis]
        _ = recursiveReach adjacent source (fuel + 1) := rfl

end TurboFlow
