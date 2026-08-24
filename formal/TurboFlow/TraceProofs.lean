import TurboFlow.Trace

namespace TurboFlow

private def incrementUntilTwo (state : Nat) (_ : Unit) : Option Nat :=
  if state < 2 then some (state + 1) else none

example : runTrace incrementUntilTwo 0 [(), ()] = some 2 := by decide
example : runTrace incrementUntilTwo 0 [(), (), ()] = none := by decide

end TurboFlow
