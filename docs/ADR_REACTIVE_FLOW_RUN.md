# Resumable CFlow Reactive Runs

Status: Accepted for issue #4

## Context

TurboFlow currently treats one call to `turbo_flow_publish()` as both admission and execution.
The call owns no durable run object, cannot represent CFlow `WAIT`, and cannot distinguish a full
or closed scheduler from a failure that occurs after admission. `turbo_flow_publish_async()` adds a
bounded ingress queue, but its request record is not a Reactive Streams Subscription and therefore
cannot carry demand, a waker, cancellation, or an independently observable terminal result.

The immutable plan delivered by issue #3 is safe to borrow while a publication is active. Its
transport descriptor, however, lacks CMeta managed-value traits. A CFlow Publisher may retain or
move a `turbo_flow_msg_t`, so COPY, MOVE, and DESTROY must use the message's existing clone, move,
and cleanup rules instead of treating the envelope as trivial bytes.

## Decision

Add an opaque `turbo_flow_run_t` and size/versioned run configuration and result structures. A run
owns exactly one CFlow Subscription, its terminal result, condition variable, cancellation state,
and reference count. It borrows the immutable compiled plan through the owning `turbo_flow_t` only
until the run reaches a terminal state. The flow maintains a bounded registry of live Subscriptions;
the existing asynchronous-ingress capacity is the initial admission bound. Run admission returns
`SALTS_ENOSPC` when that registry is full and `SALTS_ESHUTDOWN` after ingress closes.

The public run boundary accepts a typed `cflow_publisher` and an explicitly borrowed Scheduler. A
null Scheduler selects the flow-owned bounded CFlow worker Scheduler. Explicitly borrowed
Schedulers must remain alive until the run reaches a terminal state or is canceled and closed.
Publisher ownership transfers only after CFlow subscribe succeeds; failed admission leaves it with
the caller. The public CMake target consequently exports its CFlow dependency.

Each run owns a sealed identity CFlow Graph typed with the compiled plan's message descriptor. This
keeps the Graph borrowed by its Subscription valid even when a terminal result handle outlives the
Flow.
Its Subscriber receives borrowed const values, clones each value through the descriptor, and invokes
the existing immutable TurboFlow plan exactly once. It never casts away const and never models a
legacy callback as a fabricated CFlow callable. The descriptor defines managed COPY, MOVE, and
DESTROY traits backed by `turbo_flow_msg_clone()`, `turbo_flow_msg_move()`, and
`turbo_flow_msg_cleanup()`.

`turbo_flow_run_request()` is the only demand-advancing operation. Its result preserves scheduler
admission distinctions: full maps to `SALTS_ENOSPC`, closed to `SALTS_ESHUTDOWN`, allocation failure
to `SALTS_ENOMEM`, invalid input to `SALTS_EINVAL`, cancellation to `SALTS_ECANCELED`, unsupported
capability to `SALTS_ENOTSUP`, and execution failure to `SALTS_EIO` unless the graph already recorded
a more precise first error. Demand is retained after a transient full rejection, as required by
CFlow. A wait timeout reports `SALTS_ETIMEDOUT` without changing the run. A configured deadline is
a state transition: it cancels the Subscription and publishes `SALTS_ETIMEDOUT` exactly once.

Terminal transition uses first-error-wins under the run mutex. Completion, graph failure,
cancellation, deadline, and flow stop compete through the same transition function; only the winner
sets the public result, signals waiters, and releases the active-publication reference. Completion,
graph failure, and explicit cancellation close and remove the run immediately. A deadline callback
cannot synchronously close a Subscription on the same single-worker Scheduler without self-waiting;
it therefore cancels and publishes the terminal result, while caller close or flow stop performs
the blocking close and registry removal outside that Scheduler callback. CFlow Subscription close
is safe from a Subscriber callback and
defers destruction until the active pump exits. Cancellation also cancels the Publisher's active
waitable, so no stale waker remains registered.

The synchronous `turbo_flow_publish()` facade constructs a one-value managed Publisher, opens a run
on an inline Scheduler, requests one value, waits for its terminal result, and closes the handle.
It has no native fallback. Inline execution preserves the caller-thread behavior and the facade
returns the same graph error/status as before.

Shutdown order is fixed: close ingress, snapshot and retain registered runs, cancel each outside the
flow mutex, wait for all active publications, destroy the flow-owned Scheduler, then release the
compiled plan and runtime resources. No callback, CFlow close, or allocator call occurs while the
flow registry mutex is held. A terminal run detaches from the flow, so its result handle can be read
and closed after flow shutdown without accessing freed flow state.

## State and ownership

- `turbo_flow_t` owns the immutable plan, default Scheduler, and bounded live-Subscription registry.
- `turbo_flow_run_t` owns its identity Graph, Subscription, result storage, synchronization, and
  handle lifetime.
- CFlow owns the Publisher after successful subscribe; otherwise the caller retains ownership.
- A caller-provided Scheduler is borrowed through terminal settlement; the default Scheduler is
  owned and drained by the flow.
- Message values delivered by CFlow are borrowed; the bridge owns only its managed clone.
- The registry is the single source of truth for Subscriptions that stop may still need to close.
  Result state belongs solely to the run and changes once.

## Alternatives considered

1. Extend `turbo_flow_publish_async()` with polling fields. This retains queue-job semantics and
   cannot correctly express demand, WAIT/waker ownership, or CFlow admission status.
2. Run CFlow only when a Publisher returns WAIT and use native publish otherwise. This creates two
   execution facts and violates the no-fallback contract.
3. Expose `cflow_subscription` directly. This leaks graph lifetime and TurboFlow error/stop
   semantics and prevents the runtime from enforcing bounded active-run admission.
4. Cast the borrowed CFlow value to mutable and dispatch it directly. This violates the Subscriber
   contract and lets legacy stages mutate storage owned by the Publisher.

## Consequences

- The new API is additive, but consumers of `TurboFlow::Graph` gain a public `Salts::CFlow`
  dependency and include requirement.
- Opening, requesting, waiting, canceling, and reading a run are O(1), except bounded registry
  removal, which is O(R) for at most the configured active-run capacity. Native graph work remains
  O(V+E) per value.
- The bridge performs one managed message clone per delivered value. This is required for const and
  mutation isolation; benchmarks must report its allocation and latency cost against the previous
  synchronous facade.
- On the Windows release benchmark (`100,000` single-value publications through a linear
  three-stage plan), the native batch baseline measured `864 ns/op`, a run on a borrowed inline
  Scheduler measured `2,415 ns/op`, and the synchronous Reactive facade measured `2,469 ns/op`.
  The facade adds about `55 ns` over the public run path; the remaining approximately `2.8x`
  migration cost is run/Subscription/synchronization/Graph setup and is now an explicit future
  optimization target rather than an unmeasured assumption.
- All queues and live run counts remain bounded. Backpressure is observable rather than converted
  into blocking or silent retry.
- Existing DSL, persisted formats, callback ABI, settlement rules, and synchronous return codes stay
  unchanged.

## Migration and rollback

Existing callers continue to use `turbo_flow_publish()` and receive the same visible result. New
Publisher integrations adopt the run API explicitly. The optional `TurboFlow::TurboDbAdapter` now
wraps native ORM Publishers without exposing database ownership inside Graph core. Rollback removes
the additive run API and restores the direct synchronous call path; no DSL, persisted data, or
deployment configuration requires migration. Because no fallback is retained, rollback is a
source-level change rather than a runtime switch.

## Verification

- Deterministic Publishers cover VALUE, VALUE_AND_DONE, WAIT plus valid wake, DONE, and ERROR.
- Existing ownership and synchronous publication regression tests cover clone/move/destruction,
  downstream settlement, and stop drain behavior; the run tests additionally prove Publisher
  destruction and WAIT cancellation.
- Admission tests distinguish full, closed, invalid, canceled, unsupported, and runtime errors
  while verifying demand retention after full rejection.
- Deadline and stop tests compete with later cancellation and verify that the first terminal result
  remains unchanged. A detached completed handle is also read and closed after Flow destruction.
- Differential tests compare synchronous graph output, mutation, settlement, and error status with
  the pre-run contract.
- The publish benchmark reports native single-message batch, public Reactive run, and synchronous
  facade latency. The full CTest suite remains the regression gate.
