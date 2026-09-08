# Managed Source Run Binding

Issue #58 adds the core-only contract that lets a pre-registered managed Source create its one
Reactive run during `turbo_flow_start()`. It does not migrate a concrete CNet, CHTTP, or TurboDB
owner and does not open normal runtime admission before start commits.

## Chosen boundary

One size/versioned `turbo_flow_managed_source_registration_t` atomically registers an adapter and
one explicit managed Source resource for the same borrowed owner context. Core stages the adapter
with `shutdown == NULL`, validates and copies the managed resource into the canonical resource
registry, verifies the copied descriptor has the Source role, and only then installs the real
shutdown callback. Failure rolls both vectors back and leaves context ownership with the caller.
There is no inferred capability, second registry, unregister path, or ordinary-adapter fallback.

During a compiled Source adapter's synchronous `start` callback, the callback may call
`turbo_flow_managed_source_run_open(flow, stage, publisher, config, run_out)` exactly once. A
thread-local callback token and the Flow's current adapter/stage indices prove that the caller is
inside the matching managed Source start. The supplied stage must be the exact callback view, name
the compiled Source, and resolve to the registered adapter. Calls from another thread, outside the
callback, for a non-Source/mismatched stage, or after a successful open fail fast without changing
run or publish counts. Ordinary `turbo_flow_run_open()` keeps its existing STARTED/OPEN requirement.

The Source owner retains the Publisher backing context until its `stop` callback returns; the run
owns its CFlow Subscription. Core records the opened run in the active-adapter entry and owns that
run handle after the start callback succeeds. If open setup or a later adapter start fails, core
cancels/closes the partial Source run before rolling adapters back. Registry teardown invokes the
owner shutdown callback exactly once only after no active run can retain the context.

## Start, stop, and facts

The Reactive Scheduler and bounded `active_runs` storage are initialized before adapters start.
The scoped open admits one run against `async_ingress_config.queue_capacity` without changing the
global Flow state or `admission_state`; successful admission increments `active_publishes` exactly
once and terminal/cancel releases it exactly once. Capacity N rejects run N+1 with `SALTS_ENOSPC`.

Stop first closes global admission and cancels ordinary non-draining runs. It stops downstream
adapters, then cancels and closes each managed Source run in reverse adapter order so no new Source
value can be accepted. Async ingress and already accepted graph work drain before the Source
adapter's `stop` callback, and the scheduler is destroyed only after all Source runs are released.
This ordering is necessary because each live run itself owns one `active_publishes` admission; waiting
for that counter before canceling an unbounded Source would deadlock. Start rollback uses the same
run-before-owner-stop rule and leaves `active_runs` and `active_publishes` empty.

## Alternatives rejected

- Setting Flow state to STARTED during adapter start would globally admit publications and make a
  failed start externally observable.
- Letting Source owners call ordinary `turbo_flow_run_open()` after start creates a registration
  lifetime gap and permits a freed owner context to remain in the managed registry.
- A separate Source registry duplicates adapter/resource identity and breaks atomic rollback.
- Inferring Source management from adapter role flags cannot prove owner lifetime or capabilities.

The public additions are source-compatible but expose a new explicit registration choice. Tests
must cover C and C++ headers, all invalid-input rollback points, duplicate identity, exact callback
scope, bounded run capacity, Publisher terminal/error/cancel ownership, start rollback, stop order,
and unchanged ordinary run-open behavior.
