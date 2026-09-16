# Inbox-to-Graph non-blocking source run plan

> **Issue:** #122
> **Parents:** #117, #118
> **Compatibility:** exact ABI only; no direct-publish, synchronous-driver or legacy-layout fallback

## Goal

Add one provider-neutral owner that claims an already-admitted Inbox v2 record,
opens a demand-driven CFlow run for the configured TurboFlow source, and settles
that exact claim only after an explicit non-blocking poll observes a terminal
Graph result. Source admission remains the commit point and never executes the
Graph. The driver never waits for terminal state; an explicitly supplied inline
Scheduler may nevertheless execute accepted Graph work inside `request()`.

## Architecture decision

The first delivery is an opaque, serialized single-record run. It reuses
`turbo_flow_run_open()` / `turbo_flow_run_request()` /
`turbo_flow_run_snapshot()` rather than wrapping `turbo_flow_publish()` in a
nominally asynchronous API. A caller-owned scheduler makes request admission
non-blocking and leaves scheduling policy above the driver.

The driver borrows one Inbox, one started Flow and one scheduler. It owns a copy
of the graph source name. While a record is active it owns the move-only Inbox
claim, the message, Publisher/run handle and the eventual immutable Graph
outcome. It is intentionally not thread-safe: one serialized owner calls it.
The Inbox provider remains the sole fact source for durable record phase and
claim validity.

Alternatives rejected for this slice:

- Calling `turbo_flow_publish()` from request blocks on the Graph and makes
  `GRAPH_ACTIVE` unobservable.
- Settling from a Graph callback couples Inbox I/O to CFlow execution and makes
  settlement retries capable of replaying Sink side effects.
- Letting each HTTP/WS/socket/MQTT DLL own the claim/run state duplicates the
  same ambiguity handling across transports.

## Exact public contract

Create `turbo_flow_inbox_source.h` with API version 1 and no short-layout
acceptance:

- `turbo_flow_inbox_source_config_t { size, version, inbox, flow,
  graph_source_name, scheduler, max_message_bytes }`; all dependencies are
  borrowed through successful destroy, while the source name is copied at
  create. The non-zero message bound covers the complete owned layout.
- Opaque `turbo_flow_inbox_source_t` owns at most one record run.
- `turbo_flow_inbox_source_result_t { size, version, state, record_id,
  graph_status, settlement_status }` reports `GRAPH_ACTIVE`, `COMPLETED`,
  `FAILED`, `SETTLEMENT_PENDING`, `SETTLEMENT_UNKNOWN`, or
  `OWNER_LOST_UNKNOWN`.
- `turbo_flow_inbox_source_create()` validates the exact config and creates an
  idle owner; it does not claim a record.
- `turbo_flow_inbox_source_request()` claims one record, builds its owned
  message, opens the CFlow run and requests one value. Success means only that
  the run was requested; the normal success path never waits, polls or settles.
  Scheduler semantics remain explicit: an inline Scheduler may execute Graph
  work before request returns, while external-poll owners use a deferred or the
  Flow-owned worker Scheduler.
  A request-admission failure synchronously cancels the run and settles the
  preparation failure if its terminal outcome is already known.
- `turbo_flow_inbox_source_poll()` snapshots the run without waiting. ACTIVE is
  returned as `SALTS_OK`. On the first observed terminal result it attempts the
  matching Inbox settlement exactly once and retains the terminal run until
  settlement is confirmed. When settlement succeeds, poll returns the exact
  Graph status and releases the run.
- `turbo_flow_inbox_source_cancel()` cancels only GRAPH_ACTIVE, then records
  `SALTS_ECANCELED` through fail without executing queued business/Sink work.
  It cannot bypass a pending or unknown settlement.
- `turbo_flow_inbox_source_retry_settlement()` is the only operation permitted
  to repeat a transiently failed complete/fail transition.
- `turbo_flow_inbox_source_reconcile_settlement()` is the only operation
  permitted to recheck an ambiguous `SALTS_EALREADY` outcome.
- `turbo_flow_inbox_source_destroy()` succeeds only while idle; it never closes
  or destroys the borrowed Inbox, Flow or scheduler.

Empty Inbox returns `SALTS_ENOENT` from request. Request, poll, retry and
reconcile reject invalid exact-version values with `SALTS_EINVAL`. A request
while Graph or settlement state is owned returns `SALTS_EBUSY`. Poll while idle
returns `SALTS_ENOENT`.

## Message-owned metadata layout

The Graph message owns one checked byte layout inside its `mem_buffer_t`:

```text
[turbo_flow_inbox_source_context_t POD]
[source_id bytes][admission_id bytes][correlation bytes][payload bytes]
```

`turbo_flow_inbox_source_context_t` contains only POD integer scalars and
`{ offset, length }` POD spans: record ID, source sequence, source ID span,
admission ID span and correlation span. It contains no `vstr`, native pointer,
DLL address, allocator handle or provider-owned view. Every offset/length is
overflow-checked against both `max_message_bytes` and the owning message buffer
before publication, and is validated again by accessors.

The public accessors expose only immutable views backed by the owned message
buffer:

- `turbo_flow_inbox_source_context(message)` returns the validated POD header.
- `turbo_flow_inbox_source_source_id(message)`,
  `turbo_flow_inbox_source_admission_id(message)` and
  `turbo_flow_inbox_source_correlation(message)` return zero-copy `vstr` views
  resolved from the validated spans.

The returned views are immutable and valid only while the message buffer is
alive; callers must not retain them. The context itself contains no `vstr` or
pointer. This layout remains valid across Graph clone/move/async boundaries
because offsets are resolved against the retained message buffer.

## State and settlement rules

```text
IDLE --request/claim/open/request(1)--> GRAPH_ACTIVE
GRAPH_ACTIVE --poll sees OPEN/ACTIVE--> GRAPH_ACTIVE
GRAPH_ACTIVE --poll sees COMPLETED----> settle complete once
GRAPH_ACTIVE --poll sees FAILED/CANCELED--> settle fail once
GRAPH_ACTIVE --explicit cancel-----------> settle fail(SALTS_ECANCELED) once

settlement OK       --> COMPLETED or FAILED result, then IDLE
settlement ECANCELED--> OWNER_LOST_UNKNOWN
settlement transient error --> SETTLEMENT_PENDING
settlement EALREADY -------> SETTLEMENT_UNKNOWN

SETTLEMENT_PENDING --poll/request/destroy--> EBUSY, no provider/Graph call
SETTLEMENT_UNKNOWN --poll/request/destroy--> EBUSY, no provider/Graph call
SETTLEMENT_PENDING --retry_settlement-----> repeat settlement only
SETTLEMENT_UNKNOWN --reconcile_settlement-> reconcile settlement only
```

- Every non-success Graph status is persisted through `fail(status)` and is
  never retried implicitly. A successful failure settlement reports `FAILED`.
- A transient settlement error retains the exact claim and Graph result in
  `SETTLEMENT_PENDING`.
- `SALTS_EALREADY` is not successful settlement: the durable outcome is
  ambiguous, so the owner enters `SETTLEMENT_UNKNOWN`. Poll cannot retry or
  clear it. Reconciliation may advance only from this state; repeated EALREADY
  remains unknown.
- `SALTS_ECANCELED` means a durable owner takeover invalidated the lease. The
  result is `OWNER_LOST_UNKNOWN`, never COMPLETED/FAILED, and Graph is not run
  again. Durable retry/discard policy remains an explicit Inbox recovery action.
- Destroy returns `SALTS_EBUSY` for GRAPH_ACTIVE, SETTLEMENT_PENDING and
  SETTLEMENT_UNKNOWN. `OWNER_LOST_UNKNOWN` has already invalidated the local
  claim and releases the driver slot, so a later destroy succeeds. Destroy
  cannot silently cancel, discard, forget or reinterpret a claim.

## RED acceptance tests

`test_flow_inbox_source.c` uses the real bounded memory Inbox behind a faulting
provider fixture and a manual CFlow scheduler. It does not impersonate a real
network transport. The required assertions are:

1. A claimed record exceeding `max_message_bytes` is failed with ENOSPC before
   Graph open, leaves no CLAIMED lease and permits source destruction.
2. Admission alone does not run business or Sink stages. Request only starts;
   poll reports GRAPH_ACTIVE; scheduler execution still does not settle; the
   next poll observes terminal success and completes exactly once.
3. Graph failure skips Sink, is failed exactly once, appears in the Inbox failed
   index and is not rerun or resettled by another poll.
4. Transient completion failure enters SETTLEMENT_PENDING. Poll, request and
   destroy cannot advance it; explicit retry completes it without Graph replay.
5. EALREADY enters SETTLEMENT_UNKNOWN with the same non-replay/non-destroy
   rules; only explicit reconcile may advance it.
6. Both business and Sink stages recover the same message-owned POD metadata
   only through checked zero-copy getters.

Additional implementation acceptance covers exact ABI/argument failures,
`OWNER_LOST_UNKNOWN`, repeated EALREADY, `max_message_bytes` and active
cancellation. Reconciliation after process recovery remains part of the
multi-worker/crash-recovery follow-up under #118.

## Tasks

1. Land the TinyTest contract and opaque run owner with checked message layout.
2. Add the public header/source to the Graph package and installed C/C++
   consumer with exact ABI tests.
3. Add TurboDB provider integration using the same run contract; do not label
   fixture source names as real network coverage.
4. Remove the protocol graph short-layout/direct-publication path in its
   separately reviewed incompatible boundary change.
5. Update #117/#118/#122 and run focused Release, Debug/ASan, installed-consumer
   and adjacent Graph/Inbox tests.

## Migration, rollback and remaining scope

This introduces no compatibility layer and changes no stored data. New
deployments wire Source DLLs to Inbox admission and this run owner to the common
graph source. Rollback is deployment-level replacement of the complete verified
binary/config set; runtime does not select an older synchronous or direct path.

HTTP/WS/socket/MQTT/FlowMQ/FlowIE wire acknowledgements, real Sink delivery,
cancellation/drain across plugin generations, crash recovery reconciliation and
multi-worker demand remain #117/#118 follow-up acceptance. Fixture names in
this test are identities only, not protocol conformance evidence.

## Verification

```powershell
cmake --build --preset win-release-user --target test_flow_inbox_source test_flow_inbox
ctest --preset win-release-user -R "^(test_flow_inbox_source|test_flow_inbox)$" --output-on-failure
cmake --build --preset win-dev-user --target test_flow_inbox_source test_flow_inbox
ctest --preset win-dev-user -R "^(test_flow_inbox_source|test_flow_inbox)$" --output-on-failure
cmake --build --preset install-win-dev-user
```

Run the full Release suite only after focused tests are green. Completion claims
require fresh output plus `clang-format --dry-run --Werror` and
`git diff --check`.
