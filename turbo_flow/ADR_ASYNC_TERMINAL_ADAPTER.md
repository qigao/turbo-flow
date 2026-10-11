# ADR: Async Terminal Adapter Stage Contract

## Context

The existing `turbo_flow_adapter_ops_t::consume` contract is synchronous. Its
message envelope, settlement scope, stage observer event, sink observer event,
and publication result all end before `consume` returns. Network send APIs such
as `cnet_send_buffer()` instead return admission after retaining a buffer and report the
authoritative result later. Treating admission as completion would acknowledge
work that can still fail and would release settlement state too early.

Issue #20 requires one reusable contract for CNet and later CHTTP sinks. It
affects the public Graph API, runtime dispatch, publication completion,
settlement, observer timing, adapter lifecycle, and the optional CNet module.

## Decision

Add a separate size/versioned async-terminal registration. Do not append fields
to `turbo_flow_adapter_ops_t` and do not reinterpret synchronous `consume`.

An async terminal callback receives a borrowed message plus a core-created,
move-only terminal claim. A successful callback must move the claim to its
explicit owner. Rejection preserves core ownership and returns the concrete
error synchronously. The claim retains an independent message view and the
stage/settlement context until it is consumed by exactly one terminal call.

Only asynchronous publications and Reactive runs may enter an async terminal
stage. Synchronous `turbo_flow_publish*` and batch publication fail with
`SALTS_ENOTSUP`; the runtime never waits for a send in the publish call.

The runtime tracks all claims belonging to a publication. Stage-end,
sink-complete, settlement application, flow-complete, async publication
completion, and Reactive value accounting occur only after authoritative
terminal completion. The first concrete failure is preserved. A claim also
holds Flow shutdown accounting, so `turbo_flow_stop()` cannot clear adapters
while an accepted terminal is live.

Async-terminal adapters are compile-time restricted to inline, terminal,
non-source stages without retry, reorder, or stage deadline. These features need
continuation-aware semantics and are rejected instead of silently changing
behavior. Existing settlement owner operations remain the single settlement
fact source.

CNet sinks use a bounded `cflow_io_actor`. Actor admission transfers the claim;
capacity rejection preserves it. Backend send admission is not terminal. The
Actor is completed only by `on_send` or the corresponding connection/session
terminal. Cancellation is a request and does not settle a submitted native
operation. The caller owns progress and drives both CNet and the Actor; no
background thread is created.

The raw datagram sink uses `cnet_datagram` tagged `on_send` callbacks. The
packet sink uses `cnet_packet_endpoint_init_ex` and `cnet_packet_send_tagged`
from the installed Salts SDK for UDP, KCP, secure KCP and FEC. UDP completes
at its native send terminal; KCP success requires peer acknowledgement of
every segment in the logical message. Admission alone is never success.

## Ownership and state

- Graph owns registration metadata and publication trackers.
- A successful async callback transfers one terminal claim to the adapter.
- The IO Actor owns each accepted CNet operation and its claim until delivery is
  acknowledged.
- CNet owns copied send bytes after successful native admission.
- Existing settlement-owner callbacks remain the only owner of durable claim
  state.
- One caller thread owns each sink, CNet poll loop, Actor driver, and Executor
  drain. Cross-thread completion entry remains limited to the thread-safe Actor
  completion API.

The state order is `registered -> started -> accepting -> closing -> drained ->
destroyed`. Capacity is fixed at initialization. No state is independently
advanced by a cache or compatibility path.

## Failure semantics

- Invalid contract or topology: compile-time `SALTS_EINVAL`/`SALTS_ENOTSUP`.
- Full Actor request/command capacity: immediate `SALTS_ENOSPC`.
- Stale CNet handle: preserved `SALTS_ENOENT`.
- Native/session failure: preserved terminal Salts status.
- Close before backend submission: terminal `SALTS_ECANCELED`.
- Close after backend submission: wait for `on_send` or connection terminal.
- Duplicate use of the same moved/consumed claim: `SALTS_EALREADY`.

Adapter stop callbacks settle their Flow claims before reporting an error with
`turbo_flow_adapter_report_stop_status()`. The Flow remains FAILED and retains
its plan, registry, and failed adapter bindings until an explicit
`turbo_flow_stop()` retry succeeds. Successfully stopped bindings are skipped
on retries; one host serializes lifecycle operations.

The core records a binding in its active table before invoking `start()`. A
failed start therefore participates in stop rollback, including when the
failure left a partially initialized native owner. An allocation failure in
this table occurs before the adapter can create resources. Stop must tolerate
partial initialization and already-released owners; an initialization error
without remaining resources is not a cleanup error. A failed rollback retains
the exact binding for explicit stop retry and prevents reset/destroy.

This replaces the previous success-only registration, which lost cleanup
ownership when start or the subsequent active-table allocation failed. Keeping
rollback exclusively inside start was rejected because bounded native cleanup
can fail and must remain retryable through the Flow lifecycle. Adding another
public callback or adapter-specific registry was unnecessary: the existing
stop callback and active table provide that ownership. CNet stream/packet/UDP
and CHTTP server/WebSocket starts now leave partial owners for the core's stop
rollback instead of invoking a second, unchecked cleanup path.

For the raw UDP sink, lifecycle mutation and Actor destruction are serialized
with progress entry, submissions and snapshots by the adapter mutex/condition.
The progress owner maintains a list of operations successfully admitted to
CNet, bounded by `send_capacity`, to reach their Flow claims on drain failure.
CNet owns copied payload slots; the Actor owns operation release. Failing stop
settles each pending Flow claim with `SALTS_ESHUTDOWN` but retains both owners
until native terminal delivery and Actor acknowledgement. Queued operations
are cancelled through Actor close. Native stop is attempted once with the
configured timeout; local Actor work drains only while it makes progress.
Destroy errors preserve initialized flags unless the SDK actually released its
implementation. Snapshot `active_requests` counts retained Actor tokens and
may remain nonzero after the Flow claims are complete. Subsequent terminals
only retire these tokens and do not increment successful-send counters.

The pending list is adapter bookkeeping for Actor-owned operations, with no
additional payload allocation or independent native completion authority.
Settlement and removal are O(`send_capacity`); this is a lifecycle correctness
change, with no claimed performance benefit. The public layouts and capacity
configuration remain unchanged.

The packet sink follows the same one-attempt stop protocol. Its existing
fixed operation array owns generation-tag correlation, bounded by
`send_capacity`; failed stop scans that array to settle outstanding Flow claims
with `SALTS_ESHUTDOWN`. Tagged send admission copies UDP and KCP payloads, so
these claims may release their message storage while native payloads and Actor
tokens remain retained. Close drains queued Actor work while local progress
exists; native timeout or busy does not prove quiescence. Late terminals retire
the retained slots without repeating completion or successful-send accounting.
The host explicitly retries stop after an error, keeping the Flow and sink alive.

Packet source open has a separate rollback boundary: endpoint admission may
succeed before Graph run admission fails. A fixed cleanup timeout cannot prove
native drain, so open must not free its callback context after failed stop or
destroy. Rollback now uses the checked source stop/destroy path. Successful
rollback returns the original admission error and a NULL output. Failed
rollback returns its cleanup error and transfers the still-live source handle
to the caller; the snapshot retains the initial open status and stage. The
caller retains the source and Flow, retries stop explicitly, then destroys the
source. Only snapshot/stop/destroy may be used on a failed-open handle.

Returning NULL after failed cleanup would lose the only owner, while an
unbounded internal retry could hang open. An implicit global retirement queue
would introduce a new progress owner. Returning the existing opaque handle
uses the existing bounded storage and serialized-thread lifecycle without new
public types or hidden background work.

`turbo_flow_destroy()` attempts stop once for a running Flow. If that stop
fails, it returns without releasing the Flow or invoking registry shutdown;
the stop error remains available in `turbo_flow_last_error()`. If a previous
stop or start rollback already requires a retry, destroy retains the Flow and
records `SALTS_EBUSY` without invoking adapter callbacks. The host must keep
the Flow pointer and adapter contexts, explicitly stop successfully, then
destroy. Plan clearing rejects active adapter bindings instead of implicitly
retrying their stop and discarding an error. The void destroy signature and
successful shutdown behavior are preserved.

## Alternatives rejected

1. Extend `turbo_flow_adapter_ops_t` in place. Existing callers zero-initialize
   it without a size field, so this is not safely versionable.
2. Return success from synchronous `consume` at send admission. This reports a
   false terminal result and can acknowledge failed work.
3. Block `consume` until `on_send`. This stalls Graph workers, can deadlock the
   caller-owned progress loop, and violates issue #20.
4. Add a private CNet-only completion mechanism. CHTTP needs the same lifecycle,
   and duplicate state machines would diverge.
5. Treat KCP packet admission as completion. The current SDK provides tagged
   logical-message terminals, including peer acknowledgement for KCP; an
   admission result cannot substitute for that terminal.

## Compatibility and migration

Successful synchronous execution, DSL, publication calls, and observer ordering
do not change. Failed starts now produce a stop event for the failing binding;
existing adapters must make stop safe after partial or failed initialization.
Function signatures and public struct layouts are unchanged. New adapters opt
in through the new registration function. Hosts
that use an async terminal stage must publish through `turbo_flow_publish_async`
or a Reactive run and must drive the adapter's explicit poll API.

Before deploying the failed-start protocol, audit external adapters so their
stop callbacks tolerate partial initialization. Adapters that may retain
resources after failed start must provide stop. Deploy the core and native
adapter start/stop changes together; an old core cannot retry a failed start
whose adapter now delegates cleanup to core rollback.

Packet-source callers must inspect the output pointer even on open failure.
Deploy those caller changes with the source rollback change. Successful open
and ordinary failure remain unchanged; only failed cleanup transfers a handle
alongside an error. In-tree calls are formal tests and an installed-header
function-pointer probe; the failure tests now retain and explicitly clean up
returned owners. External callers need the same audit.

CHTTP can reuse the claim contract by moving one claim into each deferred client
request or server response operation and completing it from the protocol's
authoritative terminal callback.

## Verification

Core tests cover move ownership, synchronous rejection, delayed observer and
settlement timing, concrete failure, duplicate terminal, fan-out aggregation,
and message lifetime. Lifecycle regressions also cover a stop error during
destroy, repeated explicit stop failures, and failed start rollback; registry
shutdown occurs only after a successful stop retry. CNet integration tests cover stream logical-send
completion and capacity-one backpressure plus raw datagram tag correlation,
capacity backpressure, cancellation, and stop/drain. The adapters preserve
CNet's stale-handle and connection-terminal statuses and independently reject
zero, partial, or over-length successful completion as `SALTS_EPROTO`. C and
C++ header probes plus the installed consumer verify the additive API and
`Salts::CFlow` dependency propagation.

Failed-start regressions cover cleanup failure in the failing binding itself,
a real CNet client retained after connect admission fails, CHTTP initialization
with no remaining native owner, and HTTP/WebSocket route rejection after native
server initialization. Rollback must preserve cleanup errors and permit explicit
retry; resource-free rollback permits registry reset and handle destruction.
UDP fault regressions also cover concrete errors after real native drain,
pre-drain timeout/error, native destroy rejection, two real pending sends,
exact-once publication completion, and snapshots racing stop and retry.
Packet regressions cover a timeout returned on the first attempt, rejected
native destruction and explicit retry, plus two native-admitted UDP or KCP
messages across repeated timeout/busy/error stops. Claims finish exactly once,
tokens remain until actual native drain, and late terminals do not count success.
Packet source tests reject a real Graph source name after native initialization,
then inject stop timeout/error/busy and destroy rejection before native cleanup.
They verify original-error snapshots, retained handles, rejection of premature
destroy, repeated stop failure, successful retry and a released Flow run gate.

Stream/listener source open uses the same retained-owner failure contract. Their
native client stop may report a progress error after reaching quiescence; checked
destroy and final STOPPED state determine whether cleanup is complete. Complete
open rollback returns the original admission error with NULL even in this case.
Pending cleanup returns a FAILED owner and cleanup error, retaining the original
admission status/stage in its snapshot. Tests additionally cover listener close,
listener/TLS destroy, scheduler shutdown, and stream connect rejection after Graph
run admission. See the source-specific ADRs for caller migration and alternatives.

## Rollback

Reverting the failed-start protocol requires restoring both success-only core
registration and the adapters' earlier self-cleanup together. Reverting only
the core loses rollback ownership for partial native initialization. Retain
matching SDK/preset versions and keep stop-retry owners alive until cleanup
succeeds before replacing binaries.

Do not roll back CNet stream/listener/packet-source failure handling while an open-failure handle
is retained. Finish its checked stop/destroy first; retaining the caller's
non-NULL-on-error handling is compatible with older ordinary-failure behavior.

The async-terminal registration API is additive. Its rollback removes the registration,
claims, and CNet sink files without changing synchronous adapter layout or
serialized configuration. Graphs using async-terminal adapters then fail
preflight because their adapter provider is absent; there is no fallback.
