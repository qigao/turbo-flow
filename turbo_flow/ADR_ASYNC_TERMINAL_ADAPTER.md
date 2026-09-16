# ADR: Async Terminal Adapter Stage Contract

## Context

The existing `turbo_flow_adapter_ops_t::consume` contract is synchronous. Its
message envelope, settlement scope, stage observer event, sink observer event,
and publication result all end before `consume` returns. Network send APIs such
as `cnet_send()` instead return admission after copying bytes and report the
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

The first packet sink targets raw `cnet_datagram`, whose tagged `on_send`
callback identifies every admitted datagram. `cnet_packet_endpoint` currently
lacks tagged logical-message completion for KCP/secure KCP/FEC, tracked by
qigao/salts#219 and TurboFlow #26. Those modes remain unsupported rather than
falling back to admission semantics.

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

## Alternatives rejected

1. Extend `turbo_flow_adapter_ops_t` in place. Existing callers zero-initialize
   it without a size field, so this is not safely versionable.
2. Return success from synchronous `consume` at send admission. This reports a
   false terminal result and can acknowledge failed work.
3. Block `consume` until `on_send`. This stalls Graph workers, can deadlock the
   caller-owned progress loop, and violates issue #20.
4. Add a private CNet-only completion mechanism. CHTTP needs the same lifecycle,
   and duplicate state machines would diverge.
5. Treat KCP packet admission as completion. CNet exposes no authoritative
   per-logical-message terminal today; qigao/salts#219 is the required upstream
   capability.

## Compatibility and migration

Existing synchronous adapters, DSL, publication calls, and observer ordering do
not change. New adapters opt in through the new registration function. Hosts
that use an async terminal stage must publish through `turbo_flow_publish_async`
or a Reactive run and must drive the adapter's explicit poll API.

CHTTP can reuse the claim contract by moving one claim into each deferred client
request or server response operation and completing it from the protocol's
authoritative terminal callback.

## Verification

Core tests cover move ownership, synchronous rejection, delayed observer and
settlement timing, concrete failure, duplicate terminal, fan-out aggregation,
and message lifetime. CNet integration tests cover stream logical-send
completion and capacity-one backpressure plus raw datagram tag correlation,
capacity backpressure, cancellation, and stop/drain. The adapters preserve
CNet's stale-handle and connection-terminal statuses and independently reject
zero, partial, or over-length successful completion as `SALTS_EPROTO`. C and
C++ header probes plus the installed consumer verify the additive API and
`Salts::CFlow` dependency propagation.

## Rollback

The implementation is additive. Rollback removes the async registration,
claims, and CNet sink files without changing synchronous adapter layout or
serialized configuration. Graphs using async-terminal adapters then fail
preflight because their adapter provider is absent; there is no fallback.
