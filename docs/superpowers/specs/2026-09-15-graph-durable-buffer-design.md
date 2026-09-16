# Graph Durable Buffer Design

Status: approved architecture, written-spec review pending.

Tracking: #127 core, #128 partitioned drain, #129 protocol migration, #130 recovery conformance, #131 observability/control. Related: #118, #117, #116.

## Problem

TurboFlow currently proves storage-before-business-Graph ordering with a protocol-oriented shape:

```text
real network Source -> protocol decode -> Inbox admission
InboxSource demand -> business Graph -> Sink -> settlement
```

This works, but it is too specialized for high-rate production data. A fast Source can outrun downstream Graph processing. If storage remains only an intake concern, every new Source family must reproduce the same buffering pattern or remain synchronously coupled to downstream execution.

The system needs a provider-neutral Graph primitive that can absorb bursts, persist work, decouple producer and consumer rates, recover after process loss, and be inserted at any Graph edge.

TurboDB is therefore a provider for a generic durable Graph boundary, not a JTT808/CoAP-specific path.

## Core decision

Introduce a first-class **durable buffer boundary** with one logical Graph identity and two independently progressed runtime sides:

```text
upstream Graph
    |
    v
[ durable admission / sink side ]
    |
    | provider admission / commit
    v
[ bounded provider: memory or TurboDB ]
    |
    | claim
    v
[ durable source / drain side ]
    |
    v
downstream Graph
```

A successful upstream execution ends when the selected provider has accepted or committed the durable record. It does not synchronously continue through downstream Graph work.

Downstream execution starts only when the durable source side claims records and receives explicit drain demand or worker progress.

This boundary reuses the existing `turbo_flow_inbox_t` ownership/state-machine contract and `InboxSource` settlement semantics. It must not create a second queue, database abstraction, executor, or retry state machine.

## Compiler and execution-region semantics

`buffer` is **not** an ordinary transform stage. It is a compiler-visible execution cut.

For a logical graph:

```text
source -> decode -> intake -> normalize -> rules -> sink
```

where `intake` is a durable buffer, the compiler/runtime lowers it conceptually into two execution regions:

```text
Region A:
source -> decode -> durable-admission(intake) -> terminal

Region B:
durable-source(intake) -> normalize -> rules -> sink
```

Required rules:

- the incoming side of a buffer is a terminal durable-admission boundary for that execution;
- the outgoing side is a synthetic logical Source backed by provider claims;
- no call stack, scheduler task, or synchronous continuation crosses the buffer boundary;
- downstream scheduling may happen immediately after commit if capacity exists, but it is a new execution, not continuation of Region A;
- one named buffer may fan in multiple producers intentionally; all admitted records emerge from the same logical durable source;
- downstream fan-out is ordinary Graph fan-out after the durable source;
- if different continuations require different recovery identities/order domains, use different named buffers rather than hiding route-specific state in provider internals.

The implementation may initially materialize these as two runtime plans/owners even though the user config presents one logical Graph. The public semantics are the execution cut, not the internal plan count.

## Scope

The durable buffer must be usable by any Source or intermediate stage:

```text
JTT808 -> buffer -> business
CoAP   -> buffer -> business
MQTT   -> buffer -> business
HTTP   -> buffer -> business
CNet   -> buffer -> business
```

It must also be legal inside a Graph:

```text
source -> decode -> buffer(raw-normalized) -> expensive processing -> buffer(processed) -> sink
```

The node has no protocol-specific fields.

## Provider model

Initial providers:

1. **bounded memory**
   - finite records, bytes, record size, and claims;
   - process-local only;
   - never claims crash durability;
   - useful for low-rate or explicitly non-durable workloads.

2. **TurboDB**
   - transactional admission;
   - durable pending/failed/history state;
   - process restart and owner-takeover semantics;
   - database failure never switches provider and never bypasses the boundary.

The Graph topology remains unchanged when switching providers. Provider/resource selection is configuration.

## Durable record and message projection

Never persist raw `turbo_flow_msg_t` memory. Runtime messages contain process-local state such as `transport_context` and private content projections; those must not cross process boundaries.

Persist a pointer-free envelope derived from the current Inbox v2 record contract. It contains at minimum:

- stable logical producer/source identity;
- admission/replay identity;
- source or boundary sequence where available;
- timestamp or explicit unknown value;
- message type and flags;
- content descriptor/schema identity;
- correlation bytes;
- payload bytes;
- optional stable partition key once partitioned drain is enabled.

The durable envelope does **not** persist:

- `transport_context`;
- `_content_handle` or parsed projection pointers;
- DLL/native/session handles;
- borrowed process addresses;
- scheduler/run objects.

Recovery constructs a fresh `turbo_flow_msg_t` with new process-local ownership. Schema-bound projections are resolved/rebuilt lazily from the trusted content descriptor and registry after recovery.

## Stable identity and replay modes

A generic intermediate Graph message does not automatically have a cross-process stable admission identity. The runtime message `id` is not sufficient as a universal durable replay key.

The durable buffer therefore has two explicit identity modes.

### Stable upstream identity

When the upstream message carries a trusted, pointer-free stable identity supplied by the Source/domain adapter, the buffer uses it to form the durable admission key.

Examples include protocol message sequence/correlation identities or another registered stable application identity.

Exact replay of the same stable identity + complete record returns the original durable receipt according to the provider contract. Different content under the same identity is a protocol/data-integrity error.

### Generated boundary identity

If no stable upstream identity exists, the buffer generates a new bounded admission identity before its first provider call and reuses that identity for every retry of that admission attempt.

Each generated binding uses a fresh Salts UUID namespace together with provider
generation and a checked local sequence (`g<generation>:<binding-uuid>:<sequence>`).
This avoids identity reuse after unbind/rebind or across Flow instances; entropy
failure rejects bind. Stable identity mode is unchanged. Provider pointers remain
runtime-only metadata. A projection requires canonical payload bytes; neither an
active result claim nor a committed result may cross admission.

Properties:

- retries of the same in-process admission attempt reuse the same identity;
- after successful commit the generated identity is part of the durable record;
- if the process dies before the producer can prove whether the commit happened, a later producer redelivery may receive a new identity;
- therefore generated mode provides at-least-once semantics but **does not claim cross-process duplicate suppression**.

A deployment that requires exact replay deduplication across producer reconnect/restart must provide a stable upstream identity. The buffer must not synthesize a false exactly-once guarantee from a process-local message ID.

The stable logical `source_id` for an intermediate buffer admission is derived from configured Graph identity (for example flow/buffer/producer identity), never from a process pointer or ephemeral plugin address.

## Admission semantics

For each upstream message:

1. validate payload/content and durable-identity mode;
2. encode a pointer-free durable record;
3. admit through the selected provider;
4. wait only for the provider admission/commit contract;
5. report upstream terminal success after successful admission;
6. never invoke downstream Graph as part of that execution.

Failure behavior:

- malformed/non-persistable message: fail closed;
- provider capacity: explicit backpressure/failure;
- database transaction failure: exact error, zero downstream execution;
- configured TurboDB failure: zero memory fallback;
- no silent drop;
- no direct bypass from upstream to downstream Graph.

## Drain semantics

The durable source side owns claim and settlement progression.

Initial single-owner lifecycle:

1. claim at most one FIFO record;
2. materialize a fresh Graph message;
3. request downstream Graph execution;
4. observe terminal Graph/Sink outcome;
5. complete or fail the durable claim explicitly;
6. never implicitly retry a failed/unknown settlement.

This can initially reuse the current `InboxSource` implementation shape.

## Delivery guarantee

The boundary provides **at-least-once downstream Graph execution** under durable replay. It must not claim end-to-end exactly-once semantics.

Ambiguous crash window:

```text
claim
 -> downstream side effect succeeds
 -> process crashes before durable record completion
```

After restart, the system cannot truthfully know the global side-effect outcome unless the downstream system exposes its own idempotency/reconciliation contract.

The record therefore remains replayable or transitions to explicit owner-lost/unknown state according to the provider contract. Retry and reconciliation are explicit operations.

## Ordering and parallelism

The core boundary must not hard-code global FIFO as the permanent scalability model.

Phase 1 may use one serialized claim/run owner.

Phase 2 (#128) adds bounded partitioned drain:

```text
partition_by = source_id | device_id | session_id | stable custom key
workers = N
max_in_flight = M
batch_claim = K
ordering = partition | global
```

Semantics:

- `ordering=partition`: records within one partition remain ordered; independent partitions may run concurrently;
- `ordering=global`: one total order is preserved, with its throughput cost explicit;
- no hidden reordering inside a partition;
- no unbounded worker queue;
- each in-flight record owns one unique claim token;
- worker loss cannot create simultaneous valid ownership of one record.

This prevents one slow device from blocking unrelated devices while preserving per-device order when required.

## Capacity and backpressure

The buffer remains bounded even when backed by disk/database storage.

Required finite limits include:

- live record count;
- retained bytes;
- max bytes per record;
- in-flight claims;
- worker/in-flight bounds once partitioning exists.

Follow-up controls (#131) include high/low watermarks, max record age where supported, pause/resume drain, and close-admission-while-draining.

If producer rate is permanently higher than drain rate, backlog eventually reaches the configured hard limit. Admission must then return explicit backpressure/failure. Disk capacity does not create an unbounded queue.

## Configuration/DSL direction

Exact parser syntax is subject to implementation planning, but the semantic target is a named Graph boundary:

```text
buffer intake {
  provider = turbodb
  resource = telemetry.db
  identity = stable_or_generated
  max_records = 10000000
  max_bytes = 100GB
  max_record_bytes = 1MB
  max_in_flight = 1024
  workers = 8
  partition_by = source_id
  ordering = partition
}
```

and Graph wiring:

```text
source telemetry adapter jtt808.input
stage normalize ...
stage output adapter mqtt.output

graph main {
  telemetry -> intake
  intake -> normalize
  normalize -> output
}
```

A low-rate deployment can select `provider = memory` without changing topology.

Provider-specific database connection details belong to the configured resource/provider, not the Graph message path.

## Runtime ownership

The storage provider owns record state. The durable-buffer owner orchestrates admission/drain but does not mutate provider internals directly.

Only provider operations advance admitted/pending, claimed, completed, failed, retried, discarded/history, and owner-generation/takeover state.

Downstream Graph borrows immutable message data derived from the claim. Graph execution does not own or directly mutate durable storage state.

## Observability

Storage commit, Graph completion, Sink completion, protocol ACK, and domain transaction remain distinct observations.

Provider-neutral buffer snapshots (#131) expose at minimum admitted/committed records and bytes, pending records/bytes, in-flight claims, failed/unknown records, retained history counts, backpressure counts, oldest pending age/claim latency where measurable, drain rate, watermark state, and later worker/partition activity.

No raw database cursor/handle escapes through the public Graph surface.

## Recovery

TurboDB recovery (#130) preserves fail-closed durable semantics:

- committed pending record survives restart;
- failed/uncommitted admission is invisible;
- claimed record under owner takeover becomes explicit owner-lost/unknown when final Graph outcome is unknown;
- stale claim tokens cannot settle a later generation;
- completed/discarded history supports reconciliation;
- unknown/old storage schema is rejected;
- memory provider does not fake durable recovery.

## Migration from current protocol intake

Draft PR #126 contains useful prototype work:

```text
CNet Source -> ProtocolNetworkIntake -> Inbox
InboxSource -> business Graph -> CNet Sink
```

It proves real JT/T808 TCP session/fragment behavior, real CoAP UDP input, storage-before-business ordering, deterministic protocol replay identity, bounded admission/backpressure, destination independence, and the current Inbox/InboxSource state machine.

Protocol-specific TurboDB parity stops here. Final production integration migrates those paths onto the generic Graph durable-buffer boundary (#129).

The end state contains one production durable storage boundary, not both a generic Graph buffer and a competing protocol-only runtime.

## Issue split

- **#127**: provider-neutral durable-buffer core, memory/TurboDB selection, compiler execution-cut semantics.
- **#128**: bounded parallel drain, partition ordering, high-throughput worker model.
- **#129**: migrate JT/T808/CoAP real-network intake onto the generic boundary.
- **#130**: crash/restart/owner-loss/ambiguous-settlement conformance.
- **#131**: backlog metrics, watermark state, pause/resume/close/drain controls.
- **#118**: real-protocol acceptance slice validating #127/#129 rather than implementing a protocol-only database path.

## Acceptance

Core #127:

1. one provider-neutral Graph durable-buffer contract;
2. buffer is a compiler/runtime execution cut, not a synchronous stage;
3. memory and TurboDB use the same logical Graph topology;
4. upstream execution terminates only after successful storage admission/commit;
5. downstream execution never starts as synchronous continuation of upstream;
6. admission/database failure produces zero downstream execution and zero provider fallback;
7. durable representation is pointer-free and projections are rebuilt after recovery;
8. stable-identity mode provides exact replay matching; generated mode explicitly does not claim cross-process dedupe;
9. capacity 0/1/N/N+1 behavior is explicit and bounded;
10. Graph success/failure/cancel/settlement-unknown/retry/reconcile states are explicit;
11. process restart/owner takeover preserves truthful at-least-once semantics;
12. any Source/intermediate stage can feed the same buffer without protocol-specific storage code;
13. installed consumer/ABI/export/dependency gates pass;
14. Debug/ASan and Release gates pass.

Real-network #129/#118:

1. fragmented JT/T808/TCP -> generic durable buffer -> business Graph -> real Sink;
2. CoAP/UDP -> same generic durable buffer -> same downstream Graph shape -> real Sink;
3. memory/TurboDB provider choice changes config, not topology;
4. two explicit Sink destinations remain independent;
5. TCP generation reuse cannot inherit partial parser state;
6. no protocol-specific database fallback path remains.

## Non-goals

- no Kafka-compatible wire protocol;
- no distributed consensus;
- no implicit unbounded queue;
- no hidden retry/drop policy;
- no false exactly-once claim;
- no raw runtime pointer/session persistence;
- no automatic legacy Inbox/schema conversion;
- no protocol-specific TurboDB fork;
- no C/CMake/runtime fallback.
