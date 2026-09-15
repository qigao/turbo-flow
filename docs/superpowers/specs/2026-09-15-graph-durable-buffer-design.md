# Graph Durable Buffer Design

Status: approved architecture, written-spec review pending.

Tracking: #127 core, #128 partitioned drain, #129 protocol migration, #130 recovery conformance, #131 observability/control. Related: #118, #117, #116.

## Problem

TurboFlow currently proves storage-before-business-Graph ordering with a protocol-oriented shape:

```text
real network Source -> protocol decode -> Inbox admission
InboxSource demand -> business Graph -> Sink -> settlement
```

This works functionally, but it is too specialized for high-rate production data. A fast Source can outrun downstream Graph processing. If storage remains only an intake concern, every new Source family must either reproduce the same buffering pattern or stay synchronously coupled to downstream execution.

The system needs a provider-neutral Graph primitive that can absorb bursts, persist work, decouple producer and consumer rates, recover after process loss, and be inserted at any Graph edge.

TurboDB must therefore be a storage provider for a generic durable Graph boundary, not a JTT808/CoAP-specific path.

## Core decision

Introduce a first-class **durable buffer boundary** with one logical Graph identity and two independently progressed runtime sides:

```text
upstream Graph
    |
    v
[ durable admission / sink side ]
    |
    | commit
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

A successful upstream execution ends when the selected provider has accepted/committed the durable record. It does not synchronously continue through downstream Graph work.

Downstream execution starts only when the durable source side claims records and receives explicit drain demand or worker progress.

This boundary reuses the existing `turbo_flow_inbox_t` ownership/state-machine contract and `InboxSource` settlement semantics. It must not create a second queue, database abstraction, executor, or retry state machine.

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

Initial provider set:

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

The Graph topology must remain unchanged when switching providers. Provider/resource selection is configuration.

## Persisted Graph record

Never persist raw `turbo_flow_msg_t` memory. Runtime messages may contain process-local pointers, transport contexts, DLL/session/native handles, borrowed views, or other non-portable state.

Persist a pointer-free envelope derived from the current Inbox v2 record contract. The durable representation contains at minimum:

- stable source identity;
- stable admission/replay identity;
- source sequence;
- timestamp or explicit unknown value;
- message type and flags;
- content descriptor/schema identity;
- correlation bytes;
- payload bytes;
- optional stable partition key once partitioned drain is enabled.

Recovery always creates a fresh process-local Graph message from the durable envelope.

Any attempt to serialize transport/session pointers, DLL addresses, native handles, or borrowed runtime views is invalid.

## Graph semantics

The durable buffer is not an ordinary synchronous transform stage.

Incorrect model:

```text
source -> turbodb-stage -> slow-stage
```

where the database stage writes and immediately calls the next stage in the same execution.

Correct model:

```text
Execution A:
source -> durable admission -> provider commit -> upstream terminal

Execution B, later:
provider claim -> durable source -> downstream Graph -> settlement
```

This separation is the mechanism that absorbs bursts and decouples Source speed from Graph speed.

## Admission semantics

For each upstream message:

1. validate content and stable identity;
2. encode a pointer-free durable record;
3. admit through the selected provider;
4. wait only for the provider's admission/commit contract;
5. report upstream completion after successful admission;
6. do not invoke downstream Graph as part of that execution.

Failure behavior:

- malformed message: fail closed;
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

Example ambiguous crash window:

```text
claim
 -> downstream Sink side effect succeeds
 -> process crashes before durable record completion
```

After restart, the system cannot truthfully know that the side effect was globally committed unless that downstream system provides its own reconciliation mechanism.

The record therefore remains replayable or transitions to explicit owner-lost/unknown state according to the provider contract.

Retry and reconciliation are explicit operations.

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
- `ordering=global`: one total order is preserved, with its throughput cost made explicit;
- no hidden reordering inside a partition;
- no unbounded worker queue;
- each in-flight record owns one unique claim token;
- worker loss cannot create simultaneous valid ownership of one record.

This avoids one slow device blocking every other device while preserving per-device order where required.

## Capacity and backpressure

The buffer remains bounded even when backed by disk/database storage.

Required finite limits include:

- live record count;
- retained bytes;
- max bytes per record;
- in-flight claims;
- worker/in-flight bounds once partitioning exists.

Follow-up controls may include:

- high watermark;
- low watermark;
- max record age;
- pause/resume drain;
- close admission while draining.

If producer rate is permanently higher than drain rate, the backlog eventually reaches the configured hard limit. At that point admission must return explicit backpressure/failure. Disk capacity is not an excuse for an unbounded queue.

## Configuration/DSL direction

Exact parser syntax is subject to implementation design, but the semantic target is a named Graph boundary:

```text
buffer intake {
  provider = turbodb
  resource = telemetry.db
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

A low-rate deployment may use:

```text
buffer intake {
  provider = memory
  ...
}
```

without changing Graph topology.

Provider-specific database connection details belong to the configured resource/provider, not the Graph message path.

## Runtime ownership

Storage provider owns record state.

The durable-buffer owner may orchestrate admission/drain, but it does not mutate provider internals directly.

Only provider operations advance:

- admitted/pending;
- claimed;
- completed;
- failed;
- retried;
- discarded/history;
- owner-generation/takeover state.

Downstream Graph borrows immutable message data derived from the claim. Graph execution does not own or directly mutate durable storage state.

## Observability

Storage commit, Graph completion, Sink completion, protocol ACK, and domain transaction remain distinct observations.

Provider-neutral buffer snapshot should expose at minimum:

- admitted/committed records and bytes;
- pending records/bytes;
- in-flight claims;
- failed/unknown records;
- completed/discarded history counts where retained;
- admission rejection/backpressure counts;
- oldest pending age / claim latency where measurable;
- drain rate;
- high/low watermark state;
- active workers/partitions once #128 exists.

No raw database cursor/handle is exposed through the public Graph surface.

## Recovery

TurboDB recovery must preserve the existing fail-closed durable semantics:

- committed pending record survives restart;
- failed admission is invisible;
- claimed record under owner takeover becomes explicit owner-lost/unknown if final Graph outcome is not known;
- stale claim tokens cannot settle a later generation;
- completed/discarded history supports reconciliation;
- unknown/old storage schema is rejected;
- memory provider does not fake durable recovery.

These are tracked by #130.

## Migration from current protocol intake

Current PR #126 contains useful prototype work:

```text
CNet Source -> ProtocolNetworkIntake -> Inbox
InboxSource -> business Graph -> CNet Sink
```

This work is not discarded. It proves:

- real JT/T808 TCP session/fragment behavior;
- real CoAP UDP input;
- storage-before-business ordering;
- deterministic replay identity;
- bounded admission/backpressure;
- configured destination independence;
- the current Inbox provider/InboxSource state machine.

However, protocol-specific TurboDB parity must stop here. Final production integration should migrate those paths onto the generic Graph durable-buffer boundary (#129).

The end state must contain one production durable storage boundary, not both a generic Graph buffer and a competing protocol-only runtime.

## Issue split

- **#127**: provider-neutral durable-buffer core, memory/TurboDB selection, Graph boundary semantics.
- **#128**: bounded parallel drain, partition ordering, high-throughput worker model.
- **#129**: migrate JT/T808/CoAP real-network intake onto the generic boundary.
- **#130**: crash/restart/owner-loss/ambiguous-settlement conformance.
- **#131**: backlog metrics, watermark state, pause/resume/close/drain controls.
- **#118**: remains the real-protocol acceptance slice and validates #127/#129 rather than implementing a protocol-only database path.

## Acceptance

Core acceptance for #127:

1. one provider-neutral Graph durable-buffer contract;
2. memory and TurboDB use the same Graph topology;
3. upstream Graph completes only after successful storage admission/commit;
4. downstream Graph never runs before storage admission;
5. admission/database failure produces zero downstream execution and zero provider fallback;
6. durable representation is pointer-free;
7. capacity 0/1/N/N+1 behavior is explicit and bounded;
8. Graph success/failure/cancel/settlement-unknown/retry/reconcile states are explicit;
9. process restart/owner takeover preserves truthful at-least-once semantics;
10. any Source can feed the same buffer contract without protocol-specific storage code;
11. installed consumer/ABI/export/dependency gates pass;
12. Debug/ASan and Release gates pass.

Real-network acceptance for #129/#118:

1. fragmented JT/T808/TCP -> generic durable buffer -> business Graph -> real Sink;
2. CoAP/UDP -> same generic durable buffer -> same business Graph shape -> real Sink;
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
