# Issue #118 Real Protocol Intake Completion Design

Date: 2026-09-15

Parent: #118

Related: #117, #73, #74, #115, #116, #124, PR #119, PR #123, PR #125

## 1. Purpose

Complete the remaining network-facing acceptance of #118 without reopening the already-merged Inbox v2 design and without making unfinished RulesForge, FlowMQ, or Flowie providers prerequisites.

The accepted completion slice uses two real protocol sources with different transport classes:

- JT/T 808 over a real CNet TCP listener.
- CoAP over a real CNet UDP packet endpoint.

Both sources pass through the same configured Inbox contract, the same existing Inbox-to-Graph driver, the same business Graph topology, and the same CNet datagram Sink provider type. Destination selection is explicit configuration and is never derived from ingress protocol identity.

FlowMQ remains tracked by #74. Flowie remains tracked by #115. The real RulesForge/TurboScript DLL migration remains tracked by #73. None of those tasks is a prerequisite for this #118 completion slice.

## 2. Existing Facts This Design Reuses

The merged tree at base commit `ed4d51b7db575f4f59c95a583ff90c5538268ad9` already provides:

1. Inbox v2 with bounded memory and durable TurboDB/SQLite providers, exact schema preflight, generation ownership, takeover, replay/history semantics, and no database-to-memory fallback.
2. A provider-neutral Inbox-to-Graph driver from PR #123. Business Graph execution begins only after a record has been claimed from Inbox and settlement is explicit.
3. `ProtocolInbox` from PR #125. `turbo_flow_protocol_inbox_admit()` synchronously encodes a decoded protocol message into the durable `turbo-flow.protocol.inbox` v1 TBE envelope and admits it to the configured Inbox.
4. `ProtocolSource` ABI v1. A caller opens a protocol parser session, feeds transport bytes, and receives complete decoded frames through one `admit` callback. A complete frame is not consumed until that callback succeeds. Capacity rejection retains the current frame and requires an explicit empty-feed retry.
5. Independent protocol DLLs for MQTT-SN, CoAP, LwM2M, OCPP, GB/T 32960, and JT/T 808.
6. PR #119 exact-ID/version/absolute-path DLL loading plus retained catalog snapshots and transactional Product provider descriptors.
7. Real CNet TCP/TLS listener Source and UDP/KCP packet Source owners with bounded queues/resources and real loopback tests.
8. Real CNet stream/datagram/packet Sink owners with explicit native/protocol terminal boundaries.
9. TurboFlow managed async-terminal claims, which let a Sink retain one publication until an external terminal condition is known without borrowing the caller's message.

The missing piece is an installed composition owner above Graph/CNet/Protocol that can combine a configured CNet Source provider and a configured protocol provider while preserving storage-first admission, bounded backpressure, module leases, and exact lifecycle ownership.

## 3. Goals

This change must:

- prove two real protocol network Sources can reach the same configured Inbox before any business Graph work begins;
- expose one installed high-level configured intake owner rather than a test-only bridge;
- keep the existing Inbox v2 ABI/schema and `ProtocolInbox` envelope unchanged;
- keep protocol DLLs responsible only for framing/codec semantics;
- keep CNet responsible for connection/session/transport truth;
- keep Inbox responsible for record/claim/settlement truth;
- keep the existing Inbox driver responsible for starting and settling business execution;
- preserve explicit destination configuration at the business Sink boundary;
- preserve bounded memory and backpressure at every handoff;
- preserve exact generation identity across connection/session reuse;
- retain every DLL/catalog lease until the last callback using it has terminated;
- fail closed when durable admission fails;
- add no compatibility shim, alternate provider, static fallback, or runtime fallback.

## 4. Non-Goals

This slice does not:

- implement or complete RulesForge/TurboScript typed-operation DLL execution from #73;
- implement FlowMQ Source/Sink from #74;
- implement Flowie Source/Sink from #115;
- generalize the new intake owner to KCP/secure-KCP in v1;
- promise end-to-end exactly-once delivery;
- delay native protocol ACK until business Graph completion unless the native protocol owner already requires that behavior;
- add automatic retries for business Graph, result Sink, network, or non-capacity database failures;
- migrate legacy data/configuration/ABI;
- add a second mutable connection/session registry beside CNet;
- change Inbox v2 storage naming or schema;
- allow protocol identity to implicitly select a business Sink destination;
- extend the PluginHost root ABI or the generic Product owner vtable.

KCP remains supported by CNet itself, but ProtocolNetworkIntake v1 rejects it because a blocked Inbox must pause Source `poll()` while KCP requires independent timer/control progress. A future KCP intake contract needs an explicit transport-progress lane and is outside #118.

## 5. Selected End-to-End Paths

### 5.1 JT/T 808 over TCP

The TCP path is:

`real TCP peer -> configured CNet listener Source DLL -> intake plumbing Flow -> ProtocolNetworkIntake async Sink -> JT/T 808 protocol DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> existing Inbox Graph driver -> shared business Graph -> configured CNet datagram Sink -> real UDP peer`

The listener Source remains the transport owner. Its CNet connection handle `{slot, generation}` is the authoritative connection identity.

### 5.2 CoAP over UDP

The UDP path is:

`real UDP peer -> configured CNet packet Source DLL (packet_mode=udp) -> intake plumbing Flow -> ProtocolNetworkIntake async Sink -> CoAP protocol DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> existing Inbox Graph driver -> shared business Graph -> configured CNet datagram Sink -> real UDP peer`

The packet Source remains the transport/session owner. Its message-owned `turbo_flow_cnet_packet_message_context_t` and generation-checked `cnet_packet_session` are authoritative transport identity.

### 5.3 Intake plumbing Flow is not the business Graph

The pre-storage intake Flow is deliberately restricted to:

- exactly one configured CNet Source stage;
- exactly one `protocol.intake` terminal Sink stage;
- one direct source-to-sink edge;
- no business transform/operation;
- no business database capability;
- no business destination routing;
- no result Sink.

The #118 invariant is **storage admission before business Graph execution**, not "no scheduler or CFlow primitive may run before storage." Test counters for this invariant count only the post-Inbox business Graph requested by the existing Inbox driver.

## 6. Required CNet Listener Message Identity Addition

The packet Source already stores immutable, message-owned session identity inside the emitted message buffer. The TCP listener Source currently emits owned bytes but does not attach its `cnet_connection {slot, generation}` to the message.

Add one exact, read-only listener message context owned by `message->buffer`:

```c
#define TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION 1u

typedef struct turbo_flow_cnet_listener_message_context_s {
  size_t size;
  uint32_t version;
  cnet_connection connection;
} turbo_flow_cnet_listener_message_context_t;

TURBO_FLOW_C_API const turbo_flow_cnet_listener_message_context_t *
turbo_flow_cnet_listener_message_context(const turbo_flow_msg_t *message);
```

The listener Source allocates one buffer large enough for the context plus payload, stores the context at the front, sets `message->transport_context`, and points `message->payload` at the bytes after the context. The accessor succeeds only when the context is message-owned, exact-versioned, structurally valid, and the payload lies after the context inside the same buffer.

This is a projection only. It does not create a second mutable connection table. A stale listener message retains the generation captured when the receive callback copied its bytes, so slot reuse cannot reinterpret old bytes under a newer generation.

The public payload contract remains `message->payload`; consumers must not assume payload begins at offset zero of `message->buffer`.

## 7. Installed ProtocolNetworkIntake Owner

The production composition is an installed high-level owner named `TurboFlow::ProtocolNetworkIntake`. It lives above Graph, CNetAdapter, ProtocolIngress, ProtocolIngressInbox, and PluginHost; those lower layers do not depend on it.

It adds no raw-feed API and does not extend PluginHost. It consumes the existing public transactional provider/catalog contracts and existing CNet/protocol message contexts.

### 7.1 Public API

```c
#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION UINT32_C(1)

typedef struct turbo_flow_protocol_network_intake_s
    turbo_flow_protocol_network_intake_t;

typedef struct turbo_flow_protocol_network_intake_config_s {
  size_t size;
  uint32_t version;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  const turbo_flow_resolved_config_t *resolved;
  turbo_flow_inbox_t *inbox;
  const char *source_adapter_name;
  const char *intake_adapter_name;
} turbo_flow_protocol_network_intake_config_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT \
  {sizeof(turbo_flow_protocol_network_intake_config_t), \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION, NULL, NULL, NULL, NULL, NULL}

typedef enum turbo_flow_protocol_network_intake_state_e {
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED = 1,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_BACKPRESSURED,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPING,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED,
  TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED
} turbo_flow_protocol_network_intake_state_t;

typedef struct turbo_flow_protocol_network_intake_snapshot_s {
  size_t size;
  uint32_t version;
  turbo_flow_protocol_network_intake_state_t state;
  int status;
  size_t active_sessions;
  size_t pending_claims;
  size_t pending_bytes;
  uint64_t frames_admitted;
  int backpressured;
} turbo_flow_protocol_network_intake_snapshot_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT \
  {sizeof(turbo_flow_protocol_network_intake_snapshot_t), \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION, \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED, SALTS_OK, 0u, 0u, 0u, 0u, 0}

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_create(
    const turbo_flow_protocol_network_intake_config_t *config,
    turbo_flow_t **intake_flow_io,
    turbo_flow_protocol_network_intake_t **out,
    turbo_flow_config_error_t *error);

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_start(
    turbo_flow_protocol_network_intake_t *intake);

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_poll(
    turbo_flow_protocol_network_intake_t *intake, uint32_t timeout_ms,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot);

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_snapshot(
    const turbo_flow_protocol_network_intake_t *intake,
    turbo_flow_protocol_network_intake_snapshot_t *snapshot);

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_stop(
    turbo_flow_protocol_network_intake_t *intake, uint64_t timeout_ms);

TURBO_FLOW_C_API int turbo_flow_protocol_network_intake_destroy(
    turbo_flow_protocol_network_intake_t *intake);
```

### 7.2 Ownership

`create()` borrows `resolved` only for the call and copies every required value. It borrows the selected `inbox` for the complete owner lifetime; the caller keeps the Inbox alive until `destroy()` succeeds.

`create()` retains `catalog` and therefore every represented DLL needed by the chosen CNet Source provider and protocol provider. The owner releases that retention only after the configured Source Product owner and protocol registry/owner are destroyed.

`intake_flow_io` contains a parsed, uncompiled two-stage intake Flow. Preflight failures leave it caller-owned. Immediately before the first materialization/registration side effect, `create()` moves the Flow by setting `*intake_flow_io = NULL`; from that point all cleanup belongs to the intake owner even when creation later fails.

The owner does not own or create the configured Inbox provider and never substitutes another one.

## 8. Exact Intake Configuration

The adapter named by `intake_adapter_name` must have kind `protocol.intake` and exactly these fields:

```yaml
schema_version: 1
protocol_provider: jtt808 | coap
protocol_kind: jtt808 | coap
protocol_version: <exact protocol version>
source_id: <stable configured source identity>
max_sessions: <non-zero bounded integer>
max_frame_size: <non-zero bounded integer>
max_pending_claims: <non-zero bounded integer>
max_pending_bytes: <non-zero bounded integer>
```

Unknown or missing fields fail preflight.

The supported v1 pairs are exact:

- `protocol_provider=jtt808`, `protocol_kind=jtt808`, `protocol_version=2019-A1`, Source kind `cnet.listener_source`.
- `protocol_provider=coap`, `protocol_kind=coap`, `protocol_version=RFC7252`, Source kind `cnet.packet_source` with `packet_mode=udp`.

`max_sessions` must not exceed the configured CNet connection/session capacity. `max_frame_size` must not exceed the configured CNet `max_message_bytes` and the opened protocol provider limit.

The owner reads the Source's configured `scheduler_max_steps_per_poll` and `max_message_bytes`. One CNet Source poll performs at most two bounded Scheduler passes, so preflight requires:

- `max_pending_claims >= 2 * scheduler_max_steps_per_poll`, with overflow checked;
- `max_pending_bytes >= max_pending_claims * max_message_bytes`, with overflow checked.

This conservative bound guarantees that one already-entered CNet `poll()` call cannot overflow the intake's retained async-claim storage after an Inbox capacity block becomes visible.

All limits are finite and preallocated. No retained-claim vector grows at runtime.

## 9. Source Provider and Protocol Provider Assembly

`ProtocolNetworkIntake` uses the same retained catalog snapshot for both sides.

For the CNet Source:

1. project the configured Source adapter from `resolved`;
2. obtain the transactional Product catalog from the snapshot;
3. find the exact provider whose `kind` equals the Source adapter kind;
4. run that provider's `preflight()` with no external side effects;
5. after ownership transfer, call its `materialize()` into the intake Flow and retain the returned `turbo_flow_plugin_product_owner_v1_t`;
6. require `TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL` and a non-NULL `poll` callback.

For the protocol:

1. create a protocol registry from the same retained snapshot;
2. create one registered protocol owner by exact `protocol_provider` name and exact protocol open request;
3. require the opened kind and version to match the intake configuration;
4. borrow the provider-neutral `turbo_flow_protocol_t` only while that protocol owner remains alive.

The intake Sink itself is built into `ProtocolNetworkIntake`; it is registered directly on the intake Flow as a managed async-terminal Sink before compilation. It is not another plugin provider and creates no second DLL-loading path.

## 10. Parser-Session Mapping

The parser-session table is a fixed derived projection keyed by CNet slot and generation. It is not a second mutable transport registry.

Rules:

1. First message for a slot opens one `ProtocolSource` parser session using that CNet generation.
2. Repeated messages for the same slot and generation feed the same parser session.
3. If the same slot appears with a newer generation, close the old parser session before opening the new one.
4. A message carrying an older generation than the retained slot generation fails with `SALTS_EPROTO` before durable admission.
5. Generation change retires only transport-local partial parser state that has not crossed Inbox admission.
6. Parser session identifiers are process-local and never enter durable identity.
7. The table has exactly `max_sessions` entries and never grows.
8. A close notification channel is not added solely for #118; bounded inactive parser state may remain until slot reuse or intake shutdown.
9. Shutdown closes every retained parser session before destroying `ProtocolSource`.

This prevents a partial JT/T 808 frame from generation N from being completed by generation N+1 bytes after a TCP slot is reused.

For CoAP/UDP, `device_id` passed to `ProtocolSource` is the canonical numeric peer endpoint derived from the immutable packet message context, not a raw CNet struct. JT/T 808 leaves `device_id` empty at session open because the frame itself carries the stable terminal identity.

## 11. Managed Async-Terminal Intake Sink

The intake Flow terminates at a managed async-terminal Sink owned by `ProtocolNetworkIntake`.

Every Sink submission moves the provided `turbo_flow_async_terminal_claim_t` before returning `SALTS_OK`. The claim's retained message therefore remains valid until the intake explicitly completes that claim.

### 11.1 Normal submission

When no frame is capacity-blocked:

1. validate listener or UDP packet message context and obtain `{slot, generation}`;
2. map/open the correct parser session;
3. feed `message->payload` exactly once to `ProtocolSource`;
4. if the bytes are only a partial stream frame, complete the async claim with `SALTS_OK` after `ProtocolSource` has copied them into its bounded parser buffer;
5. if all complete frames exposed by that message are admitted to Inbox, complete the claim with `SALTS_OK`;
6. if a complete frame is retained because Inbox returned a capacity status, keep that claim live and mark the owner backpressured;
7. if a non-capacity error occurs, complete the claim with that error and transition the intake to FAILED.

A TCP message may expose multiple complete frames. The claim remains live until every frame represented by that message has either crossed Inbox admission or the submission becomes terminal.

### 11.2 Submission while already backpressured

If `ProtocolSource` already retains one complete frame, later submissions that entered the same CNet `poll()` call are not fed. Their moved async claims are appended to the preallocated FIFO pending-claim array.

The configured claim/byte bounds are large enough for the worst case from one CNet `poll()` call. Overflow is therefore an invariant violation and fails closed; it is not a normal backpressure branch.

### 11.3 Retry and poll ownership

`turbo_flow_protocol_network_intake_poll()` is the only operation allowed to call the configured Source Product owner's `poll()` callback.

Its order is:

1. retry the currently retained `ProtocolSource` frame using the documented empty-feed call;
2. when that succeeds, process queued async claims FIFO until the queue is empty or another frame becomes capacity-blocked;
3. while any retained frame or queued claim remains blocked, do **not** call the CNet Source Product owner's `poll()`;
4. only when the intake queue is clear, invoke Source `poll(timeout_ms)` once;
5. observe any backpressure/error produced synchronously by that Source poll and return an updated snapshot.

For TCP and plain UDP, pausing Source `poll()` is the transport backpressure boundary used by #118. No new receive is advanced by TurboFlow while storage is blocked. KCP is rejected by v1 because its independent protocol timers cannot use this rule.

Repeated caller `poll()` calls are the explicit retry operation. The owner performs no background retry thread and no alternate provider selection.

## 12. ProtocolInbox Admission and Durable Identity

`ProtocolSource.ops.admit` calls `turbo_flow_protocol_inbox_admit()` and no other successful data path.

A frame crosses acceptance only when the selected Inbox owns a complete independent copy.

The identity resolver produces:

- `source_id`: the exact configured `source_id`;
- `source_sequence`: decoded protocol metadata sequence;
- `correlation`: decoded protocol correlation when present;
- `timestamp_ns`: host admission timestamp, never part of the dedupe key;
- `admission_id`: canonical ASCII `<protocol>/<device>/<message_type>/<sequence>/<xxh3-128(payload)>`.

The Inbox already deduplicates on `(source_id, admission_id)` and compares the complete record on replay. A hypothetical hash collision with different content therefore fails `SALTS_EPROTO`; it never silently aliases different records.

The durable record never stores:

- C pointers;
- DLL addresses;
- raw CNet connection/session structs;
- process-local parser session IDs;
- borrowed network buffers.

CNet generation participates only in in-process stale-event rejection.

## 13. Store-Before-Business-Graph Boundary

No network owner or intake plumbing Flow executes business logic.

The only path to business Graph execution is:

1. protocol frame decoded;
2. `ProtocolInbox` admission succeeds;
3. Inbox record becomes claimable;
4. existing provider-neutral Inbox Source/driver claims it;
5. shared business Graph is requested;
6. terminal Graph outcome settles that Inbox claim through the existing driver contract.

A focused test holds/fails Inbox admission and asserts the post-Inbox business Graph invocation count stays zero.

The business Graph used by this #118 acceptance is ordinary CFlow/TurboFlow behavior and is not evidence that #73 RulesForge/TurboScript DLL migration is complete.

## 14. Business Sink and Destination Semantics

Both ingress protocols use the same CNet datagram Sink provider type and the same business Graph topology.

Two explicit destination configurations are required:

- configuration A sends the business result to real UDP loopback peer A;
- configuration B sends the same business result to real UDP loopback peer B.

The destination comes only from configured business/Sink binding. Switching JT/T 808 to CoAP does not select another destination by itself.

Protocol reply sinks remain separate from independent business result sinks. A reply-specific session/generation is never generalized into ordinary result routing.

## 15. Lifecycle and Failure Ordering

### 15.1 Create/preflight

Before the first side effect, `create()` validates:

1. exact public ABI shapes;
2. parsed intake Flow is exactly Source -> terminal protocol.intake Sink;
3. both named adapters exist in resolved config;
4. exact protocol.intake fields and supported transport/protocol pair;
5. Source provider exists in the retained transactional Product catalog;
6. Source provider preflight succeeds;
7. protocol provider exists in the retained protocol catalog and supports the configured open request;
8. every capacity multiplication fits and satisfies the conservative pending-claim bound;
9. the selected Inbox is an exact live v2 handle.

Then ownership is transferred and assembly proceeds:

1. retain the catalog snapshot;
2. create protocol registry and exact protocol owner;
3. create `ProtocolInbox`;
4. create `ProtocolSource`;
5. preallocate parser-session and async-claim storage;
6. register the internal managed async-terminal intake Sink;
7. materialize the configured CNet Source Product owner into the Flow;
8. compile the Flow;
9. publish the `ProtocolNetworkIntake` owner in COMPILED state.

Any failure after ownership transfer unwinds in reverse order and returns no partial public owner.

### 15.2 Start

`start()` calls `turbo_flow_start()` exactly once. The configured CNet Source adapter opens its native transport from its normal managed Source `start` callback. No network socket is opened during preflight/create.

### 15.3 Stop

`stop()` is explicit and retryable:

1. mark intake STOPPING so no new public poll may advance the Source;
2. quiesce the configured Source Product owner;
3. stop accepting new parser feeds;
4. explicitly complete every retained async claim with `SALTS_ECANCELED` and force-close unadmitted parser state;
5. call `turbo_flow_stop()` so the real CNet Source performs its normal stop/drain contract;
6. call Source owner `drain(timeout_ms)`;
7. leave the owner STOPPED only after Flow and Source owner are quiescent.

Already-admitted Inbox records are never discarded by intake shutdown. Only bytes that have not crossed Inbox admission are canceled.

### 15.4 Destroy

`destroy()` succeeds only from STOPPED/failed-cleanup state with no live async claims.

It destroys the intake Flow while the Source Product owner context is still alive, then invokes Source owner shutdown/destroy, destroys `ProtocolSource`, `ProtocolInbox`, protocol owner and registry, and finally releases the retained catalog snapshot.

The caller-owned Inbox remains untouched.

Any owner/module lifetime error fails closed and keeps the remaining state retryable; no force-unload path invalidates callbacks.

## 16. Configuration and Provider Rules

The complete installed configuration explicitly selects:

- ordered plugin DLL manifest with exact ID/version/absolute path;
- real CNet Source adapter instance;
- `protocol.intake` adapter instance;
- protocol provider and protocol version;
- stable intake `source_id`;
- parser/frame/pending-claim capacity bounds;
- chosen Inbox provider/resource constructed by the host;
- shared business Graph binding;
- configured CNet datagram business Sink;
- explicit UDP destination.

The intake owner does not call `turbo_flow_plugin_generation_create()` for its two-stage Flow because `protocol.intake` is a built-in high-level composition Sink rather than a plugin Product provider. It still uses the exact same transactional Source provider `preflight/materialize` contract from the retained snapshot.

The post-Inbox business Graph continues using the normal generation machinery.

There is no protocol fallback, transport fallback, database fallback, old DLL fallback, static provider fallback, or CMake-selected runtime fallback.

## 17. RED -> GREEN Acceptance Matrix

Implementation begins with focused RED tests proving current master lacks the required production composition.

Required GREEN coverage:

1. **Public intake ABI**: exact C/C++ header shape, zero-state output, create/start/poll/snapshot/stop/destroy state errors.
2. **Listener message context**: exact ABI/lifetime and payload-after-context validation.
3. **JT/T 808 TCP real peer**: fragmented real TCP traffic crosses configured listener Source DLL -> ProtocolNetworkIntake -> JT/T 808 DLL -> ProtocolSource -> ProtocolInbox -> Inbox -> business Graph -> real CNet datagram Sink.
4. **CoAP UDP real peer**: real UDP datagram crosses configured packet Source DLL -> ProtocolNetworkIntake -> CoAP DLL -> ProtocolSource -> ProtocolInbox -> Inbox -> same business Graph -> same Sink provider type.
5. **KCP rejection**: `cnet.packet_source` with `packet_mode=kcp` fails ProtocolNetworkIntake preflight before network side effects.
6. **Store before business Graph**: hold/fail Inbox admission and assert business Graph invocation count is zero.
7. **No database fallback**: force TurboDB admission/commit failure and assert business Graph count is zero and memory-provider admission count is zero.
8. **Capacity 0/1/N/N+1**: zero parser/pending capacity fails preflight; runtime covers one/N/N+1 sessions plus existing Inbox record/byte/in-flight limits.
9. **Exactly one admission after retry**: a frame retained because of Inbox capacity becomes exactly one Inbox record after space is available.
10. **No Source progress while blocked**: CNet Source owner poll counter/native receive counter does not advance while retained-frame retry remains capacity-blocked.
11. **TCP generation reuse**: partial JT/T 808 frame on generation N cannot be completed by generation N+1 bytes in a reused listener slot.
12. **Stale UDP generation**: stale CoAP packet session generation is rejected before durable admission.
13. **Pending-claim bound**: one Source poll may fill but cannot exceed preallocated claim/byte storage; invalid config that cannot cover two Scheduler passes fails preflight.
14. **Partial TCP chunk**: an async claim for a partial frame completes after bounded parser copy, while no Inbox record exists until a complete frame is decoded/admitted.
15. **Business Graph failure**: failure settles through the existing Inbox driver and never triggers implicit replay.
16. **Cancel/drain**: stop cancels only unadmitted bytes; accepted Inbox records remain claimable/settleable.
17. **Unload lease**: active intake/source/protocol owner prevents catalog/module unload; release succeeds only after terminal cleanup.
18. **Memory/TurboDB parity**: both providers run the same real-network behavior assertions except durability-only recovery expectations.
19. **TurboDB recovery**: after committed admission and restart/takeover under the existing provider contract, the record is claimable and is never duplicated into memory.
20. **Different destinations**: the same business result reaches UDP destination A and B in separate configs; ingress protocol does not choose the destination.
21. **Installed package**: fresh C and C++ installed consumers link `TurboFlow::ProtocolNetworkIntake` without private build-tree headers.
22. **DLL gates**: canonical plugin exports, dependency closure, CRT/profile checks, no retired protocol Graph/runtime artifact, and no direct JTT808/CoAP dependency from Graph core.
23. **Debug/ASan and Release**: focused tests, affected adjacent tests, full CTest, install consumer, formatting, and `git diff --check` all pass.

## 18. Test Architecture

Keep three levels separate so fixtures are never presented as real network gates.

### Level A: focused contract tests

- public ProtocolNetworkIntake lifecycle and exact config validation;
- listener message-context ABI and lifetime;
- async-claim move/complete behavior;
- parser generation mapping;
- retained-frame empty-feed retry;
- pending claim/byte bounds;
- KCP rejection;
- assembly rollback and catalog lease lifetime.

### Level B: real local network integration tests

- TCP loopback peer emitting fragmented JT/T 808 frames;
- UDP loopback peer emitting CoAP frames;
- configured CNet Source Product owner loaded through the PluginHost catalog;
- selected real protocol DLL loaded through the same catalog snapshot;
- real memory or TurboDB Inbox;
- existing Inbox business-Graph driver;
- configured real CNet datagram Sink to UDP loopback peer A or B.

Mocks may observe counters or inject a provider error, but cannot replace the real network Source/Sink, protocol DLL, or selected Inbox implementation in the positive end-to-end gates.

### Level C: package/profile gates

- Debug/ASan full suite;
- Release full suite;
- fresh installed C and C++ consumers;
- DLL exports/dependencies/CRT checks;
- schema/package negative assertions for retired components and old ABI requests.

## 19. Compatibility and Migration

This remains an intentionally incompatible line of development under #117/#118 policy.

No old protocol-to-Graph runtime is restored. No legacy mapper is restored. No previous storage schema is migrated or auto-created. No C/CMake/runtime compatibility alias is added.

Two additive public contracts are introduced:

1. `turbo_flow_cnet_listener_message_context_t` plus its accessor.
2. `TurboFlow::ProtocolNetworkIntake` and its size/versioned owner API.

The listener payload remains accessed only through `message->payload`; private buffer-offset assumptions receive no compatibility layout.

Deployment replaces the complete validated binary/configuration set. A new ProtocolNetworkIntake must not run with stale protocol, CNet, PluginHost, or storage DLL generations.

## 20. Completion Boundary for #118

After the acceptance matrix is GREEN, #118 may mark its real two-protocol network requirement complete using JT/T 808/TCP and CoAP/UDP.

The issue must still avoid claiming:

- FlowMQ completion (#74);
- Flowie completion (#115);
- real RulesForge/TurboScript typed-operation DLL completion (#73);
- KCP/secure-KCP ProtocolNetworkIntake support;
- universal HTTP/WS/MQTT protocol coverage;
- end-to-end exactly-once semantics.

Those remain independent capabilities. #118 completion proves that the configured transport -> protocol -> durable Inbox -> shared business Graph -> configured Sink architecture works with two real protocols and two transport classes.