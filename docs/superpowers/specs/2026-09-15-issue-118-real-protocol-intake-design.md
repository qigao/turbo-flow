# Issue #118 Real Protocol Intake Completion Design

Date: 2026-09-15

Parent: #118

Related: #117, #73, #74, #115, #116, #124, PR #123, PR #125

## 1. Purpose

Complete the remaining network-facing acceptance of #118 without reopening the already-merged Inbox v2 design and without making unfinished RulesForge, FlowMQ, or Flowie providers prerequisites.

The accepted completion slice uses two real protocol sources with different transport classes:

- JT/T 808 over a real CNet TCP listener.
- CoAP over a real CNet UDP packet endpoint.

Both sources must pass through the same configured Inbox contract, the same existing Inbox-to-Graph driver, the same Graph topology, and the same Sink provider type. Destination selection is explicit configuration and must not be derived from ingress protocol identity.

FlowMQ remains tracked by #74. Flowie remains tracked by #115. The real RulesForge/TurboScript DLL migration remains tracked by #73. None of those tasks is a prerequisite for this #118 completion slice.

## 2. Existing Facts This Design Reuses

The merged tree at base commit `ed4d51b7db575f4f59c95a583ff90c5538268ad9` already provides:

1. Inbox v2 with bounded memory and durable TurboDB/SQLite providers, exact schema preflight, generation ownership, takeover, replay/history semantics, and no database-to-memory fallback.
2. A provider-neutral Inbox-to-Graph driver from PR #123. Graph execution begins only after a record has been claimed from Inbox and settlement is explicit.
3. `ProtocolInbox` from PR #125. `turbo_flow_protocol_inbox_admit()` synchronously encodes a decoded protocol message into the durable `turbo-flow.protocol.inbox` v1 TBE envelope and admits it to the configured Inbox.
4. `ProtocolSource` ABI v1. A caller opens a protocol parser session, feeds transport bytes, and receives complete decoded frames through one `admit` callback. A complete frame is not consumed until that callback succeeds. Capacity rejection retains the current frame and requires an explicit empty-feed retry.
5. Independent protocol DLLs for MQTT-SN, CoAP, LwM2M, OCPP, GB/T 32960, and JT/T 808.
6. Real CNet TCP/TLS listener Source and UDP/KCP packet Source owners with bounded queues/resources and real loopback tests.
7. Real CNet stream/datagram/packet Sink owners with explicit native/protocol terminal boundaries.

The missing piece is composition between the real CNet transport owners and `ProtocolSource` while preserving both owners' lifecycle, capacity, generation, and failure contracts.

## 3. Goals

This change must:

- prove two real protocol network Sources can reach the same configured Inbox before any Graph work begins;
- keep the existing Inbox v2 ABI/schema and `ProtocolInbox` envelope unchanged;
- keep protocol DLLs responsible only for framing/codec semantics;
- keep CNet responsible for connection/session/transport truth;
- keep Inbox responsible for record/claim/settlement truth;
- keep the Graph driver responsible for starting and settling business execution;
- preserve explicit destination configuration at the Sink boundary;
- preserve bounded memory and backpressure at every handoff;
- preserve exact generation identity across connection/session reuse;
- fail closed when durable admission fails;
- add no compatibility shim, no alternate in-process path, and no runtime fallback.

## 4. Non-Goals

This slice does not:

- implement or complete RulesForge/TurboScript typed-operation DLL execution from #73;
- implement FlowMQ Source/Sink from #74;
- implement Flowie Source/Sink from #115;
- promise end-to-end exactly-once delivery;
- delay native protocol ACK until business Graph completion unless the native protocol owner already requires that behavior;
- add automatic retries for Graph, Sink, network, or database failures;
- migrate legacy data/configuration/ABI;
- add a second connection/session registry beside CNet;
- change Inbox v2 storage naming or schema;
- allow protocol identity to implicitly select a Sink destination.

## 5. Selected End-to-End Paths

### 5.1 JT/T 808 over TCP

The TCP path is:

`real TCP peer -> CNet listener Source -> transport/protocol intake bridge -> JT/T 808 protocol DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> existing Inbox Graph driver -> shared Graph -> configured CNet Sink -> real peer`

The listener Source remains the transport owner. Its CNet connection handle `{slot, generation}` is the authoritative connection identity.

### 5.2 CoAP over UDP

The UDP path is:

`real UDP peer -> CNet packet Source -> transport/protocol intake bridge -> CoAP protocol DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> existing Inbox Graph driver -> shared Graph -> configured CNet Sink -> real peer`

The packet Source remains the transport/session owner. Its message-owned `turbo_flow_cnet_packet_message_context_t` and generation-checked `cnet_packet_session` are authoritative transport identity.

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

The listener Source allocates one buffer large enough for the context plus payload, stores the context at the front, and points `message->payload` at the bytes after the context. The accessor succeeds only when the context is message-owned, exact-versioned, and structurally valid.

This is a projection only. It does not create a second mutable connection table. CNet remains the only source of connection state.

A stale listener message retains the generation captured when the network callback copied the bytes. Slot reuse therefore cannot cause old bytes to be parsed under a newer connection generation.

## 7. Generic Transport-to-Protocol Intake Bridge

Introduce one provider-neutral composition owner rather than one adapter per protocol.

Suggested public name:

```c
turbo_flow_protocol_transport_intake_t
```

The bridge owns:

- one retained/opened protocol provider instance selected by exact plugin ID/version;
- one `turbo_flow_protocol_source_t`;
- one `turbo_flow_protocol_inbox_t`;
- a bounded table of parser sessions keyed by transport identity;
- one serialized progress lane;
- no network socket, no database, no Graph run, and no Sink.

The bridge borrows or retains host-owned resources only through their public contracts. It never calls a protocol plugin's internal symbols and never accesses a CNet owner's internal state.

### 7.1 Transport events accepted by the bridge

The bridge accepts two exact transport event shapes:

```c
typedef enum turbo_flow_protocol_transport_kind_e {
  TURBO_FLOW_PROTOCOL_TRANSPORT_STREAM = 1,
  TURBO_FLOW_PROTOCOL_TRANSPORT_PACKET = 2
} turbo_flow_protocol_transport_kind_t;

typedef struct turbo_flow_protocol_transport_message_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_transport_kind_t transport_kind;
  uint64_t owner_id;
  uint64_t generation;
  const char *device_id;
  const uint8_t *data;
  size_t data_size;
} turbo_flow_protocol_transport_message_t;
```

`owner_id` is the stable transport-local slot/session identity converted to a bridge-local scalar. `generation` must be nonzero and must come from the authoritative CNet handle. `device_id` is optional only for protocol DLLs that derive a stable identity from the frame itself.

The implementation may use a private internal event representation if existing CFlow message APIs are sufficient; the public ABI must expose no raw pointer or DLL address in durable state.

### 7.2 Parser-session mapping

For every live transport identity, the bridge maintains at most one `ProtocolSource` parser session.

Rules:

1. First message for `{owner_id, generation}` lazily opens one parser session.
2. Repeated messages for the same pair feed the same parser session.
3. If the same `owner_id` appears with a newer generation, the bridge closes the previous parser session before opening the new one.
4. A message with an older/stale generation is rejected and never admitted to Inbox.
5. Parser session identifiers are bridge-owned counters and are not persisted as durable source identity.
6. Shutdown closes all live parser sessions before destroying the `ProtocolSource`.

This prevents a partial JT/T 808 frame from one TCP connection from being completed by bytes belonging to a later connection reusing the same slot.

## 8. Admission and Backpressure Semantics

The bridge configures `ProtocolSource.ops.admit` to call `turbo_flow_protocol_inbox_admit()` and no other successful data-path callback.

A frame crosses the acceptance boundary only when the selected Inbox owns a complete independent copy.

### 8.1 ProtocolSource retained frame

If `ProtocolInbox` or Inbox reports a capacity failure, `ProtocolSource` retains the complete current frame. The bridge must not re-feed those bytes. It retries only through the documented empty-feed call until admission succeeds or a terminal failure is selected by policy.

The test suite must prove one retained frame becomes exactly one Inbox record after capacity becomes available.

### 8.2 TCP backpressure

For the TCP listener path, once the bridge cannot accept more protocol work, it stops requesting new listener Source demand. CNet therefore does not admit another application receive into this bridge until capacity returns.

Transport close/error progress remains owned by the CNet listener contract. The bridge must not poll or peek CNet behind the listener owner.

### 8.3 UDP backpressure

For the UDP packet path, protocol/transport progress cannot be globally demand-gated because KCP/secure-KCP control traffic may require polling even with zero Graph demand. The CNet packet owner therefore continues its own transport progress while bridge demand is stopped.

The packet Source's fixed queue is the bounded handoff. If that queue is exhausted, its existing terminal capacity behavior applies. The bridge must not add an unbounded spill queue.

### 8.4 Durable failure

When TurboDB Inbox admission/commit fails:

- the Graph is not started for that record;
- the record is not silently admitted to memory;
- the protocol intake does not bypass Inbox;
- the failure remains observable as an intake/storage failure;
- no implicit retry is performed unless a caller explicitly invokes a supported retry operation.

## 9. Identity and Durable Envelope Rules

`ProtocolInbox` continues to produce the existing `turbo-flow.protocol.inbox` v1 TBE envelope.

The durable identity resolver must derive `source_id`, `admission_id`, `correlation`, `source_sequence`, and timestamp from configured source identity plus protocol/application metadata. It must not persist:

- C pointers;
- DLL addresses;
- CNet connection/session structs as raw bytes;
- process-local parser session ids;
- borrowed network buffers.

CNet generation values may participate in in-process stale-event rejection but are not sufficient as the durable replay identity by themselves.

JT/T 808 and CoAP tests must verify equivalent memory/TurboDB durable envelope semantics for the same normalized protocol input.

## 10. Graph Boundary

No network owner executes the business Graph directly.

The only path to Graph execution is:

1. protocol frame decoded;
2. `ProtocolInbox` admission succeeds;
3. Inbox record becomes claimable;
4. the existing provider-neutral Inbox Source/driver claims the record;
5. the shared Graph is requested;
6. terminal Graph outcome settles the original Inbox claim according to the existing driver contract.

A focused test must hold Inbox admission before commit and assert the Graph invocation counter remains zero.

The Graph used by this #118 acceptance is the existing CFlow graph contract. It is not evidence that #73 real RulesForge/TurboScript DLL migration is complete.

## 11. Sink and Destination Semantics

Both ingress protocols must use the same Sink provider type and Graph topology.

Run at least two explicit destination configurations. For example:

- configuration A sends the business result to UDP loopback destination A;
- configuration B sends the same business result to UDP loopback destination B.

The output destination is read from explicit configured business/Sink binding. Changing ingress from JT/T 808 to CoAP must not change the selected destination when business input and destination configuration are otherwise the same.

Protocol reply sinks, if tested, remain separate from independent business destination sinks. A reply-specific session/generation is not generalized into ordinary result routing.

## 12. Lifecycle and Ownership

Startup order:

1. validate exact configuration and capability versions with no network side effects;
2. resolve/load the selected protocol DLL and retain its module lease;
3. bind the selected Inbox provider;
4. create `ProtocolInbox`;
5. create `ProtocolSource`;
6. create the transport/protocol bridge session table;
7. start/open the real CNet Source owner;
8. publish the assembled generation only after every required owner is valid.

Failure during assembly unwinds in reverse order and publishes no partial generation.

Shutdown order:

1. stop new bridge demand/admission;
2. stop new network application delivery while allowing native owner drain semantics;
3. settle or cancel any bridge-retained complete protocol frame explicitly;
4. close all parser sessions;
5. destroy `ProtocolSource`;
6. destroy `ProtocolInbox`;
7. release protocol/module leases;
8. release transport owner after its own stop/drain contract completes.

Any live parser session, retained frame, Inbox claim, Graph run, Sink terminal operation, or module callback that requires a DLL must keep the corresponding generation/module lease alive. Unload while such work exists returns busy/fails closed; it never invalidates borrowed callbacks.

## 13. Configuration Rules

Configuration must explicitly select:

- transport owner/resource;
- protocol DLL provider ID and exact supported version;
- protocol kind/version;
- Inbox provider (`memory` or `turbodb`) and its resource;
- bounded transport/protocol session capacity;
- maximum protocol frame bytes;
- Inbox record/byte/in-flight limits already defined by Inbox v2;
- shared Graph binding;
- Sink provider/resource;
- explicit destination.

Invalid or unsupported combinations fail preflight. There is no protocol fallback, transport fallback, database fallback, old DLL fallback, static engine fallback, or CMake-selected runtime fallback.

## 14. RED -> GREEN Acceptance Matrix

The implementation must begin with focused RED tests that prove the current master lacks the required composition.

Required GREEN coverage:

1. **JT/T 808 TCP real peer**: fragmented real TCP traffic crosses listener Source -> bridge -> JT/T 808 DLL -> ProtocolSource -> ProtocolInbox -> Inbox -> Graph -> real Sink.
2. **CoAP UDP real peer**: real UDP datagram crosses packet Source -> bridge -> CoAP DLL -> ProtocolSource -> ProtocolInbox -> Inbox -> the same Graph -> the same Sink provider type.
3. **Store before Graph**: hold/fail Inbox admission and assert Graph invocation count is zero.
4. **No database fallback**: force TurboDB commit failure and assert Graph count is zero and memory-provider admission count is zero.
5. **Capacity 0/1/N/N+1**: cover bridge parser-session capacity, retained complete frame, Inbox record capacity, total bytes, and in-flight claims where applicable.
6. **Exactly one admission after retry**: a frame retained because of Inbox capacity is admitted exactly once after space becomes available.
7. **TCP generation reuse**: feed a partial JT/T 808 frame on generation N, reuse the slot with generation N+1, and prove bytes from N cannot complete a frame in N+1.
8. **Stale packet generation**: a stale CoAP packet session generation is rejected before durable admission.
9. **Graph failure**: failure settles through the existing Inbox driver contract and does not trigger implicit replay.
10. **Send outcome separation**: Sink admission, send terminal, and any protocol acknowledgement remain distinct observations.
11. **Cancel/drain**: accepted ownership is either drained or explicitly cancelled; no accepted record disappears.
12. **Unload lease**: active bridge/parser/claim/run/send work prevents provider unload; release becomes possible after terminal cleanup.
13. **Memory/TurboDB parity**: both providers run the same network behavior assertions except durability-specific crash recovery expectations.
14. **TurboDB recovery**: after committed admission and process/provider restart simulation, the record is claimable under the existing recovery/takeover contract and is not duplicated into memory.
15. **Different destinations**: the same business result is observed at two different explicit destinations in two configurations; ingress protocol does not choose the destination.
16. **Installed package**: C and C++ installed consumers configure and load the required public components without private build-tree linkage.
17. **DLL gates**: unique canonical exports, dependency closure, CRT/profile checks, and no retired protocol Graph/runtime artifact.
18. **Debug/ASan and Release**: focused tests, affected adjacent tests, full CTest, install consumer, formatting, and `git diff --check` all pass.

## 15. Test Architecture

Keep three levels separate so no fixture is presented as a real network gate.

### Level A: focused contract tests

- listener message context exact ABI and lifetime;
- bridge parser-session generation behavior;
- retained-frame retry;
- capacity and stale-event errors;
- assembly rollback and unload lease.

### Level B: real local network integration tests

- TCP loopback peer emitting fragmented JT/T 808 frames;
- UDP loopback peer emitting CoAP frames;
- real CNet Source owner on ingress;
- real selected protocol DLL loaded through the catalog;
- real memory or TurboDB Inbox;
- existing Inbox Graph driver;
- real CNet Sink to a loopback peer.

Mocks may observe counters but cannot replace the network Source/Sink, protocol DLL, or selected Inbox implementation in these tests.

### Level C: package/profile gates

- Debug/ASan full suite;
- Release full suite;
- fresh installed C/C++ consumers;
- DLL exports/dependencies/CRT checks;
- schema/package negative assertions for retired components and old ABI requests.

## 16. Compatibility and Migration

This is an intentionally incompatible line of development under #117/#118 policy.

No old protocol-to-Graph runtime is restored. No legacy source/sink mapper is restored. No previous storage schema is migrated or auto-created. No C/CMake/runtime compatibility alias is added.

The listener message context is additive to the current CNet listener Source ABI, but consumers that rely on payload starting at buffer offset zero must continue to use `message->payload` rather than inspect `message->buffer` internals. The documented public payload view remains unchanged.

Deployment replaces the complete validated binary/configuration set. It must not mix a new bridge with stale protocol or storage DLL generations.

## 17. Completion Boundary for #118

After the acceptance matrix above is GREEN, #118 may mark its real two-protocol network requirement complete using JT/T 808/TCP and CoAP/UDP.

The issue must still avoid claiming:

- FlowMQ completion (#74);
- Flowie completion (#115);
- real RulesForge/TurboScript typed-operation DLL completion (#73);
- universal HTTP/WS/MQTT protocol coverage;
- end-to-end exactly-once semantics.

Those remain independent capabilities. #118 completion proves the shared intake-storage architecture works with two real protocols and two transport classes, not that every planned provider has been implemented.
