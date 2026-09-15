# Issue #118 Real Protocol Intake Completion Design

Date: 2026-09-15

Parent: #118

Related: #117, #73, #74, #115, #116, #124, PR #123, PR #125

## 1. Purpose

Complete the remaining network-facing acceptance of #118 without reopening the already-merged Inbox v2 design and without making unfinished RulesForge, FlowMQ, or Flowie providers prerequisites.

The accepted completion slice uses two real protocol sources with different transport classes:

- JT/T 808 over a real CNet TCP listener.
- CoAP over a real CNet UDP packet endpoint.

Both sources must pass through the same configured Inbox contract, the same existing Inbox-to-Graph driver, the same business Graph topology, and the same CNet datagram Sink provider type. Destination selection is explicit configuration and must not be derived from ingress protocol identity.

FlowMQ remains tracked by #74. Flowie remains tracked by #115. The real RulesForge/TurboScript DLL migration remains tracked by #73. None of those tasks is a prerequisite for this #118 completion slice.

## 2. Existing Facts This Design Reuses

The merged tree at base commit `ed4d51b7db575f4f59c95a583ff90c5538268ad9` already provides:

1. Inbox v2 with bounded memory and durable TurboDB/SQLite providers, exact schema preflight, generation ownership, takeover, replay/history semantics, and no database-to-memory fallback.
2. A provider-neutral Inbox-to-Graph driver from PR #123. Business Graph execution begins only after a record has been claimed from Inbox and settlement is explicit.
3. `ProtocolInbox` from PR #125. `turbo_flow_protocol_inbox_admit()` synchronously encodes a decoded protocol message into the durable `turbo-flow.protocol.inbox` v1 TBE envelope and admits it to the configured Inbox.
4. `ProtocolSource` ABI v1. A caller opens a protocol parser session, feeds transport bytes, and receives complete decoded frames through one `admit` callback. A complete frame is not consumed until that callback succeeds. Capacity rejection retains the current frame and requires an explicit empty-feed retry.
5. Independent protocol DLLs for MQTT-SN, CoAP, LwM2M, OCPP, GB/T 32960, and JT/T 808.
6. Real CNet TCP/TLS listener Source and UDP/KCP packet Source owners with bounded queues/resources and real loopback tests.
7. Real CNet stream/datagram/packet Sink owners with explicit native/protocol terminal boundaries.

The missing piece is composition between the real CNet transport owners and `ProtocolSource` while preserving both owners' lifecycle, capacity, generation, and failure contracts.

## 3. Goals

This change must:

- prove two real protocol network Sources can reach the same configured Inbox before any business Graph work begins;
- keep the existing Inbox v2 ABI/schema and `ProtocolInbox` envelope unchanged;
- keep protocol DLLs responsible only for framing/codec semantics;
- keep CNet responsible for connection/session/transport truth;
- keep Inbox responsible for record/claim/settlement truth;
- keep the existing Inbox driver responsible for starting and settling business execution;
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
- add a second mutable connection/session registry beside CNet;
- change Inbox v2 storage naming or schema;
- allow protocol identity to implicitly select a Sink destination.

## 5. Selected End-to-End Paths

### 5.1 JT/T 808 over TCP

The TCP path is:

`real TCP peer -> CNet listener Source -> intake plumbing Flow -> protocol-intake Sink -> JT/T 808 protocol DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> existing Inbox Graph driver -> shared business Graph -> CNet datagram Sink -> real UDP peer`

The listener Source remains the transport owner. Its CNet connection handle `{slot, generation}` is the authoritative connection identity.

### 5.2 CoAP over UDP

The UDP path is:

`real UDP peer -> CNet packet Source -> intake plumbing Flow -> protocol-intake Sink -> CoAP protocol DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> existing Inbox Graph driver -> shared business Graph -> CNet datagram Sink -> real UDP peer`

The packet Source remains the transport/session owner. Its message-owned `turbo_flow_cnet_packet_message_context_t` and generation-checked `cnet_packet_session` are authoritative transport identity.

### 5.3 Intake plumbing Flow is not the business Graph

CNet Sources already publish `turbo_flow_msg_t` through CFlow/TurboFlow Source machinery. This design reuses that machinery rather than adding a second callback API to CNet.

The pre-storage intake Flow is deliberately restricted to transport plumbing:

- exactly one real CNet Source;
- exactly one protocol-intake Sink;
- no business transform/operation;
- no business database capability;
- no business destination routing;
- no result Sink.

The #118 invariant is therefore **storage admission before business Graph execution**, not "no scheduler or CFlow primitive may run before storage." A test counter for the store-before-Graph invariant counts only the post-Inbox business Graph requested by the existing Inbox driver.

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

The public payload contract remains `message->payload`; consumers must not assume payload begins at offset zero of `message->buffer`.

## 7. Host-Owned Protocol-Intake Sink

Introduce one host-owned composition helper, `turbo_flow_protocol_transport_intake_t`, used to back a single Sink in the dedicated intake plumbing Flow. It is not a new protocol provider, transport provider, database, queue, or business Graph.

The intake owner contains:

- one retained/opened protocol provider instance selected by exact plugin ID/version;
- one `turbo_flow_protocol_source_t`;
- one `turbo_flow_protocol_inbox_t`;
- one fixed parser-session table sized exactly to configured transport capacity;
- one serialized Sink/progress lane;
- no network socket;
- no database implementation;
- no business Graph run;
- no business result Sink.

The Sink consumes the existing `turbo_flow_msg_t` emitted by CNet. No additional generic transport-message public ABI is introduced.

For stream input, the Sink obtains transport identity only through `turbo_flow_cnet_listener_message_context(message)`. For packet input, it obtains transport identity only through `turbo_flow_cnet_packet_message_context(message)`.

The helper may be public as an opaque lifecycle handle if required by the existing adapter/progress ownership pattern, but its message handoff remains the ordinary TurboFlow/CFlow Sink contract. There is no public `feed raw bytes` escape hatch that bypasses the configured real CNet Source in the installed integration path.

## 8. Parser-Session Mapping

The parser-session table is a bounded derived projection keyed by CNet's stable slot and generation. It is not a second mutable transport session registry.

For both listener and packet handles, slot zero-based identity is converted to a nonzero bridge key only for indexing/validation; the original CNet generation remains authoritative.

Rules:

1. The first message for a slot opens one `ProtocolSource` parser session using that CNet generation.
2. Repeated messages for the same slot and generation feed the same parser session.
3. If the same slot appears with a newer generation, the intake owner closes the old parser session before opening the new one.
4. A message with an older generation than the slot's retained generation is rejected before durable admission.
5. A generation change discards no accepted Inbox record; it only retires transport-local partial parser state that has not crossed the Inbox admission boundary.
6. Parser session identifiers are process-local intake counters and are never persisted as durable source identity.
7. The fixed parser-session table has the same slot bound as the configured transport owner. It cannot grow at runtime.
8. A transport connection/session close does not require a second terminal-event channel solely for this slice. A parser entry may remain allocated but inactive until that slot is reused or the intake owner shuts down. This retained state is bounded by transport slot capacity; reuse performs exact generation retirement before new bytes are fed.
9. Shutdown closes every retained parser session before destroying `ProtocolSource`.

This prevents a partial JT/T 808 frame from generation N from being completed by bytes belonging to generation N+1 after a TCP slot is reused.

## 9. Admission and Backpressure Semantics

The intake owner configures `ProtocolSource.ops.admit` to call `turbo_flow_protocol_inbox_admit()` and no other successful data-path callback.

A frame crosses the acceptance boundary only when the selected Inbox owns a complete independent copy.

### 9.1 ProtocolSource retained frame

If `ProtocolInbox` or Inbox reports a capacity failure, `ProtocolSource` retains the complete current frame. The intake Sink does not acknowledge successful consumption of the corresponding upstream message until the frame has either been admitted or a terminal failure has been selected.

The intake owner must not re-feed retained frame bytes. It retries only through the documented empty-feed call until admission succeeds or an explicit terminal action ends the intake operation.

A focused test must prove one retained frame becomes exactly one Inbox record after capacity becomes available.

### 9.2 TCP backpressure

For the TCP listener path, when the intake Sink cannot accept another CFlow value, downstream demand remains withheld. The CNet listener Source therefore stops registering new application receives for this intake Flow until capacity returns.

Transport close/error progress remains owned by the CNet listener contract. The intake owner never polls or peeks CNet behind that owner.

### 9.3 UDP backpressure

For the UDP packet path, protocol/transport progress cannot be globally demand-gated because KCP/secure-KCP control traffic may require polling even with zero downstream demand. The CNet packet owner therefore continues its own transport progress while the intake Flow withholds value demand.

The packet Source's fixed queue remains the bounded handoff. If that queue is exhausted, its existing capacity/terminal behavior applies. The intake owner adds no unbounded spill queue.

### 9.4 Zero and bounded capacities

A configured parser-session capacity of zero fails preflight before network side effects. Capacity one, N, and N+1 are exercised at runtime. Existing Inbox provider validation remains authoritative for record, byte, single-record, and in-flight-claim bounds; the intake layer does not reinterpret those limits.

### 9.5 Durable failure

When TurboDB Inbox admission/commit fails:

- the business Graph is not started for that record;
- the record is not silently admitted to memory;
- the protocol intake does not bypass Inbox;
- the failure remains observable as an intake/storage failure;
- no implicit retry is performed unless a caller explicitly invokes a supported retry operation.

## 10. Identity and Durable Envelope Rules

`ProtocolInbox` continues to produce the existing `turbo-flow.protocol.inbox` v1 TBE envelope.

The durable identity resolver derives `source_id`, `admission_id`, `correlation`, `source_sequence`, and timestamp from configured source identity plus protocol/application metadata. It does not persist:

- C pointers;
- DLL addresses;
- CNet connection/session structs as raw bytes;
- process-local parser session ids;
- borrowed network buffers.

CNet generation values participate in in-process stale-event rejection but are not sufficient as durable replay identity by themselves.

JT/T 808 and CoAP tests verify equivalent memory/TurboDB durable envelope semantics for the same normalized protocol input.

## 11. Business Graph Boundary

No network owner or intake plumbing Flow executes business logic.

The only path to business Graph execution is:

1. protocol frame decoded;
2. `ProtocolInbox` admission succeeds;
3. Inbox record becomes claimable;
4. the existing provider-neutral Inbox Source/driver claims the record;
5. the shared business Graph is requested;
6. terminal business Graph outcome settles the original Inbox claim according to the existing driver contract.

A focused test holds or fails Inbox admission and asserts the post-Inbox business Graph invocation counter remains zero.

The business Graph used by this #118 acceptance is the existing CFlow graph contract. It is not evidence that #73 real RulesForge/TurboScript DLL migration is complete.

## 12. Sink and Destination Semantics

Both ingress protocols use the same CNet datagram Sink provider type and the same business Graph topology.

Two explicit destination configurations are required:

- configuration A sends the business result to real UDP loopback peer A;
- configuration B sends the same business result to real UDP loopback peer B.

The output destination comes from explicit Sink/business binding. Changing ingress from JT/T 808 to CoAP does not change the selected destination when business input and destination configuration are otherwise the same.

Protocol reply sinks, if separately tested, remain distinct from independent business destination sinks. A reply-specific session/generation is never generalized into ordinary result routing.

## 13. Lifecycle and Ownership

Preflight/startup order:

1. validate exact configuration and capability versions with no network side effects;
2. resolve/load the selected protocol DLL and retain its module lease;
3. bind the selected Inbox provider;
4. create `ProtocolInbox`;
5. create `ProtocolSource`;
6. create the fixed parser-session table;
7. assemble the dedicated intake plumbing Flow with the protocol-intake Sink;
8. open/start the real CNet Source owner against that intake Flow;
9. publish the assembled generation only after every required owner is valid.

Failure during assembly unwinds in reverse order and publishes no partial generation.

Shutdown order:

1. close new intake Sink admission/demand;
2. request the real CNet Source owner's documented stop/drain behavior so no new application values can enter;
3. finish or explicitly terminate any currently retained complete `ProtocolSource` frame without feeding new transport bytes;
4. close every retained parser session;
5. destroy `ProtocolSource`;
6. destroy `ProtocolInbox`;
7. tear down the intake plumbing Flow/Sink owner;
8. release protocol/module leases after all callbacks using them are gone;
9. destroy the stopped CNet Source owner according to its own lifecycle contract.

If existing CFlow ownership requires the intake Flow to outlive the stopped CNet owner handle, implementation must order only the final handle destruction accordingly; it must not release protocol callbacks or Inbox ownership while an intake callback can still run.

Any live parser session, retained frame, Inbox claim, business Graph run, Sink terminal operation, or module callback that requires a DLL keeps the corresponding generation/module lease alive. Unload while such work exists returns busy/fails closed; it never invalidates borrowed callbacks.

## 14. Configuration Rules

Configuration explicitly selects:

- real transport Source owner/resource;
- protocol DLL provider ID and exact supported version;
- protocol kind/version;
- Inbox provider (`memory` or `turbodb`) and its resource;
- bounded transport/protocol session capacity;
- maximum protocol frame bytes;
- Inbox record/byte/in-flight limits already defined by Inbox v2;
- shared business Graph binding;
- CNet datagram Sink provider/resource;
- explicit UDP destination.

The installed integration path must materialize these through the existing host catalog/generation/configuration assembly. It must not hard-link a protocol DLL into Gateway/Core merely to satisfy this test.

Invalid or unsupported combinations fail preflight. There is no protocol fallback, transport fallback, database fallback, old DLL fallback, static engine fallback, or CMake-selected runtime fallback.

## 15. RED -> GREEN Acceptance Matrix

Implementation begins with focused RED tests proving current master lacks the required real transport-to-ProtocolSource composition.

Required GREEN coverage:

1. **JT/T 808 TCP real peer**: fragmented real TCP traffic crosses listener Source -> intake Sink -> JT/T 808 DLL -> ProtocolSource -> ProtocolInbox -> Inbox -> business Graph -> real CNet datagram Sink.
2. **CoAP UDP real peer**: real UDP datagram crosses packet Source -> intake Sink -> CoAP DLL -> ProtocolSource -> ProtocolInbox -> Inbox -> the same business Graph -> the same CNet datagram Sink provider type.
3. **Store before business Graph**: hold/fail Inbox admission and assert business Graph invocation count is zero.
4. **No database fallback**: force TurboDB commit failure and assert business Graph count is zero and memory-provider admission count is zero.
5. **Capacity 0/1/N/N+1**: zero parser-session capacity fails before network open; runtime covers one/N/N+1 transport slots plus existing Inbox record/byte/in-flight limits.
6. **Exactly one admission after retry**: a frame retained because of Inbox capacity is admitted exactly once after space becomes available.
7. **TCP generation reuse**: feed a partial JT/T 808 frame on generation N, reuse the listener slot with generation N+1, and prove bytes from N cannot complete a frame in N+1.
8. **Stale packet generation**: a stale CoAP packet session generation is rejected before durable admission.
9. **Business Graph failure**: failure settles through the existing Inbox driver contract and does not trigger implicit replay.
10. **Send outcome separation**: CNet datagram Sink admission/native terminal and any protocol acknowledgement remain distinct observations.
11. **Cancel/drain**: accepted Inbox ownership is either drained or explicitly cancelled/failed through existing contracts; no accepted record disappears.
12. **Unload lease**: active intake/parser/claim/run/send work prevents provider unload; release becomes possible after terminal cleanup.
13. **Memory/TurboDB parity**: both providers run the same network behavior assertions except durability-specific recovery expectations.
14. **TurboDB recovery**: after committed admission and restart/takeover under the existing provider contract, the record is claimable and is not duplicated into memory.
15. **Different destinations**: the same business result is observed at UDP destination A and destination B in separate configurations; ingress protocol does not choose the destination.
16. **Installed package**: C and C++ installed consumers configure/load required public components without private build-tree linkage.
17. **DLL gates**: canonical exports, dependency closure, CRT/profile checks, and no retired protocol Graph/runtime artifact.
18. **Debug/ASan and Release**: focused tests, affected adjacent tests, full CTest, install consumer, formatting, and `git diff --check` all pass.

## 16. Test Architecture

Keep three levels separate so no fixture is presented as a real network gate.

### Level A: focused contract tests

- listener message context exact ABI and lifetime;
- intake parser-session generation behavior;
- retained-frame retry;
- zero/one/N/N+1 capacity behavior;
- stale-event errors;
- assembly rollback and unload lease.

### Level B: real local network integration tests

- TCP loopback peer emitting fragmented JT/T 808 frames;
- UDP loopback peer emitting CoAP frames;
- real CNet Source owner on ingress;
- real selected protocol DLL loaded through the catalog;
- real memory or TurboDB Inbox;
- existing Inbox business-Graph driver;
- real CNet datagram Sink to UDP loopback peer A or B.

Mocks may observe counters but cannot replace the network Source/Sink, protocol DLL, or selected Inbox implementation in these tests.

### Level C: package/profile gates

- Debug/ASan full suite;
- Release full suite;
- fresh installed C/C++ consumers;
- DLL exports/dependencies/CRT checks;
- schema/package negative assertions for retired components and old ABI requests.

## 17. Compatibility and Migration

This is an intentionally incompatible line of development under #117/#118 policy.

No old protocol-to-Graph runtime is restored. No legacy source/sink mapper is restored. No previous storage schema is migrated or auto-created. No C/CMake/runtime compatibility alias is added.

The listener message context is additive to the current CNet listener Source ABI, but consumers that rely on payload starting at buffer offset zero must migrate to the documented `message->payload` view. No compatibility layout is provided for consumers inspecting private buffer internals.

Deployment replaces the complete validated binary/configuration set. It must not mix a new intake composition with stale protocol or storage DLL generations.

## 18. Completion Boundary for #118

After the acceptance matrix above is GREEN, #118 may mark its real two-protocol network requirement complete using JT/T 808/TCP and CoAP/UDP.

The issue must still avoid claiming:

- FlowMQ completion (#74);
- Flowie completion (#115);
- real RulesForge/TurboScript typed-operation DLL completion (#73);
- universal HTTP/WS/MQTT protocol coverage;
- end-to-end exactly-once semantics.

Those remain independent capabilities. #118 completion proves the shared intake-storage architecture works with two real protocols and two transport classes, not that every planned provider has been implemented.
