# Issue #118 Real Protocol Intake Completion Design

Date: 2026-09-15

Parent: #118

Related: #117, #73, #74, #115, #116, #124, PR #119, PR #123, PR #125

## 1. Purpose

Complete #118's remaining network-facing acceptance without reopening Inbox v2 and without making RulesForge #73, FlowMQ #74, or Flowie #115 prerequisites.

The accepted completion slice is:

- JT/T 808 over a real configured CNet TCP listener.
- CoAP over a real configured CNet UDP packet Source.

Both paths use the same configured Inbox contract, the existing Inbox-to-Graph driver, one shared business Graph topology, and the same CNet datagram Sink provider type. Business destination selection comes only from explicit Sink configuration.

## 2. Existing contracts reused

The base architecture already provides:

1. Inbox v2 with bounded memory and durable TurboDB/SQLite providers, generation ownership, replay/history semantics, and no database-to-memory fallback.
2. PR #123's provider-neutral `turbo_flow_inbox_source_t`: business Graph work starts only after an Inbox claim and settlement is explicit.
3. PR #125's `ProtocolInbox`: decoded messages are encoded into the existing `turbo-flow.protocol.inbox` v1 TBE envelope and admitted to the configured Inbox.
4. `ProtocolSource` ABI v1: framing is session-scoped; a complete frame is consumed only after the admit callback succeeds; `SALTS_EBUSY`, `SALTS_ENOSPC`, and `SALTS_ENOBUFS` retain the complete frame and require an explicit empty-feed retry.
5. Independent protocol DLLs including CoAP and JT/T 808.
6. PR #119 exact-ID/version/absolute-path plugin loading, retained catalog snapshots, protocol registries, and transactional Product providers.
7. Real CNet listener/packet Sources and real CNet datagram Sink owners.
8. Managed async-terminal claims that can retain one publication until an external terminal outcome is known.

The missing production capability is one installed owner above Graph/CNet/Protocol/PluginHost that composes a configured CNet Source provider with a configured protocol provider while preserving storage-first admission, bounded backpressure, generation identity, and module lifetimes.

## 3. Goals

The change must:

- expose an installed `TurboFlow::ProtocolNetworkIntake` owner, not a test-only bridge;
- keep Inbox v2 ABI/schema and the ProtocolInbox envelope unchanged;
- keep protocol DLLs responsible for codec/framing only;
- keep CNet as the only transport connection/session fact source;
- keep Inbox as the only admission/claim/settlement fact source;
- keep business Graph execution behind `turbo_flow_inbox_source_t`;
- preserve bounded state at every handoff;
- preserve CNet slot/generation identity across slot reuse;
- retain catalog/module ownership until every callback terminates;
- provide observable Source-poll and bound-endpoint state for deterministic acceptance tests and operations;
- fail closed with no provider/runtime fallback.

## 4. Non-goals

This slice does not:

- complete RulesForge/TurboScript DLL execution (#73);
- complete FlowMQ (#74) or Flowie (#115);
- support KCP/secure-KCP in ProtocolNetworkIntake v1;
- promise end-to-end exactly-once delivery;
- add automatic business Graph/Sink/network/non-capacity-storage retries;
- add a second mutable transport registry;
- change Inbox v2 schema/naming;
- extend PluginHost root ABI or generic Product-owner ABI;
- add a raw-feed public API or static provider fallback.

KCP remains available in CNet. ProtocolNetworkIntake v1 rejects it because Inbox backpressure pauses Source `poll()`, while KCP needs independent timer/control progress. A future KCP intake needs a separate transport-progress lane.

## 5. End-to-end paths

### 5.1 JT/T 808 / TCP

`real TCP peer -> configured CNet listener Source DLL -> intake Flow -> ProtocolNetworkIntake async Sink -> JT/T 808 DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> InboxSource -> shared business Graph -> configured CNet datagram Sink -> real UDP peer`

The CNet listener `cnet_connection {slot,generation}` is the authoritative transport identity.

### 5.2 CoAP / UDP

`real UDP peer -> configured CNet packet Source DLL (packet_mode=udp) -> intake Flow -> ProtocolNetworkIntake async Sink -> CoAP DLL -> ProtocolSource -> ProtocolInbox -> configured Inbox -> InboxSource -> shared business Graph -> configured CNet datagram Sink -> real UDP peer`

The packet message's generation-checked `cnet_packet_session` and immutable session info are authoritative transport identity.

### 5.3 Intake Flow is not the business Graph

The pre-storage Flow is exactly one configured CNet Source, one terminal `protocol.intake` Sink, and one direct edge. It contains no business operation, business DB capability, destination routing, or result Sink.

The invariant is **Inbox admission before business Graph execution**. Scheduler/CFlow plumbing may run before storage; business logic may not.

## 6. CNet listener message identity

Add one message-owned projection:

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

The listener buffer becomes `[context][payload]`; `message->transport_context` points to the context and `message->payload` points after it. The accessor validates that both views belong to the same message-owned buffer and that the captured connection generation is nonzero.

This is a read-only projection, not a second connection table. Consumers continue to use `message->payload`, never buffer offset zero.

## 7. CNet configured-source connection snapshot

The existing generic `turbo_flow_adapter_connection_snapshot_at()` seam must become usable for configured CNet listener and packet Sources. Their adapter registration sets `ops.connection_snapshot`.

For the two #118 Source kinds the returned endpoint is canonical and uses the actual bound port after start:

- listener: `tcp://<numeric-host>:<bound-port>`;
- UDP packet Source: `udp://<numeric-host>:<bound-port>`.

Before native start, a configured zero port may appear as port zero. After successful `ProtocolNetworkIntake.start()`, `ProtocolNetworkIntake.snapshot().source_endpoint` must contain the actual nonzero bound endpoint. No private CNet handle is exposed.

## 8. Installed ProtocolNetworkIntake owner

`TurboFlow::ProtocolNetworkIntake` is an ordinary installed shared library above Graph, PluginHost, CNetAdapter, ProtocolIngress, and ProtocolIngressInbox. Lower layers do not depend on it.

### 8.1 Public API

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
  uint64_t source_polls;
  int backpressured;
  char source_endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
} turbo_flow_protocol_network_intake_snapshot_t;

#define TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT \
  {sizeof(turbo_flow_protocol_network_intake_snapshot_t), \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION, \
   TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED, SALTS_OK, \
   0u, 0u, 0u, 0u, 0u, 0, {0}}

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

### 8.2 Ownership

`resolved` is borrowed only during create/preflight; required values are copied. The caller-owned Inbox is borrowed for the owner's complete lifetime and is never closed/destroyed/substituted by ProtocolNetworkIntake.

`create()` retains the supplied catalog snapshot. The protocol registry retains its own snapshot ownership. Every retained catalog/module reference survives until the configured CNet Source Product owner and protocol owner/registry have been destroyed.

`intake_flow_io` contains a parsed uncompiled two-stage Flow. Preflight failure leaves it caller-owned. Immediately before the first registration/materialization side effect, create moves it by setting `*intake_flow_io = NULL`; from that point cleanup is owned by the create path.

## 9. Exact `protocol.intake` configuration

The adapter named by `intake_adapter_name` has kind `protocol.intake` and exactly:

```yaml
schema_version: 1
protocol_provider: jtt808 | coap
protocol_kind: jtt808 | coap
protocol_version: <exact-version>
source_id: <stable-source-id>
max_sessions: <positive-bound>
max_frame_size: <positive-bound>
max_pending_claims: <positive-bound>
max_pending_bytes: <positive-bound>
```

Unknown/missing fields fail preflight.

Only two v1 pairings are accepted:

- `jtt808`, `2019-A1`, `cnet.listener_source`;
- `coap`, `RFC7252`, `cnet.packet_source` with `packet_mode=udp`.

`max_sessions` cannot exceed CNet connection/session capacity. `max_frame_size` cannot exceed CNet `max_message_bytes` or the opened protocol's limit.

One CNet Source poll performs at most two bounded Scheduler passes. Therefore preflight requires overflow-safe:

- `max_pending_claims >= 2 * scheduler_max_steps_per_poll`;
- `max_pending_bytes >= max_pending_claims * source.max_message_bytes`.

The fixed storage is allocated before start and never grows.

## 10. Catalog/provider assembly

Using the same retained snapshot:

1. project the configured Source adapter from resolved config;
2. obtain the transactional Product catalog;
3. find exactly one provider whose `kind` equals the Source adapter kind;
4. run provider preflight before any side effect;
5. after Flow ownership transfer, materialize it into the intake Flow and retain its complete Product owner vtable;
6. require control-thread ownership plus `EXTERNAL_POLL` and a non-NULL `poll` callback;
7. create a protocol registry from the snapshot;
8. create one exact named protocol owner using the configured kind/version/frame bound;
9. borrow the provider-neutral `turbo_flow_protocol_t` only while that owner lives;
10. create/register the built-in managed async-terminal `protocol.intake` Sink and compile the Flow.

No protocol DLL is hard-linked into the high-level owner and no second DLL loader is created.

## 11. Parser-session mapping

A fixed table is keyed by CNet slot. Each entry stores current generation, last TurboFlow message ID, and process-local ProtocolSource session ID.

Rules:

1. first message for a slot opens one parser session;
2. same slot/generation feeds the same parser;
3. a different generation with a later TurboFlow message ID closes old parser state before opening new state;
4. a different generation with a non-increasing message ID is stale and fails `SALTS_EPROTO` before durable admission;
5. generation change retires only unadmitted partial parser bytes;
6. parser IDs/generations never enter durable identity;
7. table size equals configured `max_sessions` and never grows;
8. shutdown force-closes every retained parser session.

JT/T 808 leaves session `device_id` empty because the frame contains stable device identity. CoAP derives session device ID from the immutable packet peer as canonical numeric ASCII, never by persisting a raw CNet struct.

## 12. Managed async-terminal intake Sink

Every accepted Sink submission moves its `turbo_flow_async_terminal_claim_t` before returning `SALTS_OK`.

### 12.1 Normal submission

1. validate listener or UDP packet message context;
2. map/open exact parser session;
3. feed `message->payload` once;
4. if bytes are only a partial stream frame, complete the async claim `SALTS_OK` after ProtocolSource copies them into its bounded parser buffer;
5. if all exposed complete frames reach Inbox, complete `SALTS_OK`;
6. if a complete frame is retained on a capacity status, keep the claim live and mark backpressure;
7. a non-capacity error completes the claim with that error and marks the intake terminal.

A single TCP message may expose multiple frames; its claim remains live until every frame represented by that message has been admitted or terminally failed.

### 12.2 Already backpressured

Submissions already entered by the same bounded CNet poll are moved into the preallocated FIFO pending-claim store without feeding their bytes. The conservative preflight bound guarantees this cannot exceed configured claim/byte storage during one Source poll. Exceeding the preallocated invariant fails closed.

### 12.3 Retry/poll ownership

`ProtocolNetworkIntake.poll()` is the only operation allowed to call Source Product owner `poll()`.

Its order is:

1. empty-feed retry the currently retained ProtocolSource frame;
2. when admitted, drain queued async claims FIFO until empty or another capacity block;
3. while blocked or queued, do not call Source owner `poll()`;
4. when clear, overflow-check/increment `source_polls` and invoke Source owner `poll(timeout_ms)` once;
5. refresh Sink metrics and Source connection snapshot.

Repeated public `poll()` calls are the explicit retry action. No background retry thread exists.

## 13. Durable identity and replay

ProtocolInbox's resolver returns:

- `source_id`: exact configured `source_id`;
- `source_sequence`: decoded metadata sequence;
- `correlation`: decoded metadata correlation if present;
- `timestamp_ns`: **zero**, meaning protocol timestamp unknown in v1;
- `admission_id`: canonical ASCII `<protocol>/<device>/<message_type>/<sequence>/<xxh3-128(raw-payload)>`.

`timestamp_ns` must be deterministic because Inbox exact replay compares the complete record, including timestamp. Host arrival time is observability, not durable protocol identity, and must not be stored in this v1 record.

The Inbox deduplicates `(source_id, admission_id)` and then checks complete-record equality. A hash collision with different content therefore returns `SALTS_EPROTO`; it cannot silently alias records.

No C pointer, DLL address, raw CNet struct, parser ID, borrowed network buffer, or transport generation is persisted.

## 14. Store-before-business boundary

Business execution remains:

1. decode frame;
2. ProtocolInbox admission succeeds;
3. record becomes claimable;
4. InboxSource claims it;
5. business Graph is requested;
6. terminal business outcome settles the Inbox claim.

Holding/failing admission must leave the business Graph invocation count at zero.

## 15. Business destination semantics

Both protocols use the same CNet datagram Sink provider type and the same business Graph topology. Separate runs configure destination A or B. The `protocol.intake` config remains unchanged when only business destination changes.

Protocol reply routing remains a separate concern and is not generalized into business result routing.

## 16. Lifecycle

### Create

Before ownership transfer validate public ABI, exact topology/config, Source provider availability/preflight, protocol provider/version, all capacity arithmetic, and one exact live Inbox v2 handle.

After transfer:

1. retain catalog;
2. create protocol registry/owner;
3. create ProtocolInbox and ProtocolSource;
4. allocate parser/pending storage;
5. register internal managed async Sink;
6. materialize configured CNet Source Product owner;
7. compile Flow;
8. publish COMPILED owner.

No network socket opens during create.

### Start

`start()` calls `turbo_flow_start()` once. CNet opens its native Source in the normal managed Source start callback. A successful post-start snapshot resolves the configured CNet Source's generic connection snapshot and publishes the actual bound endpoint.

### Poll

Poll follows Section 12.3. `source_polls` counts actual Product-owner poll calls, including a call that returns an error. It does not increase during storage backpressure.

### Stop

From RUNNING/BACKPRESSURED:

1. mark STOPPING;
2. quiesce Source owner;
3. force-close ProtocolSource accepting/parser state and complete all unadmitted async claims with `SALTS_ECANCELED`;
4. call `turbo_flow_stop()` so CNet executes its normal stop/drain path;
5. call Source owner `drain(timeout_ms)`;
6. publish STOPPED only when quiescent.

Already-admitted Inbox records are untouched.

From COMPILED, no native Source has started; quiesce the Product owner and transition to STOPPED without calling a native stop.

### Destroy

Destroy requires STOPPED and no live async claims. Destroy the Flow while callback contexts are alive; then Source owner shutdown/destroy, Sink object, ProtocolSource/ProtocolInbox, protocol owner/registry, and retained catalog reference. Caller-owned Inbox is untouched.

Any cleanup error keeps retryable owned state; no force unload invalidates callbacks.

## 17. Acceptance matrix

Required GREEN evidence:

1. exact C/C++ ProtocolNetworkIntake ABI/lifecycle;
2. listener message-context ABI/lifetime;
3. configured CNet Source connection snapshot reports actual TCP/UDP bound endpoint;
4. real fragmented JT/T808/TCP -> memory Inbox -> business Graph -> real CNet datagram Sink;
5. real CoAP/UDP -> memory Inbox -> same business Graph/Sink type;
6. KCP pairing rejected before network side effects;
7. storage admission held/failed => business Graph count zero;
8. TurboDB storage failure => intake fails/backpressures according to status and no memory fallback exists;
9. capacity retry admits one retained frame exactly once;
10. while retained storage capacity remains blocked, `source_polls` does not increase;
11. partial TCP chunk completes upstream after bounded parser copy but creates no Inbox record;
12. TCP generation reuse cannot combine generation N partial bytes with N+1;
13. stale UDP generation is rejected before admission;
14. fixed pending-claim/byte bound is sufficient for two Source scheduler passes and cannot grow;
15. business Graph failure settles only through existing InboxSource semantics and never implicitly replays;
16. stop cancels only unadmitted bytes; admitted records remain valid;
17. catalog/module unload remains blocked until intake cleanup releases its retained snapshot;
18. identical wire replay produces identical durable record identity, including `timestamp_ns == 0`;
19. memory/TurboDB real-network parity;
20. TurboDB clean-close/reopen recovery;
21. business destination A/B independence;
22. fresh installed C and C++ `ProtocolNetworkIntake` consumers;
23. dependency/export/profile gates plus Debug/ASan and Release full suites.

## 18. Test architecture

### Focused contracts

Cover listener context, CNet configured-source connection snapshot, exact intake config/topology, parser generation fencing, async-claim ownership, deterministic identity/replay, capacity retry, pending bounds, KCP rejection, public lifecycle, and catalog pin/unload.

### Real local network

Use real CNet plugin Source/Sink, real JT/T808 or CoAP DLL, real memory/TurboDB Inbox, existing InboxSource driver, and real loopback TCP/UDP peers. Mocks may inject focused owner/storage failures but never replace positive real-network endpoints.

### Package/profile

Run Debug/ASan, Release full CTest, fresh installed C/C++ consumer, plugin canonical-export checks, dependency closure, CRT/profile checks, formatting, and `git diff --check`.

## 19. Compatibility

This remains an incompatible #117/#118 line. No old protocol-to-Graph runtime, mapper, storage schema migration, ABI alias, or fallback is restored.

Two additive public contracts are introduced:

1. `turbo_flow_cnet_listener_message_context_t` plus accessor;
2. `TurboFlow::ProtocolNetworkIntake` size/versioned owner API.

The existing generic CNet adapter connection-snapshot callback is populated for configured listener/UDP packet Sources; no new generic Graph ABI is needed.

## 20. Completion boundary

Once all acceptance items are GREEN, #118 may close using JT/T808/TCP and CoAP/UDP as its two real network protocols. The closure must not claim FlowMQ #74, Flowie #115, RulesForge #73, KCP intake support, universal HTTP/WS/MQTT coverage, or end-to-end exactly-once semantics.