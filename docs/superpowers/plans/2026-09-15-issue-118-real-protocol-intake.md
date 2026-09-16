# Issue #118 Real Protocol Network Intake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver installed `TurboFlow::ProtocolNetworkIntake` support for real JT/T 808/TCP and CoAP/UDP input, with durable Inbox admission before business Graph execution, bounded backpressure, transport-generation fencing, deterministic replay identity, and configured CNet output.

**Architecture:** `ProtocolNetworkIntake` sits above Graph, PluginHost, CNetAdapter, ProtocolIngress, and ProtocolIngressInbox. It retains one PluginHost catalog snapshot, materializes exactly one configured CNet Source Product owner, opens exactly one configured protocol owner, terminates a two-stage intake Flow with a built-in managed async-terminal Sink, and feeds decoded messages only to the caller-selected Inbox. The post-Inbox business Flow remains an ordinary plugin generation driven by `turbo_flow_inbox_source_t` and a configured CNet datagram Sink.

**Tech Stack:** C11/C17, C++17 header probes, TurboFlow Graph/PluginHost, Salts CFlow/CNet/DataBind, TurboDB ORM/SQLite, xxHash3-128, TinyTest, CMake presets.

**Spec:** `docs/superpowers/specs/2026-09-15-issue-118-real-protocol-intake-design.md`

## Global Constraints

- Before implementation, use `superpowers:using-git-worktrees`; create the implementation worktree from the exact reviewed plan head, not an older master/spec head.
- Supported v1 pairings are only `jtt808 + cnet.listener_source` and `coap + cnet.packet_source(packet_mode=udp)`.
- ProtocolNetworkIntake v1 rejects KCP/secure-KCP before native network side effects.
- Do not change Inbox v2 ABI/schema and do not add database-to-memory fallback.
- Do not restore protocol-to-business-Graph direct publication.
- Do not extend PluginHost root ABI, generic Product-owner ABI, or add a raw-feed public API.
- `resolved` is borrowed only during create; the caller-owned Inbox is borrowed until intake destruction; every required string/value is copied.
- The catalog snapshot remains retained until the CNet Source owner and protocol owner/registry are fully destroyed.
- Partial TCP bytes may complete their async claim only after ProtocolSource copied them into bounded parser storage. A complete frame blocked on Inbox capacity keeps its async claim live.
- While storage is blocked, public `poll()` retries retained protocol work and must not call the CNet Source Product owner `poll()`; `snapshot.source_polls` proves this invariant.
- Durable v1 `timestamp_ns` is exactly zero so exact network replay produces an identical Inbox record. Local arrival time belongs to observability, not durable identity.
- Positive end-to-end tests use real CNet Source/Sink owners, real JT/T808 or CoAP DLLs, and real memory/TurboDB Inbox providers.
- Fixture plugins are allowed only for focused lifecycle/fault tests.
- Tasks 1–5 and 8–9 follow RED→GREEN. Tasks 6–7 are real-network acceptance gates over the implementation from Tasks 1–5; if they are already GREEN, do not invent speculative production changes.
- Each commit contains only the task's scoped test/production changes. Do not fix unrelated failures.

---

## File Map

### New production files

- `ingress/protocol/network/CMakeLists.txt`
- `ingress/protocol/network/include/turbo_flow_protocol_network_intake.h`
- `ingress/protocol/network/src/flow_protocol_network_intake_internal.h`
- `ingress/protocol/network/src/flow_protocol_network_intake_config.c`
- `ingress/protocol/network/src/flow_protocol_network_intake_sink.c`
- `ingress/protocol/network/src/flow_protocol_network_intake.c`

### New tests/support

- `ingress/protocol/tests/test_protocol_network_intake_config.c`
- `ingress/protocol/tests/test_protocol_network_intake_core.c`
- `ingress/protocol/tests/test_protocol_network_intake.c`
- `ingress/protocol/tests/protocol_network_intake_header_cpp.cpp`
- `ingress/protocol/tests/protocol_network_source_fixture.c`
- `ingress/protocol/tests/protocol_network_e2e_fixture.h`
- `ingress/protocol/tests/protocol_network_e2e_fixture.c`
- `ingress/protocol/tests/test_protocol_network_jtt808.c`
- `ingress/protocol/tests/test_protocol_network_coap.c`
- `ingress/protocol/tests/protocol_network_turbodb_fixture.h`
- `ingress/protocol/tests/protocol_network_turbodb_fixture.c`
- `ingress/protocol/tests/test_protocol_network_turbodb.c`
- `tests/install_protocol_network_intake_consumer/CMakeLists.txt`
- `tests/install_protocol_network_intake_consumer/main.c`
- `tests/install_protocol_network_intake_consumer/header.cpp`
- `tests/install_protocol_network_intake_consumer/run.cmake`

### Existing files modified

- `io/cnet/include/turbo_flow_cnet.h`
- `io/cnet/src/turbo_flow_cnet_listener_source.c`
- `io/cnet/src/turbo_flow_cnet_plugin.c`
- `io/cnet/tests/test_cnet_listener_source.c`
- `io/cnet/tests/cnet_stream_source_header_cpp.cpp`
- `io/cnet/tests/test_cnet_plugin.c`
- `ingress/protocol/CMakeLists.txt`
- `ingress/protocol/tests/CMakeLists.txt`
- `CMakeLists.txt`
- `cmake/TurboFlowConfig.cmake.in`
- `ingress/protocol/README.md` only after all behavior gates are GREEN

---

### Task 1: Add message-owned TCP listener identity

**Files:**
- Modify: `io/cnet/include/turbo_flow_cnet.h`
- Modify: `io/cnet/src/turbo_flow_cnet_listener_source.c`
- Modify: `io/cnet/tests/test_cnet_listener_source.c`
- Modify: `io/cnet/tests/cnet_stream_source_header_cpp.cpp`

**Interfaces:**
- Produces `turbo_flow_cnet_listener_message_context_t` and `turbo_flow_cnet_listener_message_context()`.
- Later parser-session mapping uses only `connection.slot` + `connection.generation` from this message-owned context.

- [ ] **Step 1: Write compile/runtime RED**

Add this to the existing C++ header probe before the production declaration exists:

```cpp
turbo_flow_cnet_listener_message_context_t listener_context = {0};
if (listener_context.size != 0u) return 1;
```

Change the existing listener graph probe to require:

```c
const turbo_flow_cnet_listener_message_context_t *context =
    turbo_flow_cnet_listener_message_context(message);
check_not_null(context);
check_equal(context->size, sizeof(*context));
check_equal(context->version, TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION);
check_true(context->connection.generation != 0u);
```

Add one malformed message whose `transport_context` points outside `message->buffer` and assert the accessor returns NULL.

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset win-dev-user --target test_cnet_listener_source
```

Expected: compile failure on the missing context type/accessor.

- [ ] **Step 3: Add exact public projection**

```c
#define TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION 1u

typedef struct turbo_flow_cnet_listener_message_context_s {
  size_t size;
  uint32_t version;
  cnet_connection connection;
} turbo_flow_cnet_listener_message_context_t;

#define TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_V1_SIZE \
  sizeof(turbo_flow_cnet_listener_message_context_t)

TURBO_FLOW_C_API const turbo_flow_cnet_listener_message_context_t *
turbo_flow_cnet_listener_message_context(const turbo_flow_msg_t *message);
```

In `listener_source_on_receive()` allocate `sizeof(context) + view->size`, write the context first, copy payload after it, set `message->transport_context`, and point `message->payload` after the context. Preserve existing message id/content-descriptor behavior.

Accessor validation must match packet-context ownership rules: context address inside used buffer, exact context size/version, nonzero connection generation, and payload starting exactly after context inside the same buffer.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build --preset win-dev-user --target test_cnet_listener_source test_cnet_packet_source
ctest --preset win-dev-user -R "^(test_cnet_listener_source|test_cnet_packet_source)$" --output-on-failure
```

- [ ] **Step 5: Commit**

```bash
git add io/cnet/include/turbo_flow_cnet.h \
        io/cnet/src/turbo_flow_cnet_listener_source.c \
        io/cnet/tests/test_cnet_listener_source.c \
        io/cnet/tests/cnet_stream_source_header_cpp.cpp
git commit -m "feat(cnet): preserve listener message connection identity"
```

---

### Task 2: Populate the existing generic CNet Source connection snapshot

**Files:**
- Modify: `io/cnet/src/turbo_flow_cnet_plugin.c`
- Modify: `io/cnet/tests/test_cnet_plugin.c`

**Interfaces:**
- Produces no new public ABI.
- `turbo_flow_adapter_connection_snapshot_at()` becomes usable for configured `cnet.listener_source` and `cnet.packet_source`.
- Later ProtocolNetworkIntake snapshots copy the configured Source endpoint from this existing public Graph seam.

- [ ] **Step 1: Write RED in configured CNet plugin test**

After an existing configured generation is started, enumerate adapter snapshots:

```c
turbo_flow_connection_snapshot_t connection;
int listener_seen = 0;
int packet_seen = 0;
for (size_t i = 0u; i < turbo_flow_adapter_count(flow); ++i) {
  memset(&connection, 0, sizeof(connection));
  if (turbo_flow_adapter_connection_snapshot_at(flow, i, &connection) != SALTS_OK) continue;
  if (connection.adapter_name && strcmp(connection.adapter_name, "listener.source") == 0) {
    check_true(strncmp(connection.endpoint, "tcp://127.0.0.1:", 16u) == 0);
    check_true(strcmp(connection.endpoint, "tcp://127.0.0.1:0") != 0);
    listener_seen = 1;
  }
  if (connection.adapter_name && strcmp(connection.adapter_name, "packet.source") == 0) {
    check_true(strncmp(connection.endpoint, "udp://127.0.0.1:", 16u) == 0);
    check_true(strcmp(connection.endpoint, "udp://127.0.0.1:0") != 0);
    packet_seen = 1;
  }
}
check_true(listener_seen);
check_true(packet_seen);
```

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset win-dev-user --target test_cnet_plugin
ctest --preset win-dev-user -R "^test_cnet_plugin$" --output-on-failure
```

Expected: Source adapters return `SALTS_ENOTSUP` because `ops.connection_snapshot` is not populated.

- [ ] **Step 3: Implement Source connection snapshot**

Add one callback used only by CNet Source registrations:

```c
static int cnet_plugin_source_connection_snapshot(void *ctx,
                                                  turbo_flow_connection_snapshot_t *out) {
  cnet_plugin_owner_t *owner = (cnet_plugin_owner_t *)ctx;
  if (!owner || !out) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  out->adapter_name = owner->name;
  out->adapter_kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  out->direction = TURBO_FLOW_ADAPTER_INPUT;
  out->connection_limit = owner->config.kind == TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE
                              ? owner->config.max_connections
                              : owner->config.endpoint.session_capacity;
  out->last_status = owner->last_status;

  if (owner->state == TURBO_FLOW_MANAGED_BOUNDARY_RUNNING)
    out->state = TURBO_FLOW_CONNECTION_READY;
  else if (owner->state == TURBO_FLOW_MANAGED_BOUNDARY_FAILED)
    out->state = TURBO_FLOW_CONNECTION_FAILED;
  else if (owner->state == TURBO_FLOW_MANAGED_BOUNDARY_STOPPING)
    out->state = TURBO_FLOW_CONNECTION_CLOSING;
  else
    out->state = TURBO_FLOW_CONNECTION_STOPPED;

  if (owner->config.kind == TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE) {
    turbo_flow_cnet_listener_source_snapshot_t source =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    uint16_t port = owner->config.listener.port;
    if (owner->handle.listener_source) {
      int rc = turbo_flow_cnet_listener_source_snapshot(owner->handle.listener_source, &source);
      if (rc != SALTS_OK) return rc;
      port = source.bound_port;
      out->connections_current = source.active_connections;
      out->in_flight_messages = source.receive_pending != 0 ? 1u : 0u;
      out->last_status = source.status;
    }
    (void)snprintf(out->endpoint, sizeof(out->endpoint), "tcp://%s:%u",
                   owner->config.listener.host, (unsigned)port);
    return SALTS_OK;
  }

  if (owner->config.kind == TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE) {
    turbo_flow_cnet_packet_source_snapshot_t source = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
    uint16_t port = owner->config.endpoint.datagram.port;
    const char *scheme = owner->config.endpoint.protocol == CNET_PACKET_UDP ? "udp" : "kcp";
    if (owner->handle.packet_source) {
      int rc = turbo_flow_cnet_packet_source_snapshot(owner->handle.packet_source, &source);
      if (rc != SALTS_OK) return rc;
      port = source.bound_port;
      out->connections_current = source.sessions_opened >= source.sessions_closed
                                     ? source.sessions_opened - source.sessions_closed
                                     : 0u;
      out->in_flight_messages = source.queue_depth;
      out->last_status = source.status;
    }
    (void)snprintf(out->endpoint, sizeof(out->endpoint), "%s://%s:%u", scheme,
                   owner->config.endpoint.datagram.host, (unsigned)port);
    return SALTS_OK;
  }

  return SALTS_ENOTSUP;
}
```

Set `ops.connection_snapshot = cnet_plugin_source_connection_snapshot` in `cnet_plugin_register_source()` only.

- [ ] **Step 4: Run GREEN**

```bash
cmake --build --preset win-dev-user --target test_cnet_plugin
ctest --preset win-dev-user -R "^test_cnet_plugin$" --output-on-failure
```

- [ ] **Step 5: Commit**

```bash
git add io/cnet/src/turbo_flow_cnet_plugin.c io/cnet/tests/test_cnet_plugin.c
git commit -m "feat(cnet): expose configured source connection snapshots"
```

---

### Task 3: Freeze exact `protocol.intake` configuration and topology

**Files:**
- Create: `ingress/protocol/network/CMakeLists.txt`
- Create: `ingress/protocol/network/src/flow_protocol_network_intake_internal.h`
- Create: `ingress/protocol/network/src/flow_protocol_network_intake_config.c`
- Create: `ingress/protocol/tests/test_protocol_network_intake_config.c`
- Modify: `ingress/protocol/CMakeLists.txt`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**

```c
typedef enum flow_protocol_network_transport_kind_e {
  FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP = 1,
  FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP = 2
} flow_protocol_network_transport_kind_t;

typedef struct flow_protocol_network_intake_settings_s {
  turbo_flow_protocol_kind_t protocol_kind;
  flow_protocol_network_transport_kind_t transport_kind;
  char protocol_provider[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
  char source_id[TURBO_FLOW_PROTOCOL_INBOX_SOURCE_ID_MAX + 1u];
  char source_adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char intake_adapter_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  size_t max_sessions;
  size_t max_frame_size;
  size_t max_pending_claims;
  size_t max_pending_bytes;
  size_t source_scheduler_max_steps;
  size_t source_max_message_bytes;
} flow_protocol_network_intake_settings_t;

int flow_protocol_network_intake_preflight(
    const turbo_flow_resolved_config_t *resolved, const turbo_flow_t *flow,
    const char *source_adapter_name, const char *intake_adapter_name,
    flow_protocol_network_intake_settings_t *settings,
    turbo_flow_config_error_t *error);
```

- [ ] **Step 1: Add exact config/topology RED**

Use a parsed Flow containing exactly:

```c
static const char graph[] =
    "source wire adapter tcp.input\n"
    "stage durable adapter protocol.store\n"
    "stage main {\n"
    "  wire -> durable\n"
    "}\n";
```

Use a full existing CNet listener config plus this exact intake config:

```yaml
protocol.store:
  kind: protocol.intake
  config:
    schema_version: 1
    protocol_provider: jtt808
    protocol_kind: jtt808
    protocol_version: 2019-A1
    source_id: fleet.primary
    max_sessions: 4
    max_frame_size: 1024
    max_pending_claims: 64
    max_pending_bytes: 65536
```

Assert valid preflight returns `SALTS_OK`. Add independent negative cases for unknown/missing fields, wrong Source/protocol pairing, unsupported version, KCP packet Source, zero bounds, session bound above CNet capacity, insufficient pending-claim bound, insufficient pending-byte bound, wrong adapter names, non-PARSED Flow, stage count not equal to two, edge count not equal to one, and an edge other than source-index -> sink-index.

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake_config
```

Expected: missing internal preflight implementation.

- [ ] **Step 3: Implement exact parser and topology check**

Allowed intake fields are exactly:

```c
static const char *const intake_fields[] = {
    "schema_version", "protocol_provider", "protocol_kind", "protocol_version",
    "source_id", "max_sessions", "max_frame_size", "max_pending_claims",
    "max_pending_bytes"};
```

Require exact field count and reject any other name. Parse only `jtt808` and `coap`. Read Source bounds using public resolved-config getters.

Topology is accepted only when:

```c
if (turbo_flow_state(flow) != TURBO_FLOW_STATE_PARSED) return SALTS_EINVAL;
if (turbo_flow_stage_count(flow) != 2u) return SALTS_EINVAL;
if (turbo_flow_edge_count(flow) != 1u) return SALTS_EINVAL;
```

Find the source/sink stage by matching `stage->adapter_name` to the copied configured names, require `source->is_source != 0`, `sink->is_source == 0`, then require the sole `turbo_flow_edge_at(flow, 0u)` to connect those exact stage indexes and be `TURBO_FLOW_EDGE_UNCONDITIONAL`.

For pending bounds:

```c
if (settings->source_scheduler_max_steps > SIZE_MAX / 2u) return SALTS_ERANGE;
size_t required_claims = settings->source_scheduler_max_steps * 2u;
if (settings->max_pending_claims < required_claims) return SALTS_ERANGE;
if (settings->source_max_message_bytes > SIZE_MAX / settings->max_pending_claims)
  return SALTS_ERANGE;
size_t required_bytes = settings->source_max_message_bytes * settings->max_pending_claims;
if (settings->max_pending_bytes < required_bytes) return SALTS_ERANGE;
```

- [ ] **Step 4: Add internal core target and run GREEN**

```cmake
add_library(tf_protocol_network_intake_core STATIC
            src/flow_protocol_network_intake_config.c)
set_target_properties(tf_protocol_network_intake_core PROPERTIES
                      POSITION_INDEPENDENT_CODE ON
                      FOLDER "ingress/protocol/network/internal")
target_include_directories(tf_protocol_network_intake_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tf_protocol_network_intake_core
  PUBLIC TurboFlow::Graph TurboFlow::ProtocolIngressInbox TurboFlow::PluginHost)
```

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake_config
ctest --preset win-dev-user -R "^test_protocol_network_intake_config$" --output-on-failure
```

- [ ] **Step 5: Commit**

```bash
git add ingress/protocol/network ingress/protocol/CMakeLists.txt \
        ingress/protocol/tests/CMakeLists.txt \
        ingress/protocol/tests/test_protocol_network_intake_config.c
git commit -m "test(protocol): freeze network intake configuration"
```

---

### Task 4: Implement bounded async-terminal intake and deterministic replay identity

**Files:**
- Create: `ingress/protocol/network/src/flow_protocol_network_intake_sink.c`
- Create: `ingress/protocol/tests/test_protocol_network_intake_core.c`
- Modify: `ingress/protocol/network/src/flow_protocol_network_intake_internal.h`
- Modify: `ingress/protocol/network/CMakeLists.txt`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**

```c
typedef struct flow_protocol_network_intake_sink_s
    flow_protocol_network_intake_sink_t;

typedef struct flow_protocol_network_intake_sink_metrics_s {
  size_t active_sessions;
  size_t pending_claims;
  size_t pending_bytes;
  uint64_t frames_admitted;
  int backpressured;
  int terminal_status;
} flow_protocol_network_intake_sink_metrics_t;

typedef struct flow_protocol_network_intake_sink_config_s {
  turbo_flow_t *flow;
  const char *adapter_name;
  turbo_flow_protocol_t *protocol;
  turbo_flow_inbox_t *inbox;
  const flow_protocol_network_intake_settings_t *settings;
} flow_protocol_network_intake_sink_config_t;

int flow_protocol_network_intake_sink_create(
    const flow_protocol_network_intake_sink_config_t *config,
    flow_protocol_network_intake_sink_t **out);
int flow_protocol_network_intake_sink_register(flow_protocol_network_intake_sink_t *sink);
int flow_protocol_network_intake_sink_retry(flow_protocol_network_intake_sink_t *sink);
void flow_protocol_network_intake_sink_cancel(flow_protocol_network_intake_sink_t *sink,
                                             int status);
void flow_protocol_network_intake_sink_metrics(
    const flow_protocol_network_intake_sink_t *sink,
    flow_protocol_network_intake_sink_metrics_t *metrics);
void flow_protocol_network_intake_sink_destroy(flow_protocol_network_intake_sink_t *sink);
```

- [ ] **Step 1: Write focused RED**

Required cases:

1. first half of a valid JT/T808 frame completes its async publication after bounded parser copy and produces zero Inbox records;
2. second half completes the frame while a one-slot Inbox is full, so publication remains pending;
3. after capacity is freed, `sink_retry()` admits exactly one record without re-feeding frame bytes;
4. one TCP message containing two frames can admit the first and retain the second;
5. a second async claim arriving while blocked is queued without feeding;
6. slot generation N partial bytes followed by later message id/generation N+1 closes N parser state and cannot complete N's frame;
7. generation mismatch with non-increasing TurboFlow message id is stale and completes `SALTS_EPROTO`;
8. missing/malformed CNet context fails before admission;
9. CoAP packet context creates a nonempty canonical peer device id;
10. identical complete wire frame replay produces one Inbox record, the same receipt identity, and a durable claimed record with `timestamp_ns == 0`;
11. cancel completes all unadmitted retained/queued claims with `SALTS_ECANCELED`.

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake_core
```

Expected: missing sink symbols.

- [ ] **Step 3: Implement fixed parser/pending storage**

```c
typedef struct intake_parser_slot_s {
  int in_use;
  uint32_t transport_slot;
  uint64_t generation;
  uint64_t last_message_id;
  uint64_t protocol_session_id;
} intake_parser_slot_t;

typedef struct intake_pending_claim_s {
  turbo_flow_async_terminal_claim_t claim;
  size_t retained_bytes;
} intake_pending_claim_t;
```

Allocate both arrays to configured bounds during create. No reserve/growth path exists after create.

ProtocolSource session id is `(uint64_t)transport_slot + 1u`. On generation mismatch: reject as stale when message id is non-increasing; otherwise close old session before opening new generation.

- [ ] **Step 4: Implement CoAP peer identity and ProtocolInbox identity**

Canonical CoAP peer strings are:

```text
udp4-7f000001-<decimal-port>
udp6-<32-lowercase-hex-address>-<decimal-port>-<decimal-scope>
```

Build admission identity with XXH3-128 over the raw-preserved decoded payload:

```c
XXH128_hash_t digest = XXH3_128bits(message->payload, message->payload_size);
int count = snprintf(storage, sizeof(storage),
                     "%s/%s/%" PRIu32 "/%" PRIu64 "/%016" PRIx64 "%016" PRIx64,
                     protocol_name, metadata->device_id, metadata->message_type,
                     metadata->sequence, digest.high64, digest.low64);
if (count < 0 || (size_t)count >= sizeof(storage)) return SALTS_EMSGSIZE;
identity->source_id = vstr_from_buf(settings->source_id, strlen(settings->source_id));
identity->admission_id = vstr_from_buf(storage, (size_t)count);
identity->source_sequence = metadata->sequence;
identity->timestamp_ns = 0u;
```

Copy correlation from metadata when nonempty. Never use delivery id, parser id, CNet generation, or local arrival time in durable identity.

- [ ] **Step 5: Implement claim ownership, retry, and cancel**

Every accepted submit first moves its claim:

```c
turbo_flow_async_terminal_claim_t owned = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
int rc = turbo_flow_async_terminal_claim_move(&owned, claim);
if (rc != SALTS_OK) return rc;
```

When clear, feed only this message once. Immediate partial/success completes `owned` with `SALTS_OK`. Capacity result stores it as the blocking claim. When already blocked, append `owned` to the fixed FIFO without feeding.

`flow_protocol_network_intake_sink_retry()` calls `turbo_flow_protocol_source_session_feed(source, session_id, generation, NULL, 0u, &result)` only for the retained blocked frame. After success it completes that claim, then processes queued claims FIFO until clear or a new capacity block occurs.

`flow_protocol_network_intake_sink_cancel()` first calls `turbo_flow_protocol_source_force_shutdown(protocol_source, status)`, then completes every live async claim with the same non-OK status.

- [ ] **Step 6: Register exact managed async-terminal Sink**

Use caller-owned Sink lifetime and a registry shutdown callback that only marks registration detached:

```c
turbo_flow_adapter_ops_t adapter_ops = {0};
turbo_flow_async_terminal_adapter_ops_t async_ops =
    TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
turbo_flow_adapter_schema_t schema = {0};
turbo_flow_managed_boundary_provider_ops_t boundary =
    TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
turbo_flow_managed_async_terminal_registration_t registration =
    TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
adapter_ops.shutdown = intake_sink_registry_shutdown;
async_ops.submit = intake_async_submit;
schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
schema.roles = TURBO_FLOW_ADAPTER_SINK;
schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
registration.adapter_name = sink->adapter_name;
registration.adapter_ops = &adapter_ops;
registration.async_ops = &async_ops;
registration.schema = &schema;
registration.owner_name = sink->adapter_name;
registration.boundary_ops = &boundary;
registration.ctx = sink;
```

- [ ] **Step 7: Run GREEN**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_intake_core test_protocol_source test_protocol_inbox
ctest --preset win-dev-user -R \
  "^(test_protocol_network_intake_core|test_protocol_source|test_protocol_inbox)$" \
  --output-on-failure
```

- [ ] **Step 8: Commit**

```bash
git add ingress/protocol/network/src ingress/protocol/network/CMakeLists.txt \
        ingress/protocol/tests/CMakeLists.txt \
        ingress/protocol/tests/test_protocol_network_intake_core.c
git commit -m "feat(protocol): add bounded network intake sink"
```

---

### Task 5: Add installed ProtocolNetworkIntake owner and exact lifecycle

**Files:**
- Create: `ingress/protocol/network/include/turbo_flow_protocol_network_intake.h`
- Create: `ingress/protocol/network/src/flow_protocol_network_intake.c`
- Create: `ingress/protocol/tests/test_protocol_network_intake.c`
- Create: `ingress/protocol/tests/protocol_network_intake_header_cpp.cpp`
- Create: `ingress/protocol/tests/protocol_network_source_fixture.c`
- Modify: `ingress/protocol/network/CMakeLists.txt`
- Modify: `ingress/protocol/tests/CMakeLists.txt`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Public header exactly matches the approved spec, including snapshot fields `source_polls` and `source_endpoint`.
- Consumes Tasks 3–4 internal interfaces and Task 2 generic connection snapshots.

- [ ] **Step 1: Write ABI/lifecycle/catalog RED**

C++ probe instantiates both public init macros. Focused fixture plugin registers exactly one transactional Source provider and returns a complete external-poll Product owner.

Required RED cases:

- valid fixture + real JT/T808 protocol module creates COMPILED owner and moves `flow_io` to NULL;
- public ABI mismatch leaves `flow_io` caller-owned and `out == NULL`;
- Source provider preflight failure leaves Flow caller-owned;
- protocol provider/version mismatch occurs before Source materialization;
- Source materialize failure unwinds protocol/Sink/catalog ownership;
- caller releases its snapshot reference, then PluginHost destroy returns `SALTS_EBUSY` while intake retains the catalog;
- `poll()` before start returns `SALTS_EBUSY` and `source_polls == 0`;
- fixture poll failure increments `source_polls` once and makes intake FAILED with the exact error;
- stop from COMPILED reaches STOPPED without Source poll/start side effects;
- destroy before STOPPED returns `SALTS_EBUSY`;
- after stop/destroy, PluginHost unload succeeds.

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake
```

- [ ] **Step 3: Implement owner state and exact provider lookup**

```c
struct turbo_flow_protocol_network_intake_s {
  turbo_flow_protocol_network_intake_state_t state;
  int status;
  turbo_flow_t *flow;
  turbo_flow_inbox_t *inbox;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_protocol_registry_t *protocol_registry;
  turbo_flow_protocol_owner_t *protocol_owner;
  turbo_flow_protocol_t *protocol;
  turbo_flow_plugin_product_owner_v1_t source_owner;
  flow_protocol_network_intake_sink_t *sink;
  flow_protocol_network_intake_settings_t settings;
  uint64_t source_polls;
  char source_endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
};
```

Find exactly one transactional adapter provider with `provider.kind == resolved_source.kind`; zero or duplicate matches fail `SALTS_ENOTSUP`/`SALTS_EALREADY` before Flow transfer.

Validate returned Source Product owner using the same shape rules as generation code: exact ABI; exactly one control-thread/thread-safe flag; quiesce/drain/shutdown/destroy required; `EXTERNAL_POLL` required and `poll` non-NULL.

- [ ] **Step 4: Implement create without network side effects**

Execution order is fixed:

```text
flow/config/inbox validation
transactional catalog projection and Source provider preflight
catalog retain
protocol registry create
exact protocol owner create + instance
ProtocolInbox/ProtocolSource/intake Sink create
move intake Flow ownership
Sink registration
Source Product owner materialize
Flow compile
publish COMPILED intake
```

Protocol request:

```c
turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
request.protocol = intake->settings.protocol_kind;
request.protocol_version = intake->settings.protocol_version;
request.max_frame_size = intake->settings.max_frame_size;
```

If any post-transfer step fails, destroy the Flow while registered callback contexts remain alive, destroy the unstarted Source owner when published, then Sink, protocol objects, and retained catalog. Return no public owner.

- [ ] **Step 5: Implement start/snapshot/poll**

`start()` only accepts COMPILED and calls `turbo_flow_start()` once.

After successful start and after every Source poll, refresh the configured Source endpoint by enumerating `turbo_flow_adapter_connection_snapshot_at()` until `snapshot.adapter_name` equals copied `source_adapter_name`; copy only a successful snapshot's endpoint.

Poll order:

```c
flow_protocol_network_intake_sink_metrics_t metrics = {0};
int rc = flow_protocol_network_intake_sink_retry(intake->sink);
if (rc != SALTS_OK) return intake_fail(intake, rc);
flow_protocol_network_intake_sink_metrics(intake->sink, &metrics);
if (metrics.terminal_status != SALTS_OK) return intake_fail(intake, metrics.terminal_status);
if (metrics.backpressured || metrics.pending_claims != 0u) {
  intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_BACKPRESSURED;
  return intake_snapshot_copy(intake, snapshot);
}
if (intake->source_polls == UINT64_MAX) return intake_fail(intake, SALTS_ERANGE);
++intake->source_polls;
rc = intake->source_owner.poll(intake->source_owner.ctx, timeout_ms);
if (rc != SALTS_OK) return intake_fail(intake, rc);
intake->state = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING;
return intake_snapshot_copy(intake, snapshot);
```

- [ ] **Step 6: Implement stop/destroy**

RUNNING/BACKPRESSURED stop order:

```text
state STOPPING
Source owner quiesce(timeout)
Sink cancel(SALTS_ECANCELED)
turbo_flow_stop(flow)
Source owner drain(timeout)
state STOPPED
```

COMPILED stop quiesces the unstarted Product owner and moves directly to STOPPED.

Destroy requires STOPPED: destroy Flow first while adapter contexts remain valid; then Source owner shutdown/destroy; Sink object; ProtocolSource/ProtocolInbox; protocol owner/registry; retained catalog. Never close/destroy the caller Inbox.

- [ ] **Step 7: Build installed shared target**

```cmake
add_library(tf_protocol_network_intake SHARED
  src/flow_protocol_network_intake.c
  include/turbo_flow_protocol_network_intake.h)
cmake_config_target(tf_protocol_network_intake
  ALIAS TurboFlow::ProtocolNetworkIntake
  FOLDER "ingress/protocol/network"
  EXPORT_NAME ProtocolNetworkIntake)
target_compile_definitions(tf_protocol_network_intake PRIVATE TURBO_FLOW_BUILD)
target_include_directories(tf_protocol_network_intake
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tf_protocol_network_intake
  PUBLIC TurboFlow::Graph TurboFlow::PluginHost
  PRIVATE tf_protocol_network_intake_core TurboFlow::CNetAdapter
          TurboFlow::ProtocolIngressInbox xxHash::xxhash)
install(FILES include/turbo_flow_protocol_network_intake.h DESTINATION include)
```

Add `tf_protocol_network_intake` to `TURBO_FLOW_EXPORT_TARGETS`; package component registration remains Task 9.

- [ ] **Step 8: Run GREEN**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake test_cnet_plugin
ctest --preset win-dev-user -R "^(test_protocol_network_intake|test_cnet_plugin)$" --output-on-failure
```

- [ ] **Step 9: Commit**

```bash
git add ingress/protocol/network ingress/protocol/tests/test_protocol_network_intake.c \
        ingress/protocol/tests/protocol_network_intake_header_cpp.cpp \
        ingress/protocol/tests/protocol_network_source_fixture.c \
        ingress/protocol/tests/CMakeLists.txt CMakeLists.txt
git commit -m "feat(protocol): add configured network intake owner"
```

---

### Task 6: Real fragmented JT/T808/TCP acceptance on memory Inbox

**Files:**
- Create: `ingress/protocol/tests/protocol_network_e2e_fixture.h`
- Create: `ingress/protocol/tests/protocol_network_e2e_fixture.c`
- Create: `ingress/protocol/tests/test_protocol_network_jtt808.c`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Shared harness owns configured PluginHost/snapshot, caller-selected Inbox, ProtocolNetworkIntake, business CNet datagram-Sink generation, InboxSource driver, and real loopback peers.

- [ ] **Step 1: Build real configured harness**

Plugin manifest contains exact `turbo-flow.cnet` v1.0.0 and `jtt808` v1.0.0 module paths supplied by CMake definitions. Intake Flow is exactly:

```c
static const char intake_graph[] =
    "source wire adapter tcp.input\n"
    "stage durable adapter protocol.store\n"
    "stage main {\n"
    "  wire -> durable\n"
    "}\n";
```

Business Flow is:

```c
static const char business_graph[] =
    "source inbox\n"
    "stage output adapter udp.output\n"
    "stage main {\n"
    "  inbox -> output\n"
    "}\n";
```

Create normal business plugin generation, call `turbo_flow_start(turbo_flow_plugin_generation_flow(generation))`, and create `turbo_flow_inbox_source_t` against logical source `inbox`. During delivery, drive `turbo_flow_plugin_generation_poll(generation, 0u, &error)` and `turbo_flow_inbox_source_poll()` until terminal settlement.

- [ ] **Step 2: Use canonical real JT/T808 frame**

Copy the existing protocol-conformance frame builder using this unescaped body and the same XOR/0x7d escaping rules:

```c
static const uint8_t unescaped[] = {
    0x02u, 0x00u, 0x40u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u,
    0x00u, 0x00u, 0x00u, 0x01u, 0x23u, 0x45u, 0x00u, 0x01u};
```

- [ ] **Step 3: Run the real acceptance**

After `ProtocolNetworkIntake.start()`, snapshot and parse `source_endpoint` with:

```c
unsigned port = 0u;
check_equal(sscanf(snapshot.source_endpoint, "tcp://127.0.0.1:%u", &port), 1);
check_true(port > 0u && port <= UINT16_MAX);
```

Connect a real TCP client to that port. Send first frame half; poll intake until Source accepted bytes, then assert Inbox `pending_records == 0`. Send second half; poll until Inbox `pending_records == 1` and assert business UDP receiver still has zero datagrams. Request the InboxSource run, drive business generation/InboxSource, and assert exactly one real UDP datagram.

Register a business Flow observer and assert business stage-completion count is zero before the InboxSource request.

- [ ] **Step 4: Prove TCP generation reuse**

Open connection A, send only the first half, close A, then connect B to the same listener and send only the second half. Drive intake and assert no Inbox record appears. Send a complete valid frame on B and assert exactly one record appears. This is the real-network generation-fence gate; the focused stale-message-id case remains in Task 4.

- [ ] **Step 5: Run acceptance + adjacent gates**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_jtt808 test_cnet_listener_source test_protocol_plugins \
  test_protocol_source test_protocol_inbox test_flow_inbox_source
ctest --preset win-dev-user -R \
  "^(test_protocol_network_jtt808|test_cnet_listener_source|test_protocol_plugins|test_protocol_source|test_protocol_inbox|test_flow_inbox_source)$" \
  --output-on-failure
```

A RED must be fixed only in the already-defined Task 1–5 contracts; do not alter JTT codec or Inbox schema.

- [ ] **Step 6: Commit**

```bash
git add ingress/protocol/tests/protocol_network_e2e_fixture.h \
        ingress/protocol/tests/protocol_network_e2e_fixture.c \
        ingress/protocol/tests/test_protocol_network_jtt808.c \
        ingress/protocol/tests/CMakeLists.txt ingress/protocol/network/src
git commit -m "test(protocol): prove real JTT808 TCP intake"
```

---

### Task 7: Real CoAP/UDP acceptance and destination independence

**Files:**
- Create: `ingress/protocol/tests/test_protocol_network_coap.c`
- Modify: `ingress/protocol/tests/protocol_network_e2e_fixture.c`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Reuses Task 6 harness and Task 4 canonical packet-device identity.

- [ ] **Step 1: Add exact CoAP configuration and KCP negative gate**

Load exact `coap` v1.0.0 DLL. Positive Source uses `cnet.packet_source` with `packet_mode: udp` and explicit zero KCP/security/FEC fields. Intake uses `protocol_provider: coap`, `protocol_kind: coap`, `protocol_version: RFC7252`.

Valid wire frame:

```c
static const uint8_t coap_get[] = {0x40u, 0x01u, 0x12u, 0x34u};
```

Create a separate valid-KCP resolved config and assert `turbo_flow_protocol_network_intake_create()` returns `SALTS_ENOTSUP`, `flow_io` remains caller-owned, and its connection snapshot still shows configured port zero because the Source never started.

- [ ] **Step 2: Run real UDP intake**

Start the UDP intake, obtain the actual endpoint:

```c
unsigned port = 0u;
check_equal(sscanf(snapshot.source_endpoint, "udp://127.0.0.1:%u", &port), 1);
check_true(port > 0u && port <= UINT16_MAX);
```

Bind a real UDP sender, send `coap_get`, poll until one Inbox record appears, and in a dedicated test instance claim/decode the ProtocolInbox TBE envelope to assert `deviceId` starts with `udp4-` and is nonempty. Complete that dedicated record.

- [ ] **Step 3: Prove destination A/B independence**

Use two fresh runs. Keep protocol.intake config and CoAP wire input byte-identical. Configure only the business CNet datagram Sink peer port differently.

```c
check_equal(run_a.receiver_a_datagrams, 1u);
check_equal(run_a.receiver_b_datagrams, 0u);
check_equal(run_b.receiver_a_datagrams, 0u);
check_equal(run_b.receiver_b_datagrams, 1u);
```

- [ ] **Step 4: Run acceptance**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_coap test_cnet_packet_source test_cnet_datagram_sink
ctest --preset win-dev-user -R \
  "^(test_protocol_network_coap|test_cnet_packet_source|test_cnet_datagram_sink)$" \
  --output-on-failure
```

- [ ] **Step 5: Commit**

```bash
git add ingress/protocol/tests/test_protocol_network_coap.c \
        ingress/protocol/tests/protocol_network_e2e_fixture.c \
        ingress/protocol/tests/CMakeLists.txt ingress/protocol/network/src
git commit -m "test(protocol): prove real CoAP UDP intake"
```

---

### Task 8: TurboDB parity, storage backpressure/failure, and restart recovery

**Files:**
- Create: `ingress/protocol/tests/protocol_network_turbodb_fixture.h`
- Create: `ingress/protocol/tests/protocol_network_turbodb_fixture.c`
- Create: `ingress/protocol/tests/test_protocol_network_turbodb.c`
- Modify: `ingress/protocol/tests/protocol_network_e2e_fixture.h`
- Modify: `ingress/protocol/tests/protocol_network_e2e_fixture.c`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Reuses public TurboDB Inbox v2 only; no production fault hook is introduced.

- [ ] **Step 1: Add exact test-only v2 SQLite fixture**

Copy the authoritative column/index contracts from `io/turbodb/tests/test_turbodb_inbox.c`, changing only the namespace consistently to `network`. Provision a file-backed SQLite DB, exact meta row, unique `(source_id, admission_id)` index, and phase index.

- [ ] **Step 2: Parameterize real-network harness by caller-selected Inbox**

Make the Task 6/7 harness accept `turbo_flow_inbox_t *`. Run one JT/T808 path and one CoAP path on memory, then the same two paths on TurboDB. Shared assertions are one admission, no business work before claim, one business UDP result, and valid post-stop admitted record state.

- [ ] **Step 3: Write writer-lock RED for explicit backpressure and Source-poll gating**

With a live TurboDB Inbox, open a second ORM connection, begin a serializable transaction, and execute this no-op write inside it:

```sql
UPDATE network_inbox_meta_v2
SET admitted = admitted
WHERE singleton_id = 1
```

Send one real valid frame and poll until intake reports `backpressured`. Save `uint64_t blocked_source_polls = snapshot.source_polls`. While the writer lock remains held, call public intake `poll()` ten times with timeout zero and assert after every call:

```c
check_true(snapshot.backpressured);
check_equal(snapshot.source_polls, blocked_source_polls);
check_equal(business_stage_calls, 0u);
```

Rollback the external writer transaction, poll again, then assert TurboDB contains exactly one new record and the retained frame was not duplicated.

- [ ] **Step 4: Write non-capacity storage-failure RED with no alternate provider**

Use a fresh temp DB. After `turbo_flow_turbodb_inbox_create()` succeeds, use a second connection to execute:

```sql
DROP TABLE network_inbox_records_v2
```

Create a separate memory Inbox but do not pass it to ProtocolNetworkIntake. Snapshot its `admitted` counter before the network send. Send one real valid frame and poll until the storage operation returns a non-capacity error. Assert intake state is FAILED, business Graph count remains zero, and the unused memory Inbox `admitted` counter is unchanged.

This proves the high-level owner cannot choose an alternate provider: its public config contains exactly one Inbox handle.

- [ ] **Step 5: Add clean-close/reopen recovery**

```text
real network frame -> committed TurboDB record
stop/destroy ProtocolNetworkIntake
close/destroy TurboDB Inbox
reopen same namespace/file with EXCLUSIVE mode
start business generation + InboxSource
process exactly one recovered record
observe exactly one UDP business datagram
```

Do not duplicate takeover testing already covered by `test_turbodb_inbox`.

- [ ] **Step 6: Run GREEN**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_turbodb test_turbodb_inbox test_turbodb_inbox_source
ctest --preset win-dev-user -R \
  "^(test_protocol_network_turbodb|test_turbodb_inbox|test_turbodb_inbox_source)$" \
  --output-on-failure
```

- [ ] **Step 7: Commit**

```bash
git add ingress/protocol/tests/protocol_network_turbodb_fixture.h \
        ingress/protocol/tests/protocol_network_turbodb_fixture.c \
        ingress/protocol/tests/test_protocol_network_turbodb.c \
        ingress/protocol/tests/protocol_network_e2e_fixture.h \
        ingress/protocol/tests/protocol_network_e2e_fixture.c \
        ingress/protocol/tests/CMakeLists.txt
git commit -m "test(protocol): prove durable network intake"
```

---

### Task 9: Install/export ProtocolNetworkIntake and prove dependency closure

**Files:**
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Modify: `CMakeLists.txt`
- Create: `tests/install_protocol_network_intake_consumer/CMakeLists.txt`
- Create: `tests/install_protocol_network_intake_consumer/main.c`
- Create: `tests/install_protocol_network_intake_consumer/header.cpp`
- Create: `tests/install_protocol_network_intake_consumer/run.cmake`

**Interfaces:**
- Produces installed `TurboFlow::ProtocolNetworkIntake` component.
- Does not make NetworkIntake a plugin and does not add plugin exports.

- [ ] **Step 1: Write install-consumer RED**

```cmake
cmake_minimum_required(VERSION 3.20)
project(TurboFlowProtocolNetworkIntakeConsumer LANGUAGES C CXX)
find_package(TurboFlow CONFIG REQUIRED COMPONENTS ProtocolNetworkIntake)
add_executable(protocol_network_intake_consumer main.c header.cpp)
target_link_libraries(protocol_network_intake_consumer PRIVATE TurboFlow::ProtocolNetworkIntake)
```

`main.c` includes only installed `turbo_flow_protocol_network_intake.h` and validates both init macro versions. `header.cpp` includes the same public header from C++.

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset install-win-dev-user
ctest --preset win-dev-user -R "protocol_network_intake.*install" --output-on-failure
```

Expected: package does not yet recognize `ProtocolNetworkIntake`.

- [ ] **Step 3: Register package component**

Add `ProtocolNetworkIntake` to `_TurboFlow_supported_components`, `_TurboFlow_salts_utils_components`, and `_TurboFlow_rules_forge_components`. Do not add it to `_TurboFlow_needs_turbodb`, because the owner accepts any caller-supplied Inbox and does not require TurboDB at link/package time.

Register the fresh-install CTest using the same isolated install-prefix pattern as existing consumers.

- [ ] **Step 4: Add explicit dependency/export closure assertions**

The package gate must verify:

```text
ProtocolNetworkIntake -> Graph
ProtocolNetworkIntake -> PluginHost
ProtocolNetworkIntake -> CNetAdapter
ProtocolNetworkIntake -> ProtocolIngressInbox
Graph does not depend on ProtocolNetworkIntake
ProtocolIngress does not depend on CNetAdapter
JT/T808 and CoAP plugin DLLs do not depend on ProtocolNetworkIntake
```

Use the repository's existing platform dependency-inspection mechanism. Also verify the JT/T808, CoAP, and CNet plugin DLLs retain only the canonical plugin discovery export; `tf_protocol_network_intake` is an ordinary shared library.

- [ ] **Step 5: Run GREEN**

```bash
cmake --build --preset install-win-dev-user
ctest --preset win-dev-user -R "protocol_network_intake.*install" --output-on-failure
```

- [ ] **Step 6: Commit**

```bash
git add cmake/TurboFlowConfig.cmake.in CMakeLists.txt \
        tests/install_protocol_network_intake_consumer
git commit -m "build: install protocol network intake component"
```

---

### Task 10: Exact-head full verification, documentation, and #118 evidence

**Files:**
- Modify after all gates pass: `ingress/protocol/README.md`
- Update GitHub issue #118 only with verified exact-head evidence.

**Interfaces:**
- Produces merge-ready evidence; no new behavior.

- [ ] **Step 1: Run formatting/diff checks**

```bash
clang-format --dry-run --Werror \
  io/cnet/include/turbo_flow_cnet.h \
  io/cnet/src/turbo_flow_cnet_listener_source.c \
  io/cnet/src/turbo_flow_cnet_plugin.c \
  ingress/protocol/network/include/turbo_flow_protocol_network_intake.h \
  ingress/protocol/network/src/*.c \
  ingress/protocol/network/src/*.h \
  ingress/protocol/tests/test_protocol_network_*.c \
  ingress/protocol/tests/protocol_network_*.c \
  ingress/protocol/tests/protocol_network_*.h
git diff --check
```

- [ ] **Step 2: Run Debug/ASan focused gate**

```bash
cmake --preset win-dev-user
cmake --build --preset win-dev-user --parallel
ctest --preset win-dev-user -R \
  "(protocol_network|protocol_source|protocol_inbox|flow_inbox_source|cnet_listener_source|cnet_packet_source|cnet_datagram_sink|cnet_plugin|turbodb_inbox)" \
  --output-on-failure
```

- [ ] **Step 3: Run Release full suite**

```bash
cmake --preset win-release-user
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user --output-on-failure
```

- [ ] **Step 4: Run fresh install/package gate**

```bash
cmake --build --preset install-win-dev-user
ctest --preset win-dev-user -R "install" --output-on-failure
```

- [ ] **Step 5: Check every acceptance item against a named test**

Record exact test/run evidence for:

```text
public intake ABI/lifecycle
listener message context
configured Source actual endpoint
real JT/T808/TCP
real CoAP/UDP
KCP preflight rejection
store-before-business-Graph
TurboDB lock backpressure
non-capacity TurboDB failure/no alternate Inbox
retained-frame exactly-once retry
source_polls frozen while blocked
partial TCP claim completion without Inbox admission
TCP generation reuse
stale UDP generation
pending claim/byte bound
deterministic replay with timestamp_ns=0
business Graph failure settlement from existing test_flow_inbox_source
stop cancellation preserving admitted records
catalog/module pin/unload
memory/TurboDB parity
TurboDB clean reopen
business destination A/B independence
installed C/C++ consumer
dependency/export/profile checks
Debug/ASan and Release full suites
```

If any line lacks evidence, leave #118 open and report that exact missing gate.

- [ ] **Step 6: Update protocol documentation**

Document only the proven installed path:

```text
configured CNet Source -> ProtocolNetworkIntake -> ProtocolSource -> ProtocolInbox -> Inbox
InboxSource -> shared business Graph -> configured CNet Sink
```

State that FlowMQ #74, Flowie #115, RulesForge DLL #73, and KCP intake remain separate.

- [ ] **Step 7: Update #118**

Post exact head SHA, named focused tests, Release full-suite count/result, install result, dependency/export result, and remaining non-goals. Close #118 only when the live issue has no independent unchecked requirement beyond the approved two-protocol boundary.

- [ ] **Step 8: Commit docs**

```bash
git add ingress/protocol/README.md
git commit -m "docs: record real protocol intake completion"
```

Do not commit build artifacts, `.codegraph/`, local SQLite files, or temporary socket files.
