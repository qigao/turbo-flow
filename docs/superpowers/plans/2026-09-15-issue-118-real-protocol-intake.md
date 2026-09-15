# Issue #118 Real Protocol Network Intake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用安装态 `TurboFlow::ProtocolNetworkIntake` 完成 JT/T 808/TCP 与 CoAP/UDP 两条真实 Source -> ProtocolSource -> ProtocolInbox -> Inbox -> shared business Graph -> configured CNet Sink 路径，并保持 storage-before-business、bounded backpressure、generation fencing 和 no-fallback 语义。

**Architecture:** 新增一个位于 Graph/CNet/Protocol/PluginHost 之上的高层 owner。它从同一 retained PluginHost catalog 中解析并 materialize 配置的 CNet Source Product owner、打开配置的 protocol provider，把 CNet message 送入 internal managed async-terminal intake Sink；完整 frame 只有在 `ProtocolInbox` admission 成功后才完成对应 async claim。business Graph 仍只由现有 `turbo_flow_inbox_source_t` 驱动，CNet datagram Sink 仍由普通 plugin generation materialize。

**Tech Stack:** C11/C17、C++17 header probes、TurboFlow Graph/PluginHost、Salts CFlow/CNet/DataBind、TurboDB ORM/SQLite、xxHash3-128、TinyTest、CMake presets。

**Spec:** `docs/superpowers/specs/2026-09-15-issue-118-real-protocol-intake-design.md`

## Global Constraints

- 执行前先使用 `superpowers:using-git-worktrees` 创建/确认隔离工作区；实现分支从批准 spec 的 exact head 开始。
- 只支持 #118 已批准的两组 v1 pairing：`jtt808 + cnet.listener_source(TCP)` 与 `coap + cnet.packet_source(packet_mode=udp)`；KCP/secure-KCP 必须在 preflight fail closed。
- 不修改 Inbox v2 ABI/schema，不新增 database->memory fallback，不恢复 protocol->Graph 直连路径。
- 不扩 PluginHost root ABI，不扩 generic Product owner vtable，不增加 raw-feed public API。
- `ProtocolNetworkIntake` 只拥有 intake plumbing Flow、CNet Source Product owner、protocol owner/registry、ProtocolSource/ProtocolInbox、parser/pending-claim state；它借用 caller-owned Inbox。
- `resolved` 只在 create/preflight 期间借用并复制所需字段；catalog snapshot 必须 retain 到所有 Source/protocol callback 完全终止。
- partial TCP bytes 可在复制进 bounded parser buffer 后完成 upstream async claim；包含完整但尚未写入 Inbox 的 frame 不得提前完成 claim。
- backpressure 时 `ProtocolNetworkIntake::poll()` 不再调用 CNet Source Product owner `poll()`；只做 documented empty-feed retry 与 FIFO pending-claim drain。
- production positive tests 必须使用真实 CNet Source/Sink、真实 protocol DLL、真实 memory/TurboDB Inbox；fixture 只能用于 focused contract/fault tests。
- 不使用 legacy alias、兼容目录、静态替代 provider、候选 DLL 搜索或 CMake/runtime fallback。
- 每个 task 先提交 RED，再只做该 task 的最小 GREEN；相邻失败不顺手修。

---

## File Structure

### New production files

- `ingress/protocol/network/CMakeLists.txt` — internal core + installed `tf_protocol_network_intake` target。
- `ingress/protocol/network/include/turbo_flow_protocol_network_intake.h` — size/versioned public owner API。
- `ingress/protocol/network/src/flow_protocol_network_intake_internal.h` — private settings、sink、metrics contracts。
- `ingress/protocol/network/src/flow_protocol_network_intake_config.c` — exact `protocol.intake` + Source config/topology preflight。
- `ingress/protocol/network/src/flow_protocol_network_intake_sink.c` — async-terminal claim ownership、parser session mapping、identity、retry queue。
- `ingress/protocol/network/src/flow_protocol_network_intake.c` — catalog/provider assembly、public lifecycle、Source owner poll ownership。

### New test/support files

- `ingress/protocol/tests/test_protocol_network_intake_config.c` — internal exact-config/topology RED/GREEN。
- `ingress/protocol/tests/test_protocol_network_intake_core.c` — async Sink、partial frame、capacity retry、generation tests。
- `ingress/protocol/tests/test_protocol_network_intake.c` — public owner ABI/lifecycle/catalog rollback tests。
- `ingress/protocol/tests/protocol_network_intake_header_cpp.cpp` — installed/public C++ header probe。
- `ingress/protocol/tests/protocol_network_source_fixture.c` — canonical-export transactional Source fixture for focused lifecycle faults only。
- `ingress/protocol/tests/protocol_network_e2e_fixture.h`
- `ingress/protocol/tests/protocol_network_e2e_fixture.c` — cross-platform socket/config/business-driver harness shared by real network tests。
- `ingress/protocol/tests/test_protocol_network_jtt808.c` — real fragmented TCP/JT808 path。
- `ingress/protocol/tests/test_protocol_network_coap.c` — real UDP/CoAP + destination independence path。
- `ingress/protocol/tests/protocol_network_turbodb_fixture.h`
- `ingress/protocol/tests/protocol_network_turbodb_fixture.c` — pre-provisioned v2 SQLite fixture and writer-lock helper。
- `ingress/protocol/tests/test_protocol_network_turbodb.c` — real network/TurboDB parity, lock/no-fallback, restart recovery。
- `tests/install_protocol_network_intake_consumer/CMakeLists.txt`
- `tests/install_protocol_network_intake_consumer/main.c`
- `tests/install_protocol_network_intake_consumer/header.cpp`
- `tests/install_protocol_network_intake_consumer/run.cmake`

### Existing files to modify

- `io/cnet/include/turbo_flow_cnet.h`
- `io/cnet/src/turbo_flow_cnet_listener_source.c`
- `io/cnet/tests/test_cnet_listener_source.c`
- `io/cnet/tests/cnet_stream_source_header_cpp.cpp`
- `ingress/protocol/CMakeLists.txt`
- `ingress/protocol/tests/CMakeLists.txt`
- `CMakeLists.txt`
- `cmake/TurboFlowConfig.cmake.in`
- `README.md` or `ingress/protocol/README.md` only in the final documentation task after all behavior gates are GREEN。

---

### Task 1: Add message-owned CNet listener connection identity

**Files:**
- Modify: `io/cnet/include/turbo_flow_cnet.h`
- Modify: `io/cnet/src/turbo_flow_cnet_listener_source.c`
- Modify: `io/cnet/tests/test_cnet_listener_source.c`
- Modify: `io/cnet/tests/cnet_stream_source_header_cpp.cpp`

**Interfaces:**
- Produces:
  - `turbo_flow_cnet_listener_message_context_t`
  - `turbo_flow_cnet_listener_message_context(const turbo_flow_msg_t *)`
- Later tasks use `context->connection.slot` and `context->connection.generation` as the only TCP parser-session transport identity.

- [ ] **Step 1: Write the compile/runtime RED**

Extend the existing C++ probe and listener graph probe before adding production declarations:

```cpp
extern "C" int cnet_listener_source_header_cpp_probe(void) {
  turbo_flow_cnet_listener_source_config_t config = TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_snapshot_t snapshot =
      TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
  turbo_flow_cnet_listener_message_context_t context = {0};
  return config.version == TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION &&
                 context.size == 0u
             ? 0
             : 1;
}
```

Change `listener_source_graph_probe_t` to retain contexts and require the accessor in the sink:

```c
typedef struct listener_source_graph_probe_s {
  size_t count;
  uint64_t ids[4];
  char payloads[4][32];
  turbo_flow_cnet_listener_message_context_t contexts[4];
} listener_source_graph_probe_t;

static int listener_source_graph_sink(turbo_flow_msg_t *message, void *ctx) {
  listener_source_graph_probe_t *probe = ctx;
  const turbo_flow_cnet_listener_message_context_t *transport =
      turbo_flow_cnet_listener_message_context(message);
  if (!probe || !transport || transport->size != sizeof(*transport) ||
      transport->version != TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION)
    return SALTS_EPROTO;
  probe->contexts[probe->count] = *transport;
  /* keep the existing payload/id assertions and increment */
  return SALTS_OK;
}
```

Also add a malformed-message accessor case where `transport_context` is outside `message->buffer`; expect NULL.

- [ ] **Step 2: Run the focused target and verify RED**

Run:

```bash
cmake --build --preset win-dev-user --target test_cnet_listener_source
```

Expected: compile failure on the missing `turbo_flow_cnet_listener_message_context_t` / accessor. Do not patch any protocol code yet.

- [ ] **Step 3: Add the exact public CNet projection**

In `turbo_flow_cnet.h` add:

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

In `listener_source_on_receive()` allocate context + payload, never a second mutable connection state:

```c
size_t buffer_size;
turbo_flow_cnet_listener_message_context_t *transport;

if (view->size > SIZE_MAX - sizeof(*transport)) {
  listener_source_fail(source, SALTS_ERANGE, 0, "receive_size");
  return;
}
buffer_size = sizeof(*transport) + view->size;
buffer = mem_get_buffer(mem_global(), buffer_size);
if (!buffer) {
  listener_source_fail(source, SALTS_ENOMEM, 0, "receive_copy");
  return;
}
transport = (turbo_flow_cnet_listener_message_context_t *)mem_buffer_data(buffer);
memset(transport, 0, sizeof(*transport));
transport->size = sizeof(*transport);
transport->version = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION;
transport->connection = connection;
memcpy(mem_buffer_data(buffer) + sizeof(*transport), view->data, view->size);
mem_set_used(buffer, buffer_size);
source->ready_message.buffer = buffer;
source->ready_message.payload =
    vstr_from_buf(mem_buffer_const_data(buffer) + sizeof(*transport), view->size);
source->ready_message.transport_context = transport;
```

Implement the accessor with the same structural rules as `turbo_flow_cnet_packet_message_context()`:

```c
const turbo_flow_cnet_listener_message_context_t *
turbo_flow_cnet_listener_message_context(const turbo_flow_msg_t *message) {
  const char *base;
  const turbo_flow_cnet_listener_message_context_t *context;
  uintptr_t base_address, context_address;
  size_t used, offset;
  if (!message || !message->buffer || !message->transport_context) return NULL;
  base = mem_buffer_const_data(message->buffer);
  used = mem_buffer_used(message->buffer);
  base_address = (uintptr_t)base;
  context_address = (uintptr_t)message->transport_context;
  if (!base || context_address < base_address) return NULL;
  offset = (size_t)(context_address - base_address);
  if (offset > used || used - offset < sizeof(*context)) return NULL;
  context = (const turbo_flow_cnet_listener_message_context_t *)message->transport_context;
  if (context->size != sizeof(*context) ||
      context->version != TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION ||
      context->connection.generation == 0u)
    return NULL;
  if (message->payload.data != base + offset + context->size ||
      message->payload.len > used - offset - context->size)
    return NULL;
  return context;
}
```

- [ ] **Step 4: Run focused CNet regression**

```bash
cmake --build --preset win-dev-user --target test_cnet_listener_source test_cnet_packet_source
ctest --preset win-dev-user -R "^(test_cnet_listener_source|test_cnet_packet_source)$" --output-on-failure
```

Expected: both PASS; existing payload text remains unchanged even though buffer offset changed.

- [ ] **Step 5: Commit**

```bash
git add io/cnet/include/turbo_flow_cnet.h \
        io/cnet/src/turbo_flow_cnet_listener_source.c \
        io/cnet/tests/test_cnet_listener_source.c \
        io/cnet/tests/cnet_stream_source_header_cpp.cpp
git commit -m "feat(cnet): preserve listener message connection identity"
```

---

### Task 2: Freeze exact `protocol.intake` config and two-stage topology internally

**Files:**
- Create: `ingress/protocol/network/CMakeLists.txt`
- Create: `ingress/protocol/network/src/flow_protocol_network_intake_internal.h`
- Create: `ingress/protocol/network/src/flow_protocol_network_intake_config.c`
- Create: `ingress/protocol/tests/test_protocol_network_intake_config.c`
- Modify: `ingress/protocol/CMakeLists.txt`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Produces internal-only:

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

- Later tasks must not reparse YAML or use CNet private config structs; they consume only `settings` plus public resolved/catalog APIs.

- [ ] **Step 1: Add config/topology RED cases**

Create `test_protocol_network_intake_config.c` with one valid parsed graph:

```c
static const char intake_graph[] =
    "source wire adapter tcp.input\n"
    "stage durable adapter protocol.store\n"
    "stage main {\n"
    "  wire -> durable\n"
    "}\n";
```

Use resolved YAML containing a full existing CNet listener config and:

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

RED assertions:

```c
check_equal(flow_protocol_network_intake_preflight(
                resolved, flow, "tcp.input", "protocol.store", &settings, &error),
            SALTS_OK); /* initially fails to link: implementation absent */
```

Then add independent negative fixtures for:
- unknown/missing intake field -> `SALTS_EINVAL`;
- `protocol_provider=coap` with listener Source -> `SALTS_EINVAL`;
- JTT808 version other than `2019-A1` -> `SALTS_ENOTSUP`;
- CoAP packet Source with `packet_mode=kcp` -> `SALTS_ENOTSUP`;
- zero `max_sessions/max_pending_claims/max_pending_bytes` -> `SALTS_ERANGE`;
- `max_pending_claims < 2 * scheduler_max_steps_per_poll` -> `SALTS_ERANGE`;
- `max_pending_bytes < max_pending_claims * max_message_bytes` -> `SALTS_ERANGE`;
- a third business stage in the intake Flow -> `SALTS_EINVAL`;
- wrong adapter names or non-PARSED Flow -> `SALTS_EINVAL`.

- [ ] **Step 2: Build and verify RED**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake_config
```

Expected: missing internal symbols/target implementation, not a CNet or protocol DLL failure.

- [ ] **Step 3: Implement exact settings parser**

Use exactly this allowed field table:

```c
static const char *const intake_fields[] = {
    "schema_version",      "protocol_provider", "protocol_kind",
    "protocol_version",    "source_id",         "max_sessions",
    "max_frame_size",      "max_pending_claims", "max_pending_bytes"};
```

Reject any field not present in that set and require the count to equal the array count. Parse `protocol_kind` only as `jtt808` or `coap`.

Read Source bounds through public resolved-config accessors:

```c
if (strcmp(source.kind, "cnet.listener_source") == 0) {
  settings->transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP;
  rc = turbo_flow_resolved_adapter_get_u64(&source, "max_connections", &transport_capacity);
} else if (strcmp(source.kind, "cnet.packet_source") == 0) {
  const char *mode = NULL;
  settings->transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP;
  rc = turbo_flow_resolved_adapter_get_string(&source, "packet_mode", &mode);
  if (rc == SALTS_OK && strcmp(mode, "udp") != 0) rc = SALTS_ENOTSUP;
  if (rc == SALTS_OK)
    rc = turbo_flow_resolved_adapter_get_u64(&source, "session_capacity", &transport_capacity);
} else {
  rc = SALTS_ENOTSUP;
}
```

Check the conservative pending bound with overflow guards before multiplication:

```c
if (steps > SIZE_MAX / 2u) return intake_config_error(..., SALTS_ERANGE, ...);
required_claims = steps * 2u;
if (settings->max_pending_claims < required_claims) return ...;
if (settings->source_max_message_bytes > SIZE_MAX / settings->max_pending_claims) return ...;
required_bytes = settings->source_max_message_bytes * settings->max_pending_claims;
if (settings->max_pending_bytes < required_bytes) return ...;
```

Validate exact pairings and exact two-stage Source -> terminal Sink topology using public stage-plan inspection. Do not compile or register adapters in this function.

- [ ] **Step 4: Add the private core CMake target and run GREEN**

In `ingress/protocol/network/CMakeLists.txt` start with an internal static target only:

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

Add `add_subdirectory(network)` after `common`/`inbox` in `ingress/protocol/CMakeLists.txt`.

Run:

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake_config
ctest --preset win-dev-user -R "^test_protocol_network_intake_config$" --output-on-failure
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ingress/protocol/network \
        ingress/protocol/CMakeLists.txt \
        ingress/protocol/tests/CMakeLists.txt \
        ingress/protocol/tests/test_protocol_network_intake_config.c
git commit -m "test(protocol): freeze network intake configuration"
```

---

### Task 3: Implement the bounded async-terminal protocol intake engine

**Files:**
- Create: `ingress/protocol/network/src/flow_protocol_network_intake_sink.c`
- Create: `ingress/protocol/tests/test_protocol_network_intake_core.c`
- Modify: `ingress/protocol/network/src/flow_protocol_network_intake_internal.h`
- Modify: `ingress/protocol/network/CMakeLists.txt`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Consumes `flow_protocol_network_intake_settings_t` from Task 2.
- Produces internal-only:

```c
typedef struct flow_protocol_network_intake_sink_s
    flow_protocol_network_intake_sink_t;

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
int flow_protocol_network_intake_sink_register(
    flow_protocol_network_intake_sink_t *sink);
int flow_protocol_network_intake_sink_retry(
    flow_protocol_network_intake_sink_t *sink);
void flow_protocol_network_intake_sink_cancel(
    flow_protocol_network_intake_sink_t *sink, int status);
void flow_protocol_network_intake_sink_metrics(
    const flow_protocol_network_intake_sink_t *sink,
    size_t *active_sessions, size_t *pending_claims,
    size_t *pending_bytes, uint64_t *frames_admitted, int *backpressured,
    int *terminal_status);
void flow_protocol_network_intake_sink_destroy(
    flow_protocol_network_intake_sink_t *sink);
```

- [ ] **Step 1: Write focused RED for partial-frame ownership and capacity retry**

Build a tiny parsed Flow with logical source -> internal intake Sink. Create a test `turbo_flow_protocol_t` using `turbo_flow_protocol_create()` with kind JTT808 and a deterministic inspect callback.

Construct a listener-owned message buffer exactly as CNet will:

```c
static turbo_flow_msg_t listener_message(uint64_t id, uint32_t slot, uint32_t generation,
                                         const uint8_t *data, size_t size) {
  turbo_flow_msg_t message;
  mem_buffer_t *buffer = mem_get_buffer(mem_global(),
                                        sizeof(turbo_flow_cnet_listener_message_context_t) + size);
  turbo_flow_cnet_listener_message_context_t *context =
      (turbo_flow_cnet_listener_message_context_t *)mem_buffer_data(buffer);
  context->size = sizeof(*context);
  context->version = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION;
  context->connection.slot = slot;
  context->connection.generation = generation;
  memcpy(mem_buffer_data(buffer) + sizeof(*context), data, size);
  mem_set_used(buffer, sizeof(*context) + size);
  turbo_flow_msg_init(&message);
  message.id = id;
  message.buffer = buffer;
  message.transport_context = context;
  message.payload = vstr_from_buf(mem_buffer_const_data(buffer) + sizeof(*context), size);
  return message;
}
```

Required RED behavior:

```c
/* first half: copied parser state, async publication completes, no Inbox record */
check_equal(turbo_flow_publish_async(flow, "wire", &first_half, completion_cb, &first), SALTS_OK);
wait_for_completion(&first);
check_equal(first.status, SALTS_OK);
check_equal(inbox_snapshot.pending_records, 0u);

/* prefill 1-slot Inbox; second half completes frame but publication stays pending */
check_equal(turbo_flow_publish_async(flow, "wire", &second_half, completion_cb, &second), SALTS_OK);
check_equal(second.calls, 0u);
check_true(backpressured);

/* free capacity, retry with no re-feed; one and only one protocol record appears */
check_equal(flow_protocol_network_intake_sink_retry(sink), SALTS_OK);
wait_for_completion(&second);
check_equal(second.status, SALTS_OK);
check_equal(protocol_record_count, 1u);
```

Also add RED cases for:
- one message containing two JTT frames where the second blocks;
- queued second async claim while first frame is blocked;
- slot generation N partial then N+1 bytes -> no cross-generation completion;
- crafted older generation/non-increasing message id -> `SALTS_EPROTO`;
- malformed/missing CNet transport context;
- CoAP packet context -> canonical peer-derived device id;
- cancel -> every retained claim completes `SALTS_ECANCELED`, no unadmitted record appears.

- [ ] **Step 2: Run RED**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake_core
```

Expected: missing sink engine symbols.

- [ ] **Step 3: Implement fixed parser-session and pending-claim storage**

Use preallocated arrays only:

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

Map protocol session id from transport slot without persisting it:

```c
static int intake_protocol_session_id(uint32_t slot, uint64_t *out) {
  if (!out || (uint64_t)slot == UINT64_MAX) return SALTS_ERANGE;
  *out = (uint64_t)slot + 1u;
  return *out == 0u ? SALTS_ERANGE : SALTS_OK;
}
```

On generation change, call `turbo_flow_protocol_source_session_close()` before the new `session_open()`. Reject non-increasing message IDs for a different generation instead of reopening stale parser state.

- [ ] **Step 4: Implement canonical CoAP device and durable admission identity**

Do not persist raw packet structs. Format peer identity deterministically from bytes:

```c
/* IPv4 example: udp4-7f000001-54321; IPv6 uses 32 hex digits plus port/scope. */
static int intake_packet_device_id(const cnet_packet_session_info *info,
                                   char out[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u]);
```

Create `ProtocolInbox` with an identity resolver that hashes the complete raw-preserved payload:

```c
XXH128_hash_t digest = XXH3_128bits(request->message->payload,
                                    request->message->payload_size);
int written = snprintf(admission_storage, sizeof(admission_storage),
                       "%s/%s/%" PRIu32 "/%" PRIu64 "/%016" PRIx64 "%016" PRIx64,
                       turbo_flow_protocol_kind_name(metadata->protocol),
                       metadata->device_id, metadata->message_type, metadata->sequence,
                       digest.high64, digest.low64);
```

Return `source_id` from the copied settings, correlation from metadata, sequence from metadata, and a nonzero host admission timestamp. Never include CNet generation in `admission_id`.

- [ ] **Step 5: Implement async submit/retry/cancel rules**

The submit callback always moves an accepted claim first:

```c
turbo_flow_async_terminal_claim_t owned = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
int rc = turbo_flow_async_terminal_claim_move(&owned, claim);
if (rc != SALTS_OK) return rc;
```

If not blocked, feed only the retained claim's message. On capacity result, store `owned` as the blocking claim. If already blocked, enqueue `owned` without feeding. On immediate success/partial copy, call:

```c
(void)turbo_flow_async_terminal_complete(&owned, SALTS_OK, NULL);
```

On non-capacity failure:

```c
sink->terminal_status = rc;
(void)turbo_flow_async_terminal_complete(&owned, rc, NULL);
```

`retry()` must call only an empty feed for the blocked session, then drain pending claims FIFO until empty or another capacity block.

- [ ] **Step 6: Register as a managed async-terminal Sink**

Use `turbo_flow_register_managed_async_terminal_adapter()` with:

```c
schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
schema.roles = TURBO_FLOW_ADAPTER_SINK;
schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
async_ops.submit = intake_async_submit;
registration.adapter_name = sink->adapter_name;
registration.owner_name = sink->adapter_name;
registration.ctx = sink;
```

The boundary is read-only management metadata; no generic resource command is added.

- [ ] **Step 7: Run focused GREEN**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_intake_core test_protocol_source test_protocol_inbox
ctest --preset win-dev-user -R \
  "^(test_protocol_network_intake_core|test_protocol_source|test_protocol_inbox)$" \
  --output-on-failure
```

Expected: all PASS; especially the retained-frame test shows no duplicate admission.

- [ ] **Step 8: Commit**

```bash
git add ingress/protocol/network/src \
        ingress/protocol/network/CMakeLists.txt \
        ingress/protocol/tests/CMakeLists.txt \
        ingress/protocol/tests/test_protocol_network_intake_core.c
git commit -m "feat(protocol): add bounded network intake sink"
```

---

### Task 4: Add the complete installed `ProtocolNetworkIntake` owner and catalog lifecycle

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
- Public API must match the approved spec exactly:
  - `turbo_flow_protocol_network_intake_create`
  - `start`
  - `poll`
  - `snapshot`
  - `stop`
  - `destroy`
- Consumes internal preflight/settings and sink from Tasks 2–3.
- Retains one catalog reference for Source provider callback lifetime; protocol registry retains its own catalog reference.

- [ ] **Step 1: Write public ABI/lifecycle RED**

The C++ probe must compile the exact public structures:

```cpp
#include "turbo_flow_protocol_network_intake.h"
extern "C" int protocol_network_intake_header_cpp_probe(void) {
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  turbo_flow_protocol_network_intake_snapshot_t snapshot =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  return config.version == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION
             ? 0
             : 1;
}
```

Create a canonical-export fixture plugin that registers only one transactional adapter provider named `cnet.listener_source`. Its owner vtable must be complete and external-poll capable:

```c
descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD |
                   TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
descriptor.ctx = owner;
descriptor.quiesce = fixture_quiesce;
descriptor.drain = fixture_drain;
descriptor.shutdown = fixture_shutdown;
descriptor.destroy = fixture_destroy;
descriptor.poll = fixture_poll;
```

The fixture materializer registers a SOURCE adapter with the requested configured name but performs no network I/O. Compile variants for poll failure / materialize failure without extra DLL exports.

RED cases:
- valid fixture + real JTT808 protocol module creates COMPILED owner and moves `flow_io` to NULL;
- invalid public sizes/versions leave `flow_io` and `out` unchanged;
- Source preflight failure happens before `flow_io` ownership transfer;
- protocol name/version mismatch happens before Source materialize;
- materialize failure returns no public owner and destroys copied protocol/sink state;
- host destroy returns `SALTS_EBUSY` after caller releases its snapshot reference while intake still retains one;
- `poll()` before start -> `SALTS_EBUSY`;
- fixture poll error -> intake FAILED with exact status;
- stop from COMPILED performs no network start and reaches STOPPED;
- destroy before stop -> `SALTS_EBUSY`;
- after stop/destroy, PluginHost can unload fixture + protocol modules.

- [ ] **Step 2: Verify RED**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake
```

Expected: missing public target/header/symbols.

- [ ] **Step 3: Implement public owner shape and provider lookup**

Internal owner fields:

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
};
```

Find the configured Source transactional provider by exact `source_view.kind`; reject zero or duplicate matches.

Validate the returned Product owner exactly like generation semantics: exact ABI, exactly one threading flag, required quiesce/drain/shutdown/destroy, and external-poll implies non-NULL `poll`.

- [ ] **Step 4: Implement create with no network side effects**

Order:

```text
preflight settings/topology
project transactional catalog + Source provider preflight
retain snapshot
create protocol registry
create exact protocol owner and instance
create/register internal intake Sink
move *flow_io -> owner->flow
materialize configured Source Product owner into that Flow
compile Flow
publish COMPILED owner
```

Protocol request is exact:

```c
turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
request.protocol = settings.protocol_kind;
request.protocol_version = settings.protocol_version;
request.max_frame_size = settings.max_frame_size;
```

No `turbo_flow_start()` occurs inside create.

If compile fails after CNet Source materialization, the Source has not been started, so destroy the Flow while the Source owner context still exists, invoke the unstarted owner cleanup, destroy Sink/protocol objects, and release the retained snapshot. Add a focused regression proving real CNet materialize + forced compile failure leaves PluginHost unloadable.

- [ ] **Step 5: Implement start/poll/snapshot**

`start()`:

```c
if (intake->state != TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED) return SALTS_EBUSY;
rc = turbo_flow_start(intake->flow);
intake->state = rc == SALTS_OK ? TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING
                               : TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED;
intake->status = rc;
return rc;
```

`poll()`:

```c
rc = flow_protocol_network_intake_sink_retry(intake->sink);
flow_protocol_network_intake_sink_metrics(..., &backpressured, &terminal_status);
if (terminal_status != SALTS_OK) return intake_fail(intake, terminal_status);
if (!backpressured && pending_claims == 0u)
  rc = intake->source_owner.poll(intake->source_owner.ctx, timeout_ms);
/* refresh metrics; BACKPRESSURED is a state projection, not an error */
```

Never call Source owner `poll()` while sink metrics report retained/queued claims.

- [ ] **Step 6: Implement stop/destroy in explicit lifecycle order**

`stop()` from COMPILED simply quiesces/destroys no native handle and marks STOPPED. From RUNNING/BACKPRESSURED:

```text
state=STOPPING
source_owner.quiesce(timeout)
sink_cancel(SALTS_ECANCELED)
turbo_flow_stop(flow)
source_owner.drain(timeout)
state=STOPPED
```

`destroy()` requires STOPPED, destroys the Flow first while adapter ctx is valid, then Source `shutdown/destroy`, Sink, protocol owner/registry, and retained catalog reference. It never closes/destroys caller-owned Inbox.

- [ ] **Step 7: Add installed shared target, but no package component yet**

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

Add `tf_protocol_network_intake` to the top-level export target list.

- [ ] **Step 8: Run focused lifecycle GREEN**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_intake
ctest --preset win-dev-user -R "^test_protocol_network_intake$" --output-on-failure
```

- [ ] **Step 9: Commit**

```bash
git add ingress/protocol/network \
        ingress/protocol/tests/test_protocol_network_intake.c \
        ingress/protocol/tests/protocol_network_intake_header_cpp.cpp \
        ingress/protocol/tests/protocol_network_source_fixture.c \
        ingress/protocol/tests/CMakeLists.txt CMakeLists.txt
git commit -m "feat(protocol): add configured network intake owner"
```

---

### Task 5: Prove real fragmented JT/T 808 over configured CNet TCP into memory Inbox

**Files:**
- Create: `ingress/protocol/tests/protocol_network_e2e_fixture.h`
- Create: `ingress/protocol/tests/protocol_network_e2e_fixture.c`
- Create: `ingress/protocol/tests/test_protocol_network_jtt808.c`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Test harness owns configured PluginHost + snapshot, memory Inbox, ProtocolNetworkIntake, business CNet datagram-Sink generation, InboxSource driver, and real loopback sockets.
- Production code changes are allowed only when this real gate exposes a behavior missing from Tasks 1–4; do not replace the network with a fixture.

- [ ] **Step 1: Build a reusable real-network harness**

Reuse the repository's existing CNet YAML field set (`backend`, client/socket/TLS fields, bounded Source/Sink tails) rather than inventing shortened CNet config.

The configured plugin manifest must load at least:

```yaml
plugins:
  - id: turbo-flow.cnet
    version: 1.0.0
    path: <FLOW_CNET_PLUGIN_PATH>
  - id: jtt808
    version: 1.0.0
    path: <FLOW_PROTOCOL_JTT808_MODULE>
```

Use the approved two-stage intake Flow:

```c
static const char intake_graph[] =
    "source wire adapter tcp.input\n"
    "stage durable adapter protocol.store\n"
    "stage main {\n"
    "  wire -> durable\n"
    "}\n";
```

Business Flow remains separate:

```c
static const char business_graph[] =
    "source inbox\n"
    "stage output adapter udp.output\n"
    "stage main {\n"
    "  inbox -> output\n"
    "}\n";
```

Materialize `udp.output` through normal `turbo_flow_plugin_generation_create()` and create `turbo_flow_inbox_source_t` against logical source `inbox`.

- [ ] **Step 2: Copy the existing canonical JTT808 frame builder into test support**

Use the already-conforming unescaped bytes from protocol tests:

```c
const uint8_t unescaped[] = {
    0x02u, 0x00u, 0x40u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u,
    0x00u, 0x00u, 0x00u, 0x01u, 0x23u, 0x45u, 0x00u, 0x01u};
```

Keep the checksum/0x7d escaping logic byte-for-byte equivalent to `test_protocol_plugins.c` so the real JTT DLL, not a test codec, validates it.

- [ ] **Step 3: Write the real TCP RED**

Test sequence:

```text
create memory Inbox
create configured PluginHost/snapshot
create + start ProtocolNetworkIntake
connect a real TCP client to configured listener port
send first half of one valid JTT808 frame
poll intake until bytes_received advances
assert Inbox pending_records == 0
send second half
poll intake until Inbox pending_records == 1
assert business UDP peer still received 0 datagrams
request InboxSource business run
poll business generation + InboxSource until settlement terminal
assert real UDP peer receives exactly one datagram
```

Add a Flow observer on the business Flow and assert its stage count remains zero before Inbox admission.

- [ ] **Step 4: Verify RED before changing production**

```bash
cmake --build --preset win-dev-user --target test_protocol_network_jtt808
ctest --preset win-dev-user -R "^test_protocol_network_jtt808$" --output-on-failure
```

Expected initial failure must point to a real composition/lifecycle bug if Tasks 1–4 are incomplete; do not replace the network gate with direct Sink calls.

- [ ] **Step 5: Make only the minimal production correction, if RED exposes one**

Allowed correction areas are only:
- Task 4 source-owner poll/start/stop sequencing;
- Task 3 async claim/parser semantics;
- Task 1 listener context validation.

Do not change JTT codec semantics or Inbox schema to satisfy the test.

- [ ] **Step 6: Run focused GREEN and adjacent gates**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_jtt808 test_cnet_listener_source test_protocol_plugins \
  test_protocol_source test_protocol_inbox test_flow_inbox_source
ctest --preset win-dev-user -R \
  "^(test_protocol_network_jtt808|test_cnet_listener_source|test_protocol_plugins|test_protocol_source|test_protocol_inbox|test_flow_inbox_source)$" \
  --output-on-failure
```

- [ ] **Step 7: Commit**

```bash
git add ingress/protocol/tests/protocol_network_e2e_fixture.* \
        ingress/protocol/tests/test_protocol_network_jtt808.c \
        ingress/protocol/tests/CMakeLists.txt \
        ingress/protocol/network/src
git commit -m "test(protocol): prove real JTT808 TCP intake"
```

---

### Task 6: Prove real CoAP/UDP and destination independence

**Files:**
- Create: `ingress/protocol/tests/test_protocol_network_coap.c`
- Modify: `ingress/protocol/tests/protocol_network_e2e_fixture.c`
- Modify: `ingress/protocol/tests/CMakeLists.txt`
- Modify production network-intake files only if the real gate exposes a scoped bug.

**Interfaces:**
- Reuses the Task 5 harness.
- Uses packet message context peer identity to supply CoAP `device_id`.

- [ ] **Step 1: Add exact CoAP/UDP config and KCP negative test**

Positive Source config must use:

```yaml
kind: cnet.packet_source
config:
  packet_mode: udp
  # all existing explicit empty KCP/security/FEC fields remain present
```

Intake config:

```yaml
protocol_provider: coap
protocol_kind: coap
protocol_version: RFC7252
```

Use the existing valid GET frame:

```c
static const uint8_t coap_get[] = {0x40u, 0x01u, 0x12u, 0x34u};
```

Before the positive path, change only `packet_mode` to `kcp` with a valid KCP policy and assert `turbo_flow_protocol_network_intake_create()` returns `SALTS_ENOTSUP` while the real Source has not opened a socket.

- [ ] **Step 2: Write real UDP RED**

Send `coap_get` from a bound real UDP peer to the configured packet Source. Poll intake until one Inbox record appears, then claim it directly once to verify decoded envelope metadata contains a nonempty canonical peer-derived device id; return the record to PENDING using the existing explicit failure/retry path or run this metadata check in a dedicated provider fixture so the business test still starts from PENDING.

Preferred no-state-distortion approach: decode the TBE envelope from a read-only provider claim in a dedicated test instance, complete it, and use a fresh instance for business delivery.

- [ ] **Step 3: Prove destination is configuration, not protocol**

Run the same CoAP input twice with identical business graph shape and payload but two resolved configs:

```text
run A -> udp.output peer_port = receiver_A
run B -> udp.output peer_port = receiver_B
```

Assertions:

```c
check_equal(receiver_a.datagrams, 1u);
check_equal(receiver_b.datagrams, 0u); /* after run A */
/* reset, run B */
check_equal(receiver_a.datagrams, 0u);
check_equal(receiver_b.datagrams, 1u);
```

The `protocol.intake` section is unchanged between A/B; only the business CNet datagram Sink destination changes.

- [ ] **Step 4: Add stale packet generation focused check**

Use Task 3 internal test support to enqueue a valid packet-context message for slot S/generation N+1, then a crafted older S/N message with a non-increasing message ID. Assert the stale claim completes `SALTS_EPROTO` and Inbox admission count does not increase.

The real positive CoAP test still uses the actual CNet packet Source.

- [ ] **Step 5: Run GREEN**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_coap test_cnet_packet_source test_cnet_datagram_sink
ctest --preset win-dev-user -R \
  "^(test_protocol_network_coap|test_cnet_packet_source|test_cnet_datagram_sink)$" \
  --output-on-failure
```

- [ ] **Step 6: Commit**

```bash
git add ingress/protocol/tests/test_protocol_network_coap.c \
        ingress/protocol/tests/protocol_network_e2e_fixture.* \
        ingress/protocol/tests/CMakeLists.txt \
        ingress/protocol/network/src
git commit -m "test(protocol): prove real CoAP UDP intake"
```

---

### Task 7: Run the same real network contract on TurboDB, including lock/no-fallback and restart recovery

**Files:**
- Create: `ingress/protocol/tests/protocol_network_turbodb_fixture.h`
- Create: `ingress/protocol/tests/protocol_network_turbodb_fixture.c`
- Create: `ingress/protocol/tests/test_protocol_network_turbodb.c`
- Modify: `ingress/protocol/tests/CMakeLists.txt`

**Interfaces:**
- Reuses public `TurboFlow::TurboDbAdapter` and exact v2 schema.
- Does not introduce a production failure hook.

- [ ] **Step 1: Create an exact v2 database fixture**

Copy the existing authoritative DDL from `io/turbodb/tests/test_turbodb_inbox.c` into one shared protocol-network test fixture, including:
- `<namespace>_inbox_meta_v2` exact columns;
- `<namespace>_inbox_records_v2` exact columns;
- unique `(source_id, admission_id)` index;
- phase index;
- exact v2 metadata row;
- file-backed SQLite config accepted by `turbo_flow_turbodb_inbox_create()`.

Use a protocol-specific namespace such as `network` consistently in table names and config.

- [ ] **Step 2: Parameterize the Task 5/6 harness over `turbo_flow_inbox_t *`**

The real TCP/JTT808 and UDP/CoAP routines must accept an already-created Inbox handle and make no provider-specific assumptions.

Run both paths once with memory and once with TurboDB. Assertions shared across providers:
- exactly one admission;
- business Graph starts only after claim;
- exactly one UDP business result;
- cancel/stop leaves admitted record state valid.

- [ ] **Step 3: Add real SQLite writer-lock RED for no fallback**

After creating the TurboDB Inbox, open a second ORM connection and hold a write transaction without changing logical metadata:

```c
check_equal(orm_transaction_begin(connection, ORM_ISOLATION_SERIALIZABLE,
                                  &lock_tx, &error), ORM_STATUS_OK);
/* acquire SQLite writer ownership with a no-op write */
inbox_db_execute_in_transaction(connection, lock_tx,
    "UPDATE network_inbox_meta_v2 SET admitted=admitted WHERE singleton_id=1", &error);
```

Also create an unused memory Inbox and snapshot it.

Send one valid real JTT808 or CoAP frame, then poll intake repeatedly while the DB writer lock is held:

```c
check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_OK);
check_true(snapshot.backpressured);
check_equal(business_stage_calls, 0u);
check_equal(memory_snapshot.admitted, 0u);
```

Rollback/release the external lock, poll again, and assert the same retained frame is admitted exactly once to TurboDB and memory remains zero.

- [ ] **Step 4: Add restart/recovery test**

Sequence:

```text
real network intake -> committed TurboDB record
stop/destroy ProtocolNetworkIntake
close/destroy TurboDB Inbox provider
reopen same pre-provisioned namespace from same file
create/start the same business Graph + InboxSource
claim/process record once
assert UDP result exactly once
assert no memory Inbox admission
```

Use exclusive reopen after clean close; takeover semantics remain covered by existing TurboDB tests and need not be reimplemented here.

- [ ] **Step 5: Run TurboDB focused and existing provider gates**

```bash
cmake --build --preset win-dev-user --target \
  test_protocol_network_turbodb test_turbodb_inbox test_turbodb_inbox_source
ctest --preset win-dev-user -R \
  "^(test_protocol_network_turbodb|test_turbodb_inbox|test_turbodb_inbox_source)$" \
  --output-on-failure
```

Expected: real network/TurboDB test GREEN; existing schema/takeover gates unchanged.

- [ ] **Step 6: Commit**

```bash
git add ingress/protocol/tests/protocol_network_turbodb_fixture.* \
        ingress/protocol/tests/test_protocol_network_turbodb.c \
        ingress/protocol/tests/protocol_network_e2e_fixture.* \
        ingress/protocol/tests/CMakeLists.txt
git commit -m "test(protocol): prove durable network intake"
```

---

### Task 8: Export/install the component and prove package/dependency closure

**Files:**
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Create: `tests/install_protocol_network_intake_consumer/CMakeLists.txt`
- Create: `tests/install_protocol_network_intake_consumer/main.c`
- Create: `tests/install_protocol_network_intake_consumer/header.cpp`
- Create: `tests/install_protocol_network_intake_consumer/run.cmake`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces installed component/target: `TurboFlow::ProtocolNetworkIntake`.
- No new runtime plugin export; protocol/CNet provider DLLs keep canonical `turbo_flow_plugin_get_api` only.

- [ ] **Step 1: Write package RED**

Consumer CMake:

```cmake
cmake_minimum_required(VERSION 3.20)
project(TurboFlowProtocolNetworkIntakeConsumer LANGUAGES C CXX)
find_package(TurboFlow CONFIG REQUIRED COMPONENTS ProtocolNetworkIntake)
add_executable(protocol_network_intake_consumer main.c header.cpp)
target_link_libraries(protocol_network_intake_consumer
  PRIVATE TurboFlow::ProtocolNetworkIntake)
```

`main.c` must include only installed headers and instantiate exact init values:

```c
#include <turbo_flow_protocol_network_intake.h>
int main(void) {
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  turbo_flow_protocol_network_intake_snapshot_t snapshot =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  return config.version == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION
             ? 0
             : 1;
}
```

C++ file includes the same header and uses no private build-tree include path.

- [ ] **Step 2: Run install consumer RED**

```bash
cmake --build --preset install-win-dev-user
ctest --preset win-dev-user -R "protocol_network_intake.*install" --output-on-failure
```

Expected before package update: unsupported `ProtocolNetworkIntake` component or missing imported target.

- [ ] **Step 3: Add package component/dependency declarations**

Append `ProtocolNetworkIntake` to `_TurboFlow_supported_components`.

Because its public headers/types use Graph/PluginHost and its installed shared library depends on the same SaltsUtils/RulesForge closure as Graph/CNet, add it to:

```cmake
_TurboFlow_salts_utils_components
_TurboFlow_rules_forge_components
```

Do **not** add it to `_TurboFlow_needs_turbodb`; the public owner accepts a caller-owned Inbox and does not require TurboDB unless the application explicitly requests `TurboDbAdapter`.

- [ ] **Step 4: Register the install CTest in top-level CMake**

Follow the existing installed-consumer pattern and run against the fresh install prefix, not the build tree.

- [ ] **Step 5: Verify dependency direction**

Check generated/imported link interfaces and platform DLL dependencies:

```text
ProtocolNetworkIntake -> Graph, PluginHost, CNetAdapter, ProtocolIngressInbox, xxHash/Salts runtime
Graph -X-> ProtocolNetworkIntake
ProtocolIngress -X-> CNetAdapter
Protocol DLLs -X-> ProtocolNetworkIntake
```

On Windows use the repository's existing dependency inspection scripts/tooling; on Unix use the existing CI closure mechanism. The test must fail if Graph gains a NetworkIntake/CNet/protocol DLL dependency.

Also assert every plugin DLL still exports only its canonical root discovery symbol for plugin registration; `tf_protocol_network_intake` is an ordinary installed library, not a plugin.

- [ ] **Step 6: Run install GREEN**

```bash
cmake --build --preset install-win-dev-user
ctest --preset win-dev-user -R "protocol_network_intake.*install" --output-on-failure
```

- [ ] **Step 7: Commit**

```bash
git add cmake/TurboFlowConfig.cmake.in CMakeLists.txt \
        tests/install_protocol_network_intake_consumer
git commit -m "build: install protocol network intake component"
```

---

### Task 9: Final exact-head verification, docs, and #118 evidence update

**Files:**
- Modify only after all gates are GREEN:
  - `ingress/protocol/README.md` and/or `README.md`
  - issue #118 evidence/checklist through GitHub
- No new behavior in this task.

**Interfaces:**
- Consumes all previous task outputs.
- Produces the merge-ready exact-head evidence set.

- [ ] **Step 1: Run formatting/diff checks before broad CI**

```bash
clang-format --dry-run --Werror \
  io/cnet/include/turbo_flow_cnet.h \
  io/cnet/src/turbo_flow_cnet_listener_source.c \
  ingress/protocol/network/include/turbo_flow_protocol_network_intake.h \
  ingress/protocol/network/src/*.c \
  ingress/protocol/network/src/*.h \
  ingress/protocol/tests/test_protocol_network_*.c \
  ingress/protocol/tests/protocol_network_*.c \
  ingress/protocol/tests/protocol_network_*.h
git diff --check
```

- [ ] **Step 2: Run Debug/ASan focused exact-head gate**

```bash
cmake --preset win-dev-user
cmake --build --preset win-dev-user --parallel
ctest --preset win-dev-user -R \
  "(protocol_network|protocol_source|protocol_inbox|flow_inbox_source|cnet_listener_source|cnet_packet_source|cnet_datagram_sink|turbodb_inbox)" \
  --output-on-failure
```

Do not call Task 9 GREEN if any selected adjacent test is RED.

- [ ] **Step 3: Run Release full suite**

```bash
cmake --preset win-release-user
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user --output-on-failure
```

Expected: complete suite PASS at one exact commit SHA.

- [ ] **Step 4: Run fresh install/package gate**

```bash
cmake --build --preset install-win-dev-user
```

Then run the repository's installed consumers including the new ProtocolNetworkIntake C/C++ consumer from the install prefix.

- [ ] **Step 5: Verify the acceptance matrix explicitly**

Record evidence for:

```text
JT/T808/TCP real peer: GREEN
CoAP/UDP real peer: GREEN
store-before-business-Graph: GREEN
TurboDB lock/no memory fallback: GREEN
capacity retained-frame exactly-once admission: GREEN
TCP generation reuse: GREEN
stale UDP generation: GREEN
KCP preflight rejection: GREEN
memory/TurboDB parity: GREEN
TurboDB clean-reopen recovery: GREEN
destination A/B independence: GREEN
catalog/module pin/unload: GREEN
installed C/C++ consumer: GREEN
full Debug/ASan + Release: GREEN
```

If any line lacks a concrete test/run, leave #118 open and report that missing gate instead of broadening scope.

- [ ] **Step 6: Update documentation only to claims proven above**

Document the installed path:

```text
configured CNet Source -> ProtocolNetworkIntake -> ProtocolSource -> ProtocolInbox -> Inbox
InboxSource -> shared business Graph -> configured CNet Sink
```

State explicitly that FlowMQ #74, Flowie #115, RulesForge DLL #73, and KCP intake remain separate.

- [ ] **Step 7: Update #118 with exact head and verification evidence**

Post the exact commit SHA, named CTest gates, full-suite result, install result, and the remaining non-goals. Close #118 only if its current issue body has no independent unchecked requirement beyond the approved two-protocol completion boundary.

- [ ] **Step 8: Commit documentation**

```bash
git add README.md ingress/protocol/README.md
git commit -m "docs: record real protocol intake completion"
```

Do not include generated build artifacts, `.codegraph/`, local databases, or temporary socket fixtures.
