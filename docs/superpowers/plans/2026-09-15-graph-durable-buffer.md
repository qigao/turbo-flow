# Graph Durable Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement #127 as one provider-neutral Graph durable-buffer boundary that terminates an upstream execution after bounded storage admission, later claims the stored record, and starts a new downstream Graph execution from the same logical buffer node. The same Graph topology must work with bounded memory or TurboDB selected through configured transactional resource providers.

**Architecture:** Keep one parsed/compiled Graph and one stable stage-index space. A `buffer` declaration compiles to a special runtime node with no ordinary executor. Upstream execution reachability stops at that node; the node admits a pointer-free Inbox v2 record and ends that execution without activating outgoing edges. A single-owner drain driver later claims one record and starts a new run from the buffer stage's outgoing edges. Existing `turbo_flow_inbox_t`, InboxSource settlement behavior, `turbo_flow_run_t`, transactional resource providers, and PluginHost generation lifecycle remain the only storage/execution/ownership mechanisms.

**Tech Stack:** C17, Lemon/re2c, TurboFlow Graph runtime, CFlow reactive runs, PluginHost transactional resource providers, Salts TinyTest/CSTL, Inbox v2, TurboDB SQLite/ORM inbox provider, CMake/Ninja.

**Spec:** `docs/superpowers/specs/2026-09-15-graph-durable-buffer-design.md`

**Tracking:** #127 core. Follow-ups: #128 partitioned drain, #129 protocol migration, #130 restart/owner-loss conformance, #131 observability/control. #118 remains real-protocol acceptance.

## Global Constraints

- No protocol-specific fields or JTT808/CoAP knowledge in #127 core.
- No legacy ABI/config/schema compatibility layer, converter, runtime fallback, C fallback, or CMake fallback.
- A configured TurboDB provider never switches to memory after any error.
- Never persist raw `turbo_flow_msg_t`, `transport_context`, `_content_handle`, DLL/native/session handles, scheduler objects, or borrowed process pointers.
- `buffer` is a compiler/runtime execution cut, not a synchronous transform stage.
- Upstream success means provider admission/commit succeeded. It does not mean downstream Graph/Sink completion.
- Downstream settlement remains explicit `complete/fail/retry/reconcile`; no blind retry and no exactly-once claim.
- #127 is single-owner/single-record drain. Worker pools, batch claim, and partition ordering belong to #128.
- #127 does not claim restart/detach support. Current TurboDB Inbox `destroy` refuses live pending records. Retirement with backlog must return an exact busy/error and preserve state; #130 owns detach/reopen/takeover lifecycle.
- Transactional resource provider materialization happens before `turbo_flow_compile()`. A provider binds an already-created Inbox to a parsed buffer resource; compile then validates the binding.
- Every task follows exact RED -> GREEN. Do not fix a later failure before the current task's first failure is understood.
- Keep PR #126 Draft. Do not mark Ready or merge until final exact-head runtime/package gates pass.

## Frozen Public/Core Contracts

### Graph DSL

#127 adds one root declaration:

```text
buffer intake resource intake.store

source telemetry
stage normalize
stage main {
  telemetry -> intake
  intake -> normalize
}
```

`buffer` declarations are root-only in #127. A buffer accepts only `resource`; it does not accept `adapter`, `operation`, `worker`, `pool`, `exec`, `retry`, or `reorder` options.

### Public stage view

Add to `turbo_flow_stage_plan_t`:

```c
int is_buffer;
```

Public `turbo_flow_stage_at()` exposes this field so transactional resource providers validate Graph references without private headers.

### Durable identity metadata

Create `turbo_flow/include/turbo_flow_durable_buffer.h`:

```c
#define TURBO_FLOW_DURABLE_BUFFER_API_VERSION UINT32_C(1)
#define TURBO_FLOW_DURABLE_SOURCE_ID_MAX 127u
#define TURBO_FLOW_DURABLE_ADMISSION_ID_MAX 255u
#define TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES (2u * 1024u * 1024u)

typedef struct turbo_flow_durable_identity_s {
  size_t size;
  uint32_t version;
  vstr source_id;
  vstr admission_id;
  uint64_t source_sequence;
} turbo_flow_durable_identity_t;

#define TURBO_FLOW_DURABLE_IDENTITY_INIT \
  {sizeof(turbo_flow_durable_identity_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION, \
   {NULL, 0u}, {NULL, 0u}, 0u}

TURBO_FLOW_C_API int turbo_flow_msg_set_durable_identity(
    turbo_flow_msg_t *message, const turbo_flow_durable_identity_t *identity);

TURBO_FLOW_C_API int turbo_flow_msg_durable_identity(
    const turbo_flow_msg_t *message, turbo_flow_durable_identity_t *out);
```

The setter copies bounded identity bytes into core-owned message metadata. The getter returns borrowed immutable views valid with the message. Identity survives clone/move/retain-view and never uses `transport_context`.

### Provider-neutral binding

The same header defines:

```c
typedef struct turbo_flow_durable_buffer_binding_s turbo_flow_durable_buffer_binding_t;

typedef enum turbo_flow_durable_identity_mode_e {
  TURBO_FLOW_DURABLE_IDENTITY_GENERATED = 1,
  TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED = 2
} turbo_flow_durable_identity_mode_t;

typedef struct turbo_flow_durable_buffer_binding_config_s {
  size_t size;
  uint32_t version;
  const char *resource_name;
  turbo_flow_inbox_t *inbox;
  turbo_flow_durable_identity_mode_t identity_mode;
  size_t max_message_bytes;
} turbo_flow_durable_buffer_binding_config_t;

#define TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT \
  {sizeof(turbo_flow_durable_buffer_binding_config_t), \
   TURBO_FLOW_DURABLE_BUFFER_API_VERSION, NULL, NULL, \
   TURBO_FLOW_DURABLE_IDENTITY_GENERATED, \
   TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES}

TURBO_FLOW_C_API int turbo_flow_durable_buffer_bind(
    turbo_flow_t *flow, const turbo_flow_durable_buffer_binding_config_t *config,
    turbo_flow_durable_buffer_binding_t **out);

TURBO_FLOW_C_API int turbo_flow_durable_buffer_progress(
    turbo_flow_durable_buffer_binding_t *binding);

TURBO_FLOW_C_API int turbo_flow_durable_buffer_quiesce(
    turbo_flow_durable_buffer_binding_t *binding);

TURBO_FLOW_C_API int turbo_flow_durable_buffer_drain(
    turbo_flow_durable_buffer_binding_t *binding, uint64_t timeout_ms);

TURBO_FLOW_C_API int turbo_flow_durable_buffer_unbind(
    turbo_flow_durable_buffer_binding_t *binding);
```

Binding owns no Inbox. The Product owner owns the Inbox and keeps it alive until unbind succeeds. `progress` is caller-serialized and non-blocking: idle may claim/start one record; active polls/settles that one record; empty storage is normal success. `quiesce` closes new admission. `drain` never claims new backlog after quiesce; it only settles an already-owned run/claim. In #127 any remaining live backlog blocks retirement instead of being dropped. `unbind` is forbidden while Graph is started or a claim remains unresolved.

### Generated identity

At bind time snapshot the Inbox provider generation. For each first generated-mode admission allocate a monotonically increasing local sequence before the provider call:

```text
source_id       = configured buffer stage name
admission_id    = "g<provider-generation>:<local-sequence>"
source_sequence = local-sequence
```

The same generated ID is reused if that one admission operation internally retries the provider call. Sequence overflow returns `SALTS_ERANGE` before admission. Provider generation prevents intentional key reuse across a new TurboDB ownership generation. Generated mode still promises only at-least-once producer redelivery semantics.

Stable-required mode fails closed unless `turbo_flow_msg_durable_identity()` returns non-empty stable IDs; exact provider replay then applies to the complete record.

---

## Task 1: Parse `buffer` and expose stage-plan identity

**Files:**
- Modify: `turbo_flow/parser/flow_lexer.re`
- Modify: `turbo_flow/parser/flow_grammar.y`
- Modify: `turbo_flow/src/flow_parser_internal.h`
- Modify: `turbo_flow/src/flow_parser.c`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/include/turbo_flow.h`
- Modify: `turbo_flow/src/flow_core.c`
- Create: `turbo_flow/tests/test_flow_durable_buffer.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

- [ ] **RED:** add `test_flow_durable_buffer` parse-only coverage:

```c
static const char graph[] =
    "buffer intake resource intake.store\n"
    "source telemetry\n"
    "stage normalize\n"
    "stage main {\n"
    "  telemetry -> intake\n"
    "  intake -> normalize\n"
    "}\n";
```

Require parse success, `turbo_flow_find_stage(flow, "intake") >= 0`, and `turbo_flow_stage_at(...)->is_buffer == 1`. Add negative cases for missing resource, duplicate buffer/stage/source name, and a buffer declaration inside a reusable stage template.

- [ ] Run RED:

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user --target test_flow_durable_buffer -j2
ctest --test-dir build/linux-gcc-debug -R '^test_flow_durable_buffer$' --output-on-failure
```

Expected first failure: parser treats `buffer` as an identifier/top-level syntax error, or the new public field does not compile.

- [ ] Add `TURBO_FLOW_TOKEN_BUFFER` and recognize the keyword in `flow_keyword_token()`.
- [ ] Add grammar equivalent to:

```text
buffer_decl ::= BUFFER IDENT RESOURCE adapter_name
```

as a root `top_item` only.
- [ ] Add `flow_parse_add_buffer(ctx, name, resource)`. Record a special stage with `is_buffer=1`, copied resource name, and no adapter/operation/executor options.
- [ ] Add `is_buffer` to all internal/public stage copy/reset/view paths.
- [ ] Reject buffer declarations outside the root declaration area.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_turbo_flow -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_turbo_flow)$' --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/parser turbo_flow/include/turbo_flow.h turbo_flow/src turbo_flow/tests
git commit -m "feat(graph): parse durable buffer boundaries"
```

## Task 2: Compile buffer as a non-executor execution cut

**Files:**
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_compile.c`
- Modify: `turbo_flow/src/flow_plan.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** extend the test with sealed-plan assertions. In this task only, compile may succeed before Task 4 adds provider-binding validation. Require:
  - buffer node has `FLOW_RUNTIME_NODE_BUFFER`;
  - `flow_executor_plan_for_stage(flow, buffer_index) == NULL`;
  - buffer semantic plan is not a native lowering candidate and carries external-I/O/settlement barriers;
  - ordinary stages still own executors.

Give the test target private access to `turbo_flow/src` for sealed-plan inspection; export no internal APIs.

- [ ] Add `FLOW_RUNTIME_NODE_BUFFER`.
- [ ] Define descriptor-only core semantic metadata `core.buffer` with Message input/output. It is not an executable provider callback.
- [ ] `flow_build_runtime_plan()` skips executor creation for buffer nodes.
- [ ] `flow_verify_compiled_plan()` accepts no executor only for source/port/buffer special nodes.
- [ ] Mark buffer semantics with `FLOW_LOWERING_BARRIER_EXTERNAL_IO | FLOW_LOWERING_BARRIER_SETTLEMENT`; exclude it from native-to-CFlow lowering regions.
- [ ] Add execution-region reachability distinct from logical topology reachability:

```c
int flow_mark_execution_region_from_stage(
    const turbo_flow_t *flow, uint8_t *reachable, uint32_t *worklist,
    size_t capacity, uint32_t origin_stage);
```

Traversal includes a reached buffer but stops before its outgoing edges unless the buffer itself is `origin_stage`.
- [ ] Change `flow_run_message_from_stage()` to use execution-region reachability for runtime prevalidation, reorder reservations, and queue construction.
- [ ] Runtime prevalidation must explicitly skip `is_buffer` nodes; never call `flow_dispatch_validate_stage()` on a buffer because it intentionally has no executor.
- [ ] Do not execute durable admission yet; Task 4 adds the queued-buffer branch.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_run test_turbo_flow -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_run|test_turbo_flow)$' --output-on-failure
```

- [ ] Commit:

```bash
git commit -am "feat(graph): compile durable buffer execution cuts"
```

## Task 3: Add clone-safe generic durable identity metadata

**Files:**
- Create: `turbo_flow/include/turbo_flow_durable_buffer.h`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_message.c`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`
- Create: `turbo_flow/tests/durable_buffer_header_cpp.cpp`
- Modify: `turbo_flow/tests/CMakeLists.txt`

- [ ] **RED:** C and C++ include probes plus set/get/clone/retain-view/move/clear/cleanup tests. Verify identity is copied independently from caller memory.

```c
char source[] = "device-A";
char admission[] = "seq-42";
turbo_flow_durable_identity_t id = TURBO_FLOW_DURABLE_IDENTITY_INIT;
id.source_id = vstr_from_buf(source, strlen(source));
id.admission_id = vstr_from_buf(admission, strlen(admission));
id.source_sequence = 42u;
check_equal(turbo_flow_msg_set_durable_identity(&message, &id), SALTS_OK);
source[0] = 'X';
admission[0] = 'X';
check_equal(turbo_flow_msg_durable_identity(&message, &observed), SALTS_OK);
check_equal(memcmp(observed.source_id.data, "device-A", 8u), 0);
```

Negative cases: empty IDs, >127/>255 bytes, wrong ABI, and mutation while a projection result claim is active.

- [ ] Store identity inside existing `flow_msg_projection_t` with fixed bounded arrays and `has_durable_identity`; do not allocate a second sidecar.
- [ ] `flow_msg_projection_empty()` must treat identity-only sidecars as non-empty.
- [ ] Clone and retain-view remain correct with identity-only, descriptor-only, projection, and result combinations.
- [ ] Clear projection/content/result APIs never discard independent durable identity metadata.
- [ ] Install/export the new header with Graph.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_data_schema test_flow_projection_owner -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_data_schema|test_flow_projection_owner)$' --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/include/turbo_flow_durable_buffer.h turbo_flow/src turbo_flow/tests turbo_flow/CMakeLists.txt
git commit -m "feat(graph): add durable message identity metadata"
```

## Task 4: Add provider-neutral binding registry and admission

**Files:**
- Modify: `turbo_flow/include/turbo_flow_durable_buffer.h`
- Create: `turbo_flow/src/flow_durable_buffer.c`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/src/flow_compile.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** manually create bounded memory Inbox, bind `intake.store`, compile/start:

```text
buffer intake resource intake.store
source telemetry
stage downstream
stage main {
  telemetry -> intake
  intake -> downstream
}
```

Register downstream callback counter. Publish one message; require `SALTS_OK`, Inbox `pending_records == 1`, `admitted == 1`, downstream counter remains zero.

Add failures for unbound resource, one binding referenced by multiple buffers, stable-required without identity, malformed payload, provider capacity, and provider error. Every failure has zero downstream executions.

- [ ] Add a flow-owned registry of borrowed binding pointers keyed by exact resource name. Binding copies resource/config metadata but owns no Inbox.
- [ ] Compile resolves every buffer resource to exactly one live binding and sets binding stage index/name. #127 rejects one binding referenced by multiple buffers.
- [ ] Bind snapshots Inbox generation; require nonzero generation and finite nonzero `max_message_bytes`.
- [ ] Encode one pointer-free Inbox v2 record:
  - validate payload backing;
  - use `turbo_flow_msg_content_descriptor()` when present;
  - otherwise create deterministic generic opaque descriptor with `TURBO_FLOW_DOMAIN_DATA`, `TURBO_FLOW_CONTENT_PROFILE_GENERIC`, `TURBO_FLOW_DATA_ENCODING_OPAQUE`, media type `application/octet-stream`, and configured buffer stage name as non-secret content identity;
  - correlation is empty in #127;
  - copy timestamp/type/flags/payload;
  - never serialize runtime pointers/projections.
- [ ] Stable mode consumes generic durable identity and fails closed if absent.
- [ ] Generated mode checks local-sequence exhaustion before increment, then formats `g<generation>:<sequence>` once before provider call.
- [ ] Add internal `flow_durable_buffer_admit_stage(flow, stage_index, message)`.
- [ ] In the runtime queue, when `stage->is_buffer`:
  1. admit durable record;
  2. mark buffer terminal for this execution;
  3. do not call normal dispatch;
  4. do not call `flow_apply_completion()` for the buffer;
  5. therefore no outgoing edge is activated in this upstream execution.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_inbox test_flow_run -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_inbox|test_flow_run)$' --output-on-failure
```

- [ ] Commit:

```bash
git commit -am "feat(graph): admit messages at durable buffer boundaries"
```

## Task 5: Refactor InboxSource into one shared claim-to-Graph driver

**Files:**
- Create: `turbo_flow/src/flow_inbox_driver_internal.h`
- Create: `turbo_flow/src/flow_inbox_driver.c`
- Modify: `turbo_flow/src/flow_inbox_source.c`
- Modify: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_inbox_source.c`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** freeze existing public InboxSource suite and add an internal-driver case whose claimed record starts from a buffer origin rather than an adapter-free Source.

Internal driver config:

```c
typedef enum flow_inbox_driver_origin_kind_e {
  FLOW_INBOX_DRIVER_SOURCE = 1,
  FLOW_INBOX_DRIVER_BUFFER = 2
} flow_inbox_driver_origin_kind_t;

typedef struct flow_inbox_driver_config_s {
  turbo_flow_inbox_t *inbox;
  turbo_flow_t *flow;
  flow_inbox_driver_origin_kind_t origin_kind;
  uint32_t origin_stage;
  cflow_scheduler *scheduler;
  size_t max_message_bytes;
} flow_inbox_driver_config_t;
```

- [ ] Move current record->message construction and `IDLE / GRAPH_RUNNING / SETTLE_COMPLETE / SETTLE_FAIL / SETTLE_UNKNOWN` state machine into `flow_inbox_driver.c` without semantic change.
- [ ] Keep public `turbo_flow_inbox_source_t` as a thin wrapper that resolves an adapter-free Source and delegates to the driver.
- [ ] Generalize run origin internally without changing public `turbo_flow_run_open()`:
  - add an internal origin-stage form that accepts either a real Source or `is_buffer` node;
  - normal Source path keeps `flow_publish_message_entered()` semantics;
  - buffer-origin path starts runtime execution from `origin_stage == buffer_stage`, so outgoing edges activate and admission is not repeated.
- [ ] Do **not** call bare `flow_run_message_from_stage()` and treat its immediate return as terminal. Add an internal publish-from-origin helper that participates in the same `flow_async_publication` completion accounting used by ordinary runs. If any downstream async-terminal/worker stage remains active, the run stays `ACTIVE` and Inbox settlement waits for the async publication callback.
- [ ] Preserve public InboxSource result states, cancellation, retry-settlement, and EALREADY reconciliation exactly.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_flow_inbox_source test_flow_durable_buffer test_flow_run test_flow_async_terminal -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_inbox_source|test_flow_durable_buffer|test_flow_run|test_flow_async_terminal)$' --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/src turbo_flow/tests turbo_flow/CMakeLists.txt
git commit -m "refactor(graph): share inbox claim graph driver"
```

## Task 6: Implement single-owner automatic drain and lifecycle

**Files:**
- Modify: `turbo_flow/src/flow_durable_buffer.c`
- Modify: `turbo_flow/src/flow_inbox_driver.c`
- Modify: `turbo_flow/include/turbo_flow_durable_buffer.h`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** after upstream publication produces one pending record and zero downstream calls, repeatedly call `turbo_flow_durable_buffer_progress(binding)`. Require exactly one downstream execution, then Inbox pending zero/completed one. A second empty progress round is `SALTS_OK` and does nothing.
- [ ] Add downstream failure, cancel, settlement provider failure, EALREADY reconciliation, and stable replay cases by reusing InboxSource fault patterns.
- [ ] `progress()` is serialized and non-blocking:
  - idle: claim/start at most one record; map `SALTS_ENOENT` to normal `SALTS_OK`;
  - active: poll/settle owned record;
  - never own two claims.
- [ ] `quiesce()` closes new durable admission via Inbox close. Do not mark quiesced on provider failure.
- [ ] `drain(timeout)` after quiesce never claims new backlog. It may poll an already-started run after Graph stop only to observe cancellation/terminal state and settle that existing claim. Once driver is idle, snapshot storage; any remaining pending/failed/live record returns `SALTS_EBUSY` in #127.
- [ ] `unbind()` requires non-STARTED Graph and idle driver; it removes only binding metadata and never destroys Inbox.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_inbox_source -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_inbox_source)$' --output-on-failure
```

- [ ] Commit:

```bash
git commit -am "feat(graph): drive durable buffer claims and settlement"
```

## Task 7: Add configured bounded-memory resource provider

**Files:**
- Create: `io/durable/CMakeLists.txt`
- Create: `io/durable/src/turbo_flow_durable_memory_plugin.c`
- Create: `io/durable/src/turbo_flow_durable_memory_plugin_config.c`
- Create: `io/durable/src/turbo_flow_durable_memory_plugin_internal.h`
- Create: `io/durable/tests/CMakeLists.txt`
- Create: `io/durable/tests/test_durable_memory_plugin.c`
- Modify: root `CMakeLists.txt`
- Modify: package/install CMake as required

Provider kind:

```text
flow.durable.memory
```

Exact config v1:

```yaml
channels:
  intake.store:
    kind: flow.durable.memory
    config:
      schema_version: 1
      identity_mode: generated
      max_message_bytes: 1048576
      max_records: 1024
      max_total_bytes: 67108864
      max_record_bytes: 1048576
      max_claims: 64
```

`identity_mode` is exactly `generated` or `stable_required`.

- [ ] **RED:** PluginHost generation fixture must fail until the transactional resource provider exists. Add exact-schema negatives for unknown/missing fields, zero bounds, `max_claims > max_records`, record bytes > total, invalid identity mode, and zero/multiple buffer references.
- [ ] Implement one transactional resource provider, not adapter provider.
- [ ] `preflight` is side-effect-free and validates exact config plus exactly one `is_buffer` reference.
- [ ] `materialize` creates bounded memory Inbox, binds it, and publishes Product owner flags `CONTROL_THREAD | EXTERNAL_POLL`.
- [ ] Owner callbacks:
  - `poll`: non-blocking `turbo_flow_durable_buffer_progress()`; do not block the control thread with the supplied timeout;
  - `quiesce`: durable-buffer quiesce;
  - `drain`: durable-buffer drain;
  - `shutdown`: unbind, then destroy already-closed/empty memory Inbox and propagate exact status;
  - `destroy`: free owner only after successful lifecycle.
- [ ] Install `tf_durable_memory_plugin`; do not add a redundant public adapter library.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target test_durable_memory_plugin test_flow_plugin_generation -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_durable_memory_plugin|test_flow_plugin_generation)$' --output-on-failure
```

- [ ] Commit:

```bash
git add io/durable CMakeLists.txt cmake
git commit -m "feat(graph): add configured memory durable buffer provider"
```

## Task 8: Add TurboDB resource provider with #127 parity

**Files:**
- Create: `io/turbodb/src/turbo_flow_turbodb_plugin.c`
- Create: `io/turbodb/src/turbo_flow_turbodb_plugin_config.c`
- Create: `io/turbodb/src/turbo_flow_turbodb_plugin_internal.h`
- Modify: `io/turbodb/CMakeLists.txt`
- Create: `io/turbodb/tests/test_turbodb_durable_buffer_plugin.c`
- Modify: `io/turbodb/tests/CMakeLists.txt`

Provider kind:

```text
flow.durable.turbodb
```

Exact #127 config v1:

```yaml
channels:
  intake.store:
    kind: flow.durable.turbodb
    config:
      schema_version: 1
      identity_mode: stable_required
      filename: /tmp/turbo-flow-buffer.sqlite3
      namespace: telemetry
      max_message_bytes: 1048576
      max_records: 100000
      max_total_bytes: 1073741824
      max_record_bytes: 1048576
      max_claims: 64
      connection_count: 4
      open_mode: exclusive
```

#127 accepts file-backed SQLite and `open_mode: exclusive` only. Takeover/restart belongs to #130.

- [ ] **RED:** use the existing `test_turbodb_inbox.c` fixture shape: temp SQLite file, explicitly provision exact v2 meta/records tables/indexes, load plugin through PluginHost, and run the same Graph topology as memory with only channel kind/config changed.
- [ ] Negative cases: `:memory:`, old/wrong/missing schema, unknown config field, provider create/commit failure, no memory fallback, and pending backlog on retirement.
- [ ] Add separate SHARED `tf_turbodb_plugin` following CNet's adapter+plugin packaging pattern. Keep `TurboFlow::TurboDbAdapter` unchanged as the public adapter/library target.
- [ ] Preflight validates exact serialized config without database creation/migration/repair.
- [ ] Materialize builds existing `orm_config_t` (`driver = sqlite`, one `filename` option), calls `turbo_flow_turbodb_inbox_create()`, then binds through the same core API as memory.
- [ ] Product owner lifecycle/poll matches Task 7.
- [ ] If provider destroy sees live records and returns `SALTS_EBUSY`, propagate it; do not force-close or claim restart solved.

- [ ] **GREEN in full SDK environment:**

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user --target test_turbodb_durable_buffer_plugin test_turbodb_inbox -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_turbodb_durable_buffer_plugin|test_turbodb_inbox)$' --output-on-failure
```

- [ ] Commit:

```bash
git add io/turbodb
git commit -m "feat(turbodb): provide graph durable buffer resource"
```

## Task 9: Run provider-neutral conformance matrix

**Files:**
- Create: `turbo_flow/tests/durable_buffer_conformance.h`
- Modify: `io/durable/tests/test_durable_memory_plugin.c`
- Modify: `io/turbodb/tests/test_turbodb_durable_buffer_plugin.c`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] Share behavior assertions, not provider setup. Run the same logical matrix against memory and TurboDB where durability differences do not apply.
- [ ] Required cases:
  1. admission before downstream execution;
  2. capacity 1/N/N+1 and retained-byte/per-record limits;
  3. generated identity provider-call retry stability;
  4. stable identity exact replay;
  5. same stable identity with changed complete record -> `SALTS_EPROTO`;
  6. downstream success -> complete;
  7. downstream failure -> failed;
  8. cancel -> explicit canceled/failed settlement;
  9. settlement pending/unknown/reconcile truthfulness;
  10. quiesce rejects new admission while an owned run may settle;
  11. pending backlog blocks #127 retirement;
  12. provider error produces zero downstream execution and zero fallback.
- [ ] Internal-edge case: `source -> transform -> buffer -> transform -> sink`.
- [ ] Fan-in case: two Sources -> one named buffer, serialized FIFO drain for #127.
- [ ] Fan-out case: buffer -> two downstream branches, both begin only in downstream execution.

- [ ] **GREEN:**

```bash
cmake --build --preset linux-dev-user --target \
  test_flow_durable_buffer test_durable_memory_plugin test_turbodb_durable_buffer_plugin -j2
ctest --test-dir build/linux-gcc-debug \
  -R '^(test_flow_durable_buffer|test_durable_memory_plugin|test_turbodb_durable_buffer_plugin)$' \
  --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/tests io/durable/tests io/turbodb/tests
git commit -m "test(graph): require durable buffer provider parity"
```

## Task 10: Package, install, CI, and close #127 core gate

**Files:**
- Modify: `tests/install_consumer/CMakeLists.txt`
- Modify: `tests/install_consumer/main.c`
- Add installed plugin consumer coverage for memory/TurboDB plugins where needed
- Modify/create no-secret compile-contract CMake/workflow under `.github/ci` and `.github/workflows`
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Modify root/package CMake files required for Graph header/plugin installation
- Update spec status and #127/#126 evidence only after exact gates pass

- [ ] Installed C/C++ consumers compile `turbo_flow_durable_buffer.h` and public `is_buffer` stage field.
- [ ] Graph component exports durable-buffer API without TurboDB dependency.
- [ ] `TURBO_FLOW_BUILD_TURBODB_ADAPTER=OFF` still builds/installs Graph + memory durable provider; no substitute/fallback TurboDB path appears.
- [ ] Verify `tf_durable_memory_plugin` and, when enabled, `tf_turbodb_plugin` runtime install/export/dependency closure.
- [ ] Extend existing **no-secret** GitHub compile-contract CI to compile new Graph durable-buffer core and memory-plugin translation units with ASan+UBSan. Do not reintroduce `QIGAO_CI_READ_TOKEN` or any cross-repository secret.
- [ ] Do not claim that no-secret compile-contract CI runs TurboDB runtime when ORM/private SDK dependencies are absent. TurboDB runtime evidence must come from full SDK gates.

- [ ] Focused Debug/ASan:

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user -j2
ctest --test-dir build/linux-gcc-debug \
  -R '(durable_buffer|flow_inbox|flow_run|plugin_generation|turbodb_inbox)' \
  --output-on-failure
```

- [ ] Full Debug/ASan:

```bash
ctest --test-dir build/linux-gcc-debug --output-on-failure
```

- [ ] Release:

```bash
cmake --preset linux-release-user
cmake --build --preset linux-release-user -j2
ctest --test-dir build/linux-gcc-release --output-on-failure
```

- [ ] Run repository install-consumer/profile/ABI/export/dependency gates against a fresh install prefix.
- [ ] `git diff --check` and formatting checks are clean.
- [ ] Scan new #127 source for unfinished markers/placeholders, compatibility shims, database-to-memory fallback, or direct durable-buffer bypass; none may remain.
- [ ] Confirm #128/#130/#131 remain open and are not falsely reported complete by #127.
- [ ] Update #127 with exact head SHA and exact runtime/package evidence. Keep #126 Draft until #129 migrates JTT808/CoAP and final #118 acceptance passes.

- [ ] Commit:

```bash
git add .github cmake tests docs CMakeLists.txt
git commit -m "build(graph): gate durable buffer core"
```

## Final #127 Definition of Done

#127 is complete only when one exact head proves all of the following:

- `buffer` is a compiler-visible execution cut with no synchronous continuation across storage;
- upstream success occurs only after provider admission/commit;
- downstream starts only from a later provider claim;
- one Graph topology selects memory or TurboDB by resource provider;
- generated/stable identity modes obey documented replay guarantees;
- no runtime pointer/session/projection is persisted;
- no provider failure bypasses buffer or selects fallback storage;
- existing InboxSource public behavior survives the shared-driver refactor;
- single-owner drain truthfully handles success/failure/cancel/settlement unknown;
- pending backlog blocks #127 retirement instead of being lost; restart/detach remains #130;
- provider-neutral conformance, Debug/ASan, Release, install/package, ABI/export, and no-secret compile-contract gates have fresh exact-head evidence.

Only after this gate should #129 replace `ProtocolNetworkIntake -> Inbox -> InboxSource` with the generic durable-buffer boundary. #128 may then scale drain concurrency without changing #127 ownership or settlement semantics.
