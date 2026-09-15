# Graph Durable Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement #127 as one provider-neutral Graph durable-buffer boundary that can terminate an upstream execution after bounded storage admission, later claim the stored record, and start a new downstream Graph execution from the same logical buffer node. The same Graph topology must work with bounded memory or TurboDB selected through configured transactional resource providers.

**Architecture:** Keep one parsed/compiled Graph and one stable stage-index space. A `buffer` declaration compiles to a special runtime node with no ordinary executor. Upstream execution reachability stops when it reaches a buffer; the buffer admits a pointer-free Inbox v2 record and terminates that execution without activating outgoing edges. A single-owner durable drain driver later claims one record and starts a new Graph run from the buffer stage's outgoing edges. Existing `turbo_flow_inbox_t`, `InboxSource` settlement behavior, `turbo_flow_run_t`, transactional resource providers, and PluginHost generation lifecycle remain the only storage/execution/ownership mechanisms. Do not create a second queue, database abstraction, executor, or retry state machine.

**Tech Stack:** C17, Lemon/re2c parser, TurboFlow Graph runtime, CFlow reactive runs, PluginHost transactional resource providers, Salts TinyTest/CSTL, existing Inbox v2, existing TurboDB SQLite/ORM inbox provider, CMake/Ninja.

**Spec:** `docs/superpowers/specs/2026-09-15-graph-durable-buffer-design.md`

**Tracking:** #127 core. Follow-ups are #128 partitioned drain, #129 protocol migration, #130 restart/owner-loss conformance, #131 observability/control. #118 remains real-protocol acceptance.

## Global Constraints

- No protocol-specific fields or JTT808/CoAP knowledge in #127 core.
- No old ABI/config/schema compatibility layer, converter, runtime fallback, C fallback, or CMake fallback.
- A configured TurboDB provider never switches to memory after any error.
- Never persist `turbo_flow_msg_t` raw bytes, `transport_context`, `_content_handle`, DLL/native/session handles, scheduler objects, or borrowed process pointers.
- `buffer` is a compiler/runtime execution cut, not a synchronous transform stage.
- Upstream success means the selected provider accepted/committed the record. It does not mean downstream Graph/Sink completion.
- Downstream settlement remains explicit `complete/fail/retry/reconcile`; no blind retry and no exactly-once claim.
- #127 is single-owner/single-record drain. No worker pool, batch claim, or partition scheduler; those belong to #128.
- #127 does not claim crash/restart detach support. The current TurboDB Inbox `destroy` refuses live pending records. Generation retirement with pending backlog must therefore return an exact busy/error and preserve state; #130 owns the future detach/reopen/takeover lifecycle.
- Resource provider materialization occurs before `turbo_flow_compile()`. A provider binds an already-created Inbox to a parsed buffer resource; compile then validates the binding.
- All code changes use exact RED -> GREEN gates. Do not fix a later failure before the current task's first failure is understood.
- Keep PR #126 Draft while #127 is being developed. Do not mark Ready or merge until the final exact-head runtime/package gates pass.

## Frozen public/core contracts for this plan

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

`buffer` declarations are root-only in #127. Reusable composite-stage-local buffers are not part of this issue. A buffer accepts only `resource`; it does not accept `adapter`, `operation`, `worker`, `pool`, `exec`, `retry`, or `reorder` options.

### Public stage view

Add one field to `turbo_flow_stage_plan_t`:

```c
int is_buffer;
```

Exactly one of `is_source`, internal port state, or `is_buffer` may describe a special non-executor node. Public `turbo_flow_stage_at()` exposes `is_buffer` so transactional resource providers can validate references without private headers.

### Durable identity metadata

Create `turbo_flow/include/turbo_flow_durable_buffer.h` with:

```c
#define TURBO_FLOW_DURABLE_BUFFER_API_VERSION UINT32_C(1)
#define TURBO_FLOW_DURABLE_SOURCE_ID_MAX 127u
#define TURBO_FLOW_DURABLE_ADMISSION_ID_MAX 255u

typedef struct turbo_flow_durable_identity_s {
  size_t size;
  uint32_t version;
  vstr source_id;
  vstr admission_id;
  uint64_t source_sequence;
} turbo_flow_durable_identity_t;

#define TURBO_FLOW_DURABLE_IDENTITY_INIT \
  {sizeof(turbo_flow_durable_identity_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION, {NULL, 0u}, \
   {NULL, 0u}, 0u}

TURBO_FLOW_C_API int turbo_flow_msg_set_durable_identity(
    turbo_flow_msg_t *message, const turbo_flow_durable_identity_t *identity);

TURBO_FLOW_C_API int turbo_flow_msg_durable_identity(
    const turbo_flow_msg_t *message, turbo_flow_durable_identity_t *out);
```

The setter copies bounded identity bytes into core-owned message metadata. The getter returns borrowed immutable views valid with the message. Identity metadata must survive message clone/move/retain-view operations. It is not stored in `transport_context`.

### Provider-neutral buffer binding

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
  {sizeof(turbo_flow_durable_buffer_binding_config_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION, \
   NULL, NULL, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, \
   TURBO_FLOW_INBOX_SOURCE_DEFAULT_MAX_MESSAGE_BYTES}

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

Binding owns no Inbox. The transactional Product owner owns the Inbox and keeps it alive until unbind succeeds. `bind` is valid only while the Graph is parsed and not started. `progress` is caller-serialized and non-blocking: when idle it may claim/start one record; when active it polls/settles the owned record; an empty Inbox is normal success. `quiesce` closes new buffer admission. `drain` never claims new backlog after quiesce; it only finishes/cancels/reconciles an already-owned run/claim and then requires no live claim. In #127, pending backlog makes provider retirement busy rather than dropping data. `unbind` is forbidden while the Graph is started or the binding owns an unresolved claim.

### Generated identity rule

For `TURBO_FLOW_DURABLE_IDENTITY_GENERATED`, binding snapshots the Inbox provider generation at bind time and assigns each first admission attempt a monotonic local sequence. The durable identity is:

```text
source_id     = configured buffer stage name
admission_id  = "g<provider-generation>:<local-sequence>"
source_sequence = local-sequence
```

The generated ID is created before the first provider call and reused for every retry of that same admission attempt. Because a TurboDB provider generation advances on reopen/takeover, a new process/provider generation does not intentionally reuse the old generated key. This mode still promises only at-least-once redelivery semantics; it does not claim cross-process producer deduplication.

For `TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED`, admission fails closed unless `turbo_flow_msg_durable_identity()` returns non-empty stable source/admission IDs. Exact provider replay rules then apply to the complete record.

---

## Task 1: Add the `buffer` DSL declaration and stage-plan identity

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

- [ ] **RED:** add `test_flow_durable_buffer` with a parse-only case for:

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

Assert parse succeeds only after the new syntax exists, `turbo_flow_find_stage(flow, "intake") >= 0`, and `turbo_flow_stage_at(...)->is_buffer == 1`. Add negative cases for missing resource, duplicate buffer/stage/source name, and `buffer` inside a reusable `stage foo { ... }` template.

- [ ] Build the RED target:

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user --target test_flow_durable_buffer -j2
ctest --test-dir build/linux-gcc-debug -R '^test_flow_durable_buffer$' --output-on-failure
```

Expected RED: parser rejects `buffer` as an identifier/top-level declaration or the new public `is_buffer` field does not compile.

- [ ] Add `TURBO_FLOW_TOKEN_BUFFER` and recognize the `buffer` keyword in `flow_keyword_token()`.
- [ ] Add grammar production exactly equivalent to:

```text
buffer_decl ::= BUFFER IDENT RESOURCE adapter_name
```

and allow it as a root `top_item` only.
- [ ] Add `flow_parse_add_buffer(ctx, name, resource)`; create a stage-plan entry with `is_buffer=1`, copied `resource_name`, no adapter/operation/executor options, and fail fast outside root declaration scope.
- [ ] Add `is_buffer` to internal/public stage plans and all copy/reset/view paths. `turbo_flow_stage_at()` must expose it.
- [ ] Add parser validation that a buffer is neither source nor port and has a non-empty resource.

- [ ] **GREEN:** rerun the focused target, then parser/core regression:

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_turbo_flow -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_turbo_flow)$' --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/parser turbo_flow/include/turbo_flow.h turbo_flow/src turbo_flow/tests
git commit -m "feat(graph): parse durable buffer boundaries"
```

## Task 2: Compile buffers as non-executor execution-cut nodes

**Files:**
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_compile.c`
- Modify: `turbo_flow/src/flow_plan.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** extend the test with compile-plan assertions. In this task only, a parsed buffer may compile before provider-binding validation is added in Task 4. Assert:
  - compile succeeds;
  - buffer node has `FLOW_RUNTIME_NODE_BUFFER`;
  - `flow_executor_plan_for_stage(flow, buffer_index) == NULL`;
  - buffer semantic plan is not a lowering candidate and carries external-I/O/settlement barriers;
  - ordinary stages still have executors.

Use `INCLUDES ${PROJECT_SOURCE_DIR}/turbo_flow/src` for this test target so the test can inspect sealed-plan internals without exporting them publicly.

- [ ] Confirm RED is the current compiler assumption that every non-source/non-port stage owns an executor.

- [ ] Add `FLOW_RUNTIME_NODE_BUFFER`.
- [ ] Define a core metadata operation name `core.buffer` for semantic typing only; it has Message input/output and is never an executable callback.
- [ ] In `flow_build_runtime_plan()`, do not create an executor for `is_buffer` nodes.
- [ ] Update `flow_verify_compiled_plan()` so source/port/buffer special nodes require no executor while ordinary stages still require one.
- [ ] Mark buffer semantics with `FLOW_LOWERING_BARRIER_EXTERNAL_IO | FLOW_LOWERING_BARRIER_SETTLEMENT`; do not place buffers in native-to-CFlow lowering candidate regions.
- [ ] Add an execution-reachability helper distinct from logical topology reachability:

```c
int flow_mark_execution_region_from_stage(
    const turbo_flow_t *flow, uint8_t *reachable, uint32_t *worklist,
    size_t capacity, uint32_t origin_stage);
```

Traversal includes a reached buffer node but does not traverse its outgoing edges unless that buffer is `origin_stage`. Existing compile-time logical reachability remains unchanged.

- [ ] Update `flow_run_message_from_stage()` to use execution-region reachability for prevalidation, reorder reservation, and runtime queue construction. Do not yet execute buffer admission; Task 4 adds that branch.

- [ ] **GREEN:** compile-plan tests plus run regressions:

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_run test_turbo_flow -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_run|test_turbo_flow)$' --output-on-failure
```

- [ ] Commit:

```bash
git commit -am "feat(graph): compile durable buffer execution cuts"
```

## Task 3: Add clone-safe generic durable identity metadata to messages

**Files:**
- Create: `turbo_flow/include/turbo_flow_durable_buffer.h`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_message.c`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`
- Create: `turbo_flow/tests/durable_buffer_header_cpp.cpp`
- Modify: `turbo_flow/tests/CMakeLists.txt`

- [ ] **RED:** include the new header from C and C++. Add tests that set identity, clone, retain-view, move, clear projection/content, and cleanup. Verify returned identity bytes and sequence remain unchanged and independent of caller buffers.

Example requirement:

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

Add negative cases for empty IDs, IDs over 127/255 bytes, wrong size/version, and mutation while a projection result claim is active.

- [ ] Implement identity storage inside the existing core-owned `flow_msg_projection_t` sidecar using fixed bounded arrays plus a `has_durable_identity` flag. Do not allocate a second sidecar and do not use `transport_context`.
- [ ] Update `flow_msg_projection_empty()` so an identity-only sidecar is retained.
- [ ] Ensure clone and retain-view copies remain valid with owned descriptors/projections and identity-only sidecars.
- [ ] Ensure `turbo_flow_msg_clear_projection()`, `turbo_flow_msg_clear_content()`, and `turbo_flow_msg_clear_result()` do not accidentally free identity-only metadata.
- [ ] Install/export `turbo_flow_durable_buffer.h` with the Graph component.

- [ ] **GREEN:** C/C++ ABI and message regression:

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_data_schema test_flow_projection_owner -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_data_schema|test_flow_projection_owner)$' --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/include/turbo_flow_durable_buffer.h turbo_flow/src turbo_flow/tests turbo_flow/CMakeLists.txt
git commit -m "feat(graph): add durable message identity metadata"
```

## Task 4: Add the provider-neutral buffer binding registry and durable admission path

**Files:**
- Modify: `turbo_flow/include/turbo_flow_durable_buffer.h`
- Create: `turbo_flow/src/flow_durable_buffer.c`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/src/flow_compile.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** create a bounded memory Inbox in the test, bind it to resource `intake.store`, compile/start this Graph:

```text
buffer intake resource intake.store
source telemetry
stage downstream
stage main {
  telemetry -> intake
  intake -> downstream
}
```

Register a downstream callback that increments a counter. Publish one message and assert:
  - publish returns `SALTS_OK` after Inbox admission;
  - Inbox snapshot has `pending_records == 1` and `admitted == 1`;
  - downstream counter is still zero;
  - no outgoing buffer edge executed in the upstream publication.

Add RED cases:
  - compile with an unbound buffer resource fails;
  - one resource bound to two buffer stages fails in #127;
  - malformed/non-self-contained payload fails before admission;
  - stable-required mode without durable identity fails and admits zero records;
  - provider `SALTS_ENOSPC` returns upstream and downstream remains zero.

- [ ] Implement a flow-owned registry of borrowed `turbo_flow_durable_buffer_binding_t *` keyed by exact resource name. `bind` copies the resource name/config but never owns/destroys `config.inbox`.
- [ ] During compile, resolve every buffer's resource to exactly one live binding, set its stage index/name on the binding, and reject duplicate use of one binding in #127.
- [ ] At bind time snapshot the Inbox generation. Require nonzero generation and valid finite `max_message_bytes`.
- [ ] Implement pointer-free record encoding from `turbo_flow_msg_t`:
  - payload must pass `flow_msg_payload_validate()`;
  - copy existing `turbo_flow_msg_content_descriptor()` when present;
  - when absent, create an exact generic opaque DATA descriptor rather than persisting projection pointers;
  - correlation is empty in #127 unless a future generic message metadata contract provides one;
  - `timestamp_ns`, `type`, `flags`, and payload bytes are copied;
  - never serialize `transport_context` or `_content_handle`.
- [ ] Stable mode reads `turbo_flow_msg_durable_identity()` and fails closed if absent.
- [ ] Generated mode allocates the local sequence before first provider call and formats `g<generation>:<sequence>` into bounded binding-owned scratch/state so a retry of that same admission uses the same identity.
- [ ] Add `flow_durable_buffer_admit_stage(flow, stage_index, message)`.
- [ ] In `flow_run_message_from_stage()`, special-case a queued buffer node before `flow_dispatch_validate_stage()`/`flow_dispatch_stage()`:
  1. call durable admission;
  2. mark this node terminal for the current execution;
  3. do **not** call `flow_apply_completion()` for the buffer node;
  4. therefore never activate outgoing edges in the upstream execution.

Do not register an ordinary adapter/executor for this node.

- [ ] **GREEN:** focused admission/cut/capacity tests plus Inbox regression:

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_inbox test_flow_run -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_inbox|test_flow_run)$' --output-on-failure
```

- [ ] Commit:

```bash
git commit -am "feat(graph): admit messages at durable buffer boundaries"
```

## Task 5: Refactor InboxSource into one reusable claim-to-Graph settlement driver

**Files:**
- Create: `turbo_flow/src/flow_inbox_driver_internal.h`
- Create: `turbo_flow/src/flow_inbox_driver.c`
- Modify: `turbo_flow/src/flow_inbox_source.c`
- Modify: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_inbox_source.c`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** first freeze all current public InboxSource behavior with the existing `test_flow_inbox_source` suite. Add a durable-buffer internal-driver case whose claimed record must start downstream from a buffer origin, not from an adapter-free public Source.

- [ ] Introduce internal origin metadata:

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

The driver owns the current claim/message/run and the existing phases `IDLE`, `GRAPH_RUNNING`, `SETTLE_COMPLETE`, `SETTLE_FAIL`, `SETTLE_UNKNOWN`.

- [ ] Move the current record-to-message materialization and complete/fail/retry/reconcile state machine from `flow_inbox_source.c` into `flow_inbox_driver.c` without semantic changes.
- [ ] Keep public `turbo_flow_inbox_source_t` as a thin wrapper that resolves and validates an adapter-free Source then delegates to the shared driver.
- [ ] Generalize the internal run opening seam without changing public `turbo_flow_run_open()`:
  - normal Source origin keeps current `flow_publish_message_entered()` behavior;
  - buffer origin is allowed only for an `is_buffer` stage and feeds the retained message into a new execution starting from that buffer's outgoing edges;
  - async terminal/worker completion must still keep the run active until terminal.
- [ ] A buffer-origin run must call execution-region logic with `origin_stage == buffer_stage`, which intentionally traverses the buffer's outgoing edges but does not execute the buffer admission again.
- [ ] Preserve exact current public InboxSource result states and settlement errors.

- [ ] **GREEN:** both old Source and new buffer origin behavior:

```bash
cmake --build --preset linux-dev-user --target test_flow_inbox_source test_flow_durable_buffer test_flow_run test_flow_async_terminal -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_inbox_source|test_flow_durable_buffer|test_flow_run|test_flow_async_terminal)$' --output-on-failure
```

- [ ] Commit:

```bash
git add turbo_flow/src turbo_flow/tests turbo_flow/CMakeLists.txt
git commit -m "refactor(graph): share inbox claim graph driver"
```

## Task 6: Implement single-owner automatic durable drain and lifecycle

**Files:**
- Modify: `turbo_flow/src/flow_durable_buffer.c`
- Modify: `turbo_flow/src/flow_inbox_driver.c`
- Modify: `turbo_flow/include/turbo_flow_durable_buffer.h`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] **RED:** after one upstream publication has produced one pending record and zero downstream executions, call `turbo_flow_durable_buffer_progress(binding)` until idle. Assert:
  - exactly one downstream execution occurs;
  - successful Graph completion calls Inbox complete;
  - pending returns to zero and completed becomes one;
  - a second progress round on empty storage is `SALTS_OK` and does nothing.

Add cases for downstream Graph failure, cancel, settlement provider failure, `SALTS_EALREADY` reconciliation, and stable replay. Reuse existing InboxSource test fault patterns; do not introduce a second settlement state machine.

- [ ] Implement `progress()` as a serialized non-blocking state machine:
  - if driver idle: request at most one claim; map `SALTS_ENOENT` to normal `SALTS_OK`;
  - if driver active: poll/settle it;
  - never claim a second record while one run/settlement is owned.
- [ ] Implement `quiesce()` to stop future buffer admission and call `turbo_flow_inbox_close()`. Retry exact `SALTS_EBUSY`; never mark quiesced on failure.
- [ ] Implement `drain(binding, timeout_ms)` for retirement:
  - after quiesce, never claim a new pending record;
  - if an existing run was canceled/terminated by Graph stop, poll it until its owned claim is settled or timeout/error occurs;
  - after no live driver claim remains, snapshot Inbox;
  - if `pending_records`, `failed_records`, or other live records remain, return `SALTS_EBUSY` in #127 rather than discard/retry/drop;
  - this limitation is the explicit handoff to #130.
- [ ] `unbind()` requires non-STARTED Graph and idle driver. It removes the flow registry entry and frees only binding-owned metadata; it never destroys the Inbox.

- [ ] **GREEN:** run lifecycle and error-state matrix:

```bash
cmake --build --preset linux-dev-user --target test_flow_durable_buffer test_flow_inbox_source -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_flow_durable_buffer|test_flow_inbox_source)$' --output-on-failure
```

- [ ] Commit:

```bash
git commit -am "feat(graph): drive durable buffer claims and settlement"
```

## Task 7: Add configured bounded-memory durable resource provider

**Files:**
- Create: `io/durable/CMakeLists.txt`
- Create: `io/durable/src/turbo_flow_durable_memory_plugin.c`
- Create: `io/durable/src/turbo_flow_durable_memory_plugin_config.c`
- Create: `io/durable/src/turbo_flow_durable_memory_plugin_internal.h`
- Create: `io/durable/tests/CMakeLists.txt`
- Create: `io/durable/tests/test_durable_memory_plugin.c`
- Modify: root `CMakeLists.txt`
- Modify: package/install configuration as required for installed plugin placement

Production provider kind:

```text
flow.durable.memory
```

Exact resolved channel config v1:

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

`identity_mode` accepts only `generated` or `stable_required`.

- [ ] **RED:** build a plugin-host fixture with this DLL, resolved config, and a Graph containing `buffer intake resource intake.store`. Assert generation preflight/materialize succeeds only after the new resource provider exists. Add exact-schema failures for unknown/missing fields, zero capacities, `max_claims > max_records`, record bytes > total bytes, invalid identity mode, and a resource referenced by zero/multiple buffer stages.

- [ ] Implement one transactional **resource** provider, not an adapter provider.
- [ ] `preflight` is side-effect-free and validates exact config plus exactly one buffer reference with `stage->is_buffer`.
- [ ] `materialize`:
  1. creates `turbo_flow_inbox_memory_create()` with exact finite bounds;
  2. binds it with `turbo_flow_durable_buffer_bind()`;
  3. publishes one Product owner with `CONTROL_THREAD | EXTERNAL_POLL`;
  4. registers no Graph adapter/stage callback.
- [ ] Product owner callbacks:
  - `poll`: call `turbo_flow_durable_buffer_progress()`; ignore the external poll timeout rather than block the control thread;
  - `quiesce`: call durable-buffer quiesce;
  - `drain`: call durable-buffer drain with provided timeout;
  - `shutdown`: unbind, then destroy the already-closed empty memory Inbox; propagate exact error;
  - `destroy`: free owner memory only after successful shutdown lifecycle.
- [ ] Never allocate a fallback provider after any memory provider error.
- [ ] Install `tf_durable_memory_plugin` to the normal plugin runtime location; do not add an unnecessary public adapter library.

- [ ] **GREEN:** plugin generation E2E: publish -> memory commit -> upstream complete -> repeated generation poll -> downstream execute -> settlement.

```bash
cmake --build --preset linux-dev-user --target test_durable_memory_plugin test_flow_plugin_generation -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_durable_memory_plugin|test_flow_plugin_generation)$' --output-on-failure
```

- [ ] Commit:

```bash
git add io/durable CMakeLists.txt cmake
git commit -m "feat(graph): add configured memory durable buffer provider"
```

## Task 8: Add TurboDB durable resource provider with basic #127 parity

**Files:**
- Create: `io/turbodb/src/turbo_flow_turbodb_plugin.c`
- Create: `io/turbodb/src/turbo_flow_turbodb_plugin_config.c`
- Create: `io/turbodb/src/turbo_flow_turbodb_plugin_internal.h`
- Modify: `io/turbodb/CMakeLists.txt`
- Create: `io/turbodb/tests/test_turbodb_durable_buffer_plugin.c`
- Modify: `io/turbodb/tests/CMakeLists.txt`

Production provider kind:

```text
flow.durable.turbodb
```

Exact #127 channel config v1:

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

#127 accepts only file-backed SQLite and `open_mode: exclusive`. `takeover` and restart reconciliation belong to #130.

- [ ] **RED:** use the existing `test_turbodb_inbox.c` fixture pattern to create a temp SQLite file and explicitly provision the exact v2 metadata/records tables and indexes. Load `tf_turbodb_plugin` through PluginHost. Assert the same Graph topology used by the memory provider works with only the channel kind/config changed.

- [ ] Add negative tests:
  - `:memory:` rejected;
  - missing/old/wrong schema rejected;
  - unknown config field rejected;
  - provider create/commit failure returns exact error and downstream execution count remains zero;
  - no memory provider is materialized or consulted;
  - pending backlog during generation retirement returns busy rather than deleting/forgetting records.

- [ ] Build `tf_turbodb_plugin` as a separate SHARED plugin target following the existing CNet packaging pattern. Keep `TurboFlow::TurboDbAdapter` as the existing adapter/library API; do not turn that public target itself into a PluginHost root ABI.
- [ ] Provider preflight validates exact serialized config without creating/migrating/repairing database objects.
- [ ] Materialize builds the existing `orm_config_t` with driver `sqlite` and one `filename` option, then calls `turbo_flow_turbodb_inbox_create()`; it never creates schema objects.
- [ ] Bind the resulting Inbox through the same `turbo_flow_durable_buffer_bind()` contract used by memory.
- [ ] Product owner lifecycle/poll is identical to Task 7 except it owns TurboDB Inbox connections.
- [ ] If shutdown reaches `turbo_flow_inbox_destroy()` and the provider still has live records, propagate `SALTS_EBUSY`; do not close connections by force and do not mark restart recovery solved.

- [ ] **GREEN:** basic provider parity in a full developer SDK environment:

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

## Task 9: Run provider-neutral durable-buffer conformance matrix

**Files:**
- Create: `turbo_flow/tests/durable_buffer_conformance.h`
- Modify: `io/durable/tests/test_durable_memory_plugin.c`
- Modify: `io/turbodb/tests/test_turbodb_durable_buffer_plugin.c`
- Modify: `turbo_flow/tests/test_flow_durable_buffer.c`

- [ ] Extract behavior assertions, not provider setup, into one conformance helper. Run the same logical cases against memory and TurboDB where durability differences do not apply.
- [ ] Required cases:
  1. admission before downstream execution;
  2. capacity 1 accepts one and rejects N+1 without downstream bypass;
  3. retained-byte and per-record limits;
  4. generated identity exact retry inside one admission attempt;
  5. stable identity exact replay returns original receipt;
  6. same stable identity with changed payload/content/timestamp fails `SALTS_EPROTO`;
  7. downstream success -> complete;
  8. downstream Graph failure -> failed record;
  9. cancel -> explicit failed/canceled outcome;
  10. settlement pending/unknown/reconcile retains ownership truthfully;
  11. quiesce rejects new admission while an already-owned run can settle;
  12. pending backlog blocks #127 retirement; no implicit retry/discard/drop;
  13. provider error never invokes downstream and never selects another provider.
- [ ] Add one internal-edge case `source -> transform -> buffer -> transform -> sink`, proving the buffer is not limited to protocol intake or direct Source edges.
- [ ] Add fan-in case with two Sources writing one named buffer; one serialized durable source drains FIFO in #127.
- [ ] Add fan-out-after-buffer case and verify fan-out occurs only in the downstream execution.

- [ ] **GREEN:** focused conformance set:

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

## Task 10: Package, install, CI, and close the #127 core gate

**Files:**
- Modify: `tests/install_consumer/CMakeLists.txt`
- Modify: `tests/install_consumer/main.c`
- Add/modify installed plugin consumer fixture if needed for `tf_durable_memory_plugin` / `tf_turbodb_plugin`
- Modify: `.github/ci/protocol-network-compile/CMakeLists.txt` or create a generic durable-buffer compile-contract project beside it
- Modify: `.github/workflows/protocol-network-intake.yml` only if reusing that workflow remains semantically correct; otherwise create `.github/workflows/graph-durable-buffer.yml`
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Modify: root/package CMake files required for exported Graph header and plugin installation
- Update: `docs/superpowers/specs/2026-09-15-graph-durable-buffer-design.md` status/evidence only after exact gates pass
- Update: issue #127 and Draft PR #126 evidence

- [ ] Installed C and C++ consumers must compile `turbo_flow_durable_buffer.h` and the public `is_buffer` stage-plan field.
- [ ] Verify Graph component exports the durable-buffer API without requiring TurboDB.
- [ ] Verify `TURBO_FLOW_BUILD_TURBODB_ADAPTER=OFF` still allows Graph + memory durable provider to build/install; no CMake fallback should silently create a TurboDB substitute.
- [ ] Verify `tf_durable_memory_plugin` and, when TurboDB is enabled, `tf_turbodb_plugin` are installed as runtime plugins with expected exports/dependency closure.
- [ ] Extend the existing **no-secret** GitHub compile-contract CI to compile new Graph durable-buffer core and memory-provider translation units with ASan+UBSan instrumentation. Do not reintroduce `QIGAO_CI_READ_TOKEN` or any cross-repository secret.
- [ ] Do not claim the no-secret compile-contract CI exercises TurboDB runtime if ORM/private SDK dependencies are absent. TurboDB runtime evidence comes from the full configured developer/CI SDK gate below.

- [ ] Focused Debug/ASan runtime gate:

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user -j2
ctest --test-dir build/linux-gcc-debug \
  -R '(durable_buffer|flow_inbox|flow_run|plugin_generation|turbodb_inbox)' \
  --output-on-failure
```

- [ ] Full Debug/ASan gate:

```bash
ctest --test-dir build/linux-gcc-debug --output-on-failure
```

- [ ] Release gate:

```bash
cmake --preset linux-release-user
cmake --build --preset linux-release-user -j2
ctest --test-dir build/linux-gcc-release --output-on-failure
```

- [ ] Install/package consumer gate using the repository's existing install-consumer workflow/targets. Verify fresh prefix consumption, C/C++ headers, exported targets, plugin runtime files, and native ABI/dependency checks.
- [ ] `git diff --check` and repository formatting checks must be clean.
- [ ] Confirm source tree contains no `TODO`, `TBD`, placeholder implementation, compatibility shim, database-to-memory fallback, or direct durable-buffer bypass introduced by #127.
- [ ] Confirm #128/#130/#131 remain open for partition/restart/observability work and are not falsely marked complete by #127.
- [ ] Update #127 with exact commit SHA and exact test/run evidence. Keep #126 Draft until #129 migrates JTT808/CoAP and final #118 acceptance passes.

- [ ] Commit final #127 packaging/evidence changes:

```bash
git add .github cmake tests docs CMakeLists.txt
git commit -m "build(graph): gate durable buffer core"
```

## Final #127 Definition of Done

#127 is complete only when all of the following are true on one exact head:

- `buffer` is a compiler-visible execution cut with no synchronous continuation across storage;
- upstream success occurs only after provider admission/commit;
- downstream execution starts only from a later provider claim;
- one Graph topology selects memory or TurboDB by configured resource provider;
- generated and stable identity modes obey the documented replay guarantees;
- no runtime pointer/session/projection is persisted;
- no provider failure bypasses the buffer or selects fallback storage;
- existing InboxSource public behavior is preserved through the shared driver refactor;
- single-owner drain truthfully handles Graph success/failure/cancel/settlement unknown;
- pending backlog blocks #127 retirement instead of being lost; restart/detach remains explicitly #130;
- provider-neutral conformance, Debug/ASan, Release, install/package, ABI/export, and no-secret compile-contract gates have fresh exact-head evidence.

Only after this gate should #129 replace `ProtocolNetworkIntake -> Inbox -> InboxSource` with the generic durable-buffer boundary. #128 may then scale drain concurrency without changing #127 ownership or settlement semantics.
