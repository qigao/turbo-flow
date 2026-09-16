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
- A message carrying a non-null `transport_context`, non-persistable result sidecar, or another runtime-only capability is rejected at the durable boundary; the boundary never silently drops such state.
- Schema-bound projection values may cross a durable boundary only because canonical payload bytes remain the source of truth. Persist payload + content descriptor, never projection pointers. A projection/result that cannot be reconstructed from canonical payload is rejected.
- `buffer` is a compiler/runtime execution cut, not a synchronous transform stage.
- Upstream success means provider admission/commit succeeded. It does not mean downstream Graph/Sink completion.
- Downstream settlement remains explicit `complete/fail/retry/reconcile`; no blind retry and no exactly-once claim.
- #127 is single-owner/single-record drain. Worker pools, batch claim, and partition ordering belong to #128.
- #127 does not claim crash/restart detach support. Clean generation retirement pre-drains accepted backlog while downstream operations/Sinks are still active. #130 owns crash detach/reopen/takeover and ambiguous restart recovery.
- Transactional resource materialization happens before `turbo_flow_compile()`. A provider binds an already-created Inbox to a parsed buffer resource; compile validates the binding.
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

Public `turbo_flow_stage_at()` exposes it so transactional resource providers validate references without private headers.

### Generic durable metadata

Create `turbo_flow/include/turbo_flow_durable_buffer.h`:

```c
#define TURBO_FLOW_DURABLE_BUFFER_API_VERSION UINT32_C(1)
#define TURBO_FLOW_DURABLE_SOURCE_ID_MAX 127u
#define TURBO_FLOW_DURABLE_ADMISSION_ID_MAX 255u
#define TURBO_FLOW_DURABLE_CORRELATION_MAX 255u
#define TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES (2u * 1024u * 1024u)

typedef struct turbo_flow_durable_identity_s {
  size_t size;
  uint32_t version;
  vstr source_id;
  vstr admission_id;
  vstr correlation;
  uint64_t source_sequence;
} turbo_flow_durable_identity_t;

#define TURBO_FLOW_DURABLE_IDENTITY_INIT \
  {sizeof(turbo_flow_durable_identity_t), TURBO_FLOW_DURABLE_BUFFER_API_VERSION, \
   {NULL, 0u}, {NULL, 0u}, {NULL, 0u}, 0u}

TURBO_FLOW_C_API int turbo_flow_msg_set_durable_identity(
    turbo_flow_msg_t *message, const turbo_flow_durable_identity_t *identity);

TURBO_FLOW_C_API int turbo_flow_msg_durable_identity(
    const turbo_flow_msg_t *message, turbo_flow_durable_identity_t *out);
```

Source/admission IDs are required and copied; correlation is optional and copied. Metadata survives clone/move/retain-view and never uses `transport_context`.

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

Binding owns no Inbox. Product owner owns Inbox lifetime. `progress` is caller-serialized/non-blocking. `quiesce` closes new admission. While Graph is STARTED, `drain` may continue processing accepted backlog until idle or timeout. After Graph is STOPPED/FAILED it may only settle an already-owned terminal/canceled claim and verify no live backlog. `unbind` requires non-STARTED Graph and no unresolved claim.

### Generated identity

At bind time snapshot Inbox provider generation. For each generated-mode admission allocate local sequence before provider call:

```text
source_id       = configured buffer stage name
admission_id    = "g<provider-generation>:<binding-uuid>:<local-sequence>"
source_sequence = local-sequence
correlation     = empty
```

The generated ID is reused for an internal retry of that admission operation. Sequence overflow returns `SALTS_ERANGE`. Provider generation avoids intentional key reuse across a new TurboDB ownership generation. Generated mode still promises only at-least-once producer redelivery semantics.

Stable-required mode fails closed unless generic durable metadata provides non-empty source/admission IDs. Optional correlation is copied to Inbox correlation.

---

## Task 1: Parse `buffer` and expose stage identity

**Files:** `turbo_flow/parser/flow_lexer.re`, `turbo_flow/parser/flow_grammar.y`, `turbo_flow/src/flow_parser_internal.h`, `turbo_flow/src/flow_parser.c`, `turbo_flow/src/flow_internal.h`, `turbo_flow/include/turbo_flow.h`, `turbo_flow/src/flow_core.c`, new `turbo_flow/tests/test_flow_durable_buffer.c`, `turbo_flow/tests/CMakeLists.txt`.

- [ ] **RED:** parse:

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

Require `turbo_flow_stage_at(...)->is_buffer == 1`. Negative: missing resource, duplicate node name, buffer inside reusable stage template.

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user --target test_flow_durable_buffer -j2
ctest --test-dir build/linux-gcc-debug -R '^test_flow_durable_buffer$' --output-on-failure
```

- [ ] Add `TURBO_FLOW_TOKEN_BUFFER`, root grammar `buffer_decl ::= BUFFER IDENT RESOURCE adapter_name`, and `flow_parse_add_buffer()`.
- [ ] Store copied resource name, `is_buffer=1`, no adapter/operation/executor options; reject non-root declaration.
- [ ] Add `is_buffer` to all internal/public stage copy/reset/view paths.
- [ ] **GREEN:** `test_flow_durable_buffer` + `test_turbo_flow`.
- [ ] Commit `feat(graph): parse durable buffer boundaries`.

## Task 2: Compile buffer as non-executor execution cut

**Files:** `flow_internal.h`, `flow_compile.c`, `flow_plan.c`, `flow_runtime.c`, `test_flow_durable_buffer.c`.

- [ ] **RED:** sealed-plan assertions: `FLOW_RUNTIME_NODE_BUFFER`, no executor, ordinary executors unchanged, external-I/O/settlement barriers.
- [ ] Add descriptor-only core semantic metadata `core.buffer` with Message input/output and no executable callback.
- [ ] `flow_build_runtime_plan()` skips buffer executor; `flow_verify_compiled_plan()` accepts no executor only for source/port/buffer special nodes.
- [ ] Exclude buffer from native-to-CFlow lowering regions.
- [ ] Add execution-region reachability:

```c
int flow_mark_execution_region_from_stage(
    const turbo_flow_t *flow, uint8_t *reachable, uint32_t *worklist,
    size_t capacity, uint32_t origin_stage);
```

Reached buffer is included but traversal stops before its outgoing edges unless it is `origin_stage`. Logical compile-time topology reachability remains unchanged.
- [ ] `flow_run_message_from_stage()` uses execution-region reachability for prevalidation/reorder/queue and skips `flow_dispatch_validate_stage()` for buffer nodes.
- [ ] **GREEN:** `test_flow_durable_buffer`, `test_flow_run`, `test_turbo_flow`.
- [ ] Commit `feat(graph): compile durable buffer execution cuts`.

## Task 3: Add clone-safe generic durable metadata

**Files:** new durable header, `flow_internal.h`, `flow_message.c`, Graph CMake, durable tests + C++ header probe.

- [ ] **RED:** set/get/clone/retain-view/move/clear/cleanup; mutate caller source/admission/correlation after setter and prove stored bytes independent.
- [ ] Negative: empty source/admission, over-limit fields, bad ABI, mutation while result claim active.
- [ ] Store fixed arrays + `has_durable_identity` inside existing `flow_msg_projection_t`; no second sidecar.
- [ ] `flow_msg_projection_empty()` retains identity-only sidecar. Clone/retain-view and clear projection/content/result preserve durable metadata.
- [ ] Install/export header with Graph.
- [ ] **GREEN:** durable test + data-schema/projection-owner regression.
- [ ] Commit `feat(graph): add durable message identity metadata`.

## Task 4: Add provider-neutral binding registry and admission

**Files:** durable header, new `flow_durable_buffer.c`, `flow_internal.h`, `flow_core.c`, `flow_compile.c`, `flow_runtime.c`, Graph CMake/tests.

- [ ] **RED:** manually create memory Inbox, bind `intake.store`, compile/start:

```text
buffer intake resource intake.store
source telemetry
stage downstream
stage main {
  telemetry -> intake
  intake -> downstream
}
```

Publish one message. Require upstream `SALTS_OK`, Inbox pending/admitted one, downstream zero.
- [ ] Negative: unbound resource, one binding used by multiple buffers, stable-required without identity, malformed payload, non-null `transport_context`, non-persistable result sidecar, provider capacity/error. Admission failure always leaves downstream zero.
- [ ] Add flow-owned registry of borrowed bindings keyed by exact resource name. Compile resolves each buffer to one binding and sets stage index/name.
- [ ] Bind snapshots Inbox generation; require nonzero generation and finite max-message bound.
- [ ] Record encoder:
  - validate payload backing;
  - reject non-null `transport_context`;
  - reject `turbo_flow_msg_result(...) != NULL` or other non-reconstructable runtime capability;
  - allow schema projection only with canonical payload + content descriptor; persist payload/descriptor only;
  - absent descriptor -> deterministic generic opaque DATA descriptor (`application/octet-stream`, buffer name identity);
  - stable mode copies correlation; generated mode correlation empty;
  - copy timestamp/type/flags/payload only after validation.
- [ ] Generated sequence overflow fails before provider call.
- [ ] Add `flow_durable_buffer_admit_stage()`.
- [ ] Runtime queued buffer branch: admit; mark terminal for this execution; call neither ordinary dispatch nor `flow_apply_completion()`; outgoing edges remain inactive.
- [ ] **GREEN:** durable + Inbox + run regression.
- [ ] Commit `feat(graph): admit messages at durable buffer boundaries`.

## Task 5: Refactor InboxSource into shared claim-to-Graph driver

**Files:** new `flow_inbox_driver_internal.h`, new `flow_inbox_driver.c`, `flow_inbox_source.c`, `flow_run.c`, `flow_internal.h`, Graph CMake/tests.

- [ ] **RED:** freeze public InboxSource and add buffer-origin claim case.
- [ ] Shared config:

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

- [ ] Move record->message construction and existing claim/run/settlement phases into shared driver with no semantic change.
- [ ] Public InboxSource becomes adapter-free Source wrapper.
- [ ] Generalize run origin internally; public `turbo_flow_run_open()` unchanged. Buffer origin starts execution at `origin_stage == buffer_stage`, following outgoing edges without re-admission.
- [ ] **Critical:** do not treat bare `flow_run_message_from_stage()` return as terminal. Buffer-origin publish participates in the same `flow_async_publication` completion accounting as ordinary Reactive runs, so async-terminal/worker descendants keep run ACTIVE until actual callback completion.
- [ ] Preserve cancel, retry-settlement, and EALREADY reconcile behavior.
- [ ] **GREEN:** InboxSource + durable + run + async-terminal tests.
- [ ] Commit `refactor(graph): share inbox claim graph driver`.

## Task 6: Implement single-owner drain plus generation pre-retire drain

**Files:** `flow_durable_buffer.c`, `flow_inbox_driver.c`, durable header/tests, `turbo_flow/src/flow_plugin_generation.c`, `turbo_flow/src/flow_run.c`, generation tests.

- [ ] **RED:** normal `progress()` converts one pending record into exactly one downstream execution/settlement; empty progress is success. Add downstream failure/cancel/settlement-unknown cases.
- [ ] `progress()` owns at most one claim: idle -> claim/start one; active -> poll/settle; `SALTS_ENOENT` -> success.
- [ ] `quiesce()` closes new Inbox admission; no implicit drop/retry.
- [ ] `drain(binding, timeout)`:
  - if Graph STARTED: continue `progress()` until no pending/in-flight live record remains or timeout/failure/unknown state blocks progress;
  - if Graph STOPPED/FAILED: never claim new backlog, only settle an already-owned terminal/canceled claim and verify storage empty.
- [ ] Add an **internal** buffer-origin run admission mode allowed while Graph is `STARTED` but ordinary publish admission is `PAUSED`. This mode is usable only by durable drain and still participates in active-run/async completion accounting.
- [ ] Add internal `flow_durable_buffers_prepare_retire(flow, timeout_ms)`:
  1. if flow has no durable bindings, return success without changing lifecycle;
  2. pause ordinary Graph publish admission;
  3. wait for already-accepted upstream publishes to leave their current execution region;
  4. derive upstream-to-downstream buffer order from the compiled stage DAG (compile rejects cycles), independent of declaration/binding order;
  5. in that order, quiesce one binding and drain it to idle using the special paused-state buffer-origin run mode while later providers, operation bindings, Sink owners, and Graph runtime remain active; finally verify every provider is closed and has no live records/claims;
  6. on timeout, failed/unknown record, or exact provider error, return that error **before** operation bindings are closed, owners are quiesced, or Graph is stopped;
  7. on success, all durable bindings have no live records/claims and normal generation retirement may proceed.
- [ ] Call this helper at the beginning of `flow_plugin_generation_retire()` before `flow_plugin_operations_close()`, `generation->poll_closed = 1`, owner quiesce, and Graph stop.
- [ ] If pre-retire drain fails, keep generation/owners/operation bindings intact enough for diagnostic/operator handling; never continue destructive retirement steps.
- [ ] Existing no-buffer generation lifecycle must remain behaviorally unchanged.
- [ ] `unbind()` requires non-STARTED Graph and idle driver; removes metadata only, never destroys Inbox.

- [ ] **GREEN:** durable lifecycle + PluginGeneration retirement regression, including a case with pending healthy backlog that is drained before operation/sink owner quiesce.
- [ ] Commit `feat(graph): drain durable buffers before generation retirement`.

## Task 7: Add configured bounded-memory resource provider

**Files:** new `io/durable/CMakeLists.txt`, memory plugin/config/internal source, tests; root/package CMake.

Provider kind `flow.durable.memory`:

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

- [ ] **RED:** PluginHost generation fails until transactional resource provider exists. Negative exact-schema/bounds/reference cases.
- [ ] Preflight is side-effect-free and requires exactly one `is_buffer` reference.
- [ ] Materialize creates bounded memory Inbox, binds it, returns Product owner `CONTROL_THREAD | EXTERNAL_POLL`; registers no Graph adapter.
- [ ] Owner `poll` calls non-blocking `progress()`.
- [ ] Owner `quiesce` is now verification/idempotent close only; normal PluginGeneration retirement has already run `flow_durable_buffers_prepare_retire()` while downstream remained active.
- [ ] Owner `drain` after Graph stop verifies no live claim/backlog; never starts new work.
- [ ] Owner `shutdown`: unbind then destroy closed/empty Inbox; exact error propagation. `destroy`: free owner.
- [ ] Install `tf_durable_memory_plugin`; no redundant adapter library or fallback provider.
- [ ] **GREEN:** memory plugin + PluginGeneration tests including exact retirement ordering.
- [ ] Commit `feat(graph): add configured memory durable buffer provider`.

## Task 8: Add TurboDB resource provider with #127 parity

**Files:** new TurboDB plugin/config/internal sources, modify `io/turbodb/CMakeLists.txt`, new plugin test/CMake.

Provider kind `flow.durable.turbodb`:

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

#127 supports file-backed SQLite + `exclusive` only; takeover/restart is #130.

- [ ] **RED:** existing TurboDB Inbox fixture shape: temp SQLite; explicit v2 schema/index provisioning; load plugin; same Graph topology as memory with only channel kind/config changed.
- [ ] Negative: `:memory:`, old/wrong/missing schema, unknown config field, create/commit failure, no memory fallback, pre-retire drain failure.
- [ ] Add separate SHARED `tf_turbodb_plugin` following CNet adapter+plugin packaging. Keep `TurboFlow::TurboDbAdapter` unchanged.
- [ ] Preflight validates serialized config only; never creates/migrates/repairs DB schema.
- [ ] Materialize constructs existing `orm_config_t` (`sqlite`, one `filename` option), calls existing TurboDB Inbox create, then same bind API as memory.
- [ ] Owner lifecycle matches Task 7. If pre-retire drain cannot reach idle, generation retirement stops before operation close/owner quiesce/Graph stop. If provider destroy nevertheless reports live records, propagate error; never force-close.

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user --target test_turbodb_durable_buffer_plugin test_turbodb_inbox -j2
ctest --test-dir build/linux-gcc-debug -R '^(test_turbodb_durable_buffer_plugin|test_turbodb_inbox)$' --output-on-failure
```

- [ ] Commit `feat(turbodb): provide graph durable buffer resource`.

## Task 9: Provider-neutral conformance matrix

**Files:** new `turbo_flow/tests/durable_buffer_conformance.h`, memory/TurboDB plugin tests, core durable test.

- [ ] Equivalent cases against both providers where durability differences do not apply:
  1. admission before downstream execution;
  2. capacity 1/N/N+1 + byte limits;
  3. generated identity internal retry stability;
  4. stable identity + correlation exact replay;
  5. changed complete record under same stable identity -> `SALTS_EPROTO`;
  6. transport context/result capability rejected rather than lost;
  7. downstream success/failure/cancel;
  8. settlement pending/unknown/reconcile;
  9. generation pre-retire pauses new publishes and drains accepted backlog while downstream operation/Sink owners are still live;
  10. failed/unknown backlog aborts retirement before destructive lifecycle steps;
  11. provider error -> zero downstream + zero fallback.
- [ ] Internal edge: `source -> transform -> buffer -> transform -> sink`.
- [ ] Fan-in two Sources -> one buffer, serialized FIFO #127.
- [ ] Fan-out after buffer only in downstream execution.
- [ ] **GREEN:** focused durable core/memory/TurboDB targets.
- [ ] Commit `test(graph): require durable buffer provider parity`.

## Task 10: Package, install, CI, and close #127 core gate

**Files:** install consumers, package config, plugin install wiring, no-secret compile-contract workflow/project, spec/evidence updates.

- [ ] Installed C/C++ consumers compile durable header + public `is_buffer`.
- [ ] Graph component exports durable API without TurboDB dependency.
- [ ] `TURBO_FLOW_BUILD_TURBODB_ADAPTER=OFF` still builds/installs Graph + memory provider; no substitute path.
- [ ] Verify memory and enabled TurboDB plugin runtime install/export/dependency closure.
- [ ] Extend existing **no-secret** GitHub compile-contract CI for new core + memory-plugin TUs with ASan+UBSan. Do not reintroduce `QIGAO_CI_READ_TOKEN` or cross-repo secrets.
- [ ] Do not claim no-secret CI runs TurboDB runtime without full ORM/private SDK. TurboDB runtime evidence comes from full SDK gates.

Focused Debug/ASan:

```bash
cmake --preset linux-dev-user
cmake --build --preset linux-dev-user -j2
ctest --test-dir build/linux-gcc-debug \
  -R '(durable_buffer|flow_inbox|flow_run|plugin_generation|turbodb_inbox)' \
  --output-on-failure
```

Full Debug/ASan:

```bash
ctest --test-dir build/linux-gcc-debug --output-on-failure
```

Release:

```bash
cmake --preset linux-release-user
cmake --build --preset linux-release-user -j2
ctest --test-dir build/linux-gcc-release --output-on-failure
```

- [ ] Run fresh-prefix install-consumer/profile/ABI/export/dependency gates.
- [ ] `git diff --check` and formatting clean.
- [ ] Scan new #127 source for unfinished implementation markers/placeholders, compatibility shims, database-to-memory fallback, or direct durable-buffer bypass; none remain.
- [ ] Confirm #128/#130/#131 remain open and are not reported complete by #127.
- [ ] Update #127 with exact head/test evidence. Keep #126 Draft until #129 migrates JTT808/CoAP and #118 acceptance passes.
- [ ] Commit `build(graph): gate durable buffer core`.

## Final #127 Definition of Done

One exact head must prove:

- compiler-visible execution cut with no synchronous continuation across storage;
- upstream success only after provider admission/commit;
- downstream only after later claim;
- memory/TurboDB selected by resource config without topology change;
- generated/stable identity + correlation semantics match documented guarantees;
- runtime transport/result capabilities are rejected rather than silently lost;
- no runtime pointer/session/projection is persisted;
- no provider failure bypass/fallback;
- existing InboxSource behavior survives shared-driver refactor;
- single-owner drain truthfully handles success/failure/cancel/settlement unknown;
- PluginGeneration pre-retire drains accepted backlog while downstream bindings/Sinks are still available and aborts before destructive lifecycle steps on failure;
- crash/restart detach remains #130;
- provider-neutral conformance, Debug/ASan, Release, install/package, ABI/export, and no-secret compile-contract gates have fresh exact-head evidence.

Only after #127 is green should #129 replace `ProtocolNetworkIntake -> Inbox -> InboxSource` with the generic durable-buffer boundary. #128 may then scale drain concurrency without changing #127 ownership/settlement semantics.

### Task 4 admission boundary correction (2026-09-16)

RED head `17b6bc01b5ba64c07a15c8e8e6eebf0102d71c19`: public compile run
35067314312 / job 104700441378 passed; runtime run 35067310947 / job
104700431505 reproduced seven failing admission cases (9 passed). First failure:
projection with descriptor but no canonical payload was admitted instead of ENOTSUP.
Additional failures cover an invisible active result claim on an unprojected message,
stale generation/handle, restart validation, rebind ID reuse, and provider aliases.

Generated bindings now allocate a Salts UUID namespace at bind time, failing on
entropy errors. Generation plus local sequence alone collided after rebind and
incorrectly deduplicated a new record. The namespace is an opaque admission-ID
component, not a new persisted field or a cross-process deduplication guarantee.
Provider identity and generation are checked at compile/start/admission. This is
not an atomic takeover fence: that remains the provider's responsibility.

### Task 6 retirement ordering clarification (2026-09-16)

Closing every provider before draining prevents a healthy `first -> second` buffer chain
from admitting the already accepted record into `second`. Retirement therefore closes
and drains cuts in compiled DAG order. It never bypasses a closed provider. One timeout
budget covers ordinary publish drain and all buffer drains. No-buffer retirement is unchanged.

The frozen binding API has no public settlement retry/reconcile entry point. An unknown
settlement remains pinned without replay; explicit recovery currently requires the internal
Inbox driver interface. Provider/operator integration must design a public recovery entry
point separately. Reset returns EBUSY, and void destroy retains the complete Flow with an
EBUSY diagnostic while a driver is unresolved; this is a safe retained state, not a claim
that an external opaque-binding caller can already recover it.
