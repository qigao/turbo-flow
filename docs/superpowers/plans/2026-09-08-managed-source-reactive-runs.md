# Managed Source Reactive Runs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Atomically register managed Source adapters and let each compiled Source start callback open exactly one bounded CFlow Reactive run without opening global runtime admission early.

**Architecture:** Reuse the canonical adapter/resource registries and existing Reactive scheduler. A callback-scoped token authorizes a private start-time run admission path; the corresponding active-adapter entry owns the run handle so rollback and stop close the run before calling the Source owner.

**Tech Stack:** C11, CFlow Reactive Publisher/Subscription/Scheduler, CSTL `vec_t`, Salts synchronization, TinyTest, CMake Presets, CodeGraph.

**Spec:** `docs/MANAGED_SOURCE_RUNS.md`

## Global Constraints

- Keep ordinary `turbo_flow_run_open()` restricted to `STARTED` plus globally open admission.
- Register adapter and managed resource atomically; failure never invokes owner `shutdown`.
- Use one canonical adapter registry, resource registry, and active-run registry; add no fallback or capability inference.
- Enforce `async_ingress_config.queue_capacity` for start-time and ordinary Reactive runs.
- Close a managed Source run before its `stop` or `shutdown` callback can release owner context.
- Preserve C11 and C++ installed-header consumption and all existing public structure layouts.

---

### Task 1: Public atomic managed Source registration

**Files:**
- Modify: `turbo_flow/include/turbo_flow.h`
- Modify: `turbo_flow/src/flow_core.c`
- Create: `turbo_flow/tests/test_flow_managed_source.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `turbo_flow_adapter_ops_t`, `turbo_flow_adapter_schema_t`, `turbo_flow_managed_boundary_provider_ops_t`.
- Produces: `turbo_flow_managed_source_registration_t`, `TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_INIT`, and `turbo_flow_register_managed_source_adapter()`.

- [x] **Step 1: Add failing registration tests**

  Define a fake owner whose metadata/descriptor/snapshot describe one Source. Test every null,
  size/version, missing callback, non-Source schema, non-Source boundary, duplicate adapter/UID, and
  deterministic allocation-fault input. After each failure assert adapter lookup, resource count,
  managed count, and shutdown count remain zero. Test success exposes exactly one adapter and one
  managed Source descriptor and invokes shutdown once on Flow destruction.

- [x] **Step 2: Run the focused test and confirm RED**

  Run: `cmake --preset win-dev-user && cmake --build --preset win-dev-user --target test_flow_managed_source`

  Expected: compilation fails because the new size/versioned types and function are absent.

- [x] **Step 3: Implement the atomic registration**

  Add version 1 registration fields `adapter_name`, `adapter_ops`, `schema`, `owner_name`,
  `boundary_ops`, and `ctx`. Validate a start callback, pure Source schema role/direction, complete
  resource callbacks, and Source descriptor. Stage `shutdown = NULL`, register adapter then resource,
  roll both vectors back on error, and restore shutdown only at the ownership commit point.

- [x] **Step 4: Build and run registration tests**

  Run: `cmake --build --preset win-dev-user --target test_flow_managed_source && ctest --preset win-dev-user -R '^test_flow_managed_source$' --output-on-failure`

  Expected: PASS with unchanged registries for every rejected input.

- [x] **Step 5: Commit the registration transaction**

  Run: `git add turbo_flow/include/turbo_flow.h turbo_flow/src/flow_internal.h turbo_flow/src/flow_core.c turbo_flow/tests/CMakeLists.txt turbo_flow/tests/test_flow_managed_source.c docs && git commit -m "feat(flow): register managed Source boundaries"`

### Task 2: Callback-scoped start-time run admission

**Files:**
- Modify: `turbo_flow/include/turbo_flow.h`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_adapter.c`
- Modify: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/tests/test_flow_managed_source.c`

**Interfaces:**
- Consumes: Task 1's managed Source marker on `flow_adapter_registration_t` and existing `flow_run_open_internal()` setup.
- Produces: `turbo_flow_managed_source_run_open(turbo_flow_t *, const turbo_flow_stage_plan_t *, cflow_publisher *, const turbo_flow_run_config_t *, turbo_flow_run_t **)` and an active-adapter-owned run pointer.

- [x] **Step 1: Add failing scope, identity, and capacity tests**

  From the fake start callback open an array Publisher for the exact callback stage and assert one
  active run. Add calls outside start, from another thread, twice in one callback, with copied or
  mismatched stage views, a non-Source stage, invalid Publisher/config, and queue capacity N+1.
  Assert all failures leave `active_runs` and `active_publishes` unchanged and ordinary
  `turbo_flow_run_open()` still rejects before the Flow reaches STARTED.

- [x] **Step 2: Run focused tests and confirm behavioral RED**

  Run: `cmake --build --preset win-dev-user --target test_flow_managed_source && ctest --preset win-dev-user -R '^test_flow_managed_source$' --output-on-failure`

  Expected: start-time open returns `SALTS_EINVAL` through the existing global admission check.

- [x] **Step 3: Implement the scoped admission token**

  Reserve active-adapter capacity before side effects. Around only a managed Source's start callback,
  install thread-local Flow/stage/adapter identity plus an empty run slot. The new API validates the
  exact token and callback view, then calls a refactored run setup whose registry admission accepts
  either normal STARTED/OPEN state or that exact start token. Commit the run to the slot only after
  subscription setup succeeds; reject duplicates before allocating.

- [x] **Step 4: Run focused and adjacent run tests**

  Run: `ctest --preset win-dev-user -R '^(test_flow_managed_source|test_flow_run|test_turbo_flow)$' --output-on-failure`

  Expected: all tests PASS and normal run semantics are unchanged.

- [x] **Step 5: Commit scoped run admission**

  Run: `git add turbo_flow/include/turbo_flow.h turbo_flow/src/flow_internal.h turbo_flow/src/flow_adapter.c turbo_flow/src/flow_run.c turbo_flow/tests/test_flow_managed_source.c && git commit -m "feat(flow): open managed Source runs during start"`

### Task 3: Rollback and stop ordering

**Files:**
- Modify: `turbo_flow/src/flow_adapter.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/tests/test_flow_managed_source.c`

**Interfaces:**
- Consumes: Task 2's active-adapter run pointer.
- Produces: `flow_close_managed_source_runs()` used by start rollback, normal stop, reset, and destroy paths.

- [x] **Step 1: Add failing rollback and ordering tests**

  Make the Source callback open successfully and then fail; make a later adapter fail; exercise a
  Publisher error and Flow stop with one accepted asynchronous message. Record ordered events for
  run terminal, active publish drain, Source stop, and shutdown. Assert run close occurs once before
  Source stop, shutdown occurs once only at registry teardown, and both run/publish counts reach zero.

- [x] **Step 2: Run the lifecycle tests and confirm RED**

  Run: `ctest --preset win-dev-user -R '^test_flow_managed_source$' --output-on-failure`

  Expected: the partial run remains registered or Source stop precedes run release.

- [x] **Step 3: Implement one rollback/stop protocol**

  Cancel and close each managed Source run in reverse active-adapter order, clear its slot before
  owner callbacks can reenter, and preserve the first terminal/adapter error. Use it after a failed
  start callback, during later-start rollback, after downstream admission is closed and accepted
  work drains in normal stop, and before registry shutdown. Do not unregister adapters/resources.

- [x] **Step 4: Run lifecycle tests under repetition**

  Run: `ctest --preset win-dev-user -R '^test_flow_managed_source$' --repeat until-fail:100 --output-on-failure`

  Expected: 100 consecutive PASS results with exact-once counters and ordering.

- [x] **Step 5: Commit lifecycle ordering**

  Run: `git add turbo_flow/src/flow_adapter.c turbo_flow/src/flow_runtime.c turbo_flow/src/flow_run.c turbo_flow/tests/test_flow_managed_source.c && git commit -m "fix(flow): close managed Source runs before owners"`

### Task 4: Documentation and installed consumers

**Files:**
- Modify: `docs/MANAGED_BOUNDARIES.md`
- Modify: `tests/install_consumer/main.c`
- Modify: `tests/install_consumer/CMakeLists.txt` only if the existing C/C++ source list cannot reference the API directly.

**Interfaces:**
- Consumes: all public Task 1/2 declarations.
- Produces: installed C and C++ compile/link coverage and final operator-facing lifecycle documentation.

- [x] **Step 1: Extend the install consumer**

  Declare initialized registration values and typed function pointers for both new exported APIs in
  the shared `.c` source compiled as C and C++. Exercise invalid input without mutating registries.

- [x] **Step 2: Document ownership and no-fallback behavior**

  Link `docs/MANAGED_BOUNDARIES.md` to `docs/MANAGED_SOURCE_RUNS.md`; state callback scope,
  Publisher backing lifetime, capacity, stop order, errors, and the absence of runtime mutation.

- [x] **Step 3: Run final verification**

  Run: `cmake --preset win-dev-user`

  Run: `cmake --build --preset win-dev-user`

  Run: `ctest --preset win-dev-user --output-on-failure`

  Run: `cmake --preset win-release-user && cmake --build --preset win-release-user`

  Run: `ctest --preset win-release-user --output-on-failure`

  Run: `cmake --build --preset install-win-dev-user`

  Run: `codegraph sync . && codegraph affected -p . turbo_flow/include/turbo_flow.h turbo_flow/src/flow_core.c turbo_flow/src/flow_adapter.c turbo_flow/src/flow_run.c turbo_flow/src/flow_runtime.c`

  Run: `git diff --check && rg.exe -n "TODO|FIXME|HACK|fallback" docs/MANAGED_SOURCE_RUNS.md turbo_flow/include/turbo_flow.h turbo_flow/src turbo_flow/tests/test_flow_managed_source.c tests/install_consumer/main.c`

  Expected: Debug/ASan and Release suites pass, install succeeds, affected tests are covered, and no
  placeholder or runtime fallback was introduced.

- [x] **Step 4: Commit documentation and consumer coverage**

  Run: `git add docs tests/install_consumer && git commit -m "docs(flow): define managed Source run ownership"`
