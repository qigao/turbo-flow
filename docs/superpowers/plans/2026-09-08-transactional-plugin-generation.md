# Transactional Plugin Graph Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement #70 so DLL-backed Product providers preflight, materialize, compile, roll back, drain, and release module leases as one bounded Graph generation transaction.

**Architecture:** Extend PluginHost ABI minor 2 with separate transactional adapter/resource factory catalogs; legacy Product providers remain available to explicit embedded consumers but are never accepted by generation assembly. A generation moves one parsed `turbo_flow_t`, retains one immutable PluginHost snapshot, stores plugin-owned opaque owner vtables in a reserved CSTL vector, compiles before publication, and destroys owners in reverse order before releasing the module lease.

**Tech Stack:** C11, TurboFlow Graph/Product/PluginHost, Salts CFlow/CSTL/Core, TinyTest, CMake presets, Windows DLL ABI.

**Spec:** https://github.com/qigao/turbo-flow/issues/70

## Global Constraints

- Gateway-capable providers use only the transactional factory ABI; no legacy callback, static adapter, CMake-selected implementation, protocol, or transport fallback is allowed.
- All cross-DLL structures are pure C, size/versioned, and use opaque handles; plugin-created owners are destroyed by plugin callbacks.
- Preflight callbacks run for every referenced resource and adapter before the parsed Graph is consumed or any materialization callback runs.
- Once materialization begins, `*flow_io` is set to `NULL`; success transfers the Graph to the generation, while later failure destroys the entire Graph and every transferred owner.
- Owner storage is reserved before materialization, bounded by `owner_capacity`, and uses CSTL `vec_t`; capacity exhaustion is reported before side effects.
- Generation/catalog/load/lifecycle mutation is caller-serialized on the Gateway control thread. Data-plane runs obtain explicit generation leases.
- A successful factory call transfers exactly one valid owner descriptor. A failed factory call retains responsibility for partial state and leaves the output zeroed.
- Normal teardown is reverse-order owner quiesce, Graph stop, reverse-order owner drain, reverse-order owner shutdown, Graph destruction, reverse-order owner destruction, snapshot release.
- Any lifecycle callback failure leaves the generation allocated in a retryable state and preserves already completed owner transitions.
- Build, test, and install use the repository's `win-dev-user`, `win-release-user`, and `install-win-dev-user` presets under `VsDevCmd.bat`.

---

### Task 1: Add the transactional provider catalog to PluginHost ABI minor 2

**Files:**
- Create: `turbo_flow/include/turbo_flow_plugin_generation.h`
- Modify: `turbo_flow/include/turbo_flow_plugin.h`
- Modify: `turbo_flow/src/flow_plugin.c`
- Create: `turbo_flow/tests/plugin_generation_fixture.c`
- Modify: `turbo_flow/tests/test_flow_plugin_host.c`
- Create: `turbo_flow/tests/plugin_generation_header_cpp.cpp`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `turbo_flow_plugin_catalog_snapshot_t`, `turbo_flow_resolved_config_t`, `turbo_flow_t`, PluginHost transactional registration.
- Produces: `turbo_flow_plugin_product_owner_v1_t`, transactional adapter/resource factory descriptors, `turbo_flow_plugin_transactional_product_catalog_v1_t`, and `turbo_flow_plugin_catalog_snapshot_transactional_product_catalog()`.

- [x] **Step 1: Write the failing catalog and C++ header tests**

  Add a DLL fixture that declares only `TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER` and `TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE`, calls `add_transactional_adapter_provider` / `add_transactional_resource_provider`, and exposes no additional export. In `test_flow_plugin_host.c`, request capacities 1/1, load the fixture, snapshot it, and assert one factory of each kind is returned. Compile `plugin_generation_header_cpp.cpp` with static assertions for standard-layout public structures.

- [x] **Step 2: Run RED**

  Run `cmake --fresh --preset win-dev-user` and build `test_flow_plugin_host`. Expected: compilation fails because the transactional ABI types and registration callbacks do not exist.

- [x] **Step 3: Implement the catalog ABI**

  Bump `TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR` to `2u`. Append transactional provider capacities to `turbo_flow_plugin_host_config_t`, preserving `TURBO_FLOW_PLUGIN_HOST_CONFIG_V1_0_SIZE` and adding `TURBO_FLOW_PLUGIN_HOST_CONFIG_V1_1_SIZE`. Append typed registration callbacks to `turbo_flow_plugin_registration_v1_t`. Add bounded host/snapshot vectors containing normalized factory descriptors and module indexes. Reject duplicate kinds across legacy and transactional catalogs and reject unknown capability bits.

  The owner descriptor has this exact prefix and required lifecycle callbacks:

  ```c
  typedef struct turbo_flow_plugin_product_owner_v1_s {
    size_t size;
    uint32_t abi_major;
    uint32_t abi_minor;
    uint64_t flags;
    void *ctx;
    int (*quiesce)(void *ctx, uint64_t timeout_ms);
    int (*drain)(void *ctx, uint64_t timeout_ms);
    int (*shutdown)(void *ctx);
    void (*destroy)(void *ctx);
  } turbo_flow_plugin_product_owner_v1_t;
  ```

  Transactional providers contain `kind`, `ctx`, a no-side-effect `preflight` callback, and a `materialize` callback that registers into a parsed Graph and transfers one owner on success. Exactly one of `TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD` and `TURBO_FLOW_PLUGIN_PRODUCT_OWNER_THREAD_SAFE` is required.

- [x] **Step 4: Run GREEN for catalog behavior**

  Reconfigure, build, and run `ctest --preset win-dev-user -R test_flow_plugin_host --output-on-failure`. Expected: fixture registration, duplicate/capacity validation, snapshot catalog view, and C++ compilation pass while existing ABI-minor-1 fixtures still load.

- [ ] **Step 5: Commit the catalog slice**

  Commit the public header, host vectors, fixtures, and focused tests with `feat(plugin): add transactional provider catalog`.

### Task 2: Assemble and compile a bounded generation transaction

**Files:**
- Create: `turbo_flow/src/flow_plugin_generation.c`
- Create: `turbo_flow/tests/test_flow_plugin_generation.c`
- Modify: `turbo_flow/include/turbo_flow_plugin_generation.h`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `turbo_flow_plugin_catalog_snapshot_transactional_product_catalog()`, `turbo_flow_plugin_catalog_snapshot_retain()`, public resolved adapter/channel views, public stage plan views, `turbo_flow_compile()`.
- Produces: `turbo_flow_plugin_generation_create()`, `turbo_flow_plugin_generation_flow()`, `turbo_flow_plugin_generation_state()`, and `turbo_flow_plugin_generation_owner_count()`.

- [ ] **Step 1: Write failing ownership and preflight tests**

  Cover a two-adapter parsed Graph with literal expected outcomes: owner capacity 1 returns `SALTS_ENOSPC`, invokes no callbacks, and leaves `flow_io` non-NULL; provider preflight failure invokes no materialize callback and leaves `flow_io` non-NULL; successful preflight moves `flow_io`, materializes each unique adapter once, compiles the Graph, and returns two owners.

- [ ] **Step 2: Run RED**

  Build and run `test_flow_plugin_generation`. Expected: compilation/link failure for missing generation entry points.

- [ ] **Step 3: Implement preflight, move, materialize, and compile**

  Resolve unique stage references in stage order using `turbo_flow_stage_at()`, `turbo_flow_resolved_config_adapter()`, and `turbo_flow_resolved_config_channel()`. Validate factory catalog structure and provider owner flags. Count required owners with checked addition, reject over-capacity before callbacks, reserve the CSTL vector, retain the snapshot, run all resource preflights followed by all adapter preflights, then move the Graph and materialize resources followed by adapters. Compile only after every owner is recorded.

- [ ] **Step 4: Add failing rollback tests**

  Exercise failure at first and second materialize callbacks and a compile failure after successful materialization. Assert generation output is NULL, `flow_io` is NULL after materialization begins, successful owners receive exactly one reverse-order rollback destruction, and the caller's original structured provider error is preserved.

- [ ] **Step 5: Implement rollback**

  On materialize/owner-validation/compile failure, destroy all transferred owners in reverse order using their DLL-side `destroy`, destroy the moved Graph, release the retained snapshot, destroy the CSTL vector, and free the generation. Never call a legacy Product provider.

- [ ] **Step 6: Run GREEN for assembly**

  Run `ctest --preset win-dev-user -R "test_flow_plugin_(host|generation)" --output-on-failure`. Expected: all capacity, move, compile, and rollback cases pass.

- [ ] **Step 7: Commit the assembly slice**

  Commit with `feat(plugin): assemble transactional graph generations`.

### Task 3: Add retryable lifecycle and explicit run/callback leases

**Files:**
- Modify: `turbo_flow/include/turbo_flow_plugin_generation.h`
- Modify: `turbo_flow/src/flow_plugin_generation.c`
- Modify: `turbo_flow/tests/plugin_generation_fixture.c`
- Modify: `turbo_flow/tests/test_flow_plugin_generation.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: compiled generation and plugin-owned owner lifecycle callbacks.
- Produces: `turbo_flow_plugin_generation_lease_acquire()`, `turbo_flow_plugin_generation_lease_release()`, and `turbo_flow_plugin_generation_destroy()`.

- [ ] **Step 1: Write failing lease and reverse-lifecycle tests**

  Assert an acquired lease makes generation destroy return `SALTS_EBUSY` without invoking lifecycle callbacks. After release, assert reverse-order quiesce, Graph stop, reverse-order drain, reverse-order shutdown, Graph destruction, reverse-order owner destruction, then snapshot release. Verify PluginHost destruction remains `SALTS_EBUSY` until generation destruction succeeds.

- [ ] **Step 2: Run RED**

  Run the focused generation test. Expected: missing lease/destroy APIs or wrong lifecycle order fails.

- [ ] **Step 3: Implement lease and lifecycle state machines**

  Store one state per owner so successful lifecycle callbacks are never repeated. Reject lease counter overflow with `SALTS_ENOSPC` and release underflow with `SALTS_EINVAL`. Destroy returns `SALTS_EBUSY` while leases exist. On quiesce, Graph stop, drain, or shutdown failure, return the exact first status with `$.generation`/owner path context and keep the generation retryable. Free Graph, owners, snapshot, and generation only after every required transition succeeds.

- [ ] **Step 4: Add failing retry tests**

  Build fixture variants whose quiesce, drain, and shutdown callbacks each fail once. Assert the first destroy returns `SALTS_EIO`, the generation remains inspectable, the second destroy resumes at the failed transition, and every prior successful callback and final destroy occurs exactly once.

- [ ] **Step 5: Run GREEN for lifecycle**

  Run `test_flow_plugin_generation` repeatedly 100 times and run `test_flow_plugin_host`. Expected: no duplicate callbacks, invalid transitions, lease underflow, or unload-before-owner behavior.

- [ ] **Step 6: Commit the lifecycle slice**

  Commit with `feat(plugin): govern generation lifecycle and leases`.

### Task 4: Document and install the public generation contract

**Files:**
- Modify: `turbo_flow/ADR_UNIFIED_PLUGIN_HOST.md`
- Modify: `turbo_flow/ADR_GRAPH_PRODUCT_BOUNDARY.md`
- Modify: `README.md`
- Modify: `tests/install_consumer/main.c`
- Modify: `turbo_flow/CMakeLists.txt`

**Interfaces:**
- Consumes: ABI minor 2 headers and PluginHost target.
- Produces: installed C/C++ consumer coverage and migration guidance for #69/#71/#72/#74.

- [ ] **Step 1: Add failing installed-consumer assertions**

  Include `turbo_flow_plugin_generation.h`, instantiate every initializer, bind function pointers to generation create/lease/destroy APIs, require ABI minor 2 and nonzero default transactional capacities, and build the installed consumer before installing the new header/symbols. Expected: install-consumer configure or link fails.

- [ ] **Step 2: Update install/export and architecture documentation**

  Install the new header through `TURBO_FLOW_HEADERS`; document Graph move semantics, preflight purity, owner callback requirements, retry states, explicit leases, reverse shutdown, and the fact that legacy Product assembly is embedded-only and never a Gateway fallback.

- [ ] **Step 3: Run GREEN for installed consumption**

  Run `cmake --build --preset install-win-dev-user` and the install-consumer CTest case under `win-dev-user`. Expected: the installed package supplies the header and all symbols through `TurboFlow::PluginHost`.

- [ ] **Step 4: Commit documentation and packaging**

  Commit with `docs(plugin): define graph generation ownership contract`.

### Task 5: Full verification, review, PR, and issue evidence

**Files:**
- Modify: `docs/superpowers/plans/2026-09-08-transactional-plugin-generation.md` checkboxes only after each command succeeds.
- Modify: GitHub issue #70 acceptance checkboxes and evidence comment after local verification.

**Interfaces:**
- Consumes: completed implementation and test artifacts.
- Produces: merged PR and reproducible evidence for #70.

- [ ] **Step 1: Run source and ABI hygiene checks**

  Run formatting on changed C/C++ files, `git diff --check`, placeholder scans, legacy-fallback scans, and CodeGraph affected analysis. Inspect the DLL fixture export table to confirm only `turbo_flow_plugin_get_api` is exported.

- [ ] **Step 2: Run Debug/ASan verification**

  Under `VsDevCmd.bat`, run fresh configure, full build, focused generation/host/Product tests, 100 focused repetitions, and full `ctest --preset win-dev-user --output-on-failure`.

- [ ] **Step 3: Run Release and install verification**

  Under `VsDevCmd.bat`, run `cmake --fresh --preset win-release-user`, full Release build/test, `cmake --build --preset install-win-dev-user`, and installed C/C++ consumers.

- [ ] **Step 4: Review the complete diff against #70**

  Check every acceptance item against a named test/output, verify existing embedded `turbo_flow_product_assemble_graph()` tests are unchanged, and confirm no code path invokes it from the new generation API.

- [ ] **Step 5: Commit, push, open PR, and merge**

  Push `feat/issue-70-product-generation`, open a PR linked to #70, wait for required checks, merge only from a clean verified head, and record the merge commit plus commands in #70 and parent #63.
