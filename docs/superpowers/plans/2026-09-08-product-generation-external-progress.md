# Product Generation External Progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement #78 so a Gateway can synchronously drive DLL-owned external-poll Product owners without linking a concrete I/O adapter or creating hidden workers.

**Architecture:** Extend the size-versioned Product owner descriptor with an optional `poll` callback guarded by owner and root capability flags. ABI minor 3 adds an explicit extension boundary plus a capacity-aware publish helper, so old hosts reject progress plugins before materialization and old owner buffers are never overrun. A Graph generation owns the only poll cursor, invokes each eligible owner once per successful call, gives the total blocking budget to one rotating owner and zero timeout to the remainder, and retains the catalog snapshot throughout. Lifecycle-only v1.0 prefixes remain valid. Rollback destroys the moved Graph before plugin owners so registry shutdown/detach completes before same-DLL storage destruction.

**Tech Stack:** C11, TurboFlow PluginHost/Product/Graph generation, Salts CSTL, TinyTest, CMake presets, Windows DLL ABI.

**Spec:** https://github.com/qigao/turbo-flow/issues/78

## Global Constraints

- Poll is a caller-serialized Gateway control-thread operation; no callback may outlive the call.
- `timeout_ms` is one total budget: only the rotating first external-poll owner receives it, all remaining owners receive zero.
- Owners without the external-poll flag remain lifecycle-only; flagged owners must provide the appended callback and a descriptor large enough to contain it.
- A plugin that may return a flagged owner declares the ABI-minor-3 root capability and publishes through the caller-capacity-aware helper; direct cross-DLL structure assignment is forbidden.
- Poll is valid only for an active, started generation and fails after quiesce begins.
- Provider errors identify the exact `$.adapters.<name>.owner.poll` or `$.channels.<name>.owner.poll` path.
- No static/direct adapter call, hidden thread, transport cast, scheduler substitution, C fallback, or CMake fallback is allowed.
- Verification uses `win-dev-user`, `win-release-user`, and `install-win-dev-user` under `VsDevCmd.bat`.

---

### Task 1: Specify and test the external-poll ABI

**Files:**
- Modify: `turbo_flow/include/turbo_flow_plugin_generation.h`
- Modify: `turbo_flow/tests/plugin_generation_header_cpp.cpp`
- Modify: `turbo_flow/tests/plugin_generation_fixture.c`
- Modify: `turbo_flow/tests/test_flow_plugin_generation.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

- [x] **Step 1: Add RED header and fixture tests**

  Bind the new callback, bounded owner publish helper, and `turbo_flow_plugin_generation_poll()` from C++; freeze the v1.0 layout; compile one real prefix-only fixture and external-poll fixtures with 1/N eligible owners. Runtime tests verify rotating order and exact timeout values.

- [x] **Step 2: Run RED**

  Build `test_flow_plugin_generation` and the C++ header check. Expected: compilation fails because the flag, callback field, v1.0 size constant, and generation poll API do not exist.

- [x] **Step 3: Add the additive owner descriptor surface**

  Append an explicit aligned extension guard and `poll` after the complete v1.0 object, define the frozen prefix size, add owner/root external-poll capabilities, bump ABI minor to 3, add capacity-aware publishing, and declare the generation poll entry point. Pre-v1.3 hosts reject the unknown root capability before materialization.

- [x] **Step 4: Commit the ABI/test slice**

  Commit with `test(plugin): specify external generation progress` after the intended RED evidence has been captured.

### Task 2: Implement bounded fair generation polling

**Files:**
- Modify: `turbo_flow/src/flow_plugin_generation.c`
- Modify: `turbo_flow/tests/plugin_generation_fixture.c`
- Modify: `turbo_flow/tests/test_flow_plugin_generation.c`

- [x] **Step 1: Validate descriptor flag/size/callback invariants**

  Accept lifecycle-only v1.0 prefixes. Reject unknown flags, a callback without its flag, a flagged prefix too small for `poll`, or a flagged descriptor with a NULL callback before the owner enters the generation vector.

- [x] **Step 2: Implement one bounded poll round**

  Store `poll_cursor` in the generation. Require a started active Graph. Starting at the cursor, visit every external-poll owner once, pass the requested timeout only to the first eligible owner and zero to all others, advance the cursor to the owner after that first eligible owner, and return the first exact callback failure with owner path context.

- [x] **Step 3: Add boundary tests and run GREEN**

  Cover no owners, lifecycle-only owners, 1/N owners, fairness, timeout 0/nonzero, callback error, invalid descriptor combinations, and polling after stop/quiesce. Run the focused test and repeat it 100 times.

- [x] **Step 4: Commit the implementation slice**

  Commit with `feat(plugin): drive external Product owner progress`.

### Task 3: Correct rollback ordering for registered adapter owners

**Files:**
- Modify: `turbo_flow/src/flow_plugin_generation.c`
- Modify: `turbo_flow/tests/plugin_generation_fixture.c`
- Modify: `turbo_flow/tests/test_flow_plugin_generation.c`

- [x] **Step 1: Add a RED rollback-order assertion**

  Make the fixture owner record whether its Graph registry shutdown has happened. On compile/materialization rollback, assert Graph destruction/shutdown precedes owner `destroy`; this models CNet Sink's requirement that Flow detaches its registration before `*_sink_destroy()` succeeds.

- [x] **Step 2: Reverse the rollback boundary**

  Destroy the moved Graph first, then invoke transferred plugin owner destroy callbacks in reverse creation order, then release the snapshot and vectors. Preserve the original provider/compile error.

- [x] **Step 3: Run focused regression and commit**

  Run PluginGeneration, PluginHost, Product, managed Source, and all six CNet adapter tests. Commit with `fix(plugin): detach graph before rollback owner destroy`.

### Task 4: Document, install, verify, and merge

**Files:**
- Modify: `turbo_flow/ADR_UNIFIED_PLUGIN_HOST.md`
- Modify: `turbo_flow/ADR_GRAPH_PRODUCT_BOUNDARY.md`
- Modify: `README.md`
- Modify: `tests/install_consumer/main.c`
- Modify: `docs/superpowers/plans/2026-09-08-product-generation-external-progress.md`

- [x] **Step 1: Update installed consumer and docs**

  Document the external-poll capability, total-budget/fairness rule, active-state requirement, module lease, and no-hidden-worker constraint. Bind the installed C and C++ declarations through `TurboFlow::PluginHost`.

- [x] **Step 2: Run source and ABI hygiene**

  Format changed C/C++ files, run `git diff --check`, placeholder/fallback scans, CodeGraph affected analysis, and fixture export inspection.

- [x] **Step 3: Run Debug/ASan, Release, and install verification**

  Run focused repeats and full suites under `win-dev-user` and `win-release-user`, then `install-win-dev-user` and installed C/C++ consumers.

- [x] **Step 4: Review, push, merge, and record evidence**

  Review every #78 acceptance item against named evidence, open a PR linked to #78, merge a clean verified head, update #78 and #63, then return to #69 from updated `master`.
