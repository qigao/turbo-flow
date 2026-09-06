# Immutable CMeta/CFlow Plan Implementation Plan

> **For Codex:** Execute this plan task-by-task with tests first and review each checkpoint before proceeding.

**Goal:** Compile TurboFlow DSL into one sealed, verified plan with O(1) runtime lookup and explicit CMeta/CFlow lowering metadata, without changing existing DSL or runtime behavior.

**Architecture:** Replace the four independent runtime vectors with an owning `flow_compiled_plan_t`. Build it transactionally from the parsed graph, seal it after structural and semantic verification, and expose only const runtime accessors. Keep mutable pool sizing in a separate stage-indexed runtime overlay. Represent type identity with CMeta stable atoms and effect/lowering boundaries with CMeta/CFlow semantics; never fabricate a CFlow callable for the legacy message callback ABI.

**Tech Stack:** C11, Salts CMeta/CFlow/CSTL/Core, TinyTest, CMake Presets, CodeGraph.

---

### Task 1: Establish the compiled-plan ownership boundary

**Files:**
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/src/flow_plan.c`
- Modify: `turbo_flow/tests/test_turbo_flow.c`

1. Add a failing compile-plan test that expects one plan carrier, a sealed flag, exact stage/edge counts, and direct stage-index maps.
2. Run `test_turbo_flow --filter "compile validation"` and confirm the new assertion fails.
3. Define `flow_compiled_plan_t`, lifecycle helpers, and invalid-index sentinel. Move nodes, CSR edges, segments, and executors into that carrier.
4. Build into a temporary initialized plan; validate indices/counts; move it into the flow only on complete success.
5. Update internal tests and the reachability benchmark to use the plan carrier.
6. Re-run the filtered tests and confirm they pass.

### Task 2: Make executor and segment lookup O(1)

**Files:**
- Modify: `turbo_flow/src/flow_plan.c`
- Modify: `turbo_flow/src/flow_dispatch.c`
- Modify: `turbo_flow/src/flow_disruptor.c`
- Modify: `turbo_flow/src/flow_executor.c`
- Modify: `turbo_flow/src/flow_pool.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/tests/test_turbo_flow.c`
- Modify: `turbo_flow/benchmarks/bench_turbo_flow.c`

1. Add failing tests for source/port invalid executor indices, executable-stage exact executor indices, and worker-pool exact segment indices.
2. Run the focused plan tests and confirm RED.
3. Populate `executor_by_stage` and `worker_segment_by_stage` while building the plan. Reject duplicate or out-of-range mappings before sealing.
4. Replace scans with checked array access. Pass the already-resolved executor from validation into dispatch so one stage dispatch does one lookup.
5. Add a lookup benchmark whose operation count matches the actual number of indexed lookups.
6. Run focused tests and benchmark smoke execution.

### Task 3: Add semantic descriptors, effects, and lowering barriers

**Files:**
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_plan.c`
- Add: `turbo_flow/tests/flow_plan_type_probe.c`
- Modify: `turbo_flow/tests/test_turbo_flow.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

1. Add failing tests that compare independently constructed descriptor identities from two translation units and inspect stage barrier/effect classification.
2. Run the focused plan test and confirm RED.
3. Link Graph and compatibility targets to `Salts::CFlow`; include CMeta/CFlow only behind the internal plan boundary.
4. Create stable semantic atoms for message, operation, input, and output descriptors. Compare with `cmeta_type_equal`, never pointer equality.
5. Map operation/runtime contracts to `cmeta_effects` and named barrier flags. Partition connected candidate stages without allocating an O(V^2) cache.
6. Verify the plan rejects a required-CFlow lowering request with `SALTS_ENOTSUP` when any callback is untyped or any barrier is present; do not continue through the native path.
7. Re-run focused tests.

### Task 4: Separate mutable pool state from the plan

**Files:**
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/src/flow_plan.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/src/flow_executor.c`
- Modify: `turbo_flow/src/flow_disruptor.c`
- Modify: `turbo_flow/tests/test_turbo_flow.c`

1. Add a failing resize test that snapshots stage, executor, worker segment, semantic metadata, and seal generation before resize, then requires byte-for-byte equality afterward.
2. Run only the pool-resize group and confirm RED against current mutation behavior.
3. Add a stage-indexed runtime capacity overlay initialized from the sealed plan.
4. Make start/rebuild consume the overlay and make resize mutate/rollback only that overlay.
5. Re-run resize, pause/timeout, stop, and concurrent-publish tests.

### Task 5: Differential behavior and complexity regression

**Files:**
- Modify: `turbo_flow/tests/test_turbo_flow.c`
- Modify: `turbo_flow/benchmarks/bench_turbo_flow.c`

1. Add a test-only legacy oracle derived from parsed `stages` and `edges`.
2. Compare stage indices, incoming/outgoing topology, route name/kind/predicate presence, effects, and source locations for linear, diamond, conditional-route, emitting, worker, retry, and window plans.
3. Keep existing negative compile cases as the error-code/location oracle and add a transactional-build failure assertion that no partial plan is published.
4. Add or update linear and diamond single-message benchmarks so reported operations correspond to V+E work and indexed plan lookup.
5. Run `test_turbo_flow`, then all graph/core-labelled tests.

### Task 6: Documentation and final verification

**Files:**
- Modify: `PRIMITIVE_GRAPH_ARCHITECTURE.md`
- Modify: `turbo_flow/parser/GRAMMARS.md`
- Modify: `docs/ADR_IMMUTABLE_CFLOW_PLAN.md`

1. Document plan ownership, immutability, stage/edge indexing, CMeta identity, lowerable semantics, every barrier, unsupported combinations, resize ownership, and #4 handoff.
2. Run `codegraph sync .` and inspect affected symbols/tests.
3. Run fresh Release configure, full build, and `ctest --preset win-release-user --output-on-failure` from the VS developer environment.
4. Inspect `git diff --check`, review the complete diff by severity, and confirm no unrelated or generated artifacts are tracked.
5. Commit the implementation, update issue #3 with evidence, open a PR, obtain review, merge only after checks pass, and update parent #2.
