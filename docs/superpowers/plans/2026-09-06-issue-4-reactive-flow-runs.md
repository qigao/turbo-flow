# Reactive Flow Runs Implementation Plan

> **For Codex:** Execute this plan task-by-task with tests first and review each checkpoint before proceeding.

**Goal:** Add bounded, resumable CFlow Reactive runs with exact demand, WAIT, cancellation, deadline, admission, and shutdown semantics while preserving `turbo_flow_publish()` as a synchronous facade with no native fallback.

**Architecture:** Each opaque run owns one sealed identity CFlow Graph, one Subscription, and one terminal-result state machine. A flow owns a bounded default Scheduler and the registry of live Subscriptions. CMeta managed-value traits protect message ownership at the const Subscriber boundary. All terminal sources converge on one first-error-wins transition, and stop cancels runs before destroying Scheduler or plan state.

**Tech Stack:** C11, Salts CMeta/CFlow/CSTL/Executor, TinyTest, CMake Presets, CodeGraph.

---

### Task 1: Freeze the public run contract and dependency boundary

**Files:**
- Modify: `turbo_flow/include/turbo_flow/turbo_flow.h`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/CMakeLists.txt`
- Add: `turbo_flow/tests/test_flow_run.c`

1. Add a compile-time test for opaque handles, size/version initializers, state/result fields, and the Publisher/Scheduler types; confirm it fails to compile.
2. Add the smallest additive declarations for open, request, wait, cancel, snapshot, and close.
3. Export `Salts::CFlow` through the public target and verify build-tree plus installed-package consumers compile.
4. Document Publisher transfer, borrowed Scheduler lifetime, timeout/deadline differences, and return codes at the declarations.

### Task 2: Make the message envelope a managed CMeta value

**Files:**
- Modify: `turbo_flow/src/flow_plan_semantics.c`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/tests/test_flow_run.c`

1. Add failing lifecycle-counter tests for CFlow copy, move, destruction, and an allocation-failure path.
2. Define COPY, MOVE, and DESTROY traits using the existing message lifecycle functions; do not byte-copy owned payloads.
3. Expose the descriptor only as a const internal accessor and verify stable CMeta identity remains equal across translation units.
4. Run the lifecycle tests under the existing sanitizer-capable preset when available.

### Task 3: Build the bounded run and Subscription core

**Files:**
- Modify: `turbo_flow/src/flow_internal.h`
- Add: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/test_flow_run.c`

1. Add failing tests for VALUE, VALUE_AND_DONE, DONE, ERROR, explicit demand, zero demand, and first-error-wins.
2. Add the sealed typed identity Graph and a Subscriber bridge that clones borrowed messages before invoking the immutable native plan.
3. Implement the run mutex/condition/refcount, bounded active-run registry, and transactional open path.
4. Map `cflow_subscription_request_result()` statuses exactly; prove a FULL result retains demand for a later wake/request cycle.
5. Detach and release the active-publication reference exactly once at terminal settlement.

### Task 4: Implement WAIT, cancellation, deadlines, and stop ordering

**Files:**
- Modify: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/tests/test_flow_run.c`

1. Add a Publisher that returns WAIT and records bind, wake, and cancel events; confirm tests fail before implementation.
2. Require a valid wake to resume WAIT and prove cancellation unregisters/cancels the waitable.
3. Add deadline tasks only when the selected Scheduler advertises delayed capability; reject unsupported or failed timer admission before Publisher ownership transfers.
4. Race completion, graph failure, user cancel, deadline, and stop, asserting one terminal result and one resource release.
5. Close ingress, snapshot retained run references under the flow mutex, cancel outside the mutex, wait for publications, then destroy the owned Scheduler and plan.

### Task 5: Route the synchronous facade through runs

**Files:**
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/src/flow_run.c`
- Modify: `turbo_flow/tests/test_turbo_flow.c`
- Modify: `turbo_flow/tests/test_flow_run.c`

1. Add differential tests for output, message mutation, retries, settlement, observer events, and every existing synchronous error return.
2. Implement a one-value managed Publisher and inline-Scheduler run path.
3. Remove the old direct execution call from `turbo_flow_publish_ex()`; do not retain a capability or error fallback.
4. Re-run publish, pause/resume, stop, concurrent publish, and adapter suites.

### Task 6: Measure performance and close compatibility gaps

**Files:**
- Modify: `turbo_flow/benchmarks/bench_turbo_flow.c`
- Modify: `turbo_flow/tests/test_package_consumer.c`
- Modify: `README.md`

1. Benchmark pre-run synchronous-equivalent work, the public synchronous facade, native Publisher runs, WAIT/wake, and full admission using fixed graph/value counts.
2. Report operations, message clone/destruction counts, allocations where observable, and P50/P95/P99 latency; fail performance claims if measurements are unavailable.
3. Verify C and C++ header consumers, build-tree targets, and installed-package targets.
4. Run focused tests, adjacent regression groups, full CTest, and the benchmark smoke command from a fresh configured tree.
5. Review ownership, exact terminal/error semantics, public compatibility, and issue acceptance criteria before opening the PR.
