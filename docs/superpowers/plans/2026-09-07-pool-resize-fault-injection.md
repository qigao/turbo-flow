# Pool Resize Fault Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this
> plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Add deterministic internal fault injection that proves both pool-resize rollback outcomes
without changing TurboFlow's public API or normal runtime behavior.

**Architecture:** Keep the sealed compiled plan as immutable input and the per-stage runtime config
as the only mutable parallelism fact. Add a private per-flow rebuild-attempt callback in
flow_internal.h; flow_rebuild_pool_resources invokes it after old resources are stopped and pool
records are cleared, but before any replacement resource is constructed. Tests provide a bounded
two-result script and inspect real lifecycle state.

**Tech Stack:** C11, TurboFlow runtime, Salts thread/coroutine/Disruptor owners, TinyTest, CMake
Presets.

**Spec:** https://github.com/qigao/turbo-flow/issues/15

## Global Constraints

- No public API, installed header, CMake option, compile-time branch, or fallback path is added.
- The callback and its context are borrowed, per-flow, and mutated only by the control-thread test
  while no resize is running.
- Pool rebuild remains a lifecycle-exclusive control operation after admission is RESIZING and
  active publishes reach zero.
- A failed resize followed by successful rollback restores old parallelism, restores prior
  admission, advances generation once, and returns the first rebuild error.
- Two failed rebuilds leave runtime state FAILED, admission CLOSED, and all adapter/data-plane/
  executor resources stopped; generation does not advance.
- The sealed compiled plan's target node, executor, data segment, semantic row, and index mappings
  remain byte-for-byte unchanged in both paths.

---

### Task 1: Specify rollback behavior with failing tests

**Files:**
- Modify: turbo_flow/tests/test_turbo_flow.c

**Interfaces:**
- Consumes: turbo_flow_resize_pool(), turbo_flow_pool_resource_status_at(), the private
  flow_pool_rebuild_fault_t field, and existing adapter lifecycle probes.
- Produces: two deterministic regression tests for one-failure rollback and two-failure shutdown.

- [x] **Step 1: Add a bounded fault script**

~~~c
typedef struct pool_rebuild_fault_script_s {
  int results[2];
  size_t count;
  size_t calls;
} pool_rebuild_fault_script_t;

static int pool_rebuild_fault_next(void *ctx, size_t attempt) {
  pool_rebuild_fault_script_t *script = (pool_rebuild_fault_script_t *)ctx;
  check_equal(attempt, script->calls + 1u);
  return script->calls < script->count ? script->results[script->calls++] : SALTS_OK;
}
~~~

- [x] **Step 2: Add the rollback-success test**

Create a worker-pool flow, snapshot the stage/executor/segment plan bytes, inject
{SALTS_ENOMEM, SALTS_OK}, resize from 2 to 3, and assert:

~~~c
check_equal(turbo_flow_resize_pool(flow, &command), SALTS_ENOMEM);
check_equal(script.calls, 2u);
check_equal(after.snapshot.parallelism, 2u);
check_equal(after.generation, before.generation + 1u);
check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
check_equal(flow->compiled_plan.sealed, 1);
~~~

Also compare the saved sealed-plan structures with memcmp().

- [x] **Step 3: Add the rollback-failure test**

Register a source adapter with start/stop probes, create a downstream worker pool, inject
{SALTS_ENOMEM, SALTS_EIO}, then assert:

~~~c
check_equal(turbo_flow_resize_pool(flow, &command), SALTS_EIO);
check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_FAILED);
check_equal(flow->admission_state, FLOW_ADMISSION_CLOSED);
check_equal(adapter.stop_count, 1);
check_equal(vec_size(&flow->active_adapters), 0u);
check_equal(vec_size(&flow->worker_pool_adapters), 0u);
check_equal(vec_size(&flow->threadpool_adapters), 0u);
check_equal(vec_size(&flow->coro_adapters), 0u);
check_equal(flow->runtime_generation, generation_before);
check_equal(flow->compiled_plan.sealed, 1);
~~~

Also compare the saved sealed-plan structures with memcmp().

- [x] **Step 4: Run RED verification**

Run:

~~~powershell
cmake --build --preset win-dev-user --target test_turbo_flow
~~~

Expected: compilation fails because flow_pool_rebuild_fault_t and the private per-flow hook do not
exist yet. This proves production support is absent before implementation.

### Task 2: Add the minimal private rebuild-attempt seam

**Files:**
- Modify: turbo_flow/src/flow_internal.h
- Modify: turbo_flow/src/flow_runtime.c

**Interfaces:**
- Consumes: the lifecycle-exclusive flow_rebuild_pool_resources() path.
- Produces: flow_pool_rebuild_fault_t with a borrowed callback/context and monotonic attempt count.

- [x] **Step 1: Define the private hook**

~~~c
typedef int (*flow_pool_rebuild_fault_fn)(void *ctx, size_t attempt);

typedef struct flow_pool_rebuild_fault_s {
  flow_pool_rebuild_fault_fn before_create;
  void *ctx;
  size_t attempts;
} flow_pool_rebuild_fault_t;
~~~

Store one flow_pool_rebuild_fault_t in struct turbo_flow_s. calloc initialization leaves it
disabled in every normal runtime.

- [x] **Step 2: Invoke it at the construction boundary**

After stopping data-plane/executor owners and clearing pool_records:

~~~c
if (flow->pool_rebuild_fault.before_create) {
  int rc = flow->pool_rebuild_fault.before_create(
      flow->pool_rebuild_fault.ctx, ++flow->pool_rebuild_fault.attempts);
  if (rc != SALTS_OK) {
    return flow_set_error_keep_state(flow, rc, 0, 0, "pool resource rebuild failed");
  }
}
~~~

Do not retry, translate, or replace the injected status.

- [x] **Step 3: Run GREEN verification**

Run:

~~~powershell
cmake --build --preset win-dev-user --target test_turbo_flow
ctest --preset win-dev-user -R ^test_turbo_flow$ --output-on-failure
~~~

Expected: test_turbo_flow passes, including both new rollback cases.

### Task 3: Verify, document evidence, and publish for review

**Files:**
- Modify: docs/superpowers/plans/2026-09-07-pool-resize-fault-injection.md

**Interfaces:**
- Consumes: completed implementation and regression tests.
- Produces: reproducible verification evidence and an issue-closing pull request.

- [x] **Step 1: Run Debug and Release validation**

~~~powershell
cmake --fresh --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user --output-on-failure
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
cmake --build --preset install-win-dev-user
~~~

- [x] **Step 2: Audit the affected surface**

Run codegraph sync/affected, git diff --check, verify no public header or CMake change, and confirm
the worktree contains only the plan, private runtime/header changes, and test changes.

Verification evidence (2026-09-07):

- Debug/ASan fresh configure and full build completed with `win-dev-user`.
- Debug/ASan full suite: 26/26 passed; `test_turbo_flow` repeated 25 times without failure.
- Debug install completed with `install-win-dev-user`; installed-consumer test passed.
- Release fresh configure, full build, and full suite completed with 26/26 passing.
- CodeGraph identified `turbo_flow/tests/test_turbo_flow.c` as the affected test surface.
- `git diff --check` passed, and no public header or CMake file contains the private hook.

- [ ] **Step 3: Commit, push, and create the PR**

Commit the behavior/test change, record verification in this plan, push
test/issue-15-resize-fault-injection, create a PR against master with Closes #15, update issue #15
with the evidence, and leave the PR unmerged.
