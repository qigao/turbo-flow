# Deterministic Schedule Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one-shot and bounded interval schedules complete exactly once under Debug/ASan without sleep-based retries or fallback behavior.

**Architecture:** Keep the schedule adapter as the sole owner of timer and per-run state. Synchronize flow readiness through the existing runtime mutex, and make async publication a single-slot atomic reservation whose terminal-state recheck closes the completion/callback race.

**Tech Stack:** C11 atomics, Salts mutex/condition/timer/threadpool APIs, TurboFlow async ingress, TinyTest, CMake Presets, CTest, MSVC Debug/ASan.

**Spec:** GitHub issue #35, `test(schedule): make one-shot timer completion deterministic under Debug/ASan`.

## Global Constraints

- Preserve current schedule configuration, payload, snapshot, start/stop, and restart behavior.
- Fail fast on timer creation, thread creation, publish admission, and invalid configuration; add no fallback.
- Do not inflate sleeps or add test retries to production/test code.
- A bounded schedule may have at most one accepted async publication in flight and may never exceed `repeat_limit`.
- `turbo_flow_stop()` must prevent new schedule publication and wait for every already accepted publication before returning.
- Validate Debug/ASan with 100 schedule-test repetitions and validate Release plus the full adjacent suite.

---

### Task 1: Replace polling waits with condition-based schedule assertions

**Files:**
- Modify: `schedule/tests/test_turbo_flow_schedule.c`

**Interfaces:**
- Consumes: `salts_mutex_t`, `salts_cond_t`, `salts_hrtime()`, `salts_cond_timedwait()` and the existing `schedule_capture_stage()` callback.
- Produces: test-local `schedule_capture_init()`, `schedule_capture_destroy()`, `schedule_capture_called()`, and `schedule_wait_called()` helpers.

- [x] **Step 1: Make callback completion an explicit condition**

Add a mutex and condition to `schedule_capture_t`. Initialize and destroy them for each test fixture. After `schedule_capture_stage()` increments `called`, lock the wait mutex, broadcast the condition, and unlock it.

```c
static int schedule_capture_init(schedule_capture_t *capture) {
  memset(capture, 0, sizeof(*capture));
  salts_mutex_init(&capture->wait_mutex);
  salts_cond_init(&capture->called_cond);
  if (!capture->wait_mutex || !capture->called_cond) {
    salts_cond_destroy(&capture->called_cond);
    salts_mutex_destroy(&capture->wait_mutex);
    return SALTS_ENOMEM;
  }
  return SALTS_OK;
}
```

- [x] **Step 2: Replace the 5 ms polling loop**

Implement a bounded condition wait using a named one-second nanosecond timeout. Recheck the atomic predicate while holding the wait mutex so a notification cannot be lost between the predicate check and wait.

```c
static int schedule_wait_called(schedule_capture_t *capture, int expected) {
  const uint64_t started_at = salts_hrtime();
  salts_mutex_lock(&capture->wait_mutex);
  while (atomic_load_explicit(&capture->called, memory_order_acquire) < expected) {
    const uint64_t elapsed = salts_hrtime() - started_at;
    if (elapsed >= SCHEDULE_TEST_WAIT_TIMEOUT_NS ||
        salts_cond_timedwait(&capture->called_cond, &capture->wait_mutex,
                             SCHEDULE_TEST_WAIT_TIMEOUT_NS - elapsed) != 0) {
      break;
    }
  }
  salts_mutex_unlock(&capture->wait_mutex);
  return atomic_load_explicit(&capture->called, memory_order_acquire) >= expected;
}
```

- [x] **Step 3: Remove post-stop sleeps and preserve exact-count checks**

For one-shot and bounded interval cases, capture the count immediately before `turbo_flow_stop()`, stop the flow, and assert the count is unchanged immediately after stop. Keep the coalescing case's callback delay because it is the tested workload, not a completion wait.

- [x] **Step 4: Build and prove the existing race is RED**

Run from a VS x64 developer environment:

```powershell
cmake --build --preset win-dev-user --target test_turbo_flow_schedule
ctest --preset win-dev-user -R "^test_turbo_flow_schedule$" --repeat until-fail:100 --output-on-failure
```

Expected before the production fix: at least one repetition reports either `called == 4` for the bounded interval or no one-shot completion. The wait itself must not fail because of a polling interval.

- [x] **Step 5: Commit the test harness change**

```powershell
git add schedule/tests/test_turbo_flow_schedule.c docs/superpowers/plans/2026-09-07-schedule-completion-race.md
git commit -m "test(schedule): synchronize completion assertions"
```

### Task 2: Close readiness and completion admission races

**Files:**
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `schedule/src/flow_schedule.c`
- Test: `schedule/tests/test_turbo_flow_schedule.c`

**Interfaces:**
- Consumes: `turbo_flow_state(const turbo_flow_t *)`, `turbo_flow_publish_async()`, `flow_schedule_publish_complete()`, and `async_inflight`.
- Produces: synchronized `turbo_flow_state()` reads and private `flow_schedule_try_reserve_publish(turbo_flow_schedule_t *)` single-slot admission.

- [ ] **Step 1: Synchronize runtime state observation**

Lock `runtime_mutex` around the `flow->state` read in `turbo_flow_state()`. This makes the schedule bootstrap observe `TURBO_FLOW_STATE_STARTED` only after `turbo_flow_start()` commits both state and open admission under the same mutex.

```c
turbo_flow_state_t turbo_flow_state(const turbo_flow_t *flow) {
  turbo_flow_state_t state;
  if (!flow || !flow->runtime_sync_initialized) return TURBO_FLOW_STATE_FAILED;
  salts_mutex_lock((salts_mutex_t *)&flow->runtime_mutex);
  state = flow->state;
  salts_mutex_unlock((salts_mutex_t *)&flow->runtime_mutex);
  return state;
}
```

- [ ] **Step 2: Reserve the async publication slot atomically**

Add a private helper that first checks `started` and `completed`, changes `async_inflight` from zero to one with compare-exchange, and then rechecks both terminal flags. If either flag changed while acquiring the slot, release it and refuse publication.

```c
static int flow_schedule_try_reserve_publish(turbo_flow_schedule_t *schedule) {
  unsigned expected = 0u;
  if (!atomic_load_explicit(&schedule->started, memory_order_acquire) ||
      atomic_load_explicit(&schedule->completed, memory_order_acquire))
    return 0;
  if (!atomic_compare_exchange_strong_explicit(&schedule->async_inflight, &expected, 1u,
                                                memory_order_acq_rel,
                                                memory_order_acquire))
    return 0;
  if (!atomic_load_explicit(&schedule->started, memory_order_acquire) ||
      atomic_load_explicit(&schedule->completed, memory_order_acquire)) {
    atomic_store_explicit(&schedule->async_inflight, 0u, memory_order_release);
    return 0;
  }
  return 1;
}
```

- [ ] **Step 3: Transfer slot ownership through async completion**

Call the reservation helper from `flow_schedule_timer_callback()` before building/submitting the message. Remove the separate inflight increment from `flow_schedule_publish_async()`. On submit failure and in `flow_schedule_publish_complete()`, release the owned slot with a release store after terminal state and counters have been committed.

- [ ] **Step 4: Build and run the focused test GREEN**

```powershell
cmake --build --preset win-dev-user --target test_turbo_flow_schedule
ctest --preset win-dev-user -R "^test_turbo_flow_schedule$" --output-on-failure
```

Expected: all schedule cases pass and every exact-count assertion remains unchanged.

- [ ] **Step 5: Commit the runtime fix**

```powershell
git add turbo_flow/src/flow_core.c schedule/src/flow_schedule.c
git commit -m "fix(schedule): serialize async completion admission"
```

### Task 3: Verify Debug/ASan, Release, and adjacent regressions

**Files:**
- Verify only; no source changes expected.

**Interfaces:**
- Consumes: `win-dev-user`, `win-release-user`, and the CTest schedule target.
- Produces: reproducible verification evidence for issue #35 and the pull request.

- [ ] **Step 1: Stress Debug/ASan 100 times**

```powershell
ctest --preset win-dev-user -R "^test_turbo_flow_schedule$" --repeat until-fail:100 --output-on-failure
```

Expected: 100 consecutive passes with no ASan diagnostics.

- [ ] **Step 2: Run the full Debug/ASan suite**

```powershell
ctest --preset win-dev-user --output-on-failure
```

Expected: all configured tests pass.

- [ ] **Step 3: Configure, build, and run Release**

```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user --target test_turbo_flow_schedule
ctest --preset win-release-user -R "^test_turbo_flow_schedule$" --repeat until-fail:100 --output-on-failure
ctest --preset win-release-user --output-on-failure
```

Expected: both the schedule stress run and full Release suite pass.

- [ ] **Step 4: Inspect the final diff and issue requirements**

```powershell
git diff master...HEAD --check
git diff --stat master...HEAD
git status --short
```

Expected: no whitespace errors, only planned files changed, and a clean worktree.
