# Provider-Neutral Outbox Source Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the provider-neutral, bounded Source and settlement core tracked by GitHub issue #50 to the optional `TurboFlow::TurboDbAdapter` component.

**Architecture:** An owner-thread `turbo_flow_turbodb_outbox_source_t` pulls one borrowed receipt view at a time through a versioned provider vtable, copies accepted identity/payload bytes into a message-owned `mem_buffer_t`, and opens one independent `turbo_flow_run_t` per receipt. Owner-driven `poll()` observes each run's terminal result and maps it to exactly one acknowledge, requeue, or dead-letter-then-ack sequence; provider callbacks never run on Graph workers.

**Tech Stack:** C11, Salts CFlow/CSTL, TurboFlow Graph runs, TinyTest, CMake Presets.

**Spec:** GitHub issue #50; parent GitHub issue #13; upstream Redis binding dependency `qigao/turbodb#21`.

## Global Constraints

- The API belongs only to the optional `TurboFlow::TurboDbAdapter` component; `TurboFlow::Graph` gains no TurboDB or Redis dependency.
- Provider operations are non-blocking and owner-thread-affine. `WAIT` uses a CFlow waitable whose waker only marks readiness.
- Demand, fetch count, in-flight messages, in-flight retained bytes, identity bytes, payload bytes, and delivery attempts are all hard bounded.
- A successful Graph run acknowledges its matching token once. Retryable failure and cancellation requeue without acknowledgement.
- Permanent failure runs an explicitly configured dead-letter callback and acknowledges only after that callback succeeds.
- Stop admits no new fetches and cancels/requeues every active claim before destroy can succeed.
- No raw Redis command, reply parsing, PubSub, materialization, fallback, conditional skip, or hidden worker is permitted.

---

### Task 1: Versioned Receipt and Source Contract

**Files:**
- Modify: `io/turbodb/include/turbo_flow_turbodb.h`
- Modify: `io/turbodb/tests/turbodb_adapter_header_cpp.cpp`
- Create: `io/turbodb/tests/test_turbodb_outbox_source.c`
- Modify: `io/turbodb/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `turbo_flow_t`, `turbo_flow_run_t`, `cflow_waitable`, `vstr`.
- Produces: `turbo_flow_turbodb_outbox_source_config_default`, `turbo_flow_turbodb_outbox_source_open`, `request`, `poll`, `snapshot`, `stop`, `destroy`, and message-context accessors.

- [ ] **Step 1: Write failing ABI/default/config-validation tests**

  Add tests that include the header from C and C++, inspect every default bound, reject NULL/short/version-mismatched configs and missing callbacks, and verify failed open leaves the output NULL and performs no provider fetch.

- [ ] **Step 2: Run the focused target and verify RED**

  Run `cmake --build --preset win-dev-user --target test_turbodb_adapter` in the VS development environment. Expected: compilation fails because the outbox types/functions do not exist.

- [ ] **Step 3: Add the public declarations and minimal validation**

  Define `turbo_flow_turbodb_outbox_record_t`, fetch budget/step enums and structs, provider operations, failure/shutdown policy enums, immutable source config, state/snapshot structs, the opaque owner, and the functions listed above. Use separate outbox API version/size constants and named finite defaults.

- [ ] **Step 4: Implement default construction and fail-fast open validation**

  Create `io/turbodb/src/turbo_flow_turbodb_outbox.c`; validate Flow STARTED state, source name, vtable sizes/callbacks, finite capacities, `in_flight_bytes >= sizeof(private storage)+1`, delivery attempts, message IDs, and explicit permanent/shutdown policies before allocating or invoking provider code.

- [ ] **Step 5: Run the focused target and verify GREEN**

  Build and run `ctest --preset win-dev-user -R '^test_turbodb_adapter$' --output-on-failure`. Expected: the new contract tests pass.

### Task 2: Demand-Bounded Fetch and Message Ownership

**Files:**
- Modify: `io/turbodb/src/turbo_flow_turbodb_outbox.c`
- Modify: `io/turbodb/tests/test_turbodb_outbox_source.c`

**Interfaces:**
- Consumes: provider `fetch(ctx, budget)` returning `RECORD`, `WAIT`, `IDLE`, `DATA_LOSS`, or `ERROR`.
- Produces: owner-thread `request/poll`, bounded slot storage, and `turbo_flow_turbodb_outbox_message_context/identity`.

- [ ] **Step 1: Write failing zero-demand and minimum-budget tests**

  Use a deterministic fake provider to prove zero-demand poll performs zero fetches and each observed budget equals `min(outstanding demand, fetch_count, free message slots)` plus the exact remaining retained-byte bound.

- [ ] **Step 2: Run focused RED**

  Expected: assertions fail because request/poll do not fetch records.

- [ ] **Step 3: Implement bounded owner state with CSTL Vec slots**

  Reserve and resize exactly `in_flight_messages` slots at open. Saturate demand addition, locate free slots without allocation, and call provider fetch only while all four admission budgets remain positive.

- [ ] **Step 4: Write failing owned-message and invalid-record tests**

  Verify accepted context/identity/payload remain valid after the fake provider mutates its source bytes; reject zero tokens/indexes, empty or oversized identities, oversized payloads, delivery attempt zero/overflow, retained-size overflow, and message-ID exhaustion. Every fetched-but-rejected token must be requeued and must not reach a Graph stage.

- [ ] **Step 5: Implement one contiguous retained message buffer**

  Store fixed receipt metadata followed by identity and payload bytes, point `transport_context` inside the buffer, and expose validating accessors. Create a one-value managed CFlow Publisher, move it into one `turbo_flow_run_t`, request one value, consume one demand unit, and account the exact `mem_buffer_used()` bytes.

- [ ] **Step 6: Run focused GREEN**

  Expected: all demand, budget, copy-ownership, overflow, and accessor tests pass under Debug/ASan.

### Task 3: Terminal Settlement, WAIT, and Shutdown

**Files:**
- Modify: `io/turbodb/src/turbo_flow_turbodb_outbox.c`
- Modify: `io/turbodb/tests/test_turbodb_outbox_source.c`

**Interfaces:**
- Consumes: `turbo_flow_run_snapshot/cancel/close`, provider `ack/requeue/dead_letter/cancel_fetch`, and failure classifier.
- Produces: exactly-once slot settlement state machine and observable counters/errors.

- [ ] **Step 1: Write failing success and retryable-failure tests**

  Verify one successful Graph terminal invokes only matching-token acknowledge once; graph failure invokes only requeue once; repeated poll never duplicates either callback.

- [ ] **Step 2: Implement per-slot terminal settlement phases**

  Keep run, token, bytes, graph status, and phase in each slot. Close/release only after its provider callback succeeds. Preserve an active phase after callback failure so the same idempotent operation is retried by a later owner poll.

- [ ] **Step 3: Write failing permanent failure tests**

  Verify explicit permanent classification invokes dead-letter then acknowledge; an acknowledgement retry does not repeat successful dead-letter; missing DLQ policy fails fast and leaves/requeues the claim without acknowledging.

- [ ] **Step 4: Implement permanent-failure sequencing**

  Track `DLQ_PENDING` and `ACK_PENDING` separately. Never acknowledge before dead-letter returns `SALTS_OK`, and expose the first provider error stage/status through snapshot.

- [ ] **Step 5: Write failing WAIT/data-loss/stop tests**

  Verify WAIT is armed only with demand, wake enables exactly one resumed fetch, stop cancels the waitable/provider fetch, no fetch begins after stop, active Graph runs are canceled and requeued, deleted/trimmed receipt increments a distinct data-loss counter/status, and destroy returns `SALTS_EBUSY` until stop succeeds.

- [ ] **Step 6: Implement waitable and bounded stop state machines**

  The waker atomically marks readiness only. Owner poll cancels/unarms before resuming fetch. Stop changes state before cancellation, cancels every run, requeues terminal claims, and becomes STOPPED only when no slot or fetch wait remains.

- [ ] **Step 7: Run focused GREEN**

  Expected: all source and settlement tests pass with exact callback/counter assertions.

### Task 4: Documentation, Package Boundary, and Verification

**Files:**
- Create: `io/turbodb/ADR_TURBODB_OUTBOX_SOURCE.md`
- Modify: `io/turbodb/CMakeLists.txt`
- Modify: `io/turbodb/tests/CMakeLists.txt`
- Modify: `CMakeLists.txt` or package files only if the existing component export requires it.

**Interfaces:**
- Consumes: completed outbox Source API.
- Produces: installed optional component with no new core dependency and reproducible verification evidence.

- [ ] **Step 1: Document state, ownership, error, and migration semantics**

  Record the single owner thread, immutable config, copied receipt bytes, one-run-per-token mapping, at-least-once redelivery, idempotent projection requirement, settlement retry phases, stop/requeue policy, capacity formula, and explicit dependency on `qigao/turbodb#21` for real Redis binding.

- [ ] **Step 2: Wire source and header tests into the existing optional target**

  Add the implementation source to `tf_turbodb_adapter`; do not create a core link edge or a conditional runtime fallback.

- [ ] **Step 3: Verify focused and adjacent Debug/ASan tests**

  Run the TurboDb adapter test, Flow run tests, async-terminal tests, and package-consumer test using `win-dev-user`.

- [ ] **Step 4: Verify full Debug/ASan and Release suites**

  Run configure/build/ctest for `win-dev-user` and `win-release-user`, both with `--output-on-failure`. Expected: 100% pass with the optional TurboDb component enabled; the normal package-consumer test continues to validate the component-independent install.

- [ ] **Step 5: Update GitHub issue evidence and prepare review**

  Check only criteria directly proven by tests, link the upstream #21 blocker for real Redis/Cluster/Sentinel work, request code review, and merge only with no unresolved HIGH/MED findings.
