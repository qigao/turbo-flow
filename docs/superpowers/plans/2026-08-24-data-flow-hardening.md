# Turbo Flow Data-Path Hardening Implementation Plan

> **For Codex:** Execute this plan task by task with test-first checkpoints and preserve the existing staged dependency/preset changes.

**Goal:** Make message ownership, graph completion, rule decisions, and asynchronous ingress bounded and mechanically verifiable without changing valid existing flows.

**Architecture:** Keep `turbo_flow_msg_t` as the single transport envelope. Centralize its payload/view invariant at the message boundary, keep graph reachability iterative and runtime-owned, reject contradictory single-valued rule decisions, and account asynchronous ingress capacity in both entries and retained bytes. Treat network handoff profiles and the Lean refinement proof as separate layers built on these runtime invariants.

**Tech Stack:** C11, TurboUtils memory buffers and TinyTest, CMake presets with vcpkg BoringSSL compatibility targets, Lean 4 for the abstract model/refinement layer.

---

### Task 0: Restore the BoringSSL-backed build baseline

**Files:**
- Modify: `CMakeLists.txt:71`
- Verify: `vcpkg_installed/x64-windows/share/OpenSSL/OpenSSLConfig.cmake`

- [ ] Change OpenSSL discovery to config mode so the official BoringSSL port supplies `OpenSSL::SSL` and `OpenSSL::Crypto`.
- [ ] Run a fresh `win-release-user` configure from `VsDevCmd.bat`.
- [ ] Build the smallest existing Turbo Flow test targets and record the baseline result.

### Task 1: Enforce the message payload ownership invariant

**Files:**
- Modify: `turbo_flow/src/flow_message.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/src/flow_async_ingress.c`
- Modify: `turbo_flow/include/turbo_flow_internal.h`
- Test: `turbo_flow/tests/test_turbo_flow.c`

- [ ] Add tests proving that a borrowed payload outside its backing `mem_buffer_t` is rejected by retain, synchronous publish, batch publish, and asynchronous publish.
- [ ] Run the focused tests and observe the expected failures.
- [ ] Add one overflow-safe internal validator for owned payloads and buffer-backed views.
- [ ] Apply the validator before any retain/clone or cross-thread handoff.
- [ ] Re-run focused ownership tests and adjacent message/runtime tests.

### Task 2: Remove recursive graph reachability from the per-message path

**Files:**
- Modify: `turbo_flow/src/flow_completion.c`
- Modify: `turbo_flow/include/turbo_flow_internal.h`
- Test: `turbo_flow/tests/test_turbo_flow.c`
- Benchmark: `turbo_flow/tests/benchmark_turbo_flow.c`

- [ ] Add a deep linear graph regression test that exercises completion without relying on the C call stack.
- [ ] Add a representative graph-completion benchmark before changing the algorithm.
- [ ] Run the focused test/benchmark to capture the recursive baseline or failure.
- [ ] Replace recursive traversal with a bounded iterative worklist owned by the execution workspace; reuse compiled graph adjacency where available.
- [ ] Verify deep, fan-out, fan-in, routed, and broadcast completion behavior.

### Task 3: Reject contradictory single-valued rule decisions

**Files:**
- Modify: `turbo_flow/src/flow_policy.c`
- Test: `turbo_flow/tests/test_flow_policy.c`
- Document: `docs/ARCHITECTURE.md`

- [ ] Add failing tests for multiple matching route, batch, or retry actions under `ALL_MATCHES`.
- [ ] Return a typed protocol/configuration error instead of silently applying last-write-wins.
- [ ] Preserve additive actions and document which decisions are single-valued.
- [ ] Run policy tests and graph-routing regression tests.

### Task 4: Bound asynchronous ingress by retained bytes

**Files:**
- Modify: `turbo_flow/include/turbo_flow.h`
- Modify: `turbo_flow/src/flow_async_ingress.c`
- Modify: `turbo_flow/src/flow_product.c`
- Test: `turbo_flow/tests/test_turbo_flow.c`
- Test: `turbo_flow/tests/test_flow_config.c`

- [x] Add failing tests for per-message and aggregate in-flight byte limits.
- [x] Extend the size-versioned configuration compatibly: accept the previous struct size and read new fields only when present.
- [x] Reserve retained bytes before enqueue and release exactly once on every completion/rejection/shutdown path.
- [x] Parse and validate the corresponding YAML fields with fail-fast errors.
- [ ] Expose byte-budget rejection counters in a versioned runtime snapshot.
- [ ] Verify queue-full, byte-full, shutdown, and successful drain paths under ASan.

### Task 5: Define high-concurrency network handoff profiles

**Files:**
- Modify: `io/socket/include/turbo_flow_socket.h`
- Modify: `io/socket/src/socket.c`
- Modify: `ingress/protocol/graph/include/turbo_flow_protocol_graph.h`
- Modify: `ingress/protocol/graph/src/flow_protocol_graph.c`
- Test: `io/socket/tests/test_socket.c`
- Test: `ingress/protocol/graph/tests/test_flow_protocol_graph.c`

- [x] Specify `inline` and `async_bounded` handoff modes with explicit ownership, settlement, queue, and byte-budget semantics.
- [x] Add tests proving that event-loop callbacks do not block in `async_bounded` mode and that `PENDING` completes exactly once for owner-thread settlement.
- [x] Reuse the bounded async ingress rather than adding another queue implementation.
- [x] Keep `inline` as the compatibility default and verify existing socket/protocol behavior.

### Task 6: Connect runtime invariants to the Lean flow model

**Files:**
- Follow: `docs/superpowers/plans/2026-08-24-lean-flow-model.md`
- Update: `docs/FORMAL_FLOW_MODEL.md`

- [ ] Execute the existing Lean plan after the C runtime invariants above have stable tests.
- [ ] Model payload provenance, bounded handoff, single-valued decisions, iterative reachability equivalence, and exactly-once settlement.
- [ ] Record the C-to-Lean abstraction map and identify assumptions not yet enforced by code.
- [ ] Run `lake build` and the C regression suite; keep proof claims limited to the checked model and documented refinement boundary.

### Verification sequence

- [ ] Focused TinyTest filters for the currently modified invariant.
- [ ] Adjacent Turbo Flow, policy, config, protocol runtime, and protocol graph CTest targets.
- [ ] Windows Release suite through `win-release-user`.
- [ ] Windows ASan suite through `win-dev-user` for ownership and shutdown paths.
- [ ] Lean `lake build` after the formal task is implemented.
