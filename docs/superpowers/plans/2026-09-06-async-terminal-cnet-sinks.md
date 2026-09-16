# Async Terminal CNet Sinks Implementation Record

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox state for resumable execution.

**Goal:** Deliver issue #20 with a reusable Graph async-terminal stage contract and bounded CFlow IO Actor based CNet stream/raw-datagram sinks.

**Architecture:** Add a separate versioned registration and move-only claim instead of changing synchronous adapter ABI. A per-publication tracker defers stage/sink/flow completion and settlement until every authoritative terminal arrives. CNet sink owners drive CNet, IO Actor, and its manual Executor on one caller-owned lane.

**Tech Stack:** C11, TurboFlow Graph, Salts CFlow IO Actor/Executor, Salts CNet, CSTL, TinyTest, CMake Presets.

**Spec:** `turbo_flow/ADR_ASYNC_TERMINAL_ADAPTER.md`, GitHub issues #20/#26,
upstream qigao/salts#219.

## Global Constraints

- Do not restore TurboParser, TurboNet, TurboHttp, CoroNet, aliases, or fallback paths.
- Preserve synchronous adapter ABI and behavior.
- Never settle a send from CNet admission.
- Keep all capacities fixed and report full/closed/stale errors distinctly.
- Use RED-GREEN-REFACTOR and run the smallest relevant test before broad suites.
- Keep Graph independent of CNet; only `TurboFlow::CNetAdapter` links CNet.

---

## Task 1: Core contract surface and compile validation

- [x] Add C/C++ tests for size/version validation, duplicate binding,
      transport-neutral HTTP schema reuse, and unsupported sync publish.
- [x] Add terminal-only, direct-inline compile validation with retry, reorder,
      worker, deadline, source, port, and outgoing-edge rejection.
- [x] Add `turbo_flow_async_terminal_claim_t`, move/complete/message APIs, async
      adapter ops, and additive registration to `turbo_flow.h`.
- [x] Store the copied async ops in the internal adapter registration.
- [x] Extend compile/runtime validation without changing synchronous adapters.
- [x] Run `test_turbo_flow` and header compatibility tests.

## Task 2: Deferred publication, observer, and settlement lifecycle

- [x] Add tests for delayed stage/sink/flow observer events, delayed async
      completion, retained message lifetime, exactly-once completion, concrete
      failure propagation, and settlement timing.
- [x] Implement the bounded publication tracker and thread-local dispatch scope.
- [x] Make async ingress and Reactive run value accounting wait for all claims.
- [x] Keep accepted claims in Flow shutdown accounting until terminal completion.
- [x] Add fan-out aggregation and stop/drain race tests.
- [x] Run `test_turbo_flow`, `test_flow_run`, and ASan variants.

## Task 3: CNet stream sink

- [x] Add a Pipe integration test for connect state, capacity-one full admission,
      retained bytes, authoritative logical-send completion, and exact callback count.
- [x] Preserve CNet stale-handle/connection-terminal status and reject any zero,
      partial, or over-length logical completion as `SALTS_EPROTO`.
- [x] Add a size/versioned opaque stream sink owner and snapshot API.
- [x] Register its async terminal adapter before compile and start CNet from the
      adapter lifecycle.
- [x] Move accepted claims into a capacity-one CFlow IO Actor; call `cnet_send`
      only from the Actor backend and complete only from `on_send` or connection
      terminal.
- [x] Drive Actor -> CNet -> Actor -> Executor from explicit poll.
- [x] Run `test_cnet_stream_sink` and neighboring CNet source tests.

## Task 4: CNet raw datagram sink

- [x] Add raw UDP integration tests for tag correlation, capacity full, concrete
      terminal status, exact callback count, and stop/drain cancellation.
- [x] Add a size/versioned opaque datagram sink owner, fixed peer, and snapshot.
- [x] Use Actor request IDs as `cnet_datagram_send` tags and complete only from
      tagged `on_send`.
- [x] Track packet-endpoint/KCP sink work in #26 until qigao/salts#219 provides
      tagged logical-send completion; do not fall back to admission semantics.
- [x] Run `test_cnet_datagram_sink` and `test_cnet_packet_source`.

## Task 5: Documentation, package verification, and issue handoff

- [x] Update CNet adapter documentation and installed C/C++ consumer coverage.
- [x] Confirm no active legacy TurboParser/TurboNet/TurboHttp references were added.
- [x] Run configure/build/test with `cmake --preset win-dev-user`, plus Debug,
      ASan, Release, and install-consumer validation.
- [x] Sync CodeGraph and inspect affected tests.
- [x] Commit, push, open the stacked PR, link #20 and qigao/salts#219, and report
      exact verification evidence without merging until the user asks.
