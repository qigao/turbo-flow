# Tagged Packet Terminal Sink Implementation Plan

> **For Codex:** Execute this plan sequentially because TurboFlow consumes the installed Salts package produced by the preceding task.

**Goal:** Add authoritative tagged logical-send completion to Salts CNet packet endpoints, then expose it as a bounded TurboFlow async-terminal sink for UDP, KCP, secure KCP, and FEC sessions.

**Architecture:** Salts remains the single transport owner. A size/versioned terminal configuration enables a fixed operation pool; raw UDP settles from NativeIO datagram completion, while KCP settles only after its cumulative acknowledgement point passes the logical message's final sequence. TurboFlow moves each graph async-terminal claim into a fixed-capacity operation and settles it only from the matching Salts callback. Session failure and shutdown retain concrete errors and drain all admitted operations without fallback.

**Tech Stack:** C11, Salts CNet/NativeIO/KCP, TurboFlow async-terminal adapters, CFlow IO Actor, CSTL where already used, TinyTest, CMake user presets.

---

## Task 1: Specify and test Salts packet terminal semantics

**Files:**
- Modify: `C:/projects/cpp/turbonet/salts/cnet/include/cnet/cnet.h`
- Modify: `C:/projects/cpp/turbonet/salts/cnet/tests/cnet_packet_endpoint_test.c`
- Add: `C:/projects/cpp/turbonet/salts/cnet/ADR_PACKET_SEND_TERMINALS.md`

1. Document the owner, fixed capacity, tag ownership, UDP native-terminal boundary, KCP acknowledgement boundary, close/stop ordering, and error propagation.
2. Add TinyTest cases that prove admission is not completion, tags correlate exactly once, capacity rejects without callback, stale sessions do not admit, KCP multi-fragment sends wait for acknowledgements, secure KCP follows the same boundary, and close/stop settle retained sends.
3. Build and run `cnet_packet_endpoint_test`; confirm the new test fails because the additive API is absent.

## Task 2: Implement Salts #219

**Files:**
- Modify: `C:/projects/cpp/turbonet/salts/cnet/include/cnet/cnet.h`
- Modify: `C:/projects/cpp/turbonet/salts/cnet/src/cnet_kcp.c`
- Add: `C:/projects/cpp/turbonet/salts/cnet/src/cnet_kcp_internal.h`
- Modify: `C:/projects/cpp/turbonet/salts/cnet/src/cnet_secure_kcp.c`
- Modify: `C:/projects/cpp/turbonet/salts/cnet/src/cnet_secure_kcp_internal.h`
- Modify: `C:/projects/cpp/turbonet/salts/cnet/src/cnet_packet_endpoint.c`
- Modify: `C:/projects/cpp/turbonet/salts/cnet/README.md`

1. Add an additive size/versioned terminal configuration, `cnet_packet_endpoint_init_ex`, and tagged send admission without changing legacy `cnet_packet_send` semantics.
2. Add internal KCP send markers based on the final admitted sequence and wrap-safe acknowledgement checks; when tagged terminals are enabled, reject KCP stream mode because logical message boundaries are required, while preserving legacy init behavior.
3. Add a fixed operation pool and per-session FIFO in the packet endpoint. Never allocate on the send/poll hot path.
4. Settle UDP from its unique native completion and KCP from protocol acknowledgement. On close/failure/stop, queue one concrete terminal per admitted operation and dispatch callbacks outside internal mutation.
5. Run the focused test to green, then CNet adjacent tests and full Debug/ASan tests.

## Task 3: Publish and install Salts

1. Build and test Release.
2. Install Debug and Release with `install-win-dev-user` and `install-win-release-user`.
3. Review the diff, commit, push, open a PR linked to salts#219, merge after checks are green, and close #219 with verification evidence.

## Task 4: Specify and test TurboFlow #26

**Files:**
- Modify: `io/cnet/include/turbo_flow_cnet.h`
- Add: `io/cnet/tests/test_cnet_packet_sink.c`
- Modify: `io/cnet/tests/CMakeLists.txt`
- Add: `io/cnet/ADR_CNET_PACKET_SINK.md`

1. Define a size/versioned opaque sink owner with fixed in-flight capacity, exact session/tag correlation, move-only claim ownership, and explicit stop/drain states.
2. Add TinyTest cases for UDP, KCP, secure KCP/FEC, full capacity (`SALTS_ENOSPC`), stale session, native/session failure, cancellation race, exactly-once settlement, and stop/destroy ordering.
3. Build the new test and confirm RED before production implementation.

## Task 5: Implement the TurboFlow packet sink

**Files:**
- Add: `io/cnet/src/turbo_flow_cnet_packet_sink.c`
- Modify: `io/cnet/CMakeLists.txt`
- Modify: `tests/install_consumer/main.c`

1. Register an async-terminal adapter whose submit moves the claim only after all synchronous validation succeeds.
2. Correlate the endpoint's opaque tag to a fixed operation slot/generation and settle only from the tagged terminal callback.
3. Preserve Salts errors; translate only the sink's own fixed-capacity exhaustion to `SALTS_ENOSPC`.
4. Stop admission, request endpoint stop, continue progress until all transport terminals and Actor acknowledgements drain, then destroy.
5. Extend installed C/C++ consumer coverage for public headers, targets, and transitive dependencies.

## Task 6: Verify, publish, and update issues

1. Run focused, adjacent, and full Debug/ASan tests with `win-dev-user`.
2. Run full Release tests with `win-release-user`.
3. Install both profiles and run installed consumer tests.
4. Review the final diff, commit, push, open and merge the TurboFlow PR after checks are green.
5. Close #26, update #5's checklist/evidence, and select the next unblocked issue.
