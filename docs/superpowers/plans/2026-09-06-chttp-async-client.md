# CHTTP Async Client Stage Implementation Plan

> **For Codex:** Execute this plan inline with red/green checkpoints. Do not merge until the user explicitly requests it.

**Goal:** Replace the removed legacy HTTP client path with a bounded `Salts::CHTTP` adapter whose accepted requests suspend one graph publication and resume downstream with an owned HTTP response message.

**Architecture:** Add a transport-neutral move-only async 0..1-output claim to TurboFlow core. The claim retains the input publication, marks the stage as a CFlow `FLAT_MAP` boundary, and resumes only the subgraph downstream of that stage when completed. Build `TurboFlow::CHTTPAdapter` on top of one long-lived, caller-polled `chttp_async_client`; CHTTP owns HTTP/1.1 reuse and HTTP/2 multiplexing, while the adapter owns request contexts, retry/deadline policy, response copying, and shutdown drain.

**Tech Stack:** C11, TurboFlow Graph, Salts CHTTP/CNet/CFlow/CSTL, TinyTest, CMake presets.

---

### Task 1: Define and verify the async flat-map claim

**Files:**
- Modify: `turbo_flow/include/turbo_flow.h`
- Modify: `turbo_flow/src/flow_internal.h`
- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/src/flow_compile.c`
- Modify: `turbo_flow/src/flow_dispatch.c`
- Modify: `turbo_flow/src/flow_runtime.c`
- Modify: `turbo_flow/src/flow_plan_semantics.c`
- Modify: `turbo_flow/src/flow_async_terminal.c`
- Test: `turbo_flow/tests/test_flow_async_emit.c`
- Modify: `turbo_flow/CMakeLists.txt`

1. Write failing tests for move-only ownership, synchronous-publication rejection, downstream response delivery, zero-output completion, terminal error, duplicate completion, topology rejection, and CFlow `FLAT_MAP` semantics.
2. Run only the new test and confirm the API/build fails before implementation.
3. Implement the smallest transport-neutral async claim, sharing publication accounting with async terminal claims.
4. Run the focused test and adjacent async-terminal/emitter tests.

### Task 2: Add the bounded CHTTP adapter

**Files:**
- Create: `io/chttp/include/turbo_flow_chttp.h`
- Create: `io/chttp/src/turbo_flow_chttp.c`
- Create: `io/chttp/CMakeLists.txt`
- Create: `io/chttp/tests/CMakeLists.txt`
- Create: `io/chttp/tests/test_chttp_adapter.c`
- Modify: `CMakeLists.txt`
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Modify: `tests/install_consumer/CMakeLists.txt`
- Modify: `tests/install_consumer/main.c`

1. Write failing lifecycle and real loopback tests for GET/HEAD/POST/PUT/DELETE/PATCH, request/response bounds, cancellation, and completion exactly once.
2. Implement one caller-owned `chttp_async_client`, fixed request template fields, copied flat request bodies, owned response-message construction, bounded request contexts, poll, cancel, snapshot, stop, and detach/destroy.
3. Keep protocol, TLS profile, retry policy, method idempotency, response limits, and overall deadline explicit; reject unsupported streaming/retry combinations without fallback.
4. Add H1 reuse/stale-connection and H2 multiplex/sibling-isolation tests using CHTTP loopback servers where supported by the configured Salts build.

### Task 3: Document the ownership decision and package surface

**Files:**
- Create: `io/chttp/ADR_CHTTP_ASYNC_CLIENT.md`
- Modify: `turbo_flow/ARCHITECTURE.md`
- Modify: `README.md`

1. Document the single progress owner, claim lifecycle, state ownership, error semantics, retry gates, deadline behavior, shutdown order, and lack of legacy/fallback paths.
2. Document `find_package(TurboFlow COMPONENTS CHTTPAdapter)` and the installed header/target.

### Task 4: Verify and prepare the PR

1. Run CodeGraph affected analysis for all changed files.
2. Configure with `cmake --preset win-dev-user` under the MSVC developer environment.
3. Build and run focused Debug tests, full Debug/ASan tests, Release tests, and Debug/Release install-consumer checks.
4. Confirm no removed legacy product references and no untracked build helper remains.
5. Review the diff, commit with issue reference, push the branch, and open a stacked PR for #6 without merging.
