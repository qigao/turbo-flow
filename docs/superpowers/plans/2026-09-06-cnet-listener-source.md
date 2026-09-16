# CNet TCP/TLS Listener Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded caller-driven TCP/TLS listener owner that publishes accepted stream bytes into a TurboFlow reactive run only under downstream demand.

**Architecture:** Extend the optional CNetAdapter with a separate opaque listener source. One owner serializes listener accept, accepted CNet client progress, a fixed connection-slot table, one demand-gated receive, a manual CFlow scheduler, and one TurboFlow run; borrowed callback bytes become one managed message before Graph execution.

**Tech Stack:** C11, Salts CNet/CFlow/CMeta memory, TurboFlow Graph, TinyTest, CMake Presets.

**Spec:** `io/cnet/ADR_CNET_LISTENER_SOURCE.md`

## Global Constraints

- All public calls and callbacks belong to one serialized owner thread.
- `max_connections`, `max_message_bytes`, Scheduler storage, and CNet storage are positive hard bounds.
- One downstream demand admits at most one receive; no silent drop, truncation, growth, retry, or fallback.
- TLS configuration and handshake errors remain fail-closed and never select plaintext.
- The Graph target must remain independent of CNet and no legacy transport alias may be added.

---

### Task 1: Public listener owner contract

**Files:**
- Modify: `io/cnet/include/turbo_flow_cnet.h`
- Modify: `io/cnet/tests/cnet_stream_source_header_cpp.cpp`
- Create: `io/cnet/tests/test_cnet_listener_source.c`
- Modify: `io/cnet/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `turbo_flow_t`, `cnet_listener_config`, `cnet_client_config`, optional `cnet_tls_server_config` and `cnet_stream_socket_options`.
- Produces: `turbo_flow_cnet_listener_source_{open,request,poll,snapshot,stop,destroy}` and size/versioned config/snapshot types.

- [x] **Step 1: Write a failing C ABI/defaults test and C++ header probe**

```c
turbo_flow_cnet_listener_source_config_t config =
    TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
turbo_flow_cnet_listener_source_snapshot_t snapshot =
    TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
check_equal(config.version, TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION);
check_equal(snapshot.state, TURBO_FLOW_CNET_LISTENER_SOURCE_NEW);
```

- [x] **Step 2: Configure and build the test to verify RED**

Run through `VsDevCmd.bat`:

```text
cmake --fresh --preset win-dev-user
cmake --build --preset win-dev-user --target test_cnet_listener_source
```

Expected: compile failure because the listener contract is not declared.

- [x] **Step 3: Add the opaque contract and documentation**

Declare one opaque owner, lifecycle enum, copied config, snapshot, init macros, six functions, parameter/return/error documentation, and a complete TCP example. Keep existing stream declarations unchanged.

- [x] **Step 4: Rebuild to reach the linker RED state**

Expected: declarations compile and link fails because the six functions are not implemented.

### Task 2: Bounded TCP listener state machine

**Files:**
- Create: `io/cnet/src/turbo_flow_cnet_listener_source.c`
- Modify: `io/cnet/CMakeLists.txt`
- Modify: `io/cnet/tests/test_cnet_listener_source.c`

**Interfaces:**
- Consumes: Task 1 API, `cnet_listener_accept_peer`, `cnet_receive`, manual CFlow Scheduler, `turbo_flow_run_open`, `mem_buffer_t`.
- Produces: fixed connection slots, round-robin receive admission, owning message handoff, snapshots and lifecycle.

- [x] **Step 1: Add failing invalid-config tests**

Cover NULL/size/version, unstarted Flow, zero bounds, `max_connections > client.connection_capacity`,
`max_message_bytes > client.receive_buffer_bytes`, invalid listener options, TLS storage mismatch, and ensure
`source_out == NULL` after every failure.

- [x] **Step 2: Implement transactional open and cleanup**

Allocate the owner and one fixed slot array with checked multiplication, initialize Scheduler/client/socket policy/
listener/TLS in dependency order, move the Publisher into one run, and unwind each successful step exactly once.

- [x] **Step 3: Add a real TCP zero-demand test and verify RED**

Bind port zero, connect an external CNet client, send `"first"`, poll both owners, and assert Graph count remains
zero and no receive is pending before `request(1)`.

- [x] **Step 4: Implement accept and demand-gated receive**

Accept only into a free fixed slot, use per-slot stable observer state, select CONNECTED slots round-robin from the
Publisher resume callback, and allow only one global pending receive.

- [x] **Step 5: Implement borrowed-to-owning message copy and verify GREEN**

Copy the callback view into `mem_buffer_t`, create a `turbo_flow_msg_t`, wake the Publisher after callback return,
move exactly once, and assert the literal payload and message id in the real Graph probe.

- [x] **Step 6: Add capacity, two-client fairness, remote-close and slot-reuse tests**

With `max_connections == 1`, prove a second peer is not accepted until the first terminal callback retires its
slot. With two active connections and two requests, prove both payloads arrive without one connection starving.

- [x] **Step 7: Add oversize/error tests and implement first-error preservation**

Send `max_message_bytes + 1` bytes, assert `SALTS_EMSGSIZE`, zero partial delivery and stable snapshot status/stage;
matching generation handles alone may mutate slots.

### Task 3: Fail-closed TLS listener

**Files:**
- Create: `io/cnet/tests/listener_source_tls_fixture.h`
- Modify: `io/cnet/tests/test_cnet_listener_source.c`
- Modify: `io/cnet/src/turbo_flow_cnet_listener_source.c`

**Interfaces:**
- Consumes: `cnet_tls_server_init`, `cnet_listener_accept_tls_peer`, verified CNet TLS client configuration.
- Produces: the same listener API with TLS selected only by non-NULL server configuration.

- [x] **Step 1: Add invalid certificate and TLS-storage RED tests**

Assert missing certificate/key returns the exact CNet setup error and zero TLS storage returns `SALTS_ENOTSUP`,
with no published owner.

- [x] **Step 2: Add a real TLS loopback test**

Write the test certificate/key to TinyTest temporary files, open the listener, connect a verified client using the
certificate as CA and `server_name = "localhost"`, request one value, send literal `"tls"`, and assert one owning
Graph payload.

- [x] **Step 3: Implement TLS context ownership and accept dispatch**

Create the TLS server context during open, select only `cnet_listener_accept_tls_peer` for TLS owners, preserve
handshake failure status/native/stage, and destroy the context after accepted sessions quiesce.

### Task 4: Shutdown, packaging and full verification

**Files:**
- Modify: `io/cnet/tests/test_cnet_listener_source.c`
- Modify: `tests/install_consumer/main.c`
- Modify: `io/cnet/ADR_CNET_LISTENER_SOURCE.md`

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: installed/verified listener source and evidence for issue #22.

- [x] **Step 1: Add stop/drain/destroy tests**

Assert destroy-before-stop is `SALTS_EBUSY`, stop closes admission before connections, timeout keeps the owner
retryable by contract, successful repeated stop is `SALTS_EALREADY`, and post-stop request/poll are
`SALTS_ESHUTDOWN`.

- [x] **Step 2: Implement ordered shutdown and cleanup**

Close listener, close the run, close all live connections, stop/destroy client, destroy TLS/listener/Scheduler,
clean any ready message, and only then publish STOPPED.

- [x] **Step 3: Extend the installed-package consumer**

Compile the init macros and function pointers through installed `turbo_flow_cnet.h` while linking
`TurboFlow::CNetAdapter`.

- [x] **Step 4: Run focused and full verification**

Run through `VsDevCmd.bat`:

```text
cmake --build --preset win-dev-user --target test_cnet_listener_source
ctest --preset win-dev-user -R "cnet_listener_source|install_consumer" --output-on-failure
cmake --build --preset win-dev-user --parallel
ctest --preset win-dev-user --output-on-failure
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user --output-on-failure
```

Inspect exports/dependencies with `dumpbin`, run `git diff --check`, and scan production/CMake files with
`rg.exe` for forbidden legacy names and fallback branches.

- [x] **Step 5: Commit and publish the stacked PR**

```text
git add io/cnet tests/install_consumer docs/superpowers/plans/2026-09-06-cnet-listener-source.md
git commit -m "feat(cnet): add bounded listener source owner"
git push -u origin feat/issue-22-cnet-listener-source
```

Create a PR based on `feat/issue-18-cnet-stream-source`, include fresh verification evidence, and close #22.
