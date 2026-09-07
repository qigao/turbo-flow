# CHTTP Deferred Server HTTP/2 Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete TurboFlow issue #7 by running the existing bounded CHTTP deferred server Flow
adapter over h2c and TLS ALPN `h2`, with stream-local failure isolation.

**Architecture:** Keep the current TurboFlow request slot and async-terminal state machine unchanged.
CHTTP remains the sole HTTP/2 stream/deferred-handle owner; TurboFlow copies callback-borrowed
request data before publication and replies through the generation-checked public handle. Remove
only the obsolete startup rejection now that qigao/salts#214 supplies the required H2 semantics.

**Tech Stack:** C11, TurboFlow async publication/terminal claims, Salts CHTTP/CNet, TinyTest, CMake
Presets.

**Spec:** `io/chttp/ADR_CHTTP_DEFERRED_SERVER.md` and qigao/turbo-flow#7.

## Global Constraints

- Do not add an HTTP/1.1, synchronous, legacy transport, C, or CMake fallback.
- Keep request/response/header/body/stream capacities explicit and bounded.
- Callback request views are borrowed; only the existing message-owned copy may cross into Flow.
- One failed, canceled, or blocked H2 stream must not close or rebuild a healthy sibling stream.
- TLS verification and ALPN `h2` selection remain explicit; no plaintext fallback.
- Server stop closes admission, drains accepted deferred handles, then destroys CHTTP ownership.

---

### Task 1: Enable h2c on the existing deferred adapter

**Files:**
- Modify: `io/chttp/tests/test_chttp_server_adapter.c`
- Modify: `io/chttp/src/turbo_flow_chttp_server.c`

**Interfaces:**
- Consumes: `chttp_server_config.enable_http2`, `chttp_options.protocol`,
  `chttp_server_response_defer()`.
- Produces: unchanged `turbo_flow_chttp_server_register()` behavior with working H1 and H2
  transports.

- [x] **Step 1: Add an explicit bounded H2 test configuration**

```c
static chttp_server_config chttp_server_adapter_h2_config(void) {
  chttp_server_config config = chttp_server_adapter_config();
  config.enable_http2 = 1;
  config.h2_stream_capacity = 4u;
  config.h2_input_buffer_bytes = 64u * 1024u;
  config.h2_output_buffer_bytes = 64u * 1024u;
  config.h2_hpack_dynamic_table_bytes = 4096u;
  config.h2_max_settings_count = 16u;
  return config;
}
```

- [x] **Step 2: Replace the obsolete rejection test with an h2c deferred round trip**

Create a Flow `source -> inspect -> response`, force the client request to
`CHTTP_HTTP_2`, and assert status/body plus copied request metadata reports HTTP major version 2.

- [x] **Step 3: Run the focused test and observe RED**

Run: `ctest --preset win-dev-user -R "^test_chttp_server_adapter$" --output-on-failure`

Expected: the h2c case fails because `chttp_server_adapter_start_native()` returns
`SALTS_ENOTSUP` while `enable_http2` is set.

- [x] **Step 4: Remove only the obsolete HTTP/2 startup guard**

```c
/* Delete this obsolete gate; all normal CHTTP validation remains. */
if (server->config.enable_http2) return SALTS_ENOTSUP;
```

- [x] **Step 5: Re-run the focused test and observe GREEN**

Run: `ctest --preset win-dev-user -R "^test_chttp_server_adapter$" --output-on-failure`

Expected: all server-adapter cases pass, including the new h2c round trip.

### Task 2: Prove H2 sibling-stream isolation through Flow

**Files:**
- Modify: `io/chttp/tests/test_chttp_server_adapter.c`

**Interfaces:**
- Consumes: `chttp_async_client_submit()`, `chttp_async_client_poll()`, the adapter's configured
  graph-error response, and a one-connection client pool.
- Produces: deterministic evidence that two requests share one H2 connection while terminating
  independently.

- [x] **Step 1: Add one conditional Flow stage and an owning async completion probe**

```c
static int chttp_server_adapter_isolate(turbo_flow_msg_t *message, void *ctx) {
  if (message->payload.len == 4u && memcmp(message->payload.data, "fail", 4u) == 0)
    return SALTS_EIO;
  return SALTS_OK;
}
```

The completion callback copies status/body into caller-owned test storage and is invoked exactly
once per CHTTP request.

- [x] **Step 2: Submit failing and successful requests before polling**

Use one `chttp_async_client`, set both request options to `CHTTP_HTTP_2`, submit both requests, and
poll until two completions or the bounded poll limit.

- [x] **Step 3: Assert stream-local terminal behavior**

Assert the failed graph request receives the configured graph-error status/body, the sibling
receives its successful body, each callback fires once, the client admits both requests with
`connection_capacity == 1`, and the TurboFlow server snapshot returns `active_requests == 0` with
two completions.

- [x] **Step 4: Run the focused Debug test**

Run: `ctest --preset win-dev-user -R "^test_chttp_server_adapter$" --output-on-failure`

Expected: PASS without resetting the shared H2 connection.

### Task 3: Prove TLS ALPN h2 without fallback

**Files:**
- Modify: `io/chttp/tests/test_chttp_server_adapter.c`
- Reuse: `io/cnet/tests/listener_source_tls_fixture.h`

**Interfaces:**
- Consumes: `cnet_tls_server_config`, `cnet_tls_client_config`, `chttp_tls_profile_init()`, and
  explicit ALPN list `{ "h2" }`.
- Produces: one verified TLS H2 deferred Flow round trip.

- [x] **Step 1: Configure a test certificate and explicit server/client ALPN**

```c
static const char *alpn[] = {"h2"};
server_tls.alpn_protocols = alpn;
server_tls.alpn_protocol_count = 1u;
client_tls.alpn_protocols = alpn;
client_tls.alpn_protocol_count = 1u;
```

Use `tls://127.0.0.1:<port>`, `server_name="localhost"`, the fixture certificate as CA, and
`CHTTP_HTTP_2`; do not retry as plaintext or H1.

- [x] **Step 2: Execute the Flow request and verify protocol metadata**

Assert the graph observes HTTP major version 2, the deferred response completes successfully,
and the configured response body is returned.

- [x] **Step 3: Destroy in owner-safe order**

Stop/destroy the CHTTP client, stop/destroy Flow and its registered server adapter, destroy the TLS
profile, then remove fixture files after all TLS borrowers stop.

- [x] **Step 4: Run the focused Debug test**

Run: `ctest --preset win-dev-user -R "^test_chttp_server_adapter$" --output-on-failure`

Expected: PASS with ALPN `h2`; certificate or ALPN errors fail explicitly.

### Task 4: Update contract, verify, and deliver

**Files:**
- Modify: `io/chttp/include/turbo_flow_chttp.h`
- Modify: `io/chttp/ADR_CHTTP_DEFERRED_SERVER.md`
- Modify: `docs/superpowers/plans/2026-09-07-chttp-deferred-server-h2.md`

**Interfaces:**
- Consumes: qigao/salts#229 stacked dependency.
- Produces: documented H1/H2 contract and updated PR #32 / issue #7 evidence.

- [x] **Step 1: Update public and architecture documentation**

Document h2c/TLS ALPN support, stream-local generation binding, deferred-body non-goal, and the
absence of H1/synchronous/CMake fallback.

- [x] **Step 2: Run formatting and static diff checks**

Run `clang-format` on modified C/C++ sources, then `git diff --check` and scan for obsolete H2
rejection text or unowned TODO/FIXME/HACK markers.

- [x] **Step 3: Run Debug and Release verification**

Run configure/build/test through `win-dev-user` and `win-release-user`, first focused and then full
CTest. Install Debug through `install-win-dev-user` and run the installed C/C++ consumer checks.

- [ ] **Step 4: Sync structure evidence and deliver**

Run `codegraph sync .`, inspect diff/impact, commit, push `refactor/issue-7-chttp-deferred-server`,
update PR #32 and the #7 acceptance checklist, and leave merging to an explicit user request.
