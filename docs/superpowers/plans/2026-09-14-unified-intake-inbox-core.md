# Unified Intake Inbox Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为所有 Source DLL 提供同一个先接纳、后图处理的 inbox vtable，并交付有界内存 provider。

**Architecture:** Source 只提交带当前 envelope schema、业务 content descriptor、correlation 和 payload bytes 的不可变记录。Inbox provider 是记录、claim 与失败状态的唯一 owner；Graph 仅借用 claim view，TurboDB provider 后续实现同一 ABI，任何 provider 失败都原样返回且不得切换 provider。

**Tech Stack:** C11、Salts Core/CSTL/CMeta/CFlow、TinyTest、CMake presets。

**Spec:** `docs/architecture/transport-independent-business-graph.md`

## Global Constraints

- 不兼容旧结构、旧数据、旧配置或旧 DLL ABI，也不提供 C/CMake/runtime fallback。
- 持久化字段不得包含进程指针、DLL 地址、裸 session 或函数指针。
- 记录数、总 retained bytes、单记录 retained bytes 与并发 claim 数都必须有硬上限。
- 接纳失败保留调用方输入所有权；接纳成功后 provider 拥有完整副本。
- Graph 借用 claim view，view 在成功 complete/fail 前有效；claim 只有一个 owner。
- fail 只形成显式 FAILED 状态，只有 retry 才能重新领取。
- close 后拒绝新接纳，但允许处理已接纳记录；destroy 仅在 closed 且 drained 时成功。

---

### Task 1: Public inbox vtable contract and bounded memory provider

**Files:**
- Create: `turbo_flow/include/turbo_flow_inbox.h`
- Create: `turbo_flow/src/flow_inbox.c`
- Create: `turbo_flow/tests/test_flow_inbox.c`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `turbo_flow_content_descriptor_t`, `vstr`, `vec_t`, `mem_buffer_t`, `salts_mutex_t`.
- Produces: `turbo_flow_inbox_t`, `turbo_flow_inbox_ops_v1_t`, `turbo_flow_inbox_memory_create()`, `turbo_flow_inbox_admit()`, `turbo_flow_inbox_claim()`, `turbo_flow_inbox_complete()`, `turbo_flow_inbox_fail()`, `turbo_flow_inbox_retry()`, `turbo_flow_inbox_discard()`, `turbo_flow_inbox_close()`, `turbo_flow_inbox_snapshot()`, `turbo_flow_inbox_destroy()`.

- [x] **Step 1: Write failing contract tests**

```c
check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
check_equal(claim.record.payload, "payload");
check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
```

Add independent cases for invalid 0 limits, N/N+1 records and bytes, old envelope schema, copy ownership, unique claim, FAILED visibility, explicit retry, stale token, close/drain, and destroy-before-drain.

- [x] **Step 2: Run RED**

Run: `cmake --build --preset win-release-user --target test_flow_inbox`

Expected: configure/build fails because `turbo_flow_inbox.h` and its API do not exist.

- [x] **Step 3: Implement the minimal vtable and memory owner**

```c
typedef struct turbo_flow_inbox_s {
  size_t size;
  uint32_t version;
  const turbo_flow_inbox_ops_v1_t *ops;
  void *ctx;
} turbo_flow_inbox_t;

TURBO_FLOW_C_API int turbo_flow_inbox_memory_create(
    const turbo_flow_inbox_memory_config_t *config,
    turbo_flow_inbox_t *out);
```

Preallocate a bounded `vec_t` of record pointers. Copy record bytes before locking; mutate slot state and counters under one mutex; never invoke an external callback or allocate while holding the mutex.

- [x] **Step 4: Run GREEN and adjacent tests**

Run: `cmake --build --preset win-release-user --target test_flow_inbox && ctest --preset win-release-user -R "test_flow_inbox|test_flow_managed_source|test_turbodb_outbox_source" --output-on-failure`

Expected: all selected tests pass with no warnings.

### Task 2: Document exact protocol and migration boundary

**Files:**
- Modify: `docs/architecture/transport-independent-business-graph.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1 public API and its tested state machine.
- Produces: exact ownership, capacity, state, concurrency, shutdown and provider-selection documentation.

- [x] **Step 1: Record the implemented state machine**

```text
admit: caller bytes --copy--> PENDING
claim: PENDING -> CLAIMED(token)
fail: CLAIMED(token) -> FAILED
retry: FAILED -> PENDING
complete: CLAIMED(token) -> removed
close: OPEN -> CLOSED; destroy requires CLOSED + zero records
```

- [x] **Step 2: State compatibility and provider rules**

Document that only `turbo-flow.inbox.record` version 1 is accepted, TurboDB must implement the same vtable, configured provider identity is singular, and an error never triggers memory fallback.

- [x] **Step 3: Verify docs against exported names**

Run: `rg.exe -n "turbo_flow_inbox_|turbo-flow.inbox.record" turbo_flow/include/turbo_flow_inbox.h docs/architecture/transport-independent-business-graph.md README.md`

Expected: every documented identifier exists in the public header.

### Task 3: Validate and publish the increment

**Files:**
- Modify: `tests/install_consumer/main.c`

**Interfaces:**
- Consumes: installed `TurboFlow::Graph` package and Task 1 header.
- Produces: installed-SDK compile/link coverage for the new ABI.

- [x] **Step 1: Add installed-consumer API checks**

```c
turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
if (turbo_flow_inbox_memory_create(&config, &inbox) != SALTS_OK) return 1;
if (turbo_flow_inbox_close(&inbox) != SALTS_OK) return 1;
if (turbo_flow_inbox_destroy(&inbox) != SALTS_OK) return 1;
```

- [x] **Step 2: Run release and Debug/ASan validation**

Run: `cmake --build --preset win-release-user --parallel && ctest --preset win-release-user --output-on-failure`

Run: `cmake --preset win-dev-user && cmake --build --preset win-dev-user --target test_flow_inbox && ctest --preset win-dev-user -R test_flow_inbox --output-on-failure`

Expected: release suite and focused Debug/ASan test pass.

- [x] **Step 3: Commit and update issue #118**

```text
feat(flow): add unified bounded intake inbox
```

Report the implemented acceptance subset and leave TurboDB persistence, config/provider host binding, Graph driver, and real two-protocol end-to-end coverage open.
