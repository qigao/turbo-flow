# Atomic Managed Async Terminal Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one additive, size/versioned C API that atomically registers an asynchronous terminal adapter and exactly one managed Sink boundary for the same owner context.

**Architecture:** The new aggregate is an explicit orchestration contract over the existing adapter and canonical resource registries. Core validates the aggregate, stages the async-terminal adapter with its shutdown callback suppressed, then registers the managed resource; any resource-stage failure removes only the staging adapter and never transfers owner lifetime. The shutdown callback is restored as the final ownership-transfer commit point. Existing registration structures, symbols, ownership rules, and managed-boundary discovery remain unchanged, and no adapter role or legacy snapshot is used to infer a boundary.

**Tech Stack:** C11, TurboFlow/CFlow registries, CMeta-style size/version contracts, CSTL vectors, TinyTest, CMake Presets.

**Spec:** [GitHub issue #54](https://github.com/qigao/turbo-flow/issues/54) and `docs/MANAGED_BOUNDARIES.md`.

## Global Constraints

- Preserve the ABI and behavior of all existing adapter/resource registration APIs.
- Keep one explicit owner context for adapter callbacks and managed-boundary callbacks.
- Fail fast on malformed size/version, missing callbacks, conflicting synchronous consume callbacks, duplicates, owner errors, or allocation failures.
- On failure, leave pre-existing adapter/resource registrations unchanged and retain caller ownership of the context.
- Do not add fallback, implicit capability inference, a second resource registry, or an unregister API.
- Keep `.codegraph/` and generated build artifacts out of commits.

---

## Task 1: Define the public aggregate and prove RED

**Files:**

- Modify: `turbo_flow/include/turbo_flow.h`
- Create: `turbo_flow/tests/test_flow_managed_async_terminal.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

- [x] Add `TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_API_VERSION`.
- [x] Add `turbo_flow_managed_async_terminal_registration_t` with `size`, `version`, explicit adapter/boundary contracts, and one `ctx`.
- [x] Add `TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT` and declare `turbo_flow_register_managed_async_terminal_adapter()`.
- [x] Write focused tests that require successful dual registration and rejection of invalid outer ABI/required fields.
- [x] Build the focused target and record the expected linker/behavior failure before implementing core.

The public shape is:

```c
typedef struct turbo_flow_managed_async_terminal_registration_s {
  size_t size;
  uint32_t version;
  const char *adapter_name;
  const turbo_flow_adapter_ops_t *adapter_ops;
  const turbo_flow_async_terminal_adapter_ops_t *async_ops;
  const turbo_flow_adapter_schema_t *schema;
  const char *owner_name;
  const turbo_flow_managed_boundary_provider_ops_t *boundary_ops;
  void *ctx;
} turbo_flow_managed_async_terminal_registration_t;

int turbo_flow_register_managed_async_terminal_adapter(
    turbo_flow_t *flow,
    const turbo_flow_managed_async_terminal_registration_t *registration);
```

## Task 2: Implement transactional registration and rollback

**Files:**

- Modify: `turbo_flow/src/flow_core.c`
- Modify: `turbo_flow/tests/test_flow_managed_async_terminal.c`

- [x] Add private tail-rollback helpers for adapter and resource registrations and reuse them in existing adjacent paths.
- [x] Validate the aggregate and nested async/boundary contracts before invoking owner callbacks.
- [x] Record both registry sizes and stage the async-terminal adapter with a copied ops table whose shutdown callback is suppressed.
- [x] Register the managed boundary, then restore shutdown only after both registrations validate; on failure remove only entries appended since the recorded sizes and preserve the original error code.
- [x] Test malformed adapter/schema, duplicate adapter, duplicate resource UID, metadata/descriptor errors, and resource failure after adapter staging.
- [x] For each failure, prove managed and canonical resource counts plus adapter lookup match their pre-call values; prove no shutdown callback fires on failed ownership transfer.
- [x] Test successful compile/start/stop, an attached async-terminal contract, and exactly one managed boundary using the shared context.

State ownership and failure ordering:

1. Before success, `ctx` remains caller-owned.
2. Adapter allocation and schema-copy failures occur before owner boundary callbacks.
3. The staging adapter has no shutdown callback, so resource callback, validation, duplicate, or allocation failures can remove it without consuming caller ownership.
4. Resource rollback performs no owner callback and restores the prior vector size.
5. Restoring the staged adapter's shutdown callback is the ownership-transfer commit point; success exposes both registrations until normal Flow registry teardown.

## Task 3: Verify installed ABI surface and document the decision

**Files:**

- Modify: `tests/install_consumer/main.c`
- Modify: `docs/MANAGED_BOUNDARIES.md`

- [x] Reference the initializer, version, and function symbol from installed C and C++ headers.
- [x] Document background, alternatives, chosen commit order, ownership/error semantics, performance cost, migration path for CNet sinks, and rollback/removal path.
- [x] Run the focused test, managed-boundary and async-terminal adjacent tests, install-consumer tests, all Debug/ASan tests, and the unaffected Release suite through repository presets.
- [ ] Re-run the complete Release suite after the installed TurboDB package profile conflict tracked by `qigao/turbodb#24` is resolved.
- [x] Inspect `git diff --check`, changed files, and `.codegraph/` exclusion before review.
- [x] Request independent code review and resolve all HIGH/MED/LOW findings.
- [ ] Open a PR linked to #54, update parent #28, and merge only after required checks remain green.

Expected complexity is O(R) for existing resource UID validation and O(A) for adapter-name validation, matching current registry behavior. The API is setup-path only; it adds no dispatch-path allocation or branch.
