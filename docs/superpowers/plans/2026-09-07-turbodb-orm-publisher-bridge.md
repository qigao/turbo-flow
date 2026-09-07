# TurboDb ORM Publisher Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `TurboFlow::TurboDbAdapter` that converts native TurboDb ORM typed Publishers into managed `turbo_flow_msg_t` Publishers without materialization or format conversion.

**Architecture:** `Orm::C` remains the cursor and transaction owner. A thin Publisher decorator allocates one CMeta row only when CFlow requests a value, forwards WAIT/waker/cancel/terminal behavior unchanged, and attaches the completed row as an owning TurboFlow message projection. Query and command helpers only open the corresponding native ORM Publisher and pass it to the same decorator.

**Tech Stack:** C11, TurboDb `Orm::C`, Salts CMeta/CFlow Reactive, TurboFlow Graph, TinyTest, CMake presets/package components.

**Spec:** https://github.com/qigao/turbo-flow/issues/41

## Global Constraints

- No TurboParser, TurboNet, TurboHttp, compatibility target, C fallback, CMake fallback, synchronous ORM materialization, or JSON intermediary.
- `TurboFlow::Graph` must not link `Orm::C`; only the optional `TurboFlow::TurboDbAdapter` target may depend on TurboDb.
- Enabling the adapter requires a non-empty, existing `TURBODB_ROOT` and `find_package(Orm CONFIG REQUIRED ... NO_DEFAULT_PATH)`.
- Publisher ownership moves only after every adapter configuration, schema identity, CMeta type, trait, size, alignment, and message-ID precondition passes.
- Query, connection, transaction, row metadata, and schema lifetimes remain explicit; the adapter does not invent a second database state machine.
- Every growing or retained allocation is bounded by ORM limits, one row per Publisher resume, and TurboFlow runtime demand/in-flight capacity.
- Tests follow RED → GREEN → REFACTOR and use `cmake --preset win-dev-user` / `ctest --preset win-dev-user` from `VsDevCmd.bat`.

---

### Task 1: Optional build and package boundary

**Files:**

- Modify: `CMakeOptions.cmake`
- Modify: `CMakeUserPresets.json`
- Modify: `CMakeLists.txt`
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Create: `io/turbodb/CMakeLists.txt`
- Create: `io/turbodb/tests/CMakeLists.txt`
- Create: `io/turbodb/tests/test_turbodb_adapter.c`

**Interfaces:**

- Consumes: installed `Orm::C`, `TurboFlow::Graph`, `Salts::CFlow`, `Salts::TinyTest`.
- Produces: build target `tf_turbodb_adapter`, alias/export `TurboFlow::TurboDbAdapter`, and test target `test_turbodb_adapter`.

- [ ] **Step 1: Add the adapter test target before its implementation exists**

  Add `TURBO_FLOW_BUILD_TURBODB_ADAPTER` with default `OFF`. When `ON`, require and verify `TURBODB_ROOT`, discover `Orm` only under that root, add `io/turbodb`, export the target, and include TurboDb's runtime directory. Enable it in desktop user presets with profile-matching roots. Add a TinyTest source that includes `<turbo_flow_turbodb.h>` and calls the wished-for API.

- [ ] **Step 2: Configure and build the test to verify RED**

  Run from `VsDevCmd.bat`:

  ```powershell
  cmake --fresh --preset win-dev-user
  cmake --build --preset win-dev-user --target test_turbodb_adapter
  ```

  Expected: configure succeeds against `$env{PKG_ROOT}/turbodb/debug`; compilation fails because `turbo_flow_turbodb.h` or its declared functions do not exist.

- [ ] **Step 3: Commit the verified build boundary with the following behavior**

  The source-tree package must reject an enabled adapter without `TURBODB_ROOT`; a package consumer requesting `TurboDbAdapter` must do the same. A build with the option disabled must never discover `Orm`.

---

### Task 2: Typed Publisher to managed message decorator

**Files:**

- Create: `io/turbodb/include/turbo_flow_turbodb.h`
- Create: `io/turbodb/src/turbo_flow_turbodb.c`
- Modify: `io/turbodb/tests/test_turbodb_adapter.c`

**Interfaces:**

- Consumes:

  ```c
  cflow_publisher *typed_publisher;
  const turbo_flow_turbodb_source_config_t *config;
  ```

- Produces:

  ```c
  #define TURBO_FLOW_TURBODB_API_VERSION 1u

  typedef struct turbo_flow_turbodb_source_config_s {
    size_t size;
    uint32_t version;
    const turbo_flow_data_schema_t *projection_schema;
    uint64_t first_message_id;
    uint32_t message_type;
    uint32_t message_flags;
  } turbo_flow_turbodb_source_config_t;

  int turbo_flow_turbodb_publisher_wrap(
      cflow_publisher *typed_publisher,
      const turbo_flow_turbodb_source_config_t *config,
      cflow_publisher *message_publisher);
  ```

  Success moves and clears `typed_publisher`; failure leaves it live and leaves `message_publisher` empty.

- [ ] **Step 1: Write focused failing tests for ownership and validation**

  Use real CFlow test Publishers with literal CMeta descriptors. Assert that a valid trivial row Publisher moves only on success, emits message IDs `41` then `42`, preserves configured type/flags, and exposes the row through `turbo_flow_msg_projection()`. Assert `SALTS_EINVAL` for null/short/wrong-version config, non-DATA schema, empty schema strings, schema projection type mismatch, live output Publisher, invalid row descriptor, non-power-of-two alignment, zero first ID, and missing copy/destroy traits. For every rejection assert the input Publisher is still valid.

- [ ] **Step 2: Run the focused test and verify RED**

  ```powershell
  cmake --build --preset win-dev-user --target test_turbodb_adapter
  ctest --preset win-dev-user -R '^test_turbodb_adapter$' --output-on-failure
  ```

  Expected: link failure for `turbo_flow_turbodb_publisher_wrap`.

- [ ] **Step 3: Implement minimal decorator storage and validation**

  Validate all inputs before moving ownership. Allocate one aligned row block per resume with checked `size + align + header` arithmetic. The projection destroy callback invokes non-trivial CMeta destroy and frees the original allocation. The projection clone callback allocates aligned storage and uses trivial `memcpy` or `copy_construct`; it returns failure without publishing a partial clone.

- [ ] **Step 4: Implement Reactive forwarding**

  The decorator output type is `turbo_flow_message_type()`. `resume()` allocates empty row storage and calls the inner Publisher exactly once. WAIT, DONE, and ERROR free the still-empty block and preserve the original step/waitable/error. VALUE initializes a message and binds the owning projection. `bind_terminal_waker`, `cancel`, `poll_terminal`, and `destroy` forward to the inner Publisher. A local bind/allocation/ID error cancels the inner Publisher and becomes the decorator's unique ERROR terminal.

- [ ] **Step 5: Run GREEN and add WAIT/cancel tests**

  A fake Publisher first returns a real armed WAIT, then a row. Assert no row is requested before downstream demand, the same waitable wakes the TurboFlow run, cancellation reaches the inner Publisher once, and an invalid waitable becomes a failed run rather than polling or fallback.

- [ ] **Step 6: Add managed row and ID-boundary tests**

  Use a row containing owned `tstr` with literal copy/destroy counters. Clone the emitted `turbo_flow_msg_t`, destroy source and clone, and assert two independent strings and two destroys. Emit `UINT64_MAX`, then provide another row and assert the first row remains delivered while the next resume terminates with a stable ID-exhaustion error.

- [ ] **Step 7: Run focused tests and commit**

  ```powershell
  cmake --build --preset win-dev-user --target test_turbodb_adapter
  ctest --preset win-dev-user -R '^test_turbodb_adapter$' --output-on-failure
  ```

---

### Task 3: Native ORM query and command helpers

**Files:**

- Modify: `io/turbodb/include/turbo_flow_turbodb.h`
- Modify: `io/turbodb/src/turbo_flow_turbodb.c`
- Modify: `io/turbodb/tests/test_turbodb_adapter.c`

**Interfaces:**

- Produces:

  ```c
  int turbo_flow_turbodb_query_open(
      orm_query_t *query, const orm_flow_config_t *orm_config,
      const turbo_flow_turbodb_source_config_t *source_config,
      cflow_publisher *message_publisher, orm_error_t *orm_error);

  int turbo_flow_turbodb_query_open_in_transaction(
      orm_query_t *query, orm_transaction_t *transaction,
      const orm_flow_config_t *orm_config,
      const turbo_flow_turbodb_source_config_t *source_config,
      cflow_publisher *message_publisher, orm_error_t *orm_error);

  int turbo_flow_turbodb_command_open(
      orm_query_t *query,
      const turbo_flow_turbodb_source_config_t *source_config,
      cflow_publisher *message_publisher, orm_error_t *orm_error);

  int turbo_flow_turbodb_command_open_in_transaction(
      orm_query_t *query, orm_transaction_t *transaction,
      const turbo_flow_turbodb_source_config_t *source_config,
      cflow_publisher *message_publisher, orm_error_t *orm_error);
  ```

  ORM failures remain in caller-owned `orm_error`; return values map to stable Salts categories. If wrapping fails after ORM open, the helper destroys the inner Publisher and leaves output empty.

- [ ] **Step 1: Write a failing SQLite row-through-Graph test**

  Open `:memory:`, create a query yielding literal rows `(7,19)` and `(11,29)`, open the adapter Publisher, and pass it to `turbo_flow_run_open()`. Assert zero stage calls before demand, one exact projected row after each `turbo_flow_run_request(run, 1)`, and unique completion after the second row.

- [ ] **Step 2: Verify RED, implement query helpers, verify GREEN**

  Call the exact native `orm_query_open_flow` or `_in_transaction` API, then call `turbo_flow_turbodb_publisher_wrap`. No synchronous execute/materialization API is allowed.

- [ ] **Step 3: Write failing command and limit tests**

  Execute `create table` and `insert` through the command helper and assert the typed `orm_command_result_t.affected_rows` projection. Configure `max_result_rows=1` for a two-row SQLite query and assert the run emits one row then fails with the native limit diagnostic.

- [ ] **Step 4: Implement command helpers and verify GREEN**

  Call `orm_query_open_command_flow` or its transaction variant, wrap the resulting command Publisher, and preserve the command Publisher's own output CMeta type.

- [ ] **Step 5: Add transaction lifecycle test**

  Open a transaction query Publisher, open a TurboFlow run, and assert `orm_transaction_commit()` returns `ORM_STATUS_BUSY` while the Subscription is live. Close/cancel the run, then assert commit or rollback succeeds before destroying the transaction and connection.

- [ ] **Step 6: Run focused and adjacent suites, then commit**

  ```powershell
  cmake --build --preset win-dev-user --target test_turbodb_adapter test_flow_run
  ctest --preset win-dev-user -R '^(test_turbodb_adapter|test_flow_run)$' --output-on-failure
  ```

---

### Task 4: Installed component contract and documentation

**Files:**

- Modify: `CMakeLists.txt`
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Modify: `tests/install_consumer/CMakeLists.txt`
- Modify: `tests/install_consumer/component/CMakeLists.txt`
- Modify: `tests/install_consumer/component/main.c`
- Modify: `tests/install_consumer/run.cmake`
- Create: `io/turbodb/ADR_TURBODB_ORM_SOURCE.md`

**Interfaces:**

- Consumes: installed `TurboFlowConfig.cmake`, `TURBODB_ROOT`, `OrmConfig.cmake`.
- Produces: component-aware dependency discovery and a C/C++-compatible installed header/target.

- [ ] **Step 1: Add failing installed-component consumer**

  Request only `TurboDbAdapter`, include `turbo_flow_turbodb.h`, initialize the versioned config, and link `TurboFlow::TurboDbAdapter`. Run the existing install-consumer harness and verify failure before package metadata is complete.

- [ ] **Step 2: Implement strict package discovery**

  Generate whether the adapter was built into `TurboFlowConfig.cmake`. Only a request set containing `TurboDbAdapter` may require `TURBODB_ROOT` and `Orm::C`. Verify the resolved `Orm_DIR` stays below the declared root. Add a negative configure test with `TURBODB_ROOT` unset and assert the explicit root diagnostic. Keep the existing Config-only consumer successful with TurboDb disabled from package discovery.

- [ ] **Step 3: Document ownership and operation**

  Record the source-of-truth boundaries, one-row allocation bound, demand/WAIT sequence, projection lifetime, query/connection/transaction/schema borrowing, exact error propagation, cancellation, shutdown order, and the fact that this adapter provides no materialization or fallback.

- [ ] **Step 4: Verify Debug/ASan, Release, and core-only configuration**

  ```powershell
  cmake --fresh --preset win-dev-user
  cmake --build --preset win-dev-user --parallel
  ctest --preset win-dev-user --output-on-failure
  cmake --fresh --preset win-release-user
  cmake --build --preset win-release-user --parallel
  ctest --preset win-release-user --output-on-failure
  ```

  In a separate configure command explicitly set `TURBO_FLOW_BUILD_TURBODB_ADAPTER=OFF`, unset `TURBODB_ROOT`, disable `Orm` discovery, and build `TurboFlow::Graph`'s concrete target. Expected: configure/build succeeds without TurboDb.

- [ ] **Step 5: Self-review and commit**

  Check every API path for pre-move validation, exactly one destroy, checked allocation arithmetic, error text lifetime, waitable forwarding, and output cleanup. Confirm `.codegraph/` and build trees are not staged.

