# Task 1 report — transactional schema catalog

## Status

Implemented the standalone schema catalog only. No operation factory, generation binding, or engine capability was added.

## Implementation

- Added public ABI 1.4 schema wrapper/catalog declarations and snapshot query in `turbo_flow_plugin_operation.h`.
- Extended the root capability bits, registration callback table, and bounded host configuration with `schema_capacity`.
- Preserved old configuration prefixes: sizes through the complete 1.3 prefix copy no schema tail and therefore normalize to zero schema capacity; partial 1.4 tails are not consumed.
- Added a bounded CSTL schema wrapper vec to the existing PluginHost fact source.
- Added descriptor, version, storage type, stable identity, duplicate identity/version, and capacity admission checks.
- Propagated `registration.first_error`, included schemas in capability reconciliation, and rolled back the schema tail with the existing module transaction.
- Copied immutable schema wrappers into snapshots, retained all existing module leases, and destroyed the copied vec with the snapshot/host.
- Added real DLL fixtures, C/C++ header consumption, semantic cross-DLL CMeta equality with deliberately different storage descriptor addresses, rollback, swallowed-error, ABI, old-prefix, capacity, lease, and immutable snapshot tests.
- Added the header to the installed header list and `Salts::CMeta` to the public PluginHost dependency closure; updated the install consumer ABI assertion.

## TDD evidence

### Baseline

Command:

`cmake --build --preset win-dev-user --target test_flow_plugin_host test_flow_plugin_generation && ctest --preset win-dev-user -R test_flow_plugin --output-on-failure`

Result: 2/2 existing plugin tests passed before edits.

### RED

The first configure/build attempt after adding the new fixture/test was blocked before compilation by a stale cache entry:

`FindTools: re2c executable does not exist: C:/Users/lockg/scoop/shims/re2c.exe`

After invalidating only `RE2C_EXECUTABLE` with `cmake --preset win-dev-user -U RE2C_EXECUTABLE`, CMake resolved the existing `C:/tools/cpp-dev/bin/re2c.exe`. The first actual build then failed in the new test/fixture at the wished-for CMeta type expression (`cmeta_type(int)` was not provided by the installed CMeta surface); this was a test-source API spelling error, not a production behavior RED, and was corrected to the official `CMETA_TYPEOF(int)` surface. The first executable schema test then failed 1/4 at the immutable-snapshot case because the second fixture accidentally reused the first fixture's stable schema identity. Assigning a distinct stable ID made the intended old/new snapshot count assertions meaningful.

This history is reported explicitly: the requested initial missing-header RED was not observed because the stale tool cache stopped configure first, and production/header edits were already present by the time the cache was repaired.

### GREEN and regressions

All Windows commands ran through the required `VsDevCmd.bat` environment.

- Debug focused build: `test_flow_plugin_schema test_flow_plugin_host test_flow_plugin_generation` — passed.
- Debug schema repeat: `ctest --preset win-dev-user -R ^test_flow_plugin_schema$ --repeat until-fail:5 --output-on-failure` — 5/5 passed.
- Debug plugin regression: `ctest --preset win-dev-user -R test_flow_plugin --output-on-failure` — 3/3 passed.
- Release focused build: same three targets with `win-release-user` — passed.
- Release schema repeat: 5/5 passed.
- Release plugin regression: 3/3 passed.
- Debug full suite: initial run 52/53 exposed the install consumer's stale literal ABI-minor assertion (`3u`); after updating it to `4u` and checking nonzero installed `schema_capacity`, rerun passed 53/53.
- C++ public-header fixture compiled in both Debug and Release.
- Install manifest contains `include/turbo_flow_plugin_operation.h`; the full install consumer passed.
- `git diff --check` passed.

## Files

- `turbo_flow/include/turbo_flow_plugin_operation.h` (new)
- `turbo_flow/include/turbo_flow_plugin.h`
- `turbo_flow/src/flow_plugin.c`
- `turbo_flow/CMakeLists.txt`
- `turbo_flow/tests/plugin_schema_fixture.c` (new)
- `turbo_flow/tests/test_flow_plugin_schema.c` (new)
- `turbo_flow/tests/plugin_operation_header_cpp.cpp` (new, approved ledger ruling)
- `turbo_flow/tests/CMakeLists.txt`
- `tests/install_consumer/main.c`

## Concerns

- The requested missing-header RED could not be captured for the reason documented above; all subsequent behavioral failures and GREEN runs are recorded accurately.
- This commit intentionally exposes metadata only. It does not claim completion of TurboFlow #73's operation factory, generation, or engine work.
