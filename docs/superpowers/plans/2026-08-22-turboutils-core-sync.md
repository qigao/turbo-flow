# TurboUtils Core API Sync Implementation Plan

> **For Codex:** Execute this plan inline in the user-selected working tree. Preserve the pre-existing `CMakeUserPresets.json` and `presets/Compilers.json` changes.

**Goal:** Migrate TurboFlow from removed TurboUtils compatibility names to the current exported C API, string/view names, and `turboutils::stl` containers without changing runtime behavior.

**Architecture:** Keep TurboFlow's existing shared-library boundaries and introduce one project-owned visibility header. Keep container adaptation private to TurboFlow: raw byte containers preserve the existing trivial-storage semantics and translate positive `turbo_stl_status` values back to the repository's established `TURBO_E*` contract.

**Tech Stack:** C11, CMake, TurboUtils::Core, TurboUtils::STL, TinyTest, MSVC presets.

---

### Task 1: Own the public export contract

**Files:**
- Create: `turbo_flow/include/turbo_flow_export.h`
- Modify: TurboFlow public headers containing `CXX_C_API`
- Modify: component `CMakeLists.txt` files defining `SHARED_CXX`

1. Add Windows/non-Windows visibility handling and `TURBO_FLOW_C_API` C linkage.
2. Replace removed `CXX_C_API` annotations with `TURBO_FLOW_C_API`.
3. Replace build-side `SHARED_CXX` with `TURBO_FLOW_BUILD` and install the export header.
4. Build `turbo_flow` to expose the next compatibility failures.

### Task 2: Migrate strings and views

**Files:**
- Modify: production headers/sources, tests, examples, and benchmarks using legacy TurboUtils string names

1. Replace `tstr_t` with `tstr` and `tstr_v` with `vstr`.
2. Replace `tstr_v_*` calls and the removed `turbo_str_view.h` include with current view APIs.
3. Scan to ensure removed names no longer occur in buildable code.
4. Build the core target and repair signature-level incompatibilities.

### Task 3: Move containers to TurboUtils::STL

**Files:**
- Create: `turbo_flow/include/turbo_flow_stl_error_internal.h`
- Modify: sources/headers that include old container headers or initialize raw containers
- Modify: owning component `CMakeLists.txt` files

1. Add a private raw-container adapter with status translation, alignment, zero-init, and explicit limits.
2. Move includes to `<turbostl/...>` and rename set APIs to hash-set APIs where applicable.
3. Replace legacy size-only initialization with aligned `*_init_bytes` initialization.
4. Replace the seven old typed facades in discovery/Redis with private raw facades that preserve their call sites.
5. Link `TurboUtils::STL` only to targets that compile container-using code.

### Task 4: Restore tests and validate packaging

**Files:**
- Modify: tests only if current TinyTest reports removed assertion helpers
- Modify: CMake target dependencies only when build evidence requires it

1. Build all configured targets and fix only migration-caused compiler/linker errors.
2. Run the closest core/component tests, then the full configured CTest suite.
3. Run fresh configure/build verification and legacy-symbol scans.
4. Inspect the diff and confirm pre-existing unrelated changes remain untouched.
