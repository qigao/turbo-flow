# Salts Foundation Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace TurboFlow's direct TurboUtils package dependency with the installed Salts and SaltsUtils packages without changing TurboFlow's public C API or runtime behavior.

**Architecture:** Keep the current TurboFlow Graph, adapter, TurboNet, TurboHttp, and TurboParser boundaries intact in this slice. Replace only the foundational package roots and imported targets: `TurboUtils::Core` becomes `Salts::Core`, `TurboUtils::STL` becomes `Salts::CSTL`, and `TurboUtils::TinyTest` becomes `Salts::TinyTest`; SaltsUtils supplies `Salts::DataBind` required by RulesForge.

**Tech Stack:** C11, CMake Presets, Salts Core/CSTL/TinyTest, SaltsUtils DataBind, TinyTest, MSVC/Ninja.

**Spec:** `turbo_flow/PRIMITIVE_GRAPH_ARCHITECTURE.md`

## Global Constraints

- Preserve all existing TurboFlow public headers, exported target names, DSL syntax, error codes, and runtime behavior.
- Resolve first-party packages only from profile-specific environment roots with `NO_DEFAULT_PATH`.
- Keep Debug, Release, Linux, and Android dependency roots isolated.
- Do not add compatibility aliases named `TurboUtils::*`; stale consumers must fail instead of silently binding the wrong runtime.
- Do not migrate TurboNet, TurboHttp, TurboParser, CFlow, FlowMQ, CHTTP, or CRPC in this slice.

---

### Task 1: Declare Salts package roots and package discovery

**Files:**
- Modify: `CMakeUserPresets.json`
- Modify: `CMakeLists.txt`
- Modify: `cmake/TurboFlowConfig.cmake.in`

**Interfaces:**
- Consumes: installed package roots `$env{PKG_ROOT}/salts/<profile>` and `$env{PKG_ROOT}/salts-utils/<profile>`.
- Produces: imported targets `Salts::Core`, `Salts::CSTL`, `Salts::TinyTest`, and `Salts::DataBind` in both build-tree and installed-consumer configurations.

- [ ] **Step 1: Add profile-specific roots**

For every host profile, replace `TURBOUTILS_ROOT` with:

```json
"SALTS_ROOT": "$env{PKG_ROOT}/salts/release",
"SALTS_UTILS_ROOT": "$env{PKG_ROOT}/salts-utils/release"
```

Use `debug` for debug profiles and `salts-android` / `salts-utils-android` for Android profiles. Add both roots to the matching runtime `PATH` or `LD_LIBRARY_PATH`.

- [ ] **Step 2: Make configure fail fast on missing Salts roots**

Change the root validation list in `CMakeLists.txt` to begin with:

```cmake
foreach(_turbo_flow_dependency_root IN ITEMS
        SALTS_ROOT SALTS_UTILS_ROOT TURBOPARSER_ROOT TURBONET_ROOT
        TURBOHTTP_ROOT RULES_FORGE_ROOT)
```

- [ ] **Step 3: Discover Salts from exact roots**

Replace the TurboUtils lookup with:

```cmake
find_package(Salts CONFIG REQUIRED
             PATHS "$ENV{SALTS_ROOT}" NO_DEFAULT_PATH)
find_package(SaltsUtils CONFIG REQUIRED
             PATHS "$ENV{SALTS_UTILS_ROOT}" NO_DEFAULT_PATH)
```

Place the same dependencies in `cmake/TurboFlowConfig.cmake.in` before loading `RulesForge`, because its exported target links `Salts::Core` and `Salts::DataBind`.

- [ ] **Step 4: Verify configure-time failure and success**

Run:

```powershell
cmake --list-presets
cmake --fresh --preset win-release-user
```

Expected: configure succeeds using only the preset's Salts roots; removing either root in a temporary command environment fails with the missing-root diagnostic.

- [ ] **Step 5: Commit**

```bash
git add CMakeUserPresets.json CMakeLists.txt cmake/TurboFlowConfig.cmake.in
git commit -m "build: migrate foundation package roots to Salts"
```

### Task 2: Replace live imported target references

**Files:**
- Modify: every non-generated `CMakeLists.txt` returned by `rg.exe -l 'TurboUtils::' --glob 'CMakeLists.txt' .`
- Modify: `cmake/CmakeUtils.cmake`
- Modify: `turbo_flow/CMakeLists.txt`

**Interfaces:**
- Consumes: targets established by Task 1.
- Produces: the same TurboFlow build and test targets with Salts-backed foundational symbols.

- [ ] **Step 1: Replace target mappings mechanically**

Apply exactly these mappings:

```text
TurboUtils::Core     -> Salts::Core
TurboUtils::STL      -> Salts::CSTL
TurboUtils::TinyTest -> Salts::TinyTest
```

- [ ] **Step 2: Remove the obsolete DataBind compatibility alias**

Delete the `TurboUtils::DataBind` alias block from `turbo_flow/CMakeLists.txt`. RulesForge's installed contract consumes `Salts::DataBind` from SaltsUtils.

- [ ] **Step 3: Update runtime test-path discovery**

In `cmake/CmakeUtils.cmake`, replace `TurboUtils::Core` with `Salts::Core` in the runtime target list. Preserve the existing TurboParser, TurboNet, and TurboHttp entries.

- [ ] **Step 4: Prove no live build references remain**

Run:

```powershell
rg.exe -n 'TurboUtils::|TURBOUTILS_ROOT|find_package\(TurboUtils|find_dependency\(TurboUtils' --glob '!build/**' --glob '!docs/superpowers/plans/**' .
```

Expected: no matches in active CMake or preset files.

- [ ] **Step 5: Commit**

```bash
git add -- '*CMakeLists.txt' cmake/CmakeUtils.cmake
git commit -m "build: link TurboFlow targets against Salts"
```

### Task 3: Synchronize repository guidance and architecture facts

**Files:**
- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `turbo_flow/ARCHITECTURE.md`
- Modify: `schedule/README.md`
- Modify: `turbo_flow/include/turbo_flow_export.h`

**Interfaces:**
- Consumes: the dependency boundary implemented by Tasks 1-2.
- Produces: documentation that names Salts as the foundational owner without claiming CFlow/FlowMQ/CHTTP/CRPC integration is already implemented.

- [ ] **Step 1: Update current dependency names**

Replace factual live references to `TurboUtils::Core`, `TurboUtils::STL`, and `TurboUtils::Cron` with the actual Salts targets. Do not rewrite historical plan files.

- [ ] **Step 2: State the migration boundary**

Add a short README statement that this slice changes the foundational package only; TurboNet/TurboHttp adapters remain compatibility components until their dedicated Salts CNet/CHTTP/CRPC migrations.

- [ ] **Step 3: Check documentation claims**

Run:

```powershell
rg.exe -n 'TurboUtils::|TURBOUTILS_ROOT' AGENTS.md README.md turbo_flow/ARCHITECTURE.md schedule/README.md turbo_flow/include/turbo_flow_export.h
```

Expected: no stale live dependency claims.

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md README.md turbo_flow/ARCHITECTURE.md schedule/README.md turbo_flow/include/turbo_flow_export.h
git commit -m "docs: record Salts foundation boundary"
```

### Task 4: Build, test, and installed-consumer verification

**Files:**
- Verify: `CMakeUserPresets.json`
- Verify: generated build tree and installed package outside source control.

**Interfaces:**
- Consumes: all preceding tasks.
- Produces: reproducible evidence that the dependency migration preserves behavior and exports usable package metadata.

- [ ] **Step 1: Configure in the MSVC environment**

Run from `VsDevCmd.bat`:

```powershell
cmake --fresh --preset win-release-user
```

Expected: configure resolves Salts and SaltsUtils from the Release profile roots.

- [ ] **Step 2: Build the smallest foundational targets**

```powershell
cmake --build --preset win-release-user --target turbo_flow_config turbo_flow_graph turbo_flow
```

Expected: all three targets build successfully.

- [ ] **Step 3: Run core regression tests**

```powershell
ctest --preset win-release-user -R "test_(turbo_flow|flow_domain|flow_config|flow_compat_boundary)$" --output-on-failure
```

Expected: every selected test passes.

- [ ] **Step 4: Expand to the complete configured suite**

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
```

Expected: build and test suite pass without loading TurboUtils binaries.

- [ ] **Step 5: Verify installation**

```powershell
cmake --build --preset install-win-release-user
```

Inspect the installed `TurboFlowConfig.cmake`; it must discover `Salts` and `SaltsUtils`, and must not create `TurboUtils::*` aliases.

- [ ] **Step 6: Commit**

```bash
git add CMakeUserPresets.json CMakeLists.txt cmake turbo_flow io ingress observe schedule security sink codec README.md AGENTS.md
git commit -m "build: complete Salts foundation migration"
```
