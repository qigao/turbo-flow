# Salts Foundation Migration and Legacy Stack Removal Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove TurboFlow's obsolete TurboUtils, TurboParser, TurboNet, and TurboHttp build/runtime boundaries so the remaining graph, protocol, codec, schedule, security, and observability modules build only on installed Salts and SaltsUtils packages.

**Architecture:** Salts becomes the single foundation package, including its native JSON, YAML, and CSV parsers, while SaltsUtils supplies DataBind and Cron for retained code. The CoroNet/TurboHttp implementations are deleted rather than hidden behind aliases, feature flags, or fallback discovery. Later issues reintroduce transport and HTTP capabilities through CNet/CHTTP with new ownership semantics.

**Tech Stack:** C11, CMake presets, Salts Core/CSTL/JsonParser/CYamlJsonAdapter/CsvParser/TinyTest, SaltsUtils DataBind/Cron, RulesForge.

---

### Task 1: Pin the failing legacy dependency baseline

**Files:**
- Inspect: `CMakeLists.txt`
- Inspect: `CMakeUserPresets.json`

**Step 1: Run the release configure**

Run: `cmake --fresh --preset win-release-user` from a Visual Studio developer environment.

**Step 2: Verify the expected failure**

Expected: configure fails because `TURBOUTILS_ROOT` points at a removed package installation.

### Task 2: Replace foundation package discovery and exported dependencies

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `CMakeUserPresets.json`
- Modify: `cmake/TurboFlowConfig.cmake.in`
- Modify: `cmake/CmakeUtils.cmake`

**Step 1: Add exact Salts package roots**

Require `SALTS_ROOT` and `SALTS_UTILS_ROOT`, discover both with `CONFIG REQUIRED`, and keep `RULES_FORGE_ROOT`. Remove all discovery, runtime-copy, and generated-package references to the four legacy packages.

**Step 2: Preserve deterministic package lookup**

Use `NO_DEFAULT_PATH` for repository-owned packages and add only the three installed package roots to runtime synchronization.

**Step 3: Make the consumer package truthful**

Export `find_dependency(Salts CONFIG REQUIRED)` and `find_dependency(SaltsUtils CONFIG REQUIRED)`. Do not create compatibility targets or conditionally search for legacy packages.

### Task 3: Migrate retained targets to canonical Salts targets

**Files:**
- Modify: all retained `CMakeLists.txt` files referencing `TurboUtils::*` or `TurboParser::*`
- Modify: `turbo_flow/CMakeLists.txt`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Step 1: Replace direct target dependencies**

Apply these exact mappings:

- `TurboUtils::Core` -> `Salts::Core`
- `TurboUtils::STL` -> `Salts::CSTL`
- `TurboUtils::TinyTest` -> `Salts::TinyTest`
- `TurboParser::DataBind` -> `Salts::DataBind`
- `TurboParser::Parser` -> the exact native Salts parser target used by each module
- `TurboParser::Cron` -> `Salts::Cron`

Retained JSON, YAML, and CSV call sites use `Salts::JsonParser`,
`Salts::CYamlJsonAdapter`, and `Salts::CsvParser` directly. No TurboParser
compatibility header or wrapper remains.

**Step 2: Remove the DataBind compatibility alias**

Delete the `TurboUtils::DataBind` imported target and link RulesForge after SaltsUtils has defined `Salts::DataBind`.

**Step 3: Verify no retained CMake file names a legacy package or target**

Run: `rg.exe -n "Turbo(Utils|Parser|Net|Http)|TURBO(UTILS|PARSER|NET|HTTP)" --glob "CMakeLists.txt" --glob "*.cmake*" --glob "*.json" .`

Expected: no matches.

### Task 4: Delete legacy transport and HTTP implementations

**Files:**
- Delete: `io/`
- Delete: `ingress/protocol/coronet/`
- Delete: CoroNet-only protocol tests and benchmarks
- Modify: `ingress/protocol/CMakeLists.txt`
- Modify: `ingress/protocol/tests/CMakeLists.txt`
- Modify: `CMakeOptions.cmake`
- Modify: `CMakeLists.txt`

**Step 1: Remove target construction and installation**

Remove `io` and CoroNet subdirectories, legacy export targets, component flags, soak configuration, and runtime path handling. Do not leave disabled targets or placeholder implementations.

**Step 2: Remove tests that exercise deleted code**

Delete only tests/benchmarks whose production target was removed. Retain graph protocol, registry, plugin, business-protocol, codec, schedule, security, and observability coverage.

**Step 3: Verify there are no code/config references**

Run repository searches for CoroNet, TurboNet, TurboHttp, and removed TurboFlow target names. Historical implementation plans may retain archival text; active build files and current product documentation may not.

### Task 5: Synchronize current architecture documentation

**Files:**
- Modify: `README.md`
- Modify: `turbo_flow/ARCHITECTURE.md`
- Modify: `turbo_flow/ADR_GRAPH_PRODUCT_BOUNDARY.md`
- Modify: other current non-historical documents found by repository search

**Step 1: Remove claims that deleted modules are currently available**

Describe the repository as graph/protocol/codec/schedule/security/observe foundation only. Link transport/HTTP restoration to issues #5, #6, and #7 without retaining fallback instructions.

**Step 2: Update dependency and ownership language**

Use Salts, CMeta, CFlow, CSTL, CNet, and CHTTP terminology consistently; explicitly state that CNet/CHTTP adapters are not present until their tracked issues land.

### Task 6: Configure, build, test, install, and consume

**Files:**
- Verify: `build/Msvc-Release/`
- Verify: installed TurboFlow package

**Step 1: Fresh configure**

Run: `cmake --fresh --preset win-release-user`

Expected: success with only Salts, SaltsUtils, RulesForge, and vcpkg dependencies.

**Step 2: Build the retained product**

Run: `cmake --build --preset win-release-user --parallel`

Expected: all retained production and test targets build.

**Step 3: Run tests**

Run: `ctest --preset win-release-user --output-on-failure`

Expected: all registered tests pass.

**Step 4: Install and validate downstream package loading**

Run the repository install target, then configure a minimal consumer that calls `find_package(TurboFlow CONFIG REQUIRED)` and links `TurboFlow::Flow`.

Expected: configure and link succeed without any legacy package root or target.

**Step 5: Review the final diff and commit**

Confirm deleted APIs are limited to the explicitly authorized legacy modules, retained public graph behavior is unchanged, and no generated `.codegraph` content is staged.
