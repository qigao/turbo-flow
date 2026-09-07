# Component-aware Package Dependencies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the installed TurboFlow package resolve only the dependencies required by requested components, while preserving the existing no-component consumer and rejecting unknown or incomplete component requests without legacy compatibility fallback.

**Architecture:** Keep one exported `TurboFlowTargets.cmake` and make `TurboFlowConfig.cmake` the dependency-routing boundary. Every installed target remains available, but SaltsUtils and RulesForge are discovered only when a requested component's public or imported link closure needs them; Salts remains mandatory for every supported component. Installation consumers exercise the generated package rather than inspecting template text.

**Tech Stack:** CMake package config helpers, CTest, MSVC/Ninja presets

**Spec:** https://github.com/qigao/turbo-flow/issues/10, refined by the repository rule that TurboNet, TurboHttp, CoroNet, and compatibility fallback must not return.

## Global Constraints

- Do not add TurboNet, TurboHttp, CoroNet, TurboParser, compatibility targets, aliases, feature flags, or fallback discovery.
- First-party packages remain exact-root dependencies in the producer build.
- Installed consumers must provide each required first-party `*_ROOT`; cached `*_DIR`, `CMAKE_PREFIX_PATH`, registry, and system locations are not fallback sources.
- Existing `find_package(TurboFlow CONFIG REQUIRED)` consumers keep resolving the full installed package dependency set.
- Requested components fail fast when unknown or when a required dependency is unavailable.
- The Config-only consumer must configure, build, and run while SaltsUtils and RulesForge discovery is explicitly disabled.
- This slice does not invent CNet, CHTTP, or TurboDb components before their owning targets exist.
- CMake tool discovery must also fail during configure; missing `re2c`, the project
  `lemon` target, or its checked-in template must never fall back or defer failure.

---

### Task 1: Specify installed-package component behavior

**Files:**
- Create: `tests/install_consumer/component/CMakeLists.txt`
- Create: `tests/install_consumer/component/main.c`
- Modify: `tests/install_consumer/CMakeLists.txt`
- Modify: `tests/install_consumer/run.cmake`

**Interfaces:**
- Consumes: installed `TurboFlowConfig.cmake`, `TurboFlow::<Component>` targets, and CMake's `CMAKE_DISABLE_FIND_PACKAGE_<Package>` controls.
- Produces: real configure/build/run coverage for Config-only and full component consumers plus deterministic failure checks for a missing Graph dependency and an unknown component.

- [x] **Step 1: Add a reusable single-component consumer**

Create a small C project that requires `TURBO_FLOW_TEST_COMPONENT`, calls:

~~~cmake
find_package(TurboFlow 1.0 CONFIG REQUIRED
             COMPONENTS "${TURBO_FLOW_TEST_COMPONENT}")
target_link_libraries(turbo_flow_component_consumer
                      PRIVATE "TurboFlow::${TURBO_FLOW_TEST_COMPONENT}")
~~~

and runs an executable returning success. The consumer tests the installed package and real imported target, not template text.

- [x] **Step 2: Make the full consumer request every exported component**

Change the existing full consumer to request these literal components:

~~~cmake
Config Graph Product Flow ProtocolIngress ProtocolIngressGraph
ProtocolMqttSn ProtocolCoap ProtocolLwm2m ProtocolOcpp ProtocolGbt32960
ProtocolJtt808 ProtocolBusinessOcpp201Core MqttSink SecuritySQLite Codec
Observe Schedule
~~~

It continues linking all corresponding targets and running `turbo_flow_create()` / `turbo_flow_destroy()`.

- [x] **Step 3: Extend the install test driver**

After staging the install, configure/build/test:

1. the full consumer with every component requested explicitly;
2. the existing no-component consumer path with the full dependency set;
3. a Config-only consumer with only stage + Salts prefixes and both `CMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE` and `CMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE`;
4. a Graph consumer with all dependencies available.

Add a bounded `run_expected_failure()` helper that requires a nonzero exit and a named diagnostic substring. Use it to prove Config fails without `SALTS_ROOT` even when `Salts_DIR` is valid, Graph fails without `RULES_FORGE_ROOT` even when `RulesForge_DIR` is valid, and `MissingComponent` fails as unsupported.

- [x] **Step 4: Run RED verification**

Run:

~~~powershell
ctest --preset win-dev-user -R ^test_turbo_flow_install_consumer$ --output-on-failure
~~~

Expected: fail because the current package config unconditionally discovers SaltsUtils/RulesForge for Config, and does not mark all exported targets as supported components.

---

### Task 2: Route package dependencies by requested component

**Files:**
- Modify: `cmake/TurboFlowConfig.cmake.in`

**Interfaces:**
- Consumes: `TurboFlow_FIND_COMPONENTS` generated by `find_package`, `find_dependency`, and exported target names.
- Produces: component-found variables and exact dependency discovery for installed consumers.

- [x] **Step 1: Declare the supported component set**

Define one `_TurboFlow_supported_components` list matching every exported `TurboFlow::<name>` target. When no components are requested, use that list only for dependency calculation so the established full-package behavior remains stable.

- [x] **Step 2: Calculate dependency closures**

Always discover Salts from the validated `SALTS_ROOT`. Discover SaltsUtils only from a validated `SALTS_UTILS_ROOT` for Graph, Product, Flow, ProtocolIngressGraph, ProtocolBusinessOcpp201Core, SecuritySQLite, Codec, Observe, or Schedule. Discover RulesForge only from a validated `RULES_FORGE_ROOT` for Graph, Product, Flow, ProtocolIngressGraph, SecuritySQLite, Codec, Observe, or Schedule. Clear cached package-directory hints before every exact-root `find_dependency(... PATHS ... NO_DEFAULT_PATH)` call.

Unknown requested components receive `TurboFlow_<name>_FOUND=FALSE` and a precise `TurboFlow_NOT_FOUND_MESSAGE`; there is no alias or substitute target.

- [x] **Step 3: Mark components from real exported targets**

After including `TurboFlowTargets.cmake`, set each known `TurboFlow_<name>_FOUND` from the existence of `TurboFlow::<name>`, then call `check_required_components(TurboFlow)`.

- [x] **Step 4: Run GREEN verification**

Run the focused install-consumer test. Expected: the full, Config-only, and Graph consumers pass; missing RulesForge and unknown component cases fail with the expected diagnostics and are counted as successful negative tests.

---

### Task 3: Verify and publish issue #10

**Files:**
- Modify: `docs/superpowers/plans/2026-09-07-component-aware-package-dependencies.md`

**Interfaces:**
- Consumes: generated package behavior and install consumer matrix.
- Produces: reproducible Debug/Release evidence and an unmerged issue-closing PR.

- [x] **Step 1: Run complete validation**

Run fresh Debug/ASan and Release configure/build/full CTest with `win-dev-user` and `win-release-user`, plus `install-win-dev-user`.

- [x] **Step 1a: Make build-tool discovery fail fast**

Add a configure fixture proving `FindTools` rejects a missing project `lemon`
target, then require `re2c`, that target, and `tools/lemon/lempar.c` during the
main configure. Do not search for a system Lemon executable.

- [x] **Step 2: Audit boundaries**

Run CodeGraph sync/affected, `git diff --check`, and active-source searches confirming no TurboNet/TurboHttp/CoroNet/TurboParser target, root, or fallback was introduced.

- [x] **Step 3: Align the GitHub issue and publish**

Update issue #10's title/body to record that the later no-fallback decision supersedes its compatibility-window language. Commit, push `build/issue-10-component-dependencies`, open a PR against `master` with `Closes #10`, post verification evidence to the issue, and leave the PR unmerged.

Published as PR #36: https://github.com/qigao/turbo-flow/pull/36

## Validation Evidence

- RED: the original installed package required SaltsUtils for a Config-only
  consumer; after component routing, a valid cached `Salts_DIR` still bypassed a
  missing `SALTS_ROOT`; the original `FindTools` also configured successfully
  without the project Lemon target.
- GREEN: `test_turbo_flow_install_consumer` configures, builds, and runs the
  explicit full-component, no-component, Config-only, and Graph consumers; its
  missing-root and unknown-component cases fail with their required diagnostics.
- GREEN: `test_turbo_flow_cmake_tools_failfast` rejects a configure without the
  project Lemon target.
- GREEN: `ctest --preset win-dev-user --output-on-failure` passed 27/27.
- GREEN: `ctest --preset win-release-user --output-on-failure` passed 27/27.
- GREEN: `cmake --build --preset install-win-dev-user` installed the Debug
  package successfully.
- Baseline note: the schedule one-shot test was observed failing intermittently
  before the final green runs and reproduced on untouched `master`; follow-up is
  tracked by issue #35 rather than hidden by retries or longer sleeps.
