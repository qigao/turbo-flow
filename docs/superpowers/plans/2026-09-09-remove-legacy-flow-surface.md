# Remove legacy Flow surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this task with independent review.

**Goal:** 完成 #95 的第一原子阶段：删除旧聚合 DLL、转发、兼容头及其构建/安装入口，组件消费者继续可用。

**Architecture:** Config、Graph、Product 和插件/适配器各自拥有实现；不再构建第二份 Graph/Product 聚合 DLL。所有调用方直接链接实际组件，不设 alias、wrapper 或 forwarder。解析/执行状态和错误契约保持不变。

**Tech Stack:** C/C++、CMake package、Windows Debug/ASan 与 Release user presets。

**Spec:** 用户最新要求“删除所有旧dll/接口，不需要任何形式的fallback（c,cmake)”；跟踪 https://github.com/qigao/turbo-flow/issues/95 。本阶段不宣称完成所有 C 层旧 adapter/stage/引擎接口删除。

## Global Constraints

- 用户明确批准破坏旧源代码、DLL 与 CMake 接口；覆盖 PR94 的历史导出保留决定。不得重建旧 alias、转发或兼容包装。
- 删除 turbo_flow DLL / TurboFlow::Flow / Flow component / turbo_flow_config.h，只保留真实组件实现；不是把旧 DLL 改名后继续聚合。
- 包版本为 2.0.0，CMake version compatibility 为 SameMajorVersion；1.x 请求拒绝，新消费者请求 2.0。
- 现有配置/Graph/Source/Sink 生命周期、插件 ABI 1.4、本阶段 operation_bindings ENOTSUP 门槛保持不变。不删除未替代的实际业务能力；C 层旧 command/stage/引擎入口继续由 #95 后续阶段处理。
- 不修改外部 SDK、vcpkg 或安装前缀，不添加依赖搜索/C/CMake fallback；不自动删除外部旧安装目录。
- 使用现有隔离 worktree、VsDevCmd、win-dev-user / win-release-user。所有本地 .superpowers/.codegraph 产物不得新增进提交。
- 先真实失败测试后实现；验证阶段使用新安装 stage，防止 build 中历史残留 DLL 掩盖缺失依赖。

## 决策与迁移

HIGH/事实：旧消费者将不能继续链接/加载旧库或包含旧伞头；迁移并重编译是预期行为，不做旧路径恢复。备选“继续转发”与“Flow alias 指向 Graph”均违反用户要求，排除。

选择组件直接依赖可消除重复实现与事实源；没有数据格式/存储迁移。所有权仍在原 Config/Graph/Product；错误直接向上返回。迁移成本集中在 CMake link/interface include 和安装验证。回滚只能显式回退本次源码提交并重新构建，不提供运行时或构建时自动回滚。

### Task 1: Remove aggregate package and migrate owned interfaces

**Files:**
- Delete: `turbo_flow/src/turbo_flow_config_forwarders.def`、`turbo_flow/include/turbo_flow_config.h`、`turbo_flow/tests/test_flow_compat_boundary.c`、`tests/install_consumer/legacy_config_main.c`、`legacy_config_import_stub.c`、`legacy_config_imports.def`（后三者同目录）。
- Modify: root `CMakeLists.txt`、`cmake/TurboFlowConfig.cmake.in`、`turbo_flow/CMakeLists.txt`、`turbo_flow/tests/CMakeLists.txt`、`turbo_flow/benchmarks/CMakeLists.txt`；`codec/observe/schedule/security` 模块及其 tests 的 CMakeLists；安装消费者 CMakeLists/run.cmake/component CMakeLists 和 CNet/CHTTP 安装消费者 CMakeLists。
- Modify includes: `turbo_flow/include/turbo_flow_security.h`、`turbo_flow_policy.h`、`turbo_flow/tests/test_flow_config.c` 及 rg 找到的剩余兼容头真实调用点。
- Modify current docs: `README.md`、`turbo_flow/ADR_GRAPH_PRODUCT_BOUNDARY.md`、`turbo_flow/ARCHITECTURE.md`、`turbo_flow/DOMAIN_CONTRACTS.md`。历史 ADR/计划不机械重写；当前使用指令必须有效。
- Modify: `turbo_flow/include/turbo_flow_resolved_config.h` 上次遗留注释（size >= sizeof 和借用至 config destroy），只改注释。

**Interfaces:** 消费既有 Config/Graph/Product API，不新增 C API；输出 2.0 component-only 包。旧 Flow component 必须返回 Unsupported TurboFlow component: Flow。当前默认无 components 解析所有现存组件不是 fallback，保持既有语义但不包含 Flow。

- [ ] Step 1: 阅读上述实现/测试至少三处，确认源归属和 root install(TARGETS ...) 列表；先在现有安装测试添加旧 component 请求拒绝的行为回归，使用既有 run_expected_failure，对真实安装包 configure，而不是 grep 源码。

```cmake
# Use the existing component consumer and package/dependency arguments.
run_expected_failure("removed Flow component" "Unsupported TurboFlow component: Flow"
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${test_root}/removed-flow-build" -G "${TURBO_FLOW_GENERATOR}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  -DTURBO_FLOW_TEST_COMPONENT=Flow)
```

保持初始请求版本 1.0 以便 baseline 真实成功导致 RED；实现后消费者迁为 2.0。为新测试复用完整既有 dependency env/args，不能因缺依赖而得到假 RED。运行 `ctest --preset win-dev-user -R '^test_turbo_flow_install_consumer$' --output-on-failure` 并记录期望失败。

- [ ] Step 2: 删除整个 add_library(turbo_flow SHARED ...) 及其所有设置/依赖/安装注册，删除 Flow supported/dependency component条目和转发/legacy fixture。将每个旧 link 迁移到真实组件：Graph API -> TurboFlow::Graph；配置解析 -> Config；product/runtime ingress配置 -> Product。不提供名字相同的接口target。

```cmake
project(TurboFlow VERSION 2.0.0 LANGUAGES C CXX)
# Preserve the existing project name/languages/options when updating VERSION.
write_basic_package_version_file(
  "${CMAKE_BINARY_DIR}/TurboFlowConfigVersion.cmake"
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
```

仅替换 VERSION 和 COMPATIBILITY，不重写根 project 的其他字段。现有库仍按实际需要保留 RulesForge 等依赖，不以删除旧包为由伪装 #73 完成。

- [ ] Step 3: 删除兼容头，迁移到直接声明所属头。例如 test_flow_config.c 需要 Product 和 Config，使用 turbo_flow_product.h / turbo_flow_resolved_config.h；security/policy 阅读其声明后包含 Graph 和实际配置头，不能隐式依赖已删除伞头。删除 compat-only TinyTest，原功能覆盖由已有 Config/Product 测试承载，不删除有意义的业务测试。

- [ ] Step 4: 扩展安装验证，2.0 C/C++ full 和 Graph/Config-only消费者均构建运行；Flow component configure按精确错误拒绝。为 component fixture增加可指定版本参数，默认 2.0；显式1.0版本请求在真实包版本检查失败，并断言版本不兼容诊断而非缺包/缺依赖。安装 stage 中不得出现 turbo_flow.dll/turbo_flow.lib、Linux/macOS旧聚合库、turbo_flow_config.h；对核心消费者/模块依赖做 dumpbin 验证不依赖旧 turbo_flow.dll，避免本机旧DLL掩盖迁移遗漏。

```cmake
if(NOT DEFINED TURBO_FLOW_TEST_PACKAGE_VERSION)
  set(TURBO_FLOW_TEST_PACKAGE_VERSION 2.0)
endif()
find_package(TurboFlow ${TURBO_FLOW_TEST_PACKAGE_VERSION} CONFIG REQUIRED
  COMPONENTS "${TURBO_FLOW_TEST_COMPONENT}")
```

可在既有 run.cmake 使用 file(GLOB) 仅检查新 stage 的旧聚合产物，无删除和扫描外部SDK；明确匹配 turbo_flow 库本名，不能把 turbo_flow_graph 等现代库误判为旧DLL。正向 build/run 及负向 configure均从真实安装包验证。

- [ ] Step 5: 更新当前文档为 2.0 组件接口，明确breaking移除和手动迁移；修复 resolved view注释为至少 sizeof 且借用至 config销毁。旧 ADR 保留背景时明确 superseded by #95，不宣称仍需兼容。检查 rg 结果区分历史说明/负向测试与有效旧路径，勿机械清理兼容性单词。

- [ ] Step 6: 两profile configure/build，先安装和配置相关聚焦测试，再全量CTest各一次；读输出、git diff --check、自审、显式提交代码/测试/docs。不得提交scratch报告。不要删除旧build树，旧产物不参与新stage；主代理另外清理明确定位的本worktree旧生成文件。

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user && cmake --build --preset win-dev-user && ctest --preset win-dev-user --output-on-failure'
```

Release 使用 win-release-user。在 report记录RED、GREEN、实际test数量和失败诊断。若编译发现隐藏 transitive include/link依赖，补其真实组件依赖，不恢复兼容头或聚合target。
