# 独立 Chttp SDK 迁移 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 #109 的独立 Chttp 接入和安装交付验证，解除 #2/#63 的实际构建阻塞；不把这一前置任务当作 #28/#58/#2 的全部完成。

**Architecture:** 保留 Graph → CHTTPAdapter → Chttp 和 PluginHost → provider DLL 的职责边界，只迁移 native 包入口、依赖验证和交付测试。共享的原生 CMake include 负责 Chttp 根、导入产物和 deferred-cancel 能力验证，不新增 function/macro。安装验证按 Gateway、provider、adapter 三层分别检查，而不是从 CMake PUBLIC 链接列表猜测 PE import。

**Tech Stack:** C11/C++17、CMake user presets、vcpkg manifest、CHttp::Client/CHttp::Server、现有 TinyTest、Windows dumpbin。

**Spec:** https://github.com/qigao/turbo-flow/issues/109 及 https://github.com/qigao/turbo-flow/issues/109#issuecomment-5645101429 。基线 commit 为 6d071becf4d1b07cbc6fbd58423cb3d34d042590。

## Global Constraints

- 包名 Chttp，目标 CHttp::Client 和 CHttp::Server；不得恢复 Salts::CHTTP、HTTPServices、旧 umbrella header 或任何 C/CMake fallback。
- 仅从 HTTP_SERVICES_ROOT 查找；host 使用 PKG_ROOT/http-services/debug|release，Android 使用 PKG_ROOT/http-services-android/debug|release；根在相关 user profile 中定义一次。
- 缺失、空、错误根、错误配置、越界 cache、越界预导入 target 均明确失败，不静默修复、不默认搜索、不跨 Debug/Release 复用。
- 第一方 configure/build/test/install 使用版本化 CMakeUserPresets.json 和 vcpkg.json；安装使用 install-<profile> build preset；不新增自定义 CMake function/macro/helper 或手工 DLL 复制兜底。
- 不改公开配置字段、版本号、生命周期、取消/重试、容量或 TLS 验证语义；发现 native 不兼容先报告具体签名/字段证据，不添加兼容代码。
- Gateway 不直接链接 CHTTPAdapter、Chttp native DLL 或旧 turbo_flow.dll；provider 仍只有 turbo_flow_plugin_get_api 一个导出。
- 不安装/删除外部 SDK，不 push/merge，不关闭父 issues；Debug SDK 缺失必须记为未通过门禁，不能用 Release 的结果替代。
- 保留根工作区的用户文件，不提交 .codegraph 或 .superpowers 产物。

## 影响与决策

HIGH：公共 adapter 头引用 native structs，必须通过 C/C++ 编译和原有集成测试，名称盘点不能证明 ABI。native DLL 拆分后旧二进制不再兼容，部署须整包重建；失败回滚是部署旧完整制品，不是在新进程增加旧实现。

MED：安装消费者原有裸 CMake 流程不满足当前 preset 契约，必须迁移实际验收路径，保留原负例和运行测试。拒绝只修改 import 正则而不验证实际 adapter 闭包的方案。

算法、业务状态归属和协议不变；运行所有权仍归 Chttp owner，Flow 仍管理消息和 adapter 生命周期。改变的是构建期依赖准入和产物解析，失败时 configure 不得生成可用半配置。

### Task 1: native SDK 入口、严格包验证与 adapter 回归

**Files:**
- Modify: `CMakeLists.txt`, `CMakeUserPresets.json`, `cmake/TurboFlowRequireCHTTP.cmake`, `cmake/TurboFlowConfig.cmake.in`。
- Modify: `io/chttp/CMakeLists.txt`, `io/chttp/tests/CMakeLists.txt`, `io/chttp/include/turbo_flow_chttp.h`, `io/chttp/tests/test_chttp_plugin.c`, `io/chttp/tests/test_chttp_client_stop_fault.c`, `io/chttp/tests/chttp_client_init_fault_support.c`, `io/chttp/ADR_CHTTP_DEFERRED_SERVER.md`。
- Create: `tests/cmake/chttp_package/CMakeLists.txt`, `tests/cmake/chttp_package/CMakeUserPresets.json`, `tests/cmake/chttp_package/vcpkg.json`, `tests/cmake/chttp_package/probe.c`；测试 fixture 可放同目录的命名子目录。

**Interfaces:**
- Consumes: Chttp 的公开 `<http_client/http.h>`、`<http_server/http.h>`；SDK package `lib/cmake/Chttp/ChttpConfig.cmake`。
- Produces: `include(TurboFlowRequireCHTTP)` 可直接执行严格发现与能力检查，不再调用 `turbo_flow_require_chttp_deferred_cancel()`；根工程和安装包用同一 include；目标/adapter API 名字不变。
- 安装消费者迁移属于 Task 2；本任务不能宣称安装验证通过。

- [ ] **Step 1: 建立预期失败测试并保存 RED 输出。** 使用版本化 fixture presets（继承仓库同平台 compiler/toolchain/flags 和 manifest 模式，不复制 flags 到 CMakeLists）。fixture 从仓库相对路径 include 实际 `cmake/TurboFlowRequireCHTTP.cmake`，非旧实现副本。真实 SDK 正例应能链接如下程序，迁移前因旧入口失败：

```c
#include <http_client/http.h>
#include <http_server/http.h>
int main(void) {
  chttp_async_client client = {0};
  chttp_server_deferred deferred = CHTTP_SERVER_DEFERRED_INIT;
  return client.impl != 0 || deferred.impl != 0;
}
```

为 fixture 提供 C-only、CXX-only user presets（CXX-only 将 probe.c 设置 LANGUAGE CXX）。负例场景由测试工程专用变量选择，在 include 之前设置被测环境/target，不修改外部 SDK：unset-root、empty-root、missing-root、empty-sdk、outside-cache、preimport-without-provenance、preimport-outside-location、wrong-config、missing-deferred-cancel。前八种分别断言匹配的根/target/config 错误；缺符号 fixture 提供当前 Server header 和无该符号的 test-only library，必须失败在链接能力探针，而非别的依赖缺失。父测试用 execute_process 检查非零退出值及具体诊断，正例必须退出零；仅 WILL_FAIL 不够。fixture 自身同样从 user preset 运行，不使用裸 -S/-B。

- [ ] **Step 2: 原生 CMake 严格准入。** 在现有 `TurboFlowRequireCHTTP.cmake` 去掉自定义函数包装，先验证环境 root 存在且是非空目录。规范化路径并校验已有 Chttp_DIR（包含 cache 和普通变量）不越界；预导入 target 无可信配置目录直接失败，不删除异常值后重试。使用唯一查找入口：

```cmake
find_package(Chttp CONFIG REQUIRED
             PATHS "$ENV{HTTP_SERVICES_ROOT}" NO_DEFAULT_PATH)
```

查找后以 `file(REAL_PATH)` + `cmake_path(IS_PREFIX ... NORMALIZE ...)` 验证配置目录及 Client/Server 的 include、runtime、import-library 路径都在指定根。Client/Server 必须是 SHARED IMPORTED，当前请求配置必须存在于 IMPORTED_CONFIGURATIONS；检查每个请求配置的 IMPORTED_LOCATION，Windows 同时检查 IMPORTED_IMPLIB 存在且在根内，禁止不同配置映射或无配置产物兜底。单配置使用 CMAKE_BUILD_TYPE，多配置逐个检查 CMAKE_CONFIGURATION_TYPES；未声明配置明确失败，不自行选择 Release。检查实际 target 属性而非仅检查 Chttp_DIR，覆盖伪造同根 Chttp_DIR 的越界 target。

检查状态通过 `CMakePushCheckState` 的 `cmake_push_check_state(RESET)` / `cmake_pop_check_state()` 保留调用方状态。探针清除普通和 CACHE 结果，且使用 `CHttp::Server` 和如下源码；C/CXX 按已启用语言选择，不因编译失败改用另一语言：

```cmake
set(CMAKE_REQUIRED_LIBRARIES CHttp::Server)
set(_turbo_flow_chttp_probe [[
#include <http_server/http.h>
int main(void) {
  return chttp_server_deferred_cancel((chttp_server_deferred *)0);
}
]])
```

失败信息明确指出 CHttp::Server 缺少可链接的 chttp_server_deferred_cancel。根 CMake 和安装 Config 都删除旧函数调用，复用本 include 的完整契约。保留现有 package 组件集合和当前整体导出策略，不扩大为 component 重新设计。

- [ ] **Step 3: 配置与头文件迁移。** 四个 host user profile 和两个 Android dependency profile 添加一次 HTTP_SERVICES_ROOT；host runtime PATH/LD_LIBRARY_PATH 从该 root 派生。公共 adapter 头同时 include 两侧头；直接使用客户端的 fault support 只 include Client，test_chttp_plugin.c 根据实际调用包含两侧。

```cmake
target_link_libraries(tf_chttp_adapter PUBLIC
  TurboFlow::Graph CHttp::Client CHttp::Server)
```

test_chttp_plugin 使用真实 Client/Server；client fault support 用 Client，WebSocket fault support 用 Server。不改 mock 注入策略、不更改产品状态机。更新活动 ADR 的依赖入口和整包迁移说明。新增/修改测试定义使用原生 CMake 命令，不增加新的 helper 使用。

- [ ] **Step 4: 验证、复核、提交。** 所有 Windows 命令从 VsDevCmd 环境执行；运行 fixture 正负例，再配置主工程并构建七个 HTTP 相关 test target：

```text
cmake --preset win-release-user
cmake --build --preset win-release-user --target test_chttp_adapter test_chttp_server_adapter test_chttp_websocket_adapter test_chttp_websocket_close_fault test_chttp_client_stop_fault test_chttp_plugin test_chttp_plugin_abi
ctest --preset win-release-user -R ^test_chttp_
cmake --preset win-dev-user
```

Debug 若缺 SDK，保存准确失败，不扩大到 SDK 安装或将测试标记通过。校验 C/C++ 头 TUs 均实际编译。用 `git diff --check` 和 rg 检查活动 io/chttp、依赖 include 无旧入口；Task 2 仍持有旧安装验证脚本不能据此声称全仓库清零。提交本任务代码和计划，完整报告测试命令/退出值/失败原因。

### Task 2: 安装消费者 presets、依赖闭包与全量交付门禁

**Files:**
- Modify: `tests/install_consumer/run.cmake`, `tests/install_consumer/CMakeLists.txt`, `tests/install_consumer/component/CMakeLists.txt`, `tests/install_chttp_plugin_consumer/run.cmake`, `tests/install_chttp_plugin_consumer/check_native_abi.cmake`, `tests/install_chttp_plugin_consumer/CMakeLists.txt`，安装测试注册所属 CMake 文件。
- Create: 上述独立消费者工程的 `CMakeUserPresets.json`、`vcpkg.json`；现有 CNet consumer 受共同 harness 调用时亦补齐这两个版本化入口，不改变 CNet 产品代码。
- Modify: `README.md` 中依赖/验证说明、`docs/performance/CNET_ADAPTER_BASELINE.md`（只在真实 benchmark 完成后追加本次证据）。

**Interfaces:**
- Consumes: Task 1 的 `HTTP_SERVICES_ROOT` 和直接 include 严格契约、原有 TurboFlow 导出组件和 provider ABI。
- Produces: 安装消费者实际 preset 驱动验证；Gateway/provider/adapter 依赖分层断言；#109 的安装与全量门禁证据。

- [ ] **Step 1: 保留并迁移测试入口。** 阅读原 run.cmake 的每个用例和构建配置，先列出原用例→新 preset/命令映射，确保 removed Flow、version 1、C++ only、全组件、Config/Graph/TurboDb 子集、缺根、unknown component、安装 operation fixture、CNet/CHTTP 动态加载正负例全部仍执行。公共 compiler/flags/toolchain 继承版本化现有 presets；新增 consumer user presets 按 active profile 使用相同 dependency roots 和独立 binary/install 路径。

```cmake
execute_process(COMMAND "${CMAKE_COMMAND}" --preset "${consumer_preset}"
  WORKING_DIRECTORY "${consumer_source_dir}"
  RESULT_VARIABLE configure_result OUTPUT_VARIABLE configure_output
  ERROR_VARIABLE configure_error)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR "Consumer configure failed: ${configure_output}\n${configure_error}")
endif()
```

build 使用 `--build --preset`，test 使用 `ctest --preset`，install 使用 `--build --preset install-...`。普通成功用例必须检查退出值；负例必须检查退出值和预期诊断，不能因任意失败而通过。不新增 run_checked 等 function/macro 使用。依赖根不复制进 -D cache。consumer 工程 vcpkg baseline 和第三方依赖依据已导出 targets 的实际闭包配置；不关闭 manifest、不复制 vcpkg DLL。禁止从 tests fixture 推断外部共同包目录。

若需要实际安装到工程既定外部 SDK prefix，先向 controller 报告确切 prefix 和现有文件影响，等待明确安装授权；不要自行改安装 prefix 或复制 DLL 到测试目录当替代安装。生成树清理必须先验证绝对路径在本工作树 build 下，优先保留已有产物，禁止原来未经验证的递归删除。

- [ ] **Step 2: 测试真实依赖层次而非旧 DLL 单项名称。** 先给 native 验证脚本提供明确的 test-only dumpbin 文本用例；分别拒绝旧 salts_chttp.dll、salts_chttp-2.dll、错误 native basename、缺 Client/Server 的 adapter、重复 native 项，以及含新 native DLL 的 Gateway。正例 adapter 必须两端齐全，Gateway 无 native import；provider 校验 adapter import 和唯一导出。测试脚本使用原生 include/变量，不新增自定义函数。

```cmake
if(gateway_dependents MATCHES "tf_chttp_adapter\\.dll|chttp_client\\.dll|chttp_server\\.dll|salts_chttp[^ \t\r\n]*\\.dll|turbo_flow\\.dll")
  message(FATAL_ERROR "Gateway must not link a concrete HTTP implementation")
endif()
```

真实 artifact 用 dumpbin /exports 和 /dependents 获取输出，三个层次分别检查。Client/Server 的共同 TLS 符号可决定 provider 实际直接 import，不能人为加未用调用来凑两个直接 import。递归的 HTTP 依赖闭包必须不存在旧 salts_chttp DLL；检查当前 Chttp native CRT，Debug 不能加载 Release，Release 不能包含 Debug CRT。保留缺 provider DLL 和缺传递依赖测试，显式控制测试目录和 PATH，不修改产品加载器。

- [ ] **Step 3: 主工程全量和已安装独立消费者验证。** 两个 Windows user profile 分别 configure、完整 build、完整 CTest、install preset 和安装消费者；SDK 缺失/外部安装授权缺失时准确报告未完成项，不把本任务或 #109 标记完成。安装消费者从指定安装前缀查找 TurboFlow，不能偷用源码 build tree。

```text
cmake --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user
cmake --build --preset install-win-release-user
```

Debug 使用对应 win-dev-user 和 install-win-dev-user，保持独立前缀。核对所有实际列出的 tests 被执行，禁用项如实报告。记录产物路径、commit、CRT 和 DLL 清单。

- [ ] **Step 4: 恢复原性能测量并交付。** 完成代码提交且树干净后构建 bench_cnet_adapter，在同 Release preset runtime 环境直接运行现有程序，不更改 warmup、消息量、容量、七次重复和统计口径。与既有基线比较实际 median/MAD、P95/P99、CPU、饱和恢复和 stop/drain 指标；不填推算值，不把静态 allocation 审计写成运行测量。更新 #109 逐项证据和 #2/#63 相关进度，父问题保留未完成事项。运行最终 diff 检查和整分支审查后再请求合并，不由任务执行者 push/merge。

### Task 3: 全量验证前置——两个旧 fail-fast fixture 的 preset 迁移

全量预检发现两个 runner 仍裸调用 CMake 并递归删除目录。执行顺序调整为 Task 1、
Task 2 已审查代码及原始基准、Task 3、Task 2 剩余交付门禁。不改变生产依赖契约。

**Files:**
- Modify: `tests/cmake/run_find_tools_failfast.cmake`, `tests/cmake/run_cnet_stop_drain_failfast.cmake`, root `CMakeLists.txt` 中对应两项测试注册。
- Create: `tests/cmake/find_tools_without_lemon/CMakeUserPresets.json` 与 `vcpkg.json`。
- Create: `tests/cmake/cnet_without_stop_drain/CMakeUserPresets.json` 与 `vcpkg.json`。
- Read: 两个 fixture 的 CMakeLists、`cmake/FindTools.cmake`、`cmake/TurboFlowRequireCNet.cmake`、既有 CHTTP fixture runner/presets。

**Interfaces:** 消费 source 参数及 `TURBO_FLOW_ACTIVE_PRESET`；产生原有两项同名 CTest。
错误仍须分别命中 `project-provided lemon target is required`、
`Salts::CNet stop-drain contract v1`，不能把任意失败视为成功。不改生产探针、HTTP checker、
外部 SDK 或安装规则。

- [ ] **Step 1: 入口 RED。** 两个 runner 删除 `REMOVE_RECURSE`、binary/generator 参数消费，保留 source 校验及原有退出码/准确诊断断言。各自的 configure 调用改为：

```cmake
if(NOT DEFINED ENV{TURBO_FLOW_ACTIVE_PRESET} OR
   NOT "$ENV{TURBO_FLOW_ACTIVE_PRESET}" MATCHES "^(win|linux)-(dev|release)-user$")
  message(FATAL_ERROR "A supported TURBO_FLOW_ACTIVE_PRESET is required")
endif()
set(fixture_preset "fixture-$ENV{TURBO_FLOW_ACTIVE_PRESET}")
execute_process(
  COMMAND "${CMAKE_COMMAND}" --fresh --preset "${fixture_preset}"
  WORKING_DIRECTORY "${fixture_source_dir}"
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_stdout ERROR_VARIABLE configure_stderr)
```

新 presets 尚不存在时，在 VsDevCmd 中运行以下测试，预期因未命中原错误诊断而失败。
记录这是入口缺失 RED，不伪称旧生产行为失败；不得先运行旧递归删除路径。

```text
ctest --preset win-release-user -R "^test_turbo_flow_(cmake_tools|cnet_stop_drain)_failfast$" --output-on-failure
```

- [ ] **Step 2: 版本化 fixture 入口。** 两个目录各使用下面完整内容；相同名字位于不同独立工程。继承 compiler/flags/toolchain/manifest，不复制 flags 或第一方依赖根进 cache。

```json
{
  "version": 6,
  "include": ["../../../CMakeUserPresets.json"],
  "configurePresets": [
    { "name": "fixture-win-release-user", "inherits": "win-release-user", "binaryDir": "${sourceDir}/build/win-release-user", "cacheVariables": { "TURBO_FLOW_SOURCE_DIR": "${sourceDir}/../../..", "CMAKE_INSTALL_PREFIX": "${sourceDir}/build/win-release-user/install" } },
    { "name": "fixture-win-dev-user", "inherits": "win-dev-user", "binaryDir": "${sourceDir}/build/win-dev-user", "cacheVariables": { "TURBO_FLOW_SOURCE_DIR": "${sourceDir}/../../..", "CMAKE_INSTALL_PREFIX": "${sourceDir}/build/win-dev-user/install" } },
    { "name": "fixture-linux-release-user", "inherits": "linux-release-user", "binaryDir": "${sourceDir}/build/linux-release-user", "cacheVariables": { "TURBO_FLOW_SOURCE_DIR": "${sourceDir}/../../..", "CMAKE_INSTALL_PREFIX": "${sourceDir}/build/linux-release-user/install" } },
    { "name": "fixture-linux-dev-user", "inherits": "linux-dev-user", "binaryDir": "${sourceDir}/build/linux-dev-user", "cacheVariables": { "TURBO_FLOW_SOURCE_DIR": "${sourceDir}/../../..", "CMAKE_INSTALL_PREFIX": "${sourceDir}/build/linux-dev-user/install" } }
  ]
}
```

两个 `vcpkg.json` 分别使用以下内容。工程按契约必定 configure 失败、无安装目标，
不添加虚假的 build/install 成功入口。

```json
{ "name": "turbo-flow-find-tools-negative", "version-string": "1.0.0" }
```

```json
{ "name": "turbo-flow-cnet-contract-negative", "version-string": "1.0.0" }
```

- [ ] **Step 3: root 注册。** 两项 `add_test` 保留 source 参数和 `-P` runner，删除不再消费的 `TURBO_FLOW_BINARY_DIR`、`TURBO_FLOW_GENERATOR`；保留测试名字、标签、串行设置及超时，只有实测超时证据才调整。禁止新增 function/macro，不重写 production include 既有函数。
- [ ] **Step 4: GREEN 及归因。** `cmake --preset win-release-user` 后重跑 Step 1 两项测试，预期 2/2；检查输出确实命中各自准确诊断。CNet 编译日志必须是 contract 宏缺失，不是 compiler/vcpkg/缺头失败。确认 manifest 开启、再次运行不依赖递归清理、未知 active profile 明确失败。相邻回归仅 CHTTP package 与 native/closure；不运行外部安装消费者，不把缺 SDK 的 Debug 或未执行的 Linux 标记通过。
- [ ] **Step 5: 提交与审查。** `git diff --check`；核实两个 runner 无递归删除或裸 `-S/-B/-G`，新增 manifest/preset 均版本化。提交本计划及限定文件，报告留在本地 SDD workspace。独立 review 后继续 Task 2 全量门禁；外部安装授权仍需明确取得。

### Task 4: 安装验证前置——ASan CRT 所有权与负例重复执行

**Files:** `tests/install_chttp_plugin_consumer/check_native_abi.cmake`、其
`closure_fixture/` 既有 fixture/presets、根 CMake 测试注册、
`tests/install_consumer/run.cmake`。不改公开 package 或产品加载器。
**Interfaces:** 沿用实际依赖边、当前工具链精确路径与签名；Debug 应用仍禁止
Release CRT，只有已认证 ASan 自身的导入允许其官方 Release CRT。

- [ ] **Step 1: RED。** 读取 `task-2-fix5-review.md`，用既有 fixture 增加
  不含 ASan 的闭包不能从 compiler directory 获得额外 CRT 信任的负例，
  明确命中 provenance 错误，不能以任意失败算成功。保留既有 ASan 正例、
  应用 wrong-CRT、outside 和 PATH-shadow 负例。另有已复现的重复运行 RED：
  `root-release-install-fix5-tests.log` 中版本负例遗留 `TurboFlow_DIR-NOTFOUND`，
  下一次被 component:57 拒绝，runner:102 未命中原版本诊断。
- [ ] **Step 2: 最小所有权修复。** 只允许实际认证 ASan 的直接 CRT 依赖
  使用 compiler-local platform boundary；其他应用边选中这些目录外 CRT 必须
  fail fast。不得用“闭包任意地方有 ASan”给其他应用边全局豁免。依赖来源的
  必要前置/后置检查只归入既有 checker；保留 native resolver，不手写 loader。

```cmake
if(direct_dependency_real IN_LIST chttp_compiler_crt_files AND
   NOT chttp_application_module_real STREQUAL chttp_compiler_runtime)
  message(FATAL_ERROR "Compiler-local CRT is not owned by the authenticated ASan runtime")
endif()
```

  本地解析和搜索解析的实际选中文件都须受同一所有权规则约束；若需更改
  native resolver 边界顺序才能保证不变量，先报告证据与最小调整。
- [ ] **Step 3: 负例初始状态隔离。** 仅在两个 configure-negative 循环
  的既有命令增加 `--fresh`，不修改正例或 package 的缓存来源校验：

```cmake
COMMAND "${CMAKE_COMMAND}" --fresh --preset
        "component-${failure_case}-${consumer_profile}"
```

```cmake
COMMAND "${CMAKE_COMMAND}" --fresh --preset
        "component-${component_negative_case}-${consumer_profile}"
```

- [ ] **Step 4: 验证与提交。** Via VsDevCmd，最小新负例 RED/GREEN 后运行
  `ctest --preset win-release-user -R "test_turbo_flow_chttp_(native|closure)_"`。
  controller 随后连续两次运行实际 Release installed consumer，再跑 Debug。
  版本负例必须始终命中原诊断；后续门禁若失败须报告具体阶段，不能称整体通过。
  仅主控运行外部安装测试。提交限定代码与本计划，自审、独立审查，不新增
  fallback/helper、复制 SDK DLL、关闭 ASan 或信任整个目录。
