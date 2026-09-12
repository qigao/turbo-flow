# Task 2 执行报告（未完成）

## 状态

Task 2 **未完成**。已实现并安全验证独立 consumer 的版本化 preset/manifest
入口、installed-prefix 严格查找、完整原用例 preset harness，以及 CHTTP
Gateway/provider/adapter 的 dumpbin 层次断言。代码侧安全工作已完成；按控制器授权边界
未运行产品 external SDK install preset、完整安装消费者、Debug CHTTP 或性能门禁，故
Task 2 仍为 BLOCKED，不能标记完成。

## 实现

- 为 full/component/CHTTP/CNet 四个独立消费者增加 Windows/Linux Debug/Release
  `CMakeUserPresets.json` 与 vcpkg manifest；full consumer 另有 preset-owned 本地
  operation fixture install prefix。
- 四个消费者的 profile 从 `PKG_ROOT/turboflow/debug|release` 派生
  `TURBO_FLOW_ROOT`；consumer 要求该根是目录，并对已定义的 `TurboFlow_DIR`
  规范化后验证仍位于根内，越界立即失败，不清 cache 重试；随后使用
  `find_package(TurboFlow ... PATHS "$ENV{TURBO_FLOW_ROOT}" NO_DEFAULT_PATH)`，不允许
  build-tree 或默认路径替代 installed package。
- CHTTP dumpbin 文本检查不含 function/macro：adapter 必须唯一导入
  `chttp_client.dll` 与 `chttp_server.dll`，拒绝旧 `salts_chttp*.dll`、错误 basename、
  未知 `chttp_*.dll`、缺失和重复；provider 必须唯一导入 adapter 且不导入 aggregate；
  Gateway 禁止具体 HTTP 实现。
- 实际 artifact 接线保持三层独立：provider、Gateway、adapter 分别读取
  `/dependents` 后验证；provider 的 Client/Server 直接导入不被人为强制。
- CHTTP consumer 不再复制到临时源码目录，避免破坏其版本化 preset include 关系。
- `tests/install_consumer/run.cmake` 已移除自定义 function/macro、裸 `-S/-B/-D`、
  build-dir CTest 和 `cmake --install`；产品使用 `install-<profile>`，consumer 使用
  configure/build/test preset，operation fixture 使用独立本地 install preset。
- 每个成功、失败场景使用独立 binaryDir；缺 SALTS/RULES_FORGE/TURBODB/CHTTP 根由
  preset 的 `null` 环境值在被测边界显式移除，不能被父 profile 补回。负例同时检查
  非零退出和指定诊断。
- 保留 removed Flow、version 1、CXX-only、Config/all/unscoped/Graph/TurboDb、unknown、
  operation fixture、CNet/CHTTP 动态加载正负例；安装集合和 PE export/import 断言仍在。
- CHTTP 真实门禁会检查 adapter 唯一导入 Client/Server、provider/Gateway 分层、旧
  `salts_chttp` 闭包，以及 adapter/Client/Server 与当前 profile 的 CRT 一致性。
- README 已记录 standalone Chttp SDK、严格根、native targets 和 preset 安装消费方式。

## TDD 证据

实施前真实 RED：

```text
cmake "-DCHTTP_TEST_DEPENDENCY=salts_chttp-2.dll" -P tests/install_chttp_plugin_consumer/check_native_abi.cmake
exit 0
```

旧 ABI 被错误接受。另以新变量传入 Client/Server 时旧脚本不检查并返回 0，证明新契约
缺失，而不是测试拼写错误。

实施后独立 GREEN（2026-09-12）：

```text
positive adapter exit=0
positive provider exit=0
positive gateway exit=0
negative salts_chttp.dll exit=1
negative salts_chttp-2.dll exit=1
negative chttp_clientXdll exit=1
negative chttp_proxy.dll exit=1
negative missing server exit=1
negative duplicate client exit=1
negative Gateway native import exit=1
negative provider missing adapter exit=1
```

根工程可重复 CTest 入口：

```text
cmd /d /c "call ...VsDevCmd.bat -arch=x64 -host_arch=x64 >nul &&
  ctest --preset win-release-user -R test_turbo_flow_chttp_native --output-on-failure"

test_turbo_flow_chttp_native_adapter .............. Passed
test_turbo_flow_chttp_native_legacy_unversioned ... Passed
test_turbo_flow_chttp_native_legacy_versioned ..... Passed
test_turbo_flow_chttp_native_wrong_basename ....... Passed
test_turbo_flow_chttp_native_missing_server ....... Passed
test_turbo_flow_chttp_native_duplicate_client ..... Passed
100% tests passed, 0 tests failed out of 6
```

调试记录：首次给负例同时设置 `WILL_FAIL` 与 `PASS_REGULAR_EXPRESSION` 后，用同一命令
稳定复现 `1/6`，五个负例均显示
`***Failed Required regular expression found`；诊断文本实际已命中，失败来自 CTest 对
两个属性的组合判定，而非 ABI 校验。最小修复是在同一
`check_native_abi.cmake` 增加外层 expected-failure 模式：它启动一次内层真实校验，并同时
要求内层非零且诊断匹配；任意其他非零不能通过。正例仍直接执行真实逻辑。重新 configure
后以上命令得到修正注册的 `6/6`；旧注册形式的结果不作为最终证据。

四个 consumer 目录分别执行 `cmake --list-presets`、
`cmake --build --list-presets`、`ctest --list-presets`，均 exit 0，并列出新增入口。
`git diff --check` exit 0。

对已指定旧 Release SDK 的安全只读 configure：

```text
cmake --preset consumer-full-win-release-user
exit 1
Could not find a configuration file for package "TurboFlow" that is
compatible with requested version "2.0" ... version: 1.0.0
```

这只证明严格 installed-prefix 拒绝旧 SDK，不是新安装包正例证据。首次命令用反斜杠
环境路径触发 CMake `Invalid character escape '\p'`，改用 preset 风格正斜杠后得到上述
真实版本拒绝；该首次错误不计作产品 RED。

## 文件

- `CMakeLists.txt`、`CMakeUserPresets.json`、`README.md`
- `tests/install_consumer/{CMakeLists.txt,CMakeUserPresets.json,vcpkg.json}`
- `tests/install_consumer/component/{CMakeLists.txt,CMakeUserPresets.json,vcpkg.json}`
- `tests/install_cnet_plugin_consumer/{CMakeLists.txt,CMakeUserPresets.json,vcpkg.json}`
- `tests/install_chttp_plugin_consumer/{CMakeLists.txt,CMakeUserPresets.json,vcpkg.json}`
- `tests/install_chttp_plugin_consumer/{run.cmake,check_native_abi.cmake}`

## 未满足门禁与风险

- **HIGH（事实）**：控制器未授权 external SDK 写入；未运行
  `install-win-release-user`、安装后 full consumer 和完整 install-consumer CTest。
- **HIGH（事实）**：没有 Debug CHTTP SDK；Debug configure/build/CTest/install/consumer
  门禁无法运行，且不能用 Release CHTTP 验证 Debug CRT。
- **HIGH（事实）**：缺根及 installed `TurboFlowConfig` 的 CHTTP 严格边界代码已落地，
  但当前指定 Release SDK 是 1.0.0，configure 在版本检查处先失败；因此未把该任意失败
  当成缺根通过。必须安装新 2.0 SDK 后才能观察精确 `SALTS_ROOT`、`RULES_FORGE_ROOT`、
  `TURBODB_ROOT`、`HTTP_SERVICES_ROOT` 诊断。
- **MED（推论）**：Windows loader 使用
  `LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS`；缺传递依赖测试
  还需在真实新安装包上确认同进程已加载模块不会掩盖隔离，仅清 PATH 不足以证明。
- **MED（事实）**：未运行全量主工程、完整测试、真实 dumpbin artifact/CRT 闭包、
  benchmark；`docs/performance/CNET_ADAPTER_BASELINE.md` 未写推算数据。
- **MED（事实）**：Linux presets 已通过 CMake 枚举但未在本 Windows 主机 configure/build；
  Apple 没有仓库 user profile，原脚本的 artifact 命名分支保留但未验证。

## Fix round 1（基于 `63d61aaaafa184802901d2300958758b5dfe7ebb`）

### 三项审查修复

- **HIGH（事实）**：installed CHTTP adapter 现在以原生
  `file(GET_RUNTIME_DEPENDENCIES)` 递归解析真实 DLL 闭包，并分别收集 resolved、
  unresolved、conflicting 结果。任意非系统 unresolved/conflict 失败；所有 TurboFlow、
  Chttp、Salts 一方 DLL 必须位于当前 profile 的显式根中；Release 拒绝闭包任意深度的
  Debug CRT，且拒绝任意深度的 `salts_chttp*.dll`。Windows 系统目录是显式 OS 边界，
  API-set/ext-ms 转发表在解析前排除，System32/SysWOW64 在解析后停止递归；这不吞掉
  `chttp_*` 或其他非系统未解析导入。
- **MED（事实）**：根 CTest 新增
  `test_turbo_flow_chttp_native_gateway_concrete` 与
  `test_turbo_flow_chttp_native_provider_missing_adapter`，和原六项共享同一检查脚本及
  expected-failure 双断言。
- **MED（事实）**：installed `TurboFlowConfig.cmake` 的 CHTTP 边界矩阵新增空根、错误根、
  空 SDK、越界 `Chttp_DIR` cache、无 provenance 预导入、越界 location/implib 预导入、
  错误 configuration；Windows/Linux 各 profile 都有独立 binaryDir preset。测试入口读取
  真正安装 Config，不 source-include 替代。

### 闭包 RED / 调试 / GREEN

首次命令（Developer Command Prompt 环境）及真实 RED：

```text
cmake --preset win-release-user &&
ctest --preset win-release-user -R test_turbo_flow_chttp_closure --output-on-failure
63% tests passed, 3 tests failed out of 8
legacy/wrong_crt/outside 在预期断言前报告大量 Windows API-set unresolved；
unresolved 负例按预期通过。
```

根因是递归解析已进入 System32 模块树，而 Windows API-set 名不是可部署文件；首次仅按
小写固定路径排除又因盘符/目录大小写未命中。按实际解析顺序将系统目录设为 post-exclude、
API-set/ext-ms 设为 pre-exclude 后，第二个 RED 留下 `api-ms-win-crt-runtime-l1-1-0.dll`；
加入该显式 API-set 边界后，outside 又揭示 expected-failure 子进程的 CMake list 参数被
分割。最终用脚本私有哨兵序列化/恢复 list，且修正 first-party basename 正则。

修正后的原生命令与结果：

```text
cmd /d /c "call ...VsDevCmd.bat -arch=x64 -host_arch=x64 >nul &&
  ctest --preset win-release-user -R test_turbo_flow_chttp_closure --output-on-failure"
test_turbo_flow_chttp_closure_setup_{legacy,wrong-crt,unresolved,outside}: Passed
test_turbo_flow_chttp_closure_{legacy,wrong_crt,unresolved,outside}: Passed
100% tests passed, 0 tests failed out of 8

ctest --preset win-release-user -R test_turbo_flow_chttp_native --output-on-failure
adapter, legacy_unversioned, legacy_versioned, wrong_basename, missing_server,
duplicate_client, gateway_concrete, provider_missing_adapter: Passed
100% tests passed, 0 tests failed out of 8
```

四个 closure fixture 都是 root -> mid -> leaf 的至少两级传递链，且由 versioned preset/
manifest 安装到测试自有 local prefix；未写产品 SDK。component preset 枚举 exit 0，包含
上述 Windows/Linux 全矩阵；`git diff --check` exit 0。

### 本轮仍未执行

- **HIGH（事实）**：未获 external SDK 安装授权，未运行产品 `install-<profile>`、真实新
  installed consumer 正例、installed Config 新负例和实际 adapter DLL 闭包。因此旧 1.0
  SDK 的版本前置拒绝不作为这些边界的 GREEN。
- **HIGH（事实）**：Debug Chttp SDK 仍不存在，Debug 安装/消费/CRT 门禁未执行。
- **MED（事实）**：Linux/Apple 与真实安装 DLL 的递归闭包未在当前 Windows 主机验证；
  test-only fixture 不替代真实安装产物证据。

## Fix round 2（基于 `e35f319159aca9318a9dc813f048e45364a0901a`）

### OS 边界与 provenance 修复

- **HIGH（事实）**：不再把 System32/SysWOW64 中的任意文件直接当作可信 OS DLL。
  `file(GET_RUNTIME_DEPENDENCIES)` 仍是唯一递归解析器；legacy CHTTP 和当前 profile 禁止的
  CRT 通过 post-include 优先于系统文件排除进入结果并被拒绝。所有非系统 resolved 文件
  不再按 basename 前缀筛选，而是一律要求位于当前 profile roots。
- **HIGH（事实）**：系统目录只用于标识 loader 实际选择的精确候选路径。根模块和原生递归
  结果中每个应用模块的直接系统边界由既有 dumpbin `/dependents` 观察；同目录优先后，按
  Windows system roots 的顺序选定精确文件。选定文件去重后，以
  `pwsh.exe -NoProfile -NonInteractive` 调用 `Get-AuthenticodeSignature -LiteralPath`；仅接受
  `Status=Valid` 和三个明确、逐字相等的 Microsoft Windows/Microsoft Corporation publisher
  subject。PowerShell、签名或 publisher 异常均 fail fast；未修改 execution policy、证书或
  trust store。此边界裁决记录在 progress.md。
- **HIGH（事实）**：Release 拒绝 Debug CRT，Debug 也拒绝 Release CRT。此 profile 约束针对
  应用闭包；已验证的 Windows 系统组件自身运行时语义不被误判为应用跨 profile 复用。

### RED / GREEN

新增 fixture 后、修复前命令：

```text
ctest --preset win-release-user -R
  "test_turbo_flow_chttp_closure_(debug_release_crt|arbitrary_outside|system_forbidden)$"
50% tests passed, 3 tests failed out of 6
debug_release_crt: CHTTP closure validation unexpectedly succeeded
arbitrary_outside: CHTTP closure validation unexpectedly succeeded
system_forbidden: 未命中 legacy 诊断，而先报 prefix-only outside 诊断
```

系统身份实现阶段的合法正例首先真实失败：VS 环境中 Windows PowerShell 5 错装载 Scoop
PowerShell 7 Security module，报 `FormatXmlUpdateException`；未降级信任。固定使用仓库主机
已有的官方 `pwsh.exe` 后，又分别暴露 subject 输出换行、Debug CRT 的明确 Compatibility/
Microsoft Corporation publisher，以及 fake system root 少一层 `/bin`；均以精确数据边界
修复，不以模糊 contains 或白名单跳过。

最终聚焦命令：

```text
cmd /d /c "call ...VsDevCmd.bat -arch=x64 -host_arch=x64 >nul &&
  cmake --preset win-release-user >nul &&
  ctest --preset win-release-user -R
  \"test_turbo_flow_chttp_(native|closure)\" --output-on-failure"
100% tests passed, 0 tests failed out of 28
Total Test time (real) = 14.34 sec
```

其中 closure 为十个 preset-owned setup 和十个验证：Release/Debug 合法两级闭包正例；
legacy、Release→Debug CRT、Debug→Release CRT、unresolved、已识别前缀 outside、任意 basename
outside、fake system legacy、fake system unsigned。unsigned 精确命中 `Status=NotSigned`；
合法 Release/Debug 分别耗时 1.68/2.44 秒。

另对现有指定 Release Chttp SDK 做只读正例（没有产品 install 或 SDK 写入）：

```text
cmake -DCHTTP_TEST_MODULE=C:/projects/cpp/external/pkgs/http-services/release/bin/chttp_client.dll
  -DCHTTP_TEST_ALLOWED_ROOTS=<release Chttp;Salts;SaltsUtils;worktree vcpkg>
  -DCHTTP_TEST_SEARCH_DIRS=<对应 bin>
  -DCHTTP_TEST_CONFIG=Release
  -P tests/install_chttp_plugin_consumer/check_native_abi.cmake
exit 0, 10.88 sec
```

### 仍未满足

- **HIGH（事实）**：外部 TurboFlow 2.0 安装仍未获授权，因此产品 install、真实新 installed
  consumer/Config 矩阵和真实 `tf_chttp_adapter.dll` 闭包仍未执行。
- **HIGH（事实）**：没有 Debug Chttp SDK；Debug fixture 只证明规则接受本 profile 构建及已签名
  Debug OS runtime，不替代真实 Debug SDK/install consumer 门禁。
- **MED（事实）**：该 Windows 测试新增 `pwsh` host 依赖与签名查询耗时；命令不可用或 trust
  验证异常时按设计拒绝。Linux/Apple 仍未在本机验证。
