# Task 1 实现报告：严格配置边界及 execution admission gate

## 实现内容

- 新增 `flow_config_operation.c`，在 Config 层对 `operation_bindings` 做严格验证并直接从 immutable canonical JSON 提供三个只读查询 API；未增加缓存、registry、vtable 或可执行 capability。
- 根字段只在显式提供时 clone 到 canonical document；旧配置缺省该字段时 canonical JSON 不变。
- 实现字段白名单、必填字段、ASCII/长度、整数/枚举、resource channel 引用、权限和 `(operation, resource)` 去重、数量与额度关系验证。
- `generation_create` 在参数和 PARSED 状态验证后、任何 catalog/provider callback、分配、preflight、materialize 或 Graph 转移前，对非空 bindings 返回 `SALTS_ENOTSUP`，错误路径为 `$.operation_bindings`。
- 安装消费者用无 resource 的完整 binding 调用 count/at/permission_at；同一源码由既有安装验收分别作为 C 与 C++ 消费者编译运行。
- 架构文档记录字段、单位、错误、借用期限、schema 仅为词法 identity/version，以及“配置支持不代表执行已接入”。

## TDD 证据

### RED：配置根字段

命令：

`cmake --build --preset win-dev-user --target test_operation_binding_config && ctest --preset win-dev-user -R "^test_operation_binding_config$" --output-on-failure`

结果：`0/1` 通过；真实批准 YAML 期望 `SALTS_OK`，实际为 `-4016` (`SALTS_EINVAL`)，失败位于新测试的 resolver 断言。原因符合预期：既有根字段白名单拒绝 `operation_bindings`。

### GREEN：配置边界

同一 focused 命令结果：`1/1` 通过。扩展后的测试覆盖完整投影、legacy 缺省、空数组、错误 shape、全部必填字段、未知字段、标识符/embedded NUL/resource、数值/枚举/权限、重复、query 失败输出及容量边界。

### RED：generation admission

命令：

`cmake --build --preset win-dev-user --target test_flow_plugin_generation && ctest --preset win-dev-user -R "^test_flow_plugin_generation$" --output-on-failure`

结果：`17/18` 测试通过；新增测试期望 `SALTS_ENOTSUP`，实际为 `SALTS_OK`。这证明合法非空 binding 在原实现中被静默忽略并进入 materialization。

### GREEN：generation admission

同一 focused 命令结果：`18/18` TinyTest cases 通过；CTest `1/1` 通过。测试验证 `error.status/path`、`generation_out == NULL`、`flow_io` 指针和值/状态保持、无 adapter materialize，并能正常关闭 host。

## 最终验证

- Debug configure + full build：成功，无编译警告。
- Debug focused：`test_operation_binding_config`、`test_flow_config`、`test_resolved_config_boundary`、`test_flow_plugin_generation`，`4/4` 通过。
- Debug repeat：config + generation 各连续 5 次，`2/2` CTest entries 通过。
- Debug full CTest（最终 CMake 分层调整后重跑）：`57/57` 通过；install consumer 通过。
- Release configure + full build：成功，无编译警告。
- Release focused：上述四项 `4/4` 通过。
- Release repeat：config + generation 各连续 5 次，`2/2` CTest entries 通过。
- Release full CTest（最终 CMake 分层调整后重跑）：`57/57` 通过；install consumer 通过。
- 额外最终安装/兼容 focused（两 profile）：install consumer、flow config、operation config、compat boundary、resolved boundary、generation 均 `6/6` 通过。
- `git diff --check`：通过。

## 文件

- 新增：`turbo_flow/src/flow_config_operation.c`、`turbo_flow/tests/test_operation_binding_config.c`。
- 修改：`turbo_flow/src/flow_config.c`、`turbo_flow/src/flow_config_internal.h`、`turbo_flow/include/turbo_flow_resolved_config.h`、`turbo_flow/include/turbo_flow_config_limits.h`、`turbo_flow/src/flow_plugin_generation.c`、`turbo_flow/CMakeLists.txt`、`turbo_flow/tests/CMakeLists.txt`、`turbo_flow/tests/test_flow_plugin_generation.c`、`tests/install_consumer/main.c`、`docs/architecture/typed-operation-plugins.md`。

## 自审与关注点

- 新源只编入 Config；Graph/aggregate glob 显式排除，聚合 target 经公开 Config 依赖获得配置 API。
- pair 去重为有界 O(n²)，最大 n=1024；控制面使用且无额外状态，符合任务约束。
- 查询字符串全部借用 canonical document，resolved config destroy 后失效。
- 未发现未解决问题；未修改外部 SDK，未扩大到 operation executable capability。

## Review fix round 1/5

根据 spec reviewer 的 MED 发现，仅扩充 `turbo_flow/tests/test_operation_binding_config.c`，
未修改生产代码：

- 对 9 个数值字段逐字段覆盖负数、分数、零、合法上界、上界 + 1 和可表示的超大数；
  每个拒绝均验证 status、精确字段 path 和 `out == NULL`，合法上界实际解析成功。
- 增加 resource 显式 null/空串，以及标识符长度 1/127/128。
- 增加 permissions 数量 0/1/32，保留既有 33 (`SALTS_ENOSPC`) 覆盖。
- 增加相同 operation + 相同显式 resource 重复拒绝，以及相同 operation + 不同 resource
  共存成功。
- 增加 `coro` 的公开 enum 投影。
- 补全 count/at/permission_at 的 NULL 参数、binding index、permission index 与短 view
  失败组合。

首次 Debug focused 运行中，测试使用超出 YAML→JSON 表示能力的十进制 token，错误在
适配层以 `$` 返回，而非进入 validator。将该测试数据改为 JSON number 可表示但远超全部
配置上界的 `9007199254740992` 后，命中预期字段路径；此为测试 fixture 修正，不涉及生产
行为变更。

验证命令（均在 VsDevCmd amd64 环境）：

`cmake --build --preset win-dev-user --target test_operation_binding_config && ctest --preset win-dev-user -R "^test_operation_binding_config$" --output-on-failure`

结果：Debug `1/1` 通过，0 failed，约 4.96 秒。

`cmake --build --preset win-release-user --target test_operation_binding_config && ctest --preset win-release-user -R "^test_operation_binding_config$" --output-on-failure`

结果：Release `1/1` 通过，0 failed，约 0.55 秒。

## Review fix round 2/5

补充 `operation_binding_at` 的 valid-sized filled view + NULL config 回归：期望返回
`SALTS_EINVAL`，同时清零 operation/resource/permission_count 并保留调用方传入的 size；
既有短 view sentinel 不变断言继续验证不足 size 时不写入。

RED 命令（VsDevCmd amd64）：

`cmake --build --preset win-dev-user --target test_operation_binding_config && ctest --preset win-dev-user -R "^test_operation_binding_config$" --output-on-failure`

RED 结果：`14/15` TinyTest cases 通过，新增 case 在 `check_null(v.operation)` 失败，实际
仍为 sentinel；证实现实现于清零前因 NULL config 提前返回。

最小修复：`flow_config_operation.c` 先验证 view 指针/size，保存 size、清零完整 view、
恢复 size，随后验证 config。未改动其他 validator 或查询语义。

GREEN 命令（VsDevCmd amd64，Debug/Release 分别执行）：

`cmake --build --preset <profile> --target test_operation_binding_config test_flow_config test_resolved_config_boundary test_flow_plugin_generation && ctest --preset <profile> -R "test_(operation_binding_config|flow_config|resolved_config_boundary|flow_plugin_generation)$" --output-on-failure`

GREEN 结果：Debug `4/4` 通过（约 5.08 秒）；Release `4/4` 通过（约 0.60 秒）；构建
输出无新增警告。`git diff --check` 通过。

## Final review fix wave

RED 证据：新增真实 YAML 的 `inline\\u0000junk`、`owner\\u0000junk`、
`none\\u0000junk` 参数化用例后，Debug focused 为 `15/16`，首个枚举值实际返回
`SALTS_OK` 而期望 `SALTS_EINVAL`。修复前 `dumpbin /exports build\\Msvc\\bin\\turbo_flow.dll`
对 `turbo_flow_config_resolve_yaml` 与 `turbo_flow_resolved_config_json` 均无匹配。

实现将枚举比较改为 JSON 字符串长度与完整字面量长度相等后再 `memcmp`。Windows aggregate
通过固定 `.def` 将记录的 20 个历史 Config 名转发至 `turbo_flow_config.dll`，不复制配置
实现或状态；非 Windows 构建路径未增加条件行为。公开查询声明补充 size、借用生命周期、
失败清零与索引错误说明。

持续兼容回归不依赖已安装 TurboFlow：仓库内 `legacy_config_imports.def` 记录历史名称，
隔离目录中的 fixture DLL 只生成 faithful `turbo_flow.dll` import library；consumer 运行目录
不含 fixture DLL。安装测试逐项检查 20 个 forwarder，并用 `dumpbin /imports` 断言 consumer
导入 `turbo_flow.dll` 且不导入 `turbo_flow_config.dll`，随后在新安装 DLL 上执行。

GREEN（均在 VsDevCmd amd64）：

`cmake --build --preset win-dev-user && ctest --preset win-dev-user -R "test_(operation_binding_config|flow_plugin_generation|turbo_flow_install_consumer)$" --output-on-failure`

结果 Debug `3/3`，0 failed，38.24 秒。

`cmake --preset win-release-user && cmake --build --preset win-release-user && ctest --preset win-release-user -R "test_(operation_binding_config|flow_plugin_generation|turbo_flow_install_consumer)$" --output-on-failure`

结果 Release `3/3`，0 failed，28.65 秒。

实际旧库一次性证据：使用
`C:\\projects\\cpp\\external\\pkgs\\turboflow\\release\\lib\\turbo_flow.lib`
链接同一 consumer，`dumpbin /imports` 显示 Config 调用来自 `turbo_flow.dll`；以新 Release
安装目录置于 PATH 后运行 `actual_release_exit=0`。该外部 SDK 只用于本地证据，未进入 CMake/CTest。
