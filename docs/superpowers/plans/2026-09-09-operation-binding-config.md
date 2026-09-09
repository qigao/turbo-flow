# Operation binding configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付 #93 第一阶段的严格 operation_bindings 配置解析、只读查询和未接入执行时的明确拒绝；不宣称 #93 全部完成。

**Architecture:** Config 的不可变 resolved document 是唯一事实源，视图仅借用其存储；Graph/PluginHost 不参与解析。generation 暂在转移 Graph 或调用插件前拒绝非空 bindings，待 #93 的完整注册/执行绑定实现以能力验证替换该门槛。旧配置和 DSL 不变。

**Tech Stack:** C11、已安装 Salts JsonParser/CYaml、TinyTest、既有 Config/PluginHost 和 CMake user presets。

**Spec:** [#93 configuration proposal](https://github.com/qigao/turbo-flow/issues/93#issuecomment-5596847809)，用户随后回复 go 批准；[typed-operation design](../../architecture/typed-operation-plugins.md)。基线 dd81334。

## Global Constraints

- 保持现有 DSL operation/resource 语法；绑定唯一键为 (operation, resource)，不是 stage 名。
- 宿主授权来自可信绑定配置，插件仅声明所需权限；不把原生 resource config 当作授权事实源。
- 不引入 C/CMake/引擎 fallback、registry、解释器、动态符号查询或新的第三方依赖。
- 旧配置缺少 operation_bindings 时行为和 canonical JSON 不变；旧 binary 遇到新字段仍明确拒绝。
- 本阶段不新增可执行 plugin capability，不修改 Plugin ABI 1.4，不删除旧 RulesForge 入口。
- 不接受绑定后静默忽略：generation 对非空列表在任何外部副作用或 flow 所有权转移前返回 SALTS_ENOTSUP。门槛注释关联 #93 和移除条件。
- 所有返回字符串/视图只借用到 resolved config destroy；解析失败不发布部分 snapshot。
- 使用 win-dev-user、win-release-user，经 VsDevCmd 执行；不改依赖根或 SDK。

## 精确配置

```yaml
version: 1
operation_bindings:
  - operation: decision.evaluate
    resource: routing
    plugin: fixture.typed
    version: 1
    input_schema: example.Input
    input_schema_version: 1
    output_schema: example.Decision
    output_schema_version: 1
    permissions: []
    execution: inline
    threading: owner
    cancellation: none
    max_inflight: 1
    max_input_bytes: 4096
    max_result_bytes: 1024
    max_retained_bytes: 8192
    max_steps: 10000
    deadline_ms: 0
channels:
  routing:
    kind: fixture.resource
    config: {}
adapters: {}
```

`operation_bindings` 缺省或空列表表示没有显式 DLL operation 绑定。非列表（包括 null）拒绝。每项必须是 mapping，除 resource 外以上字段全部必填，未知字段拒绝。resource 缺省表示 stateless；显式 null/空字符串拒绝。resource 必须引用已存在 channel，Config 只检查存在性，不声称资源 kind、artifact 或插件能力兼容。

operation/plugin/schema/resource/permission 标识符由 ASCII `[A-Za-z0-9_.-]` 组成，长度 1..127；不修复/裁剪/大小写归一化。绑定 pair 重复返回 SALTS_EALREADY，权限重复亦同；数组顺序保持，不自动去重。不同 resource 的相同 operation 可配置。

version 和两个 schema version 为整数 1..UINT32_MAX。execution 为 inline/thread/coro；threading 为 owner/thread_safe；cancellation 为 none/cooperative。Config 仅验证这些枚举词法，不推断 worker 亲和、不承诺 provider 实现取消；能力匹配属于后续 preflight。

在 turbo_flow_config_limits.h 命名硬上界：TURBO_FLOW_CONFIG_OPERATION_MAX_BINDINGS=1024u、MAX_PERMISSIONS=32u、ID_MAX=127u、MAX_INFLIGHT=1048576u、MAX_BYTES=1073741824u、MAX_STEPS=UINT32_MAX、MAX_DEADLINE_MS=3600000u（后六个同 OPERATION 前缀）。max_inflight 为 1..MAX_INFLIGHT；三个 bytes 字段为 1..MAX_BYTES；max_result_bytes <= max_retained_bytes；max_steps 为 1..MAX_STEPS；deadline_ms 为 0..MAX_DEADLINE_MS。所有数字必须 finite、整数、范围内，转换前验证。额度只是请求，不宣称已经实施。

超过 bindings/permissions 数量硬上界返回 SALTS_ENOSPC；类型/未知项/标识符/数值/不存在资源错误 SALTS_EINVAL；错误包含 `$.operation_bindings[index].field` 路径。0 数量有效，硬上界恰满有效。不得把溢出或无效输入转成默认值。

## 公开查询接口

放在 turbo_flow_resolved_config.h，不包含 plugin/Graph 头。新 enum 的前缀均为 TURBO_FLOW_CONFIG_OPERATION_：exec_t {INLINE=0, THREAD=1, CORO=2}；threading_t {OWNER=0, THREAD_SAFE=1}；cancellation_t {CANCEL_NONE=0, CANCEL_COOPERATIVE=1}。

```c
typedef struct turbo_flow_resolved_operation_binding_view_s {
  size_t size;
  const char *operation;
  const char *resource;
  const char *plugin;
  uint32_t version;
  const char *input_schema;
  uint32_t input_schema_version;
  const char *output_schema;
  uint32_t output_schema_version;
  turbo_flow_config_operation_exec_t execution;
  turbo_flow_config_operation_threading_t threading;
  turbo_flow_config_operation_cancellation_t cancellation;
  uint32_t max_inflight;
  size_t max_input_bytes;
  size_t max_result_bytes;
  size_t max_retained_bytes;
  uint32_t max_steps;
  uint64_t deadline_ms;
  size_t permission_count;
} turbo_flow_resolved_operation_binding_view_t;
int turbo_flow_resolved_config_operation_binding_count(
    const turbo_flow_resolved_config_t *config, size_t *count);
int turbo_flow_resolved_config_operation_binding_at(
    const turbo_flow_resolved_config_t *config, size_t index,
    turbo_flow_resolved_operation_binding_view_t *view);
int turbo_flow_resolved_config_operation_binding_permission_at(
    const turbo_flow_resolved_config_t *config, size_t binding_index,
    size_t permission_index, const char **permission);
```

提供 TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT，零值默认（size 除外），使用既有 TURBO_FLOW_C_API/C linkage。count NULL 入参返回 EINVAL；有效 count 输出在失败时为零。at 的 view size 不足返回 EINVAL 且不写越界；足够时先清零输出保留初始化 size，再填充；越界返回 ENOENT。permission_at 有效输出在失败时为 NULL，NULL 入参 EINVAL，任一 index 越界 ENOENT。所有非 NULL 输入对象仍须真实有效，不能声称检测 dangling pointer。不增加通过裸 config void* 进行任意 JSON 查询的公共入口。

## 文件与分层

新建 turbo_flow/src/flow_config_operation.c，承担绑定字段验证、只读投影；仅编入 turbo_flow_config，从 Graph/聚合 glob 排除。内部入口：

```c
int flow_config_validate_operation_bindings(const json_value_t *bindings,
    const json_value_t *channels, turbo_flow_config_error_t *error);
```

声明放 flow_config_internal.h。复用 flow_config.c 的键白名单与结构化错误 helper：如需跨文件复用，将现有 flow_config_error、flow_config_object_keys 改为内部非 static 声明，不复制整块逻辑；不得导出 SDK。

flow_config.c 在根白名单加入 operation_bindings，validated 后仅克隆非缺省数组到 canonical document，不额外缓存可变副本。控制面查询线性访问 JSON 数组；pair 去重 O(n²)，n<=1024，标识比较<=127 字节，非逐消息热路径，空间 O(1)（文档外）。无手写哈希/容器。

### Task 1: 严格配置边界及 execution admission gate

**Files:** 创建 src/flow_config_operation.c、tests/test_operation_binding_config.c（均在 turbo_flow/ 下）；修改 src/flow_config.c、src/flow_config_internal.h、include/turbo_flow_resolved_config.h、include/turbo_flow_config_limits.h、src/flow_plugin_generation.c、两个 CMakeLists、tests/test_flow_plugin_generation.c、tests/install_consumer/main.c（此路径位于根 tests/）、docs/architecture/typed-operation-plugins.md。

**Interfaces:** Consumes turbo_flow_config_resolve_yaml、immutable resolved document、generation_create；Produces 本计划上文完整三个只读查询 API 和内部 validator。无可执行 capability/vtable。

- [ ] Step 1：阅读 parser 实现/公开头/测试以及实际安装消费者。新建真实 YAML 测试，先仅用现有解析 API 验证合法配置返回 OK，当前应因 unknown root field 返回 EINVAL。先运行并记录 RED，再新增声明/实现。

```c
check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &config, &error), SALTS_OK);
/* 实现第一轮后扩展测试： */
size_t count = 0u;
turbo_flow_resolved_operation_binding_view_t view = TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
check_equal(turbo_flow_resolved_config_operation_binding_count(config, &count), SALTS_OK);
check_equal(count, (size_t)1);
check_equal(turbo_flow_resolved_config_operation_binding_at(config, 0u, &view), SALTS_OK);
check_equal(view.operation, "decision.evaluate");
check_equal(view.max_steps, (uint32_t)10000);
```

- [ ] Step 2：实现严格 validator、canonical clone 和公共投影，新增源只属于 Config。实际 JSON API 以安装头为准，不凭空发明 helpers。无单独 parser、重复状态或 fallback。
- [ ] Step 3：逐组 RED/GREEN 覆盖：缺省/空/两个 stateless pair 重复、不同 resource、resource 缺失、null、非数组/非 mapping、每个必填项/未知项、标识长度/非法字节/内嵌 NUL、数字负数/分数/零/越界、大数、枚举、bytes 关系、权限重复/类型/数量、bindings 0/1/max/max+1。测试数据通过 TinyTest helper 或既有字符串库构造，仅在 tests；检查结构化 path/status 与失败 out=NULL，不能只检查非零。
- [ ] Step 4：先添加 generation 拒绝合法非空 bindings 的 RED。用现有真实 transactional fixture 开启 host/snapshot，合法配置与 parsed Graph 传 generation_create，期望 ENOTSUP、error path=`$.operation_bindings`、*flow_io 保持原值、generation_out=NULL、无 materialize owner/可正常关闭 host。在参数和 PARSED 校验后、任何 catalog/provider callback/内存分配前使用 count API 拒绝非空列表；注释 `#93: replace this admission gate only when typed operation binding is fully implemented`。空列表与既有 generation 测试继续通过。
- [ ] Step 5：安装消费者用一份无 resource 的完整 binding YAML 实际调用三个新 API；同时作为 C 与 C++ 消费者编译运行。架构文档添加精确字段/单位/错误/借用期限和“配置支持不代表执行已接入”；示例只用于解析与查询，不称完整可运行 Flow。
- [ ] Step 6：经 VsDevCmd configure/build，最小 test_operation_binding_config、test_flow_config、test_resolved_config_boundary、test_flow_plugin_generation；新测试和 generation 每 profile 各重复5次，再全量 CTest 两 profile，包含安装消费。git diff --check、自审、显式提交代码与文档，不包含本地索引/scratch。

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user && cmake --build --preset win-dev-user && ctest --preset win-dev-user -R "test_(operation_binding_config|flow_config|resolved_config_boundary|flow_plugin_generation)$" --output-on-failure'
```

Release 替换为 win-release-user；重复使用 --repeat until-fail:5；全量不带 -R。需要调查错误先按 systematic-debugging，不修改外部 SDK。

## 后续验收（仍属于 #93，不在本阶段伪实现）

配置交付后，下一份计划定义真实 operation descriptor/vtable、snapshot catalog 来源校验和 generation owner/取消状态机，使用本阶段精确视图接口。只有真实 DLL 经 Graph 执行、错误/取消/结果 owner 完整并通过双 profile 测试后，才能替换 admission gate 并关闭 #93。再推进 #73 的引擎 DLL/旧入口/依赖闭包迁移。各阶段可以单独审查；本阶段不是 operation 注册/执行完成声明。
