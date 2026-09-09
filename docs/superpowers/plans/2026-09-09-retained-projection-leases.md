# Retained projection leases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 #90 的显式 retained projection，保护独立结果 context、消息 clone 与 DLL 生命周期。

**Architecture:** Graph 拥有有界 projection owner，PluginHost 用薄适配 context 持有 snapshot；Graph 不反向链接 PluginHost。worker 只操作 owner 的同步额度，控制线程负责 context 和 snapshot 的最终销毁。旧 borrowed projection 完全保留。

**Tech Stack:** C11、Salts thread primitives、TinyTest、现有 CMake user presets、真实 native DLL fixture。

**Spec:** [#90 approved strict admission proposal](https://github.com/qigao/turbo-flow/issues/90#issuecomment-5595957766)；[typed-operation DLL architecture](../../architecture/typed-operation-plugins.md)。用户于 2026-09-09 回复 `go` 批准严格跨线程 callback 准入。

## Global Constraints

- 保持既有 borrowed projection、消息公开布局、Plugin ABI 1.4 及 snapshot caller-serialized 契约；新增 API 必须显式，新增结构必须 size/version 化。
- Graph 不依赖 PluginHost，不引入第二套 registry、执行器、operation stub 或 C/CMake/引擎 fallback。
- 只接收显式声明 immutable payload、cross-thread clone/destroy、independent context 的 retained contract；不满足直接拒绝。
- 每个消息独占其可变 wrapper；不同消息可以并发 clone/clear，源消息 clone 期间必须保持存活且不被修改。
- callback/分配/释放均在锁外；worker 不调用 snapshot retain/destroy，不销毁 owner 或卸载 DLL。
- 状态归属唯一；额度覆盖在途 reservation、已提交结果和 clones；payload destroy 完成后才归还额度。
- stop 关闭新 bind/clone admission；已接受调用可完成。destroy 非阻塞，忙时返回 SALTS_EBUSY，context 清理失败保留可重试 owner。
- destroy 由控制线程在禁止后续 API 进入且所有 worker API 调用返回后执行；仅观察计数为零不能证明外部裸指针使用者已静止。
- context 和 schema 指向内容不得借用已销毁的 generation/session；native DLL 不可信内存破坏不属于本机制隔离范围。
- DLL 分配的 payload/context 由其对应 callback 释放，不跨 CRT free；最后 context 成功释放之后才释放 snapshot。
- 配额是可信 provider 声明的 payload 保留上界，不是 malloc 拦截或 native sandbox；不宣称性能提升。
- Debug/ASan 与 Release 必须使用 win-dev-user、win-release-user，经 VsDevCmd 执行。禁止更换依赖 root 或降级构建。

## 精确接口及协议

新增 `turbo_flow/include/turbo_flow_projection.h`（包含 turbo_flow.h）。结构版本独立为 major 1/minor 0，不修改插件 root ABI。

```c
typedef struct turbo_flow_projection_owner_s turbo_flow_projection_owner_t;
typedef int (*turbo_flow_projection_context_release_fn)(void *ctx);
enum {
  TURBO_FLOW_PROJECTION_IMMUTABLE = 1u,
  TURBO_FLOW_PROJECTION_CROSS_THREAD = 2u,
  TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT = 4u
};
typedef struct turbo_flow_projection_owner_config_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t flags;
  size_t capacity;
  size_t max_result_bytes;
  size_t max_retained_bytes;
  const turbo_flow_data_schema_t *schema;
  turbo_flow_projection_clone_fn clone;
  turbo_flow_destroy_fn destroy;
  void *ctx;
  turbo_flow_projection_context_release_fn release_context;
} turbo_flow_projection_owner_config_t;
typedef struct turbo_flow_projection_owner_snapshot_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  int accepting;
  size_t outstanding;
  size_t retained_bytes;
  size_t peak_outstanding;
  size_t peak_retained_bytes;
} turbo_flow_projection_owner_snapshot_t;
int turbo_flow_projection_owner_create(
    const turbo_flow_projection_owner_config_t *config,
    turbo_flow_projection_owner_t **out);
int turbo_flow_projection_owner_stop(turbo_flow_projection_owner_t *owner);
int turbo_flow_projection_owner_snapshot(
    turbo_flow_projection_owner_t *owner,
    turbo_flow_projection_owner_snapshot_t *out);
int turbo_flow_projection_owner_destroy(turbo_flow_projection_owner_t *owner);
int turbo_flow_msg_bind_retained_projection(turbo_flow_msg_t *msg,
    turbo_flow_projection_owner_t *owner, void *value);
```

提供对应 `TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT`、`TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT`；默认容量/flags 为零，调用者必须显式配置。所有函数按既有 `TURBO_FLOW_C_API` 导出。

create 校验完整 size/ABI、flags 恰为三个已知位、合法 schema、destroy/release_context 非 NULL。clone 可为 NULL，此时 clone 返回 ENOTSUP。容量及两个字节值必须非零，max_result_bytes <= max_retained_bytes。拒绝 capacity * max_result_bytes、capacity * wrapper_size 的 size_t 溢出。flags 不支持返回 ENOTSUP；参数/版本/算术不合法返回 EINVAL；OOM 返回 ENOMEM。out 在失败时置 NULL，context 所有权不转移。

成功后 owner 按值复制 config 和 schema wrapper（修正其内部 schema 指针）；schema 中字符串由独立 context 或静态模块存储保持有效。release_context 在控制线程调用，成功返回 OK 并销毁 context；失败必须保持 context 完整可重试。回调不可重入 owner 生命周期 API。void payload destroy 必须可靠完成，不支持在失败后继续清理的猜测。

每个 reservation 固定收取 max_result_bytes，含嵌套拥有型 payload 的最大保留量；总 payload 收费上限 max_retained_bytes，wrapper 内存另由 capacity 有界。原值在 bind 前由调用者拥有且不属于 owner 配额。bind 返回 OK 才转移值所有权，任何失败保持原消息和值不变。

状态：ACCEPTING -> STOPPED（幂等 stop）；STOPPED 且 outstanding > 0 时 destroy 返回 EBUSY；未 stop 时 destroy 返回 EBUSY。STOPPED 且静止、outstanding == 0 时尝试 release_context，失败保留 STOPPED；成功销毁 owner。stop 后新 reservation 返回 ECANCELED（以 Salts 实际拼写为准）。snapshot 只读，不推进状态。

clone 顺序：预留 count/bytes -> 分配 wrapper -> DLL clone -> 发布 wrapper；失败按逆序撤销，若 callback 返回错误同时产生非 NULL 新值，必须 destroy 临时值。若返回源 value 的别名，则拒绝 EPROTO 但不得 destroy 源值；NULL 成功输出也为 EPROTO。这些新严格语义仅用于 retained 路径。旧 borrowed clone 行为不改。

clear_projection 保留已有 descriptor 的旧生命周期契约，不把 descriptor 默认为由 result owner pin；允许 descriptor 指向 owner 存储的调用者仍须遵守原 borrowed descriptor 契约或显式 copy。清空 projection 时清空 retained owner 指针，避免 descriptor-only retain_view/clone 重复归还额度。move 只转移 wrapper，既有目的消息必须满足原 API 空目标前提。

PluginHost 新增一个薄工厂，声明于 `turbo_flow_plugin_operation.h`：

```c
int turbo_flow_plugin_projection_owner_create(
    turbo_flow_plugin_catalog_snapshot_t *snapshot,
    const turbo_flow_projection_owner_config_t *config,
    turbo_flow_projection_owner_t **out);
```

工厂仅控制线程调用，snapshot 必须是 live 引用且包含 callback/metadata 所属模块及依赖；这由可信 host binding 保证，不以类型地址相等或 OS 地址猜测认证。薄适配 context 保存原 config 与 retained snapshot；clone/destroy 转发给原 ctx，release_context 先调用原释放，成功后 snapshot_destroy 并释放适配 context；失败不释放 snapshot。工厂失败不消费原 ctx，回滚所有临时引用。这个工厂不是 DLL operation registration capability；未来 #73 binding 负责 catalog 来源校验。

## 文件职责

- `turbo_flow/src/flow_projection_owner.c`：Graph owner 配额与生命周期；私有头 `flow_projection_owner_internal.h` 声明供 message 使用的 reserve/release 与不可变契约访问，禁止导出为 SDK。
- `turbo_flow/src/flow_message.c`：仅扩展私有 wrapper 的 retained owner 与统一释放辅助，不依赖 PluginHost。
- `turbo_flow/src/flow_plugin_projection.c`：PluginHost 工厂桥接；从 Graph/聚合库 glob 排除，加入 PluginHost sources。
- `turbo_flow/include/turbo_flow_projection.h`：上述新公开契约；同步安装头与 C++ 编译。
- `turbo_flow/tests/test_flow_projection_owner.c`：核心行为与确定性并发；必要 fault 专用 TU 放同目录，故障注入仅测试编译，不加生产测试开关/API。
- `turbo_flow/tests/test_flow_plugin_projection.c`、`plugin_projection_fixture.c`、`plugin_projection_fixture.h`：真实 DLL 生命周期。测试专用协议不得进入生产头。
- `turbo_flow/tests/CMakeLists.txt`、`turbo_flow/CMakeLists.txt`：按现有 helper 注册和单向链接。
- 现有安装消费者和 C++ header TU：只追加新接口覆盖，保持旧 ABI fixture 原貌。

### Task 1: Graph 有界 retained projection 与消息语义

**Files:** 创建上述 Graph owner/public/private header 和 `test_flow_projection_owner.c`，修改 message、两个 CMakeLists 及新 `projection_header_cpp.cpp`。必要时创建测试专用 allocation-fault TU。

**Interfaces:** Consumes 既有 turbo_flow_msg、schema、clone/destroy 与 Salts thread primitives；Produces 本计划精确接口中的所有 Graph API（不含 PluginHost 工厂）。

- [ ] Step 1：阅读 flow_message.c、turbo_flow.h projection 契约、test_turbo_flow.c 对应测试与 flow_rulesforge.c 调用点；先写以下最小行为测试（fixture 使用真实 heap int 与计数 callback，配置显式填写三个 flags、capacity=2、max_result_bytes=sizeof(int)、总额为两份）：

```c
check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_OK);
check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
turbo_flow_msg_cleanup(&src);
check_equal(*(const int *)turbo_flow_msg_projection(&dst, NULL), expected_value);
turbo_flow_msg_cleanup(&dst);
check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
check_equal(payload_destroy_count, 2);
check_equal(context_release_count, 1);
```

- [ ] Step 2：用 win-dev-user 配置/构建新测试，记录缺失新 API 的实际 RED（不能把环境/依赖/拼写失败当 RED）；不得先写 production implementation。
- [ ] Step 3：实现最小 owner 和消息路径使测试通过；quota 逻辑采用 Salts mutex，immutable config 在发布后只读。reserve 在锁内检查 `outstanding >= capacity`、`max_result_bytes > max_retained_bytes - retained_bytes`，返回 ENOSPC；成功增加并更新峰值。所有 release 都在 payload destroy 返回后。私有 helper 不接收 PluginHost 类型。
- [ ] Step 4：逐项补 RED/GREEN：非法 size/ABI/flags/schema/capacity/overflow；不支持 clone；额度恰满及失败源保持有效；bytes 限制；clone 错误+临时值/NULL/alias；move；clear_projection + descriptor-only retain_view；clear_content；cleanup；context_release 失败后重试；stop 后 bind/clone 拒绝。每个测试断言具体状态和值，不只看成功码。

```c
check_equal(turbo_flow_msg_clone(&rejected, &src), SALTS_ENOSPC);
check_null(turbo_flow_msg_projection(&rejected, NULL));
check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
check_equal(state.outstanding, (size_t)1);
check_equal(state.retained_bytes, config.max_result_bytes);
check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
```

- [ ] Step 5：用 Salts thread/cond 设计 barrier，worker clone 或 destroy 停在真实 callback 内，控制线程 stop/destroy 得 EBUSY；解锁 barrier、join 后最终销毁。不同消息并发 clone/cleanup，不能并发销毁被 clone 的同一源。不得用 sleep 充当同步或在 worker 使用 TinyTest 断言；结果由 join 后主线程验证。fault TU 可对编译入测试的实际源 calloc 做替换（所有钩子仅在 tests），验证 wrapper OOM 的额度和 ownership rollback；不能新增生产 fault setter。
- [ ] Step 6：提供完整公共参数、错误、所有权、线程说明与可编译 C++ 示例；验证 Graph 链接不需要 PluginHost。运行新测试及 test_turbo_flow、test_flow_managed_boundary、test_flow_async_terminal 的两种 profile；每种新测试重复 5 次。全量 CTest 每 profile 一次，然后 git diff --check、自审、提交。报告准确 RED/GREEN 命令与输出。

### Task 2: PluginHost snapshot 桥接与真实 DLL 生命周期

**Files:** 创建 `flow_plugin_projection.c`、真实 DLL fixture/test 及测试私有 header；修改 plugin operation 公开头、CMake sources/install/header tests、实际安装消费者、架构文档的结果 lease 协议。

**Interfaces:** Consumes Task 1 的 Graph owner config/create/stop/snapshot/destroy 与 retained bind；Produces `turbo_flow_plugin_projection_owner_create`，不改已有 PluginHost 注册 vtable 或 ABI 1.4。

- [ ] Step 1：阅读 flow_plugin.c snapshot retain/destroy、test_flow_plugin_schema.c、test_flow_plugin_generation.c 与 fixture 模式。fixture 保持规范唯一插件入口；通过已有 resource/provider 测试协议取得 DLL 定义的 schema、clone/destroy/context_release，不把测试取符号接口塞进生产代码。
- [ ] Step 2：先写真实 DLL 失败测试：通过 host 加载并建立 snapshot、创建 owner、bind + clone，释放调用者 snapshot 和原消息后 host_destroy 仍为 EBUSY；读 clone 的值及 schema，销毁 clone、stop/destroy owner 后 host_destroy OK；observer 顺序为 payload destroy(s) -> context_release -> module destroy/unload，恰一次。记录新工厂缺失的真实 RED。
- [ ] Step 3：实现薄桥接并确保构建单向依赖。原 config 和 schema wrapper 在工厂返回后可失效，因此 bridge 存副本而非借用调用栈；callback ctx 指向 bridge，bridge 内转发到原 ctx。create 失败不调用原 release_context；retain 成功后的任何失败必须平衡 snapshot 引用。

```c
rc = bridge->original.release_context(bridge->original.ctx);
if (rc != SALTS_OK) return rc;
turbo_flow_plugin_catalog_snapshot_destroy(bridge->snapshot);
free(bridge);
return SALTS_OK;
```

- [ ] Step 4：逐项新增真实 DLL 测试：多个 clone 与 move/两类 clear；cleanup 临时 clone 失败值；context_release EBUSY 后可重试且 host 仍 busy；callback barrier 内不可卸载、worker 返回后仍需控制线程 owner destroy；真实 generation 建立/销毁后独立结果仍有效（复用当前 generation fixture 装配方式，不假冒 generation）。通过 owner snapshot 检查 quota 无泄漏。测试 DLL 计数 context 的存储和读写须同步，observer 保留在主测试内直到全部卸载。
- [ ] Step 5：补安装消费实际调用新 factory（不是只 include）；C/C++ 公共头与旧 ABI fixture 保持覆盖。相关 CTest 重复 5 次两种 profile，然后全量双 profile；不添加根依赖、fallback 或 runtime DLL copy。
- [ ] Step 6：同步架构说明：独立结果 owner 可越过 generation；依赖 session 的结果必须在 session 销毁前 drain。无 race detector 时明确 ASan 不证明无数据竞争。git diff --check、自审并提交；报告不把 #73 当作已完成。

## 统一验证命令

所有命令在如下开发环境执行：

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user'
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --build --preset win-dev-user --target test_flow_projection_owner'
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && ctest --preset win-dev-user -R test_flow_projection_owner --output-on-failure'
```

Task 2 目标替换为 test_flow_plugin_projection；Release 替换 profile 为 win-release-user。聚焦重复加 `--repeat until-fail:5`；全量不带 `-R`。新增 glob source 后重新 configure；只有损坏缓存才使用同 profile `--fresh`，不改用另一个 SDK。

## 自审与交付

Task 1 与 Task 2 共享 CMake 和公开 projection 契约，按序执行，不并行改文件。每任务独立实现/审查 gate，最后整分支审查。#90 只有全部验收有证据后才可关闭；#73 保持 OPEN。完成后创建 feature PR 供用户合并，不自动 merge。
