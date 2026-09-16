# Remove legacy stage registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for implementation and independent review.

**Goal:** 删除 #95 的 turbo_flow_register_stage_ex 及独立 stage-name callback registry，原子迁移所有仓内消费者到显式 operation descriptor/provider。

**Architecture:** Graph operation registry/provider成为用户计算节点的唯一注册事实源，compile仅按DSL显式operation/resource解析；已编译计划仍缓存callback/context。Source/port/adapter内建契约不属于旧callback fallback，保留其合法行为。测试使用真实公开注册API，不让测试工具自动改写DSL或窥探Graph。

**Tech Stack:** C11、现有CFlow/CMeta语义、TinyTest与双Windows user presets。

**Spec:** 用户删除全部旧接口且无C/CMake fallback；https://github.com/qigao/turbo-flow/issues/95 。#73/#93 typed-operation DLL工厂及引擎解耦另行完成，本轮不虚称所有operation已由DLL加载。

## Global Constraints

- 删除 turbo_flow_register_stage_ex、flow_stage_registration_t、flow_find_registration、flow_registration_destroy、flow->registrations 的声明/实现/初始化/销毁/编译消费，无alias、wrapper、stub或stage-name隐式查找路径。
- 所有旧函数消费者必须改真实 operation descriptor/provider，并在DSL显式写operation identity；不得通过test helper扫描、改写DSL、修改私有stage结构或按节点名自动绑定。
- 保持正常route/reject、mutability、thread/coro/worker、queue/backpressure、ordering、async/source/terminal、reset和资源生命周期行为；原有有效覆盖不能为迁移而删除或弱化。旧API专用校验迁移为现代API边界校验。
- 保留现代 turbo_flow_stage_fn、options、emitting/keyed/provider API 中仍有实际用途的类型；不做纯命名大替换。source/port/adapter合法内建contract不误删。不改包2.0.0/pluginABI2.0布局、外部SDK、依赖或presets，不增生产公共API。
- 测试helper仅在tests中且不安装，不被benchmark/安装消费者依赖；它只能构造显式测试契约并调用公开注册API。生产/benchmark/安装消费者直接声明自身契约，不包装退休API形状。
- 先实际RED，再实现删除；apply_patch编辑，rg.exe/fd.exe检索；不提交scratch/.codegraph。按小范围回归后双profile完整build/CTest，报告真实exitcode/失败和warning。

## 影响、状态与迁移

HIGH/事实：compile_validate_registrations 同时读取 stage-name registrations 和 operation providers，前者仅由旧公开注册入口写入。全仓旧调用集中于测试、benchmark和一个安装CHTTP消费者；无独立业务生产调用。provider路径已经缓存fn/context且有operation domain/scope/runtime验证。

选择一次迁移全部消费者后删除registry；保留旧wrapper或自动把stage.name当operation.name会保留旧路径，违反用户要求。拆成源码删除但不迁移测试会让分支不可构建。可以在工作过程中先分组迁移并跑测试，但最终提交必须闭合。

HIGH/公开行为变化：旧函数无法编译/链接，纯callback节点必须同时声明operation契约和provider，并显式配置operation。DSL语法、已有现代绑定、内建source/port/adapter节点不变；不能自动修复缺失绑定。调用方负责声明真实权限/状态/执行能力，descriptor按既有API复制、callback/context由注册者拥有，flow销毁后才能释放context。没有新增生命周期或热路径registry查找；仅删除编译期的一套线性查找。

验证风险：默认测试契约不能把所有节点假称PURE、所有executor都允许或忽略runtime约束；pool、bounded worker/reject等由每个用例显式声明。旧core.stage语义推导断言需转成同语义显式operation断言，不能将原测试意图变成断言helper自己的输出。回滚为Git/完整旧版本部署，不提供运行时fallback。

### Task 1: Migrate all callers and remove stage registry atomically

**Files:** Modify turbo_flow/include/turbo_flow.h; turbo_flow/src/flow_core.c, flow_compile.c, flow_internal.h; 所有 rg命中的test/benchmark/install C消费者：turbo_flow/tests/{test_turbo_flow,test_flow_run,test_flow_domain,test_flow_async_emit,test_flow_managed_source,test_flow_keyed_state,test_flow_policy,test_flow_rulesforge,test_flow_control,test_flow_resource_document}.c，io/cnet/tests/{test_cnet_stream_source,test_cnet_listener_source,test_cnet_packet_source}.c，io/chttp/tests/{test_chttp_adapter,test_chttp_server_adapter,test_chttp_websocket_adapter,test_chttp_client_stop_fault,test_chttp_plugin}.c，io/turbodb/tests/{test_turbodb_adapter,test_turbodb_outbox_source}.c，codec/tests/test_turbo_flow_codec.c，observe/tests/test_turbo_flow_observe.c，schedule/tests/test_turbo_flow_schedule.c，ingress/protocol/tests/test_protocol_graph.c，turbo_flow/benchmarks/bench_turbo_flow.c，tests/install_chttp_plugin_consumer/main.c；tests/install_consumer/run.cmake；当前README/架构/测试文档中旧用法。必要时Create tests/flow_operation_fixture.h（不安装、不包含私有头）；按需要更新对应tests CMake include但不改变生产目标依赖。

**Interfaces:** 消费现有 turbo_flow_register_operation、turbo_flow_register_operation_provider，失败原样返回。测试可复用下述有边界fixture，避免几十处重复descriptor设置；不新增生产入口。

```c
typedef struct flow_test_operation_s {
  turbo_flow_operation_descriptor_t descriptor;
  turbo_flow_operation_provider_registration_t provider;
} flow_test_operation_t;
/* Defaults are explicitly inline, Message -> Message, private task/data mutation.
 * Callers override full descriptor/provider fields for each non-default contract. */
static inline flow_test_operation_t flow_test_operation_init(
    const char *operation_name, turbo_flow_stage_fn fn, void *ctx);
static inline int flow_test_operation_register(turbo_flow_t *flow,
                                               const flow_test_operation_t *operation);
```

helper默认descriptor size/version1/domain DATA、input/output DATA Message、flagsSTAGE、scope MESSAGE/PRIVATE/TASK/INLINE_LANE/DATA_MUTATION、execution_mask INLINE；provider使用现有INIT、相同operation_name/fn/ctx、默认READONLY无effects。register先注册descriptor再provider，任一步失败立即返回；不吞duplicate/错误、不隐式rollback伪成功；错误注册测试直接使用公开API，以免helper部分注册影响期望。pool/reject/worker/runtime契约由测试显式填字段，不从DSL/私有plan推导。helper仅供测试复用，benchmark和安装consumer各自直接构造公开契约。

- [ ] Step 1: 安装consumer现有退休导出集合增加 turbo_flow_register_stage_ex，并正向检查 turbo_flow_register_operation/provider；运行实际安装consumer RED，当前Graph会因导出旧API失败。记录真实exitcode和对应错误，命令/编译错误不算RED。
- [ ] Step 2: 先补现代绑定行为回归：显式operation可由不同stage名复用；仅注册同名operation/provider但DSL缺少operation时compile失败、callback0调用；missing descriptor/provider拒绝；duplicate和compile后注册拒绝、reset keep_registry保留或清除现代契约。使用真实Graph行为，先跑未迁移基线了解哪些已通过。旧API先保持直到消费者分组迁移完成。

```c
"source input\n"
"stage transformed operation test.transform\n"
"stage main {\n input -> transformed\n}\n"
/* Contract and provider identity = test.transform, not transformed. */
```

- [ ] Step 3: 按文件组迁移测试DSL和注册代码，保留callback和原有断言。简单inline测试用显式fixture；复杂thread/coro/worker/reject/settlement/typed模块测试声明真实完整descriptor。模板实例可以共享一个operation，但不同context必须用不同显式operation identity/resource，不得恢复stage-name binding。动态生成DSL同样写出显式operation。旧callback抢占catalog operation的负向测试改为未绑定模块的operation provider被拒绝；不得删掉module owner安全覆盖。registration重复/锁定/重置测试改覆盖现代registry；结构/图编译测试改查显式contract，同样保持效果/typed/lowering/拓扑语义。
- [ ] Step 4: 迁移benchmark和安装CHTTP消费者，以实际公开API注册descriptor/provider，明确inline/pool/worker资源契约，保留benchmark计量和校验。不得引用test fixture header或新增fallback。更新当前文档中的完整DSL/注册示例；历史计划不机械删除。不能把仍未实现的DLL装配说成已完成。
- [ ] Step 5: 删除旧公开函数和私有registry全部存储/生命周期/查找，compile移除reg_index及provider?reg分支，callback唯一来自显式provider；保留adapter consume/async/source/port合法路由、重试拒绝规则和module owner校验。清理错误文字中的退休stage callback选项；不为裸stage自动注册operation。仅移除确证死代码，不误删核心source/port/adapter契约推导。安装负向测试转GREEN，全仓有效源码/header/benchmark确认旧symbol无引用。
- [ ] Step 6: 两profileconfigure/fullbuild，先core/domain/run再所有受影响测试，最后完整CTest各一次（含安装consumer）。benchmark目标必须可编译；仅当会改计量语义时运行最小可选benchmark，不以本轮宣称性能收益。git diff --check及自审，显式提交完整scope。完整报告只写scratch，给实际RED/GREEN、失败修正、最终命令/exitcode/计数/警告及迁移限制。

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user && cmake --build --preset win-dev-user && ctest --preset win-dev-user --output-on-failure'
```

Release对应win-release-user。最小baseline regex ^(test_turbo_flow|test_flow_domain|test_flow_run)$；安装RED ^test_turbo_flow_install_consumer$。全程只写当前worktree，测试安装到既有build下stage，不修改外部SDK。
