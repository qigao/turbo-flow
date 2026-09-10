# Module-adapter 精确布局实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** 完成 #106，删除 module-adapter V1/V2 前缀补齐与未知长尾截断。

**Architecture:** 单一公开注册入口先检查精确 size，再执行既有契约校验与原子注册。registry 仍拥有成功转移的 ctx；错误不新增 adapter/resource/primitive/binding，不转移 ctx。

**Tech Stack:** C11、TurboFlow Graph、TinyTest、CMake user presets。

**Spec:** https://github.com/qigao/turbo-flow/issues/106 （已读取验收）。

## Global Constraints

- 精确 sizeof 之外的 size 均在任何尾字段读取/复制前返回 SALTS_EINVAL。
- 删除 TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_V1_SIZE、TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_V2_SIZE、normalized 补零/min-copy/size重写；无 alias/forwarder/C/CMake fallback。
- 拒绝保持所有 registry 与已有注册状态不变；ctx 仍归调用者，不调用 shutdown。
- 完整当前注册仍支持显式 consume/consume_batch、observer逐消息通知、资源绑定和原子回滚；不禁用正常功能替代迁移。
- HIGH 兼容风险：旧调用者必须迁移并重编译，这是用户明确授权的去兼容目标。
- 不改 shared plugin ABI3、YAML、公开签名或 CMake，不修改已完成 #102/#103，不安装外部 SDK或删除用户数据。
- VS 环境、win-dev-user 与 win-release-user；安装验证使用既有暂存安装 consumer。

### Task 1: 精确注册入口与消费者迁移

**Files:**
- Modify: `turbo_flow/src/flow_core.c`，只移除注册入口的 normalization。
- Modify: `turbo_flow/include/turbo_flow.h`，删除旧前缀宏并明确精确 size/失败所有权契约。
- Test: `turbo_flow/tests/test_flow_domain.c`，真实短对象拒绝、回滚/ctx与完整单条/批量行为。
- Test: `tests/install_consumer/main.c`，实际 C/C++ 公共注册路径。

**Interfaces:** 原 `turbo_flow_register_module_adapter(flow, registration)` 签名不变；消费既有 module/operation/schema，成功后既有 registry shutdown 负责 ctx 生命周期。

- [ ] 在现有 module/operation/adapter fixture 基础上构造合法完整 registration，禁止用其他字段本就无效的对象代替布局拒绝测试。验证 size=0、sizeof(size_t)、offsetof(operation_resource_names)、offsetof(consume_batch)、sizeof-1、sizeof+1、SIZE_MAX；每次 EINVAL 且输入字节和四类 registry 数量/已有内容不变、ctx shutdown计数不变。分别覆盖空 adapter registry 与保留一个不同有效 adapter 的 registry。
- [ ] 独立 malloc(sizeof(size_t)) 与 malloc(两个旧 offsetof 前缀) 的物理短对象，只复制分配范围。拒绝后比较该范围字节不变；释放由测试调用者执行。恢复完整 registration 后同名成功注册，销毁 Flow 才触发成功转移 ctx 的一次 shutdown。测试必须清楚区分拒绝对象与已注册对象的 ctx 计数。

```c
check_equal(turbo_flow_register_module_adapter(flow, invalid), SALTS_EINVAL);
check_equal(memcmp(invalid, before, allocation_size), 0);
check_equal(turbo_flow_register_module_adapter(flow, &valid), SALTS_OK);
```

- [ ] 将旧 mixed observer/older-registration 测试拆开：有效完整 registration + observer，publish_batch四条仍调用scalar四次且observer通知四次、native batch为0；独立完整consume_batch正向路径保留，旧V2改为注册期拒绝，不能删除observer测试来掩盖语义差异。无observer且显式consume_batch=NULL的当前完整注册也应保留单条处理能力，不能依赖旧size来表达。
- [ ] 先 `cmake --build --preset win-dev-user --target test_flow_domain`，再 `ctest --preset win-dev-user -R "^test_flow_domain$" --output-on-failure`，直接重定向完整日志保存真实 RED（旧前缀/长布局被接受，预期EINVAL）。不要把编译错误当行为RED。
- [ ] 最小实现：删除 normalized 局部对象/清零/复制/重指向，在入口改为精确 size。其余字段校验、回滚、生命周期和 batch策略不改。

```c
if (!flow || !registration || registration->size != sizeof(*registration)) {
  return SALTS_EINVAL;
}
```

- [ ] 删除两个旧常量和无效兼容注释；公开注释写明精确 sizeof、失败不读尾字段/不转移ctx；默认INIT仍为当前完整结构。
- [ ] 安装consumer创建有效的最小 module/operation/adapter，用现有公开 API 实际验证旧两个前缀/超长被拒绝，随后完整注册成功；由main无条件调用，C与C++都运行。可复用已存在callbacks/fixtures概念，不引入生产测试API、CMakehelper或依赖；不使用C++不支持的C复合字面量赋值。
- [ ] 定向GREEN，随后完整 build/CTest两profile（含Product、Graph、Source/Sink及安装消费者）。日志完整重定向；先确认安装测试清理目标位于本 worktree build/Msvc或Msvc-Release/install-consumer-test，不触及SDK。
- [ ] 自审 diff、旧常量全仓引用与相关回滚测试，显式文件清单提交计划/源码/头/测试，禁止git add-f或提交.superpowers。报告完整RED/GREEN命令、出口码、结果、告警和覆盖限界，留本计划scratch供独立审查。

## 验收与风险

全部 #106 验收由 Task1 覆盖：精确布局、无副作用/所有权转移、正常单条/批量/观察者与回滚、安装C/C++、双profile。状态归属不变，失败保持调用前状态，调用者可用完整对象重试。回滚只允许显式代码revert/整体部署已验证产物，不在当前运行时保留旧转换。
