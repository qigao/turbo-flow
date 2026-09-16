# Graph operation 精确布局实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** 完成 #103，拒绝旧 Graph operation descriptor 前缀及未知长布局。

**Architecture:** 注册入口先校验精确 size，再读取完整字段并走既有语义检查、字符串复制和 registry 提交。注册表仍是唯一事实源，不引入转换层，不改变执行方式。

**Tech Stack:** C11、TurboFlow Graph、TinyTest、CMake user presets。

**Spec:** https://github.com/qigao/turbo-flow/issues/103 （已读取验收内容）。

## Global Constraints

- 在任何尾字段读取或复制前，非精确 descriptor size 返回 SALTS_EINVAL，registry 不变。
- 删除 TURBO_FLOW_OPERATION_DESCRIPTOR_V1_SIZE，不添加 alias、forwarder、C/CMake fallback。
- 保留完整 descriptor 的现有资源版本语义和 C/C++、Graph compile、typed plugin 路径。
- 不改变 shared plugin ABI3，不改 #102 async-ingress，不安装外部 SDK，不迁移用户数据。
- HIGH 兼容风险：旧二进制消费者必须重编译；用户已明确授权删除旧接口。
- 验证必须使用 VS 环境中的 win-dev-user 与 win-release-user。

### Task 1: 精确注册边界与消费者验证

**Files:**
- Modify: `turbo_flow/src/flow_domain.c`，注册入口。
- Modify: `turbo_flow/include/turbo_flow_domain.h`，公开契约及旧常量。
- Test: `turbo_flow/tests/test_flow_domain.c`，真实注册回归。
- Test: `tests/install_consumer/main.c`，安装后的注册边界；既有 C++ 消费者正常编译运行。
- Test: `turbo_flow/tests/test_flow_plugin_operation_runtime.c`，同步依赖旧 normalization 的 Graph descriptor fault 期望；保留其他 generation 错误路径。

**Interfaces:** Consumes/produces unchanged `int turbo_flow_register_operation(turbo_flow_t *, const turbo_flow_operation_descriptor_t *)`。改变仅为 size 准入。

- [x] 将旧 V1 成功用例改为拒绝：用现有 operation_descriptor helper 构造完整有效描述，测试 size=0、sizeof(size_t)、offsetof(resource_min_version)、sizeof-1、sizeof+1、SIZE_MAX；每次断言 EINVAL、count 不变、name 查询未新增，再恢复完整 size 验证同名可成功注册。物理短对象另用 malloc(sizeof(size_t))，只初始化 size；旧前缀另分配 offsetof 大小并 memcpy 有效完整对象的前缀；验证输入字节不变并释放。禁止仅把完整对象改 size 当作物理短测试。

```c
check_equal(turbo_flow_register_operation(flow, &operation), SALTS_EINVAL);
check_equal(turbo_flow_operation_count(flow), 0u);
check_null(turbo_flow_find_operation(flow, operation.name));
operation.size = sizeof(operation);
check_equal(turbo_flow_register_operation(flow, &operation), SALTS_OK);
```

- [x] 保留资源版本语义覆盖：完整 descriptor 的 min=max=9 存储后仍为9；非法 min>max 返回 EINVAL。用独立 it 区分布局和版本约束。
- [x] VS 环境执行 `cmake --build --preset win-dev-user --target test_flow_domain`，再 `ctest --preset win-dev-user -R "^test_flow_domain$" --output-on-failure`，保存真实 RED；预计旧前缀/长结构被接受，而测试要求 EINVAL。
- [x] 删除 normalized 补零与 min-copy，按以下入口校验后直接使用 descriptor，并保留既有字符串所有权复制与错误处理。

```c
if (!flow || !descriptor || descriptor->size != sizeof(*descriptor)) return SALTS_EINVAL;
if (!flow_operation_descriptor_valid(descriptor)) return SALTS_EINVAL;
```

- [x] 删除旧公开常量，将注释改为 size 必须等于 sizeof；资源 min=0 注释描述当前 any-version 语义，不再借 V1 解释。
- [x] 同一 descriptor 的批量入口 `turbo_flow_register_module_contract` 在 loop 首先校验 `operations[i].size != sizeof(operations[i])` 返回 EINVAL，然后才读 name；`flow_operation_contract_compatible` 的 required size 同样改为 equality。独立测试 operation_count=1 的物理 size_t/旧前缀/超长结构，覆盖空 registry 和已正常注册的幂等快捷路径，断言 operation/module count 不变；正常幂等和版本漂移 EPROTO 测试保留。此项只封闭相同 descriptor 的另一准入路径，不改变 module descriptor 自身 ABI。
- [x] 安装消费者实际调用注册 API：在现有有效 metadata 注册前，对旧前缀和超长 size 验证 EINVAL、count不变，再恢复精确 size 继续已有真实 DLL 执行。每个失败返回明确错误并走既有 cleanup，不新增 CMake helper。
- [x] 先重复定向 GREEN，再完整 build/CTest 两 profile（包含安装 C/C++ 消费者）。现有安装测试清理路径须先确认在本 worktree build 内；保留完整日志，说明既有告警与覆盖限界。
- [x] 自审 git diff、检索旧常量及相关兼容注释，提交实现、测试、公开文档与本计划。报告真实 RED/GREEN 命令、结果、文件及风险；不得提交 scratch。

## 验收映射

短/旧/长布局拒绝及注册无副作用由 Task1 边界测试覆盖；完整资源版本、Graph 与 typed plugin 行为由正常注册与完整套件覆盖；安装 API 由现有安装消费者扩展覆盖。无需新增依赖、公开签名或持久化格式。回滚只用显式代码 revert，不在运行时保留旧路径。
