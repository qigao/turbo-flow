# Async ingress 精确布局实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** 完成 #102，删除 Graph ingress 配置输入补默认值与 Product 输出前缀写入的旧 ABI 路径。

**Architecture:** 两个公开边界只接受完整精确 size；读取/写入尾字段前拒绝非法布局。有效配置仍先在局部验证，再一次性提交，Flow 配置与 resolver 文档各自保持既有事实源与所有权。

**Tech Stack:** C11、TurboFlow Graph/Product、TinyTest、CMake user presets。

**Spec:** https://github.com/qigao/turbo-flow/issues/102 （验收已读取）。

## Global Constraints

- 删除 TURBO_FLOW_ASYNC_INGRESS_CONFIG_V1_SIZE 及短布局补默认/只写前缀路径；完整 size 精确匹配，未知长布局返回 SALTS_EINVAL。
- 非法布局在任何尾字段访问前失败，不改输入、输出、Flow 已有配置或状态。
- 保留当前完整配置 INIT 默认值、YAML resolver 默认展开、显式字节额度及背压行为；不禁用功能，不改变 YAML 格式。
- 无 C/CMake fallback、alias、forwarder、隐式结构升级；不改变 shared plugin ABI，不安装外部 SDK、不删除用户数据。
- HIGH 兼容风险：旧消费者必须迁移并重编译，这是用户已授权的去兼容目标。
- 构建和测试在 VS 环境使用 win-dev-user、win-release-user；安装消费者仅使用现有暂存安装测试。

### Task 1: 输入/输出精确边界与完整回归

**Files:**
- Modify: `turbo_flow/src/flow_async_ingress.c`，配置验证后提交。
- Modify: `turbo_flow/src/flow_product.c`，完整查询结果原子复制。
- Modify: `turbo_flow/include/turbo_flow.h`、`turbo_flow/include/turbo_flow_product.h`，精确布局和失败不变契约。
- Test: `turbo_flow/tests/test_turbo_flow.c`、`turbo_flow/tests/test_flow_config.c`，物理短对象/正常路径。
- Test: `tests/install_consumer/main.c`，安装后 C/C++ 公共 API。

**Interfaces:** 签名不变：`turbo_flow_configure_async_ingress(flow, config)`、`turbo_flow_resolved_config_runtime_ingress(config, ingress)`。调用者仍拥有输入与查询输出；Flow 复制配置，查询只读 resolver 快照。

- [ ] 修改旧接受测试为精确拒绝。在已安装完整非默认配置（workers=2, capacity=7, max_message_bytes=4096, max_inflight_bytes=8192）的 Flow 上，用完整结构分别声明 size=0、sizeof(size_t)、offsetof(max_message_bytes)、sizeof-1、sizeof+1、SIZE_MAX；再单独分配真实 sizeof(size_t) 及真实旧前缀对象。失败返回 EINVAL，输入字节、Flow 配置所有字段与 state 均不变，async pool 不创建。恢复当前完整 INIT 可再次正常配置。真实短对象只能初始化分配范围内字节，不读不存在尾部。

```c
check_equal(turbo_flow_configure_async_ingress(flow, invalid), SALTS_EINVAL);
check_equal(flow->async_ingress_config.workers, 2u);
check_equal(flow->async_ingress_config.queue_capacity, (size_t)7);
check_equal(flow->async_ingress_config.max_message_bytes, (size_t)4096);
check_equal(flow->async_ingress_config.max_inflight_bytes, (size_t)8192);
```

- [ ] 在 test_flow_config 使用真实 resolver 的有效文档，给输出做相同非法 size 矩阵和两种真实短分配，调用查询要求 EINVAL 且分配范围字节不变。查询后继续用完整 INIT 读出正确显式值，确认快照仍有效；保留现有默认/部分YAML展开/显式额度测试。完整输出字段填充应使用独立手算值，不从实现求 expected。

```c
check_equal(turbo_flow_resolved_config_runtime_ingress(config, invalid), SALTS_EINVAL);
check_equal(memcmp(invalid, before, allocation_size), 0);
```

- [ ] 先构建 `test_turbo_flow test_flow_config` 并运行 `ctest --preset win-dev-user -R "^(test_turbo_flow|test_flow_config)$" --output-on-failure`；保存真实 RED（旧前缀或超长得到 OK，而预期 EINVAL），不能用人为占位失败。
- [ ] 输入 resolver 删除兼容判断，在精确 size 后复制完整字段，再按既有 workers/capacity/字节额度约束验证。可直接局部结构赋值，不再从默认值补旧字段。

```c
if (!config || !resolved || config->size != sizeof(*config)) return SALTS_EINVAL;
effective = *config;
```

- [ ] Product 输出入口精确 size，不保留 output_size 与短输出分支；只有全部解析/范围验证成功后才 `*ingress = resolved`。错误码与成功的配置内容保持既有语义。
- [ ] 删除旧常量和 V1 兼容注释。两公开接口明确 size 必须等于 sizeof，失败保持调用者对象/配置不变，不新增版本转换函数。
- [ ] 安装消费者增加实际输入与查询调用，至少旧前缀、未知长报 EINVAL，随后完整配置/查询成功；复用现有 cleanup，保证 C 与 C++ consumer 都执行该验证，不仅放在仅 C 的 fixture 条件内。无需新增 CMake 或依赖。
- [ ] 定向 GREEN 后运行相邻 Graph/async Source 回归（`test_flow_run`、`test_flow_async_terminal`、`test_flow_async_emit`、`test_flow_managed_source`），再完整 build/CTest 两 profile（含安装消费者）。使用既有 VsDevCmd，先核实安装测试清理路径为当前 worktree build/Msvc 或 Msvc-Release/install-consumer-test。不得复制 runtime DLL 或覆盖 SDK。
- [ ] 自审 diff、检查旧常量/兼容注释已删除、保留所有正常容量和字节 reservation 释放测试。只用显式文件清单提交计划、实现、头文件与测试；禁止 git add -f、禁止提交任何 .superpowers 报告或日志。完整报告留本计划 scratch，注明 RED/GREEN、每条命令/结果及限界。

## 验收与迁移

Task1 同时覆盖输入、输出、短/长/完整布局、状态不变、默认/显式额度、安装消费者及双 profile；不引入新状态或同步模型。失败保持旧有效配置/输出原样，调用者可用正确完整对象重试。回滚为显式 revert 并部署完整已验证产物，不在当前实现内保留旧代码。
