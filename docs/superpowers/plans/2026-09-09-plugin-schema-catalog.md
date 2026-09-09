# Plugin schema catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 #73 提供完整、可独立消费的事务化 schema 注册与 leased snapshot 查询，不宣称 typed-operation 执行或引擎迁移已完成。

**Architecture:** 在现有 PluginHost 注册事务中追加 schema 类别，复用其首错、回滚和模块 lease。新头只依赖 PluginHost/CMeta，不依赖具体引擎。

**Tech Stack:** C11、CMeta、CSTL、TinyTest、Windows Debug/ASan 与 Release user presets。

**Spec:** [typed-operation-plugins.md](../../architecture/typed-operation-plugins.md)

## Global Constraints

- 不新增 RulesForge/TurboScript 引擎 fallback、静态入口或第二套模块 registry。
- 新字段尾部扩展，ABI minor 从 3 增至 4；新 schema 类别要求 ABI minor 至少为 4。
- 原 1.3 config 前缀未提供 schema 容量时，容量为零，注册返回 SALTS_ENOSPC。
- schema/version 组合在一次 host 生命周期内唯一；不同版本可以显式共存。
- schema metadata 和可达 callback 均由 DLL 持有；snapshot 活着就不得卸载。
- 本步骤只提供 metadata，不注册尚无实现的 executable capability。

## Task 1: 事务化 schema catalog

**Files:**
- Create: `turbo_flow/include/turbo_flow_plugin_operation.h`
- Modify: `turbo_flow/include/turbo_flow_plugin.h`, `turbo_flow/src/flow_plugin.c`, `turbo_flow/CMakeLists.txt`
- Create: `turbo_flow/tests/plugin_schema_fixture.c`, `turbo_flow/tests/test_flow_plugin_schema.c`
- Modify: `turbo_flow/tests/CMakeLists.txt`

**Interfaces:**

```c
typedef struct turbo_flow_plugin_schema_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t schema_version;
  const cmeta_data_desc *data;
} turbo_flow_plugin_schema_v1_t;

typedef struct turbo_flow_plugin_schema_catalog_v1_s {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  const turbo_flow_plugin_schema_v1_t *schemas;
  size_t schema_count;
} turbo_flow_plugin_schema_catalog_v1_t;

int turbo_flow_plugin_catalog_snapshot_schema_catalog(
    const turbo_flow_plugin_catalog_snapshot_t *snapshot,
    turbo_flow_plugin_schema_catalog_v1_t *catalog_out);
```

- [ ] 添加真实 DLL fixture 与 TinyTest，先运行目标，确认新头/接口尚不存在导致构建失败。Fixture 通过唯一导出注册 CMeta int schema；重复条目、错误版本、非法 descriptor 与吞掉注册错误使用独立 DLL 变体。
- [ ] 核心用例按如下断言验证真实加载和卸载边界；host/config/error/snapshot 均由测试独立初始化，最后销毁 snapshot 再销毁 host。

```c
check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_GOOD, &error), SALTS_OK);
check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &catalog), SALTS_OK);
check_equal(catalog.schema_count, (size_t)1);
check_equal(catalog.schemas[0].schema_version, 1u);
check_true(cmeta_type_equal(catalog.schemas[0].data->storage_type, cmeta_type(int)));
check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_EBUSY);
```

- [ ] 实现 descriptor、容量、capability 位和 add_schema 回调。验证 size、major、minor>=4 且不高于当前 host、schema_version 非零、CMeta descriptor/storage type 有效、stable_id 有界且合法。重复组合返回 SALTS_EALREADY，零容量返回 SALTS_ENOSPC，非法输入返回 SALTS_EINVAL。
- [ ] 用现有 CSTL vec 保存 wrapper；add_schema 的失败写入 registration.first_error。load 成功时将实际 schema capability 与声明一起校验，任何失败调用原事务 rollback 并撤销 schema tail。
- [ ] snapshot 创建时复制 schema wrapper 数组，沿用现有所有模块 lease。查询验证输出 size/version 后写入只读数组及数量；不转移 metadata 所有权。销毁 snapshot 时释放该数组；host 清理也销毁 schema vec。
- [ ] 验证零容量、旧配置前缀、重复 schema、吞错回滚后可成功加载、错误 descriptor/ABI、旧 snapshot 不被后续注册改变、CMeta 跨 DLL 语义相等。
- [ ] 运行 `cmake --build --preset win-dev-user --target test_flow_plugin_schema test_flow_plugin_host test_flow_plugin_generation`，再 `ctest --preset win-dev-user -R test_flow_plugin --output-on-failure`；Release 使用对应 `win-release-user`。所有 Windows 命令从 VsDevCmd 环境运行。
- [ ] 对新测试执行 5 次重复，检查 C++ 头消费和安装头清单，再提交 `feat(plugin): add transactional schema catalogs`。全部步骤完成前不合并；#73 后续 operation 工厂、generation、引擎、安装依赖解耦仍按 Spec 跟踪。
