# Storage Backend Function-Table ABI

## Decision

Flowie、FlowStore 以及 local/Redis/PostgreSQL 适配器通过 `io/common/storage` 的统一 C ABI
协作。插件只导出一个版本化的 `turbo_flow_storage_backend_plugin_api_t`，其中公开的
backend operations 只有：

- `open()`：按模型和配置创建一个 provider-neutral service；
- `close()`：释放该 service 及其插件私有 owner。

模型能力通过 capability bitset 协商。宿主在调用 `open()` 前检查所需 capability，因而
不支持的模型会稳定返回 `TURBO_ENOTSUP`，不会连接后再失败。

## Ownership and lifetime

注册表借用静态 API，拥有动态加载模块。每次成功 `open()` 都由一个 opaque owner 持有；
owner 销毁时恰好调用一次 `close()`。活动 owner 存在时，registry destroy 返回
`TURBO_EBUSY`，避免卸载仍在执行代码的插件。插件 `open()` 在失败路径若已初始化
service，宿主也会调用 `close()` 清理部分状态。

`service.instance` 只承载 provider-neutral 的 FlowStore facade，`service.owner` 只供
插件 close 使用。两者均为借用指针，不能跨 owner 生命周期保存。

## Visibility boundary

Redis/PG 的具体 `create/destroy` 函数、连接对象和数据库操作不属于插件 ABI。它们只在
各自 `src/*_storage_internal.h` 中声明，由 backend adapter 内部调用；面向插件宿主的
backend 头文件仅提供配置结构、能力声明和 function-table accessor。具体 create/destroy
符号不再作为公开 API 导出，因而不能绕过 registry owner 生命周期。这样可以替换内部实现
而不改变 Flowie 或动态插件宿主的 ABI。

## Migration and compatibility

现有 provider-neutral `turbo_flow_record_store_t`、State/Index/Log/Series facade API 保持
不变。Redis/PG 旧 direct-factory 已移除，不再保证其 ABI；新装配路径统一通过 registry
owner 创建。插件 API 主版本变化时
拒绝加载，次版本允许宿主按 `size` 进行兼容扩展。

旧的 `flowie_record_store_plugin.h` / `flowie_record_store_plugin_get_api` ABI 已删除，
Flowie 动态加载器只解析 `turbo_flow_storage_backend_plugin_get_api`。命令行仍接受
`--record-store-plugin` 作为迁移别名，但它加载的也必须是新的 StorageBackend ABI；新的部署
配置应使用 `--storage-backend-plugin`。

## Alternatives and trade-offs

让宿主直接依赖 Redis/PG 的 concrete functions 会减少一次间接调用，但会暴露连接和错误
语义、阻止动态替换，并把数据库依赖传入 Flowie。把 function table 放在 Redis 或
FlowStore 会造成跨 provider 依赖；`io/common/storage` 是三者共享的稳定边界，额外的
opaque owner 和一次函数指针调用换取了生命周期隔离和可测试性。

## Rollback

若插件 ABI 需要回滚，宿主可在配置层禁用动态 backend，并继续使用已注册的 builtin API。
不得在插件边界重新暴露具体数据库操作函数；local 也必须通过 registry owner 加 typed
facade，不能让 Flowie 直接依赖 memory store create/destroy。
