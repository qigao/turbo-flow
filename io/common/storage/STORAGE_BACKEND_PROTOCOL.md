# Storage Backend Protocol

本文档是 `TurboFlow::StorageBackend` 的规范性协议。内置 local、Redis、PostgreSQL 以及后续的
SQLite/远程 provider 都必须同时满足本协议和各自的数据模型契约。

## 1. Boundary

插件边界是纯 C、版本化的 `turbo_flow_storage_backend_plugin_api_t`。动态模块必须导出
`turbo_flow_storage_backend_plugin_get_api`，function table 只允许包含：

- `open()`：根据模型和配置创建一个 service；
- `close()`：销毁该 service 及其私有 owner。

数据库连接、命令、Hash/Index/Log 实现和 allocator 不得穿过该边界。宿主只接收
provider-neutral FlowStore facade 的 opaque 指针。

## 2. Versioning

所有跨边界结构体都以 `size` 开头并带有 `version` 或 `abi_version`：

- major version 不兼容时拒绝加载；
- minor version 可向后兼容地增加尾部字段；
- 宿主按 `size >= sizeof(required_prefix)` 检查，不能要求 buffer 大小精确相等；
- 未被当前版本理解的尾部字段必须保持不变并忽略。

插件 API 的 `backend` 名称是注册表唯一键，不能为空；重复注册返回 `TURBO_EALREADY`，
超过 registry 容量返回 `TURBO_ENOSPC`。

## 3. Capability negotiation

插件必须只声明实际实现的 model capability。宿主在调用 `open()` 前完成能力检查：

| Model | Capability |
| --- | --- |
| Record | `TURBO_FLOW_STORAGE_CAP_RECORD` |
| State | `TURBO_FLOW_STORAGE_CAP_STATE` |
| Index | `TURBO_FLOW_STORAGE_CAP_INDEX` |
| Log | `TURBO_FLOW_STORAGE_CAP_LOG` |
| Series | `TURBO_FLOW_STORAGE_CAP_SERIES` |

能力缺失必须返回 `TURBO_ENOTSUP`，不得连接后静默降级到另一个模型或 memory backend。

内置 `local` backend 通过同一 registry/owner ABI 提供 `Record|State|Index|Log|Series` 五种
模型。它是进程内 volatile 实现：close 或进程退出后数据不可恢复，Record 只声明
`TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH`，不声明 `TURBO_FLOW_RECORD_STORE_DURABLE`。因此
`local` 不能作为 Flowie 的 durable `session_store`；需要会话恢复时必须选择声明 durable 的
Redis 或 PostgreSQL Record backend。`local` 是 registry 中唯一的内置进程内 backend 名称。

## 4. Open contract

`open()` 的输入 request 和 options 在调用期间由宿主借用，插件不得保存这些指针。
成功时必须填充：

- `service.size` 至少为当前 service 结构大小；
- 正确的 `abi_version` 和 `model`；
- 非空 `service.instance`；
- 非空、只供插件 `close()` 使用的 `service.owner`。

失败时不得发布半成品 service。若插件已经分配了 owner，必须仍可由 `close()` 清理；宿主
会在失败路径调用一次 cleanup。

## 5. Ownership and lifecycle

成功 open 返回的 owner 是唯一生命周期所有者。service 指针在 owner 销毁后立即失效，
宿主和调用方都不得保存或释放它。

生命周期顺序固定为：

```text
registry register/load -> owner create/open -> use service -> owner destroy/close -> unload
```

活动 owner 存在时 registry destroy 必须返回 `TURBO_EBUSY`。每个成功 open 必须恰好对应
一次 close；close 后 service 指针必须清空。插件模块只有在所有 owner 销毁后才能卸载。

## 6. Errors and shutdown

协议错误和资源错误必须原样向宿主传播，不得记录后返回成功：

- `TURBO_EINVAL`：ABI、options 或配置格式非法；
- `TURBO_ENOTSUP`：backend/model capability 不支持；
- `TURBO_EPROTO`：插件返回的 service 违反 ABI；
- `TURBO_ESHUTDOWN`：已关闭 service 的公开操作；
- `TURBO_EBUSY`：冲突、活动 owner 或 backend 明确要求串行化；
- `TURBO_ENOSPC` / `TURBO_EFBIG`：容量或单项限制；
- `TURBO_EIO`：外部存储 I/O 失败。

Provider facade 的所有读、写、查询、stats、bounds、range 和 aggregate 操作，在 close 后
都必须返回 `TURBO_ESHUTDOWN`；不得把 stats 或查询作为隐式例外。

## 7. Data and concurrency rules

每个领域状态只有一个事实源。memory、Redis、PG 和缓存不能各自推进同一业务状态。
写入命令必须在校验容量和不变量后一次提交；失败不得留下半状态。读取返回只读快照或
调用方拥有的 copy-out 数据。

Provider 默认是 single-owner、caller-serialized。若实现内部使用线程池或连接复用，必须
在 provider 文档中说明锁边界、回调线程和关闭等待规则；宿主不能从 function table 推断
线程安全。

## 8. Conformance requirements

每个 backend 在合并前必须通过 `test_storage_backend_registry` 及 backend-specific tests，
至少覆盖：

1. API version/size validation；
2. duplicate registration 和 registry capacity；
3. capability mismatch 在 open 前被拒绝；
4. open 失败后的部分资源清理；
5. close exactly once、重复 close 和 close 后所有操作；
6. active owner 阻止 module unload；
7. options 尾字段兼容和非法 options 拒绝；
8. backend 错误、容量错误和协议错误的返回值保持稳定。

测试通过只是协议合规的必要条件，不代表 provider 的性能、部署安全或数据库高可用性
已经得到保证。

## 9. Migration

direct-factory 不属于本协议，Redis/PG 的旧 direct-factory 已删除。新 provider 必须直接
实现本协议；调用方通过 registry 注册 backend、创建 opaque owner，并从 owner 获取 typed
provider-neutral facade。公开 API 的迁移不再提供 direct-factory ABI 兼容层。
