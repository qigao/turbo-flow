# ADR：以统一动态模块交付 CHTTP 能力

状态：实现已交付；验证范围见 [README](README.md) 和对应测试。

## 背景与选择

Gateway 需要通过 catalog/generation 装配 HTTP client、deferred server 和
WebSocket Source/Sink。直接依赖具体 Flow adapter 会让部署消费者持有实现类型；
将三种能力分别做 DLL 又会增加注册与部署组合。

选择一个仅导出 `turbo_flow_plugin_get_api` 的 `tf_chttp_plugin`，
在一次 host 注册事务中发布三个版本化 provider。复用已有 CHTTPAdapter 和
Salts CHTTP，避免重复实现 HTTP、TLS 或生命周期。
候选“Gateway 直接链接 adapter”继续作为已有 API 的使用方式，但不满足动态装配目标；
候选“三个 DLL”并无当前独立升级需求，不引入其额外部署复杂度。

## 边界与权衡

领域 Graph、Product 配置与 PluginHost 保持单向依赖；统一插件承载格式到具体
adapter 的适配。配置 schema 在 materialization 前完成严格校验，
generation 管理事务性 owner 和 DLL snapshot 的生命周期。
client 使用原 snapshot，不建立插件计数镜像。

代价是增加部署 DLL、匹配 profile 的动态依赖和显式控制线程 poll 要求，
并限制配置为当前 schema 能完整验证的能力。获得的是可卸载模块边界、
配置失败前无 native 创建，以及多 owner 失败后可检验的逆序回滚。
这些是结构与行为取舍，不是性能提升结论。
server/WS 的路由和终端限制保持原 adapter 行为，Graph compile 的拓扑错误仍可能
出现在接管 Flow 之后，此时必须销毁接管的 Flow 并精确回滚。

## 状态与错误

host 的 catalog 是 provider 注册事实源；generation 持有已创建 owner；
native adapter 负责其请求、session、claim 与网络状态。
TLS 借用存储跟随 owner，host 分配的内存回到原 allocator。
控制线程按 preflight → materialize/compile → start/poll → quiesce/stop/drain →
detach/destroy 推进。未结算工作或显式 lease 阻止提前卸载。
资源不足、配置错误、协议/TLS 不匹配直接报错，不引入 C、CMake、协议或静态 fallback。

## 验证与迁移

独立 installed consumer 只链接 PluginHost/Product/Graph，实际加载安装 DLL，
验证三种能力的 generation 生命周期和显式 lease；导出与依赖表作为 Windows 安装门禁。
in-tree native peer 验证真实 H1/H2、RFC8441、TLS/ALPN、共享 H2 连接和 peer 单流取消。
test-only DLL 使用真实 provider 代码验证第二 owner 分配失败的 rollback 与
allocator 配对，并制造不同 module identity 的第二/第三 kind 注册冲突。

现有直接 API 和 DSL 不改写。迁移先验证新模块和 schema，再停止旧 admission，
结算旧 generation 后切换。失败保留有引用的模块，不强行卸载。
回滚必须先完成新 generation teardown，再恢复旧模块与 binding；
不靠静态实现或旧配置自动降级。新增字段或公开语义需要单独评估 ABI/schema 版本，
本次不引入配置迁移格式，也不自动修改旧配置。
