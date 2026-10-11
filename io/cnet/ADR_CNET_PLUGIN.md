# ADR: CNet 统一 Provider DLL

## 状态

Accepted。六个 provider 的 canonical factory 迁移已由
[#252/#273](https://github.com/qigao/turbo-flow/issues/252) 落地；NetworkIntake 消费端迁移由
[#274](https://github.com/qigao/turbo-flow/issues/274) 跟踪。下面记录当前契约，替代 #69 的
transactional PluginHost 发布方式。

## 背景

`tf_cnet_adapter` 已提供 TCP、TLS、Pipe、listener、UDP、KCP 与 secure-KCP/FEC 的具体
Source/Sink owner。Product 装配通过明确的 provider identity、typed config 和 deployment resource
获取能力；模块、配置、网络句柄分别有独立的生命周期权威。

## 决策

`tf_cnet_plugin` 使用 Salts Plugin manifest 发布六个 `turbo_flow_provider_factory` Interface，
不再导出旧 `turbo_flow_plugin_get_api`：

- `cnet.stream_source`
- `cnet.listener_source`
- `cnet.packet_source`
- `cnet.stream_sink`
- `cnet.datagram_sink`
- `cnet.packet_sink`

每个 factory 完成 contract、preflight 和 materialize；结果是独立的 runtime owner Interface。
插件根以有界 owner 计数控制接纳和停止。Product 通过显式 resolver 从 generation 所持的
Component scope 获取 factory；compiled instance 持有 typed config 和 deployment resource lease，
并保存 materialize 返回的 runtime owner。具体 CNet adapter 仍独占网络句柄、CFlow run、Actor
和 terminal claim。

Graph 按 `.flow` 的 stage instance 绑定 provider materialization；同一个 provider identity 的两个
instance 不共享全局 adapter alias。缺模块、provider、resource、容量或配置必须在装配边界失败，
不恢复旧 catalog 发布，也不选择备用 transport。

## 配置和副作用边界

六个 kind 使用生成的 `schema_version = 2` typed config。`.flow` stage 中的 literals 经 MessagePlan
绑定；endpoint、TLS/PSK 材料从显式 deployment resource Interface 借用。未知字段、错误类型、
无效 backend、越界容量、互斥 TLS/packet policy 或不满足 CNet KCP/FEC 约束的配置在 preflight
阶段失败。配置只在 provider 适配层转换为 `cnet_*_config`，不会进入 Graph 领域层。
`command_buffer_bytes` / `event_buffer_bytes` 为 `0` 时明确采用 CNet 按已校验容量推导的有界存储；
队列索引和派生存储乘法在 preflight 阶段受限，显式值必须满足 provider 的保守载荷下限。CNet 公共纯
validator 与最小容量常量由
[Salts #250](https://github.com/qigao/salts/issues/250) 跟踪；其落地后应删除本层镜像约束并改用
CNet 单一事实源。

Provider preflight 只解析和校验。materialize 验证精确的 stage identity 和角色，再分配 owner、
注册具体 adapter。整个 generation 装配失败时，按既有可重试清理契约回收，Graph 保持未发布
状态。网络 bind/connect/listen 只发生在 `turbo_flow_start()` 的 adapter start 阶段。

三个 Source 使用 managed-run open：Flow 持有 run，CNet Source 持有输入 endpoint 和 demand
handoff。初始 demand 只在 generation 第一次 external poll 时注入，避免 Flow 尚未完全 STARTED
时推进输入。Managed boundary snapshot 从具体 Source 的只读 snapshot 映射实际 demand、队列、
in-flight、接收计数和错误；不从配置推测运行状态。stream/datagram/packet 三种 Sink 均通过
combined registration 原子发布 adapter、metadata 与 managed boundary。packet Sink（#80）
保留 UDP send completion / KCP ACK 结算，统一投影 Actor 队列和累计结算计数。

## Source 传输准入契约（#274）

listener 和 packet factory materialize 后，在精确 Source stage 上登记
`turbo_flow_source_transport_contract_t`：transport、session capacity、单消息字节上限、每次
external poll 的交付条数上限。Graph 复制无指针值；登记只允许在 PARSED 阶段且每个 adapter
owner 一次。查询不调用 provider、不扫描全局 alias；reset 删除 scoped registration，调用方
已取得的值副本继续有效。登记和 reset 需与读取互斥；该契约不改变 control-thread poll 归属。

容量来自已验证的 runtime config 和 adapter 存储结构：

| Source | 会话上限 | 每次 poll 的消息上限 |
| --- | --- | --- |
| TCP listener（含 TLS） | `max_connections` | 2：native progress 前后各一个 ready slot |
| UDP / KCP packet | `session_capacity` | `2 * queue_capacity`，乘法溢出返回 `SALTS_ERANGE` |

两轮 scheduler 之间才运行 native progress，排队消息不会在同一轮消费中被网络线程重新填充。
不能把 `2 * scheduler_max_steps_per_poll` 当成消息预算：Salts 的一个 scheduler task 可处理多条
Publisher 值。正式 UDP 测试将 4 条消息排队，在 steps=1 的一次 poll 中交付全部 4 条，覆盖这个
区别。契约不计协议拆帧后的输出条数，也不承诺关闭阶段的排空条数上限。

上层可在 materialize 后、start 前用
`turbo_flow_provider_adapter_source_transport_query(flow, "wire", &limits)` 取得契约；`limits`
先用 `TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT` 初始化。不存在的 stage 返回 `SALTS_ENOENT`，
已绑定但未提供此能力的 Source 返回 `SALTS_ENOTSUP`；查询失败不覆盖输出。

本步骤先提供并验证 producer 契约。NetworkIntake 仍需在 #274 后续迁移中删除 resolved CNet
字段读取，使用该契约检查 pending claims/bytes，并保留 compiled instance 和 Component scope。
不把当前旧 consumer 的 scheduler 预算视为正确的 UDP 容量证明。

选择复制 adapter 契约，避免 intake 依赖 CNet 私有 typed config 或给所有 factory 增加网络
专属方法。现有注册结构、factory Interface 和直接使用 CNetAdapter 的调用保持兼容；新查询是
additive C ABI。回滚需同时回滚依赖该契约的 consumer，不能用读取旧配置作为运行时 fallback。

## 停止、排空和卸载

停止顺序固定为：quiesce generation owner、停止 Graph adapter、排空 Graph/CNet terminal、shutdown
Graph registry、销毁 Graph、反向销毁具体 adapter owner、释放 compiled instance 的配置/resource
lease，最后释放 Component scope。插件 root 在
owner 集合非空时拒绝 shutdown/unload。失败保留第一个明确状态，调用方可按既有协议重试；没有
静默吞错或 fallback。

Windows 安装把 provider 和 canonical SDK imported targets 声明的 runtime DLL 放在同一
`bin` 目录。TLS 由 `Salts::CNet` 提供，不再单独发现或打包旧 BoringSSL/OpenSSL DLL。

## 影响与迁移

- 架构：provider 接纳使用 Component scope，deployment resource 独立持有 Plugin lease；
  Graph/Product 不读取 CNet 私有配置。
- 接口：六个 factory 使用 typed config/resource contract；既有 embedded CNetAdapter API 保持不变。
- 状态：module、generation 与网络 owner 各有唯一事实源，不复制可写状态。
- 性能：每个 generation poll 每个 owner 增加一次 vtable 间接调用；消息热路径、buffer 所有权和
  CNet/Actor 队列不增加分配。
- 部署：Gateway 必须部署 provider 及其动态依赖；缺失即启动/装配失败。
- 回滚：可从 Gateway 配置和部署中移除该 provider；不启用 direct-link 替代路径。

## 验证

`test_cnet_provider_interface` 覆盖六个真实 provider factory 和 deployment resource DSO 的
materialization；`test_flow_provider_adapter_binding` 覆盖精确 stage、契约复制、无效 ABI/容量、
重复登记和 reset；`test_cnet_packet_source` 覆盖真实 UDP 批量交付，listener 套件覆盖 TCP/TLS。
这些测试的 Windows Release 结果不替代 NetworkIntake 闭环、安装态、其他平台或 sanitizer 验收。
