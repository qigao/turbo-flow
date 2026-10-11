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

NetworkIntake v4 使用该契约检查 pending claims/bytes，已删除 Source 的 resolved CNet
字段读取。每个 intake 保留地址稳定的 Component scope、compiled instance 和 runtime owner；
decoder 配置来自精确 stage 的生成 Message；protocol/mapper 实现仍使用现有 catalog，
其发布方式迁移归 #253。

选择复制 adapter 契约，避免 intake 依赖 CNet 私有 typed config 或给所有 factory 增加网络
专属方法。现有注册结构、factory Interface 和直接使用 CNetAdapter 的调用保持兼容；新查询是
additive C ABI。回滚需同时回滚依赖该契约的 consumer，不能用读取旧配置作为运行时 fallback。

## NetworkIntake v4 消费端迁移

`source_stage_name` 指向已解析 `.flow` 中的精确 Source stage，例如 `wire`；该 stage 的
adapter identity 选择 `cnet.listener_source` 或 `cnet.packet_source`。调用方显式传入
`component_runtime`、provider resolver v2 和 resource resolver v1。Source typed config 由
MessagePlan 绑定，endpoint/TLS/PSK 由 resource Interface 提供；不再从 YAML adapter table
选择或配置 CNet。当前协议组合准入 TCP listener/JTT808 和 UDP packet/CoAP，KCP 明确拒绝。

选择复用 compiled provider instance，避免把 provider factory 或原生 CNet owner 复制进
NetworkIntake。给 CNet 恢复 PluginHost export、在 intake 中保留 resolved field reader 均会
重新建立第二条配置和发布路径，因此不采用。协议 codec/mapper 的迁移单独推进，避免扩大
Source 所有权变更。decoder 的配置迁移复用同一 typed-config binder，见下文。

create 先校验 decoder、typed config 和显式资源，再 materialize Source、登记 decoder、compile。
reply capability 在 compile 冻结 stage binding 后校验，仍早于 start 和网络 I/O。端点快照通过
`turbo_flow_provider_adapter_source_connection_snapshot()` 查询同一 stage；reply 也只路由到
该 stage，避免同 provider 的多个实例错误共享全局 adapter alias。

所有权迁移以 materialize 为界：之前失败，`*intake_flow_io` 仍归调用方；开始 materialize 后
置 NULL，由 intake 清理。普通失败返回 NULL intake；若回滚清理失败，则返回错误及非 NULL
retry owner，调用方必须显式 stop/destroy 并检查结果。stop 保留 compiled instance、resource
lease 和 Component scope；destroy 完成 shutdown、Graph teardown、owner destroy、typed config
与 resource release 后才释放 scope。任何失败保留尚未完成的清理义务。

公开 config/snapshot 使用 v4，旧版本被拒绝。消费者需同步重新编译、将 Source 和 decoder
配置移入各自 stage，并提供 resolver。回滚必须成套回滚消费者、`.flow` 和对应 SDK，不能混用
版本布局。可执行迁移示例位于 `ingress/protocol/tests/protocol_network_e2e_fixture.c` 和
`turbo_flow/tests/test_flow_rulesforge_durable_network.c`。

### Decoder typed config（#238 / #241）

v4 删除 `config.resolved`，将 `decoder_adapter_name` 改为精确的 `decoder_stage_name`。
该 stage 必须选择 `protocol.decode`，不允许附加未消费的 deployment resource。
协议配置归 `ingress/protocol/network/src/protocol_network_decoder_config.schema` 所有，
通过现有 bounded CSerde reader 和生成的 `DataBindMessageNativeArtifact` / MessagePlan
绑定，不在 Graph Core 增加字段语义。示例中的 `wire` 是已经配置的 canonical Source：

```flow
stage decode adapter protocol.decode {
  schema_version 1
  protocol_provider "coap"
  protocol_kind "coap"
  protocol_version "RFC7252"
  source_id "coap.primary"
  max_sessions 1
  max_frame_size 1024
  max_pending_claims 64
  max_pending_bytes 65536
  mapper { plugin "applicant.mapper", name "applicant-json", profile "applicant-json", message_type 2, semantic_type 50, semantic_media_type "application/json", max_semantic_bytes 256, max_output_bytes 256 }
}
stage main {
  wire -> decode
}
```

API v4 与 decoder schema 1 分别标识 C 布局和 Message 契约。旧 YAML decoder schema 2/3
不再作为输入：普通 decoder 省略 `mapper`；语义映射提供完整嵌套 record。选择可选 record，
使 DataBind 负责其整体 presence、所有子字段的必填性、类型、长度和正数约束，避免继续维护
随版本变化的私有字段表。未知、缺失或无效字段返回 canonical binder 的 `SALTS_EPROTO`；
诊断保留精确 stage，以及 MessagePlan 提供的字段名和错误文本。当前 SDK 对未知字段可能
只提供 stage 位置。跨字段语义和原生 `size_t` 溢出仍由 protocol 边界校验；mapper semantic
bytes 不得超过 frame bound，违反返回 `SALTS_ERANGE`。

绑定在 Source provider 查找、scope acquire 和 materialize 之前完成。临时 typed native 值、
MessagePlan 和 workspace 在 preflight 内释放，只有经过校验、拥有自身存储的有界 settings
进入 intake。catalog 继续固定 codec/mapper 的代码生命周期，Source retry-owner 顺序不变。
协议测试和真实 RulesForge 双 Source 组合已迁移至此路径；后者的 operation/materializer
仍消费旧 Config，属于 #93/#73/#241 的后续范围，不能据此删除整个 `TurboFlow::Config`。

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

NetworkIntake 正式测试覆盖 v2/v3 拒绝、preflight/materialize 失败、失败创建的 retry owner、
start/poll 错误和 drain/shutdown 重试。JTT808/CoAP 实网测试分别使用内存与 TurboDB 2.3.4
资源；RulesForge 组合用 TCP/UDP 两个 Source 驱动同一业务图和 UDP Sink。真实 DSO lease 断言
覆盖 create/start/stop/destroy：单 intake 的资源 lease 为 3，Source destroy 后为 2，业务图
destroy 后为 0；双 Source 组合为 4 → 2 → 0，最后释放 Component provider leases 并卸载。
这些 Windows Release 结果仍不代表其他平台、安装态或 sanitizer qualification 已完成。

2026-10-11 本地验证使用 `win-release-user`、Salts `2.3.0-rc.10`
（`6ba9868a965f3273cf87ab243dab1884072eb7a3`）和 TurboDB `2.3.4`
（`a8f849e7cd7193b89a8ade596bf9ef072a0533fc`）。NetworkIntake、provider/config/Component、
mapper、RulesForge 与实网组合共 16 个 CTest 套件通过；相邻 Plugin generation/CNet 回归
8/9 通过。仍失败的是 `test_cnet_stream_sink` 的三个 Windows Pipe 用例：当前 SDK 的
`cnet_send_slicev()` 对 Pipe 返回 `SALTS_ENOTSUP`，未跳过测试或转换发送路径。
构建和测试日志位于本地 `build/Msvc-Release/274-final-{build,tests}.log` 与
`274-adjacent-{build,tests}.log`。复验使用同一 user preset 构建上述 targets，再通过 CTest
选择相同套件；本条记录限定于这组安装 SDK 与 Windows Release。

2026-10-11 后续 #241 decoder 迁移验证使用同一 `win-release-user`，实际安装的 Salts 已为
`2.3.0-rc.11`（`40173f836d983225d14e87b1c98b78ffc7047174`），TurboDB 仍为上述 `2.3.4`。
构建日志确认同步了新的 Salts DLL；测试目录 DLL 与安装目录 DLL 的 SHA256 相同。
TurboDB 的生产 manifest 记录其构建依赖为 Salts rc.10，因此这里的结果是该 TurboDB 包与
Salts rc.11 的本地组合验证，不宣称 TurboDB 已针对 rc.11 重新发布。

本轮 decoder 配置、core、edges、owner、JT/T808/CoAP（内存及 TurboDB）和 RulesForge
双 Source 共 9/9 个 CTest 套件通过，包含 decoder 失败发生在 Source resolver 调用之前的
断言。相邻 provider config/binding/generation 和 Component generation 4/4 通过。
`test_cnet_stream_sink` 仍有上述三个 Windows Pipe 用例失败，首个返回 `SALTS_ENOTSUP`
（`-4039`）；没有跳过用例或替换传输。全部 14 个选中套件为 13 通过、1 失败。

复验先通过 `cmake --build --preset win-release-user --target <选中套件> --parallel 8`
构建下列过滤器对应的同名 targets，再执行：

```powershell
ctest --preset win-release-user -R "^test_(protocol_network_intake(_config|_core|_edges)?|protocol_network_(jtt808|coap)(_turbodb)?|flow_rulesforge_durable_network)$" --output-on-failure
ctest --preset win-release-user -R "^test_(flow_(provider_config|provider_binding|provider_generation|component_generation)|cnet_stream_sink)$" --output-on-failure
```

Windows 命令在 `VsDevCmd.bat` 环境执行，GitHub Packages token 由父环境提供。日志位于
本地 `build/Msvc-Release/241-decoder-final-{build,tests}.log` 和
`241-decoder-rc11-adjacent-{build,tests}.log`；本轮未执行安装态、其他平台或 sanitizer 验收。

上述 decoder 迁移完成时，#241 的生产消费者清单尚包含
[`flow_plugin_operation.c`](../../turbo_flow/src/flow_plugin_operation.c) 的 operation/resource
绑定，以及 [`flow_plugin_generation.c`](../../turbo_flow/src/flow_plugin_generation.c) 的
materializer 绑定。两者仍读取 resolved config，需与 #93/#73 的 typed binding 契约协调迁移，
再删除 Config target/API 和剩余 YAML 测试；本轮只移除了 NetworkIntake decoder 消费端。


### #241 materializer stage 配置迁移（2026-10-11）

Materializer 选择现由 operation stage 的 `materializer { plugin "provider.id" }`
声明，复用生成的 MessagePlan；schema/version/encoding 从 canonical operation input
导出。共享 `(operation, resource)` session 的 stages 必须选择一致，仅显式选择的
bindings 贡献 projection capacity。非空旧 YAML materializer 数组在 provider preparation
前拒绝，完整决策与迁移方式见 [typed-operation 契约](../../docs/architecture/typed-operation-abi3.md#materializer-stage-config)。

真实 RulesForge 双 Source fixture 已同步改用 `.flow` materializer 选择；其 operation
policy 与 `rfl_file/fact_type/base_dir` 部署资源仍是 resolved-config 消费者。后续先迁移
这两类，最后删除 Config target/API/parser；不能将本次 materializer 迁移计为 #241 完成。


本片段 Windows Release 验证使用 `win-release-user`、Salts `2.3.0-rc.11`
（`40173f836d983225d14e87b1c98b78ffc7047174`）和 TurboDB `2.3.4`
（`a8f849e7cd7193b89a8ade596bf9ef072a0533fc`）。TurboDB manifest 仍记录其生产依赖
为 Salts rc.10；这是安装包与 rc.11 的本地组合结果。

19/19 CTest 套件通过：materializer catalog/generation、operation runtime、Plugin
generation、provider config/binding/generation、Component generation、durable buffer、
Inbox Source，加上 decoder 迁移记录中的全部 9 个 network/RulesForge 套件。materializer
正向路径通过真实 durable buffer 获取 claim，再同步送入 generation；不调用私有 provenance
标记入口。非法配置、127/128 字节边界、共享 stages 选择/省略冲突、未选择消费者不贡献
容量、projection 滞留与释放均有正式用例。

先构建下列过滤器对应的同名 targets，再运行：

```powershell
cmake --build --preset win-release-user --target test_flow_plugin_materializer_generation test_flow_rulesforge_durable_network test_flow_plugin_materializer test_flow_plugin_operation_runtime test_flow_plugin_generation test_flow_provider_config test_flow_provider_binding test_flow_provider_generation test_flow_component_generation test_flow_durable_buffer test_flow_inbox_source test_protocol_network_intake test_protocol_network_intake_config test_protocol_network_intake_core test_protocol_network_intake_edges test_protocol_network_jtt808 test_protocol_network_coap test_protocol_network_jtt808_turbodb test_protocol_network_coap_turbodb --parallel 8
ctest --preset win-release-user -R "^test_(flow_(plugin_materializer(_generation)?|rulesforge_durable_network|plugin_operation_runtime|plugin_generation|provider_config|provider_binding|provider_generation|component_generation|durable_buffer|inbox_source)|protocol_network_(intake(_config|_core|_edges)?|jtt808(_turbodb)?|coap(_turbodb)?))$" --output-on-failure
```

构建与测试日志为 `build/Msvc-Release/241-materializer-regression-{build,tests}.log`。
本片段未执行安装态、其他平台或 sanitizer 验收；之前记录的 Windows Pipe 三个失败用例
不在本次过滤器内，仍未解决。
