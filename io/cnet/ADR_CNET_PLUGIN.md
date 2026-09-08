# ADR: CNet 统一 Provider DLL

## 状态

Accepted，实施由 GitHub #69 跟踪，父任务为 #63；统一 Source/Sink 控制面继续由 #28 跟踪。

## 背景

`tf_cnet_adapter` 已提供 TCP、TLS、Pipe、listener、UDP、KCP 与 secure-KCP/FEC 的具体
Source/Sink owner，但 Gateway 若直接链接并调用这些 API，就绕过了 PluginHost catalog、module
lease 与 generation owner 生命周期。能力缺失时也无法在统一的 assemble 边界失败。

## 决策

新增一个显式加载的 `tf_cnet_plugin` 动态库。动态库只公开 PluginHost 的 canonical
`turbo_flow_plugin_get_api` 入口，并在同一个 host transaction 内注册六个 kind：

- `cnet.stream_source`
- `cnet.listener_source`
- `cnet.packet_source`
- `cnet.stream_sink`
- `cnet.datagram_sink`
- `cnet.packet_sink`

每个 provider 通过版本化纯 C vtable 完成 preflight、materialize 和 owner 生命周期操作。插件根
使用有硬上限的 CSTL `vec_t` 保存 owner 指针；PluginHost snapshot 是 module lease 的唯一事实源，
Graph generation 是本次装配 owner 集合的唯一事实源，具体 CNet adapter 仍独占网络句柄、CFlow
run、Actor 与 terminal claim。

Gateway 只链接 `TurboFlow::PluginHost`。它必须从明确路径加载 DLL，再从不可变 catalog snapshot
装配 Graph；缺 DLL、canonical symbol、kind、依赖、容量或配置都会返回原始错误，不调用
`TurboFlow::CNetAdapter`，不选择静态实现，也不替换 transport。

## 配置和副作用边界

六个 kind 使用 `schema_version: 1` 的 exact-object 配置。所有字段都必须显式提供；未知字段、错误
类型、无效 backend、越界容量、互斥 TLS/packet policy 或不满足 CNet KCP/FEC 约束的配置在
preflight 阶段失败。配置只在 provider 适配层转换为 `cnet_*_config`，不会进入 Graph 领域层。
`command_buffer_bytes` / `event_buffer_bytes` 为 `0` 时明确采用 CNet 按已校验容量推导的有界存储；
队列索引和派生存储乘法在 preflight 阶段受限，显式值必须满足 provider 的保守载荷下限。CNet 公共纯
validator 与最小容量常量由
[Salts #250](https://github.com/qigao/salts/issues/250) 跟踪；其落地后应删除本层镜像约束并改用
CNet 单一事实源。

Provider preflight 只解析和校验。materialize 先验证每个 adapter 只绑定一个角色匹配的 Graph
stage，再分配 owner、登记 generation owner，并注册具体 adapter。任一步失败都反向撤销当前
owner；整个 generation 装配失败时，已经创建的 owner 也按反序销毁，Graph 保持未发布状态。网络
bind/connect/listen 只发生在 `turbo_flow_start()` 的 adapter start 阶段。

三个 Source 使用 managed-run open：Flow 持有 run，CNet Source 持有输入 endpoint 和 demand
handoff。初始 demand 只在 generation 第一次 external poll 时注入，避免 Flow 尚未完全 STARTED
时推进输入。Managed boundary snapshot 从具体 Source 的只读 snapshot 映射实际 demand、队列、
in-flight、接收计数和错误；不从配置推测运行状态。stream/datagram Sink 保持其既有 managed
terminal boundary；packet Sink 的统一 managed-boundary 投影属于 #28，插件加载和 owner vtable
生命周期不依赖该后续能力。

## 停止、排空和卸载

停止顺序固定为：quiesce generation owner、停止 Graph adapter、排空 Graph/CNet terminal、shutdown
Graph registry、销毁 Graph、反向销毁具体 adapter owner、释放 snapshot/module lease。插件 root 在
owner 集合非空时拒绝 shutdown/unload。失败保留第一个明确状态，调用方可按既有协议重试；没有
静默吞错或 fallback。

Windows 安装把 provider、其 target runtime DLL 和明确配置对应的 BoringSSL runtime 放在同一
`bin` 目录。PluginHost 在 UTF-8 转宽字符后把 `/` 规范化为 `\`，再调用安全的
`LoadLibraryExW(..., LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS)`；这只统一
路径表示，不扩大 DLL 搜索范围。

## 影响与迁移

- 架构：Gateway 的网络能力从链接时依赖变为 PluginHost catalog 的运行时依赖；Graph/Product
  仍不链接 CNet。
- 接口：新增六个 kind 配置面和三个 additive managed-open C ABI；既有 embedded CNetAdapter API
  保持不变。
- 状态：module、generation 与网络 owner 各有唯一事实源，不复制可写状态。
- 性能：每个 generation poll 每个 owner 增加一次 vtable 间接调用；消息热路径、buffer 所有权和
  CNet/Actor 队列不增加分配。
- 部署：Gateway 必须部署 provider 及其动态依赖；缺失即启动/装配失败。
- 回滚：可从 Gateway 配置和部署中移除该 provider；不启用 direct-link 替代路径。

## 验证

测试覆盖六 provider 原子注册、catalog/generation 容量 N-1、strict schema 正反例、失败装配回滚、
lease 阻止卸载、managed Source snapshot，以及仅链接 PluginHost 的安装态 TCP/UDP Graph 闭环。
发布前还检查 DLL export/dependency/CRT、缺 DLL/缺传递依赖错误、C/C++ installed consumer、Debug
与 Release 全量 CTest、focused repeat 和 `git diff --check`。
