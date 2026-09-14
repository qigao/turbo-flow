# ADR: Protocol Source、Inbox 与 Sink 边界

## 状态

Accepted。

2026-09-13 补充：[协议无关业务图决策](../../docs/architecture/transport-independent-business-graph.md)
细化本 ADR 的业务边界。MQTT 同时可以提供接收 Source 和发送 Sink；原始帧 + metadata
之后仍需 Source 的业务 schema 映射。业务处理/存储通过 RulesForge/TurboScript 表达，
目的地独立于来源。旧 MQTT mapper 已删除；下文记录原协议层拆分决策。

## 背景

OCPP、JT/T 808、GB/T 32960、CoAP、LwM2M 等协议需要不同的 transport、分帧、
身份和响应语义，但它们可以把经过验证的消息交给同一 TurboFlow Graph 处理。旧设计
先把所有上行映射为 MQTT topic/payload，使 MQTT 成为不必要的内部耦合点。

## 决策

采用独立的 `ingress/protocol` 集成层：

1. 仓库外的 CNet/CHTTP 宿主 owner 管理连接、TLS、event loop 与 shutdown fence。
2. Protocol codec/Source 管理 wire validation、session 和有界分帧，不拥有响应发送。
3. 中立输出包含 decoded payload 与结构化 protocol metadata，并编码为版本化 TBE envelope。
4. `TurboFlow::ProtocolIngressInbox` 用配置的稳定 identity 同步调用一次
   `turbo_flow_inbox_admit()`；成功仅表示 Inbox provider 已拥有完整记录。
5. Graph 只能由 `turbo_flow_inbox_source_t` 领取记录后启动；MQTT、HTTP、socket、storage
   等输出都是显式 Sink，协议 Source 不依赖它们。
6. PluginHost 通过统一根 vtable 加载 codec/business DLL，并以 snapshot lease 向 registry
   提供不可变 provider view；TurboFlow Core/Graph 不反向依赖具体协议插件或网络 listener。

依赖方向固定为：

```text
protocol DLL -> PluginHost snapshot -> Source -> ProtocolIngressInbox -> configured Inbox
configured Inbox -> turbo_flow_inbox_source -> RulesForge/TurboScript Graph -> explicit Sink DLL
```

## 权衡

- 优点：协议可复用、Graph 与 broker 解耦、接收先存储且消息所有权明确，支持不部署 MQTT 的产品。
- 代价：每个 Source 必须提供跨重放稳定的 admission identity；异步 Sink 必须实现有界复制与
  独立完成语义。
- 兼容性：旧 MQTT mapping API、DLL 和 package component 均已删除且不兼容。
  MQTT 输入/输出只能通过 PluginHost 加载真实客户端 provider DLL，不伪装成 protocol codec。

## 验证

- 每种 codec 的有效帧、畸形帧、版本与容量边界测试；
- codec 的分片、多帧及消息边界测试；网络断链与 TLS/WS 测试归属宿主适配仓库；
- TBE envelope golden/round-trip、descriptor、payload/metadata 所有权测试；
- 安装的 `ProtocolIngressInboxSchema` codec 与 schema/RFL/TypeScript 产物消费测试；
- Inbox 幂等重放/冲突、backpressure、shutdown 和 force-shutdown 测试；
- 安装导出测试，确认旧 Graph bridge/runtime header/target/symbol 不存在，并且 adapter
  不调用 RulesForge 或具体 Sink。当前 Inbox API 与 Graph 共用 DLL，因此 package 仍需声明
  Graph 的 RulesForge 传递构建依赖；这不改变 Source 的运行时职责边界。
