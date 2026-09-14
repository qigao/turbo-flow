# ADR: Protocol Ingress、Graph 与可选 Sink 边界

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
2. Protocol codec/runtime 管理 wire validation、session、分帧和协议响应。
3. 中立输出仅包含原始 payload 与结构化 protocol metadata。
4. Graph bridge 以 message-owned buffer 调用 `turbo_flow_publish()`。
5. MQTT、HTTP、storage 等都是 Graph 后的可选 Sink；协议 runtime 不依赖它们。
6. PluginHost 通过统一根 vtable 加载 codec/business DLL，并以 snapshot lease 向 registry
   提供不可变 provider view；TurboFlow Core/Graph 不反向依赖具体协议插件或网络 listener。

依赖方向固定为：

```text
protocol/business DLL -> PluginHost snapshot -> registry/runtime -> Graph bridge -> TurboFlow::Graph
optional sinks        -> TurboFlow::Graph output
```

## 权衡

- 优点：协议可复用、Graph 与 broker 解耦、消息所有权明确，支持不部署 MQTT 的产品。
- 代价：需要显式 Graph source 配置；异步 Sink 必须实现有界复制与 settlement。
- 兼容性：旧 MQTT mapping API、DLL 和 package component 均已删除且不兼容。
  MQTT 输入/输出只能通过 PluginHost 加载真实客户端 provider DLL，不伪装成 protocol codec。

## 验证

- 每种 codec 的有效帧、畸形帧、版本与容量边界测试；
- codec 的分片、多帧及消息边界测试；网络断链与 TLS/WS 测试归属宿主适配仓库；
- Graph bridge payload/metadata 所有权测试；
- pending settlement、backpressure、shutdown 和 force-shutdown 测试；
- 安装导出测试，确认协议层只单向依赖 Graph，且不查找 MQTT 产品。
