# ADR: Protocol Ingress、Graph 与可选 Sink 边界

## 状态

Accepted。

## 背景

OCPP、JT/T 808、GB/T 32960、CoAP、LwM2M 等协议需要不同的 transport、分帧、
身份和响应语义，但它们可以把经过验证的消息交给同一 TurboFlow Graph 处理。旧设计
先把所有上行映射为 MQTT topic/payload，使 MQTT 成为不必要的内部耦合点。

## 决策

采用独立的 `ingress/protocol` 集成层：

1. CoroNet owner 管理 socket、TLS、event loop 与 shutdown fence。
2. Protocol codec/runtime 管理 wire validation、session、分帧和协议响应。
3. 中立输出仅包含原始 payload 与结构化 protocol metadata。
4. Graph bridge 以 message-owned buffer 调用 `turbo_flow_publish()`。
5. MQTT、HTTP、storage 等都是 Graph 后的可选 Sink；协议 runtime 不依赖它们。
6. TurboFlow Core/Graph 不反向依赖具体协议插件或 CoroNet listener。

依赖方向固定为：

```text
protocol plugins -> protocol runtime -> Graph bridge -> TurboFlow::Graph
optional sinks   -> TurboFlow::Graph output
```

## 权衡

- 优点：协议可复用、Graph 与 broker 解耦、消息所有权明确，支持不部署 MQTT 的产品。
- 代价：需要显式 Graph source 配置；异步 Sink 必须实现有界复制与 settlement。
- 兼容性：旧 MQTT mapping API 已从 protocol Core 删除。需要 MQTT 输出时，调用方显式
  链接独立的 `TurboFlow::MqttSink`；MQTT 下行若需要，应作为独立 ingress adapter 接入，
  不伪装成 device protocol egress。

## 验证

- 每种 codec 的有效帧、畸形帧、版本与容量边界测试；
- TCP/TLS 分片、多帧与断链测试；UDP/WS 消息边界测试；
- Graph bridge payload/metadata 所有权测试；
- pending settlement、backpressure、shutdown 和 force-shutdown 测试；
- 安装导出测试，确认协议层只单向依赖 Graph，且不查找 MQTT 产品。
