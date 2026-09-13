# TurboFlow Protocol Ingress

`ingress/protocol` 是 TurboFlow 的可选协议 codec/runtime 层。它完成协议分帧、校验、
身份提取和响应编码，然后把中立消息投递给 `TurboFlow::Graph`。连接监听、TLS/WS、
HTTP endpoint 和 event-loop 生命周期由仓库外的 CNet/CHTTP 宿主适配层拥有。

```text
CNet/CHTTP host adapter
  -> protocol codec/session
  -> payload + protocol metadata
  -> TurboFlow::ProtocolIngressGraph
  -> Source normalization (common business schema)
  -> configured intake storage (bounded memory / TurboDB inbox)
  -> shared RulesForge/TurboScript business graph
  -> explicitly addressed sinks (MQTT, HTTP, socket, ...)
```

MQTT 接收属于 Source，发送属于 Sink；它不是内部中间格式。协议 runtime 调用 `turbo_flow_protocol_decode()`，输出原始
payload 与 `turbo_flow_protocol_metadata_t`；`TurboFlow::ProtocolIngressGraph` 将二者放入
同一个 message-owned `mem_buffer_t` 后，按 `source_handoff` 调用同步
`turbo_flow_publish()` 或有界 `turbo_flow_publish_async()`。Graph stage 可通过
`turbo_flow_protocol_graph_metadata()` 读取 metadata。旧 `TurboFlow::MqttSink`
topic mapper 已删除；它没有 I/O owner，也不是可配置 Sink。MQTT 接收/发送必须由统一
PluginHost 加载真实客户端 provider DLL，分别注册 Source/Sink，并显式绑定 schema、
接收存储和目的地。

上述原始帧/metadata 契约属于协议适配层，不是业务 schema。业务节点应只依赖统一业务对象，
存储/查询通过 RulesForge/TurboScript 使用受控能力，输出目的地不默认绑定输入协议。
不存在 protocol-derived topic mapper 或兼容入口。
完整目标与现状见[业务图设计](../../docs/architecture/transport-independent-business-graph.md)。

## 协议与传输

| 协议前端 | 预期宿主 transport | 设备身份 |
| --- | --- | --- |
| CoAP | UDP | transport identity resolver |
| LwM2M | UDP NoSec（必须显式允许） | transport identity resolver |
| OCPP 1.6J / 2.0.1 | WS / WSS | resolver；WSS 可绑定已验证证书 |
| GB/T 32960 | TCP / TLS | 帧内 VIN |
| JT/T 808 | TCP / TLS | 帧内终端号 |
| MQTT-SN 1.2 | UDP | transport identity resolver |

MQTT-SN 是设备侧 wire protocol，不等同于 MQTT 客户端 Source/Sink。协议插件不创建 MQTT
client，不保存 broker session、QoS、retained 或离线消息状态。

## 所有权与反压

- Protocol runtime 拥有分帧缓冲、session、pending delivery 与协议响应顺序。
- CNet/CHTTP 宿主 owner 拥有连接、event loop、TLS 身份和关闭栅栏。
- Graph 拥有已接纳的 `turbo_flow_msg_t`，但不拥有 socket 或协议 session。
- `inline` 是兼容默认值，Graph bridge 成功后返回 `SETTLED`。
- `async_bounded` 只在 Flow 的队列、单消息字节和总在途字节预算内接纳；成功后返回
  `PENDING`，worker completion 恰好回调一次。宿主必须把 completion 投递回网络
  owner，并在原 publish callback 返回后调用 server/runtime `settle()`；worker 不得直接
  重入单 owner protocol runtime。
- 有界接纳失败立即返回具体错误，不产生 completion，也不回退到同步执行。
- 一个 session 同时最多有一个 pending delivery；满额返回反压，不静默丢弃。
- 关闭顺序为：关闭 listener admission → drain 已接纳消息 → 发送合法协议响应 →
  等待 handler 退出 → 销毁 listener/execution。

## 构建与测试

所有选项只在仓库根 `CMakeOptions.cmake` 声明。当前仓库不构建 transport soak；
常规验证使用仓库 presets：

```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user -L protocol-ingress --output-on-failure
```

具体生命周期与错误契约见 [PROTOCOL_INGRESS.md](PROTOCOL_INGRESS.md)，架构决策见
[ADR_PROTOCOL_INGRESS_ABI.md](ADR_PROTOCOL_INGRESS_ABI.md)。
