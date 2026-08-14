# Protocol Ingress 数据与生命周期契约

## 数据边界

上行协议帧在 codec 完整验证后归一化为：

```text
payload: 完整原始 wire frame
metadata: protocol, direction, message_type, sequence,
          protocol_version, device_id, operation, correlation_id
```

`turbo_flow_protocol_decode()` 不生成 MQTT topic，也不接收 QoS、retain 或 broker
session。`turbo_flow_protocol_graph_publish()` 将 payload 与 metadata 复制到同一个
`mem_buffer_t`，因此 metadata 在 clone/move、线程池和异步 Graph 边界上保持与消息
一致的生命周期。

下行有两条显式路径：

- `turbo_flow_protocol_encode()`：把协议中立 command 编码为新 wire frame；
- `turbo_flow_protocol_reply()`：在 Graph/Sink settlement 后生成协议规定的响应。

两条路径都 fail fast，不在解析失败时互相 fallback。独立的
`TurboFlow::MqttSink` 只把调用方持有的有界 message batch 映射为 MQTT publications，
不接触 codec、protocol session、runtime、Graph bridge、MQTT client 或数据库 connection。
映射结果只在对应输入 payload 有效期间可用，外部 I/O adapter 决定事务、重试和背压。

## 生命周期

```text
registry_load/register
  -> owner_create
  -> runtime/session_open
  -> session_feed -> decode -> graph/sink admission -> settle -> reply
  -> begin_shutdown -> drain/force -> destroy
```

- Registry 容量在创建时固定；owner 存活时不可卸载插件。
- TCP/TLS 流必须先有界重组完整帧；UDP/WS 一次 receive 是一个消息边界。
- 每个 session 同时最多一个待 settlement，避免 ACK 顺序与业务提交顺序分叉。
- `begin_shutdown` 关闭新 admission，但保留 event loop 和已接纳 socket，直到响应发送
  完成；超时不会静默释放所有权，只有 `force_shutdown` 能取消。
- Graph stage 不得保存裸 socket/session 指针。需要跨阶段传播的协议 metadata 必须位于
  message-owned buffer 内。

## 错误语义

| 错误 | 含义 |
| --- | --- |
| `TURBO_EINVAL` | ABI、配置、指针或消息 shape 非法 |
| `TURBO_ENOTSUP` | 协议、版本、transport 或 command 未实现 |
| `TURBO_EPROTO` | 分帧、校验和、身份或协议字段不一致 |
| `TURBO_EMSGSIZE` | 帧、session 缓冲或调用方输出容量不足 |
| `TURBO_EBUSY` / `TURBO_ENOSPC` | admission 关闭或有界资源已满 |
| `TURBO_EPERM` | transport 身份不足或不安全模式未显式授权 |
| `TURBO_ETIMEDOUT` | 网络任务或 settlement 未在时限内完成 |

## 状态事实源

- Wire/session/ACK 状态：Protocol runtime。
- Socket/TLS/handler 状态：CoroNet owner。
- Graph 执行状态：TurboFlow runtime。
- MQTT broker/session、数据库 connection 与重试状态：产品注入的独立 I/O adapter。
- MQTT topic/QoS/retain 映射策略：无连接、同步批处理的可选 MQTT Sink。

这些状态不可双向同步，也不得在 Graph、协议插件和 Sink 中各维护一份。
