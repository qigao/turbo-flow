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

这里的归一化是 wire 表达统一，不是业务 schema 统一。Source 仍需将原始帧映射成明确版本的
业务对象，交由同一 RulesForge/TurboScript 图处理；metadata 保留在适配上下文中。
业务存储/查询通过引擎调用受控能力，Sink 目标显式绑定，不从输入协议默认推导。
参见[协议无关业务图](../../docs/architecture/transport-independent-business-graph.md)。

下行有两条显式路径：

- `turbo_flow_protocol_encode()`：把协议中立 command 编码为新 wire frame；
- `turbo_flow_protocol_reply()`：在 Graph/Sink settlement 后生成协议规定的响应。

这是本 protocol runtime 的请求响应契约，不是所有 Source 的统一 ACK 策略。
普通 MQTT Source 可按客户端原生接纳契约推进协议 ACK；它不证明业务处理或持久化完成。

两条路径都 fail fast，不在解析失败时互相 fallback。旧 `TurboFlow::MqttSink`
无 I/O mapper 已删除。MQTT 必须通过真实客户端 provider DLL 注册 Source/Sink；
目的地、QoS、事务、重试和背压由该 owner 的显式配置与契约决定，不能由输入协议 metadata
自动生成。

## 生命周期

```text
PluginHost load -> catalog snapshot -> registry_create
  -> owner_create
  -> runtime/session_open
  -> session_feed -> decode -> graph/sink admission -> settle -> reply
  -> begin_shutdown -> drain/force -> destroy
```

- PluginHost 是 DLL handle、根实例与 provider catalog 的唯一事实源；不存在协议专用 loader
  或直接 register API。
- Registry 从不可变 snapshot 构建并持有其 lease；owner 存活时 registry 销毁返回
  `SALTS_EBUSY`，registry 存活时 PluginHost 销毁返回 `SALTS_EBUSY`。
- TCP/TLS 流必须先有界重组完整帧；UDP/WS 一次 receive 是一个消息边界。
- 每个 session 同时最多一个待 settlement，避免 ACK 顺序与业务提交顺序分叉。
- `begin_shutdown` 关闭新 admission，但保留 event loop 和已接纳 socket，直到响应发送
  完成；超时不会静默释放所有权，只有 `force_shutdown` 能取消。
- Graph stage 不得保存裸 socket/session 指针。需要跨阶段传播的协议 metadata 必须位于
  message-owned buffer 内。

## 错误语义

| 错误 | 含义 |
| --- | --- |
| `SALTS_EINVAL` | ABI、配置、指针或消息 shape 非法 |
| `SALTS_ENOTSUP` | 协议、版本、transport 或 command 未实现 |
| `SALTS_EPROTO` | 分帧、校验和、身份或协议字段不一致 |
| `SALTS_EMSGSIZE` | 帧、session 缓冲或调用方输出容量不足 |
| `SALTS_EBUSY` / `SALTS_ENOSPC` | admission 关闭或有界资源已满 |
| `SALTS_EPERM` | transport 身份不足或不安全模式未显式授权 |
| `SALTS_ETIMEDOUT` | 网络任务或 settlement 未在时限内完成 |

## 状态事实源

- Wire/session/ACK 状态：Protocol runtime。
- DLL 生命周期、provider catalog 与 generation lease：PluginHost。
- 连接/TLS/handler 状态：仓库外的 CNet/CHTTP 宿主 owner。
- Graph 执行状态：TurboFlow runtime。
- MQTT broker/session、数据库 connection 与重试状态：产品注入的独立 I/O adapter。
- MQTT 客户端 Source/Sink：由真实 provider DLL 与原生会话 owner 实现，不属于 protocol runtime。

这些状态不可双向同步，也不得在 Graph、协议插件和 Sink 中各维护一份。
