# Gateway 数据与生命周期协议

## 数据契约

上行 topic：

```text
{prefix}/{protocol}/{tenant}/{device}/up/{operation}
```

下行 topic：

```text
{prefix}/{protocol}/{tenant}/{device}/down/{operation}
```

默认 `prefix=gateway`、`tenant=default`。所有字段必须是单个安全 MQTT
topic segment，不允许 `/`、`+`、`#`、控制字符或空字符串。

MQTT payload 是完整原始设备协议帧。协议 DLL 必须先验证帧，再生成
`message_type`、`sequence`、`operation`、可选 `correlation_id` 和设备
身份。下行必须重新验证 topic 与 payload 的协议、设备和 operation
一致性，才可恢复原始帧。

原始恢复与类型化编码是两个显式接口：`turbo_flow_gateway_egress()` 不改变
payload，`turbo_flow_gateway_encode()` 根据 `operation/resource/correlation`
生成新线帧。二者不会互相 fallback。协议响应由
`turbo_flow_gateway_reply()` 生成，只能在 settlement 终结后发送。

调用方拥有输入和输出缓冲区。插件不会把自身 allocator 分配的内存交给
宿主。默认最大帧为 1 MiB，可在 open request 中调小或调大；超过限制返回
`TURBO_EMSGSIZE`。

## 生命周期

```text
registry_load/register -> owner_create -> ingress/egress* -> owner_destroy
                      \-> registry_destroy
```

- registry 容量在创建时固定。
- registry 直接注册的 API 为 borrowed；动态加载的 module 为 owned。
- owner 存活期间 `registry_destroy` 返回 `TURBO_EBUSY`，不会卸载 DLL。
- 同一 gateway 的 ingress/egress 可并发；宿主必须在 owner destroy 前停止
  admission，并等待已进入的数据路径返回。
- open 失败后，registry 对任何部分初始化的 service 调用一次 close。

### 行业业务服务

```text
FlowStore commit -> committed MQTT event -> business consume
business action -> schema validation -> gateway command -> protocol encode
```

- protocol gateway 与 business provider 使用两个独立 registry；二者只能通过
  provider-neutral event/command 契约组合，不能互相调用 concrete function。
- committed event 是 MQTT/FlowStore 事实的只读派生视图。业务回调失败不会
  更改协议 ACK，也不会生成第二份 MQTT session/retained/QoS 状态。
- business command output 由调用方分配；业务 DLL 不把 allocator-owned
  buffer、DataBind handle 或 `DataBindValue *` 交给宿主。
- `schema_id/type_name` 必须成对出现，仅引用 `open` 时已加载的受信 schema。
  schema 路径、schema 文本和 JIT 生命周期不进入网络数据接口。
- 当前 ABI 为同步且 caller-serialized。异步实现必须先完成有界复制；满额时
  返回明确的 `TURBO_EBUSY`/`TURBO_ENOSPC`，不得静默丢弃或无界增长。

首个 schema-bound provider 为 `ocpp201-core`，profile 固定为
`ocpp-2.0.1-core-minimal`：

- 上行原始事实使用 `application/json`，且不附加 inner-body schema 标识；
  provider 校验 CALL envelope 与 gateway metadata 一致后，再提取 body；
- `BootNotification` 使用生成的 `ChargingStation` 与
  `BootNotificationReason` typed binding，`StatusNotification` 使用
  `StatusNotificationRequest` typed binding 并要求 RFC3339 timestamp 与正数
  EVSE/connector identity，`Heartbeat` 只接受空对象；
- `Authorize` minimal slice 只接受 generated `TransactionIdToken`
  typed unit；未装配的 certificate 与 ISO 15118 certificate hash 字段返回
  `TURBO_EPROTO`；
- `TransactionEvent` 支持 Edition 4 JSON Schema 的必填事实：
  `eventType/timestamp/triggerReason/seqNo` 与 `transactionInfo.transactionId`
  分别绑定为 `TransactionEventFacts` 和 `TransactionInfoRequired`；
  `offline` 通过 `TransactionEventFacts` 的 generated presence bitmap 区分
  “缺失”与显式 `false`；`evse.id/connectorId` 由 `TransactionEvseFacts`
  同样绑定 required/optional 状态。`idToken.idToken/type` 保持独立 typed unit，
  因为当前 DataBind runtime 尚不支持嵌套变长 message；
- `meterValue`、`transactionInfo` 的其他可选状态以及其他尚未装配字段明确返回
  `TURBO_EPROTO`，不会跳过未验证字段；
- 下行 `reset` 只接受
  `application/json/Ocpp201Core/ResetRequest`，DataBind enum 验证成功后由
  OCPP 格式适配层输出协议字符串枚举，再调用公共
  `turbo_flow_gateway_encode()`；
- 下行 `unlock-connector` 只接受
  `application/json/Ocpp201Core/UnlockConnectorRequest`，DataBind 校验并
  canonicalize 正数 EVSE/connector identity 后输出 OCPP
  `UnlockConnector`；
- provider 不在内存中推进 transaction sequence 或 charging state；这些业务事实
  仍由 FlowStore 和上层领域消费。其他未装配 action 返回
  `TURBO_ENOTSUP`。

### CoroNet transport owner

```text
server_create -> server_start -> session_open/feed*
                         \-> begin_shutdown
                              -> wait admitted settlement
                              -> wait server_is_stopped
                              -> server_destroy
```

- 每个 transport owner 只有一个串行 CoroNet context owner；runtime callback
  不允许重入。
- `max_sessions`、`max_frame_size` 和 `max_buffered_bytes` 在 create 时验证并
  一次性分配；满额时拒绝新 session。
- UDP/WS 的一次 receive 是一个完整消息边界；GB/T 32960 与 JT/T 808 使用
  有界 stream reassembly。
- 每个 session 同时只允许一个待 MQTT settlement。待提交时不继续从 socket
  读取，内核/CoroNet 的既有限额承担上游 backpressure。
- `begin_shutdown` 先用 CoroNet admission-only close 关闭 listener，保留已经
  admitted 的 handler/socket，再关闭新 session/feed；event loop 必须继续运行。
  `wait_shutdown` 超时不释放尚未 settlement 的所有权；只有显式
  `force_shutdown` 会取消它。
- runtime 在 pending 期间保留完整原始请求。settle 顺序为：生成协议响应 →
  同步发送/复制响应 → 通知 settled observer → 释放请求与 session。响应发送
  失败是 terminal transport error，不会伪装成已成功 ACK。
- transport identity resolver 在 socket 所属 event-loop thread 上执行。除
  GB/T 32960 与 JT/T 808 的帧内身份外，缺少 resolver 会在 create 时失败。

## 错误语义

| 错误 | 含义 |
|---|---|
| `TURBO_EINVAL` | ABI shape、空指针、配置或 topic segment 非法 |
| `TURBO_ENOTSUP` | plugin、协议或协商版本不支持 |
| `TURBO_EPROTO` | 线协议帧、校验和、topic/payload 一致性错误 |
| `TURBO_EMSGSIZE` | 配置上限或调用方输出缓冲区不足 |
| `TURBO_EBUSY` | owner 仍存活或 service 已停止 admission |
| `TURBO_EPERM` | 缺少认证身份，或 LwM2M NoSec 未显式授权 |
| `TURBO_ETIMEDOUT` | 网络 task 或 MQTT settlement 未在关闭时限内完成 |

## 当前解析与响应边界

- MQTT-SN：长度头、消息类型和可用 packet identifier。
- CoAP：版本、token 长度、Code、Message ID；保留 Observe/Blockwise
  options 的原始字节。
- LwM2M：使用同一 CoAP wire 校验，将 request code 映射为
  read/write/execute/delete。
- OCPP：通过 TurboUtils JSON parser 验证 CALL/CALLRESULT/CALLERROR，
  提取 Unique ID 与 Action。
- GB/T 32960：起始符、命令、VIN、数据长度和 BCC。
- JT/T 808：帧边界、转义、2019 版本头、消息体长度、BCD 终端号和 BCC。

网络 listener、TLS/WebSocket 会话和有界关闭由
`TurboFlow::GatewayCoroNet` 管理。公共协议响应和类型化下行已经实现；完整
OCPP payload schema/transaction domain state、LwM2M bootstrap/Observe/
Blockwise、DTLS/OSCORE、GB/T 32960 与 JT/T 808 全量命令体领域模型及设备
重连策略不由当前 codec 冒充实现。后续 conformance 模块必须复用本文件的
数据与关闭协议，不得在 Flowie 内建立第二份 MQTT 业务事实。
