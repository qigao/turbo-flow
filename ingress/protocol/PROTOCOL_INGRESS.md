# Protocol Ingress 数据与生命周期契约

## 数据边界

上行协议帧经 codec 完整验证后，由协议 Inbox adapter 归一化为带明确版本的 durable
envelope。该 envelope 使用固定宽度字段和有界 bytes/string，至少声明：

```text
schema/version, protocol, direction, message_type, sequence,
protocol_version, device_id, operation, correlation_id, decoded payload
```

decode 结果是 admission 调用期间的借用视图。adapter 必须在返回前完成校验和 TBE
序列化，并同步调用一次 `turbo_flow_inbox_admit()`；成功 provider 已复制全部 record
bytes，session/socket 指针、DLL 地址及原生结构体 padding 不得进入持久记录。Inbox record
payload 是 envelope bytes，content descriptor 声明 `DATA / PROTOCOL_DATA / TBE` 及精确
schema/type/version；typed record 字段保留查询所需的 Source、admission、时间、类型、序列和
correlation 信息。

这里的归一化是 wire 表达统一，不是业务 schema 统一。Inbox Source 后续将记录映射成明确版本的
业务对象，交由同一 RulesForge/TurboScript 图处理；生成的 DataBind accessor 或受检 projection
是读取 envelope 的唯一入口。
安装包导出 `TurboFlow::ProtocolIngressInboxSchema` 静态 codec target 与
`turbo_flow_protocol_inbox_envelope.h`，并把 authored `.schema`、生成的 `.rfl` 和 `.d.ts`
安装到 `share/TurboFlow/protocol/inbox`。C/C++、RulesForge 与 TurboScript 因而共享同一份
版本化 schema，不需要复制或重写二进制布局。
业务存储/查询通过引擎调用受控能力，Sink 目标显式绑定，不从输入协议默认推导。
参见[协议无关业务图](../../docs/architecture/transport-independent-business-graph.md)。

下行编码仍有两条显式 codec 路径：

- `turbo_flow_protocol_encode()`：把协议中立 command 编码为新 wire frame；
- `turbo_flow_protocol_reply()`：根据显式 Sink 输入生成协议规定的响应。

它们是供后续 Sink 使用的纯 codec 操作，不是 Protocol Source 的发送路径，也不是所有 Source
的统一 ACK 策略。
普通 MQTT Source 可按客户端原生接纳契约推进协议 ACK；它不证明业务处理或持久化完成。

两条路径都 fail fast，不在解析失败时互相 fallback。旧 `TurboFlow::MqttSink`
无 I/O mapper 已删除。MQTT 必须通过真实客户端 provider DLL 注册 Source/Sink；
目的地、QoS、事务、重试和背压由该 owner 的显式配置与契约决定，不能由输入协议 metadata
自动生成。

## 生命周期

```text
PluginHost load -> catalog snapshot -> registry_create
  -> owner_create
  -> source/session_open
  -> session_feed -> decode -> Inbox admit
  -> independent Inbox Source claim/request/poll -> Graph -> configured Sink
  -> source shutdown -> destroy
```

- PluginHost 是 DLL handle、根实例与 provider catalog 的唯一事实源；不存在协议专用 loader
  或直接 register API。
- Registry 从不可变 snapshot 构建并持有其 lease；owner 存活时 registry 销毁返回
  `SALTS_EBUSY`，registry 存活时 PluginHost 销毁返回 `SALTS_EBUSY`。
- TCP/TLS 流必须先有界重组完整帧；UDP/WS 一次 receive 是一个消息边界。
- Inbox admission 的 `SALTS_OK` 只表示 provider 已拥有不可变记录；Source 随后消费该帧，
  不等待 Graph、业务提交、响应生成或 socket send。
- Inbox 容量错误保留帧并报告反压；其他 provider 错误原样返回且 session 显式失败。
- Source shutdown 关闭新 admission 并收束 Source session，不等待 Inbox 已拥有的 Graph 工作。
- Graph/Sink 不得保存裸 socket/session 指针；需要响应路由时只持久化可由 Sink 验证 generation
  的逻辑 route 数据。
- 不存在直接 Graph publish、自动 provider retry、数据库到内存 fallback 或隐式 response send。

## 错误语义

| 错误 | 含义 |
| --- | --- |
| `SALTS_EINVAL` | ABI、配置、指针或消息 shape 非法 |
| `SALTS_ENOTSUP` | 协议、版本、transport 或 command 未实现 |
| `SALTS_EPROTO` | 分帧、校验和、身份或协议字段不一致 |
| `SALTS_EMSGSIZE` | 帧、session 缓冲或调用方输出容量不足 |
| `SALTS_EBUSY` / `SALTS_ENOSPC` | admission 关闭或有界资源已满 |
| `SALTS_EPERM` | transport 身份不足或不安全模式未显式授权 |
| `SALTS_ETIMEDOUT` | Source 网络任务未在时限内完成 |

## 状态事实源

- Wire/session 状态：Protocol Source。
- 已接纳记录、幂等身份与 claim 生命周期：配置的 Inbox provider。
- DLL 生命周期、provider catalog 与 generation lease：PluginHost。
- 连接/TLS/handler 状态：仓库外的 CNet/CHTTP 宿主 owner。
- claim、Graph 执行与 Inbox settlement：`turbo_flow_inbox_source_t` / TurboFlow runtime。
- 响应编码、endpoint generation 校验与发送完成：配置的 Sink owner。
- MQTT broker/session、数据库 connection 与重试状态：产品注入的独立 I/O adapter。
- MQTT 客户端 Source/Sink：由真实 provider DLL 与原生会话 owner 实现，不属于 Protocol Source。

这些状态不可双向同步，也不得在 Source、Inbox、Graph 和 Sink 中各维护一份。已移除的
直接 Graph bridge 没有兼容 target、header、symbol、layout 或运行时 fallback。
