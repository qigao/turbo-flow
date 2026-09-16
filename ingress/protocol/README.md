# TurboFlow Protocol Ingress

`ingress/protocol` 是 TurboFlow 的可选协议 codec/Source 层。它完成协议分帧、
校验和身份提取，并通过 `TurboFlow::ProtocolIngressInbox` 将已验证消息同步接纳到配置的
Inbox。连接监听、TLS/WS、HTTP endpoint 和 event-loop 生命周期由 CNet/CHTTP 宿主
适配层拥有。

```text
CNet/CHTTP host adapter
  -> protocol Source codec/session
  -> TurboFlow::ProtocolIngressInbox
  -> configured Inbox (bounded memory / TurboDB)
  -> turbo_flow_inbox_source claim/request/poll
  -> shared RulesForge/TurboScript business graph
  -> explicitly configured Sink (MQTT, HTTP, socket, ...)
```

协议 decode 输出只在 admission 调用期间借用。identity provider 返回的 view 必须持续到该次
`turbo_flow_protocol_inbox_admit()` 返回。Inbox adapter 校验稳定 Source/admission 身份，将协议字段
与 payload 序列化为带明确 schema/version 的 TBE envelope，并且只调用
一次 `turbo_flow_inbox_admit()`。`SALTS_OK` 仅表示所选 provider 已复制并拥有不可变记录；
它不表示 Graph、业务提交或发送成功，也不会隐式触发这些步骤。原生
`turbo_flow_protocol_metadata_t` 的内存布局不是持久格式。
安装后的 native consumer 通过 `TurboFlow::ProtocolIngressInboxSchema` 与
`turbo_flow_protocol_inbox_envelope.h` 解码；RulesForge/TurboScript schema 产物位于
`share/TurboFlow/protocol/inbox`。

MQTT 接收属于 Source，发送属于 Sink；它不是内部中间格式。MQTT 接收/发送必须由统一
PluginHost 加载真实客户端 provider DLL，分别注册 Source/Sink，并显式绑定 schema、
接收存储和目的地。协议响应同样由显式 Sink 编码并发送，Source 不拥有发送行为。

上述 envelope 契约属于协议 Source 适配层，不是业务 schema。业务节点应只依赖统一业务对象，
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

- Protocol Source 拥有有界分帧缓冲和 session；成功 admission 后可消费对应帧。
- CNet/CHTTP 宿主 owner 拥有连接、event loop、TLS 身份和关闭栅栏。
- Inbox provider 是已接纳协议记录、幂等 identity 和 claim 生命周期的唯一事实源。
- 容量错误保留当前帧并返回 Source 反压；其他 provider 错误原样返回并使 session 显式失败。
- `turbo_flow_inbox_source_t` 独立拥有 claim → Graph → settlement；Protocol Source 不等待
  Graph，也不根据 Graph/Sink 结果修改已经完成的 admission。
- 关闭 Source admission 只停止新接收并收束其 session；它不等待 Inbox 中已拥有的 Graph 工作。
- 不自动重试 provider，不从数据库回退内存，不直接 publish Graph，也不隐式发送响应。

## 构建与测试

所有选项只在仓库根 `CMakeOptions.cmake` 声明。当前仓库不构建 transport soak；
常规验证使用仓库 presets：

```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user --target test_protocol_source test_protocol_inbox test_flow_inbox_source
ctest --preset win-release-user -R "^(test_protocol_source|test_protocol_inbox|test_flow_inbox_source)$" --output-on-failure
```

具体生命周期与错误契约见 [PROTOCOL_INGRESS.md](PROTOCOL_INGRESS.md)，架构决策见
[ADR_PROTOCOL_INGRESS_ABI.md](ADR_PROTOCOL_INGRESS_ABI.md)。
