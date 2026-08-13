# ADR: MQTT Gateway 插件 ABI 与状态归属

## 背景

外部 MQTT owner 是 MQTT 业务处理层，其 MQTT 业务事实由 FlowStore 管理。新增
MQTT-SN、CoAP、LwM2M、OCPP、GB/T 32960 和 JT/T 808 时，需要避免：

- 外部 MQTT owner 直接依赖六种线协议及其会话状态；
- 六个 DLL 各自维护另一份 MQTT publish/session 事实；
- 插件跨 ABI 暴露解析器内部结构或 allocator-owned buffer；
- 网络关闭、MQTT 提交和协议 acknowledgement 形成不明确的双重提交。

## 候选方案

1. 每种协议直接注册为 external MQTT endpoint。
   - 优点：路径短。
   - 缺点：穿透 MQTT endpoint 边界，外部 MQTT owner 必须理解六种协议，状态归属
     分裂。
2. 每个 DLL 内置 MQTT client 并自行发布。
   - 优点：部署独立。
   - 缺点：连接、重试、提交与 backpressure 规则重复，难以形成统一证据。
3. 公共 Gateway ABI + 六个同级 codec DLL + 宿主 MQTT bridge。
   - 优点：协议状态与 MQTT 事实分离，ABI 小，能够逐步加入 transport。
   - 缺点：宿主需要显式完成 ingress/egress 与 MQTT publish 的编排。

## 决策

采用方案 3。

插件导出表只有 identity/capability 和 `open`/`close`。`open` 返回 opaque
provider-neutral gateway。数据路径只通过 `turbo_flow_gateway_ingress()` /
`turbo_flow_gateway_egress()`、`turbo_flow_gateway_reply()` 和
`turbo_flow_gateway_encode()`。原始路径保留完整帧并产生类型化 metadata；
响应与新下行命令使用独立入口，避免格式解释 fallback。

状态归属：

- protocol parser：对应 gateway DLL；
- network/session/backpressure：公共 `TurboFlow::GatewayCoroNet` 宿主；
- topic mapping 与 ABI/lifetime fence：`TurboFlow::Gateway`；
- MQTT connection、publish、subscription：宿主/external MQTT owner；
- MQTT session、retained、QoS、offline delivery 等业务事实：FlowStore。

## 失败和提交顺序

上行顺序为：验证协议帧 → 生成 bounded MQTT mapping → MQTT graph admission →
按原协议 settlement policy 回应设备。transport owner 保证每 session 最多
一个待 settlement，并在 pending 时暂停读取；原始请求和 socket 一直保留到
settlement response 同步发送完成。响应发送失败使该 session terminal，不会把
MQTT owner 已提交与设备已收到响应混为同一个事实。

下行在 topic、协议、设备和 operation 一致性验证完成前不会生成线协议帧。
任何验证失败均 fail fast，不做格式修复或 fallback。

## 兼容性、迁移与回滚

该能力是 TurboFlow 完整产品构建图的一部分，不修改现有 external MQTT endpoint、
Graph message ABI 或产品配置格式。构建系统不提供关闭 gateway
能力的 feature option。

ABI 使用 major/minor 和每个结构的 `size`。未来只可在结构末尾追加字段；
破坏性变更提升 major。回滚时移除 gateway 配置与 DLL，MQTT owner/FlowStore
数据格式无需迁移。

## Transport 与关闭决策

六个 DLL 保持同级且只拥有 codec。网络能力不复制到六个 DLL，而由一个公共
CoroNet transport owner 按协议/transport 矩阵 fail fast：

- MQTT-SN、CoAP：UDP；
- LwM2M：当前仅显式 NoSec UDP，DTLS 不可用时不自动降级；
- OCPP：WS/WSS，并校验协议版本对应的 WebSocket subprotocol；
- GB/T 32960、JT/T 808：TCP/TLS。

关闭先用 `coro_socket_server_close_admission()` 销毁 listener、停止新连接，
但不取消已经 admitted 的 handler；随后保持 execution context 运行，直到
gateway runtime 的 pending settlement 归零、协议响应发送完成且 CoroNet
`server_is_stopped`。超时只返回错误，不隐式取消；显式 force 才取消 accepted
task。这一顺序同时避免等待闭环和“先释放 socket、后生成响应”的所有权断裂。

## 业务服务边界

行业业务实现使用独立的版本化 C ABI，不加入协议 codec function table。业务
DLL 的 canonical export 仍只有 identity/capability 与 `open`/`close`；
`open` 返回 opaque business service。宿主只调用两个 provider-neutral 入口：

- `turbo_flow_gateway_business_consume_committed()`：消费 MQTT owner/FlowStore
  已成功提交后的不可变上行事件；
- `turbo_flow_gateway_business_prepare_command()`：校验业务 action 与 payload，
  生成调用方持有的 protocol-neutral command，再交给
  `turbo_flow_gateway_encode()`。

业务事件不进入设备 ACK 的提交判定。业务消费失败由宿主执行显式 retry 或
dead-letter policy，但不能回滚已提交的 MQTT 事实，也不能把协议响应改写成失败。
下行命令在业务校验和 schema binding 成功前不会进入协议编码器。

DataBind schema 只允许业务插件在 `open` 阶段从受信部署配置加载。跨 ABI
传递的 `schema_id` 与 `type_name` 只是已装配 schema 的标识，不是路径或 schema
文本；网络输入不能触发动态 JIT schema 创建。event、command request 和 payload
均为同步 borrowed view，需要异步排队的插件必须在返回前完成有界复制。

首个决策实例是 `ocpp201-core` minimal profile：schema 在构建时由 DataBind
2.0 host compiler 生成，并以 static generated library 链入 provider DLL。
测试另建同源 shared generated library，由 C++ consumer 校验导入 ABI。完整
OCPP CALL envelope 仍作为 FlowStore 原始事实；只有提取后的 inner typed 单元在
DLL 内进入 DataBind，因此不存在把 envelope 错标成 `ResetRequest` 或把
`DataBind *` 跨 ABI 传递的情况。

## 验证

- registry capacity、重复注册、module retention、owner cleanup；
- 六个 DLL 动态加载和相同 ABI conformance；
- OCPP business DLL 动态加载、DataBind ABI 精确匹配、generated static/shared
  consumer、Boot/Heartbeat/Authorize/StatusNotification 上行、TransactionEvent 必填事实及
  `offline`/`evse`/`idToken` 可选事实，以及 typed Reset/UnlockConnector 下行；
- 每种协议有效帧上行映射、下行 byte-for-byte round trip；
- 六种协议 settlement response 与类型化下行编码；
- callback table 的旧 minor prefix 仍可装配，新 reply 字段按尾部扩展读取；
- 非法版本、帧、topic、缓冲区和 size limit；
- CoroNet focused tests 覆盖 UDP 消息、TCP stream、WS path/subprotocol、
  pending settlement shutdown、结算前无响应/结算后有响应，以及 LwM2M
  NoSec fail-closed；
- 真实证书测试覆盖 TLS ingress、WSS mTLS、缺失客户端证书拒绝、已验证证书
  身份提取，以及损坏服务端凭据在 bind 前 fail fast；
- 重连矩阵在同一 listener 上重复 TLS 与认证 WSS 会话；每轮都要求 handler、
  session、pending settlement 和 buffered bytes 归零，并验证失败启动释放端口后
  可使用正确凭据重新创建。
- WSS reconnect 在服务端 publish completion 后才释放 client；gateway session
  quiescence 与 CoroNet accepted task 的异步 close completion 分别由 snapshot 和
  bounded shutdown wait 验证，避免混淆两个生命周期边界。
- opt-in `test_gateway_coronet_soak` 以隔离子进程随机执行 UDP 单数据报关闭、
  settlement shutdown、TLS/WSS reconnect、mTLS rejection、启动恢复和 fault
  cleanup，固定 seed，并对 ASan/LSan、RSS、句柄、线程和子进程超时统一设 gate。
- 后续阶段增加 DTLS/OSCORE 与各行业协议的完整业务 schema conformance。
