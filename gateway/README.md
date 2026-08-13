# TurboFlow MQTT Gateways

该目录提供六个同级动态库，将设备协议帧映射到 MQTT，而不改变 外部 MQTT owner 的
MQTT broker 职责：

| DLL target | 首阶段协议基线 | MQTT protocol segment |
|---|---|---|
| `tf_gateway_mqtt_sn` | MQTT-SN 1.2 | `mqtt-sn` |
| `tf_gateway_coap` | CoAP RFC 7252 | `coap` |
| `tf_gateway_lwm2m` | LwM2M 1.2.2 / CoAP | `lwm2m` |
| `tf_gateway_ocpp` | OCPP 1.6J、2.0.1 | `ocpp` |
| `tf_gateway_gbt32960` | GB/T 32960.3-2025 | `gbt32960` |
| `tf_gateway_jtt808` | JT/T 808-2019 + 第 1 号修改单 | `jtt808` |

协议 DLL 是 codec/mapping boundary，不创建监听 socket，也不拥有 MQTT
client。公共 `TurboFlow::GatewayCoroNet` 宿主负责 UDP、TCP、TLS、WS、WSS
监听，把帧送入有界 session runtime；映射后的 topic/payload 仍由调用方交给
external MQTT owner。下行既可用 `turbo_flow_gateway_egress()` 恢复经过校验的
完整原始帧，也可用 `turbo_flow_gateway_encode()` 编码受支持的类型化命令。
`turbo_flow_gateway_reply()` 只在 MQTT owner/FlowStore settlement 终结后生成协议
响应；runtime 会保留原始请求与 transport 所有权到响应同步发送完成。

公开插件只导出 `turbo_flow_gateway_plugin_get_api`，其 function table
只有 `open`/`close`。协议解析器、状态和具体实现不进入 外部 MQTT owner，也不暴露为
公共函数。

## 网络传输矩阵

| 协议 | 允许的 CoroNet transport | 身份来源 |
|---|---|---|
| MQTT-SN | UDP | 必须由宿主 resolver 提供 |
| CoAP | UDP | 必须由宿主 resolver 提供 |
| LwM2M | UDP NoSec（仅显式启用） | 必须由宿主 resolver 提供 |
| OCPP | WS / WSS | resolver；WSS 可绑定已验证客户端证书 |
| GB/T 32960 | TCP / TLS | 帧内 VIN |
| JT/T 808 | TCP / TLS | 帧内终端号 |

LwM2M 不会从 DTLS 静默降级到明文 UDP。当前 CoroNet 尚未提供 DTLS listener；
只有配置 `allow_insecure_lwm2m=1` 才允许明确的 NoSec/测试部署。TLS/WSS 必须
显式提供证书和私钥；mTLS 还必须提供客户端 CA。

传输关闭顺序固定为：关闭 listener admission → 标记 runtime draining →
在 event loop 仍运行时等待已接纳 MQTT settlement 并发送协议响应 → 等待
accepted handler 退出 → 销毁 listener 和 execution。若超时，调用方必须选择
继续等待或显式 force shutdown；普通关闭不会先取消响应所需的 socket。

## 已实现的协议动作边界

| 协议 | settlement response | 类型化下行 |
|---|---|---|
| MQTT-SN | CONNECT/WILL、REGISTER、QoS 1/2 publish、SUB/UNSUB、PING、DISCONNECT | PUBLISH、PINGREQ、DISCONNECT |
| CoAP | CON/NON request response，保留 MID/token | GET/POST/PUT/DELETE + URI-Path |
| LwM2M | CoAP read/write/execute/delete response | read/write/execute/delete resource |
| OCPP | CALLRESULT/CALLERROR envelope | 1.6J/2.0.1 已知 Action CALL、CALLRESULT、CALLERROR |
| GB/T 32960 | 命令/VIN 对应的成功、错误、重复响应帧 | 已知命令帧 |
| JT/T 808 | 0x8001 平台通用应答 | 平台应答、参数、控制、查询、文本、透传等已知消息 |

这里的“已实现”指公共 ABI、线帧校验、应答/编码和有界 transport 生命周期，
不等价于各行业协议的完整业务应用。OCPP action payload JSON Schema、LwM2M
bootstrap/Observe/Blockwise、DTLS/OSCORE，以及 GB/T 32960、JT/T 808
命令体的领域字段模型仍属于后续独立 conformance 阶段；未实现能力返回
`TURBO_ENOTSUP`，不会隐式降级。

`test_gateway_coronet` 使用临时测试 PKI 验证真实 TLS/WSS 握手、mTLS
fail-closed、证书身份提取、损坏凭据启动失败，以及关闭期间的 pending
settlement。重连矩阵在每次 TLS/WSS client 释放后检查 handler、session、
pending settlement 与 buffered bytes 全部归零，并覆盖启动失败后的同端口
恢复。WSS reconnect 使用 publish completion fence 后才释放 client，避免把
“重连资源验证”与“发送后立即断链”混成一个时序；CoroNet accepted task 的异步
close completion 则由独立的 bounded shutdown wait 完成。UDP 用例还验证一个
数据报只归属一个伪连接、第二次读取立即 EOF，并在关闭接纳后有界退出。测试不
依赖外部 broker 或系统 CA。

## Scheduled transport soak

`GATEWAY_TRANSPORT_SOAK_TESTS=ON` 注册 `test_gateway_coronet_soak`。默认执行
5 个隔离子进程；scheduled/CI 可配置：

- `GATEWAY_TRANSPORT_SOAK_DURATION_MS`：最少持续时间，最大 8 小时；
- `GATEWAY_TRANSPORT_SOAK_ITERATIONS`：最少操作数；
- `GATEWAY_TRANSPORT_SOAK_SEED`：非零可复现随机 seed；
- `GATEWAY_TRANSPORT_SOAK_TRACE`：可选精确 trace 名，用于 focused 复现；
- `GATEWAY_TRANSPORT_SOAK_RSS_TOLERANCE_BYTES`：父 runner 的 RSS 上限增量，
  默认 8 MiB。

每轮子进程有 120 秒硬超时，失败后立即停止；随机 trace 覆盖 UDP 单数据报关闭、
TCP settlement shutdown、TLS/WSS 重连、mTLS rejection、启动恢复和 fault
cleanup。父 runner 在预热后要求句柄和线程严格回到基线，并输出
`SOAK_RESULT id=GATEWAY-SOAK-001`、trace 分布、延迟分位数和资源前后值。ASan
构建在 RSS 采样前调用 sanitizer allocator purge，以免把已释放对象的 allocator
cache 当成 live resource；LSan 仍在每个子进程和父 runner 退出时检查可达泄漏。

```sh
cmake -S . -B build/linux-gcc-debug \
  -DGATEWAY_TRANSPORT_SOAK_TESTS=ON \
  -DENABLE_SANITIZER_ADDRESS=ON \
  -DENABLE_SANITIZER_LEAK=ON
cmake --build build/linux-gcc-debug --target test_gateway_coronet_soak
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1 \
LSAN_OPTIONS=exitcode=23 \
GATEWAY_TRANSPORT_SOAK_DURATION_MS=300000 \
GATEWAY_TRANSPORT_SOAK_SEED=5065510917194994511 \
./build/linux-gcc-debug/bin/test_gateway_coronet_soak
```

详见 [GATEWAY_PROTOCOL.md](GATEWAY_PROTOCOL.md) 和
[ADR_GATEWAY_ABI.md](ADR_GATEWAY_ABI.md)。

行业业务 DLL 通过 `turbo_flow_gateway_business.h` 装配。它们消费 FlowStore
提交后的不可变事件，并把 schema-bound 业务 action 映射为公共 gateway
command；不参与协议 ACK 提交，也不拥有 MQTT 业务事实。DataBind codec 与
schema handle 保留在业务 DLL 内，跨 ABI 只传 schema/type 标识和 borrowed
payload。

当前首个实现是 `tf_gateway_business_ocpp201_core`。它明确是
`ocpp-2.0.1-core-minimal` profile，而不是完整 OCPP 2.0.1 conformance：

- committed 上行支持 `BootNotification`、`Heartbeat`、`Authorize` 与
  `StatusNotification`，并支持 `TransactionEvent` 的必填事实以及
  `offline`、`evse`、`idToken` 可选身份事实；
  完整 OCPP CALL JSON 仍是 FlowStore 中的原始事实，event 的
  `schema_id/type_name` 为空；
- 下行业务 action `reset` 要求
  `application/json + Ocpp201Core + ResetRequest`，输出公共 gateway command
  的 OCPP operation `Reset`；
- 下行业务 action `unlock-connector` 要求
  `application/json + Ocpp201Core + UnlockConnectorRequest`，输出 OCPP
  operation `UnlockConnector`；
- schema v5 的 `ChargingStation`、`BootNotificationReason`、
  `StatusNotificationRequest`、`ResetRequest` 与
  `UnlockConnectorRequest`，以及 TransactionEvent fact/info 单元由
  DataBind 2.0 `tbe_compiler` 在构建时生成 typed C binding。由于当前 DataBind
  runtime 不支持嵌套变长 message，OCPP adapter 校验外层嵌套形状，内层真实
  typed 单元分别绑定，不把伪结构加入 schema。`TransactionEventFacts` 将 required
  header 与 optional `offline` 组合，`TransactionEvseFacts` 将 required `id`
  与 optional `connectorId` 组合；二者使用 generated presence bitmap。
  `transactionInfo` 与 `idToken` 仍保持独立 typed unit。schema v4 类型继续保留，
  供已有 generated consumer 兼容；`meterValue` 和
  `transactionInfo` 的其他可选状态尚未装配时 fail fast。provider 不持有
  transaction sequence/state。
- minimal `Authorize` 只接受一个 `idToken` typed unit；证书与
  ISO 15118 certificate hash 扩展尚未装配时 fail fast。

完整产品配置阶段必须找到用于 gateway business bindings 的 host
`tbe_compiler`。SDK 未安装该工具时，通过
`TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE` 显式指定；缺失时配置 fail fast，
不会回退到运行时 schema 编译。

协议基线的一手资料：

- [MQTT-SN 1.2](https://mqtt.org/mqtt-specification/)
- [CoAP RFC 7252](https://www.rfc-editor.org/rfc/rfc7252)
- [LwM2M 1.2.2](https://www.openmobilealliance.org/release/LightweightM2M/V1_2_2-20240613-A/HTML-Version/OMA-TS-LightweightM2M_Core-V1_2_2-20240613-A.html)
- [OCPP specifications](https://openchargealliance.org/my-oca/ocpp/)
- [GB/T 32960 国家标准检索](https://openstd.samr.gov.cn/bzgk/std/std_list?p.p1=0&p.p2=GB%2FT32960&p.p90=circulation_date&p.p91=desc)
- [JT/T 808-2019 第 1 号修改单](https://xxgk.mot.gov.cn/jigou/kjs/202111/t20211111_3625608.html)
