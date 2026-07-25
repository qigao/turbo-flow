# Flowie MQTT 测试矩阵

本文定义 Flowie MQTT broker/client 的增量测试计划。它补充
[`RELEASE_GATE.md`](RELEASE_GATE.md)，不扩大
[`ARCHITECTURE.md`](ARCHITECTURE.md) 声明的产品边界，也不把“已有若干成功用例”等同于
MQTT 规范符合性或生产稳定性。

## 基线与目标

截至 2026-07-24，Windows release preset 当前注册 `93` 个 CTest；其中
`test_turbo_flow_pgsql_live` 与 `flowie_server_check_smb_product` 仍为 Disabled。注册数量不是通过数量，
本矩阵不再沿用旧的 `92/92` 通过结论。此前 targeted run 暴露的
`MQTT-SOAK-004`、`MQTT-SOAK-005`、`MQTT-OWNER-003` 和 dev-server BDD 问题均已完成修复并复验；
soak `1/1`、endpoint/dev-server `2/2`、StorageBackend/local/product `4/4` 通过，且
`flowie_server --check` 通过。本轮新增的 TurboFlow disruptor 与 HTTPS auth 回归也已在远端 Linux
完整执行：`test_turbo_flow` 为 `138/138`，`test_turbo_flow_http` 为 `17/17`。这些均为 focused
或子集证据，不能替代完整产品 gate 或规定的 30/60 分钟 scheduled soak。

已覆盖的行为范围包括协议、client、endpoint、session/store fault、真实 TCP/TLS/WS/WSS/Pipe、BDD、
public MQTT smoke、Redis live、ACL/RPC/dashboard 以及 Flowie Graph integration。现有测试已覆盖 MQTT 3.1/3.1.1/5、QoS 0/1/2、retained、
Will/Will Delay、session resume、Topic Alias、Subscription Identifier、Receive Maximum、
Enhanced AUTH、ACL、真实 TCP/TLS/WS/WSS/Pipe 基本往返及 WS/WSS admission 安全边界。

上述注册数来自 CTest 配置，修复与复验结果来自测试输出；不代表组合状态空间已穷尽。与 Mosquitto 官方
broker suite 的场景分类相比，固定版本跨 broker 互操作、持续 sanitizer fuzz 和规定时长的长稳仍属于
独立执行环境。本矩阵使这些范围可逐项复验并进入分层 gate；已经进入 release gate 的子集仍按具体
测试证据界定。

首批实现已落在 `flowie/protocol/tests/test_flowie_mqtt_protocol_matrix.c`，并注册为
`test_flowie_mqtt_protocol_matrix`。它覆盖 MQTT-PROTO-001 的 fixed-header 组合、PROTO-005/008
的 VBI 与分片/粘包边界、PROTO-002/003/004/006 的 CONNECT/PUBLISH property 代表性矩阵及
PROTO-007 的 MQTT 5 control reason-code 合法集合，以及 PROTO-009 的 43 个 packet type/level
canonical round-trip；这些是对应 ID 的第一批可执行样本，不是
全部 packet/property 交叉积。该实现还修正了接收端 QoS 0 + DUP fixed-header 的协议边界。
当前矩阵已经扩展为 27 个 property specimen 与 15 个 packet/property 上下文的交叉验证。
`test_flowie_mqtt_protocol_corpus` 另外对 MQTT 3.1/3.1.1/5 固定 corpus 执行所有截断、逐 bit
变异和固定 seed 随机流，作为 MQTT-FUZZ-001/002/003 的 deterministic replay 层；它不替代
sanitizer 持续 fuzz job。

## 支持标签与验证证据

矩阵不使用程度状态。每个 ID 使用方括号标签声明适用范围：

- `[🪟]`、`[🐧]`：分别表示 Windows 与 Linux；平台标签表示可构建的支持范围，不等同于本轮通过。
- `[redis]`、`[pgsql]`：分别需要 Redis 或 PostgreSQL live backend。
- `[fixed-broker]`：需要固定版本的外部 MQTT broker/client。
- `[sanitizer]`、`[scheduled]`：分别需要 sanitizer job 或规定时长的调度任务。

只带平台标签的项目默认在对应 OS 的常规 gate 中始终执行，不需要额外的本机标签。

标签只有“有/没有”，不表达“一半实现”。实际结果独立记录，不能用一个平台的结果推定另一个平台。
2026-07-24 的 `[🪟]` 条目以 93 个注册测试为基线；focused run 当前通过，但不能写成
全量通过。CoroNet EOF/TLS 修复后的相邻 executable 证据仍需与完整 gate 分开记录。Linux full CTest
本轮尝试在未改动的 `test_turbo_flow_email` POP3 pending-source 用例处持续等待，未形成完整退出结果；
这不影响上文两个直接相关 executable 的完整通过证据。此前在 `root@eu`
以安装后的 TurboNet 重链接 Flowie 的无容器 release 子集 `23/23`、client/transport/endpoint
各连续 5 轮，以及 dev server BDD/Mosquitto client interop 各连续 10 轮，均属于历史证据；当前
focused run 的通过结果也不等于完整 scheduled gate。TurboNet 自身
Linux CTest 为 `49/49`，每项连续 3 次通过。Windows 本机 gate 和本轮 Linux 无容器 gate 均不包含
PostgreSQL、固定外部 broker、sanitizer 或规定时长的 scheduled job。

| 执行标签 | 本轮结果 | 证据边界 |
|---|---|---|
| `[🪟]` | Blocked | CTest 注册 93 个；Disabled 2 个；focused soak/endpoint/BDD/storage checks 通过，完整 gate 尚未重跑 |
| `[🪟][redis]` | Passed | Redis live 已运行并通过 |
| `[🪟][pgsql]` | Not run | PostgreSQL live 不属于本机 gate |
| `[🪟][fixed-broker]` | Not run | 固定互操作 targets 编译通过，但未把编译冒充运行结果 |
| `[🪟][sanitizer]` | Not run | deterministic corpus 已通过；持续 sanitizer job 未运行 |
| `[🪟][scheduled]` | Not run | focused soak 已通过；未运行规定的 30/60 分钟任务 |
| `[🐧]` | Passed | TurboNet release build + CTest `49/49`，每项连续 3 次；Flowie release 重链接、无容器子集 `23/23`；client/transport/endpoint 各 5 轮；dev BDD/Mosquitto client interop 各 10 轮 |
| `[🐧][redis]` | Not run | 按本轮范围保持 Docker/Redis 停止；未把配置检查冒充 live backend 测试 |
| `[🐧][pgsql]` | Not run | 按本轮范围保持 Docker/PostgreSQL 停止 |
| `[🐧][fixed-broker]` | Not run | Mosquitto CLI -> Flowie 已通过；固定外部 broker container 未启动 |
| `[🐧][sanitizer]` | Not run | deterministic corpus 已通过；Clang sanitizer/fuzz job 未运行 |
| `[🐧][scheduled]` | Not run | 未运行规定的 30/60 分钟 soak |

主要证据锚点：protocol matrix/corpus 对应 PROTO/FUZZ；session owner 与 endpoint integration
对应 OWNER/STORE；transport 与 client lifecycle 对应 NET；client mTLS、endpoint security 和 control
auth cache 对应 SEC；public HiveMQ/EMQX live suite 只能作为 INTEROP 的部分证据；release manifest 对应 GATE。

非目标：

- 不以测试伪造尚未声明的 broker 能力，例如 bridge、动态安全插件或共享集群 session owner。
- 不把 public MQTT endpoint 当作可复现的唯一互操作环境。
- 不要求 fuzz、soak 或故障注入进入每次提交的快速测试。
- 不以业务 Log 消息持久化代替 session、inflight、retained 或 pending Will 的事实源验证。

## 通用测试契约

每个下表 ID 是稳定的行为标识。实现时一个 `it(...)` 只验证一个可描述行为；矩阵行若包含多个
版本、报文类型或故障点，必须生成独立 TinyTest case 或以带名称的参数逐项报告，不能在首个失败
后隐藏其余组合。

所有测试遵守以下约束：

- 协议 oracle 同时检查 wire 结果、连接终态和 owner 状态；仅检查返回码不足以证明正确。
- parser 失败不得暴露未验证 span，`consumed` 不得超过输入长度，输出对象保持调用前的无效态。
- session owner 是连接、订阅、QoS、retained 和 Will 状态的唯一权威；Queue/store 只验证其明确
  拥有的记录。失败后必须从权威状态读取验证，不从日志推断。
- 真实网络测试使用 CoroNet TCP/TLS/WS/WSS/Pipe，保持 event-loop/socket 单 owner；不得用裸平台
  socket 绕过生产传输路径。
- 每例使用随机 client ID、topic prefix、数据库 namespace/schema；清理只删除该例资源。即使断言
  失败，也必须走 fixture teardown，关闭 socket、drain/stop owner 并释放证书和临时目录。
- 所有容量、随机种子、故障点和超时均显式记录。禁止依赖测试执行顺序、共享 retained topic 或
  固定 packet identifier。
- 测试断言使用 TinyTest typed assertions，不自定义 `main()`。不同故障域使用独立 executable，
  避免一个崩溃掩盖后续结果。

协议 correctness 向量使用编译期 C table，直接保存 binary input、MQTT version、expected result、
consumed bytes 与状态 oracle，避免 CSV/XML 对二进制、空值、嵌套 property 和分片序列的二次解析歧义。
fuzz corpus 使用原始 `.bin` 与独立 metadata；soak/benchmark 明细可使用 CSV。TinyTest 的执行结果通过
`--junit <file>` 输出 JUnit XML 给 CI，或通过 `--tap` 输出 TAP；XML 不作为测试向量输入。

建议 CTest 标签及默认时限：

| 标签 | 用途 | 默认时限 | Gate |
|---|---|---:|---|
| `mqtt-fast` | parser、codec、纯状态机 | 10 s/executable | 每次提交 |
| `mqtt-negative` | 非法输入与边界拒绝 | 20 s/executable | 每次提交 |
| `mqtt-transport` | 本地真实 CoroNet 传输 | 90 s/executable | 每次提交 |
| `mqtt-persistence` | 重启、CAS、故障点、真实 backend | 180 s/executable | release |
| `mqtt-security` | TLS/mTLS/auth/ACL 失败矩阵 | 180 s/executable | release |
| `mqtt-interop` | 固定版本的外部 broker/client | 300 s/executable | nightly/release |
| `mqtt-fuzz` | corpus replay 与 sanitizer fuzz | 由 job 指定 | nightly |
| `mqtt-soak` | 长稳、混沌与资源恢复 | 由 job 指定 | scheduled |

## A. 协议合法性与 parser

`flowie/protocol/tests/test_flowie_mqtt_protocol_matrix.c` 不打开网络，输入是完整或
分片 wire bytes，oracle 来自 MQTT 3.1、3.1.1、5 的对应规范与 Flowie 明确记录的 level 3 差异。

| ID | 优先级 | 支持/环境标签 | 前置条件与事件序列 | 预期 wire/错误 | 权威终态、隔离 | 标签/超时 |
|---|---|---|---|---|---|---|
| MQTT-PROTO-001 | HIGH | `[🪟][🐧]` | 对每种 packet type 枚举 fixed-header flags；输入一个合法 body | 仅规范允许的 flags 被接受；其余返回 protocol error | parser 无状态；失败输出不可读；逐 packet buffer | `mqtt-fast;mqtt-negative` / 10 s |
| MQTT-PROTO-002 | HIGH | `[🪟][🐧]` | 对 MQTT 5 packet type x property 建立允许矩阵 | 允许项解析成功；禁止项返回 protocol error | property view 仅借用当前输入；逐组合重置 | `mqtt-fast;mqtt-negative` / 10 s |
| MQTT-PROTO-003 | HIGH | `[🪟][🐧]` | 每个不可重复 property 出现两次；User Property 出现 0..limit 次 | 前者拒绝；后者在上限内接受，超限返回 resource/protocol error | 不保留部分 property；测试使用显式上限 | `mqtt-fast;mqtt-negative` / 10 s |
| MQTT-PROTO-004 | HIGH | `[🪟][🐧]` | Receive Maximum=0、Maximum Packet Size=0、Topic Alias=0 或超过协商上限 | range error；endpoint 映射为 MQTT 5 CONNACK/DISCONNECT `0x82`，已建连 Topic Alias 越界为 `0x94` | negotiated limits 不改变；每例新连接状态 | `mqtt-negative` / 20 s |
| MQTT-PROTO-005 | HIGH | `[🪟][🐧]` | VBI 使用截断、5 字节、溢出或非最短编码；分别放在 Remaining Length 和 property length | malformed packet；不得读过输入 | `consumed <= input_size`；guard bytes 保持不变 | `mqtt-negative` / 20 s |
| MQTT-PROTO-006 | HIGH | `[🪟][🐧]` | UTF-8 包含截断序列、NUL、surrogate、non-character；二进制/string 长度跨越尾部 | malformed packet；合法边界值 round-trip | 无借用 span 越界；每例独立 immutable input | `mqtt-negative` / 20 s |
| MQTT-PROTO-007 | HIGH | `[🪟][🐧]` | 枚举 CONNACK/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/DISCONNECT/AUTH reason code | packet/version 不允许的 code 被拒绝 | codec/parser 不保留 reason code | `mqtt-fast;mqtt-negative` / 10 s |
| MQTT-PROTO-008 | HIGH | `[🪟][🐧]` | 在 0、1、127、128、最大允许包和上限+1 处分片并拼包 | incomplete 与 complete 精确区分；粘包只消费第一帧长度 | framing buffer 是唯一 incomplete-byte owner；逐边界重建 | `mqtt-fast;mqtt-negative` / 20 s |
| MQTT-PROTO-009 | MED | `[🪟][🐧]` | 对每个成功解析的 packet 执行 encode -> parse -> encode | 规范化后的 bytes 和 typed fields 一致 | 中间 buffer 在下一轮前释放 | `mqtt-fast` / 20 s |
| MQTT-PROTO-010 | MED | `[🪟][🐧]` | level 3/4/5 CONNECT protocol-name/level 交叉组合，随后发送第二个 CONNECT | 仅 `MQIsdp/3`、`MQTT/4|5` 接受；第二 CONNECT 终止连接 | negotiated version 首包后不可变 | `mqtt-negative` / 20 s |

Endpoint 对 parser 分类的 wire oracle 必须独立断言：握手前能安全识别的 MQTT 5 CONNECT 错误返回对应
CONNACK reason，否则关闭；已建连 MQTT 5 协议错误在规范允许时发送 DISCONNECT；MQTT 3.x 无对应负
响应时直接关闭。测试不得把“关闭”与“发送错误包后关闭”视为同一个结果。

## B. Session owner 与 MQTT 状态机

扩展 `flowie/tests/test_flowie_endpoint.c`，当单文件明显膨胀时拆成
`test_flowie_session_state.c` 和 `test_flowie_session_limits.c`。所有事件通过 owner 公开/测试边界进入，
不可直接篡改内部字段制造终态。

| ID | 优先级 | 支持/环境标签 | 前置条件与事件序列 | 预期 wire/错误 | 权威终态、隔离 | 标签/超时 |
|---|---|---|---|---|---|---|
| MQTT-OWNER-001 | HIGH | `[🪟][🐧]` | QoS 1 PUBLISH 在 graph 成功、失败、超时和 ACK enqueue 失败点各执行一次 | 仅配置 settlement 达成后 PUBACK；失败不承诺 ACK | session inflight 为权威；已提交 Queue 记录不回滚，重投可重复 | `mqtt-fast;mqtt-negative` / 20 s |
| MQTT-OWNER-002 | HIGH | `[🪟][🐧]` | QoS 2 在 PUBLISH、PUBREC、PUBREL、PUBCOMP 前后断开并以同 session 重连 | reason/packet ID/DUP 符合阶段；完成一次业务 settlement | durable session inflight 阶段为权威；逐阶段新 namespace | `mqtt-fast;mqtt-persistence` / 60 s |
| MQTT-OWNER-003 | HIGH | `[🪟][🐧]` | 相同 client ID 建立第二连接，在旧连接 pending send/recv 时 takeover | 旧连接关闭，新 generation 独占 route；旧 completion 不可发到新连接 | session generation 为权威；等待旧 callback drain | `mqtt-transport;mqtt-negative` / 60 s |
| MQTT-OWNER-004 | HIGH | `[🪟][🐧]` | Receive Maximum=1，连续生成多条 outbound QoS 1/2，再逐条 ACK | wire 同时最多一个未确认包，ACK 后按序释放窗口 | outbound inflight 为权威；断开并清空 session | `mqtt-fast` / 20 s |
| MQTT-OWNER-005 | HIGH | `[🪟][🐧]` | 达到 connection/session/subscription/inflight/retained/send HWM 后再加一项 | 返回版本可表达的 quota reason；无法表达时关闭；无 silent drop | 对应 owner count 不超过上限且失败项不存在 | `mqtt-negative` / 30 s |
| MQTT-OWNER-006 | HIGH | `[🪟][🐧]` | retained 新增、替换、零 payload 删除；订阅 RAP/RH/NL 组合 | retain flag 与投递次数符合 option；删除后新订阅无历史消息 | retained store 为权威；随机 topic 清理 | `mqtt-fast` / 30 s |
| MQTT-OWNER-007 | HIGH | `[🪟][🐧]` | persistent session 订阅后断开，期间发布，再以 Clean Start false/true 重连 | false 恢复并投递；true 丢弃旧 session；Session Present 正确 | session store 为权威；结束时显式 clean session | `mqtt-fast;mqtt-persistence` / 60 s |
| MQTT-OWNER-008 | HIGH | `[🪟][🐧]` | graceful DISCONNECT、网络丢失、takeover、Will Delay、session expiry 先后竞争 | 仅非正常断开按 deadline 发布一次 Will；取消条件不发布 | pending Will record 为权威；使用虚拟/可控时钟 | `mqtt-fast;mqtt-persistence` / 60 s |
| MQTT-OWNER-009 | MED | `[🪟][🐧]` | shared group 三订阅者含一个慢消费者，连续发布并触发其 HWM | 每条只选一个可用成员；慢成员不阻塞普通订阅者 | subscription index + per-session HWM；逐组随机前缀 | `mqtt-transport` / 90 s |
| MQTT-OWNER-010 | MED | `[🪟][🐧]` | overlapping filters、Subscription Identifier、No Local、`$SYS` 边界组合 | 每 session 的投递、QoS 合并和 identifier 列表精确 | subscription index 为权威；每例独立 session 集合 | `mqtt-fast` / 30 s |
| MQTT-OWNER-011 | MED | `[🪟][🐧]` | Message Expiry 跨 Queue 等待、session offline 和重启边界 | 过期消息不投递；剩余 expiry 单调减少 | record timestamp/expiry 为权威；使用可控时钟 | `mqtt-persistence` / 90 s |
| MQTT-OWNER-012 | MED | `[🪟][🐧]` | MQTT 3.1/3.1.1 publisher 向 MQTT 5 subscriber 发布，反向再测一次 | 5 -> 3.x 去 property；3.x -> 5 有空 property block；payload 不变 | 每个 subscriber negotiated version 为权威 | `mqtt-fast` / 30 s |

## C. 真实传输与生命周期

扩展 `flowie/tests/test_flowie_transport.c`，并按 transport 参数生成具名用例。测试使用仓库已有证书 fixture
与 CoroNet helper，验证真实握手和关闭，不以 YAML 解析成功代替网络证据。

| ID | 优先级 | 支持/环境标签 | 前置条件与事件序列 | 预期 wire/错误 | 权威终态、隔离 | 标签/超时 |
|---|---|---|---|---|---|---|
| MQTT-NET-001 | HIGH | `[🪟][🐧]` | TCP/TLS/WS/WSS/Pipe x level 3/4/5：CONNECT、SUBSCRIBE、QoS 0/1/2 publish、UNSUBSCRIBE、PING、DISCONNECT | 每步收到版本正确的 ACK/投递；正常 EOF | connection/session owner 归零；每组合随机端口/pipe | `mqtt-transport` / 90 s |
| MQTT-NET-002 | HIGH | `[🪟][🐧]` | TCP/TLS/WS/WSS/Pipe 将两字节 Remaining Length 的 CONNECT 逐 byte 发送，再合并发送两个 PING | CONNECT 只完成一次；CONNACK 与两个 PINGRESP wire 结果、顺序正确 | 每个 transport 独立 endpoint；断开后 session load 归零 | `mqtt-transport;mqtt-negative` / 90 s |
| MQTT-NET-003 | HIGH | `[🪟][🐧]` | 每种 stream transport 单次 write 合并 CONNECT+两个 PING；owner lane 单次 write 合并两个 QoS1 PUBLISH，以及两个 PUBACK+PING | CONNACK/PINGRESP、publisher PUBACK 与 subscriber delivery 均按 packet 顺序生成，无丢帧或重复消费 | framing consumed 精确；ACK 后 inflight 释放，断开后 session 归零 | `mqtt-transport` / 90 s |
| MQTT-NET-004 | HIGH | `[🪟][🐧]` | pending recv、pending send、TLS handshake、WS close 各阶段触发 server stop | operation 以 shutdown/closed 结束，无 callback 访问已释放 owner | stop -> interrupt -> drain -> destroy 顺序；句柄回基线 | `mqtt-transport;mqtt-negative` / 90 s |
| MQTT-NET-005 | MED | `[🪟][🐧]` | client worker callback 内 enqueue 后续异步命令，再并发 destroy | accepted command 完成一次；拒收命令返回 shutdown；无死锁 | client worker 是状态 owner；join 后释放 context | `mqtt-transport` / 90 s |
| MQTT-NET-006 | HIGH | `[🪟][🐧]` | WS/WSS 精确/错误 path，`mqtt`/缺失/错误 token，携带合法 CONNECT 的 text frame，单帧/累计分片超限，非法 close；每次拒绝后连接合法 binary client | 仅精确 path、`mqtt` token 和 binary data 成功；text 返回 1003，超限返回 1009，非法 close 关闭 | 拒绝发生在 MQTT admission 前，protocol resource 的 session load 保持 0；listener 继续服务 | `mqtt-transport;mqtt-negative;mqtt-security` / 90 s |

`MQTT-NET-004` 已覆盖确定性 shutdown、Windows process handle 与 Linux `/proc/self/fd` 基线；TLS/WSS
在取基线前先完成一次 OpenSSL warm-up，避免把全局一次性初始化误判为泄漏。

当前 `test_flowie_transport` 已为 MQTT-NET-002 提供五种 transport 的真实连接证据，并覆盖
MQTT-NET-003 的 CONNECT+PING 合包；`test_flowie_endpoint` 进一步验证两个 QoS1 PUBLISH 合包及
PUBACK+下一 PING 合包的 owner/inflight 终态。MQTT-NET-004 由 `test_flowie_transport` 覆盖
established TCP pending recv、已建 session 的 MQTT 半包、TLS/WSS 半开握手和 WSS 未完成 close frame；
`test_flowie_endpoint` 以实时 Queue load 证明大包 fan-out send 仍在途后触发 stop。所有 stop 均受 2 秒
上限约束，并在返回后验证 connection、session、Queue 与 send budget 归零。

## D. 持久化与故障注入

使用 `flowie/tests/test_flowie_session_store_faults.c` 作为 provider-neutral contract harness；
Redis/PostgreSQL adapter 复用同一场景表并放入各自 live target。故障注入点必须位于
record-store 适配器边界，不能通过任意 sleep 猜测时序。

| ID | 优先级 | 支持/环境标签 | 前置条件与事件序列 | 预期 wire/错误 | 权威终态、隔离 | 标签/超时 |
|---|---|---|---|---|---|---|
| MQTT-STORE-001 | HIGH | `[🪟][🐧]` | clone 后、commit 前注入失败，随后销毁并重建 endpoint | 无成功 ACK；重启后看不到未提交迁移 | store 为权威，旧 owner 未 swap；随机 namespace | `mqtt-persistence;mqtt-negative` / 180 s |
| MQTT-STORE-002 | HIGH | `[🪟][🐧]` | commit 成功后、owner swap/ACK enqueue/send 前分别失败并重启 | store 保留提交；客户端重投允许 at-least-once duplicate，不丢失 | durable record 为权威；按 message key 去重观察 | `mqtt-persistence` / 180 s |
| MQTT-STORE-003 | HIGH | `[🪟][🐧]` | 两个 generation 对同 session revision 提交 CAS | 仅一个成功；失败 generation 不覆盖新状态 | store revision 为权威；结束删除 namespace | `mqtt-persistence;mqtt-negative` / 180 s |
| MQTT-STORE-004 | HIGH | `[🪟][🐧]` | 对 MQTT-OWNER-002 的每个 QoS 2 阶段提交后强制重建 | 恢复到已提交阶段，不重复业务处理，不跳过 ACK | inflight record 为权威 | `mqtt-persistence` / 180 s |
| MQTT-STORE-005 | HIGH | `[🪟][🐧]` | retained replace/delete 与 pending Will schedule/cancel 的 commit 两侧重建 | 恢复结果等于最后一次成功 commit | retained/Will record 为各自权威 | `mqtt-persistence` / 180 s |
| MQTT-STORE-006 | HIGH | `[🪟][🐧]` | startup scan 遇到截断、未知版本、超限或字段冲突记录 | listener 启动前 fail fast，无 volatile fallback | 原记录不被静默改写；隔离 schema 保留供诊断后清理 | `mqtt-persistence;mqtt-negative` / 180 s |
| MQTT-STORE-007 | HIGH | `[🪟][🐧]` | backend 启动不可用、运行中断连、commit reply 丢失后恢复 | 启动失败或当前操作失败；不得假装 commit/ACK | 不确定提交按 adapter 契约重读/CAS 决断 | `mqtt-persistence;mqtt-negative` / 180 s |
| MQTT-STORE-008 | MED | `[🪟][🐧]` | session/message expiry cleanup 与新写入并发 | 仅过期 revision 被删，新 revision 不被旧 cleanup 删除 | store revision + deadline 为权威；可控时钟 | `mqtt-persistence` / 180 s |
| MQTT-STORE-009 | MED | `[🪟][🐧]` | binary retained key、最大合法 client/topic/property/payload 边界 round-trip | bytes、长度、flags、origin metadata 完全一致 | serialized record 为权威；逐后端随机 namespace | `mqtt-persistence` / 180 s |
| MQTT-STORE-010 | MED | `[🪟][🐧][redis][pgsql]` | Redis/PostgreSQL 运行同一 provider-neutral trace | 可观察 MQTT wire 与恢复状态一致；错误码映射一致 | 不比较 backend 内部布局；各自隔离资源 | `mqtt-persistence` / 300 s |

## E. TLS、认证与 ACL

新增 `flowie/tests/test_flowie_security_integration.c`；HTTPS auth service 使用本地可编程 TLS fixture，禁止访问
生产服务。证书与 token 均为测试专用，日志断言要确认不会输出密码、token、Authentication Data 或私钥。

| ID | 优先级 | 支持/环境标签 | 前置条件与事件序列 | 预期 wire/错误 | 权威终态、隔离 | 标签/超时 |
|---|---|---|---|---|---|---|
| MQTT-SEC-001 | HIGH | `[🪟][🐧]` | 错误 CA、SAN mismatch、expired cert、证书/私钥不匹配、TLS 版本/套件不允许 | 握手失败，MQTT parser 不收到 CONNECT | TLS connection 无 session；每例独立 listener | `mqtt-security;mqtt-negative` / 180 s |
| MQTT-SEC-002 | HIGH | `[🪟][🐧]` | mTLS listener 分别接收无 client cert、非信任 cert、有效 cert | 前两者握手失败；有效证书得到 `X509_V_OK` 后才进入 CONNECT | verified identity 由 TLS owner 借给 auth；关闭后失效 | `mqtt-security` / 180 s |
| MQTT-SEC-003 | HIGH | `[🪟][🐧]` | HTTPS auth 返回 timeout、TLS error、非 2xx、错误 Content-Type、malformed/oversized JSON、错误 version/principal | MQTT 5 CONNACK `0x86` 或连接关闭；无 DB/anonymous fallback | SecurityRealm 不增加 principal；fixture 清空请求 | `mqtt-security;mqtt-negative` / 180 s |
| MQTT-SEC-004 | HIGH | `[🪟][🐧]` | Enhanced AUTH 缺失/错误 method、challenge data 越界、provider callback 失败、re-auth 改变 principal | AUTH/CONNACK/DISCONNECT 返回匹配 reason；owner change 关闭 | 原 session identity 不被部分刷新 | `mqtt-security;mqtt-negative` / 180 s |
| MQTT-SEC-005 | HIGH | `[🪟][🐧]` | default deny；publish/subscribe 每个 filter 混合 allow/deny，含 `#`、`+`、`$SYS` 与 containment 边界 | MQTT 5 返回逐操作 `0x87`；MQTT 3.x 按可表达能力拒绝/关闭；无 graph work | ACL generation + session principal 为判定输入 | `mqtt-security;mqtt-negative` / 120 s |
| MQTT-SEC-006 | MED | `[🪟][🐧]` | active session 期间发布新 ACL generation 或撤销 credential，随后 publish、subscribe、re-auth/reconnect | 行为严格符合已声明的 generation 生效点；不得混用两代规则 | published ACL bundle 是唯一事实源；测试后删除 root | `mqtt-security` / 180 s |
| MQTT-SEC-007 | MED | `[🪟][🐧]` | auth cache hit、TTL expiry、negative cache、provider unavailable、并发相同 credential | cache 命中不访问 provider；过期重新认证；失败默认 deny | provider result 为源、cache 为有界派生数据；使用计数 fixture | `mqtt-security` / 180 s |
| MQTT-SEC-008 | MED | `[🪟][🐧]` | WSS Origin 缺失/错误/允许值，仅当产品配置声明 Origin policy | 按显式 policy 接受或拒绝；未声明能力不得暗示校验 | WS handshake policy 为权威；每例新连接 | `mqtt-security;mqtt-transport` / 180 s |

## F. 跨实现互操作

新增 `flowie/interop/` runner，固定 Mosquitto、EMQX、HiveMQ CE 的发布版本或 container digest；测试报告
记录 broker/client 版本、transport、MQTT level 和随机种子。public endpoint 只保留 smoke，不作为唯一证据。

| ID | 优先级 | 支持/环境标签 | 前置条件与事件序列 | 预期 wire/错误 | 权威终态、隔离 | 标签/超时 |
|---|---|---|---|---|---|---|
| MQTT-INTEROP-001 | MED | `[🪟][🐧][fixed-broker]` | Flowie client -> 固定 broker，level 4/5 x TCP/TLS/WS/WSS，执行 connect/sub/pub/unsub/disconnect | QoS 0/1/2 payload 与 ACK 完整；无 provider-specific waiver | 外部 broker namespace；容器销毁 | `mqtt-interop` / 300 s |
| MQTT-INTEROP-002 | MED | `[🪟][🐧][fixed-broker]` | Mosquitto CLI/library client -> Flowie，执行同一核心 trace | Flowie wire 与 session 终态符合版本契约 | Flowie session store；随机 namespace | `mqtt-interop` / 300 s |
| MQTT-INTEROP-003 | MED | `[🪟][🐧][fixed-broker]` | 双向测试 retained、persistent session、offline QoS、Will、message expiry | 重连后投递与 expiry 一致 | 各 broker 自身 store；每 trace 全新实例 | `mqtt-interop;mqtt-persistence` / 300 s |
| MQTT-INTEROP-004 | MED | `[🪟][🐧][fixed-broker]` | MQTT 5 properties、Topic Alias、Subscription Identifier、Response Topic/Correlation Data | 支持字段 byte-for-byte 保留；不支持组合明确跳过并记录能力 | wire capture + receiving client 为 oracle | `mqtt-interop` / 300 s |
| MQTT-INTEROP-005 | LOW | `[🪟][🐧][fixed-broker]` | level 3 TCP/TLS；WS/WSS 仅在对端支持 `mqtt` token 与该 dialect 时运行 | 支持时核心 trace 成功；能力不匹配显示 SKIP 而非 PASS | 不改变 Flowie 已声明的 token 边界 | `mqtt-interop` / 300 s |

## G. Fuzz、生成式测试与 corpus

新增独立 fuzz target，入口覆盖 framing、property block、CONNECT/PUBLISH/SUBSCRIBE/UNSUBSCRIBE/control
view。常规构建只运行固定 corpus replay；ASan/UBSan 或 Windows 对应 sanitizer job 执行持续 fuzz。

| ID | 优先级 | 支持/环境标签 | 输入生成 | 不变量 | 隔离与复现 | 标签 |
|---|---|---|---|---|---|---|
| MQTT-FUZZ-001 | HIGH | `[🪟][🐧][sanitizer]` | 任意 bytes -> framing/parser | 不崩溃、不越界、不 UAF；结果确定；`consumed <= size` | 保存最小化输入、seed、build profile | `mqtt-fuzz;mqtt-negative` |
| MQTT-FUZZ-002 | HIGH | `[🪟][🐧][sanitizer]` | 合法 packet AST 生成后随机破坏 property/length/UTF-8/flags | 成功则可重编码并复解析；失败不暴露 span | 每输入新 parser output | `mqtt-fuzz` |
| MQTT-FUZZ-003 | MED | `[🪟][🐧][sanitizer]` | 合法 packet stream 随机分片、合并、截断 | 完整前不消费成包，完整后与未分片结果一致 | 记录 chunk vector | `mqtt-fuzz` |
| MQTT-FUZZ-004 | MED | `[🪟][🐧]` | packet/state sequence model 生成 CONNECT 到 DISCONNECT 事件 | owner 不变量、packet ID uniqueness、HWM、ACK 前置条件始终成立 | 缩减为最小事件 trace | `mqtt-fuzz` |

初始 corpus 必须包含本矩阵 A 类的每个非法边界、各版本最小合法 packet、最大 VBI 边界、历史回归输入。
任何发现的 crash 或语义偏差先加入 deterministic regression，再修复实现。

## H. Soak、混沌与资源边界

这些用例使用独立 executable/job，不进入 `mqtt-fast`。每次记录持续时间、消息数、payload 分布、并发数、
随机种子、P50/P95/P99、失败率、进程内存、线程/句柄数和 backend retry 次数。

| ID | 优先级 | 支持/环境标签 | 负载与故障 | 验收条件 | 权威终态、清理 | 标签/建议时长 |
|---|---|---|---|---|---|---|
| MQTT-SOAK-001 | MED | `[🪟][🐧][scheduled]` | reconnect storm + 相同/不同 client ID takeover | 无死锁/UAF；拒绝明确；结束后 connection/session 数符合 expiry | owner counters；停止后等待 drain | `mqtt-soak` / 30 min |
| MQTT-SOAK-002 | MED | `[🪟][🐧][scheduled]` | 100k subscription add/remove 与并发 publish | topic index 结果正确，延迟无持续增长 | subscription owner；结束删除 sessions | `mqtt-soak` / 30 min |
| MQTT-SOAK-003 | HIGH | `[🪟][🐧][scheduled]` | 一个慢 subscriber + 多个正常 subscriber，填满 send HWM | 慢连接被隔离/拒绝，正常连接持续前进，无无界内存 | per-connection Queue/HWM；内存回基线 | `mqtt-soak` / 30 min |
| MQTT-SOAK-004 | HIGH | `[🪟][🐧][scheduled]` | TLS/WSS connect/disconnect loop，随机在 handshake/read/write/close 停止 | 无句柄、证书、socket、线程泄漏 | OS handle + allocator counters；实例销毁 | `mqtt-soak;mqtt-security` / 60 min |
| MQTT-SOAK-005 | HIGH | `[🪟][🐧][scheduled][redis][pgsql]` | Redis/PG 短断、超时、重连和 commit reply 丢失 | 无 silent ACK/数据丢失；重复仅出现在声明的 at-least-once 边界 | store scan + MQTT trace；独立 namespace | `mqtt-soak;mqtt-persistence` / 60 min |
| MQTT-SOAK-006 | MED | `[🪟][🐧][scheduled]` | ingress command queue 与 worker Disruptor 分别达到 HWM，同时触发 shutdown | fail fast，无任务泄漏，accepted/durable 状态可解释 | 各边界 counters；drain 后为零 | `mqtt-soak` / 30 min |

默认资源验收：warm-up 后 RSS 不持续单调增长；停止并完成 allocator/backend 延迟释放后，Flowie 自有
allocation、连接、session、route、pending command 和 CoroNet handle 回到 fixture 基线。具体数值阈值需先
由稳定 CI 主机建立 10 次基线，再以中位数和离散度固化，不能在代码中猜一个通用绝对值。

## I. 发布门禁与执行顺序

| ID | 优先级 | 支持/环境标签 | 检查 | 失败条件 | 标签/超时 |
|---|---|---|---|---|---|
| MQTT-GATE-001 | HIGH | `[🪟][🐧]` | configure/CTest guard 枚举 `flowie-release` 必需 live tests | 任一必需测试不存在或 `Disabled` 即失败，不允许以 0 tests 通过 | `flowie-release` / 10 s |
| MQTT-GATE-002 | HIGH | `[🪟][🐧][redis][pgsql][fixed-broker]` | release manifest 收集 test、label、backend/version、证书模式与结果 | 缺少 Redis、public/固定 interop、真实 TLS/mTLS 证据即失败 | `flowie-release` / 10 s |
| MQTT-GATE-003 | MED | `[🪟][🐧][scheduled][sanitizer]` | nightly corpus、soak 和 sanitizer 结果关联同一 revision | crash、sanitizer finding、资源单调增长或无法复现的缺失 seed 即失败 | scheduled job |

实施顺序：

1. **P0 / HIGH correctness**：MQTT-PROTO-001..008、OWNER-001..008、NET-001..004、
   STORE-001..007、SEC-001..005、FUZZ-001、GATE-001。先建立 protocol/state/persistence 的失败 oracle。
2. **P1 / MED compatibility**：补齐 owner 边界、provider-neutral persistence trace、本地 TLS/auth fixture、
   固定版本互操作与 corpus replay。
3. **P2 / operational assurance**：生成式 state model、soak/chaos、资源基线和 release manifest。

P0 完成标准不是用例文件存在，而是：所有 HIGH ID 有独立可筛选结果；`mqtt-fast` 无网络依赖且零失败；
本地 transport 使用真实 socket；provider-neutral fault contract 通过；Redis release job 开启且非 Disabled；错误路径的
wire、owner 和 store 三类断言同时成立。

## 与现有测试的映射

以下映射记录当前直接证据，后续扩展继续复用相同 fixture，避免复制同一 happy path：

- Enhanced AUTH/re-auth、Topic Alias、Assigned Client Identifier、Keep Alive、Receive Maximum、expiry、
  retained、Subscription Identifier、QoS 2 replay、Will/Will Delay 已在
  `flowie/tests/test_flowie_endpoint.c` 覆盖，并包含相应非法组合与状态故障点。
- Session-store commit 故障、重建恢复与二进制边界由 provider-neutral store harness 复用统一
  record/故障契约覆盖。
- TCP/TLS/WS/WSS/Pipe 的基本 CONNECT/PING/DISCONNECT 已在
  `flowie/tests/test_flowie_transport.c` 覆盖；该 target 还验证 TLS/WSS 半开握手 shutdown，client target
  验证真实 mTLS 无证书拒绝、有效证书身份、错误 CA、SAN 不匹配与不受信客户端证书拒绝；TLS listener
  同时验证证书缺失和证书/私钥不匹配在启动边界 fail fast；WS/WSS path、subprotocol、frame type、
  单帧/累计分片超限和非法 close 负例均由 transport target 覆盖。
- provider-neutral CONNECT commit 失败、无脏 session 重试、丢失 CONNACK 后重建恢复已在
  `flowie/tests/test_flowie_session_store_faults.c` 覆盖；QoS checkpoint、CAS、丢失 commit reply、损坏记录、
  expiry revision fencing 和二进制边界已经进入统一 trace。Redis 已有本轮证据，PostgreSQL 归
  `[pgsql]` 环境。
- client local/public smoke 已分别在 `flowie/client/tests/test_flowie_mqtt_client.c` 与
  `test_flowie_mqtt_client_live.c`；固定 broker 互操作不能替换现有 public smoke，也不能依赖它的可用性。

新增 CTest target 时，先沿用相邻 `cmake_add_test(...)`、TinyTest fixture 和 CoroNet helper。只有 failure
domain 不同或需要 sanitizer/live backend 时才拆 executable。测试文件、证书、corpus 与 runner 都归属
对应 `tests/` 或 `interop/`，不得把 mock/stub 或测试数据放入生产头文件。

## 参考规范与一手测试集

- MQTT 5.0 specification: <https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html>
- MQTT 3.1.1 specification: <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html>
- MQTT 3.1/3.1.1 differences: <https://github.com/mqtt/mqtt.org/wiki/Differences-between-3.1.0-and-3.1.1>
- Mosquitto broker test inventory, pinned inspection revision `7b8aba105b77253309de24664bbae69d0cae3da0`:
  <https://github.com/eclipse-mosquitto/mosquitto/blob/7b8aba105b77253309de24664bbae69d0cae3da0/test/broker/test.py>
- Mosquitto broker test notes at the same revision:
  <https://github.com/eclipse-mosquitto/mosquitto/blob/7b8aba105b77253309de24664bbae69d0cae3da0/test/broker/readme.txt>
