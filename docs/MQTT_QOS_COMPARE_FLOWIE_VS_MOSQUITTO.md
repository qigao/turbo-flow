# flowie 与 Mosquitto 的 MQTT QoS 1/2 实现对比

> 文档用途：维护者理解 flowie 的 QoS1/2 实现相对 Mosquitto 的异同，作为后续修改、审查与测试范围划分的参考。
> 结论均标注证据类型：`事实`（来自源码/文档行号）、`计算`（可复算）、`推论`（依据+不确定性）、`常用做法`（仅辅助）。

## 对比对象与版本

- **flowie**：本仓库 `flowie/` 下的 MQTT broker。核心文件：
  - `flowie/src/flowie_session.c`（入站/出站状态机、持久化记录）
  - `flowie/src/flowie_endpoint.c`（settlement 配置、MQTT 接入）
  - `flowie/src/flowie_cluster_session_bind.c`（集群 publish/delivery/ack 路径）
  - `turbo_flow/include/turbo_flow_protocol.h`（settlement 枚举）
- **Mosquitto**：本地克隆 `C:\tmp\mosquitto`，`master` 分支 commit `5cd2546511596a269dbf53f85858c623b09ebdd6`（2026-07-30）。`事实`：以下 Mosquitto 结论均来自该克隆源码，不代表某一固定发行版（如 2.0.x）的行为。

## 结论速览

协议层面两者等价：标准 PUBLISH→PUBACK（QoS1）与 PUBLISH→PUBREC→PUBREL→PUBCOMP（QoS2）状态机，QoS1 至少一次投递、QoS2 恰好一次投递，重连时重置状态并重发未确认消息。

实现层面 flowie 相对 Mosquitto 的差异集中在四点：

1. **入站 QoS1 也被跟踪**，且 ack 时机可配置（settlement 档位）；Mosquitto 入站 QoS1 不跟踪、收到即 ack。
2. **持久化模型不同**：Mosquitto 是可选整库快照 `mosquitto.db`（默认关闭）；flowie 按 session 落盘（含出站消息体）并接入分布式协议图。
3. **集群能力不同**：Mosquitto 无原生集群（bridge/共享订阅横向扩展）；flowie 原生多节点。
4. **窗口/配额模型不同**：Mosquitto 全局配置按客户端独立配额（默认 in-flight 20、queued 1000）；flowie 按 session 的 `max_inflight_per_session`，且出站是显式 reserve/commit 两段式。
5. **I/O 模型不同**（第 6 节，源码级对比）：Mosquitto 单线程事件循环（epoll/kqueue/poll）+ 每连接包级出站队列；flowie 每连接 CoroNet 协程 + 字节级 send_hwm_bytes 预算 + 批量 sendv，背压与线程模型不同。本节只做源码级对比，未做实测。
6. **理论性能对比**（第 7 节，未实测）：高并发/多核/大扇出/慢订阅者隔离/水平扩展场景下 flowie 架构上限理论上更高；单核/海量低流量连接/成熟稳定场景 Mosquitto 可能更合适。必须同负载对照基准验证，不能以单方基准下结论。
7. **后端对接与用户门槛**（第 8 节）：对接自研 ACL/Auth 后端时 Mosquitto 需写 C 插件或用 dynsec（JSON + mosquitto_ctrl），壁垒高；flowie 以 HTTPS auth/acl 服务声明式对接，门槛更低，但需部署外部服务 + flowie-control 的依赖链。

## 1. 入站方向（broker 接收 PUBLISH）

### Mosquitto（事实）

- QoS>0 先用 `db__message_store_find(context, source_mid, ...)` 按发布方 packet id 在 `msgs_in`（inflight + queued）查重（`src/handle_publish.c:95`、`src/database.c:1041-1061`）。
- `msgs_in` 中只存在 QoS2 条目（QoS1 从不插入），因此入站去重实际只对 QoS2 生效：
  - **QoS1**：`sub__messages_queue()` 转发后立即 `send__puback()`（`src/handle_publish.c` case 1）；MQTT5 无订阅者时回 `NO_MATCHING_SUBSCRIBERS` reason。重复 QoS1 PUBLISH 每次均按新消息处理（MQTT 规范允许 at-least-once）。
  - **QoS2**：首次 `db__message_insert_incoming(..., persist=true)` 入 `msgs_in`（状态 `mosq_ms_wait_for_pubrel`），`dup==0/1` 时回 `send__pubrec()`，`dup>1` 判协议错误断开（`src/handle_publish.c:175` 附近，`src/database.c:468`）。收到 PUBREL → `db__message_release_incoming()` 转发给订阅者 → `send__pubcomp()`（`lib/handle_pubrel.c`）；PUBREL 的 mid 未知也回 PUBCOMP（幂等，兼容重连后的重复 PUBREL）。
- 入站受 receive maximum（`msgs_in.inflight_quota`）约束：满时先入 `queued`，由 `db__message_write_queued_in()` 逐条补发 PUBREC（`src/database.c:1523`）。
- 复用 mid 且内容不匹配时记录告警并从 storage 清除旧记录（`src/handle_publish.c:100-112`）。

### flowie（事实，`flowie/src/flowie_session.c`）

- **QoS1 也建立 inflight 记录**（`FLOWIE_SESSION_INFLIGHT_SETTLEMENT`，第 23-24、1244 行），待集群 settlement 推进到配置点才回 PUBACK 并移除（第 1288-1295 行）。
- **QoS2**：`publish_begin` 建 SETTLEMENT 记录 → `publish_settle` 回 PUBREC 并把状态切为 `FLOWIE_SESSION_INFLIGHT_QOS2_RELEASE`（第 1297-1305 行）→ `qos2_release`（收到 PUBREL）回 PUBCOMP 并移除（第 1315-1330 行）。
- 去重：按 packet id 查 session inflight 向量；若已处于 `QOS2_RELEASE` 且收到 dup=1 的 PUBLISH，则重发 PUBREC（第 1228-1235 行）——与 Mosquitto 重连语义等价。

**差异**：入站 QoS1 是否有跟踪与延迟 ack 能力是两者最大分野（见第 3 节）。

## 2. 出站方向（broker 向订阅者投递）

### 状态机对照（事实）

| 阶段 | Mosquitto（`lib/mosquitto_internal.h:106-112`） | flowie（`flowie_session.c:34-38`） |
|---|---|---|
| 排队待发 | `mosq_ms_queued` | `FLOWIE_SESSION_DELIVERY_QUEUED` |
| QoS1 已发未 ack | `mosq_ms_wait_for_puback` | `FLOWIE_SESSION_DELIVERY_WAIT_ACK` |
| QoS2 已发 PUBLISH | `mosq_ms_wait_for_pubrec` | `FLOWIE_SESSION_DELIVERY_WAIT_PUBREC` |
| QoS2 已发 PUBREL | `mosq_ms_wait_for_pubcomp`（含 `resend_pubrel`） | `FLOWIE_SESSION_DELIVERY_WAIT_PUBCOMP` |
| 预占 packet id | — | `FLOWIE_SESSION_DELIVERY_RESERVED`（flowie 特有） |

- Mosquitto：PUBREC(reason<0x80) → 状态切 `wait_for_pubcomp` 并发 PUBREL（`lib/handle_pubrec.c:122-131`）；PUBACK/PUBCOMP → `db__message_delete_outgoing` 删除（`lib/handle_pubackcomp.c:145`），未知 mid 仅告警。
- flowie：`delivery_ack` 统一处理 PUBACK/PUBREC/PUBCOMP；PUBREC 会把 delivery 的 packet 替换为编码后的 PUBREL 并切 `WAIT_PUBCOMP`（`flowie_session.c:1009-1021, 1036-1043`）。

### 窗口与排队（事实）

- Mosquitto：全局配置、按客户端独立配额——`max_inflight_messages`（默认 20）、`max_inflight_bytes`（默认 0 不限）、`max_queued_messages`（默认 1000）、`queue_qos0_messages`（默认 false，离线 QoS0 不排队）（`src/conf.c:339-347`；`man/mosquitto.conf.5.xml` 对应章节）。配额判断见 `db__ready_for_flight` / `db__ready_for_queue`（`src/database.c:37-130`）。`max_inflight_messages=1` 保证投递有序。
- flowie：按 session 的 `max_inflight_per_session`（同时作为 MQTT5 receive maximum，`flowie_endpoint.c:5400`），出站先 RESERVED 占 packet id 再 commit 成 QUEUED/WAIT_ACK/WAIT_PUBREC（`flowie_session.c:752-779`）。

### 重传/恢复

- Mosquitto broker 重连时 `db__message_reconnect_reset_outgoing()` 把 in-flight 重置为 `publish_qos1 / publish_qos2 / resend_pubrel` 并重发（`src/database.c:1071-1122`）。`推论`：broker 侧无活跃定时重传——`retry_interval` 在 `src/conf.c:2575` 仅解析不再使用（全仓仅此一处），定时重传只存在于 lib 客户端库 `message__retry_check()`（`lib/messages_mosq.c:300-323`）；即 Mosquitto broker 依赖客户端重连触发重发。
- flowie 在会话恢复/重建时恢复 QOS2_RELEASE 与 deliveries（`flowie_session.c:1725` 附近）。`推论（未核查到显式定时器）`：flowie 的未 ack 恢复主要靠会话记录重建 + 重连重发，`flowie_endpoint.c` / `flowie_session.c` 中未检索到独立定时重传循环。

## 3. Ack 时机可配置性（核心区别）

- **Mosquitto（事实）**：入站 ack 时机固定——QoS1 收到即转发即 PUBACK，QoS2 收到 PUBREL 才算完成；无任何延迟确认或确认点配置。
- **flowie（事实）**：`settlement_qos1` / `settlement_qos2` 各 4 档 `received / accepted / processed / durable`（`flowie_endpoint.c:61,186-189`；`turbo_flow_protocol.h:136-142`），默认 `received`（`TURBO_FLOW_PROTOCOL_SETTLEMENT_POLICY_INIT`，`turbo_flow_protocol.h:209-210`），即默认 ≈ Mosquitto。`durable` 档要求显式 durable store 提交边界后才 ack（`flowie/include/flowie.h:420-421`）。
- `推论`：这是 flowie 的实质扩展——把"何时向发布方确认"从固定语义变为可靠性/吞吐权衡点；代价是 QoS1 也占用 inflight 记录与 session 配额，`durable` 档引入存储往返延迟。

## 4. 持久化模型

| 维度 | Mosquitto（事实） | flowie（事实） |
|---|---|---|
| 默认 | `persistence=false`，纯内存（`src/conf.c` defaults） | session_store / protocol_store 按配置启用 |
| 形态 | 整库快照 `mosquitto.db`（`src/persist.h`：DB_CHUNK_CFG/BASE_MSG/CLIENT_MSG/RETAIN/SUB/CLIENT），`autosave_interval` 默认 1800s，退出/SIGUSR1 时写盘（`man/mosquitto.conf.5.xml` autosave/persistence 章节）；另有插件持久化 API（`src/plugin_persist.c:223,295`） | 每 session 记录（session_store）+ 协议图/事件流（protocol_store、`flowie_cluster_*`） |
| 入站 QoS1 | 不持久化（无记录） | 不落盘：SETTLEMENT 记录在持久化循环中被跳过，只写 `QOS2_RELEASE`（`flowie_session.c:1472-1476`） |
| 入站 QoS2 | `wait_for_pubrel` 的 client msg 持久化（`db__message_insert_incoming` persist=true，`database.c:468,547-548`） | 只持久化 `QOS2_RELEASE`（等 PUBREL）记录（`flowie_session.c:1472-1476`） |
| 出站 | queued + inflight（含 state/direction）经 DB_CHUNK_CLIENT_MSG 持久化；重启后未 ack 重发 | deliveries 除 RESERVED 外全部落盘，**含完整 packet 字节**（record type 6/7，`flowie_session.c:1482-1514`） |

`推论`：
- Mosquitto 是单进程整库快照，简单但有写放大、无跨节点一致性，适合单机。
- flowie 将出站消息体连同状态落盘并把结算点接入分布式协议图，支持多节点；`durable` 档即"先持久化、再 ack"。
- 两者都满足 MQTT 关键约束：QoS2 入站第二阶段（等 PUBREL）必须跨重启保留，否则会丢消息或重复投递。

## 5. 其他结构性差异

- **集群（事实）**：Mosquitto 无原生集群，横向扩展靠 bridge / 共享订阅 / 外部前端；flowie 原生分布式（session owner、图广播、集群 publish/delivery/ack 路径，`flowie_cluster_session_bind.c`）。
- **协议版本差异（事实）**：Mosquitto 仅在 MQTT5 下回 reason code（如 `NO_MATCHING_SUBSCRIBERS`、`QUOTA_EXCEEDED`，`src/handle_publish.c` / `lib/handle_pubrec.c`）；flowie 同样区分 MQTT3.x/5 行为（`flowie_mqtt_version_is_3x`，`flowie_session.c`）。
- **重复投递（事实）**：Mosquitto `allow_duplicate_messages`（默认 true，已弃用）只处理同一客户端多个重叠订阅造成的重复投递（`man/mosquitto.conf.5.xml`），与 QoS 握手去重无关；flowie 未发现等价配置。

## 影响与验证范围（推论）

- 若在 flowie 上做 QoS 相关改动，回归测试应至少覆盖：QoS1/2 入站重复 PUBLISH（dup 位）、PUBREL 未知 mid 幂等、重连后未 ack 出站重发、QoS2 第二阶段跨会话恢复、`settlement` 各档位下 ack 时序。
- 若以 Mosquitto 行为为基准做互操作验证，默认 `received` 档与 Mosquitto 的 ack 时机一致；`accepted/processed/durable` 档会延迟 ack，需确认客户端侧超时/重传与延迟 ack 的兼容性。

## 证据来源

- flowie：`flowie/benchmarks/README.md`（单方容量/性能基准，非对照）、`flowie/src/flowie_session.c`、`flowie/src/flowie_endpoint.c`、`flowie/src/flowie_ingress.c`、`flowie/src/flowie_cluster_session_bind.c`、`turbo_flow/include/turbo_flow_protocol.h`、`flowie/include/flowie.h`；I/O 层 `flowie/io/common/include/flow_coronet_*.h`、`flow_io_policy.h`、`flow_connection.h`。
- Mosquitto：`C:\tmp\mosquitto`（master @ 5cd2546511596a269dbf53f85858c623b09ebdd6），文件 `src/handle_publish.c`、`src/database.c`、`src/conf.c`、`src/persist.h`、`src/plugin_persist.c`、`lib/handle_pubrec.c`、`lib/handle_pubrel.c`、`lib/handle_pubackcomp.c`、`lib/messages_mosq.c`、`lib/mosquitto_internal.h`、`man/mosquitto.conf.5.xml`；I/O 层 `src/loop.c`、`src/net.c`、`src/mux_epoll.c`、`src/mux_poll.c`、`lib/packet_mosq.c`、`lib/net_mosq.c`。

## 6. I/O 模型对比（源码级）

> 本节省略实测，仅基于两侧源码的 I/O 架构对比。Mosquitto 侧文件在 `C:\tmp\mosquitto`（master @ 5cd2546511596a269dbf53f85858c623b09ebdd6）；flowie 侧为本仓库源码。

### 6.1 事件驱动模型（事实）

- **Mosquitto：单线程事件循环。** 主循环 `mosquitto_main_loop`（`src/loop.c`）每轮依次执行 keepalive / session-expiry / will-delay 检查 → `mux__handle()` 事件分发 → 持久化 autosave → 信号检查。事件后端是 mux 抽象：epoll（`src/mux_epoll.c`，`epoll_wait(db.epollfd, ep_events, MAX_EVENTS, db.next_event_ms)`）、kqueue（`src/mux_kqueue.c`）、poll/WSAPoll（`src/mux_poll.c`，Windows/fallback）。socket 全部非阻塞，事件驱动，无每连接线程。
- **flowie：CoroNet 协程模型。** 每个 MQTT 连接一个协程 handler `flowie_endpoint_client_handler(coro_socket_t *client, ...)`（`flowie/src/flowie_endpoint.c:6811` 附近），运行在 CoroNet executor（`tf_coronet_execution_t`，`io/common/include/flow_coronet_execution.h`，含 loop thread）上。连接协程内是同步风格的 `coro_socket_recv` / `coro_socket_send`，底层由 CoroNet 事件循环调度；主读循环为 `coro_socket_recv` 读块 → `flowie_ingress_feed` 帧重组/解析 → dispatch 进 flow graph（`flowie_endpoint.c:6836-6856`）。

### 6.2 接收与帧解析（事实）

- **Mosquitto**：`packet__read_single`（`lib/packet_mosq.c:473`）逐包解析：先读固定头 + remaining length，再按 `remaining_length` malloc 一块 payload 读 body；一个事件循环周期只推进一个包，跨事件累积在 `in_packet.packet_buffer`/pos。
- **flowie**：CoroNet 每连接固定两个接收 chunk（`stream_recv_buffer_bytes`，默认 4096/个，`flowie_endpoint.c:202`），MQTT 帧跨 chunk 重组。`flowie_ingress_feed`（`flowie/src/flowie_ingress.c:235`）把输入追加进 framing 的 `turbo_byte_buffer` 并循环 `flowie_ingress_pump`，一次 feed 可解析出多个完整包；解析器为 `flowie_mqtt_packet_parse`（`flowie/protocol/`）。

### 6.3 发送与出站（事实）

- **Mosquitto**：`packet__queue` 把包追加到每连接 `out_packet` 链表；`packet__write`（`lib/packet_mosq.c:272`）逐包 `net__write`，遇 EAGAIN/EWOULDBLOCK 返回并等待下一次 EPOLLOUT（`mux__add_out` 注册写事件）继续。QoS1/2 状态机（`wait_for_puback/pubrec/pubcomp`）决定哪些 PUBLISH 可排队/在途；`max_queued_messages`/`max_inflight_messages` 限制队列规模。
- **flowie**：每连接 `send_queue`（reply queue）+ `tf_io_budget`（`send_budget`，max_bytes=`send_hwm_bytes`，admission=FAIL，见 `io/common/include/flow_io_policy.h`）。`flowie_connection_reply_drain`（`flowie_endpoint.c:4077`）批量取队首请求，TCP 下用 `coro_socket_sendv` 一次写多个包（同连接 FIFO 批量发送，`flowie_endpoint.c:4140-4141`）；超 HWM 即 admission 失败 → `slow_subscriber_policy=disconnect` 隔离慢订阅者。出站 QoS 投递计入 `outbound_qos_inflight`，受 `client_receive_maximum` 限制（`flowie_endpoint.c:4105-4110`）。

### 6.4 线程模型（事实 + 推论）

- **Mosquitto**：broker 主循环单线程（`WITH_THREADING` 主要影响 lib 客户端回调线程；broker 事件循环本身单线程，持久化 autosave 也在主循环内执行）。`推论`：单核串行处理所有连接事件，CPU 密集路径受单核限制。
- **flowie**：CoroNet 协程可跨线程调度（executor 绑定 loop thread，`flow_coronet_execution.h`）；`flowie.yml` 可配 `runtime.ingress.workers`（示例默认 1）；每连接 `flowie_task_try_begin` 做并发任务限流；集群模式用 `coro_wait` 等待集群结算（`flowie_connection_cluster_wait`，`flowie_endpoint.c:517`）。

### 6.5 背压与慢订阅者（事实）

- **Mosquitto**：无每连接字节 HWM；慢订阅者导致 `out_packet` 队列堆积，受 `max_queued_messages`（默认 1000）限制，超限丢弃或断开由上层逻辑决定；TCP 背压通过 EWOULDBLOCK 挂起写。
- **flowie**：显式字节预算 `send_hwm_bytes`，超预算 admission FAIL → 慢订阅者策略 `disconnect`（`flowie_endpoint.c:2816-2821`）；仓库 benchmark 有专项 `real TCP stalled-subscriber isolation`（`flowie/benchmarks/README.md`）验证慢客户端只被隔离、健康连接不受影响。

### 6.6 与 QoS1/2 状态机的关系（事实 + 推论）

- 两侧共同点：QoS 状态机与 I/O 解耦——PUBLISH/PUBACK/PUBREC/PUBREL/PUBCOMP 都是普通出站包排队发送，QoS 语义由消息层（inflight/queued 状态）决定何时允许发送、收到 ack 后何时删除。
- 差异：Mosquitto 的“允许发送”由 `max_inflight_messages` 配额在投递入队时判定（`db__message_insert_outgoing`），写队列本身是简单链表；flowie 的出站投递带字节预算与批量 `sendv`，且 settlement 机制把“何时 ack 发布方”接入图/持久化边界（`received/accepted/processed/durable`），直接影响入站方向的 I/O 时序（ack 包何时被排入 send_queue）。`推论`：flowie 的背压粒度（字节级 HWM + 批量写）比 Mosquitto（包级配额 + 逐包写）更细，代价是每个连接需要协程栈与双接收 chunk 内存。

### 6.7 容量边界（事实）

- **Mosquitto**：每连接 in/out packet 动态内存；出站队列由 `max_queued_messages`（默认 1000）/`max_inflight_messages`（默认 20）约束。
- **flowie**：显式配置 `max_connections` / `max_inflight_per_session` / `send_hwm_bytes` / `max_packet_size` / `coroutine_stack_size`（最小 64 KiB）/ `stream_recv_buffer_bytes`；`flowie/benchmarks/README.md` 将 100k 实时 TCP 连接作为容量门禁。

## 7. 理论性能对比（flowie vs Mosquitto，未实测）

> 本节为基于第 1-6 节源码分析的**理论推断**，会话中未运行任何 flowie 与 Mosquitto 同负载对照基准。`理论优势 ≠ 实测优势`，落地前必须以同机器、同客户端、同负载的对照测试验证。

### 7.1 flowie 理论上可能占优的维度（推论，依据源码）

| 维度 | 依据 | 说明 |
|---|---|---|
| 多核扩展上限 | Mosquitto broker 单线程主循环（`src/loop.c` `mosquitto_main_loop`，核心路径无线程创建）；flowie 协程可多线程调度（`io/common/include/flow_coronet_execution.h`） | 订阅匹配、大扇出、QoS 状态迁移等 CPU 密集路径，Mosquitto 受单核瓶颈；flowie 理论上限更高。注意 flowie 示例默认 `runtime.ingress.workers: 1`，需配置才吃多核 |
| 同连接出站批量写 | Mosquitto `packet__write` 逐包 `net__write`（`lib/packet_mosq.c:272`）；flowie `flowie_connection_reply_drain` 用 `coro_socket_sendv` 批量写（`flowie_endpoint.c:4140-4141`） | 同连接高消息率流水线场景，flowie 系统调用更少、小包聚合更好 |
| 背压粒度与慢订阅者隔离 | Mosquitto 无每连接字节 HWM，靠 `max_queued_messages`（默认 1000）限队列；flowie 有 `send_hwm_bytes` 预算 + `slow_subscriber_policy=disconnect`（`flowie_endpoint.c:2816-2821`），并有 `stalled-subscriber isolation` 专项基准 | 大扇出 + 个别慢客户端场景，flowie 理论隔离性更好 |
| 水平扩展 | Mosquitto 无原生集群（bridge/共享订阅不是集群语义）；flowie 原生多节点协议图 | 单机天花板之后，flowie 有架构扩展路径，Mosquitto 没有 |

### 7.2 Mosquitto 理论上可能占优或相当的维度（推论）

| 维度 | 说明 |
|---|---|
| 每连接常驻内存 | flowie 每连接固定协程栈（最小 64 KiB）+ 双接收 chunk（4 KiB×2）+ delivery 缓冲；Mosquitto 每连接 `struct mosquitto` + 动态 packet 缓冲（payload 用后释放）。海量连接、低流量场景下 Mosquitto 常驻内存可能更低（未实测） |
| 单核/单连接低延迟 | 单线程事件循环无协程切换与跨线程调度开销、缓存局部性好（常用做法）；flowie 的 settlement（尤其 `accepted/durable`）在 ack 前插入图/持久化边界，增加发布方往返。低并发小规模场景两者差距不大甚至 Mosquitto 更省 |
| 成熟度与生态 | Mosquitto 久经考验、部署与调优资料丰富；flowie 为仓库内自研（工程风险维度，非性能维度） |

### 7.3 必须强调的两点（事实）

1. **没有同负载对照数据**：`flowie/benchmarks/README.md` 中的 `149,606 msg/s`（64 包 pipeline burst）、`24,280 deliveries/s`（16 扇出）、100k 连接峰值约 5.93 GiB private commit 等都是 flowie 单方、特定负载的测量，**不能**直接与 Mosquitto 比较；Mosquitto 侧本轮未运行任何基准。
2. **QoS1/2 的额外成本是 trade-off，不是免费优势**：默认 `settlement=received` 时 flowie 的 ack 时机 ≈ Mosquitto；配置 `accepted/processed/durable` 后发布方 ack 延迟增大，单发布者吞吐下降，换取"确认前已完成图处理/持久化"的语义保证。

### 7.4 一句话结论（推论）

高并发、多核、大扇出、慢订阅者隔离、需水平扩展的负载下，flowie 架构上限（协程多线程 + 批量写 + 字节背压 + 原生集群）理论上高于单线程 Mosquitto；单核、海量低流量连接、追求成熟稳定的场景下 Mosquitto 可能更合适。要得出"flowie 确实 outperform"的结论，必须跑同负载、同机器、同客户端的对照基准（即第 6 节开头所述未执行的实测路径）。

## 8. 后端服务对接与用户门槛（ACL / Auth）

> 用户观点：Mosquitto 对接 ACL 等后端服务非常麻烦、用户壁垒高。本节用两侧源码/文档证据核对，结论为：该观点在"对接自研后端服务"场景下成立，但 flowie 也有其外部依赖成本。

### Mosquitto（事实）

- 内置安全是**文件/静态**形态：`password_file`（用 `mosquitto_passwd` 工具维护）、`acl_file`（文本 topic/pattern 语法）、`psk_file`、`allow_anonymous`，以及 `per_listener_settings`（每个 listener 一套配置，配置面成倍放大）（`man/mosquitto.conf.5.xml`：password_file ≈934、acl_file ≈175、allow_anonymous ≈289、per_listener_settings 相关 ≈264-412）。
- 文件方式被官方视为 legacy：man 建议改用 `mosquitto_password_file` / `mosquitto_acl_file` **插件**替代 `password_file` / `acl_file`（`man/mosquitto.conf.5.xml` ≈938、≈179）。
- 对接**自定义后端**（数据库/IAM/LDAP）必须编写 **C 插件**（v5 插件 API，编译为 .so/.dll），再用 `auth_plugin` + `auth_opt_*` 加载（`man/mosquitto.conf.5.xml` ≈1170）。
- 仓库自带动态安全插件 `plugins/dynamic-security`：ACL 存 JSON 状态文件，用 `mosquitto_ctrl` 通过 MQTT `$CONTROL/dynsec/v1` 命令管理 client/group/role/ACL（`plugins/dynamic-security/README.md`）。它解决了"纯静态文件"，但仍是 broker 内 JSON + 专用 CLI，不是直接对接外部后端服务。
- 文件 ACL 支持 SIGHUP 重载（`man/mosquitto.conf.5.xml` ≈278-280）；插件体系则需要编译与运维。

→ 核对结论（推论）：要把 ACL/Auth 接到自己后端服务的团队，在 Mosquitto 上要么写 C 插件，要么接受 dynsec（JSON + mosquitto_ctrl），要么退回文件式 legacy 语法——用户壁垒确实高。

### flowie（事实）

- 声明式配置对接外部服务：`auth_provider`（HTTPS `/v4/authenticate`）+ `acl_provider`（HTTPS `/v4/acl/check`）+ `security_realm` + `auth_method`（`flowie/examples/flowie.yml` ≈74-106）；endpoint 选项 `security_realm` / `auth_method` / `trusted_proxy_cidrs`（`flowie_endpoint.c:234-236`）。
- 认证/授权走外部 HTTPS 服务（auth-service / acl-service），由 `flowie-control` 提供 realm/ACL 管理：即"接后端"= HTTP 服务对接，**无需编译 C 插件**。
- 安全配置为 fail-fast 精确绑定：缺失 `realm_channel`/`auth_method`/`auth_provider` 直接拒绝启动（`flowie_endpoint.c:2249-2260`），不留匿名降级路径。

### 门槛对比（推论）

| 维度 | Mosquitto | flowie |
|---|---|---|
| 静态/小规模 | 文件 + `mosquitto_passwd`，简单但属 legacy | YAML + 外部 HTTP 服务，链路更长 |
| 接自定义后端 | 写 C 插件（v5 API）或 dynsec（JSON+ctrl） | 对接 HTTPS auth/acl 服务 |
| 热更新 | 文件 SIGHUP reload；dynsec 经 `mosquitto_ctrl` 命令 | 由外部服务 / `flowie-control` 管理（热更新语义未核实） |
| 运维面 | `per_listener_settings` 多套配置、插件编译 | 需部署 auth/acl 服务 + `flowie-control` |

→ 结论（推论）：在"对接自研后端服务"场景，flowie 的接入门槛（HTTP 服务对接）显著低于 Mosquitto（C 插件/dynsec）；但 flowie 的代价是把 broker 内"文件即配置"的简单形态，换成了"必须部署外部认证/ACL 服务 + flowie-control"的依赖链。两者都不是零成本。
