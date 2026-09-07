# ADR: CNet UDP/KCP Packet Source Owner

## 状态

Accepted，适用于 issue #23。

## 背景

`cnet_packet_endpoint` 统一管理 UDP、plain KCP、authenticated KCP 与 Reed-Solomon FEC，并以
generation-checked handle 和固定 session table 维护 peer/session 身份。它没有内部 I/O thread；
socket completion、KCP ACK/retransmit、secure handshake 和 timer 都由调用者持续执行
`cnet_packet_poll()` 推进。其 receive view 只在 callback 内有效。

这与 TCP source 的 demand-gated receive admission 不同：若把 packet endpoint 的底层 receive 或 poll
绑定到 Graph demand，KCP 在无 demand 时会停止 ACK、握手与重传，协议状态就不再正确。

## 决策

在可选 `TurboFlow::CNetAdapter` 中增加 opaque `turbo_flow_cnet_packet_source_t`。一个 owner 独占：

- 一个 `cnet_packet_endpoint`；
- 一个预留到 `queue_capacity` 的 CSTL queue；
- 一个 manual CFlow Scheduler、一个 managed Publisher 与一个 `turbo_flow_run_t`；
- 消息计数、首个 terminal status/stage 和 portable snapshot。

所有公开调用、CNet poll、Scheduler 与 Graph run 由同一 serialized owner thread 推进。跨线程调用方必须
在外层 mailbox 排队；adapter 不增加隐式锁或 worker。

CNet 是 session 主事实源。Adapter 的 session open/get-info/close/send API 只转发到 owned endpoint，
不建立第二份可写 session table。Inbound admission 仅在 CNet 完成 wire-level 校验后返回成功；尤其 secure
KCP 在 PSK authentication 失败时不会进入 adapter `on_admit`、不会创建 session，也不会发布消息。

每个合法 callback view 在返回前复制到一个 `mem_buffer_t`。Buffer 起始处保存 size/versioned
`turbo_flow_cnet_packet_message_context_t`，随后保存 payload；`transport_context` 指向 buffer 内的 context，
所以 clone/move 和异步 Graph 边界都保留 generation handle、peer、protocol 与 conversation，且 slot
复用后旧消息身份不变。UDP 只接受 `CNET_MESSAGE_DATAGRAM`，KCP 只接受 `CNET_MESSAGE_BYTES`。

Queue 在 open 阶段按显式 capacity 完成 reserve，运行期不得增长。Queue full、oversize、无效 view、
context/session lookup 失败、分配失败、counter/message-id 溢出立即进入 terminal FAILED；触发消息不入队，
既有队列也不会在 terminal error 之后继续发布。无截断、silent drop 或 fallback。

Graph demand 只消费 queue。`poll()` 顺序是 Scheduler → `cnet_packet_poll()` → Scheduler，因此 endpoint
无论当前 demand 是否为零都会推进 socket、ACK、握手、FEC 和 timer；新消息唤醒等待中的 Publisher。

Stop 顺序为：关闭 Graph run，清理未发布 queue，停止并 drain/destroy endpoint，最后 shutdown/destroy
Scheduler。Timeout 保留 STOPPING owner 供调用者重试；destroy 只接受 STOPPED。Endpoint error status
原样保存在 owner，malformed plain-KCP 等协议错误不得转换成通用 I/O 错误。

## 影响与权衡

- 架构：新增 packet transport adapter；`TurboFlow::Graph` 仍不依赖 CNet。
- API：纯新增 size/versioned C ABI 和 C++ 可包含头文件；stream/listener ABI 不变，无 TurboNet、
  TurboHttp 或 TurboParser alias/fallback。
- 状态：CNet session table 是唯一事实源；snapshot 与 message context 是只读副本。
- 内存：常驻 queue storage 是 O(`queue_capacity`)；retained payload 上界是
  O(`queue_capacity * max_message_bytes`)。每条 callback 消息做一次 borrowed-to-owned copy。
- 性能：poll 前后各一次 bounded Scheduler pass；协议进度不会被 Graph 背压阻断。若 profiling 证明
  callback allocation 是总耗时 ≥20% 的瓶颈，再单独评估专属 buffer pool，不改变本次所有权协议。
- 迁移/回滚：消费者仅在需要 packet ingress 时链接既有 CNetAdapter 并调用新 API；回滚不影响 core
  Graph、已有 stream/listener 配置或网络协议格式。

## 验证

TinyTest 覆盖 C/C++ ABI、invalid bounds、真实 UDP/plain-KCP round trip、无 demand 协议推进、owned
session context、stale generation、queue full、oversize、matching/mismatched PSK、FEC transport bounds、
malformed KCP status、stop/destroy/repeated lifecycle。安装消费者同时引用所有新增导出，最后运行 Debug
ASan 与 Release 全量 CTest。
