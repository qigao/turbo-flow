# FlowMQ

FlowMQ 是基于 CoroNet、FMQ v2 和 TurboFlow graph/Disruptor 构建的消息传输产品。它借鉴
ZeroMQ 的通信模式，但不兼容 ZeroMQ wire/API，也不提供独立 Client SDK。远端参与者通过 graph
中的 FMQ endpoint 接入；graph 定义接收、处理、路由和发送流程。

当前稳定边界：

- 本地 C API 为 v1，FMQ wire 只支持 v2；
- 人工配置入口为 YAML v1，resolver 输出 immutable resolved snapshot；
- `REQ/REP` 严格同步，delayed reply 只属于 `ROUTER/DEALER`；
- 持久化和重放由 Queue、Redis Stream、SQLite 等 graph resource 提供；
- 当前不提供认证、授权、租户或密钥管理，只能部署在宿主建立的可信边界内。

当前架构和所有权见 [ARCHITECTURE.md](ARCHITECTURE.md)。发布前必须执行
[RELEASE_GATE.md](RELEASE_GATE.md)。

希望像 ZeroMQ socket 一样直接创建单 endpoint 时，使用
[ZMQ-like Application API](ZMQ_STYLE_API.md)。该指南包含可编译的 PUB/SUB、同步 REQ/REP、
ROUTER/DEALER delayed-reply 示例以及配套 YAML；它是 pattern-level facade，不代表 ZeroMQ
API 或 wire compatibility。

## Packages and targets

| Target | Visibility | Purpose |
| --- | --- | --- |
| `FlowMQ::Protocol` | installed | FMQ v2 encode/decode、fragmentation 和 heartbeat deadline |
| `FlowMQ::Runtime` | build tree | graph-native FlowMQ runtime |
| `FlowMQ::Broker` | build tree compatibility alias | 过渡名称，不应成为新代码依赖 |
| `TurboFlow::FMQ` | installed compatibility target | 当前公开 runtime ABI |

`flowmq_runtime_core`、`flowmq_coronet_transport` 和
`flowmq_connect_endpoint_runtime` 是私有实现，不安装、不导出。

只使用 wire codec 时链接 `FlowMQ::Protocol`：

```c
#include "flowmq_protocol.h"

flowmq_protocol_frame_t frame = {0};
tstr_t encoded = NULL;

frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
frame.pattern = FLOWMQ_PROTOCOL_PUB;
frame.message_id = 1;
frame.topic = tstr_v_from_cstr("orders.created");
frame.payload = tstr_v_from_cstr("ready");

int rc = flowmq_protocol_encode_frame(&frame, 1024, &encoded);
if (rc == TURBO_OK) {
  tstr_free(encoded);
}
```

成功 decode 后必须调用 `flowmq_protocol_frame_cleanup()`。单 packet 的 identity、topic 和 payload
借用输入；fragmented payload 由 decoded frame 持有。

## Endpoint patterns

FlowMQ operation 是 pattern-specific typed operation，不是通用 `send/receive`：

| Pattern | Pairing | Operations | Contract |
| --- | --- | --- | --- |
| PUB/SUB | PUB bind, SUB connect | `fmq.pub.send`, `fmq.sub.receive` | prefix fan-out；无匹配返回 `TURBO_ENOTCONN` |
| PUSH/PULL | PUSH bind, PULL connect | `fmq.push.send`, `fmq.pull.receive` | 对 eligible peer round-robin，一次 attempt 不改投 |
| REQ/REP | REQ connect, REP bind | `fmq.req.request/reply`, `fmq.rep.request/reply` | 单 outstanding request；REP 当前 dispatch 内回复 |
| ROUTER/DEALER | ROUTER bind, DEALER connect | `fmq.router.receive/send`, `fmq.dealer.receive/send` | identity route；支持 generation-fenced delayed reply |
| PAIR | bind/connect | `fmq.pair.receive/send` | bind 端最多一个 live peer |
| XPUB/XSUB | XPUB bind, XSUB connect | `fmq.xpub.receive/send`, `fmq.xsub.receive/send` | 显式 subscription event 和 reconnect replay |

SUB/XSUB 只有在 HELLO 后提交订阅才会收到匹配 publication。空字符串订阅全部 topic；null XSUB
topic 不发送初始订阅，允许 graph 转发 XPUB subscription control。XPUB 的订阅状态按 peer session
维护，断线时产生对应 UNSUBSCRIBE。

PUB 对当前全部匹配 peer fan-out。若部分 peer 已发送后另一 peer 失败，返回第一个错误；先前发送是
不可回滚外部副作用。启用 per-peer HWM 时，慢 subscriber 的 fail、drop-oldest 或 disconnect 只作用
于该 subscriber，其他匹配者继续接收。

REQ 状态为 `READY -> WAIT_REPLY -> READY`。REP 状态属于收到 request 的 peer session，回复必须复用
当前 correlation，并在该次 graph dispatch 返回前完成。需要异步回复时使用 ROUTER：

```c
int rc = turbo_flow_fmq_message_detach_router_route(&message);
```

detach 后 route 以 `owner_instance_id + session_id + session_generation` 存入 owned message，可跨
worker、thread、coro 和 memory queue。peer 断线、同 identity 重连或 ROUTER restart 后，旧 route
返回 `TURBO_ENOTCONN`；route 不允许持久化重放。

## Graph and YAML

Host 使用 `turbo_flow_fmq_register_adapter()` 或
`turbo_flow_fmq_register_resolved_adapter()` 注册 endpoint。C config 必须从
`TURBO_FLOW_FMQ_CONFIG_INIT` 开始；错误 size/version、未知字段和不适用于 transport 的 option 均
fail fast。

只需要一个 endpoint 而不需要自行组 graph 时，可使用薄的 Application facade。它仍然创建
TurboFlow graph 和同一个 FMQ adapter；不会创建第二套 socket、队列、线程或 pattern 状态：

```c
static int on_message(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  (void)app;
  (void)message;
  (void)ctx;
  /* message 及其 view 只借用到本次回调返回。 */
  return TURBO_OK;
}

turbo_flow_fmq_config_t endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
turbo_flow_fmq_app_options_t options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
turbo_flow_fmq_app_t *app = NULL;

endpoint.pattern = TURBO_FLOW_FMQ_SUB;
endpoint.mode = TURBO_FLOW_FMQ_CONNECT;
endpoint.transport = TURBO_FLOW_FMQ_TCP;
endpoint.host = "127.0.0.1";
endpoint.port = 7701;
endpoint.topic = "orders.";
options.on_message = on_message;

int rc = turbo_flow_fmq_app_create(&endpoint, &options, &app);
if (rc == TURBO_OK) rc = turbo_flow_fmq_app_start(app);
/* ... */
turbo_flow_fmq_app_destroy(app);
```

`turbo_flow_fmq_app_send()` 复制 payload 并发布到 facade 的 graph input；
`turbo_flow_fmq_app_send_batch()` 为 `PUB`、`PUSH`、`DEALER` 一次提交多个 copied payload，
并在全部已提交 frame 到达与单条 send 相同的交付边界后返回。TCP connect endpoint 会把编码后的
多个 frame 合并为一次 stream write；其他合法 endpoint 布局保留同一批次提交/完成语义，但不保证
一次 write。若某个 item 在准备阶段失败，之前的 item 仍会发送，`submitted` 返回实际提交数；调用方
因此必须同时检查返回码与 `submitted`。`REQ/REP` 不支持此 API，以保留严格 session/correlation
顺序。单批最多 `TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS` 项，payload 总量最多
`TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES`；超过上限分别返回 `TURBO_ERANGE` 与
`TURBO_EMSGSIZE`。
`turbo_flow_fmq_app_send_message()` 保留已有 message metadata，并作为 ROUTER detached route 的
delayed-reply 入口。REP callback 可用 `turbo_flow_fmq_app_message_set_payload_copy()` 替换 payload，
返回 `TURBO_OK` 后由同一次 graph dispatch 同步回复。生命周期和 send API 不可从同一个
`on_message` callback 重入。YAML 用户先 resolve snapshot，再调用
`turbo_flow_fmq_app_create_resolved()`；facade 不解释 YAML 文件本身。

```flow
source events adapter fmq.events.sub operation fmq.sub.receive
stage validate
stage archive
stage publish adapter fmq.events.pub operation fmq.pub.send
stage main {
  events -> validate -> archive -> publish
}
```

YAML 配置 transport、endpoint、pattern、topic、identity、timeout、heartbeat、reconnect 和 HWM；graph
只引用 adapter 与 operation，不解释 provider 私有字段。完整产品配置见
[examples/fmq.yml](examples/fmq.yml)，Application API endpoint 配置见
[examples/zmq_style.yml](examples/zmq_style.yml)。

TFCW/1 credit worker 的易失性 graph 使用 pattern.fmq.credit module、FmqCreditWorker resource 和
fmq.credit.control/dispatch/complete/worker_input operations。service 固定在
channels.<name>.config，JOB 由上游 processor 预编码。durable owner C API 仍支持 Redis/SQLite，
但 durable graph registration 在通用 claim projection 完成前返回 TURBO_ENOTSUP，不会降级到内存。

```yaml
version: 1

fragments:
  connection:
    events:
      transport: tcp
      host: 127.0.0.1
      port: 7701

adapters:
  fmq.events.pub:
    kind: fmq
    fragments:
      connection: events
    config:
      pattern: pub
      mode: bind
      topic: events
      frame_hwm_messages: 1024
      frame_admission_policy: fail
```

支持的 CoroNet transport 为 `tcp`、`tls`、`udp`、`kcp`、`pipe`、`ws` 和 `wss`。pattern 与
transport 正交，但 option 由具体 transport 校验，例如 KCP FEC、TCP keepalive、UDP multicast
不会被其他 transport 静默接受。

## Message and ACK ownership

完整 ingress message 的 payload、topic、identity、correlation 和 FMQ metadata 共用一个 retained
`mem_buffer_t`，因此可进入 TurboFlow Disruptor 和异步 executor。raw socket bytes 与 framing view
始终留在 CoroNet owner lane。REP 是唯一刻意保留 borrowed peer context 的基础模式。

ACK 必须按边界解释：

| Signal | Meaning |
| --- | --- |
| graph publish success | 当前 graph attempt 成功 |
| FMQ frame admission | 本地有界发送队列接管 encoded frame |
| transport send success | CoroNet 完成一次写入 |
| storage accept ACK | memory queue 接管，或 Redis/SQLite transaction 已提交 |
| delivery/completion ACK | consumer/worker 完成且 storage settlement 成功 |

这些 ACK 不能互相模拟。FMQ HWM 只限制本地内存，不代表远端接收、处理或持久化。durable replay
必须显式组合 storage/queue resource，FMQ 不维护隐藏临时队列。

## Wire v2 summary

每个 connection 先双向交换 HELLO，只有 pattern pairing 兼容后才接受 DATA。header 固定 32 bytes，
整数为 network byte order：

```text
offset  size  field
0       4     magic "TFMQ"
4       1     version (2)
5       1     kind (HELLO, DATA, PING, PONG, SUBSCRIBE, UNSUBSCRIBE)
6       1     sender pattern
7       1     packet flags (FIRST=0x01, LAST=0x02)
8       2     identity length
10      2     topic length
12      4     packet payload length
16      8     message ID
24      4     complete payload length
28      4     packet payload offset
32      ...   identity, topic, packet payload
```

DATA payload 每 packet 最大 64 KiB；较大 payload 按连续 offset 分片。identity/topic 只出现在 FIRST
packet。`max_frame_size` 限制完整 identity + topic + payload，identity 最大 255 bytes，topic 最大
1024 bytes。protocol v1、乱序/重叠分片、trailing bytes 和不一致 header 均返回协议错误。

## Product boundary

| Capability | Status and limit |
| --- | --- |
| PUB/SUB、PUSH/PULL、PAIR、REQ/REP、ROUTER/DEALER、XPUB/XSUB | supported，遵循上表 pattern contract |
| Load balancer、reliable request、credit worker | supported，由 graph + pattern owner 组合，不增加 wire pattern |
| Redis Stream durable replay | supported，Stream/PEL 是事实源 |
| Redis Data SET/GET | supported，binary-safe data contract，与 Stream ACK 分离 |
| SQLite Queue durable replay | supported，queue-private schema 与 transaction settlement |
| PgSQL durable outbox | not claimed；普通 PostgreSQL query/sink 不等于事务 outbox source/sink |
| TFMP management | supported，strict REQ/REP、typed command、operation/event store |
| Failure-domain deployment owner | supported，authority epoch 必须由宿主强一致服务分配 |
| Authentication/authorization/tenant | not supported |
| ZeroMQ/ZMTP compatibility | not supported |

高级应用协议分别由以下文档约束：

- [MANAGEMENT_PROTOCOL.md](MANAGEMENT_PROTOCOL.md)：TFMP management envelope、operation 和 event；
- [DEPLOYMENT_CONTROL.md](DEPLOYMENT_CONTROL.md)：failure-domain fencing、rolling upgrade 与 reconcile；
- [BULK_CREDIT_PROTOCOL.md](BULK_CREDIT_PROTOCOL.md)：TFCW credit worker、双维 credit 和 durable claim；
- [RELEASE_GATE.md](RELEASE_GATE.md)：真实 Redis、chaos、persistence 和性能趋势门槛。
