# FlowMQ

开发接入、pattern 选择、Application facade、Graph、wire v3 安全和背压说明见
[开发指南](DEVELOPER_GUIDE.md)。

FlowMQ 是基于 CoroNet 和 FMQ v3 的消息传输产品，并通过 TurboFlow graph/Disruptor 提供
可组合的高级处理能力。它借鉴 ZeroMQ 的通信模式，但不兼容 ZeroMQ wire/API，也不提供独立
Client SDK。基础 PUB/SUB、PUSH/PULL、REQ/REP、ROUTER/DEALER 等 pattern 以纯数据流转和
pattern owner 为主；graph 在这些路径上通常只是 typed operation bridge。需要规则、队列、
持久化、enrichment 或跨 pattern 组合时，才把 `turbo_flow_msg_t` 送入完整 graph。

因此，简单 FlowMQ 应用不需要配置文件；直接创建 socket/pattern 即可。YAML 和 TurboFlow
属于 proxy 等复杂产品组合，不是基础 API 的前置条件。

当前稳定边界：

- 本地 C API 为 v1，FMQ wire 只支持 v3；decoder 拒绝其他版本，不协商、不降级；
- 人工配置入口为 YAML v1，resolver 输出 immutable resolved snapshot；
- `REQ/REP` 严格同步，delayed reply 只属于 `ROUTER/DEALER`；
- 持久化和重放由 Redis Stream 或其他显式 durable store 提供；
- v3 security binding 是可选能力：未配置时仍要求宿主可信边界；配置后在七种 CoroNet transport 上
  强制认证、身份绑定与 default-deny ACL，不允许匿名回退。TLS/WSS 额外强制 TLS 1.3 exporter 绑定。

当前架构和所有权见 [ARCHITECTURE.md](ARCHITECTURE.md)。发布前必须执行
[RELEASE_GATE.md](RELEASE_GATE.md)。

希望像 ZeroMQ socket 一样直接创建单 endpoint 时，使用
[ZMQ-like Application API](ZMQ_STYLE_API.md)。该指南包含可编译的 PUB/SUB、同步 REQ/REP、
ROUTER/DEALER delayed-reply 示例以及配套 YAML；它是 pattern-level facade，不代表 ZeroMQ
API 或 wire compatibility。

## Packages and targets

| Target | Visibility | Purpose |
| --- | --- | --- |
| `FlowMQ::Protocol` | installed | FMQ v3 encode/decode、security envelope、fragmentation 和 heartbeat deadline |
| `TurboFlow::FMQ` | installed | FlowMQ Core、management、deployment 与可选 graph adapter |

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

TCP scatter/gather 集成可使用 `flowmq_protocol_encode_frame_segmented()`：

```c
flowmq_protocol_segmented_frame_t wire = FLOWMQ_PROTOCOL_SEGMENTED_FRAME_INIT;

int rc = flowmq_protocol_encode_frame_segmented(&frame, 1024, &wire);
if (rc == TURBO_OK) {
  /* wire.segments[0..segment_count) 可转换为 transport iovec 并同步发送。 */
  flowmq_protocol_segmented_frame_cleanup(&wire);
}
```

segment array、packet header、identity 和 topic 由 `wire` 持有；payload segment 借用
`frame.payload`，因此 payload backing 必须保持到 send 完成。cleanup 只释放 framing，不释放 payload。
API 不依赖 CoroNet iovec 类型，其他 transport 可以按自身 scatter/gather 类型转换。

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

## Message and Graph

The provider-to-graph boundary is also recorded in
[`../turbo_flow/ADR_MESSAGE_GRAPH_BOUNDARY.md`](../turbo_flow/ADR_MESSAGE_GRAPH_BOUNDARY.md).

`flowmq_protocol_frame_t`/`flow_fmq_frame_t` 是协议 decoder 的临时 frame view，不能跨 owner
lane、graph executor 或 queue 生命周期保存。FlowMQ provider 会将 frame 的 payload、topic、
identity、correlation 和 route metadata 转换为拥有 `mem_buffer_t` 的 `turbo_flow_msg_t`。

基础 pattern 仍由 peer session 和 pattern owner 维护匹配、correlation、HWM 和 transport side
effect；graph 不替代这些协议状态。高级组合则使用统一消息：

```text
frame -> turbo_flow_msg_t -> graph stages -> FMQ/HTTP/socket/Redis/FlowStore sink
```

graph stage 失败与 transport send、storage accept、delivery completion 是不同的 ACK 边界。

## Graph and YAML

Host 使用 `turbo_flow_fmq_register_adapter_ex()` 或
`turbo_flow_fmq_register_resolved_adapter()` 注册 endpoint。C config 必须从
`TURBO_FLOW_FMQ_CONFIG_INIT` 开始并提供显式 execution binding；错误 size、未知字段和不适用于
transport 的 option 均
fail fast。

只需要一个 endpoint 而不需要自行组高级 graph 时，可使用 Application facade。它直接创建
graph-neutral FMQ endpoint Core 并连接 typed callback，不解析或启动 Graph；只有可选 Graph
adapter 才负责拓扑组合：

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

`turbo_flow_fmq_app_send()` 复制 payload 并发布到 facade 的 graph input；这是统一消息边界，
不是对基础 pattern 已存在的 wire frame 的零拷贝承诺。
`turbo_flow_fmq_app_send_batch()` 为 `PUB`、`PUSH`、`DEALER` 一次提交多个 copied payload，
并在全部已提交 frame 到达与单条 send 相同的交付边界后返回。TCP connect endpoint 会把编码后的
多个 frame 合并为一次 stream write；其他合法 endpoint 布局保留同一批次提交/完成语义，但不保证
一次 write。若某个 item 在准备阶段失败，之前的 item 仍会发送，`submitted` 返回实际提交数；调用方
因此必须同时检查返回码与 `submitted`。`REQ/REP` 不支持此 API，以保留严格 session/correlation
顺序。单批最多 `TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_ITEMS` 项，payload 总量最多
`TURBO_FLOW_FMQ_APP_SEND_BATCH_MAX_PAYLOAD_BYTES`；超过上限分别返回 `TURBO_ERANGE` 与
`TURBO_EMSGSIZE`。
Facade 的 explicit batch 直接在同一 Core 内准备并提交 frame，不经过 Graph dispatch。单独使用
Graph adapter 时，带 observer、retry、reorder、deadline、settlement、emitter 或下游 stage 的
Graph 仍按其自身标量/批量契约执行。两者都不改变提交计数、首错、消息所有权或 FMQ/3 wire。
高频 producer 可在首次 start 前调用 `turbo_flow_fmq_app_configure_async_send()`，再用
`turbo_flow_fmq_app_send_async()` 把 copied payload 交给 facade-owned 有界队列。队列同时受 item/byte
配额约束，满时立即返回 `TURBO_ENOSPC`；accepted 消息由单 worker 保序组成 micro-batch，非空
completion 每条调用一次。`stop()` 会先关闭 admission 并排空 accepted 消息，再停止 endpoint Core。
`turbo_flow_fmq_app_send_message()` 保留已有 message metadata，并作为 ROUTER detached route 的
delayed-reply 入口。REP callback 可用 `turbo_flow_fmq_app_message_set_payload_copy()` 替换 payload，
返回 `TURBO_OK` 后由同一次 Core dispatch 同步回复。生命周期和 send API 不可从同一个
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
channels.<name>.config，JOB 由上游 processor 预编码。durable owner C API 支持 Redis Stream，
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
不会被其他 transport 静默接受。UDP bind 端按远端 IP/port 维持 HELLO 会话，但每个 peer
只保留一个未读数据报，仍然允许丢包；需要可靠、有序、FEC 和 AEAD 的 UDP 数据面应选择 KCP。

## Protocol ownership

协议字段和状态机不在 README 重复维护。请从
[协议索引](PROTOCOL_SPEC.md) 进入对应唯一正文：

- [FMQ/3 与 FMS/3 wire](FMQ_WIRE_PROTOCOL.md)
- [TFMP/1 与 TFMS snapshot](MANAGEMENT_PROTOCOL.md)
- [TFCW/1、TFBR/1 与 TFCS/1.0](BULK_CREDIT_PROTOCOL.md)
- [安全决策](ADR_FMQ_V3_SECURITY.md)
- [部署控制](DEPLOYMENT_CONTROL.md)

README 只保留产品使用层的 pattern、graph 和 facade 说明；ACK、ownership、TLS/WSS
证书和安全 envelope 的规范以专题文档为准。

## Product boundary

| Capability | Status and limit |
| --- | --- |
| PUB/SUB、PUSH/PULL、PAIR、REQ/REP、ROUTER/DEALER、XPUB/XSUB | supported，遵循上表 pattern contract |
| Load balancer、reliable request、credit worker | supported，由 graph + pattern owner 组合，不增加 wire pattern |
| Redis Stream durable replay | supported，Stream/PEL 是事实源 |
| Redis Data SET/GET | supported，binary-safe data contract，与 Stream ACK 分离 |
| PgSQL durable outbox | not claimed；普通 PostgreSQL query/sink 不等于事务 outbox source/sink |
| TFMP management | supported，bounded DEALER/ROUTER inflight、typed async operation、event store |
| Failure-domain deployment owner | supported，authority epoch 必须由宿主强一致服务分配 |
| Authentication/authorization/Group Forest | supported for optional secure v3 endpoints on TCP/TLS/UDP/KCP/Pipe/WS/WSS；HTTPS v2 authentication、SQLite/HTTPS v3 line-based dynamic ACL bundle、local immutable indexed snapshot、immutable Root Group isolation、hierarchical effective groups、default-deny exact/prefix ACL；TLS/WSS additionally enforce TLS 1.3 exporter binding |
| ZeroMQ/ZMTP compatibility | not supported |

所有高级协议和部署契约统一从 [PROTOCOL_SPEC.md](PROTOCOL_SPEC.md) 导航；发布验证仍见
[RELEASE_GATE.md](RELEASE_GATE.md)。
