# FlowMQ architecture

本文描述当前实现，不记录迁移计划。FlowMQ 是顶层 graph-native message broker，与 Flowie 并列；
`io/common` 只提供共享 CoroNet execution/transport bridge。

## Layers

```text
YAML/C API
    |
    v
TurboFlow graph + typed FMQ operations
    |
    v
FlowMQ pattern/session runtime
    |
    v
FMQ v2 framing and HELLO/heartbeat
    |
    v
CoroNet TCP/TLS/UDP/KCP/Pipe/WS/WSS
```

| Layer | Responsibility | Dependencies |
| --- | --- | --- |
| `FlowMQ::Protocol` | v2 frame validation、encode/decode、fragmentation、heartbeat deadline | TurboUtils |
| private endpoint runtime | CoroNet endpoint、stream framing、HELLO、peer session、reconnect | Protocol、CoroNet、TurboUtils |
| `FlowMQ::Runtime` | graph adapter、pattern owner、management、retry、deployment | TurboFlow、private runtime、Protocol |
| `TurboFlow::FMQ` | installed compatibility target | current FlowMQ runtime |

`FlowMQ::Protocol` 已独立安装。`flowmq_runtime_core`、`flowmq_coronet_transport` 和
`flowmq_connect_endpoint_runtime` 是 build-only target，不进入安装 export。`FlowMQ::Broker` 只是
build-tree 过渡 alias；当前安装消费者继续使用 `TurboFlow::FMQ`。

## Data path

Ingress 路径：

```text
CoroNet recv
  -> bounded stream framing
  -> complete FMQ v2 frame
  -> message-owned metadata + payload
  -> TurboFlow graph/Disruptor
  -> processor/subgraph
  -> FMQ or another typed sink
```

raw socket bytes、decoder accumulation buffer 和单次 frame view 只允许存在于 CoroNet owner lane。
进入 graph 前，FlowMQ 将 payload、topic、identity、correlation 和 metadata 放进同一个引用计数
`mem_buffer_t`。因此完整消息可以跨 broadcast ring、worker、thread 或 coro；TurboFlow 会拒绝任何
仍携带外部 borrowed transport context 的消息进入异步边界。

Egress 先生成完整 encoded frame，再经过有界 adapter/peer admission 提交给 CoroNet owner。worker
只提交 owned send command，不持有 socket 或 peer pointer。

## State ownership

| State | Single owner | Derived/borrowed views |
| --- | --- | --- |
| socket、listener、connect lifecycle | CoroNet endpoint owner | endpoint snapshot |
| peer identity、generation、HELLO、heartbeat | FlowMQ peer session | message metadata |
| subscription prefix set | XPUB/XSUB session owner | selector snapshot/event |
| REQ/REP FSM and correlation | peer session | current request metadata |
| ROUTER live route | ROUTER session registry | pointer-free route value |
| TFCW worker credit/correlation | FmqCreditWorker resource owner | snapshot/status document |
| graph attempt and executor state | TurboFlow runtime | observe snapshot |
| durable payload/replay | Queue/Redis/SQLite owner | claim/view |
| management operation/event state | TFMP owner + configured store | status/event document |

Observe、RulesForge、Queue 和 storage 不能独立推进 FlowMQ session/pattern state。缓存、snapshot 和
protocol metadata 都是 owner state 的派生证据，不是第二事实源。

## Pattern invariants

- PUB selector 返回全部 matching sessions；无匹配为 `TURBO_ENOTCONN`。部分发送失败不能回滚已完成
  的 peer send，也不能由通用 retry 重播整个广播。
- PUSH selector 只选择一个 eligible PULL peer。选择后的不确定 transport failure 不改投其他 peer。
- REQ 同时只有一个 outstanding request。timeout/disconnect 后进入 resetting，只有新 session HELLO
  成功才能恢复 READY；旧 generation reply 无法完成新 request。
- REP 的 request context 是当前 dispatch 借用值，reply 必须同步完成。它不能 detach 或持久化。
- ROUTER ingress 附带 `owner_instance_id + session_id + session_generation`。detach 后该 route 可随
  owned message 延迟回复，但旧 generation 必须返回 `TURBO_ENOTCONN`。
- PAIR bind 最多保留一个 live peer。
- XPUB subscription set 由 peer session 持有；XSUB desired subscriptions 可在 transport reconnect 后
  replay，完整 flow stop 会清除该状态。

高级 load balancer、reliable request 和 credit worker 是普通 ROUTER/DEALER/PUB/SUB 加 graph、typed
owner 和 storage settlement 形成的应用协议，不扩展 FMQ socket pattern，也不改变 wire v2。
TFCW transform adapter 不拥有 socket：它只解码 TFCW/1、串行推进 credit owner，并在 owned message
上替换 client/worker route。持久化 claim 仍由 Queue/Redis/SQLite owner 独占；在 message contract
具备通用 claim projection 前，durable TFCW 只通过显式 C owner API 组合。

## Configuration boundary

人工事实源是 YAML v1。resolver 完成 fragment/profile 合并并产生 immutable JSON snapshot；FMQ adapter
将自己的 resolved entry DataBind 到 typed config，再走与 C API 相同的校验。graph compiler 只理解
resource、adapter 和 typed operation binding，不解释 transport/topic/HWM 等 provider 字段。

Host callback、schema pointer、borrowed CoroNet context 等进程内对象不能写入 YAML，必须通过显式 C
API 注入。unknown field、错误类型、非法 pattern/mode pairing 和不适用的 transport option 均在启动前
fail fast，不回退到默认 backend 或 volatile storage。

## Control and persistence

FlowMQ data plane 不创建隐藏 durable queue。持久化必须显式组合 Queue、Redis Stream、SQLite 或其他
storage owner，并区分 accept ACK、transport send 和 delivery/completion ACK。

Management 和 deployment control 都是普通 FMQ DATA payload 上的版本化应用协议：

- TFMP 使用严格 REQ/REP 提交同步 command/query，并用单独 PUB/SUB channel 发布 live event；
- failure-domain controller 维护 membership/election/fencing，authority epoch 由宿主强一致事实源提供；
- durable side effect 只有在 typed inspector 可以证明 APPLIED/NOT_APPLIED 时才能自动 reconcile。

它们不能成为新的 socket owner、graph state owner 或 payload data plane。

## Compatibility

当前兼容契约固定为：

- public C API v1；
- wire v2 only；
- YAML `kind: fmq` 与现有 typed operation name；
- installed headers `turbo_flow_fmq*.h`；
- installed runtime target `TurboFlow::FMQ`；
- installed codec target `FlowMQ::Protocol`。

FlowMQ 不承诺 ZeroMQ API、ZMTP、socket option 或 v1 rolling compatibility。认证、授权和租户隔离不在
当前产品边界内，TLS 只表示 transport 能力，不能据此宣称 multi-tenant security boundary。
