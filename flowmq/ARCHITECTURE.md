# FlowMQ architecture

本文描述 FlowMQ 的最终架构边界。FlowMQ 是以 pattern owner 为核心、可组合 TurboFlow graph
的 message broker，与 Flowie 并列；基础 pattern 不等于完整业务 graph。
`io/common` 只提供共享 CoroNet execution/transport bridge。

简单 FlowMQ 模式直接使用公开 C API，不要求 YAML 或 TurboFlow。DEALER/ROUTER、PUB/SUB、
PUSH/PULL 等基础 pattern 由各自 owner 完成协议状态与数据流转；proxy、持久化、规则路由、
跨 provider fan-out 等复杂组合才把消息交给 TurboFlow graph。

## Layers

```text
YAML resolver --------> typed endpoint configuration <-------- C API
                                  |
                                  v
                    FlowMQ pattern/session Core
                                  |
                 +----------------+----------------+
                 |                                 |
                 v                                 v
       direct application callback       optional TurboFlow adapter
                                                   |
                                                   v
                                      graph stages / new composition
                                  |
                                  v
                 FMQ/3 framing + security + heartbeat
                                  |
                                  v
             CoroNet TCP/TLS/KCP/Pipe/WS/WSS transport
```

| Layer | Responsibility | Dependencies |
| --- | --- | --- |
| `FlowMQ::Protocol` | v3 frame/security-envelope validation、encode/decode、fragmentation、heartbeat deadline | TurboUtils |
| FlowMQ endpoint Core | CoroNet endpoint、pattern/session owner、stream framing、HELLO、peer queue、reconnect | Protocol、CoroNet、TurboUtils |
| optional TurboFlow adapter | Core message 与 graph owned message/typed operation 之间的薄适配 | FlowMQ Core、TurboFlow |
| `TurboFlow::FMQ` | 当前安装 target；management、retry、deployment 与可选 graph composition | FlowMQ Core、TurboFlow |

`FlowMQ::Protocol` 已独立安装。`flowmq_runtime_core`、`flowmq_coronet_transport` 和
`flowmq_connect_endpoint_runtime` 是 build-only target，不进入安装 export。当前安装消费者只使用
`TurboFlow::FMQ`。这里的 Core 是运行时所有权边界，不是第二套协议实现；Graph adapter 不复制
endpoint/session 状态。

## Data path

The provider/frame/message boundary is defined in
[`../turbo_flow/ADR_MESSAGE_GRAPH_BOUNDARY.md`](../turbo_flow/ADR_MESSAGE_GRAPH_BOUNDARY.md).

Ingress 路径：

```text
CoroNet recv
  -> bounded stream framing
  -> complete FMQ v3 frame (temporary protocol view)
  -> owned message buffer + typed protocol metadata
  -> basic pattern owner
  -> direct callback, or optional TurboFlow graph adapter/stages
  -> FMQ / HTTP / socket / Redis / FlowStore sink
```

raw socket bytes、decoder accumulation buffer 和单次 frame view 只允许存在于 CoroNet owner lane。
进入 direct callback、graph adapter 或异步 queue 前，FlowMQ 将 payload、topic、identity、
correlation 和 metadata 放进同一个引用计数 `mem_buffer_t`。Direct callback 在 owner dispatch
期间借用消息；若要延迟处理，调用方必须显式 clone/detach。Graph adapter 只把同一 owned message
交给 TurboFlow，不保留 socket、peer pointer 或 decoder view。

基础 pattern 的 graph bridge 只完成统一消息/typed operation 交接，不拥有 peer session、订阅
匹配、REQ/REP FSM 或 socket side effect。只有高级组合才把多个 processor、executor 和 queue
连接成完整 graph。

Egress 先生成完整 encoded frame，再经过有界 adapter/peer admission 提交给 CoroNet owner。worker
只提交 owned send command，不持有 socket 或 peer pointer。

## Core / Graph ownership protocol

- 一个 endpoint Core 独占 socket、peer/session、pattern FSM、selector、发送预算和 per-peer queue。
  Direct facade 与 Graph adapter 都只是该 Core 的调用面，不能各自推进副本状态。
- ingress dispatch 是同步、同 owner-lane 的 borrowed callback。Core 在 callback 返回后清理本次
  message；异步消费者必须先 clone/detach，失败则当前 frame 明确失败。
- Graph adapter 在 ingress callback 内构造/保留完整 owned `turbo_flow_msg_t`，然后发布到明确的
  graph source。Graph 停止后 adapter 关闭 admission，不把消息静默改投 direct callback。
- egress 在返回成功前完成本地 admission/交付边界。serialized 每次等待一个结果；explicit batch
  在同一 Core 内合并已准备好的 frame；async micro-batch 只增加一个有 item/byte 上限的 facade
  queue，三者共享相同 wire、pattern FSM、HWM 与错误语义。
- 所有增长结构均受 endpoint HWM、per-peer HWM、batch item/byte limit 或 async item/byte limit
  约束。容量不足返回显式错误或执行已配置的 admission policy，不创建隐藏无界队列。
- shutdown 顺序固定为：停止新 admission，唤醒/关闭等待者，完成或取消已接受 queue，等待
  CoroNet owner-lane task 退出，最后销毁 session/queue/socket 并清除 transport key。Graph runtime
  的销毁不能早于其 adapter 从 Core 解除 dispatch。
- Flowie 使用同一边界：MQTT broker/session Core 独占协议状态，ingress 通过注入 sink 交给 direct
  handler 或可选 Graph adapter。Flowie Core 不解析、编译或启动 graph。

## State ownership

| State | Single owner | Derived/borrowed views |
| --- | --- | --- |
| socket、listener、connect lifecycle | CoroNet endpoint owner | endpoint snapshot |
| peer identity、generation、HELLO、heartbeat | FlowMQ peer session | message metadata |
| credential lease | host key provider | v3 HELLO transient bytes；发送/消费后清零 |
| authenticated principal | FlowMQ peer session | authorization request borrowed view |
| ACL policy generation | immutable security realm | per-operation decision |
| subscription prefix set | XPUB/XSUB session owner | selector snapshot/event |
| REQ/REP FSM and correlation | peer session | current request metadata |
| ROUTER live route | ROUTER session registry | pointer-free route value |
| TFCW worker credit/correlation | FmqCreditWorker resource owner | snapshot/status document |
| graph attempt and executor state | TurboFlow runtime | observe snapshot |
| durable payload/replay | Redis Stream or explicit durable owner | claim/view |
| management operation/event state | TFMP owner + configured store | status/event document |

Observe、TurboFlow Policy 和 FlowStore 不能独立推进 FlowMQ session/pattern state。缓存、snapshot 和
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
owner 和 storage settlement 形成的应用协议，不扩展 FMQ socket pattern，也不改变 wire v3。
TFCW transform adapter 不拥有 socket：它只解码 TFCW/1、串行推进 credit owner，并在 owned message
上替换 client/worker route。持久化 claim 仍由 Redis Stream 或其他显式 durable owner 独占；在 message contract
具备通用 claim projection 前，durable TFCW 只通过显式 C owner API 组合。

## Configuration boundary

人工事实源是 YAML v1。resolver 完成 fragment/profile 合并并产生 immutable JSON snapshot；FMQ adapter
将自己的 resolved entry DataBind 到 typed config，再走与 C API 相同的校验。graph compiler 只理解
resource、adapter 和 typed operation binding，不解释 transport/topic/HWM 等 provider 字段。

Host callback、schema pointer、borrowed CoroNet context 等进程内对象不能写入 YAML，必须通过显式 C
API 注入。unknown field、错误类型、非法 pattern/mode pairing 和不适用的 transport option 均在启动前
fail fast，不回退到默认 backend 或 volatile storage。

## Control and persistence

FlowMQ data plane 不创建隐藏 durable queue。持久化必须显式组合 Redis Stream 或其他 durable
store owner，并区分 accept ACK、transport send 和 delivery/completion ACK。

Management 和 deployment control 都是普通 FMQ DATA payload 上的版本化应用协议：

- TFMP 使用稳定身份 DEALER/ROUTER 流水化 query 与异步 operation acceptance/result，并用单独
  PUB/SUB channel 发布 live event；
- failure-domain controller 维护 membership/election/fencing，authority epoch 由宿主强一致事实源提供；
- durable side effect 只有在 typed inspector 可以证明 APPLIED/NOT_APPLIED 时才能自动 reconcile。

它们不能成为新的 socket owner、graph state owner 或 payload data plane。

## Compatibility

当前公开契约固定为：

- public C API 只有当前完整结构，不设本地版本号，也不接受历史结构布局；
- wire v3 only；decoder 对其他版本返回 `TURBO_EPROTO`，无 negotiation/downgrade；
- YAML `kind: fmq` 与现有 typed operation name；
- installed headers `turbo_flow_fmq*.h`；
- installed runtime target `TurboFlow::FMQ`；
- installed codec target `FlowMQ::Protocol`。

FlowMQ 不承诺 ZeroMQ API、ZMTP、socket option 或跨 wire version rolling compatibility。v3 trusted
endpoint 本身不构成安全边界；只有显式 security binding 才启用 provider authentication、
principal/claimed identity 一致性和 realm default-deny ACL；TLS/WSS 额外强制 TLS 1.3 exporter
channel binding。`turbo_flow_fmq_security_owner_t` 是产品 composition owner：它根据 adapter 的
security metadata 和 realm `policy_source` 从宿主注册的 factory 中精确创建 auth/ACL provider，拥有
realm 与 provider 生命周期，并把 borrowed binding 注入 FMQ。FlowMQ Protocol 与 transport runtime
不依赖 SQLite、TurboHTTP 或 YAML。Flowie 与 FlowMQ 可复用相同 provider ABI，但必须使用不同 policy
namespace 和协议资源语义。详见
[ADR_FMQ_V3_SECURITY.md](ADR_FMQ_V3_SECURITY.md)。
