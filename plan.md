# TurboFlow Primitive Graph 与 FMQ 演进计划

## 1. 文档约定

- `- [x]`：实现与对应的最小验证已落入仓库；仅含“确立/定义”的架构项表示设计决策已
  文档化，其运行时落地必须另列 `- [ ]`。具体运行结果由变更交付记录或 CI 保存。
- `- [ ]`：尚未完成，或已有实现但仍缺少契约、兼容性或测试闭环。
- 本文中的 FMQ 是 TurboFlow 内部的 ZeroMQ-like message fabric，不承诺
  ZeroMQ wire compatibility。

## 2. 总体目标

TurboFlow 的基本原则和出发点是：通过 graph 组合多个小型原语与操作，形成可验证、
可观察、可控制的数据处理系统。目标架构见
`turbo_flow/PRIMITIVE_GRAPH_ARCHITECTURE.md`；本计划用完成项记录现有事实，用未完成项
记录从当前实现迁移到目标架构的工作。

### 2.1 责任边界

- FMQ 负责协议状态、连接状态、peer/session 状态和 FMQ socket options。
- `io/common` 负责 CoroNet transport、endpoint 和 socket primitive 的统一适配。
- TurboFlow stage graph 负责编排、filter、codec/DataBind 和 proxy device。
- Queue/storage/redis/sqlite/pgsql 负责异步边界、持久化和重放。
- Observe/monitoring 只采集事件，不反向驱动 FMQ 状态。

### 2.2 兼容性原则

- 保持现有 `PUB/SUB`、`PUSH/PULL`、`ROUTER/DEALER`、`PAIR` 行为稳定。
- 新增配置字段不得改变旧配置的默认行为。
- 不默认启用 implicit retry、persistence 或 silent drop。
- 未知配置、非法状态和不适用的 transport option 必须 fail fast。
- 协议能力扩展必须通过版本或 capability negotiation 明确协商。

### 2.3 基础模式核心

基础模式按 `PUB/SUB`、`PUSH/PULL`、`REQ/REP` 的顺序闭环，高级模式不得直接包装
基础模式的严格状态机。FMQ 内部只共享以下稳定原语：

- session registry：由 CoroNet context 单线程拥有 socket、peer/session 和 pattern state；
  worker 只提交拥有 encoded frame 的 send command。
- fan-out selector：按 session subscription 选择全部匹配 peer，供 `PUB` 和后续 `XPUB`
  复用。
- round-robin selector：在 eligible session 中选择一个 peer，供 `PUSH` 和后续负载分配
  复用；选择后发生不确定 transport failure 时不得隐式改投其他 peer。
- direct route：使用稳定 session handle 定向回复，供 `REP`、`ROUTER` 内部复用；跨 command
  queue 不保存 borrowed peer pointer。只有 `ROUTER` 可把 pointer-free route 复制进 owned
  message；`REP` 仍要求当前同步 request context。
- correlation tracker：以 message ID + session generation 识别 reply，供 `REQ/REP` 及
  `ROUTER/DEALER` bridge 复用。
- pattern FSM：只表达各基础模式自己的合法序列，不作为高级模式的继承层。

状态事实源只有 FMQ owner context。公开 atomic snapshot 是派生只读视图；queue、worker、
storage 和 observer 不得独立推进 session 或 pattern state。CoroNet 只负责 context、
listen/connect、字节 send/recv、timeout 和 interrupt，不感知 FMQ pattern。

### 2.4 Primitive graph 管理模型

- [x] 确立四平面边界：management、data、protocol/message-pattern、transport。
- [x] 确立单一 owner 原则：Observe、Rules、Queue 和 Disruptor 不得独立推进 adapter、
  protocol session 或业务资源状态。
- [x] 确立 `Spec -> Status/Condition -> Reconcile -> typed Owner Command` 管理闭环；
  control plane 只处理 metadata、snapshot 和 command，不搬运业务 payload。
- [x] 确立 Disruptor 为有界并发 segment 的数据纽带，而不是持久化队列或长期事实源；
  相邻 inline stage 可直接传递 envelope。
- [x] 定义 Input、Processor、Route、Buffer、Batch、Output、Settlement 和 Observe tap 的
  目标职责与所有权边界。
- [x] 定义 IO 为受控 ingress/egress：data、typed status/event、typed command 三个接口
  分离；Queue/Buffer 不伪装为 IO connection。
- [x] 定义 TurboFlow Policy 的纯判定边界：data rule 返回 data action，control
  condition 返回 owner command proposal，规则执行器自身不做 IO 或资源状态迁移。
- [x] 明确内建计算 executor 只有 `inline`、thread pool、coroutine pool；Policy 是
  inline evaluator，Disruptor worker 是 bounded data handoff/consumer lane，CoroNet context
  是 IO owner placement。后二者不是额外计算 executor；旧 `socket`、`io`、`custom` executor
  及其注册 ABI 已删除。
- [x] 明确 domain、primitive、operation、graph node 四层关系：primitive 是 domain 内的
  value/resource 名词，operation 是有单一主要 effect 的动词，node 只做绑定和调度。
- [x] 增加版本化 module catalog：声明 capability、primitive type/operation exports 和依赖范围，
  typed operation provider 显式绑定唯一 module owner；TurboFlow Policy `rules.policy` 为首个生产接入。
  Catalog 是注册/校验层，不是 Graph DSL 资源工厂或 plugin loader。
- [x] 增加原子 native module-adapter 注册：显式绑定 `(module, operation, adapter)`，
  `ADAPTER_OWNER` operation 不得由 legacy callback/未绑定 adapter 冒充；失败不转移 context 所有权。
- [x] operation resource requirement 支持 inclusive version range；保留 V1 descriptor ABI，
  旧尺寸安全归一化为 any-version，新契约可在 compile 阶段拒绝不兼容 primitive version。
- [x] 固化 HTTP 例外：继续使用 TurboHTTP/Iris native endpoint/adapter，不迁移为 generic
  `io/socket` primitive；`io.http.client/server` catalog 将 request/poll/reply 固定绑定实际
  `HttpClientConnection` / `HttpServerEndpoint`，但不替换 transport owner。
- [x] 按相同 typed binding 接入 RPC、FMQ、Flowie、Queue 与 Storage：RPC 保留 native
  client/Iris server；FMQ 按 pattern 分开 operation；Flowie 只暴露 PUBLISH ingress/packet egress；
  Queue/Storage 使用 versioned resource primitive，不将其事实源误报为 adapter owner。
- [x] 将 FlowMQ 从 `io` 子树提升为顶层 `flowmq` broker 产品，与 Flowie 并列；第一阶段保留
  `tf_fmq`、`TurboFlow::FMQ`、`turbo_flow_fmq*.h`、YAML `kind: fmq`、API v1 和 wire v2
  兼容契约，并在 build tree 增加产品别名 `FlowMQ::Runtime` 与过渡别名 `FlowMQ::Broker`。协议、endpoint runtime 与
  graph-native runtime 的物理拆分按 `flowmq/ARCHITECTURE.md` 后续分阶段完成。
- [x] 抽取只依赖 TurboUtils 的 `FlowMQ::Protocol`：独占 FMQ v2 frame
  encode/decode、fragmentation/reassembly 与 heartbeat deadline；单 packet view 借用输入，
  多 packet payload 由 decoded frame 独占并统一 cleanup。CoroNet endpoint/config validation
  留在 endpoint/runtime 边界，避免 transport 配置反向污染 wire protocol。
- [x] 建立未安装的 `flowmq_runtime_core`：独占 FMQ pattern/HELLO 校验、control frame 编码、
  bounded stream framing、XSUB/XPUB subscription registry 与 REQ/REP session 状态，移除
  FlowMQ 数据面对于 TurboFlow exchange FSM 的依赖；HELLO 成功才推进 generation 并恢复
  RESETTING，旧 generation 的迟到 reply 不能完成新请求。该 target 是 FlowMQ endpoint
  与 peer session 的私有实现，不作为 Client SDK 导出。
- [x] 将无 TurboFlow 类型依赖的 `flow_coronet_runtime` 从 `tf_executor_common` 拆为独立内部
  target；Socket/FMQ 及其他 adapter 继续通过公共依赖转接复用同一 transport bridge，避免
  为 FlowMQ endpoint 复制 CoroNet transport 实现或引入重复符号。
- [x] 增加私有 `flowmq_coronet_transport` 与 reconnect policy：FlowMQ 自有 transport enum
  通过编译期固定数值映射到共享 CoroNet runtime，统一 create/apply/connect/listen/send/
  multicast；指数 backoff、bounded jitter 和 HELLO success reset 由 runtime core 持有。
  Connect socket/execution/callback owner 由私有 `flowmq_connect_endpoint_runtime` 持有。
- [x] 将 FMQ ingress 的 borrowed `transport_context` 改为 message-owned protocol metadata，
  使完整 owned message 可进入 TurboFlow Disruptor graph；raw socket/frame view 仍留在 CoroNet
  owner lane。REQ/REP 继续要求当前 publish dispatch 内完成，ROUTER/DEALER route 继续使用
  owner instance + session + generation fence，不新增第二个 FlowMQ data-plane Disruptor。
- [x] 将 Socket、HTTP 与 RPC 收敛为同一 resource-bound module-adapter 契约：Socket 导出
  `SocketEndpoint + socket.receive/send`；HTTP/RPC 分别导出自己的 client/server resource，
  typed DSL 显式绑定 owner，native CoroNet/TurboHTTP/Iris 生命周期保持不变。
- [x] 补齐 RPC client 对 TurboHTTP provider 的 versioned 显式依赖注入：可信 host 可 borrowed 或转移
  `http_client_t` 销毁所有权；默认仍创建私有 client，YAML 拒绝 host object，并覆盖 borrowed
  client 在 flow 销毁后仍有效的生命周期契约。
- [x] module-adapter 原子注册支持 operation-specific resource name 与 primitive instance；compiler
  同时校验 module、adapter、operation、resource name/type/domain/version，注册失败回滚新增
  primitive/resource 且不转移 adapter context。
- [x] 建立全产品共享的 caller-owned provider registry：YAML resolved snapshot 在任何 native
  副作用前 preflight 全部 adapter kind；Graph parse 后只装配实际引用的 source/sink adapter 与
  processor/resource，resource 先于 adapter，同名引用只注册一次。registry 只做依赖注入，不成为
  plugin loader、全局 service locator 或资源 owner；callback 失败由 host 丢弃本次 Flow generation
  并按所有权逆序清理。`flowie_server` 已作为首个迁移 host，profile 继续约束允许的产品组合。
- [x] 明确 operation 的 data、state、lifetime、concurrency、authority 五维作用域；跨 domain
  必须经显式 bridge 转换类型、所有权、错误和 settlement。
- [x] 将 resource metadata/Status/Condition/Command/Event 查询契约落实为版本化公共 C API；
  删除旧 adapter-bound resource callback、pointer identity 与固定枚举顺序，不提供兼容 shim。
- [x] 将 protocol/message-pattern 能力整理为独立公共层，先覆盖 FMQ pattern 和 MQTT QoS
  settlement，禁止把协议专属 options 塞入万能 primitive vtable。

基础交付契约：

- `PUB` 对当前 matching sessions fan-out；无匹配返回 `TURBO_ENOTCONN`。部分 peer 已成功后
  的失败是不可回滚外部副作用，不得由通用 retry 重播整个广播。
- `PUSH` 对当前 eligible sessions 做 round-robin；消息一旦选择 peer，本次 attempt 不在
  send failure 后改投，避免同一 job 被多个 `PULL` 执行。
- `REQ` 同时只允许一个 outstanding request；timeout/disconnect 后进入 resetting，并且只有
  新 session HELLO 成功才能回到 ready。旧 session 的迟到 reply 不得完成新 request。
- `REP` 状态属于收到 request 的 session；reply 必须在当前同步 TurboFlow dispatch 内使用
  borrowed transport context 完成。FMQ REQ/REP 不提供 durable/异步回复；需要持久化工作流时
  必须改用其他 graph protocol，并由应用携带 correlation identity，不能保留
  `transport_context`。

## 3. 当前基线

### 3.1 DSL 与编排

- [x] 从 parser grammar 移除 `flow`、`graph` 和 `subgraph` 拼写。
- [x] 由 `stage` 和 `source` composition 承担编排。
- [x] 删除 `socket`、`io`、`custom` executor keyword；adapter/CoroNet owner placement 与
  compute executor 分离。
- [x] 同步更新 `GRAMMARS.md`、`flow_lexer.re`、`flow_grammar.y` 和 parser tests。
- [x] 增加 `pubsub_proxy`、`queue_proxy`、`router_dealer_proxy`、
  `xpub_xsub_proxy` 的 compile-only graph 测试。
- [x] 在 FMQ 原生支持 XPUB/XSUB 后，将 `xpub_xsub_proxy` 从 custom adapter
  schema 测试升级为 FMQ runtime test。

### 3.2 FMQ pattern 与 transport

- [x] 支持 `PUB/SUB`、`PUSH/PULL`、`ROUTER/DEALER`、`PAIR` 和 `REQ/REP` 主路径。
- [x] 支持 `tcp`、`tls`、`udp`、`kcp`、`pipe`、`ws`、`wss`。
- [x] 支持 `topic_policy = inherit` 和 `identity_policy = inherit`。
- [x] 明确 FMQ 不提供 ZeroMQ wire compatibility。

### 3.3 共享 CoroNet runtime

- [x] 统一 socket/FMQ transport 枚举及 endpoint/path 归一化。
- [x] 统一 connect/listen/send 分发。
- [x] 提供 `tf_coronet_endpoint_config_validate()`。
- [x] Generic socket concrete config 注册时执行 endpoint fail-fast 校验。
- [x] Schema-only 注册在未绑定 concrete config 时保持兼容。
- [x] Generic socket 覆盖 KCP source/sink 测试。
- [x] Generic socket 在 Windows 覆盖 Pipe source/sink 测试。
- [x] FMQ endpoint 校验与共享 primitive 保持相同规则。

### 3.4 Socket primitive options

- [x] 暴露并透传 listener-side `reuse_port`。
- [x] 暴露 KCP-only FEC options；非 KCP transport 返回 `TURBO_EINVAL`，
  backend 不支持时返回 `TURBO_ENOTSUP`。
- [x] 暴露 TCP keepalive。
- [x] 暴露 OS `SO_LINGER`。
- [x] 暴露 socket-level `send_hwm_bytes`。
- [x] 对不适用 transport 的 keepalive、OS linger 和 socket HWM fail fast。
- [x] 增加 UDP multicast join/leave、loop、TTL options；membership 仅允许 listener/bind，
  adapter 在成功 bind 后 join，并在关闭 listener 前 leave。
- [x] 增加 UDP broadcast option；通过 presence flags 区分“未配置”与显式 false/zero，
  保持旧全零 config 的 OS 默认行为。
- [x] 完成 CoroNet execution binding/context/lane affinity API、所有权、调度、错误、
  迁移与回滚设计，见 `io/common/CORONET_EXECUTION_BINDING.md`。
- [x] 实现 shared execution binding、socket 接入和 FMQ 接入；borrowed context 由宿主
  持续驱动，pool lane 覆盖 socket/FMQ start-stop，跨线程工作统一经 `coro_post`。

### 3.5 FMQ connection 与 queueing

- [x] 实现基础 reconnect 和指数退避。
- [x] 覆盖 broker delayed startup 成功路径。
- [x] 覆盖 broker unavailable 时的 connect timeout。
- [x] 实现 `frame_hwm_messages` 和 `frame_hwm_bytes` in-flight 限制，超限返回
  `TURBO_ENOSPC`。
- [x] 实现 `frame_linger_ms = 0` 的立即停止语义。
- [x] 实现 `frame_linger_ms > 0` 的限时 drain 语义。
- [x] 实现 adapter-owned 有界内存发送队列和 `fail/block/drop_oldest` policy；
  持久化与进程重启恢复仍归 queue/storage composition 层。

### 3.6 Heartbeat 与 monitoring

- [x] 实现 PING/PONG 控制帧及 receive-loop 内部消费。
- [x] 实现 `heartbeat_interval_ms` 和 `heartbeat_timeout_ms` 基础行为。
- [x] 实现 FMQ-local `event_callback/event_ctx`。
- [x] 覆盖 peer connected/disconnected、reconnect scheduled/succeeded/failed、
  heartbeat timeout、frame sent、HWM reached 和 frame dropped events。
- [x] 提供 `turbo_flow_observe_record_control_event()` 通用桥接入口。
- [x] Observe snapshot 支持 peer/reconnect/heartbeat/send/HWM/drop/error counters。
- [x] FMQ wire protocol 升级到 v2；PING/PONG 使用 v2 单包控制帧。
- [x] 按明确的非兼容决策拒绝 v1 peer，不提供滚动升级兼容路径。

### 3.7 共享 timer

- [x] 在 `io/common` 提供 `tf_timer_wait_for_ms()` 和
  `tf_timer_wait_until_ns()`。
- [x] `flow_fmq_wait_connect` 使用绝对 deadline 等待。
- [x] HTTP client 的 poll、retry wait 和 stop response 使用共享 timer。
- [x] RPC client 的 poll interval 和 startup wait 使用共享 timer。
- [x] RPC stop 先断开连接再 join worker thread。
- [x] FMQ reconnect backoff 使用 CoroNet `coro_wait`，移除固定 50 ms 分片轮询。
- [x] FMQ heartbeat wait 使用绝对 deadline state machine，并通过 CoroNet socket
  operation timeout/interrupt 驱动，不使用固定 interval 轮询。
- [x] TurboFlow adapter retry delay 使用共享、可中断 timer；FMQ/HTTP/RPC 均接入。
- [x] 共享 timer 的 stop-triggered wait 统一返回 `TURBO_ESHUTDOWN`；绝对长 deadline
  不再被零值语义误判为立即 timeout；FMQ drain 的单次 wait timeout 只触发 in-flight
  predicate 重检，不再提前结束总 linger deadline，完成 signal 仍可提前唤醒。
- [x] Generic socket sink 的 pending request 使用 heap ownership 并复制 payload；publish pump
  budget 超限后 coroutine 可安全继续，stop 会中断 active socket 并等待 request drain。
- [x] TurboNet Redis client 提供 thread-safe `redis_client_interrupt()`；TurboFlow Redis stop
  在 join source worker 前中断当前 CoroNet socket wait。
- [x] 使用真实 Redis `XREADGROUP BLOCK` 覆盖 stop interrupt latency、同 consumer pending
  重放和成功后 XACK；真实 Redis 与进程内 RESP server 均覆盖 60 秒 BLOCK 中的 stop
  interrupt（<500 ms），进程内测试额外覆盖 `TURBO_ESHUTDOWN` 与 STOPPED snapshot 传播。

### 3.8 FMQ 产品契约冻结

- [x] 本地 C API 固定为 v1，公开 config/event 携带 `size` 与 `version`；config 必须使用
  `TURBO_FLOW_FMQ_CONFIG_INIT` 初始化，错误结构尺寸或 API 版本 fail fast。
- [x] wire 仅支持 protocol v2，明确拒绝 v1，且不承诺 ZeroMQ wire/API 兼容。
- [x] PUB 按当前 subscription prefix 向所有 matching sessions fan-out；无匹配和部分发送失败
  的用户可见语义已文档化并由 runtime test 覆盖。
- [x] REQ/REP 固定为同步 dispatch reply；ROUTER/DEALER 提供 message-owned、generation-aware
  route token，可跨 worker/fan-out/memory Queue 完成 delayed reply，断线、同 identity 重连或
  ROUTER restart 后旧 token 返回 `TURBO_ENOTCONN`，且不允许持久化重放。
- [x] 持久化与重放固定为 Redis Streams、SQLite 等 graph composition 行为，不在 FMQ 内维护
  隐藏 durable queue。
- [x] 人类维护的 FMQ 配置使用 YAML v1；resolver 输出 immutable resolved snapshot，再投影到
  与 C API 相同的类型化校验路径。
- [x] 认证、鉴权和密钥管理不属于当前 FMQ v2 产品契约，本阶段不据此宣称安全边界。

### 3.9 TurboFlow execution 与 worker data plane

- [x] 统一 thread、coro 和 disruptor execution task 的
  `submit/run/wait/yield/abort/discard` 内部原语。
- [x] `submit/wait` 由 runtime 所有，stage callback 只暴露
  `yield/abort/cancel_requested`。
- [x] `abort` 使用协作式取消，不强杀 running thread/coroutine。
- [x] `worker N` 使用 N 个真实 disruptor consumer threads，并同步传播 stage status。
- [x] 使用 `worker N capacity M` 配置有界 ring；默认 1024，显式值必须是
  1..1048576 的 2 次幂。
- [x] worker ring 使用 multi-producer blocking claim；满时同步 backpressure，不将
  瞬时 claim 竞争误报为 `TURBO_ENOSPC`，不隐式 drop。
- [x] 覆盖 thread/coro yield、accepted/running abort、worker 并发、容量传递、
  stop/restart 和 message ownership。
- [x] 使 `turbo_flow_publish()` 支持并发调用；sequence/reorder reservation、
  producer-local completion/error、coro lane 与 stop admission/drain 均有同步边界。
- [x] `exec coro lanes N pool M` 使用每 lane 独立 scheduler/pool/mutex，支持 N 路
  producer 并发且不共享 pool bookkeeping。
- [x] Release benchmark 覆盖 4 producer worker ring 与 4 lane pooled coro。
- [x] worker idle path 使用 Disruptor waiter-aware claim：短自旋后 park，publish 在存在
  waiter 时唤醒，stop 显式 interrupt；移除持续 `try_claim + yield` polling。

### 3.10 Graph observability 与 control plane

- [x] core 提供只读 runtime snapshot：state、publish admission、active publish、
  stage/edge/adapter/pool 数量。
- [x] Observe 提供统一 graph snapshot：payload message/byte/error/latency、连接 gauge、
  pool saturation 和 TurboUtils OS CPU/memory/load。
- [x] thread、coro 与 Disruptor worker 统一复用 `turbo_flow_pool_snapshot_t`，暴露
  parallelism、capacity、queued、active、completed/failed/canceled/rejected。
- [x] pool records 以 runtime generation 为单位重建；stop 保留最后一代 STOPPED snapshot，
  restart 不追加 stale records，Disruptor submit/start/finish 进入统一 counters。
- [x] snapshot 使用 caller-owned 固定结构；OS 指标仅在 pull 时采集，消息热路径只做
  relaxed atomic counters，不记录逐消息日志。
- [x] 明确 snapshot 与 parse/compile/start/stop/reset/destroy 由 host 串行化；publish
  admission/completion 与计数读取保持线程安全，不把 lifecycle vectors 伪装成全并发快照。
- [x] 定义类型化 connection snapshot provider，包含 endpoint、连接 state/limit、in-flight
  message/byte 与 last status；FMQ 首个接入，core 按注册级 adapter 枚举。
- [x] Generic socket、HTTP client/server、RPC client/server、S3、SMTP、POP3、Redis 与 FMQ
  接入 connection snapshot provider；HTTP/RPC client 只报告逻辑 endpoint/state/request
  load，S3 同样只报告逻辑 object endpoint/state/request load，不猜测 TurboHTTP 内部复用
  连接数，且 snapshot 不包含 credential。
- [x] 已删除公开 FlowQueue；FlowStore 通过类型化 stats 报告 records/bytes/rejects/trims，
  thread/coro/Disruptor pool 继续使用 `turbo_flow_pool_snapshot_t`。
- [x] 定义通用类型化 resource provider，让 Observe 可聚合 queue/buffer、connection 和 pool，
  同时保留各资源专属 snapshot，不用 connection 字段承载 queue depth。
- [x] `io/common` 提供协议无关的 connection state primitive；generic socket、Redis 和
  HTTP client 使用同一 endpoint/state/in-flight/last-status snapshot ABI，协议专属 options
  仍由各 adapter/client 持有。
- [x] Generic socket sink 通过 control DSL 支持幂等 quiesce/resume admission；source
  quiesce 和 endpoint replace 在具备安全 interrupt/rebuild 前 fail fast 返回 `TURBO_ENOTSUP`。
- [x] SMTP pending request 改为 heap ownership 并复制 payload；stop 通过 TurboNet
  `smtp_interrupt()` 中断 CoroNet wait，接入统一 connection snapshot 和 quiesce/resume。
- [x] POP3 增加 caller-owned raw retrieval 与 thread-safe `pop3_interrupt()`；TurboFlow
  source 使用 UIDL 做 adapter-lifetime 去重，默认不删除服务器消息，并接入统一
  connection snapshot、quiesce/resume 和可中断 poll timer。
- [x] 使用本地 smtp4dev 验证 SMTP 投递和 POP3 raw source：`smtp://127.0.0.1:25`、
  `pop3://127.0.0.1:110`，POP3 认证 `turbo/turbo`，不执行 `DELE`。
- [ ] HIGH：修复 TurboNet `pop3_retrieve_message()` / `pop3_retrieve_headers()` 返回对象的
  backing `mem_pool_t` 生命周期；当前 TurboFlow POP3 source 仅使用 ownership 明确的 raw API。
- [x] 定义并实现幂等 pause/resume admission 与 deadline drain；command 不直接修改
  observer snapshot，stop 与 paused admission 使用不同内部状态。
- [x] 定义并实现 runtime-owned Disruptor worker、thread worker 与 coro lane resize command；
  command 按 stage + pool kind 定位，先 drain 再重建 generation，失败恢复旧配置，timeout
  保持 paused，stop/并发 lifecycle command 返回显式错误。
- [x] 实现 host-owned reconcile tick：调用方拥有 loop/state，Observe 拉取 observed snapshot、
  计算 desired parallelism 并调用 resize command；不创建隐式 controller thread，runtime 仍是
  live pool 唯一 owner。
- [x] pool policy 支持 min/max、scale step、高低水位、连续观测 hysteresis、cooldown、drain
  deadline 与可选 per-core 1m OS load guard；resize command 定义失败回滚与 stop 竞争语义。
- [x] 定义 adapter owner command，FMQ 实现幂等 quiesce/resume 与结构化 endpoint replace；
  active replace 失败恢复旧 endpoint 与 READY resources。pool resize 不隐式 quiesce source，
  host 按 quiesce ingress -> drain -> resize -> resume 顺序编排。
- [x] 新增 control DSL：无条件 command 以及 `if|when <typed facts> then <command>`；
  core 内建 runtime/pool/adapter facts，条件纯求值且每条 rule 只执行一个 owner command。
- [x] Observe 提供 `traffic.*`、`system.*`、`graph.*` typed facts bridge；host 可用
  同一 schema/provider ABI 注入业务数据快照，未知字段、类型错误与超限均 fail fast。
- [x] HTTP/RPC connection provider 经 Observe 聚合为 `graph.connection_providers`，并覆盖
  条件 DSL 驱动 adapter owner command；Queue snapshot 不计入 connection provider。
- [x] S3 connection provider 经 Observe 聚合为 `graph.connection_providers`；PUT sink 支持
  条件 DSL quiesce/resume，snapshot 覆盖 STOPPED/READY、request load 和 last status。
- [x] `if|when <typed facts> then <command>` 不限定资源种类；所有资源状态、payload
  派生数据和业务数据均通过 immutable snapshot provider 接入，action 仍由资源 owner 执行。
- [x] 保留 data-plane `route ... when <message expr>` 处理单条 payload；control-plane
  condition 只读 snapshots，不直接写资源状态或触发任意回调副作用。
- [x] 提供 Prometheus/OpenTelemetry adapter 时保持核心 snapshot ABI 不依赖 exporter。

## 4. 必须先解决的契约问题

### 4.1 Timeout presence 与零值语义

当前 C config 的 `0` 同时被用于“字段未设置并从 `timeout_ms` 回填”和候选的
“禁用 timeout/立即 retry”语义。在没有 presence 信息时两者不可区分。

- [x] 为内部 resolved timeout 增加 presence bits，公开 config 使用命名 sentinel。
- [x] 定义 timeout/reconnect 的 absent、`0`、正数语义。
- [x] 保持旧 C config 中 `0` 回填默认 timeout/reconnect 的兼容行为。
- [x] 使用 `*_TIMEOUT_DISABLED` / `*_RECONNECT_DISABLED` 表达显式禁用。
- [x] 覆盖 legacy fallback、explicit zero、explicit value 和 mixed timeout tests。

### 4.2 Connect 与 handshake timeout 能力边界

CoroNet 当前提供 per-socket current-operation timeout，没有独立的 connect 和
TLS/WebSocket handshake timeout API。一次 connect 调用可能同时包含 transport
连接和握手。

- [x] 第一阶段将 connect 定义为完整 connect operation
  deadline，包括其中发生的 handshake。
- [x] 在 CoroNet 提供可拆分握手 API 前，不声明 TLS/WS/WSS connect 与 handshake
  timeout 可独立生效；client 侧两者映射为同一 deadline，显式冲突时 fail fast。
- [x] server/BIND 侧显式 handshake timeout 在 CoroNet 能力补齐前返回 `TURBO_ENOTSUP`，
  非握手 transport 显式设置该 option 返回 `TURBO_EINVAL`。
- [x] socket/FMQ C config、共享 resolver、option schema 和接口文档使用同一契约。

### 4.3 Protocol version 与 capability negotiation

FMQ 已选择不兼容的 protocol v2，不使用 HELLO capability bitmap。v2 DATA payload
以 64 KiB 为单包上限：小消息单包，大消息使用 connection-local message ID、总长度和
连续 offset 自动拆分与重组；identity/topic 只出现在 FIRST 包。版本不匹配、未知 flag、
不连续 offset、跨包元数据不一致和未知 control frame 均 fail fast 返回 `TURBO_EPROTO`。
PING/PONG 是 v2 内建的单包控制帧，不支持 v2 的 peer 在 HELLO 前即因版本不匹配被拒绝。

- [x] 选择非兼容 protocol v2，并定义 32-byte packet header。
- [x] 定义版本、flag、message ID、长度和 offset 错误语义。
- [x] 实现单包/多包自适应编码与有界重组。
- [x] 覆盖 v2 单包、多包、partial input、offset 篡改和 v1 拒绝。

### 4.4 Profile 与 adapter 配置事实源

现有配置设计规定 profile 只绑定 stage parameters，adapter config 是具体外部行为
的唯一事实源。不得直接引入第二套可独立修改协议行为的 profile 状态。

- [x] 选择 profile 仅通过 stage parameter 引用命名 adapter config；禁止 adapter overlay。
- [x] 因不允许 overlay，不定义字段合并顺序或 presence override；部署差异必须由 host
  先物化为命名 concrete adapter entry，resolved JSON 只保留 profile provenance。
- [x] 保持 concrete adapter config 为运行时只读快照和唯一最终事实源；动态 endpoint
  更新只通过 adapter owner command 校验、提交和回滚，不反向修改 profile。
- [x] 在 host resolver 完成前，不在 `.flow` grammar 中接受未解析 profile，并以 parser
  回归测试锁定 fail-fast 行为。

## 5. 分阶段路线图

### Phase 1：完成 socket/FMQ option 契约

目标：使已暴露和待新增的 socket options 具有明确、可测且跨 adapter 一致的语义。

- [x] 完成第 4.1 节的 timeout presence/zero-value 契约。
- [x] 完成第 4.2 节的 connect/handshake timeout 契约。
- [x] 在 `io/common` 集中定义 timeout option names 和 resolved config。
- [x] socket 与 FMQ 统一通过 CoroNet runtime primitive 应用 connect/send/recv timeout。
- [x] 根据 CoroNet 单 wait 能力，不暴露独立 handshake timeout 语义；字段表示 client
  connect+handshake 联合 deadline，并对冲突/不支持方向 fail fast。
- [x] 实现 UDP multicast/broadcast options。
- [x] 定义 context/lane affinity API 与 lifecycle contract，明确 pool lane 是稳定
  `coro_context_t` 选择而非 OS CPU affinity。
- [x] 实现 `_ex` registration、shared execution helper、socket/FMQ pool-lane coverage。
- [x] 统一 socket/FMQ 的 timeout、interrupt、KCP FEC、reuse-port、keepalive、
  OS linger、socket HWM、UDP extras 和 affinity validation。

验收：

- [x] 旧 `timeout_ms` 配置保持 fallback 到各 phase 的既有行为。
- [x] 显式 phase timeout 只覆盖对应 phase，不影响其他 phase。
- [x] keepalive、linger、socket HWM 与 KCP FEC 用于不适用 transport 时返回明确错误，
  不静默忽略。
- [x] timeout tests 覆盖 silent TCP receive 与 silent WebSocket upgrade。
- [x] linger tests 覆盖 drain、deadline expiry 和 immediate close。
- [x] HWM fail policy 覆盖 message count、byte count 和并发 in-flight accounting。

### Phase 2：显式 reconnect/heartbeat 状态机

目标：让 connect-mode endpoint 使用单一、可观察、可停止的状态机。

公开 connection snapshot 状态集合：`stopped`、`connecting`、`ready`、`backoff`、
`closing`、`failed`。当前 CoroNet connect 调用包含 transport connect 与 handshake，
因此 handshake 不作为独立公开状态；尚未启动的 endpoint 使用 `stopped`，不另设 `idle`。

- [x] FMQ adapter 是 endpoint state 的唯一所有者；peer/session 只维护 peer 派生连接数。
- [x] FMQ 状态迁移集中通过单一 transition helper 发布 snapshot 字段；socket
  create/connect/destroy 和 event callback 留在调用层，不在状态 helper 中执行 I/O/回调。
- [x] 实现 reconnect initial/max interval 的指数退避与上限。
- [x] 实现 bounded equal reconnect jitter；实际 wait 位于指数退避基准的
  `[ceil(base / 2), base)`，下一次退避仍从未抖动的 base 推进，且不超过 max。
- [x] 使用 CoroNet `coro_wait` 实现可中断 backoff。
- [x] 使用绝对 deadline 驱动 heartbeat；timeout 返回失败并由 session owner 移除 peer。
- [x] disconnect 后同步 send 返回本次 transport error，request completion 释放 frame budget；
  reconnect 只新建 transport，不持有或重放已完成 request。异步 send 从 post 前到 completion
  计入 lane task；stop 后才获得执行机会的 request 返回 `TURBO_ESHUTDOWN`，不再启动 socket I/O。
- [x] reconnect 不重放已经向 caller 返回失败或成功的 message。
- [x] stop 通过 socket interrupt、`coro_wait_interrupt` 和 context stop 中断
  connect/handshake operation、backoff 和 heartbeat wait。

验收：

- [x] broker delayed startup 后连接成功。
- [x] broker 断开后进入 backoff，并在恢复后重新连接；断线前已交付消息不重放。
- [x] heartbeat timeout 移除 peer、连接 gauge 归零并只产生一次状态事件。
- [x] stop during retry delay、Redis BLOCK 和 FMQ backoff pending wait 返回
  `TURBO_ESHUTDOWN`。
- [x] private owned context 和由调用方转移所有权的 `OWNED_CONTEXT` 行为一致；socket
  覆盖真实 TCP source 收包，FMQ 覆盖独立 owned contexts 上的 PUB/SUB 往返与 shutdown。

### Phase 3：FMQ sink retry integration

目标：将 FMQ sink 接入 TurboFlow adapter retry contract。

- [x] 为 FMQ sink 实现 `consume_retry`。
- [x] 将 retry eligibility 从单一错误码升级为 attempt delivery stage；只有明确尚未开始
  socket 外部副作用的失败可重试，PUB partial fan-out 及已开始 PUSH/REQ send 均不可重放。
- [x] non-retryable：invalid pattern、oversized frame、metadata inheritance failure、HWM。
- [x] 每次 attempt 使用隔离的 message clone。
- [x] stop 中断 retry delay，并以 `TURBO_ESHUTDOWN` 明确结束等待。

验收：

- [x] `stage out adapter fmq.pub retry attempts 3 delay 10` 可成功重试。
- [x] non-retryable error 不重复发送。
- [x] stop during retry delay 返回 `TURBO_ESHUTDOWN`，restart 后 timer 可复用。

### Phase 4：FMQ drain queue 与 HWM policy

目标：在不引入持久化职责的前提下，提供有界的内存发送队列。

共享原语基线见 `turbo_flow/POLICY_PRIMITIVES.md`：FMQ 使用 composite message/byte
admission budget 管理总占用，adapter-owned FIFO request queue 管理 frame ownership 和
发送顺序。默认 `fail`、带 deadline 且 stop 可中断的 `block`、仅淘汰尚未开始发送请求的
`drop_oldest` 均已实现；持久化不是该内存队列的职责。

- [x] 明确 adapter 是队列 owner；HWM 定义容量，request 持有 encoded frame，completion
  或 shutdown cleanup 释放 frame 与 budget。
- [x] 实现 `fail` policy，容量不足返回 `TURBO_ENOSPC`。
- [x] 实现可停止的 `block` policy，deadline 返回 `TURBO_ETIMEDOUT`，stop 返回
  `TURBO_ESHUTDOWN`。
- [x] 实现 `drop_oldest` policy，仅淘汰最旧 queued request，以 `TURBO_ECANCELED`
  完成该请求并发出 `FRAME_DROPPED` event；active send 不被淘汰。
- [x] 定义 disconnect、linger timeout 和 reconnect 时的队列行为：每个 accepted request
  只完成一次并释放 frame budget；linger deadline 后中断未完成 transport wait，已完成的
  transport result 不回写；reconnect 只影响未来 send，不持有或重放 completed request。
- [x] 保持持久化和进程重启恢复在 queue/storage composition 层。

验收：

- [x] 覆盖 fail/block/drop_oldest。
- [x] 覆盖 stop while blocked。
- [x] 覆盖 frame/message count 与 byte limits。
- [x] 验证 dropped/failed frame 完成后 in-flight counters 归零。

### Phase 5：REQ/REP

目标：提供比 DEALER/ROUTER 更严格的 request/reply 语义。

- [x] 新增 `REQ` 和 `REP` pattern、HELLO compatibility、C config 和 option schema 值。
- [x] REQ 主状态机：ready -> wait reply -> ready；第二个 concurrent request 返回
  `TURBO_EBUSY`。
- [x] REP 主状态机：ready -> processing request -> ready；无 request context 的 reply 返回
  `TURBO_EBUSY`，未 reply 的 dispatch 以 `TURBO_EPROTO` 关闭 session。
- [x] wire message ID 作为 correlation ID，并提供同步 dispatch 内的 borrowed 查询 API。
- [x] 使用稳定 `{session_id, generation}` 取代跨 send command 保存的 borrowed peer pointer；
  send drain 只在 FMQ owner context 内解析 route token，旧 runtime generation 必定失效。
- [x] 完成 REQ timeout/disconnect 恢复：旧 session 作废，新 HELLO 后恢复 ready，旧 correlation
  不污染下一 request。
- [x] 明确并实现 retry 与 REQ FSM 的组合：已提交 request 不自动重发；未提交失败才允许
  policy retry。

验收：

- [x] TCP REQ/REP repeated round trip 与 correlation 一致。
- [x] REQ 连续发送 request 被拒绝。
- [x] REP 在收到 request 前发送 reply 被拒绝。
- [x] timeout、disconnect 和 reconnect 后状态可复验。
- [x] pending reply 时 stop 丢弃旧 session reply；stop 本身返回 `TURBO_OK`，停止后的
  `turbo_flow_publish()` 按 core lifecycle 契约返回 `TURBO_EINVAL`，新 HELLO 后才允许下一
  request。
- [x] 覆盖 TCP/private、TCP/`OWNED_CONTEXT`、TCP/host-driven `BORROWED_CONTEXT` 和
  Pipe/`POOL_LANE` 的 REQ/REP 路径。

### Phase 6：XPUB/XSUB

目标：在基础 fan-out/session 原语稳定后支持 subscription-aware proxy device。

- [x] 新增 `XPUB` 和 `XSUB` pattern。
- [x] 新增 `SUBSCRIBE` 和 `UNSUBSCRIBE` control frames。
- [x] SUB/XSUB 发送 subscription control。
- [x] XPUB 向上游 stage 发布或向 XSUB 转发 subscription event。
- [x] subscription state 只由 FMQ peer/session 状态推导。
- [x] XPUB/XSUB 复用 fan-out/session registry，不继承 PUB/SUB 的静态 HELLO-only subscription
  或 sink/source role 限制。

验收：

- [x] XSUB subscription 可传播到 XPUB。
- [x] XPUB 只向 matching subscribers 发布。
- [x] subscription churn 不泄漏 topic state。
- [x] proxy template 通过 FMQ runtime test。

### Phase 7：Persistence composition

目标：通过 queue/storage 组合提供 durable delivery，不把 durable queue 放入 FMQ core。

- [x] 定义 downstream failure 的 requeue contract。
- [x] 提供 memory queue 标准组合。
- [x] 完成 Redis Stream source/sink 的运行时交付与 ACK 闭环；进程内 RESP integration
  覆盖 XADD、XREADGROUP delivery、成功后 XACK 与 downstream failure 不 XACK；真实 Redis
  server 额外覆盖 Data SET/GET、同 consumer pending restart replay、成功后 XACK 与 BLOCK
  interrupt latency。
- [x] 提供 Redis Data 固定 key 的 binary-safe SET sink 与 GET transform；missing key、value
  bound 和配置错误 fail fast，且不混用 Streams ACK 语义。
- [x] 提供 SQLite queue；SQLite row 为 durable 事实源，sink commit 后才成功，source
  claim 后仅在 graph 成功时删除，失败和重启均恢复 pending，语义为显式 at-least-once。
- [x] Queue 以 `.yml` named channel 固化 `backend: memory|sqlite`、`pattern: push_pull`、容量和
  持久化配置；source/sink adapter 只引用同一 channel，非 Queue pattern/backend fail fast。
- [x] 区分 accept ACK 与 delivery ACK：memory enqueue/SQLite commit/Redis XADD 或 SET 成功只表示
  backend 接收；Queue downstream + finalize 和 Redis Stream downstream + XACK 成功才表示交付完成。
- [x] Redis `.yml` adapter 以 `pattern: stream|data` 选择契约；Stream 显式声明
  `role: source|sink`，Data 显式声明 `operation: set|get`，pattern 字段不可交叉混用。
- [x] 提供 PgSQL durable outbox source/sink：固定表是唯一 durable 事实源，sink 以 transaction
  advisory lock 原子执行容量检查与 INSERT，并只在 COMMIT 后上报 `DURABLE`；source 以 session
  advisory row lock 排除并发领取，graph 成功后 DELETE，失败解锁留存并 fail fast，语义为显式
  at-least-once。公开 versioned C ABI、credential-free Status V1 和 `.yml` named outbox channel 已
  固化，adapter 仅声明 channel/role，未知/交叉字段在 I/O 前拒绝。
- [ ] 在真实 PostgreSQL 运行 outbox contract suite，证明 COMMIT-backed `DURABLE`、容量、失败留存、
  restart recovery 与双 source 排他领取；测试已作为 `TURBO_FLOW_PGSQL_LIVE_TESTS` opt-in target
  提供，当前本机 `127.0.0.1:5432` 未监听且无 Docker，不能把未运行写成通过。
- [x] 禁止 borrowed transport context 跨异步边界。

验收：

- [x] downstream failure 可 requeue。
- [x] process restart 后 durable source 可恢复；SQLite restart test 覆盖 committed 与 stale
  in-flight row 恢复。
- [x] 外部副作用失败后的重试或补偿状态明确；SQLite source 仅在 graph 成功后删除 durable
  row，publish/settlement 失败恢复 pending 并停止隐式重试；不可回滚 crash window 明确采用
  at-least-once，要求 consumer 以 idempotency key 或同事务 completion 去重。

### Phase 8：Discovery 与 control plane

目标：通过 graph composition 和 control API 动态管理 endpoint。

- [x] FMQ 通过 adapter owner command 提供幂等 pause/resume endpoint API。
- [x] 提供 replace peer list API；固定 adapter slots 按稳定 peer ID 映射，整批预校验与复制后
  才向 owner 提交命令，失败按逆序回滚且不推进 registry version。
- [x] FMQ connection snapshot 提供只读 endpoint/state/connection/in-flight/last status。
- [x] 实现 discovery source 和 registry version comparison；source 填充 caller-owned、
  pointer-free snapshot，旧版本拒绝、同版本相同数据幂等、同版本冲突拒绝。
- [x] 单 endpoint update 与 peer list replace 均通过显式结构化 owner command 提交；批次在
  全部成功后才更新 controller 事实源。
- [x] active endpoint 更新失败时回滚旧 endpoint，并验证旧 state 恢复 READY。

验收：

- [x] 新 endpoint 被发现后建立连接；FMQ real owner test 验证新 BIND 进入 READY。
- [x] registry 删除 endpoint 后 graceful close；FMQ test 验证 owner quiesce 后进入 STOPPED。
- [x] registry error 不破坏现有连接；fetch error 前后 endpoint/state/version 与 command count
  保持不变。

### Phase 9：配置 profile 与 resolved JSON

目标：在不破坏 adapter 单一事实源的前提下复用运行参数。

人类维护的配置文件统一使用 `.yml`；不提供 TOML 或 human-authored JSON 双轨入口。
resolver 输出 immutable `.resolved.json` 供工具、诊断与 runtime projection 使用。

- [x] 完成 host profile resolver；YAML v1 被解析为 immutable resolved JSON document，profile
  lookup 返回命名 adapter 的只读视图。
- [x] 定义 YAML `connection`、`timer`、`thread`、`coro` named fragments；未引用的 fragment
  也全量校验，禁止坏配置潜伏。
- [x] 将 fragment/profile 引用解析为 concrete adapter resolved config；fragment 与 adapter
  config 字段冲突 fail fast，不定义隐式 override 顺序。
- [x] FMQ resolved-config 注册入口只读取 immutable resolved JSON snapshot，并投影到公开类型化
  config 后复用同一注册与校验路径；其他 adapter 仍由各自验收项跟踪。
- [x] resolver 对未知字段、错误类型、冲突 override 和 unresolved reference fail fast；FMQ
  projection 额外拒绝未知 option、错误类型与非 FMQ adapter kind。
- [x] 配置关闭的 `TURBO_FLOW_BUILD_*` target 不读取对应 adapter config；host 在注册前用 enabled
  kind preflight 扫描 name/kind，disabled kind 在字段 projection 和 flow mutation 前 fail fast。

验收：

- [x] socket/fmq/http/rpc 可复用同一命名配置片段；各模块 resolved projection tests 复用
  `timer.bounded`，connection/thread/coro 仍按 adapter 字段契约选择性消费。
- [x] FMQ 可复用 connection/timer/thread/coro 命名片段，并覆盖 profile projection 与安装示例。
- [x] resolved JSON 的 adapter `config` 记录最终值，`sources` 按字段记录 fragment 或
  adapter.config 来源；runtime 不持有可变 fragment 状态。
- [x] 人工配置只接受 `.yml`，resolver 生成的 JSON 只作为 immutable tool/diagnostic artifact 和
  typed runtime view；公开 typed C config 是 host API，不是第二个文件读取器。
- [x] profile resolver、parser/compiler 和 observe tests 覆盖成功与失败路径；HTTP integration
  从同一 snapshot 注册 client/server、compile graph 并附加 Observe，错误 host-only field 在
  flow mutation 前拒绝。

### Phase 10：Primitive graph resource contract

目标：在不改变现有 graph DSL 和 adapter 行为的前提下，让“谁在干什么、谁管理谁”成为
可由类型、generation 和测试验证的公共契约。

- [x] 盘点 runtime、stage/pool、Disruptor segment、queue/buffer、IO connection、protocol
  aggregate、storage 和 rule set 的 owner 与现有 snapshot/command 能力。
- [x] 建立 Data、Execution、IO/Transport、Protocol/Pattern、Buffer/Persistence、Rules、
  Management 基础 domain catalog，以及 versioned value/resource primitive descriptor。
- [x] 实现 operation descriptor registry：domain/version、input/output type、required
  resource、五维 scope、execution mask 与 source/stage/bridge role；descriptor 不承载 callback。
- [x] DSL 增加 `operation <binding>` 与 `resource <binding>`，host registry 深拷贝 descriptor；
  未显式绑定的 runtime node 解析为可查询的 `core.source` / `core.stage.*` concrete operation，
  不保留跳过 contract validation 的旧路径，DSL 不能自行提升 scope/authority。
- [x] Graph compiler 校验所有 runtime node/edge 的 operation role、resource kind/domain/type、
  owner/pool concurrency、execution policy、management authority 和 input/output domain/type。
- [x] 扩展 operation descriptor 的 handoff、ordering、capacity/backpressure、deadline/cancel、
  error 和 settlement runtime contract；截断旧 descriptor 直接拒绝，只接受完整当前 contract。
- [x] 将当前可执行契约接入既有 runtime：`bounded + block` 映射 worker Disruptor，ordering
  映射 reorder，reject/retry 映射 reject edge 与 adapter retry；公开只读 segment plan。
- [x] 落地 runtime-owned pool resource 的首个管理契约切片：稳定 UID、owner、generation、
  caller-owned typed Status/Condition，以及带 `expected_generation` 的 resize command；start、
  restart、成功 resize 和 rollback rebuild 推进 generation，stale command fail fast。
- [x] 定义 schema-backed resource document：公共 envelope 只保留 domain/kind/UID/generation
  与 schema identity，具体内容使用 owned immutable payload；pool Status 首个输出 DataBind schema
  document，并由真实 DataBind 动态绑定测试验证，core 不新增 DataBind 链接依赖。
- [x] 统一业务消息 content contract：payload bytes 可直接作为 opaque data，或附加独立
  `turbo_flow_data_schema_t` schema-bound projection；projection 是可清除/重建的派生视图，
  Core 通过 provider clone/destroy hooks 管理生命周期且不链接 DataBind。
- [x] 完成业务 payload content descriptor、registry 与 domain lookup 接入：
  - [x] Core：media type、encoding、schema key/version、domain profile 与 payload identity 独立；
    message 只借用 immutable descriptor，clone/move/descriptor-only retain 不复制或释放 descriptor；
    descriptor 与 projection 可跨 domain，但 declared schema identity 必须双向一致。
  - [x] Registry：host 显式 create/destroy，深拷贝 schema identity/projection type，按
    `(domain, profile, media type, schema name/type/version)` 精确解析；拒绝 runtime schema text，
    不使用 singleton，也不根据唯一候选猜测 schema。
  - [x] HTTP：request/response adapter 归一化 Content-Type；unknown 无 binding 时保持 opaque，
    显式 binding 冲突 fail fast。
  - [x] S3：PutObject 使用配置 Content-Type 并在外部副作用前校验 payload；GetObject 先读取
    StatObject/HEAD 的实际 Content-Type，再附加 object profile、bucket/key identity 与可选 binding。
  - [x] Database：PostgreSQL sink 识别/校验 parameter payload；query adapter 根据真实 PGresult
    输出 rowset/command descriptor，并通过显式 mapper 接入可选 row projection。
  - [x] FMQ：DATA 与 SUBSCRIBE/UNSUBSCRIBE 分别附加 data/control profile，topic 作为 request-local
    immutable descriptor identity。
  - [x] MQTT：公开并测试 application/control profile、domain/flag 约束与同一 registry exact
    resolution contract；broker 实现留在外部 `turbo-mqtt` 仓库。
- [x] 在 TurboUtils DataBind 内新增并测试 `data_bind_value_clone()` 深拷贝 API，并由 Codec
  注册 projection clone hook；DataBind projection 的 message clone/retry 获得独立值树，其他
  未提供 clone hook 的 provider 仍明确返回 `TURBO_ENOTSUP`。
- [x] 实现 operation execution deadline：从 callback 进入 `RUNNING` 开始计时；thread/coro/
  Disruptor task 在 yield/query 点协作返回 `TURBO_ETIMEDOUT`，inline/adapter 在 callback
  返回后检查；不强杀 C callback，不把 pool/ring 排队计入 execution deadline。source deadline
  在 adapter 提供 per-message owner contract 前继续 fail fast。
- [x] 实现其余当前 fail-fast 的 runtime contract：worker `fail/drop`、generic
  complete/requeue/dead-letter、protocol ACK settlement 和 `SETTLE` error mode。
- [x] 将稳定 resource identity、kind、generation、owner reference 和 `observed_generation`
  扩展到 runtime、Disruptor segment、queue/buffer、connection、protocol aggregate、storage 和
  rule set；connection/queue 当前进程内 snapshot identity 不能冒充稳定 UID。
- [x] 为各 Domain/resource kind 注册独立 DataBind schema-backed Spec/Status/Condition/Event
  document provider；公共 envelope 使用 `size` 做 ABI 演进，owner-native typed API 只保留热路径
  和强命令边界。Status 保持 owner-native；九种 canonical resource kind 的 common
  Spec/Conditions/Event 使用独立 schema，从同一 metadata/snapshot 派生，Event 固定为 latest
  observation + generation gap 而非伪造 journal，owner document 始终优先。
- [x] 扩展 owner command dispatcher，校验 expected generation、生命周期、deadline 和
  idempotency key；adapter/pool command 共用 dispatcher，冲突 fail fast，不静默覆盖新状态。
- [x] 实现通用 host-owned reconcile helper：比较 caller-owned metadata/value/Conditions；收敛或
  observed generation 未追平时不发令，否则每 tick 最多经统一 dispatcher 发一条 typed command。
  Observe pool policy 复用该 helper，不创建隐藏 controller thread。
- [x] 定义 caller-owned 多 owner resize workflow，按 `quiesce ingress -> drain graph -> resize
  pool -> resume graph -> resume ingress` 每 tick 推进一步；失败记录精确 phase、owner 最终有效
  generation 和幂等 attempt，显式 retry，不复制 payload、不伪造跨 owner 原子事务。

验收：

- [x] Observe/read_field 不能推进资源状态，snapshot mutation 测试可检测违规。
- [x] HTTP、S3 与 Database 使用不同 schema/type 读取 Domain-specific Status；未知 schema/version、
  缺字段、错误类型、敏感字段和超限 document fail fast，不能退化为无约束字符串 map。
- [x] 重复 command 幂等；stale generation 被拒绝；失败后 owner 保持最后一个有效状态。
- [x] graph、FMQ、HTTP/S3/PostgreSQL、Queue、Storage 与 Rule Set 已迁移到新 resource API；
  旧 adapter-bound resource API、pointer identity 和固定枚举顺序已删除且零符号匹配。

### Phase 11：Disruptor segment 与 settlement contract

目标：让 `disruptor.h` 成为 graph 并发数据纽带，同时保持 bounded、ownership、ordering、
completion 和 backpressure 可证明。

- [x] 定义统一 entry header：runtime generation、stage/edge identity、ownership、ordering
  key/sequence、deadline/cancel 和 completion handle。
- [x] 保持 direct inline chain 不建立物理 ring；worker-pool stage 使用现有 bounded Disruptor，
  direct/fan-out/fan-in segment 作为只读 lowering metadata 暴露。
- [x] 定义每个 segment 的 capacity 与 `block/fail/explicit drop` policy；Disruptor 满载不得
  被描述为 durable queue。
- [x] 定义 Settlement result：success、retryable failure、requeue、dead-letter、canceled 和
  protocol-owner settlement command。
- [x] 对异步边界强制 owned route token；拒绝 borrowed `transport_context` 跨 segment。
- [x] 统一 stop：close admission、interrupt wait、deadline drain、terminal completion，覆盖
  producer/consumer/resize race。

验收：

- [x] ownership、满载 backpressure、按 key 顺序、unordered fan-in 拒绝和 completion 错误
  传播均有 focused tests。
- [x] 与当前 inline/worker 实现做吞吐及 P50/P95/P99 对比；无 profile/benchmark 证据时
  不替换已有稳定路径。

### Phase 12：TurboFlow Policy 与透明控制

目标：让数据规则和资源控制条件使用同一 typed facts 基础，但保持执行权限和副作用边界
分离。

- [x] 定义有版本的 rule program/action ABI，以及 instruction、time、memory 和 output quotas。
- [x] data rule 只返回 mutate-private/route/drop/batch-key/retry-class/dead-letter 等 data
  actions，由 graph runtime 验证并执行。
- [x] control rule 只返回 typed command proposal，由 host 做授权、generation 校验和 owner
  command 调用。
- [x] snapshot、Condition、command result 和 bounded event 可由 exporter 查询；业务 payload
  默认不进入日志、metrics 或 control event。
- [x] 提供 Prometheus/OpenTelemetry exporter adapter，保持 core/resource snapshot ABI 不依赖
  exporter SDK。

验收：

- [x] 固定 facts snapshot 的规则结果确定；未知字段、类型错误、配额超限和未授权 action
  fail fast。
- [x] 规则无法直接持有 adapter/session 指针、调用 IO、ACK 协议消息或修改 Status。

### Phase 13：Flowie MQTT application

目标：新建 Flowie 应用；只迁移、重命名 TurboMQTT 的 parser 与必要协议基础实现，以
TurboFlow primitive 和 CoroNet 重建 connection、session、processor、sink 与 persistence，
验证 protocol owner、data plane 与 settlement 的完整分层。

- [x] 建立独立 `Flowie::Protocol` SDK，使用 re2c/Lemon 解析 packet envelope，并提供
  MQTT UTF-8、普通/共享 topic filter 与无插件依赖 ACL line parser。
- [x] 禁止导入 TurboMQTT I/O、queue、processor、sink、worker 与 plugin runtime；迁移来源、
  备选方案、状态归属、回滚和验证门记录于 `flowie/ARCHITECTURE.md`。
- [x] 迁移并重构 CONNECT/PUBLISH/SUBSCRIBE/MQTT 5 properties 的 bounded typed packet
  decoder/iterator；使用 `flowie_mqtt_` namespace、borrowed byte-span 和 packet-context property
  validation，不暴露 TurboMQTT 类型或分配型 property 链表。
- [x] 补齐 ACK/UNSUBSCRIBE/DISCONNECT/AUTH typed decoder 与必要 packet encoder；encoder 只写入
  caller-owned bounded buffer，不发送网络数据。
- [x] 实现 PUBLISH protocol/data bridge 和 MQTT Topic Filter SecurityRealm matcher；bridge 只产生
  copied metadata、generation-checked route 与 borrowed payload view，不生成 ACK。
- [x] 实现 private、single-CoroNet-lane session owner 当前闭环：CONNECT 身份/expiry/generation、
  bounded subscription/inflight、SUBSCRIBE 全包原子更新、QoS 1/2 settlement gate，以及不执行 IO 的
  PUBACK/PUBREC/PUBCOMP intent；持久 QoS 2 release state 跨 reconnect 保留，未 settlement 的 graph
  attempt 在断线时清除并等待显式 redelivery。
- [x] session owner 保持 endpoint-private，不伪装成 transport `Connection` resource；公开管理只由
  endpoint `ProtocolAggregate` 聚合 session/subscription/inflight/retained 状态，graph、Queue 和 sink
  均不能独立推进 MQTT session。
- [x] 复用 shared bounded stream primitive 实现 private connection ingress：每条 MQTT connection
  的 socket、framing、parser、close 和 backpressure 由同一 CoroNet lane owner 串行推进；半包不
  发布、粘包逐包发布。完整 wire packet 在消费 framing view 前复制为 owned message，并只向配置的
  TurboFlow source admission 一次；只有完整 owned message 才能按 graph stage 配置进入 worker
  Disruptor，raw stream bytes 不跨 lane。
- [x] 实现 bidirectional `flowie_endpoint` adapter primitive：直接持有 CoroNet
  TCP/TLS/WS/WSS/Pipe listener、accepted connection lane、framing/parser、连接限额与
  stop/drain；inbound source 不把未完成 framing bytes 注册成 Queue，outbound reply sink 只在
  完整 encoded control packet + message-owned route admission 后进入真实 bounded Queue。
- [x] 在 connection owner 强制 CONNECT 首包并由 protocol level 锁定 MQTT 3.1.1/5；非 CONNECT
  首包和第二个 CONNECT 都是 terminal protocol error，版本状态不跨 lane 或下放给 graph 猜测。
- [x] 为 `flowie_endpoint` 实现 strict typed YAML projection/registration；unknown field、wrong
  type、wrong kind 和 endpoint 约束均在注册前 fail fast，配置文件使用 `.yml`。
- [x] 实现 bounded CONNACK/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/PINGRESP codec，并将 session
  settlement ACK intent 映射为纯 encoder command。
- [x] 为 `flowie_endpoint` 实现 generation-aware reply/ACK sink path：owned MQTT route 可跨
  worker，owner lane 用 TurboUtils hash map 查找 connection，经 `send_hwm_bytes` bounded Queue
  顺序发送；HWM 失败只关闭对应 connection。
- [x] 以显式 `manage_sessions` 模式将 CONNECT 绑定到 endpoint-owned session registry，
  固化 no-auth CONNACK/session-present/duplicate-active/quota/close-after-reply policy，并在同一
  CoroNet lane 将 provisional connection route 原子重绑为 generation-fenced session route；
  session-aware endpoint config 曾固化为 ABI v2；settlement policy 继续显式升级为 ABI v3，
  不把新增字段伪装成旧布局。
- [x] 在 managed session ingress 复用 typed parser、session owner 与 bounded reply Queue：
  SUBSCRIBE/UNSUBSCRIBE 原子更新 session 并返回 SUBACK/UNSUBACK，PINGREQ 返回 PINGRESP，
  PUBREL generation-check 后返回 PUBCOMP，no-auth AUTH 返回 0x8C DISCONNECT；这些控制包不进入
  application graph。
- [x] 将 QoS 0/1/2 PUBLISH 交给现有 `publish_begin`/RECEIVED settlement gate；QoS ACK 在 owner
  lane 生成，只有 admit_graph 的完整 owned PUBLISH 进入配置的 TurboFlow stage，并以
  `worker 1 capacity 8` 真实验证跨 lane Disruptor 数据处理与 QoS2 duplicate 抑制。
- [x] 将同一个 bidirectional endpoint 作为显式 graph sink 实现 subscription fan-out：worker
  只提交完整 owned PUBLISH command，CoroNet owner lane 在 session 主事实提交后增量维护派生 selector，
  startup/clean-start/失效修复才从唯一事实源原子重建，
  普通重叠订阅按 session 合并，共享订阅按 exact share-filter 独立 round-robin，`no_local` 与
  UNSUBSCRIBE 生效；无匹配是成功投递语义。出站 QoS 使用 broker-owned packet-id/inflight，真实
  TCP 测试覆盖 QoS1 PUBACK、QoS2 PUBREC/PUBREL/PUBCOMP 及 persistent reconnect DUP replay。
- [x] subscription selector 使用 topic-level trie 裁剪 exact/`+`/`#` 候选，并以 croaring 64-bit
  bitmap + typed member hash 完成普通重叠订阅去重、共享 rank selection 与 `$SYS` root wildcard
  隔离；filter hash + stable entry slot 支持单 filter 增量 add/update/remove，trie terminal binding
  支持 O(1) bucket 删除、空分支裁剪和 removed slot 复用，且无关订阅变更不重置 shared cursor；
  session owner 仍是唯一事实源，trie/bitmap 只是可原子修复的派生层。
- [x] 以 exact 100k live TCP、完整 endpoint selector/member 状态和真实 packet fan-out 证明功能部署容量：
  Windows/MSVC Release workload 使用 1 publisher + 100,000 MQTT 5 subscriber，完成
  CONNECT/SUBSCRIBE、Connection/ProtocolAggregate snapshot 和逐连接 byte-for-byte QoS 0 packet
  fan-out，最终 `connected=100000`、`delivered=100000`、`status=0`。Endpoint ABI v7 从
  `max_connections` 推导 private CoroNet pool capacity，并以严格 YAML `coroutine_stack_size` 和
  `recv_buffer_size` 配置 private-context capacity；capacity workload 使用 32 KiB stack 与两块
  4 KiB receive chunk，固定主项约 40 KiB/connection。实测 50k/100k 峰值 private commit 约
  3.07/5.93 GiB。该 gate 证明
  reference host 的并发功能容量，不是可移植 SLA；该 live-TCP setup 数据早于增量 selector 路径，
  混合了串行 TCP/MQTT handshake、owner 创建、订阅提交和驱动开销，不作为当前 mutation throughput。
- [x] 增加可复算的 100k 内部容量基线：同时持有 100k session owners，并构建/匹配含 exact、`+`、
  `#`、shared filter 的 100k 派生 trie，输出 create/CONNECT、build、match throughput 与
  P50/P95/P99；Release benchmark 进一步覆盖 16 个真实 MQTT 5 TCP subscriber 的 1000 次 fan-out，
  500 次 TCP + CONNECT/CONNACK/close/owner cleanup churn、8 次完整 100k wildcard/shared rebuild、
  100k unique-filter bound removal/空分支裁剪、256 次单次返回 100k candidates 的匹配，以及
  1 KiB receive-buffer 慢订阅者的重复 HWM 隔离。
  该内部基线本身不替代 live TCP 证据；它与上一项 exact 100k 网络契约共同覆盖索引成本、连接内存
  和完整 MQTT packet fan-out。
- [x] 消除 endpoint-wide socket-send HOL：全局 Queue 只负责跨线程 command handoff/route validation，
  每条 connection 使用独立 `send_hwm_bytes` budget、TurboUtils deque，并由既有 connection owner
  coroutine 在 receive wake 后执行单一 FIFO drain，不为 fan-out 临时 spawn 第二 coroutine；endpoint
  aggregate budget 以 `send_hwm_bytes * max_connections` checked capacity 约束总量。单 subscriber 的
  send HWM 或 MQTT outbound-inflight 超限只关闭该连接并释放其未发送队列，其他匹配者继续接收；
  drain lifetime pin、stop interrupt/wait、QoS2 reconnect replay 与真实快/慢订阅者回归均已覆盖。
  公开 endpoint ABI v7/YAML 以 `slow_subscriber_policy: disconnect` 固化该策略；Queue Status schema v2
  报告 per-connection HWM、策略枚举和饱和 subscriber 隔离计数，单 peer 超限不把 endpoint 标记为
  saturated，也不关闭健康连接 admission。
- [x] 在 CoroNet owner lane 实现 process-local session expiry scheduler：disconnect 按 monotonic
  deadline + session generation arm，reconnect 取消旧 deadline，过期删除同步移除 selector member，
  已失效 selector 留待下一次 fan-out 原子修复；
  MQTT 5 DISCONNECT Session Expiry Interval override 先由 session owner 校验并更新。
- [x] 将 CONNECT/PUBLISH/SUBSCRIBE 绑定到显式注入的 TurboFlow authentication provider 与 YAML
  `security_realm` decision：realm/auth method 必须与 typed binding 精确匹配；凭据仅在 CONNECT
  callback 期间借用，session 只复制 principal；MQTT Topic Filter ACL 以 filter containment 授权，
  deny 不修改 session、不进入 graph，真实 TCP 覆盖 `0x86/0x87`。
- [x] 新增 protocol-neutral `turbo_flow_record_store_t`：namespaced provider 暴露 bounded scan、
  per-key revision CAS 与 all-or-none batch commit；SQLite transaction 和 Redis Hash + Lua 均实现
  durable ACK，YAML `kind: record_store` projection 与真实 Redis/SQLite restart tests 覆盖冲突、
  容量、重复 key 和无 volatile fallback。单-key blob store 不用于全 broker 热路径。
- [x] session owner 提供 canonical versioned LTV record codec 与 deep clone：只保存 subscription、
  已完成 PUBREC 的 inbound QoS2 release、已提交的 outbound QoS delivery；restore 强制 inactive，
  使用新 owner instance，不恢复 route、credential、reserved delivery 或未 settlement graph attempt。
- [x] 通过 additive `flowie_endpoint_bindings_t` 将 managed session owner 接入 durable record
  store：resolved YAML `session_store` 必须与注入 channel 精确匹配；启动先 scan/校验 LTV、清理
  过期记录、恢复 inactive owner 与 selector。CONNECT/SUBSCRIBE/UNSUBSCRIBE、双向 QoS、fan-out
  delivery、disconnect/close 采用 clone -> revision CAS commit -> swap，ACK/send 不越过 commit。
  真实 SQLite endpoint recreation 覆盖 subscription restore 与未确认 QoS1 的 DUP replay；binary
  MQTT packet clone 使用 `tstr_clone`，不再被 NUL 截断。
- [x] endpoint owner lane 提供有界 exact-topic retained fact source：RETAIN=1 原子替换，零 payload
  删除，Message Expiry 到期清理；SUBSCRIBE 按 RH=0/1/2 与订阅前态重放，shared subscription 不
  重放，SUBACK 在线序上先于 retained PUBLISH。重放复用 broker-owned packet ID、session delivery、
  durable session CAS 与 bounded reply Queue，不建立第二套 processor/queue runtime。公开 ABI v4/YAML
  以 `max_retained_messages` 独立限额；显式 `session_store` 使用保留二进制 key 前缀和 versioned `FRET`
  LTV record，在 owner 状态切换前完成 PUT/replace/delete CAS，启动时恢复并 CAS 删除过期记录。
  SQLite endpoint recreation 覆盖恢复、删除和 Message Expiry；真实 Redis suite 覆盖 binary key、CAS、
  scan/recreation contract。未配置 store 时 retained 明确保持 process-local。
- [x] 提供首个 `flowie_server` 产品 host：从 YAML profile 解析 endpoint、Queue source/sink、
  `rule_set` channel 与 socket output，创建并复用同一个有界 Queue primitive，注册既有
  Flowie/TurboFlow adapter、MQTT facts provider 与 `rules.apply` operation，编译独立 `.flow` graph，并按
  start/signal/stop 顺序管理生命周期；`--check` 在绑定 listener 前执行同一套配置、primitive 与 graph
  预检。host 现按 `record_store.backend` 显式创建 SQLite/Redis session store，并以 borrowed binding
  注入 endpoint；provider 不匹配先返回 `TURBO_ENOTSUP`，同 provider 未知字段、连接/scan/record 错误
  均 fail fast，不回退到 memory。普通/SQLite `--check`、真实 Redis `--check`、SQLite endpoint recreation
  与真实 Redis record-store contract suite 分别覆盖产品装配与持久化契约。security binding 和未链接
  adapter kind 仍显式拒绝；当前 host 尚未自动装配 security provider。
- [x] MQTT owner 已独占 session、subscription、双向 QoS inflight、ACK、reconnect、expiry、retained
  与完整 Will 状态。CONNECT 深拷贝有界 Will，正常 DISCONNECT 抑制，异常关闭和 `0x04` 触发；deadline
  取 Will Delay 与 session expiry 较早者，同 client-id 重连取消 pending Will。生成的 owned PUBLISH 复用
  TurboFlow graph、pointer-free route 和 endpoint bounded owner Queue；graph/store 失败保留 pending 并
  重试。canonical session record 同时覆盖 SQLite/Redis 重建，真实 TCP、SQLite endpoint recreation 与
  live Redis suite 分别验证正常/异常、delay/expiry、重连取消和剩余 deadline 恢复；不宣称 exactly-once。
- [x] 将 FMQ PUB/SUB、PUSH/PULL、REQ/REP、ROUTER/DEALER 的 role compatibility、
  fan-out/round-robin candidate iteration、generation-fenced route matching 与单 correlation
  同步 exchange 抽入 `turbo_flow_protocol` 的 protocol-neutral pattern core；core 不拥有 peer、
  topic、payload、queue、I/O 或 ACK。FMQ v2 wire 仅做角色映射，Flowie 复用 selector/route
  primitive 而不依赖 FMQ frame/enum；protocol、Flowie endpoint 与完整 FMQ 网络 suite 覆盖该边界。
- [x] 将 QoS 1/2 `accepted` 绑定到现有 memory Queue 的 bounded enqueue commit：Queue 获取完整
  owned MQTT packet 后消费 one-shot settlement envelope，经 generation-fenced route 将 command
  投递到 Flowie bounded reply Queue，session/ACK/socket 仍只由 CoroNet owner lane 推进。真实 TCP
  测试使用同一份 YAML 创建 endpoint、Queue sink/source 和 graph，验证 PUBACK、accept/delivery
  counters、route 保留及下游重发布；worker ring 和任意 stage success 不冒充 ACCEPTED。
- [x] 将 QoS 1/2 `durable` message settlement 绑定到 SQLite Queue COMMIT 与 Redis Stream XADD
  成功回复；primitive 在 commit 后经 live route 投递 owner command，SQLite 持久化前剥离
  process-local route/envelope，Redis 只写 payload。真实 TCP + YAML 测试分别验证 PUBACK 与
  SQLite Queue recreation/Redis consumer-group replay。该能力只证明 message persistence；Flowie
  session/subscription/QoS/retained/Will owner restore 由独立 record store binding 负责，且不宣称
  exactly-once。
- [x] 将 Policy route/transform、Queue durable boundary 和现有 IO output 组合到 MQTT
  PUBLISH graph：ingress 在 Queue 可序列化的 private message flags 中保存 MQTT version + fixed
  header flags；Queue source 后的 Flowie facts provider 复用 typed parser，按 schema 提供
  `mqtt.topic/payload/payload_size/qos/retain/duplicate/packet_id/version`，不把 projection、live
  route 或 parser owner 写进持久化记录。真实 TCP 回归覆盖 memory Queue 后按 topic 选择 endpoint
  fan-out 或 `io/socket` output，并由 Policy 同时 mutate private status；SQLite Queue recreation
  回归覆盖 durable row -> Policy -> TCP socket output，完整 owned wire packet 字节保持不变。
- [x] 将上述 composition 装配进 `flowie_server` 的 strict resolved YAML profile：RuleSet 作为
  `channels.<name>.kind: rule_set` resource，profile 通过 channel reference 显式选定；MQTT facts
  provider、`rules.apply` 与 socket output 在 `--check` 时完成创建、注册和 graph route 校验，未知字段、
  错误 action、未链接 adapter kind 均 fail fast。
- [x] MQTT endpoint 的 Connection、bounded reply Queue 与 ProtocolAggregate snapshot 已复用
  TurboFlow canonical governance schema 生成 Spec、Condition、Event；回归逐一校验 schema identity、
  document payload，并确认不暴露 credential、client id、topic 或 MQTT payload。
- [x] 删除错误的独立 session `PROTOCOL_PATTERN + CONNECTION` 治理注册；session owner 继续作为
  endpoint-private 主事实源，公开管理统一由 `ProtocolAggregate` 表达，不引入第二个可写 session 资源。
- [x] 为 MQTT endpoint `ProtocolAggregate` 提供 owner-native typed Status/Conditions/Event 与 owner
  Command；QUIESCE/RESUME 在 CoroNet owner lane 只关闭/恢复新 MQTT 会话 admission，OS listener 保持
  bind，既有 connection/session 继续收发。真实 TCP 回归覆盖 generation/idempotency、quiesced 新连接
  拒绝、既有 PINGRESP、RESUME 后新 CONNECT 以及文档不泄漏 client/topic/payload。

验收：

- [x] QoS ACK 时点、duplicate delivery、session generation、retained/Will 和重连恢复通过
  owner、真实 TCP、SQLite endpoint recreation 与 live Redis protocol integration tests。
- [x] 单独固化“进入接收边界后 ACK”的兼容性、迁移和回滚：省略字段或显式 `received` 仍先提交
  session transition 并将 ACK 放入 connection-owned bounded reply Queue；graph 随后失败时先排空该
  已提交 ACK 再关闭连接。`processed` graph 失败不 ACK；`accepted`/`durable` 仍严格依赖对应 primitive
  commit 且不 fallback。真实 TCP 正反例覆盖失败时序，YAML 文档要求 quiesce/drain 后将 policy 与
  graph 一起停机迁移或回滚，当前不支持热重载。

### Phase 14：ZGuide Chapter 3/4/5/7/8 pattern framework

目标：以 FMQ v2 数据平面和 TurboFlow graph 为基础，按依赖顺序实现高级 pattern；借鉴
ZeroMQ Guide 的协议与故障模型，但不追求 ZeroMQ wire/API compatibility。

实施顺序与责任边界：

1. Chapter 3 advanced request-reply：reply envelope、worker registry、LRU load balancer、
   async request correlation 和 inter-broker 所需的逻辑地址。
2. Chapter 4 reliable request-reply：worker lease/heartbeat、重派策略、broker accept ACK、
   worker delivery ACK、at-most-once/at-least-once 与 durable request owner。
3. Chapter 5 advanced pub-sub：Espresso capture、slow subscriber policy、last-value cache、
   snapshot + ordered update 的 Clone contract。
4. Chapter 7 architecture：reactor/actor 边界、跨线程控制命令、可观测协议和 failure domain。
5. Chapter 8 distributed framework：network/service discovery、presence、peer channel manager、
   group membership、distributed logging/monitoring。

Chapter 3 当前切片：

- [x] 新增 bounded、host-serialized `turbo_flow_fmq_broker_t` 状态 owner；worker/service 名称、
  worker route 和 client return route 均深拷贝，内部不持有 socket/peer/message pointer。
- [x] worker READY 注册 live FMQ route；同 worker/service 的 idle refresh 幂等，busy refresh、
  identity/service 冲突和非 FMQ route fail fast。
- [x] 实现按 service 的 LRU idle worker 选择、唯一 request ID、in-flight 关联、reply completion
  和 send failure cancel；状态更新在 vector push 成功后提交，不产生半完成 dispatch。
- [x] worker identity 与 request ID 使用预留容量的 TurboUtils fixed-key hash map 索引，平均
  O(1) lookup；vector 只拥有连续 worker/in-flight 值，swap-remove 同步修正索引。
- [x] 使用 resolved YAML `channels.<name>` 的 `kind: fmq_pattern`、
  `pattern: load_balancer` 配置 capacity/scheduler；未知字段和 unsupported value fail fast。
- [x] focused TinyTest 覆盖 LRU 分配、service isolation、错误 worker reply、route 返回、cancel、
  busy remove、容量和 YAML validation。
- [x] 增加真实 TCP broker integration：单 ROUTER 接受 DEALER client/worker，READY 注册 worker
  route，request 按 LRU route 转发，reply 使用 broker 保留的 client route 返回；request ID 作为
  应用协议字段，不依赖任一 transport session 的 message ID。双 ROUTER frontend/backend 是
  同一状态机的部署拓扑，不另建路由事实源。
- [x] 定义 versioned `TFBR` inter-broker logical address envelope，以 origin broker ID、client ID
  和 request ID 表达可转发/可持久化返回地址；显式大端编码、严格长度/保留位校验，且不把
  process-local protocol route 编码上网或写入 Redis/SQLite。

Chapter 4 当前切片：

- [x] broker config 增加显式 API version，以及 `none`、`at_most_once`、`at_least_once`
  reliability；普通 `load_balancer` 与 `reliable_request` 的 YAML 字段组合启动时严格校验。
- [x] reliable worker 由调用方单调时钟驱动 lease；READY/heartbeat 拒绝时间倒退，busy worker
  只允许同 route heartbeat，idle worker 才能更新重连后的 live route。
- [x] reliable dispatch 排除 stale worker；过期扫描每次只提交一个状态迁移，并显式返回
  `EXPIRED_IDLE`、`EXPIRED_DROP` 或 `EXPIRED_REQUEUE`，不在状态 owner 内隐藏 payload 队列或重发。
- [x] TinyTest 覆盖 lease 边界、busy heartbeat、时间倒退、stale exclusion、at-most-once drop、
  at-least-once requeue、client route 返回、计数器及 YAML validation。
- [x] 复用 Queue memory/SQLite 与 Redis Stream 作为 durable request owner；新增显式
  `record_accept_commit`，只有 owner commit 成功后由 host 调用并生成 `ACK_ACCEPT`。broker 仅保存
  correlation metadata，不复制 payload 持久化实现。
- [x] 将 worker reply completion 映射为独立 `ACK_WORKER_COMPLETION`；heartbeat、transport send
  success 与这两种 ACK 均无等价关系，snapshot 分开统计。
- [x] 以 `(origin broker ID, client ID, request ID)` 定义跨 broker duplicate key；新增 bounded
  memory retry/dedup ledger，显式区分 pending/inflight/completed/poisoned，配置 terminal TTL、
  max attempts 和容量，且 restored inflight 不隐式重派。该 ledger 可从 pointer-free record 恢复，
  但在下一项 durable owner 接入前不宣称 restart-safe。
- [x] 为 memory/SQLite Queue 与 Redis Stream 增加显式异步 claim token；worker completion 才
  ack，lease expiry/send failure 显式 requeue。FMQ 集成测试覆盖 Queue claim 跨 delayed reply、
  expiry 后第二 worker 重派；SQLite 覆盖 durable requeue/reopen 与 stale in-flight crash recovery，
  live Redis 覆盖 PEL requeue、同 consumer 重领、严格 XACK 和 restart pending replay。

Chapter 5 当前切片：

- [x] 复用 XPUB/XSUB 已有 subscription/unsubscription 控制帧、per-peer 引用计数、断线撤销、
  reconnect replay 和 graph source 作为 Espresso capture 边界；PUB/XPUB 仍按 byte-prefix fan-out，
  无匹配返回 `TURBO_ENOTCONN`，新建连接不等于已订阅。
- [x] 新增 bounded、host-serialized `turbo_flow_fmq_pubsub_state_t`，以 exact topic 为 key 只保存
  latest value，并把 PUT/DELETE 同时写入严格递增 sequence 的有界 journal；state bytes、topic 数、
  update 数和 update bytes 分别配置，失败不消费 sequence。
- [x] snapshot 深拷贝目标 prefix subtree 并固定 barrier；update cursor 读取
  `(barrier, upper_bound]` 的有序更新，journal gap 明确返回 `TURBO_ERANGE` 要求重新 snapshot，
  不静默生成不完整 Clone 状态。不存在 key 的 DELETE 也保留 tombstone。
- [x] 使用 resolved YAML `pattern: pubsub_state` 配置 `max_topics`、`max_state_bytes`、
  `update_capacity`、`max_update_bytes`；broker-only/unknown/zero 字段 fail fast，并补 focused
  TinyTest 覆盖 latest replacement、prefix、barrier、tombstone、journal gap、配额与 YAML。
- [x] 将同一 state owner 组合进 XPUB/XSUB proxy live graph 与独立 REQ/REP recovery graph；新增
  pointer-free、network-order `TFPS/1` 应用协议及 CAPABILITIES/SNAPSHOT/UPDATES/LIVE_UPDATE，
  snapshot barrier、固定 upper bound 分页、跨连接 sequence、terminal protocol error、stale cursor
  重新 snapshot 和 hard reply bound 均有 focused TinyTest 与真实 TCP XPUB/XSUB + REQ/REP E2E。
- [x] 将慢订阅者策略从 adapter aggregate frame HWM 细化为 bounded per-peer queue：共享编码帧只
  保存一次，peer queue 保存引用，并分别支持 fail/drop-oldest/disconnect；fan-out admission ACK、
  peer write 和 delivery ACK 保持分层。启用该能力时要求非空且 live-unique subscriber identity，
  slow/HWM/drop/disconnect 事件携带该 identity；YAML/schema 固化 message/byte HWM 与 policy 的完整
  组合校验。真实 TCP 测试覆盖 fast/slow subscriber、峰值与持续过载、原子 fail、单 peer 隔离及
  恢复发布。未 profiling 前保留当前可读的 byte-prefix scan，不引入 trie/hash + CRoaring。

Chapter 7 当前切片：

- [x] 在共用 CoroNet execution placement 上增加 bounded actor mailbox；命令 bytes 按值复制、
  command ID 单调递增、绝对 deadline 在 handler 前检查，容量满、关闭和超长分别返回
  `TURBO_ENOSPC`、`TURBO_ESHUTDOWN`、`TURBO_EMSGSIZE`，不借用 caller stack。
- [x] 异步 submit 返回引用计数 reply handle，支持 poll/有界 wait；wait timeout 不取消已开始的
  owner mutation，调用方可继续 poll。同步 wrapper 只在 deadline 前等待 start，一旦开始则等待
  terminal reply，避免返回超时后仍发生不明确的状态迁移。
- [x] FMQ bind/connect scheduling/close/peer-clear 已改为 pointer-free reactor command，并只在绑定的
  CoroNet owner lane 执行；host 仍编排 quiesce/resume/replace，数据 send queue 与 control mailbox
  物理分离，未改变 FMQ v2 frame、pattern 或 REQ/REP 契约。
- [x] TinyTest 覆盖 copy ownership、capacity、queued deadline、command ID、close/drain、异步 delayed
  reply、同步 terminal status；完整 FMQ network suite 验证 private/borrowed/pool-lane 既有路径。
- [x] 实现远端 Flow Control V1：作为普通 FMQ DATA payload 运行在独立管理 flow 的严格同步
  REQ/REP 上，使用 canonical network-order typed request/reply、目标名、request ID、有界幂等历史和
  完整 runtime snapshot；协议错误与 owner command status 作为两层 ACK，均生成终态 REP。YAML
  `kind: fmq_control` 固化 protocol version、target、request 上限和 history 容量；transport 继续由
  FMQ adapter 的 CoroNet fragment 正交选择，不改变 FMQ v2 frame。
- [x] 固化 `flowmq/MANAGEMENT_PROTOCOL.md` 设计基线：定义 capability/version、failure-domain
  identity、strict REQ/REP RPC、typed command/operation、volatile/durable 两类受理 ACK、独立
  event PUB/SUB、snapshot/gap recovery、storage ownership、YAML schema、迁移/回滚和准入测试；
  保持 Control V1 wire/API/YAML 不变。
- [x] 在 TurboUtils harden LTV canonical/overflow contract，并通过已安装 `TurboUtils::Parser` 的
  `turbo_ltv_*` 公开 API 暴露：拒绝 overlong/overflow varint，builder 在 oversized/invalid 输入时
  不产生 partial wire，并固化 stream zero-copy view 生命周期；不单独 export/install LtvParser。
- [x] 实现 fixed 40-byte TFMP header + canonical LTV 基础 codec、zero-copy field iterator、caller-buffer
  builder 与 golden/malformed/limit tests；不改变 FMQ v2 或 Control V1 wire。
- [x] 实现 v1 typed field schema：覆盖全部 RPC kind、公共 response identity、nested
  target/resource/condition/event、command payload、required/repeated/critical 与 4 层 nesting；
  未知 optional 跳过，未知 critical/command fail fast。
- [x] 实现 TFMP phase-1 单线程 management owner：启动时通过 TurboUtils UUIDv7 生成
  incarnation，提供 `CAPABILITIES_GET`/`HEALTH_GET`、显式 lifecycle、稳定错误映射与 malformed
  终态 REP；只读阶段不虚报 command capability，无法恢复原 kind 时使用 response-only
  `PROTOCOL_ERROR`。
- [x] 为 phase-1 owner 注入一个 caller-owned Flow target，实现 `TARGET_LIST/GET`、
  `RESOURCE_LIST/GET/DOCUMENT_GET`；resource list 使用每请求 UID 排序快照与 generation cursor，
  document payload 在响应编码后立即释放，owner 不持有 resource 状态。
- [x] 实现同步 Flow management command：只声明 `PAUSE/RESUME/DRAIN + WAIT_TERMINAL + VOLATILE`，
  mutation 前检查 expected generation，终态改变推进 target generation；有界 memory dedup 先保留
  canonical request bytes，再执行 mutation，支持 exact replay、bytes conflict、terminal TTL 与
  capacity fail-fast。同步 command 不虚报 durable ACK；异步与 durable capability 由后续独立
  operation owner/store 项声明。
- [x] 将四种同步 resource command 适配到既有 typed resource owner：resource UID 与 generation
  仍是唯一事实源，endpoint/pool payload 使用 versioned schema，pool kind 使用独立稳定 wire enum；
  stale generation、错误 kind/payload 和容量错误均在副作用前失败，不复制 resource 状态。
- [x] 实现有界 memory operation owner：`ACCEPT_OPERATION + VOLATILE` 在 mutation 前 ACK，显式
  owner-lane claim、单调 revision、GET、accepted-state cancel、queue deadline、terminal replay；
  不把 memory acceptance 宣称为 durable。
- [x] 实现 strict `kind: fmq_management` YAML channel 与 thin REP stage：校验专用 FMQ REP
  reference、容量/TTL/inflight/协议边界，拒绝 raw UDP、未知字段和未启用的 store；Pipe REQ/REP
  端到端覆盖 CoroNet transport，stage 不使用 raw socket、不另建状态 owner。
- [x] 实现 SQLite/Redis durable operation store 与 live event channel：YAML `blob_store`
  channel 经 provider factory 显式注入；durable accept/cancel 在成功 REP 前提交，RUNNING crash
  不自动重放；event 使用 content-derived topic 与有界 incarnation/sequence replay。全路径不编码
  process-local pointer/route、不放宽 REQ/REP 同步状态机，也不将 provider 失败降级为 memory。
- [x] 实现外部 durable event journal/outbox、snapshot barrier 持久化与 Redis live crash-injection
  contract suite：`TFMS/1.1` 在 operation store 的同一次 atomic replace 中保存 incarnation、最新
  sequence 和有界 event records；`event_replay_store` 必须与外部 `operation_store` 同源，split store
  启动失败。真实 Redis suite 覆盖 binary Data、blob restart/outage 不降级、Stream pending replay 与
  explicit ACK。
- [x] 实现 nonblocking bulk transfer 的 credit pattern：ROUTER/DEALER 上的 TFCW/1 使用
  canonical LTV，`credit_worker` owner 管理 message+byte 增量 token、route-generation reset、严格
  sequence、LRU 与 bounded multi-inflight；无 credit 返回 `TURBO_FLOW_FMQ_EAGAIN`，完成/取消不自动
  返还 credit，且不把 HWM 或 transport send success 当作两类 ACK。严格 YAML 区分 volatile
  `at_most_once` 与显式 storage-bound `at_least_once`；真实 ROUTER/DEALER E2E 覆盖 READY、两个并行
  JOB 与独立 COMPLETE。
  新增 pattern.fmq.credit module、FmqCreditWorker resource 与
  fmq.credit.control/dispatch/complete/worker_input inline typed operations；易失性 graph
  原位替换 client/worker route，control message 在成功更新 owner 后 drop。durable graph 在通用
  message-owned claim projection 完成前 fail fast，不从 message ID 合成 token，也不降级为 volatile。
  worker reconnect fault injection 保留旧 in-flight 为事实源：新 TCP session 的 sequence=1 READY 在
  lease settlement 前返回 `TURBO_EBUSY`，新 route 完成旧 request 返回 `TURBO_EPROTO`；at-most-once
  lease DROP 后接受新 session grant，并恢复后续 JOB/COMPLETE。
- [x] Queue memory、SQLite 与 Redis Stream 实现 bounded multi-claim：默认仍为 1，独立
  token/view/settlement；memory 按原 enqueue sequence 重放，Redis 以同 consumer PEL 为事实源并通过
  真实 Redis 验证多 entry restart replay；SQLite 使用 queue-private schema v2 metadata、legacy 原位迁移
  和 crash 后 in-flight requeue，不占用数据库全局 `PRAGMA user_version`。
- [x] 实现运行时 credit settlement coordinator：bounded `request_id -> claim_token` 元数据在同一
  host-serialized lane 内协调 dispatch、completion、cancel 与 expiry；Queue/Redis 提供统一
  ACK/requeue/drop settler，storage 失败保留 pending record 并显式 retry，只有 storage ACK 成功才生成
  worker completion ACK，drop 不制造 delivery ACK。
- [x] 补齐默认 4096 in-flight credit pressure contract：容量满在建立第 4097 个 correlation 前返回
  `TURBO_ENOSPC`，completion 不自动返还 credit；显式增量 grant 后恢复 admission，并以确定性置换
  顺序完成全部 request，验证 hash index 与 vector swap-remove 在峰值下保持一致。另以 256 worker ×
  16 correlation 覆盖同一 lease deadline 下的 4096 次逐项 REQUEUE、worker generation 清理及
  worker swap-remove 后的 correlation index 修正。
- [x] 补齐共享 FMQ TCP transport 的重复 reconnect/stop soak：真实 PUB broker 连续 12 轮
  stop/start，SUB 每轮完成 connect、HELLO 与订阅恢复后仅接收新的唯一 payload，不重放旧消息；最终在
  reconnect wait 中 stop，并验证 500ms 有界中断及 `STOPPED/TURBO_ESHUTDOWN` 快照。
- [x] 建立 Release 延迟分位数基线：credit owner 对 4096 个 in-flight dispatch/complete 分别输出
  P50/P95/P99 与 throughput；真实 TCP ROUTER/DEALER 对 64-byte serialized echo（32 warmup、256
  samples）输出端到端 P50/P95/P99。owner-lane send completion 使用每 request coroutine wait 唤醒，
  不再以 1ms timer polling 把 Windows reply latency 量化到约 15.6ms。结果作为同机回归参考，不作为
  跨平台绝对 SLA；发布门槛应由 CI 保存历史并按目标环境设阈值。
- [x] 完成跨进程 durable credit recovery：retry ledger、claim settlement marker 与 completion-ACK
  outbox 由同一 storage transaction 推进。Redis owner 通过 TFCS/1.0 snapshot + claim disposition 的单次
  EVAL、exact XPENDING 和 snapshot equality 关闭 ambiguous reply；SQLite Queue schema v2 在同一
  transaction 中提交 snapshot + row disposition，并支持 legacy migration。FMQ 重启将 durable INFLIGHT
  归一为 PENDING，completion outbox 只保存 logical address 并等待显式 publish confirm；配置化 shutdown
  先 quiesce admission，再有界 requeue/drop 或 preserve。真实 Redis 与 SQLite 组合测试均覆盖 coordinator
  completion/restart，`credit_worker + at_least_once` YAML 仅通过匹配 storage binding 开放。

企业总线产品化门槛（2026-07 审查）：

- [x] **HIGH**：补齐 failure-domain deployment controller。现有 discovery controller 只把宿主提供的
  versioned snapshot 映射到固定 adapter slots，不拥有 registry、broker election、跨进程 membership、
  failover 或 split-brain 判定；独立 FMQ deployment owner 现拥有有界 member lease registry、跨
  failure-domain deterministic election、authority/member incarnation、route fencing generation 与
  normalized snapshot split-brain 判定；host 通过管理 transport 搬运 pointer-free command/snapshot，
  graph/discovery 只消费决议结果。authority epoch 仍由宿主强一致 election/fencing 服务分配。
- [x] **HIGH**：定义滚动升级契约。FMQ 当前只接受 wire v2 并严格拒绝 v1；发布未来 wire/API/schema
  版本前，必须提供 capability/兼容矩阵、同版本可扩展规则、双版本 gateway 或停机升级路径，并做
  mixed-version E2E。现以 release manifest 对 FMQ wire、TFMP minor、TFMS shared read/write、YAML
  schema 与 capability 计算 direct/gateway/stop/incompatible，测试覆盖 v2/v3 gap、显式 dual-stack
  gateway 和 shared writer gap；未改变当前 v2/TFMP decoder。
- [x] **HIGH**：为 durable management side effect 增加 typed reconcile。operation store 与 target
  mutation 不是同一事务；crash 后残留 RUNNING 只能转为 FAILED/INTERNAL 且禁止自动重放。在每种可
  durable command 能查询/核验实际副作用并完成补偿前，只能宣称防重复执行，不能宣称自动恢复。
  恢复中的 RUNNING 现保持 recovery-required 并阻止 READY；Flow/pool 使用内建 inspector，其他
  resource command 必须注入 typed inspector 才宣告 durable，APPLIED/NOT_APPLIED/CONFLICT 分别
  无重放完成、generation-checked goal-state 重试或无 mutation 冲突终止。
- [x] **MED**：完成纯 YAML deployment bundle。复用同一 connection/timer/thread/coro fragment 的
  socket/http/rpc projection、disabled build target isolation、resolved-config 单轨迁移和 parser/compiler/
  observe 成败测试已按 Phase 9 验收；host-owned execution object 和 Observe attach 仍保持显式，
  不被 YAML 偷偷创建。
- [x] **MED**：完成跨 owner 治理文档。沿用 Phase 10 为各 resource kind 补 Spec/Condition/Event
  schema-backed document，并给 queue/connection/protocol/storage/rule 等 owner 固化 reason、generation
  与 event gap 语义；Observe exporter 只导出派生指标，不成为第二事实源。
- [x] **MED**：建立 release soak/chaos gate。`fmq-release` label 覆盖 YAML/config、Socket/HTTP/RPC、
  resource governance、multi-adapter CoroNet execution、12 轮 broker restart、slow peer、4096 credit
  correlation、Redis/SQLite restart、management crash window、failure-domain、rolling compatibility 和
  start/stop/replace/resize generation；真实 Redis 必须显式启用。固定时限在测试内断言，
  `FMQ_BENCH_RESULT` 按同 runner 保存并以 throughput 80%/P99 125% 做趋势 gate。
- [x] **MED**：当前 FMQ 产品矩阵明确不包含数据库事务事件集成；持久化边界只宣称 SQLite Queue 与
  Redis Stream/Data。PgSQL query/sink 不冒充 durable outbox，未来若扩大产品范围再作为独立 phase
  提供 source/sink 与事务 E2E。

安全边界仍按当前决策排除在本阶段：FMQ/TFMP 可以使用 TLS transport，但没有认证、授权、租户与
密钥管理契约，因此当前产品只能部署在宿主已建立的可信网络/进程边界内，不能宣称 multi-tenant ESB。

兼容性：这一层不增加 FMQ socket pattern、不改变 v2 frame，也不放宽现有 REQ/REP 的同步
状态机。现有 `turbo_flow_fmq_message_detach_router_route()` 仍是获得 live ROUTER route 的唯一
入口；broker 只保存和选择其 pointer-free 副本。Chapter 4 在此状态 owner 上扩展可靠性，
不能把 transport send success 重新解释为 delivery ACK。

## 6. 标准组合

### 6.1 PUB/SUB filter proxy

```flow
source inbound adapter fmq.sub
stage decode adapter codec.json.in
stage validate adapter databind.order.in
stage filter
stage outbound adapter fmq.pub

stage main {
  inbound -> decode -> validate -> filter -> outbound
}
```

### 6.2 PUSH/PULL durable worker queue

```flow
source jobs adapter fmq.pull
stage durable adapter queue.jobs.sink
source next_job adapter queue.jobs.source
stage worker
stage results adapter fmq.push

stage main {
  jobs -> durable
  next_job -> worker -> results retry attempts 3 delay 100
}
```

### 6.3 ROUTER/DEALER service bridge

同步 service 可直接沿 borrowed context 回复；异步 service 必须先调用
`turbo_flow_fmq_message_detach_router_route()`，再把 owned message 交给 worker 或 memory Queue，
最终由同一个 `fmq.router` sink 发送。REQ/REP 不复用这条异步路径。

```flow
source request adapter fmq.router
stage decode adapter codec.json.in
stage service
stage encode adapter codec.json.out
stage reply adapter fmq.router

stage main {
  request -> decode -> service -> encode -> reply
}
```

### 6.4 XPUB/XSUB proxy（目标语法）

```flow
source sub_side adapter fmq.xsub
stage decode adapter codec.bin.in
stage filter
stage pub_side adapter fmq.xpub

stage main {
  sub_side -> decode -> filter -> pub_side
}
```

## 7. 验证矩阵

每个新增 capability 在声明完成前至少满足以下准入规则；不适用项必须在交付记录中说明
原因，而不是静默跳过：

- C config、option schema 和文档同步。
- 有 focused TinyTest 行为测试，不只检查字段存在。
- 覆盖 TCP 和至少一个适用的非 TCP transport。
- 覆盖 bind/connect 两侧 shutdown。
- 覆盖 private context、`BORROWED_CONTEXT`、`OWNED_CONTEXT` 和 `POOL_LANE` 的适用路径。
- 覆盖 stop during pending wait。
- 覆盖 invalid config fail fast。
- 错误码语义明确且无 silent fallback。
- 文档 DSL 示例通过 parse/compile；适用时增加 runtime test。

重点回归目标：

- `test_coronet_runtime`
- `test_io_policy`
- `test_timer`
- `test_socket`
- `test_fmq`
- `test_turbo_flow`
- `test_turbo_flow_store`
- `test_turbo_flow_observe`
- `test_turbo_flow_redis`
- 相关 CoroNet transport tests

Windows user preset 的标准复验入口：

```powershell
cmake --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user --output-on-failure
```

聚焦验证应使用同一 build tree 的 `ctest -R` 或直接运行对应 TinyTest executable；
具体命令、平台、日期和结果保存在当次变更交付记录或 CI，不在路线图中维护易失副本。

## 8. 完成定义

一个 capability 只有同时满足以下条件才可标记为 `- [x]`：

- 行为契约明确，包括输入、输出、错误码、状态所有权和线程约束。
- C config、option schema、resolved config 和文档保持一致。
- focused tests 覆盖主路径、边界、错误和 stop/shutdown。
- 协议变化包含版本或 capability compatibility tests。
- 状态迁移说明成功、失败、重试、清理及最终可接受状态。
- 与 stage、queue、storage、observe 的责任边界没有重复事实源。
- 已给出可重复的本地构建与测试命令，并取得通过结果。
