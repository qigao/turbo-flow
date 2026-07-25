# ADR: Message and Graph Boundaries

## 状态

已采纳。本文描述当前实现与产品组合边界，不改变 FMQ v3 wire 或 Flowie MQTT wire；
公开 C ABI 仅增加兼容性的 protocol-origin 类型与消息访问函数。

## 背景

TurboFlow 同时承载协议 provider、FlowMQ pattern、Flowie MQTT、HTTP/socket/Redis Stream
以及 FlowStore。若把协议层的 `frame_t`、graph 的消息 envelope 和存储中的持久化记录
描述成同一个对象，会掩盖所有权、复制、ACK 和并发边界。

## 决策

统一的 graph/provider 数据契约是 `turbo_flow_msg_t`：

```text
external bytes / protocol frame
        -> provider parser and validation
        -> turbo_flow_msg_t (owned buffer + typed metadata)
        -> optional graph stages
        -> provider or typed store sink
```

`flow_fmq_frame_t`、HTTP response/request view、socket framing view 和 Redis Stream entry
都是 provider 内部或外部存储边界的对象，不得跨异步 graph/store 生命周期直接借用。

### FlowMQ

PUB/SUB、PUSH/PULL、REQ/REP、ROUTER/DEALER 等基础 pattern 以纯数据流转和 pattern owner
为主。它们需要把协议 frame 转换为 `turbo_flow_msg_t` 才能连接统一的 typed operation、
security、settlement 或 store boundary；但基础 pattern 的默认 graph 只是一层薄 bridge，
不应被描述成业务 processor pipeline。

高级 pattern（例如 load balancer、reliable request、credit worker、规则处理、持久化和
跨 provider enrichment）才组合多个 graph stage、executor 和显式外部存储。

#### Direct terminal native batch

`turbo_flow_module_adapter_registration_t.consume_batch` 是 size-versioned 的可选 provider
ABI。它只优化 `source -> terminal adapter` 的薄 bridge：source 必须只有一条无条件边，
terminal stage 不得有下游、observer、emitter、retry、reorder、deadline、settlement 或
非 inline executor。任何条件不满足时，core 使用原有逐消息 dispatch，语义不变。

core 仍拥有消息准备和生命周期。provider 必须按升序且恰好一次调用 batch iterator；每次成功
返回的 `turbo_flow_msg_t` 都是独立 clone/retained view，provider 在读取下一项前清理它。
provider 在首个 prepare/consume 错误停止，`consumed` 只报告错误之前已成功消费的项。成功返回
必须消费全部消息；跳号、重复读取、超额计数或未消费完整批次却返回成功均为 `TURBO_EPROTO`。
旧 registration size 不暴露该回调，自动保留逐消息路径。

该优化不把 facade payload 直接借给 socket，也不改变 HWM、编码、首错、partial submission、
request completion 或 socket coalescing。其时间复杂度为 O(n)，额外活跃消息存储为 O(1)；
每个 iterator step 最多保留一个消息 view。

### Flowie

Flowie 的协议 owner 在 connection lane 处理 CONNECT、SUBSCRIBE、UNSUBSCRIBE、PING、AUTH、
QoS 状态等控制事务。经过 admission 的 application PUBLISH 转换为 `turbo_flow_msg_t` 并进入
配置的 graph；graph 输出再交给 MQTT endpoint、socket、HTTP 或显式 store adapter。

因此 Flowie 的业务消息是 graph 数据，协议 ACK/session state 仍归 Flowie owner，不由 graph
伪造或替代。

Flowie broker graph 的阶段契约是：`auth/ACL` 和 `inflight admission` 先由协议 owner 完成，
`store` 表示显式 fact-store/durable commit，`after-process` 是可选
的后处理或审计，最后由协议 owner 根据配置的 settlement prerequisite 发出 MQTT ACK。当前
入口只把 admitted PUBLISH 放入 graph；CONNECT、AUTH、SUBSCRIBE 等控制包不会因为存在 graph
而自动变成可过滤的业务消息。TurboFlow Policy 可以处理 graph 中的任意消息类型/字段，但不能越过
认证边界或推进 session state。

`store -> Policy` 与 `Policy -> store` 都是合法但不同的拓扑：前者保存原始 admitted
消息，后者保存过滤/变换后的结果。顺序必须在 `.flow` 中显式表达，不能由 YAML resolver 或
broker owner 隐式重排。

### FlowStore

FlowStore 按事实语义拆分 Record、State、Index、Log 和 TimeSeries。写入必须复制或 retain 足够的
`turbo_flow_msg_t` 数据，不能保存短生命周期 frame/view。local、Redis 或 PostgreSQL backend
只能有一个事实源；
缓存和 bitmap 都是可重建的派生查询结构，不承担 ACK/requeue 或协议 session 所有权。

## 所有权和复制

`turbo_flow_msg_t.buffer` 支持 `retain/move`，所以 buffer-backed payload 在 graph stage、
Disruptor、thread/coroutine executor 和 Flowie reply queue 之间可以近零拷贝转移。协议 framing
重组、provider 格式转换、owned payload、schema projection、retry、mutable fan-out 和
持久化序列化仍可能复制 payload。

`frame_t` 只在 provider owner lane 内有效；进入 graph 或 FlowStore 后，事实对象是
`turbo_flow_msg_t` 或其持久化表示。

### Durable protocol origin

`turbo_flow_protocol_route_t` 是 live owner capability，包含进程实例和 generation fence，禁止
序列化。durable outbox 只保存 payload、message type/flags 和
`turbo_flow_protocol_origin_t`；origin 仅包含协议、协议版本和稳定 session identity，可用于
MQTT no-local 等消息语义，不能用于结算 ACK 或定位旧连接。回放进入当前 endpoint owner 后重新
选择当前订阅与连接 route。

候选方案中，直接序列化 route 会在重启后形成 stale capability，已拒绝；只保存 payload 会丢失
MQTT 版本与 publisher identity，使回放无法安全 fan-out，也已拒绝。显式 origin 增加少量行元数据
与 API 面积，但保持 live control plane 与 durable data plane 分离。PostgreSQL 旧表通过
`ADD COLUMN IF NOT EXISTS` 原位扩展；旧 payload 行保持不变，没有 origin 的行只能进入不要求协议
origin 的 graph。回滚代码前应先排空新格式 outbox，避免旧二进制忽略新增语义。

## 影响

- 文档、接口说明和性能分析必须分别说明 frame、msg、graph stage、store record 的边界；
- 基础 FMQ throughput 可以使用受限的 direct terminal native batch，但不能绕过 msg ownership
  contract，也不能改变标量 dispatch 的可观察语义；
- Flowie 继续使用 graph 处理 admitted application messages，控制协议不强行 graph 化；
- FlowStore 的容量、保留、revision 和 durable recovery 是独立于 graph executor 的资源契约；
- 本决策不改变 wire；公开 API 为只增不改，表迁移为无损加列，验证范围包括消息 clone/clear、
  route-less MQTT fan-out、PG COMMIT/replay/delete 和失败保留。
