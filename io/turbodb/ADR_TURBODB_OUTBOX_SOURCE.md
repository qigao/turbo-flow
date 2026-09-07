# ADR: Provider-Neutral Outbox Source and Settlement Boundary

## 背景

TurboDb 的 Lua apply 路径会把状态、`metadata.applied_index` 与 Redis Stream outbox
在一个提交点写入。TurboFlow 需要消费这些事件，但 Graph 不应知道 Redis reply、consumer
group、连接重定向或 `XACK`。截至本决策，生产侧写入与 reconcile 已合入 TurboDb；类型化
`XREADGROUP`、claim 与 `XACK` owner API 由
[`qigao/turbodb#21`](https://github.com/qigao/turbodb/issues/21) 跟踪。

## 决策

`TurboFlow::TurboDbAdapter` 提供 provider-neutral
`turbo_flow_turbodb_outbox_source_t`。它由一个 owner thread 串行调用，不创建隐藏线程，也
不执行 raw Redis 命令。provider 的 `fetch` 每次返回一个借用的 receipt view，并可通过
CFlow waitable 表达尚未就绪；waker 只原子标记 ready，所有 provider callback 仍在后续
owner `poll()` 或 `stop()` 中执行。waitable cancel 是同步 quiescent boundary：它返回之后
provider 不得再保留或调用旧 waker；这也是 stop 后允许 destroy Source 的生命周期前提。

每个已接受 receipt 被复制到一个连续 `mem_buffer_t`：固定 metadata、stable identity 与
payload 都由消息拥有。一个 receipt 对应一个独立 `turbo_flow_run_t`，因此 Graph terminal
状态与 opaque token 一一对应，不需要把 receipt 状态放进 Graph。成功映射为一次 ack；
retryable failure/cancel 映射为 requeue；permanent failure 在显式 DLQ policy 下先执行
dead-letter，再进入可独立重试的 ack 阶段。DLQ 成功后 ack 失败不会重复执行 DLQ。
`max_steps` 为 1 时，dead-letter 与 ack 也必须分属两次 settlement step。

这是 at-least-once 边界。投影成功后、ack 前崩溃会导致 redelivery，下游必须按 stable
outbox/Raft identity 幂等。Stream 不是 Raft log，消费也不推进同步 state-machine commit。

## 容量与 demand

只有 `outstanding_demand > 0` 时才调用 fetch。每次提供给 provider 的记录预算为：

```text
max_records = min(outstanding_demand, configured_fetch_count, free_message_slots)
max_retained_bytes = configured_in_flight_bytes - current_in_flight_bytes
```

实际 retained bytes 等于 metadata、identity 与 payload 的连续 buffer 使用量。identity、
payload、delivery attempts、message slots 与总 retained bytes 另有各自硬上限。任何 provider
返回值超过已给预算都被视为协议错误：若 token 有效则先 requeue，再 fail fast；不提交空的
Graph 投影，也不扩大容量。

## 生命周期与错误

配置在 open 时复制；Flow、可选 scheduler、provider context 与 policy context 由调用方拥有，
且必须活到 source destroy。`stop()` 先关闭 admission，再取消 WAIT 与 provider fetch，随后
取消所有 live Graph runs 并按唯一的 `SHUTDOWN_REQUEUE` policy 释放 claim。settlement callback
失败会保留 slot/token/阶段，调用方用相同操作重试 `poll()` 或 `stop()`。只有全部 claim 已
终结后状态才进入 STOPPED，destroy 在此前返回 `SALTS_EBUSY`。

provider 报告被删除或 trim 的 pending ID 时，Source 进入 FAILED，返回 `SALTS_ENOENT`，并
增加独立 `data_loss_events`；该情况不会被解释为空队列或已确认。没有 PubSub、同步物化、
raw reply parsing 或其他 fallback。

`IDLE`、`WAIT` 与 `RECORD` 必须携带 `SALTS_OK`；不一致的 kind/status、无效 classifier
结果和 Graph admission 失败都直接返回明确错误。只要 `RECORD` 已转移非零 token，即使后续
校验、复制、Graph open 或首次 requeue 失败，Source 都先把 token 登记到有界 slot，直至
requeue 成功，避免 claim 脱离唯一事实源。provider 若重复返回仍处于 active slot 的 token，
Source 会以协议错误停止接纳，避免同一 claim 产生多个 Graph run。stop 会把包括
`ACK_PENDING` 在内的一切未结算阶段统一转换为 requeue，不会在 shutdown 中继续确认消息。

## 迁移与验证

当前 fake provider 测试覆盖 demand、预算、自持有消息、ack/requeue/DLQ、WAIT/wake、data-loss
及 stop。TurboDb #21 完成后，只新增一个把类型化 TurboDb consumer handle 映射到现有 vtable
的薄桥接层；Source 状态机、Graph API 与 receipt 布局不改变。真实 Redis、Cluster、Sentinel
及 claim/retention 故障测试留在该桥接交付中，不能用 fake 或 raw-command fallback 代替。
