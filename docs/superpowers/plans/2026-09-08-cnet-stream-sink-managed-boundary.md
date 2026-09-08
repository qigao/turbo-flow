# CNet Stream Sink Managed Boundary 实施计划

## 背景与目标

`turbo_flow_cnet_stream_sink` 已经通过 CFlow IO Actor 提供有界异步发送与终态结算，
但目前只注册为 legacy async-terminal adapter，管理平面无法直接观察其 Sink 契约、
生命周期、容量、背压与结算计数。本改动使用
`turbo_flow_register_managed_async_terminal_adapter()` 一次性、原子地注册 adapter 与
managed boundary；不保留 legacy 注册或快照推断 fallback。

## 所有权与事实源

- `turbo_flow_cnet_stream_sink_t` 是 adapter、CNet client/connection、manual executor、
  IO Actor 及 managed boundary 回调上下文的唯一 owner。
- managed UID 在注册时由固定前缀 `cnet-stream-sink:` 与 owner identity 派生一次并存入
  sink。可直接表达的 adapter 名称保持原 owner/content identity；超过 managed owner 容量的
  既有名称使用项目已有 xxHash 依赖派生稳定的 `xxh3-128:<digest>` identity，不收窄原注册
  接口可接受的名称范围。
- owner generation 固定为 `1`。该边界当前不支持命令或在线替换，因而生命周期内没有
  会改变资源身份的 managed mutation；metadata、descriptor、snapshot 使用同一 UID 与
  generation。
- queue capacity/depth/in-flight 的运行期事实源是 CFlow IO Actor stats；actor 尚未初始化
  或已经销毁时，容量使用该 owner 的固定 request capacity `1`，运行数量为零。
- lifecycle mutex/condition 将 start、poll、stop、Actor init/destroy 和 snapshot 纳入同一 owner
  同步域；snapshot 可在 drain 期间读取 Actor stats，destroy 则与该读取互斥。并发 admission
  以计数标记短暂提交窗口，snapshot 在该窗口返回 `SALTS_EBUSY`。
- accepted/completed/rejected 是 sink 持久原子计数，跨 actor 停止仍可查询：accepted 仅在
  Actor 接受 operation 后增加；completed 仅在 async claim 首次成功完成后增加；rejected
  仅统计进入 sink submit 边界但在 Actor admission 前或 admission 时被拒绝的消息。

## 生命周期映射

| CNet stream sink 状态 | Managed boundary 状态 |
| --- | --- |
| REGISTERED | REGISTERED |
| CONNECTING | STARTING |
| CONNECTED | RUNNING |
| STOPPING 且 Actor 仍有 active request | DRAINING |
| STOPPING 且无 active request | STOPPING |
| STOPPED / DETACHED | STOPPED |
| FAILED | FAILED |

`start` 一进入初始化即发布 CONNECTING；任一 CNet/Executor/Actor 初始化失败均保留明确错误并
最终发布 FAILED。停止同步关闭 admission、驱动 Actor 至 quiescent、销毁 executor/client，
随后发布 STOPPED。

## 内容、容量与背压契约

- role: `TURBO_FLOW_MANAGED_BOUNDARY_SINK`
- capability: `TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT`
- command mask: `0`，不注册 command callback
- input: IO transport domain、generic profile、opaque encoding、
  `application/octet-stream`，schema `CNetStream/NonEmptyBytes/v1`
- 实际 submit 契约继续拒绝 NULL/空 payload，并按 `max_message_bytes` fail fast。
- `queue_depth = admitted + ready`；`in_flight = active_requests - queue_depth`。
- `backpressured = active_requests >= request_capacity`；`demand` 与 `lag` 为零。
- Actor stats 若违反 `queue_depth <= active_requests` 或容量不变量，snapshot 返回
  `SALTS_EPROTO`，不截断或猜测。

## 错误与结算语义

- 所有 pre-admission failure 均返回原始 Salts 错误，并增加 rejected；claim 仍归调用方。
- admission 成功后 claim 所有权随 operation 转交 IO Actor，accepted 恰好增加一次。
- native completion 和 Actor release safety path 共用一个 complete helper；只有成功消费 live
  claim 才增加 completed，从而避免重复 completion 计数。
- native send success 仍是 messages/bytes sent 的唯一事实；managed completed 表示 owner 定义
  的终态（成功、失败或取消），两者不混用。

## 实施与验证顺序

1. 扩展 CNet stream sink 测试，先证明 legacy 注册下 managed boundary 不存在（RED）。
2. 实现固定身份、managed callbacks、计数与原子聚合注册，使注册/启动/背压/结算/停止测试
   通过（GREEN）。
3. 增加 CNet client 配置失败用例，验证 STARTING 失败最终可观测为 FAILED 且无隐式 fallback。
4. 运行 `test_cnet_stream_sink`，随后运行 CNet 相邻测试和 `win-dev-user` Debug 全套 CTest。
5. 构建并安装 Debug 包，运行已安装 package 的 C 与 C++ consumer 测试，确认公开头文件与
   target 依赖保持可用。

## 兼容性与迁移成本

- 既有 `turbo_flow_cnet_stream_sink_*` 公共 C API、legacy portable snapshot 与发送行为不变。
- 新增的是同一 adapter 对应的一项 managed boundary 枚举结果；依赖 managed count 的调用方
  将看到该新资源，这是本 issue 的预期公开能力变化。
- 不新增依赖、不改变配置格式、不使用直接 socket API。注册失败保持原子性，调用方仍拥有
  sink 上下文并由注册函数清理。
- 回滚仅需恢复 legacy 注册调用并移除 managed callbacks/counters；不会迁移或重写用户数据。
