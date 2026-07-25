# FlowMQ Control V1 Protocol

状态：已实现的兼容管理应用协议。本文是 `TFCQ` request、`TFCP` reply、STATUS/EXECUTE、
有界幂等历史和错误边界的唯一详细正文。它运行在普通 FMQ REQ/REP 的 `DATA` payload
中，不改变 FMQ/3 framing。

协议总索引见 [PROTOCOL_SPEC.md](PROTOCOL_SPEC.md)。新管理能力使用
[MANAGEMENT_PROTOCOL.md](MANAGEMENT_PROTOCOL.md) 定义的 TFMP/1；两者不得混用 magic
或把 TFMP 字段塞入 Control V1。

## 1. Scope

Control V1 只支持单 target 的 `STATUS` 与 `EXECUTE`。它是已有 `kind: fmq_control`
配置和公开 API 的兼容入口，不提供 capability、异步 operation、live event 或 durable
operation journal。传输使用独立管理 flow，不能与被管理 target flow 共用同一个同步
dispatch owner。

所有整数使用 network byte order。文本字段必须无 NUL、长度受对应常量限制；请求和
回复必须是完整 envelope，trailing bytes、未知版本、header size 错误和保留字段非零
都必须拒绝。

## 2. TFCQ request

request magic 为 `TFCQ`，version 为 `1`，固定 header 为 64 bytes；后接六个字段，
顺序固定为 `target`、`idempotency_key`、`command_target`、`condition`、
`endpoint_host`、`endpoint_path`。

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TFCQ` |
| 4 | 2 | version | `1` |
| 6 | 2 | header size | `64` |
| 8 | 2 | operation | `STATUS=1`, `EXECUTE=2` |
| 10 | 2 | command kind | EXECUTE 时有效 |
| 12 | 2 | pool kind | RESIZE_POOL 时有效 |
| 14 | 2 | adapter kind | ADAPTER 时有效 |
| 16 | 4 | parallelism | RESIZE_POOL 时有效 |
| 20 | 4 | endpoint port | REPLACE_ENDPOINT 时有效 |
| 24 | 8 | request ID | 必须非零 |
| 32 | 8 | timeout ms | DRAIN 或 adapter replace 按 schema 使用 |
| 40 | 2 x 6 | field lengths | 六个变长字段的 bytes |
| 52 | 12 | reserved | 必须全零 |
| 64 | variable | fields | 按固定顺序拼接 |

`target` 必填且最大 127 bytes；`idempotency_key` 最大
`TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX`，EXECUTE 必填，STATUS 必须为空。其余字段
上限分别来自 `TURBO_FLOW_CONTROL_NAME_MAX`、`TURBO_FLOW_CONTROL_EXPR_MAX` 和
`TURBO_FLOW_ENDPOINT_MAX`，不得用 C struct padding 或指针作为 wire 内容。

STATUS 请求必须将 command 所有字段置零。EXECUTE 的 command kind、pool、adapter、
parallelism、endpoint 和 timeout 组合必须符合本地 `turbo_flow_control` schema：
PAUSE/RESUME/DRAIN、RESIZE_POOL 和 ADAPTER 的无关字段必须为零；REPLACE_ENDPOINT
必须提供有效 host 与 1..65535 port。非法组合在 decode 阶段拒绝。

## 3. TFCP reply

reply magic 为 `TFCP`，version 为 `1`，固定 header 为 88 bytes；后接 bounded diagnostic
message，最大 159 bytes。

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TFCP` |
| 4 | 2 | version | `1` |
| 6 | 2 | header size | `88` |
| 8 | 8 | request ID | 复制 request；协议错误可为零 |
| 16 | 4 | status | signed native status |
| 20 | 4 | flags | bit 0 `REPLAYED`，其他 bit 为零 |
| 24 | 4 | runtime state | 有效 `turbo_flow_state_t` |
| 28 | 4 | accepting publishes | `0` 或 `1` |
| 32 | 4 | active publishes | runtime snapshot |
| 36 | 4 | reserved | 必须为零 |
| 40 | 8 | stage count | 必须可转换为本地 `size_t` |
| 48 | 8 | edge count | 同上 |
| 56 | 8 | adapter count | 同上 |
| 64 | 8 | pool count | 同上 |
| 72 | 4 | error line | diagnostic |
| 76 | 4 | error column | diagnostic |
| 80 | 2 | message length | 最大 159 |
| 82 | 2 | reserved | 必须为零 |
| 84 | 4 | reserved | 必须为零 |
| 88 | variable | diagnostic message | 无 NUL |

协议 dispatch 与 command status 是两个 ACK 边界。即使 command 失败，服务也必须
生成 terminal `TFCP`；成功返回只表示 reply 已生成，不等于远端已接收或 target 已
完成持久化。malformed request 无法可靠解析时，服务仍应返回 `TURBO_EPROTO` 或
`TURBO_EMSGSIZE` 的 terminal reply。

## 4. Idempotency

EXECUTE 使用 bounded in-memory history。相同 target、operation、command 和
idempotency key 的请求可以 replay，并在 reply 中设置 `REPLAYED`；同一 key 对应
不同请求必须返回冲突错误，不得再次执行。STATUS 不使用幂等历史。

历史容量由 `dedup_capacity` 控制，最大受实现常量约束；满时 fail fast 返回明确错误，
不得无提示扩容、落盘或 fallback 到另一个事实源。Control V1 不承诺跨进程重启的
durable dedup；需要该语义时使用 TFMP/1 operation store。

## 5. YAML and API boundary

`kind: fmq_control` 的 resolved channel 必须声明 `protocol_version: 1` 和非空
`target`，可选 `dedup_capacity` 与 `max_request_bytes`。错误 size/version、未知
字段、超过 request 上限的值和 target 不匹配都必须在 resolve/create 阶段失败。

公开入口为 `turbo_flow_fmq_control_request_encode/decode`、
`turbo_flow_fmq_control_reply_encode/decode`、
`turbo_flow_fmq_control_service_*` 以及 `turbo_flow_fmq_control_stage`。service
借用 target flow，host 必须保证其生命周期先于 service 销毁，并序列化 target lifecycle
调用。stage 保留 REP route/context，不把本地 pointer 或 CoroNet route 编入 wire。

## 6. Compatibility and evidence

TFMP/1 可以与 Control V1 适配到同一个内部 command/result，但不得改变 TFCQ/TFCP
magic、字段布局、公开 API 或 YAML 行为。实现与测试位于：

- `include/turbo_flow_fmq_control.h`
- `src/fmq_control_protocol.c`
- `src/fmq_control.c`
- `tests/test_fmq_control.c`

测试覆盖 request/reply roundtrip、malformed/reserved rejection、idempotency replay
与 conflict、YAML resolve，以及普通 TCP REQ/REP graph path。
