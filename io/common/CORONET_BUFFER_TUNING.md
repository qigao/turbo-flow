# CoroNet Buffer Tuning Contract

本文定义 TurboFlow 上层组件公开的 CoroNet buffer 与背压参数。它是本地配置契约，不是
FlowMQ wire protocol 或 MQTT RFC 的一部分，不产生新的协议版本。

## 1. 参数分层

| 参数 | 所有者 | 作用 | `0` 的含义 |
|---|---|---|---|
| `stream_recv_buffer_bytes` | CoroNet context | 每个 stream 的用户态 ping-pong receive buffer；每个 stream 两块 | 保留组件默认值 |
| `socket_recv_buffer_bytes` | OS socket | 请求 `SO_RCVBUF`，TCP listener 传播给 accepted socket | 不调用 `setsockopt`，保留 OS 默认 |
| `socket_send_buffer_bytes` | OS socket | 请求 `SO_SNDBUF`，TCP listener 传播给 accepted socket | 不调用 `setsockopt`，保留 OS 默认 |
| `send_hwm_bytes` | 上层/CoroNet | 限制尚未完成的用户态发送数据，提供背压 | 由组件定义，见下表 |

这四类参数不能互相替代。`SO_RCVBUF` 不限制应用队列，`send_hwm_bytes` 也不会扩大 TCP
receive window。

## 2. 公开映射

| 上层 | 用户态接收 buffer | OS socket buffer | 用户态发送上限 |
|---|---|---|---|
| FlowMQ | `turbo_flow_fmq_config_t.stream_recv_buffer_bytes` | `socket_recv_buffer_bytes` / `socket_send_buffer_bytes` | `send_hwm_bytes`、frame HWM、可选 per-peer HWM |
| Flowie server | `stream_recv_buffer_bytes` | `socket_recv_buffer_bytes` / `socket_send_buffer_bytes` | `send_hwm_bytes`，`0` 选择 1 MiB 默认值 |
| Flowie MQTT client | `flowie_mqtt_client_config_t.stream_recv_buffer_bytes` | `socket_recv_buffer_bytes` / `socket_send_buffer_bytes` | `command_queue_capacity` / `command_queue_max_bytes` |

FlowMQ 与 Flowie server 的 YAML 字段名和 C 字段名一致。

## 3. 数值与生命周期

- 所有单位均为 bytes。
- `socket_recv_buffer_bytes` 和 `socket_send_buffer_bytes` 的有效非零范围为
  `1..INT_MAX`，仅适用于 TCP、TLS、WS、WSS。
- OS socket buffer 是请求值。操作系统可以调整、clamp 或用不同方式报告实际容量。
- FlowMQ 和 MQTT client 的 `stream_recv_buffer_bytes` 有效非零范围为 1 KiB..1 MiB。
  Flowie server 使用相同范围。
- FlowMQ 和 MQTT client 的 `0` 保留 CoroNet 默认值，当前为每块 128 KiB。
  Flowie server 为控制多连接内存，`0` 选择每块 4 KiB。
- socket 参数必须在 bind/connect/listen 前应用；stream 参数必须在第一个 stream 创建前应用。
- 这些参数不支持热更新。修改后需要替换 endpoint 或重建 client。
- 非 TCP-backed transport、越界数值、非当前完整配置结构以及无法应用的 `setsockopt` 都直接返回错误，
  不静默忽略或降级。
- `stream_recv_buffer_bytes` 只允许配置组件自己创建的 private context。borrowed context、
  owned context 和 pool lane 由 context 所有者在创建 stream 前统一配置。

这些公开配置没有本地版本号，也不接受历史布局。首字段 `size` 必须精确等于当前公开结构大小。
FlowMQ wire v3 与 MQTT 3.1/3.1.1/5 是独立的协议版本事实。

## 4. 容量预算

每条 TCP-backed stream 的主要增量内存可按以下上界估算：

```text
2 * stream_recv_buffer_bytes
+ effective SO_RCVBUF
+ effective SO_SNDBUF
+ send queue/HWM
+ protocol frame/session state
```

listener 的 socket buffer 配置会传播给每个 accepted socket，因此服务端预算必须再乘以最大连接数。
例如将 1024 条连接的接收请求从 Windows 常见的 64 KiB 提高到 1 MiB，仅接收侧请求增量约为
`1024 * (1 MiB - 64 KiB) = 960 MiB`；实际提交内存由 Windows 管理策略决定。

## 5. 选择建议

- 64-byte 高频消息：优先使用 FlowMQ Batch API 或受限 micro-batch。调大 socket buffer 不能消除
  每条消息的 syscall、调度和协议固定开销。
- 64-KiB payload、512-KiB batch：Windows 本机基准中，1 MiB `SO_RCVBUF` 消除了反复出现的
  15 ms 级接收停顿；该值是当前机器的实测起点，不是跨平台默认值或性能保证。
- 延迟优先：从默认值开始，只在 benchmark 显示 receive-window/backpressure stall 后增大。
- 吞吐优先：同时记录吞吐、p50/p99/max、慢样本数和进程/内核内存；只看平均带宽不足以判断配置。
- `socket_send_buffer_bytes` 应独立验证。当前 Windows 基准中，单独增大 `SO_SNDBUF` 没有修复接收侧
  长尾，因此不应机械地与 `SO_RCVBUF` 设置成相同值。

## 6. 示例

FlowMQ C 配置：

```c
turbo_flow_fmq_config_t endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
endpoint.stream_recv_buffer_bytes = 128u * 1024u;
endpoint.socket_recv_buffer_bytes = 1024u * 1024u;
endpoint.socket_send_buffer_bytes = 512u * 1024u;
```

Flowie server YAML：

```yaml
config:
  stream_recv_buffer_bytes: 131072
  socket_recv_buffer_bytes: 1048576
  socket_send_buffer_bytes: 524288
```

MQTT client：

```c
flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
config.stream_recv_buffer_bytes = 128u * 1024u;
config.socket_recv_buffer_bytes = 1024u * 1024u;
config.socket_send_buffer_bytes = 512u * 1024u;
```
