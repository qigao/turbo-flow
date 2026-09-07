# ADR: CNet TCP/TLS Listener Source Owner

## 状态

Accepted，适用于 issue #22；packet endpoint source 由 #23 单独实现。

## 背景

Issue #18 已建立单连接 outbound stream source，但 server listener 同时拥有 accept admission、
多条 generation-checked connection 与一个下游 Graph demand。CNet listener 和 client 都没有内部
I/O thread；`cnet_listener_wait()`、`cnet_listener_accept*_peer()` 与 `cnet_client_poll()` 必须由同一
非重入 owner 串行推进。每个 receive view 仍然只在 callback 内有效。

## 决策

在既有可选 `TurboFlow::CNetAdapter` 中新增 opaque
`turbo_flow_cnet_listener_source_t`，不改变 `TurboFlow::Graph` 的依赖方向。

一个 listener source 独占：

- 一个 CNet listener、一个承载全部 accepted connection 的 CNet client；
- 可选的 fail-closed TLS server context；
- 一个固定 `max_connections` 的稳定 slot 数组；
- 一个 manual CFlow Scheduler、一个 managed Publisher 与一个 `turbo_flow_run_t`；
- 全 owner 最多一个 pending receive，以及最多一个 callback 内完成复制、等待 move 的 message。

连接 slot 是 accepted connection 的派生索引；CNet generation handle 是连接事实源。Slot 只在匹配
handle 的 CLOSED/FAILED terminal callback 后回收。任何晚到且 handle 不匹配的 callback 都不能修改
已复用 slot。Listener 满时不 accept、不分配、不驱逐，peer 留在操作系统 backlog；后续 slot 回收后
再次 accept。

Graph demand 计数 downstream value。Publisher 仅在存在 demand 时，以 round-robin 从 CONNECTED
slot 选择一条 connection 并登记 `cnet_receive(..., 1)`。全 owner 单 pending 约束保证 callback 不会
覆盖 ready message，也让公平游标的行为可确定。Callback 在返回前把 borrowed bytes 复制到
`mem_buffer_t`，然后由 Publisher move 到 Graph；大小为零、非 bytes、超过 `max_message_bytes`、
重复 callback、分配失败或 message-id 溢出均 fail fast，不截断、不丢弃、不扩容。

严格 demand gating 也意味着 remote TCP FIN 只能在已经登记的 receive 上被 CNet 观察到：若容量已满
且 Graph demand 为零，已断开的 peer 暂时仍占用 slot；下一次正 demand 会登记 receive、观察 terminal、
回收 slot，并把仍未消费的 demand 转给 backlog 中随后 accept 的 peer。Adapter 不通过无 demand 的
探测读取绕过该约束。

`poll()` 先驱动 Scheduler，再非阻塞检查 listener；无 active connection 时 listener 可以使用调用方
timeout，否则 timeout 交给 CNet client。随后推进 client、再次非阻塞 accept，最后再次驱动
Scheduler。每次 accept pass 最多填满剩余固定 slot，不超过配置容量。

TLS 由非 NULL `cnet_tls_server_config` 显式启用。Open 同步创建 server context，并要求 client 配置
提供至少 `CNET_TLS_MIN_IO_BUFFER_BYTES` 的 TLS 存储和非零 handshake timeout。任何证书、握手或
认证错误保持 CNet status/native/stage，绝不退回 plaintext。单个 peer 的 TLS/transport failure 只
回收其 slot，并更新 `connections_failed` 与 `last_connection_*`；它不污染 listener owner 的 terminal
`status/error_stage`，也不终止其他连接或新 accept。Owner 自身的不变量、容量、listener/client poll 或
Graph 错误才进入全局 FAILED 状态。

Stop 顺序是：关闭 listener admission；关闭 run 以取消新 receive demand；请求关闭全部 live
connection；drain/stop/destroy CNet client；销毁 TLS context 与 listener；最后 shutdown/destroy
Scheduler。超时保留 owner，调用方可重试 stop；destroy 只接受 STOPPED 状态。
任何未完成的资源销毁同样保持 STOPPING 与 owner 所有权，重试 stop 从尚未释放的依赖继续，只有全部
资源完成释放后才发布 STOPPED。

## 公开接口

Config 与 snapshot 使用独立的 size/version。公开函数固定为：

```c
int turbo_flow_cnet_listener_source_open(
    const turbo_flow_cnet_listener_source_config_t *config,
    turbo_flow_cnet_listener_source_t **source_out);
int turbo_flow_cnet_listener_source_request(
    turbo_flow_cnet_listener_source_t *source, size_t demand);
int turbo_flow_cnet_listener_source_poll(
    turbo_flow_cnet_listener_source_t *source, uint32_t timeout_ms,
    turbo_flow_cnet_listener_source_snapshot_t *snapshot);
int turbo_flow_cnet_listener_source_snapshot(
    const turbo_flow_cnet_listener_source_t *source,
    turbo_flow_cnet_listener_source_snapshot_t *snapshot);
int turbo_flow_cnet_listener_source_stop(
    turbo_flow_cnet_listener_source_t *source, uint32_t timeout_ms);
int turbo_flow_cnet_listener_source_destroy(
    turbo_flow_cnet_listener_source_t *source);
```

Open 同步消费 listener/client/TLS/socket/content 配置中的 pointer，复制 source name，并借用已 started
Flow 到 stop 完成。Snapshot 只复制 portable state、bound port、计数、pending 标志、owner first error
与最近一次 peer failure，不暴露内部 slot pointer。

## 影响与权衡

- 架构：新增 server-side adapter 能力；Graph/Product 仍不依赖 CNet。
- API：纯新增 opaque ABI；现有 stream source ABI 不变，无 legacy alias 或 fallback。
- 状态：CNet 是连接主事实源；slot/snapshot 是可重建派生状态。
- 性能：每条消息一次必要的 borrowed-to-owned copy；固定 slot 和单 pending receive 限制常驻资源。
- 公平性：round-robin 防止单连接长期独占 demand，但吞吐上限受单 pending receive 约束；若 profile
  证明它是瓶颈，再以独立容量协议扩展并行 receive。
- 迁移/回滚：消费者按需链接现有 CNetAdapter 并调用新接口；回滚只需移除新调用，不改变配置格式。

## 验证

测试覆盖 C/C++ ABI、invalid bounds、TCP accept/demand/copy、连接容量与 slot reuse、round-robin、
oversize、remote close、真实 TLS loopback、TLS invalid fail-closed、TLS peer failure isolation、
stop/drain/destroy、安装消费、导出符号以及 Graph 不依赖 CNet 的二进制检查。
