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
connection；drain/stop/destroy CNet client；销毁 listener 与 TLS context；最后 shutdown/destroy
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

## Open 失败的清理与迁移

原 open 回滚忽略 native stop/destroy 错误后释放 owner，可能使仍持有 callback context 的 CNet
访问失效存储。现复用正常 stop/destroy 的受检查路径：每次回滚只尝试一次 native stop（零等待），
按尚存资源依次清理；scheduler shutdown 未完成返回 `SALTS_EBUSY`，保留 scheduler 和 owner。

普通 admission 失败且回滚完成时仍返回原始错误与 NULL。若清理未完成，open 返回清理错误及
非 NULL FAILED owner；snapshot 的 `status/error_stage` 保存原始 admission 错误。调用方只允许
snapshot、显式 stop 重试和 destroy，并让 Flow 保持存活至清理完成。不得丢弃输出或用后续 open
覆盖它。Stop 发布 STOPPING 后，native 回调不再覆盖已保存的首个 owner 错误。

CNet 允许非超时 progress 错误与已完成 drain 同时发生，因此 stop 仍尝试受检查的 native destroy。
全部资源释放后发布 STOPPED，并保留本次 stop 错误供诊断；STOPPED 可以 destroy。Open 的回滚
若已到 STOPPED 则释放 owner，返回最初 admission 错误与 NULL。初始化与销毁后的存活标记取自
实际 native `impl`，避免部分销毁后重试已不存在的 client。

候选方案包括无限重试（阻塞 open）、隐藏保留 owner（调用方无法完成清理）、继续无条件释放
（生命周期不安全）；选择显式返回 owner，与 packet source 的已批准契约一致。公开签名和布局
保持不变，但失败输出语义改变。仓库调用点是正式测试和头文件示例/安装 ABI probe：测试新增
失败 owner 的 teardown，示例明确同时转交错误与 owner；外部调用方也必须审计错误分支。
回滚前须先完成所有返回 owner 的 stop/destroy，保留新调用方检查兼容旧版普通失败语义。

验证覆盖真实 native 初始化后 Graph source 名 admission 失败、stop timeout/EIO/EBUSY、重复
显式失败、client destroy 拒绝、scheduler shutdown 拒绝、原始错误保留和最终释放；还覆盖 stop
在真实 drain 后报告错误时的 NULL 输出与正常 stop 的 STOPPED 状态。故障注入仅编入私有测试
target，生产库没有测试控制入口。Listener 用真实 TLS context 补充 close、listener destroy 和 TLS destroy 的失败重试。

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
