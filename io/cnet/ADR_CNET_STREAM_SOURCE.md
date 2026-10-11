# ADR: CNet Stream Source Owner

## 状态

Accepted，适用于 issue #18；listener、packet endpoint 与异步 sink 分别由 #19、#20 扩展。

## 背景

TurboFlow Graph 已通过 `turbo_flow_run_open()` 接受拥有 `turbo_flow_msg_t` 的 CFlow
Publisher。CNet stream client 则是 caller-driven 的单一 progress owner：`cnet_receive()`
只登记需求，borrowed receive view 只在 callback 内有效，连接、超时与关闭终态由
`cnet_client_poll()` 驱动。

不能直接使用 `cflow_publisher_from_io_actor()`，因为该适配器只接受 trivial-copy / trivial-
destroy value，而 `turbo_flow_msg_t` 拥有 `mem_buffer_t`。也不能在 Graph sink 的同步
`consume` callback 中等待网络终态；该缺口归 #20，不在 source owner 中引入阻塞等待。

## 决策

新增可选 target `TurboFlow::CNetAdapter`。它位于 Graph 边界之外并公开一个 opaque
`turbo_flow_cnet_stream_source_t` owner；`TurboFlow::Graph`、`TurboFlow::Config` 与
`TurboFlow::Product` 均不链接 CNet。

一个 owner 独占以下状态：

- 一个 `cnet_client` 和 generation-checked `cnet_connection`；
- 一个 caller-driven CFlow manual Scheduler；
- 一个构造 managed `turbo_flow_msg_t` 的 CMeta Publisher；
- 一个 `turbo_flow_run_t`；
- 最多一个已登记 receive，以及最多一个 callback 内复制完成、等待 Publisher move 的 message。

所有公开操作均由同一 owner thread 串行调用。`request()` 只增加 downstream-value demand；
实际 `cnet_receive(..., 1)` 仅在 `poll()` 驱动 Scheduler、Publisher 观察到正 demand 且没有
pending receive/message 时发生。此切片不声称 MPSC request；跨线程 producer 必须在后续能力中
通过明确的 bounded mailbox 接入，不能并发调用 manual Scheduler。

`poll()` 的固定顺序是：

1. 在配置的 step 上限内驱动 Scheduler，让已有 demand 登记一次 receive；
2. 调用一次 `cnet_client_poll()`；
3. callback 返回后再次在同一 step 上限内驱动 Scheduler，让 owning message 进入 Graph。

因此 Graph 永不读取 CNet borrowed view。`on_receive` 在返回前从 Salts buffer pool 取得容量不超过
`max_message_bytes` 的 buffer，复制字节、设置 used length，并创建 message-owned payload view。
如果 view 类型不是 stream bytes、大小为零、超过上限、重复到达或分配失败，owner 立即进入
FAILED，保存原始 Salts status，并关闭连接；不丢弃、不截断、不扩容、不 fallback。

TCP、TLS 与 Pipe 只由显式 `tcp://`、`tls://`、`pipe://` URI 选择。其他 scheme 在触碰网络前
返回 `SALTS_ENOTSUP`。TLS URI 可携带调用期借用的 `cnet_tls_client_config`；CNet 在 connect
admission 内同步消费配置。TLS 验证失败保持 CNet 原始终态，绝不改用 TCP。

## 公开契约

头文件 `turbo_flow_cnet.h` 提供：

```c
typedef struct turbo_flow_cnet_stream_source_s turbo_flow_cnet_stream_source_t;

int turbo_flow_cnet_stream_source_open(
    const turbo_flow_cnet_stream_source_config_t *config,
    turbo_flow_cnet_stream_source_t **out_source);
int turbo_flow_cnet_stream_source_request(
    turbo_flow_cnet_stream_source_t *source, size_t demand);
int turbo_flow_cnet_stream_source_poll(
    turbo_flow_cnet_stream_source_t *source, uint32_t timeout_ms,
    turbo_flow_cnet_stream_source_snapshot_t *snapshot);
int turbo_flow_cnet_stream_source_snapshot(
    const turbo_flow_cnet_stream_source_t *source,
    turbo_flow_cnet_stream_source_snapshot_t *snapshot);
int turbo_flow_cnet_stream_source_stop(
    turbo_flow_cnet_stream_source_t *source, uint32_t timeout_ms);
int turbo_flow_cnet_stream_source_destroy(
    turbo_flow_cnet_stream_source_t *source);
```

Config 与 snapshot 都使用 `size` + `version`。Open 同步消费所有 config pointer，但借用
`flow` 直到成功 stop。`flow` 必须已经 started，且其 DSL 必须包含 `source_name`。初始 message ID
必须非零；递增溢出是 `SALTS_ERANGE` 终态。Snapshot 复制状态、portable status/native status、
generation handle、消息/字节计数、receive pending 和 run outstanding demand，不返回内部指针。

Stop 顺序固定为：取消并关闭 run → 停止、drain 并 destroy CNet → drain/shutdown/destroy
Scheduler → 标记 STOPPED。已进入 FAILED 的 owner 仍必须允许 stop 释放资源；destroy 在
stop 前返回 `SALTS_EBUSY`，成功后释放 opaque owner。

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
target，生产库没有测试控制入口。Stream 补充 Graph run 成功打开后 connect admission 失败的 run 释放验证。

## 影响与权衡

- 架构：将“宿主适配层必须在仓库外”收窄为“必须在 Graph target 外”；仓库可提供独立可选
  adapter target，但核心依赖方向不变。
- API：只新增组件，不修改现有 Graph/Config/Product ABI，也不恢复任何 CoroNet/TurboNet 名称。
- 状态：CNet client 是连接事实源；Publisher slot 只是从 callback 派生的有界 owning handoff。
- 性能：每个 receive 做一次必要的 borrowed-to-owning copy；不在未 profile 前增加第二队列或对象池。
- 能力：该切片把每个 CNet stream receive chunk 当作一条 Graph message，不提供 framing、重连、
  listener、UDP/KCP 或 sink；它们分别属于 protocol runtime、#19 与 #20。

## 验证

- 真实 loopback TCP：zero demand、逐条 demand、两条消息、remote close；
- callback 返回后 Graph 才消费 owning payload；
- invalid config/scheme、oversize、连接失败、重复生命周期、stop/drain；
- TLS 未配置有界存储与无效 CA 时 fail closed；真实平台 Pipe 的 zero-demand/demand handoff；
- C/C++ header、target dependency、Release、Debug/ASan、安装消费；
- `rg.exe` 扫描核心 target 不链接 CNet，且不存在旧 API/alias/fallback。
