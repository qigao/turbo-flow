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
