# CHTTP 异步 Client 图阶段

## 背景与决定

HTTP request 是会跨越多次网络 progress 的 0..1 输出操作。同步 stage 会占住 Flow
worker；terminal sink 又不能把响应继续发送给下游。这里新增通用 move-only async emit
claim，并把 CHTTP request stage 编译为 CFlow `FLAT_MAP` + async barrier。成功 admission
后 publication 保持 pending，只有 CHTTP terminal callback 才以 owned response 恢复下游或
返回 terminal error。

`TurboFlow::CHTTPAdapter` 拥有一个长生命周期 `chttp_async_client`，它是连接池、H1
复用和 H2 multiplex 的唯一事实源。Flow executor 线程只把 claim 移入按
`request_capacity` 预分配的 slot；调用 `turbo_flow_chttp_client_poll()` 的 owner 线程
独占 CHTTP 的 submit、poll 与 cancel。Flow stop 会先阻止 admission、等待正在运行的
owner poll 退出，再由同一 owner boundary drain CHTTP，保证每个 accepted claim 恰好一次
terminal。

关闭时每次 Flow stop 只调用一次 native stop，使用 `stop_timeout_ms`，不在 adapter
内部无限重试。stop 或 destroy 错误通过 `turbo_flow_adapter_report_stop_status()` 返回
Flow，失败的 binding、native client 和 TLS/context 借用继续存活，等待宿主显式重试。
报告前，未提交及仍持有的 active Flow claims 以 `SALTS_ESHUTDOWN` 完成；CHTTP 已在
submit 时复制 body，剩余 native callback 只借用保留的 slot。slot 进入内部 DRAINING
状态，不复用、不生成响应，也不重复完成 publication，直到 native terminal 释放该借用。
成功关闭并解除 registry 绑定后才释放 client，并清空它借用的 Flow 指针。

snapshot 的 `completed_requests` 记录 Flow claim 完成数；`active_requests` 仍计入
DRAINING slot，明确区分逻辑完成与 native quiescence。没有增加公开结构字段或改变 ABI。

## API、状态与错误语义

- adapter registration 复制 URI、authority、target、headers 和 `chttp_client_config`；TLS
  profile 借用至 opaque client 成功销毁。
- 输入 message 的 flat payload 是 request body。CHTTP 在 submit 返回前复制它，因此可安全
  retry；streaming body/sink 未暴露，不能被误当作 replayable。
- 响应 status、HTTP 版本、header 数、protocol、keep-alive 和 attempt 数存入
  message-owned response context；reason、headers 和 body 位于同一 `mem_buffer_t`。
- GET/HEAD/PUT/DELETE 默认可 retry；POST/PATCH 只有显式 `idempotent` 才可 retry。retry
  同时检查 transport failure class、attempt 上限和首次 admission 建立的整体 deadline；
  deadline 不会在 retry 时重置，owner poll 的阻塞时间也会裁剪到最近的整体 deadline。
- protocol、TLS scheme 和 H2 配置必须显式一致；不做 H2→H1、TLS→plaintext 或 retry
  fallback。
- 同一 adapter 的活动 message id 必须唯一；容量满返回 `SALTS_ENOBUFS`，重复 id 返回
  `SALTS_EALREADY`，取消通过 message id 请求并在后续 poll terminal。

图 DSL 仍使用既有 `stage <name> adapter <registered-name>` 语法，因此不改变文本格式；
变化只在宿主注册的新 CHTTP adapter 与异步执行语义。

## 权衡、迁移与回滚

预分配 slot 和一次响应聚合复制增加固定内存与一次 copy，换取明确容量、跨线程 claim
安全和 callback 结束后的稳定响应所有权。H1/H2 状态不在 TurboFlow 镜像，避免连接状态
双写。使用方链接 `TurboFlow::CHTTPAdapter`，在 compile 前注册、start 后持续 poll，并在
Flow destroy 后销毁 opaque client。

回滚只需移除 adapter 注册和 target linkage；core async emit API 是 transport-neutral，
可继续服务数据库或 RPC operation。CHTTP server 的 HTTP/2 deferred response 边界由
`ADR_CHTTP_DEFERRED_SERVER.md` 定义；client adapter 不提供协议 fallback。

## 验证范围

TinyTest 覆盖六种 method、owned response、容量、取消、retry/deadline、shutdown exactly
once、长 poll 的 deadline 边界、显式协议错误、H1 stale connection 重连，以及同一 H2
connection 上失败 stream 与成功 sibling 的隔离；安装消费测试验证导出 header 和
`TurboFlow::CHTTPAdapter` target。CHTTP 自身协议 parser、TLS/ALPN 和 RST_STREAM wire 细节
仍由独立 Chttp 的测试作为一手契约验证。私有关闭 fault admission 仅用于正式测试：
注入 stop I/O 错误、timeout 及 destroy BUSY，验证错误传播、owner 保留、显式重试；
真实 deferred HTTP 请求与 queued 请求共同验证关闭失败后 claims 恰好完成一次，
后续真实 native stop 的 callback 不产生第二次 publication。
