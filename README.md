# TurboFlow

TurboFlow 是基于有向 graph 的可配置数据处理器。仓库只拥有 Graph DSL、编译与执行、配置解析、
typed projection、调度、可观测性，以及可选的通用存储 adapter。

消息队列等完整产品及其控制面、业务 session、peer 和重连事实不属于本仓库。
这些产品可以调用 TurboFlow，但 TurboFlow 不反向依赖它们。

`ingress/protocol` 提供独立于产品的协议 codec/runtime：OCPP、JT/T 808、GB/T 32960、CoAP
等协议完成分帧与校验后可直接进入 Graph。连接监听、HTTP endpoint 与网络生命周期不在本仓库；
后续集成只允许通过 CNet/CHTTP 的宿主适配层进入。MQTT 只是可选 Sink，不是内部消息格式。

## 构建边界

| CMake target | 职责 |
| --- | --- |
| `TurboFlow::Config` | 解析并校验产品配置，生成只读 resolved config |
| `TurboFlow::Graph` | Graph DSL、编译、执行和通用 operation/adapter API |
| `TurboFlow::Product` | 用 resolved config 装配 Graph 与本仓库 adapters |
| `TurboFlow::Flow` | 兼容聚合 target；新代码优先链接最小 target |
| `TurboFlow::ProtocolIngress` | 可选 protocol codec/runtime，不依赖 MQTT broker |
| `TurboFlow::ProtocolIngressGraph` | 将中立协议消息投递到 `TurboFlow::Graph` |
| `TurboFlow::MqttSink` | 批量映射中立消息；不拥有 codec/session 或任何 I/O connection |
| `TurboFlow::CNetAdapter` | 可选 CNet Source/Sink owner；拥有 transport progress 与有界请求状态 |
| `TurboFlow::CHTTPAdapter` | 可选 CHTTP client、deferred server 与 WebSocket Flow Source/Sink |
| `TurboFlow::TurboDbAdapter` | 可选 TurboDb ORM Source；将 typed Publisher 的每一行转为 managed message projection |

所有构建开关只在 `CMakeOptions.cmake` 声明。不得在子目录新增隐藏 option，也不得把外部产品源码、
协议状态机或安装组件重新并入本仓库。

## Reactive run

`TurboFlow::Graph` 公开基于 CFlow Publisher 的有界、按 demand 推进的 run。Publisher 必须使用
`turbo_flow_message_type()` 返回的 managed CMeta 描述符。成功 open 后 Publisher 所有权转移给
run；失败则仍归调用方。`NULL` Scheduler 使用 Flow 自有的有界 worker Scheduler，显式传入的
Scheduler 借用至终态。`WAIT` 只由有效 waker 恢复，cancel/stop 会注销等待；queue full 与 closed
分别返回 `SALTS_ENOSPC` 与 `SALTS_ESHUTDOWN`。

```c
#include <cflow/publishers.h>
#include <turbo_flow.h>

static int publish_one(turbo_flow_t *flow) {
  turbo_flow_msg_t message;
  cflow_publisher publisher = {0};
  turbo_flow_run_t *run = NULL;
  turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;

  turbo_flow_msg_init(&message);
  message.id = 42u;
  if (!cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &message, 1u))
    return SALTS_ENOMEM;
  int rc = turbo_flow_run_open(flow, "input", &publisher, NULL, &run);
  if (rc == SALTS_OK) rc = turbo_flow_run_request(run, 1u);
  if (rc == SALTS_OK) rc = turbo_flow_run_wait(run, UINT64_MAX, &result);
  turbo_flow_run_close(run);
  if (cflow_publisher_valid(&publisher)) cflow_publisher_destroy(&publisher);
  turbo_flow_msg_cleanup(&message);
  return rc;
}
```

`turbo_flow_publish()` 仍是同步 facade，但其执行路径同样经过 one-value Publisher、Subscription
和 inline Scheduler；不存在旧 native fallback。

## TurboDb ORM Source

桌面 user presets 会启用 `TurboFlow::TurboDbAdapter`，并严格要求与构建类型匹配的
`TURBODB_ROOT`。核心 `TurboFlow::Graph` 不依赖 TurboDb；不需要数据库接入的构建可显式设置
`TURBO_FLOW_BUILD_TURBODB_ADAPTER=OFF`，此时 CMake 不查找 `TurboDB`。
启用时从 `TURBODB_ROOT` 查找 `TurboDB` package，并使用它公开的 `Orm::C` target。

适配器直接包装 `orm_query_open_flow()` / `orm_query_open_command_flow()` 返回的 typed CFlow
Publisher。每次 downstream demand 只拉取一行，将 CMeta 行值绑定为 message-owned projection；
不物化 rowset、不转 JSON，也没有旧数据库路径 fallback。`WAIT`、waker、cancel 与 terminal
状态保持原语义。调用方提供的 `turbo_flow_data_schema_t` 及其字符串、ORM query/connection、
row shape（以及事务版本中的 transaction）必须比 Publisher 和所有派生消息活得更久。

TurboDB 2.3.4 的驱动由显式 `orm_runtime_t` 加载；调用方先创建 runtime、加载对应
driver module，再用 `orm_runtime_connect()` 建立连接。关闭顺序为 run/Publisher、query、
transaction、connection，最后 checked-close 并释放 runtime。
容量或 row shape 深度不满足契约时，当前 SDK 在 Publisher 接纳前返回错误，输出保持为空；
实际行值转换失败仍通过运行中的 Publisher 传播。正式测试覆盖这两类失败及事务释放。
TurboDB 2.3.4 发布 SDK 移除了 TidesDB/TidesSQL；事务生命周期回归使用 SQLite 内存库，
保留 Publisher 占用时 commit 返回 BUSY，以及关闭、取消、绑定失败后释放事务的覆盖。
本工程不依赖已移除的驱动；Tides 组件需由其消费方显式源码构建。
参见 [TurboDB 2.3.4 发布说明](https://github.com/qigao/turbodb/releases/tag/v2.3.4)。

安装后按需请求组件：

```cmake
find_package(TurboFlow 1.0 CONFIG REQUIRED COMPONENTS TurboDbAdapter)
target_link_libraries(app PRIVATE TurboFlow::TurboDbAdapter)
```

## CHTTP 异步阶段

HTTP 来自独立的 [Chttp SDK](https://github.com/qigao/chttp)。配置和安装包消费时，
`HTTP_SERVICES_ROOT` 指向匹配的安装 profile：`http-services/debug` 或
`http-services/release`，Android 使用 `http-services-android/<profile>`。
`TurboFlow::CHTTPAdapter` 公开链接 `CHttp::Client` 和 `CHttp::Server`，使用
`<http_client/http.h>` 与 `<http_server/http.h>`；Salts 提供 CNet 与 CFlow。

`TurboFlow::CHTTPAdapter` 将 HTTP request 建模为 CFlow async `flat_map`：成功接纳后保留当前
异步 publication，CHTTP terminal callback 再产生零或一个 owned response。一个 caller-owned
client 独占 submit/poll/cancel 与 H1/H2 connection state；容量、protocol、TLS、retry、overall
deadline 和 shutdown 都是显式契约，不提供旧 HTTP 实现或协议降级 fallback。使用方应在 Graph
compile 前注册 client、start 后由一个 owner thread 调用 `turbo_flow_chttp_client_poll()`，并在
Flow destroy/detach 后销毁 client。

宿主应串行执行生命周期操作，并检查 `turbo_flow_stop()` 的结果。stop 失败时
Flow 保留 plan、registry 和失败的 adapter binding，需要显式重试；成功停止的 binding
不会重复调用。`turbo_flow_destroy()` 内部 stop 失败时同样保留 Flow，已有待重试错误时
记录 `SALTS_EBUSY` 并返回。此时必须保留 Flow 指针和 adapter context，停止成功后再
调用 destroy；需要返回码的调用方应先显式 stop。成功 destroy 后指针失效。

核心在调用 adapter start 前登记其回滚归属；start 失败时也会调用该 binding 的
stop。适配器的 stop 必须支持部分初始化或已清理状态；没有残留资源时返回成功，
有残留资源且清理失败时报告错误，供宿主显式重试。失败的 start 不再要求适配器
内部无限等待清理，公开函数签名和 struct 布局保持不变。

CNet stream/datagram/packet sink 每次 Flow stop 只调用一次原生停止，等待参数为 `stop_timeout_ms`；原生停止或销毁
失败会返回错误并保留尚未释放的 owner。原生停止失败时，仍挂起的 Flow claim
以 `SALTS_ESHUTDOWN` 完成；CNet 自持的发送存储（stream retained buffer 或 UDP
或 KCP copy slot）、IO Actor token 和回调 context 保留到真实原生终态。此时 snapshot
的 `active_requests` 可能仍非零，Flow claim 已完成；显式 stop 重试不会重复
发布完成结果。

CNet stream/listener/packet source 的 `open()` 失败后也必须检查输出 handle：普通失败且回滚完成时
输出为 NULL；若原生清理失败，则返回清理错误和仍拥有资源的非 NULL source。
snapshot 的 `status/error_stage` 保留最初的 open 错误。调用方保留 source 及其 Flow，
显式重试 stop，成功后再 destroy；失败 owner 只允许 snapshot、stop 和 destroy。
此异常路径的调用方需要同步更新，不能再假设“open 返回错误就没有待释放资源”。
Stream/listener stop 的非超时 progress 错误可能与完全清理同时发生；以 snapshot 的 STOPPED
状态确认可以 destroy，其他状态保留 owner 重试。Open 回滚若已清理完成，仍返回最初 admission
错误和 NULL，不凭单个 stop 错误制造无待清理资源的 owner。

## CHTTP WebSocket 数据流

`TurboFlow::CHTTPAdapter` 也可注册一个 WebSocket `SOURCE|SINK`：CHTTP 独占
HTTP/1.1 Upgrade、WSS/TLS、RFC 8441 Extended CONNECT、subprotocol 与 CNet WebSocket
engine；callback-borrowed frame 会复制成 message-owned Flow 输入。消息的 `type` 表达
text/binary/ping/pong/close，版本化 transport context 保存 generation-checked CHTTP session。

session/frame/byte 容量均为硬边界。Flow ingress 满时只关闭受影响 session，CHTTP command
queue 满时保留 `SALTS_ENOBUFS`，stale 与 duplicate close 分别保留
`SALTS_ENOENT`/`SALTS_EALREADY`。不存在 H1、明文、generic socket 或 legacy transport
fallback。完整所有权与关闭顺序见
[`io/chttp/ADR_CHTTP_WEBSOCKET_FLOW.md`](io/chttp/ADR_CHTTP_WEBSOCKET_FLOW.md)。

## Windows 验证

Windows 使用 [qigao/vcpkg-cache](https://github.com/qigao/vcpkg-cache) 的
`qigao-x64-windows-v145` triplet、MSVC 14.51.36231 和 Windows SDK 10.0.26100.0。
当前工具链 contract 的 vcpkg scripts SHA 是 `04a9d8e5212d01ee1dd9478eadd9caade4f8b0d4`；
`VCPKG_ROOT` 指向独立的已 bootstrap checkout，避免改变其他工程的工具链。
共享 GitHub Packages feed 只读，本地二进制缓存可读写。配置前从仓库忽略的 `.env`
加载 `GITHUB_TOKEN` 到父进程；NuGet config 只引用该环境变量，不保存凭据。

本次 SDK 迁移基于 Salts.Native `2.3.0-rc.10`、SaltsUtils.Native `4.3.0-rc.8`、
CHttp.Native `2.1.0-rc.3`、RulesForge.Native `0.9.3-rc.2` 和 TurboDB.Native `2.3.4`。
这些版本包含预发布包，尚不能代替稳定版发布验收。安装前核对各 SDK 的 `.qigao-sdk.json`
与所选 profile；不以 Release package 替代缺少的 Debug SDK。

CHttp 的安装契约要求 SaltsUtils `Crypto` component，其静态链接闭包依赖
vcpkg-cache 的 GmSSL `3.2.0#9`（Apache-2.0）；manifest 通过既有 overlay 接入。
不再直接引入没有消费方的 BoringSSL。TLS 实现与线程语义仍由 SDK 持有，
本工程不替换加密实现；缺少 GmSSL 时配置直接失败。

线程、同步和平台调用使用 SDK 的 `cmeta_*` 符号，协程使用 `coro_*`。
协议代码生成使用 SaltsUtils 的 `salts-idlc`，preset 设置
`TURBO_FLOW_IDLC_HOST_EXECUTABLE`；旧 `TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE` 已移除。
这些迁移保留 Flow 的状态、容量、错误传播和消息所有权语义，不添加旧 SDK 兼容层。
回滚时应同时恢复此前的源码、preset 和匹配 SDK，不能混用两个 ABI。

DataBind3 要求 schema 中固定大小的字段先于可变字段声明；resource document 已调整
声明顺序，既有 JSON 字段名和输出顺序保持稳定。OCPP bindings 由当前 `salts-idlc`
重新生成，使用 `data_bind_serialized_free()` 释放序列化结果。
Windows 的 runtime 同步最后复制所选 `SALTS_ROOT`，避免 TurboDB 等 SDK 捆绑的
不同 Salts/CNet DLL 覆盖本次选定版本。

在 VS 2026 的 x64 developer shell 中执行：

```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user --output-on-failure
```

2026-10-11 的 Windows x64 Release 验证：完整 build graph 构建成功，CTest
44/44 通过，包含安装包消费、TLS/WSS、OCPP schema 和 TurboDB 事务生命周期测试。
Debug、Linux、macOS 与 Android 尚未在本次迁移中验证。

安装后的 `TurboFlowConfig.cmake` 导出上述 targets 以及本仓库实际构建的 adapters；不会查找或导出
外部协议产品组件。

## 形式化模型

[形式化模型规范](docs/FORMAL_FLOW_MODEL.md) 与 [Lean 验证入口](formal/README.md) 描述核心路由、fan-in 与生命周期的抽象证明。它不是对 C 源码、编译器输出或并发内存模型的 refinement proof。
