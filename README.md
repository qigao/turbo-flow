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
`TURBO_FLOW_BUILD_TURBODB_ADAPTER=OFF`，此时 CMake 不查找 `Orm`。

适配器直接包装 `orm_query_open_flow()` / `orm_query_open_command_flow()` 返回的 typed CFlow
Publisher。每次 downstream demand 只拉取一行，将 CMeta 行值绑定为 message-owned projection；
不物化 rowset、不转 JSON，也没有旧数据库路径 fallback。`WAIT`、waker、cancel 与 terminal
状态保持原语义。调用方提供的 `turbo_flow_data_schema_t` 及其字符串、ORM query/connection、
row shape（以及事务版本中的 transaction）必须比 Publisher 和所有派生消息活得更久。

安装后按需请求组件：

```cmake
find_package(TurboFlow 1.0 CONFIG REQUIRED COMPONENTS TurboDbAdapter)
target_link_libraries(app PRIVATE TurboFlow::TurboDbAdapter)
```

## CHTTP 异步阶段

`TurboFlow::CHTTPAdapter` 将 HTTP request 建模为 CFlow async `flat_map`：成功接纳后保留当前
异步 publication，CHTTP terminal callback 再产生零或一个 owned response。一个 caller-owned
client 独占 submit/poll/cancel 与 H1/H2 connection state；容量、protocol、TLS、retry、overall
deadline 和 shutdown 都是显式契约，不提供旧 HTTP 实现或协议降级 fallback。使用方应在 Graph
compile 前注册 client、start 后由一个 owner thread 调用 `turbo_flow_chttp_client_poll()`，并在
Flow destroy/detach 后销毁 client。

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

```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user --parallel
ctest --test-dir build/Msvc-Release --output-on-failure
```

安装后的 `TurboFlowConfig.cmake` 导出上述 targets 以及本仓库实际构建的 adapters；不会查找或导出
外部协议产品组件。

## 形式化模型

[形式化模型规范](docs/FORMAL_FLOW_MODEL.md) 与 [Lean 验证入口](formal/README.md) 描述核心路由、fan-in 与生命周期的抽象证明。它不是对 C 源码、编译器输出或并发内存模型的 refinement proof。
