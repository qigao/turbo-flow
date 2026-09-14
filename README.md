# TurboFlow

TurboFlow 是基于有向 graph 的可配置数据处理器。仓库只拥有 Graph DSL、编译与执行、配置解析、
typed projection、调度、可观测性，以及可选的边界与能力 adapter。

消息队列等完整产品及其控制面、业务 session、peer 和重连事实不属于本仓库。
具体 Source/Sink provider 可依赖它们的公开客户端 SDK；Graph/Core 不反向依赖完整产品。

`ingress/protocol` 提供独立于产品的协议 codec/runtime：OCPP、JT/T 808、GB/T 32960、CoAP
等协议完成分帧与校验后由 Source 显式映射到业务 schema。连接监听、HTTP endpoint 与网络
生命周期由 CNet/CHTTP 适配 owner 管理。MQTT 接收是 Source，发送是 Sink，不是内部消息格式。

目标数据面为 **Source DLL → 统一业务 schema → 内存/数据库接收存储 → RulesForge/TurboScript 图 → Sink DLL**：
相同业务数据经 HTTP/WS/socket/MQTT 进入后复用同一处理图，输出目的地由显式绑定或业务规则决定，
不由输入协议自动推导。接收存储是各 Source 共用的能力，配置选择内存或数据库，接纳成功后才
执行业务图；数据库失败不回退内存。中间业务查询/存储通过引擎调用受控的 TurboDB 等能力。
协议 ACK、图完成与业务提交分别计量；延迟 ACK 不是普通 Source/Sink 的必需能力。
`TurboFlow::Graph` 已提供 version 1 的统一 inbox vtable 与有界内存 provider：Source 成功接纳后
provider 拥有 correlation/payload 副本，Graph 通过唯一 claim 借用不可变记录；失败记录只有显式
retry 才重新可见。该内存实现不提供崩溃恢复，TurboDB inbox 和配置到 provider 的绑定仍由 #118
继续跟踪，且数据库错误不得触发内存 fallback。
产品配置以有序 plugins: [{id, version, path}] 清单声明绝对 DLL 路径。PluginHost 只通过
turbo_flow_plugin_get_api 取得 root vtable，并在插件 load() 前精确核验 ID/版本；任一 DLL
失败会回滚整个新宿主，不搜索替代 DLL，也不切换静态实现。
目标、现状与迁移风险见[协议无关业务图设计](docs/architecture/transport-independent-business-graph.md)；
真实 Flowie/FlowMQ provider 和完整引擎 DLL 仍分别由 #115/#74/#73 跟踪。

## 构建边界

| CMake target | 职责 |
| --- | --- |
| `TurboFlow::Config` | 解析并校验产品配置，生成只读 resolved config |
| `TurboFlow::Graph` | Graph DSL、编译、执行和通用 operation/adapter API |
| `TurboFlow::Product` | 用 resolved config 装配 Graph 与本仓库 adapters |
| `TurboFlow::PluginHost` | 通过统一 DLL vtable 事务注册 Product/Protocol/Business capabilities，编译 Graph generation，并以 lease 保护模块生命周期 |
| `TurboFlow::ProtocolIngress` | 可选 protocol codec/runtime，不依赖 MQTT broker |
| `TurboFlow::ProtocolIngressGraph` | 将中立协议消息投递到 `TurboFlow::Graph` |
| `TurboFlow::CNetAdapter` | 可选 CNet Source/Sink owner；拥有 transport progress 与有界请求状态 |
| `TurboFlow::CHTTPAdapter` | 可选 CHTTP client、deferred server 与 WebSocket Flow Source/Sink |
| `TurboFlow::TurboDbAdapter` | 可选 TurboDb ORM Source；将 typed Publisher 的每一行转为 managed message projection |

所有构建开关只在 `CMakeOptions.cmake` 声明。不得在子目录新增隐藏 option，也不得把外部产品源码、
协议状态机或安装组件重新并入本仓库。

TurboFlow 2.0 移除了聚合 `TurboFlow::Flow`、`turbo_flow` 动态库及
`turbo_flow_config.h` 伞头。消费者必须按所用 API 链接 `Config`、`Graph`、`Product`
或其他明确组件，并直接包含其所属头文件。

## DLL Graph generation

Callback stage 必须在 DSL 中写出 `operation`，并通过
`turbo_flow_register_operation()` 和 `turbo_flow_register_operation_provider()` 分别注册
契约和实现。例如 `stage output operation consumer.discard` 绑定的是
`consumer.discard`，不是节点名称 `output`。同一 operation 可由不同节点复用；缺少
descriptor/provider 或省略绑定会在 compile 时失败。完整公开 API 用法见可编译的
[安装消费示例](tests/install_chttp_plugin_consumer/main.c)；它不依赖测试 fixture 或私有 Graph。
Source、routing port 和 adapter-owned consume 仍可使用内建契约。

Gateway 的外部能力只通过 `TurboFlow::PluginHost` 加载：CNet、CHTTP、TurboDB、FlowMQ、
RulesForge/TurboScript 与控制/Raft bridge 均注册 size/versioned 的纯 C vtable，不由 Graph 静态选择
实现。一次 generation 先冻结 catalog snapshot，对所有 Graph 引用执行无副作用 preflight，并预留
有界 owner 存储；全部成功后才消费 parsed Graph，按 resource、adapter 顺序 materialize 并 compile。

materialize 开始后失败会先销毁整张新 Graph，使 adapter shutdown/detach 完成，再通过 DLL owner
vtable 逆序回收已转移对象；不会调用 legacy Product provider 兜底。插件 ABI 2.0 要求可能产生
external-poll owner 的 DLL 在 root API 声明 `TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL`，并通过
`turbo_flow_plugin_product_owner_publish()` 按 host 预置容量发布完整 descriptor；ABI 1.x
插件在 load/register 前拒绝，旧短 host config、owner 布局及兼容 padding 已移除。声明
`EXTERNAL_POLL` 的 owner
由 Gateway 控制线程调用 `turbo_flow_plugin_generation_poll()` 推进：每轮每个 owner 至多一次，只有轮转
首位获得该轮总等待预算，其余均为零等待；不声明者是 lifecycle-only owner。运行方以 generation lease
包住 CFlow run 和异步 callback。退休顺序为
owner quiesce、Graph stop、owner drain、owner shutdown、Graph destroy、owner destroy、snapshot release；
生命周期失败保留可重试状态，但 retirement 一旦开始就不再接受 poll。DLL 在最后一个
snapshot/lease 释放前不可卸载。

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
find_package(TurboFlow 2.0 CONFIG REQUIRED COMPONENTS TurboDbAdapter)
target_link_libraries(app PRIVATE TurboFlow::TurboDbAdapter)
```

## CHTTP 异步阶段

`TurboFlow::CHTTPAdapter` 将 HTTP request 建模为 CFlow async `flat_map`：成功接纳后保留当前
异步 publication，CHTTP terminal callback 再产生零或一个 owned response。一个 caller-owned
client 独占 submit/poll/cancel 与 H1/H2 connection state；容量、protocol、TLS、retry、overall
deadline 和 shutdown 都是显式契约，不提供旧 HTTP 实现或协议降级 fallback。使用方应在 Graph
compile 前注册 client、start 后由一个 owner thread 调用 `turbo_flow_chttp_client_poll()`，并在
Flow destroy/detach 后销毁 client。

CHTTP 来自独立的 Chttp SDK。构建和安装消费环境必须令 `HTTP_SERVICES_ROOT` 指向与当前
Debug/Release profile 匹配的安装前缀；TurboFlow 严格从该根查找 `Chttp`，adapter 直接链接
`CHttp::Client` 与 `CHttp::Server`。旧 `Salts::CHTTP`、`salts_chttp*.dll`、默认搜索路径和
跨 profile 复用均不受支持。应用只通过 `TurboFlow::PluginHost` 加载 provider；Gateway 不应
直接导入 CHTTP adapter 或任一 Chttp native DLL。

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
ctest --preset win-release-user
cmake --build --preset install-win-release-user
```

安装后的 `TurboFlowConfig.cmake` 导出上述 targets 以及本仓库实际构建的 adapters；不会查找或导出
外部协议产品组件。仓库内的 install-consumer CTest 会再用各自版本化 user preset 和 manifest
从 `$env{PKG_ROOT}/turboflow/debug|release` 查找已安装包，并分别验证组件选择、C/C++ consumer、
Gateway/provider/adapter 依赖层次及 operation fixture；它不会用源码 build-tree package 代替
安装证据。

Windows 原生闭包测试使用系统内置 Windows PowerShell 与其
`Microsoft.PowerShell.Security` 模块；配置阶段会显式验证该前置条件。测试以只读方式结合
Windows Resource Protection 与 Authenticode 区分 OS 组件、当前 profile 的 Microsoft CRT
和普通应用 DLL，模块或 trust 查询失败时直接拒绝，不回退到仅按目录或发布者放行。
签名非 OS 负例直接读取当前 `VsDevCmd` 的 `dumpbin` 同目录下 `msobj140.dll`，不复制 DLL。
启用这些测试的主机必须提供该 MSVC 文件及有效的 Microsoft Corporation 签名，且
System32/SysWOW64 中不能有同名文件抢先解析；配置阶段显式检查这些条件。工具目录必须
属于当前 `VCToolsInstallDir`。MSVC 布局变化导致此前置条件不满足时，须更新测试主机或
fixture 契约；这不是产品运行依赖。负例须实际报告有效签名与 `Identity=NonOS` 才通过。

## 形式化模型

[形式化模型规范](docs/FORMAL_FLOW_MODEL.md) 与 [Lean 验证入口](formal/README.md) 描述核心路由、fan-in 与生命周期的抽象证明。它不是对 C 源码、编译器输出或并发内存模型的 refinement proof。
