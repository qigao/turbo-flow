# CHTTP 统一插件

`tf_chttp_plugin` 以一个动态模块注册 `chttp.client`、`chttp.server`、
`chttp.websocket_server`。唯一导出是 `turbo_flow_plugin_get_api`。
Gateway 使用 PluginHost 的 catalog snapshot 和 generation 创建能力，链接
`TurboFlow::PluginHost`、`TurboFlow::Product`、`TurboFlow::Graph`。
完整可编译消费者见 [安装消费者](../../tests/install_chttp_plugin_consumer/main.c)。
消费者不需要包含具体 adapter 头文件。

## 部署与配置

Windows 安装文件为 `bin/tf_chttp_plugin.dll`，macOS 为
`lib/libtf_chttp_plugin.dylib`，其他受支持 Unix 为 `lib/libtf_chttp_plugin.so`。
必须传入实际插件路径，并部署相同 profile 的动态依赖。
Windows 的实际导入包含 `tf_chttp_adapter.dll`、版本化的
`salts_chttp-2.dll` 和其传递依赖；安装门禁检查 DLL 的真实导入表，
拒绝无版本名 `salts_chttp.dll` 和其他 ABI 版本。
Debug/ASan 与 Release 各自使用独立 build、依赖和安装前缀，不混装 CRT。
缺少插件或依赖立即报错，不尝试静态、直接 adapter 或其他目录候选。

配置位于 Product YAML 的 `adapters.<name>.config`，使用整数
`schema_version: 1`。外层 Product 版本为 `version: 1`。
每种 kind 的全部字段都必须显式提供；未知字段、缺失字段、错误类型、
整数溢出和不一致容量在创建 native owner 前失败。空字符串、空数组或零值
仅在对应字段明确允许时用于表示禁用，并不是字段缺失的默认值。
可直接复用的三份完整配置在
[chttp_plugin_fixtures.h](../../tests/install_chttp_plugin_consumer/chttp_plugin_fixtures.h)；
[字段表及范围验证](src/turbo_flow_chttp_plugin_config.c)是 schema 的代码事实源。
测试中的容量与超时是小规模 loopback fixture 的值，不是生产容量建议。

| 配置组 | 语义 |
|---|---|
| `backend`、`network_*`、`poll_budget_ms` | 显式后端、连接/命令/请求/事件数量、字节预算、超时与控制线程 poll 上限 |
| `tls_enabled`、`tls_*`、`network_tls_*` | TLS 信任、证书/私钥、服务名、ALPN 与握手/缓冲限制 |
| client 的 `connection_uri`、`authority`、`target`、`method`、`header_names/header_values` | 单一请求目标与等长 header 数组；不得覆盖协议生成的 header |
| client 的 `request_capacity`、正文/header/H2 限制、`max_attempts`、`idempotent`、超时 | 有界排队、响应所有权与显式重试策略 |
| server 的 `bind_host/bind_port`、`path`、`method`、响应/error 字段 | 字面 IP 监听、单一路径、deferred 响应与 Graph 失败映射 |
| WebSocket 的 `session_capacity`、`frame_capacity`、frame/message/input 限制、`subprotocol` | 有界 session/frame Source 与异步 Sink；子协议为空或完整 HTTP token |

client 的 `protocol` 只接受 `h1` 或 `h2`；
server/WebSocket 接受 `h1` 或 `h1_h2`，不提供单独 H2-only listener。
client URI 显式使用 `tcp://` 或 `tls://`，与 TLS 开关一致。
TLS client 的 ALPN 必须恰为 `["http/1.1"]` 或 `["h2"]`；
TLS H1 listener 必须为 `["http/1.1"]`，
TLS `h1_h2` listener 必须依序为 `["h2", "http/1.1"]`。
TLS 关闭时相应配置必须显式清空；不会改用明文或其他协议。
WebSocket H1 使用 Upgrade，H2 使用 RFC 8441 Extended CONNECT。

## 所有权、线程与失败

client 的原 adapter snapshot 是请求计数和生命周期的唯一事实源；
插件只持有 owner、配置与 TLS 存储，不复制推进另一套 client 状态。
所有 generation 的创建、poll、lease 与销毁在 Gateway 控制线程串行调用。
client 需要控制线程驱动 poll；server/WebSocket 使用原生 server 的进度模型。
TLS 路径、ALPN 和其他 native 借用的配置存储随 owner 保持到 adapter destroy。
host 分配的 root/owner 通过创建时的 host deallocate 回收。

generation 先对所有 provider preflight，再接管原 Flow 并 materialize/compile。
preflight 失败保留调用者的 Flow；接管后的失败按逆序释放已创建 owner。
Graph 终端拓扑可能在 materialize 后的 compile 才判定失败，此时同样执行回滚。
generation 保留 catalog snapshot，因而请求、session、run、claim 仍在途时，
不能提前卸载对应 DLL。对可能超出当前控制操作的 run/回调，消费者先取得
generation lease，最后一个回调和 run close 后再归还。
活跃 lease 使 generation destroy 返回 `SALTS_EBUSY`，不推进 teardown。

adapter quiesce 停止 admission，已接受的 client 工作仍按其 poll/retry 契约结算。
generation destroy 则开始完整 teardown：quiesce、Graph stop、drain、shutdown、
Graph detach、owner destroy、snapshot release。进入该流程后不再开放 generation poll；
不要把 destroy 当作暂停 admission 的 API。失败 owner 与模块保持存活，调用者处理
明确错误并在满足生命周期条件后重试，不吞掉 stop/drain 错误。

## 兼容与迁移

现有 Graph DSL 和直接 adapter API 不变；旧调用者不会自动转换成 plugin 配置。
迁移时先准备统一插件与完整 schema 配置，验证新 catalog，停止旧 admission，
结算旧 generation，再切换 binding。失败保留旧模块或失败 owner，不能卸载仍有引用的代码。
部署回滚先停止并销毁新 generation，再恢复旧部署；不启用静态替代实现。
设计取舍见 [ADR](ADR_CHTTP_UNIFIED_PLUGIN.md)。

当前不支持隐式协议选择、缺省字段补全、旧配置 fallback、H2-only listener、
插件外部并发 poll 或 Gateway 直接访问 native owner。
插件 API 未增加按 message ID 的独立 client cancel 操作。
共享 H2 的取消隔离测试由 native peer 取消单个 deferred stream；
这不能代替尚未提供的 Gateway 独立 client cancel API 证据。

## 验证

Windows 命令必须位于 VsDevCmd.bat 的 amd64 环境。使用
`cmake --build --preset win-dev-user`、
`ctest --preset win-dev-user --output-on-failure` 和
`cmake --build --preset install-win-dev-user`；
Release 对应 `win-release-user` 与 `install-win-release-user`。
focused 测试是 `test_chttp_plugin`；安装门禁是
`test_turbo_flow_install_consumer`，包含独立 Gateway 消费者、sole export、
真实依赖/CRT、缺 DLL/依赖的失败路径。
测试 fixture DLL 不安装，不作为生产 provider。

caller lease fast-fail 与 owner teardown 分别测试：
native Source 已接受请求或 WebSocket session 存活时，通过真实插件 owner vtable
在 adapter quiesce 边界注入一次 timeout/busy，验证 generation 保留、请求/session
仍可结算，以及显式重试后 owner/root 回到原 allocator 各一次。
这不代表正常 quiesce 必须返回 busy；正常 teardown 可以同步结算并安全成功。

协议测试使用真实 native peer，经插件 catalog/generation 完成 H1/H2 HTTP、
H1/RFC8441 WebSocket 和三类 H2/TLS 往返。
它们验证行为与所有权，不提供吞吐或性能提升结论。
