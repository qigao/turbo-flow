# CHTTP unified DLL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 完成 #71 的 client、deferred server、WebSocket 三类 DLL provider，支撑 #63/#28/#2；不把 admission 前置工作视为 DLL 完成。

**Architecture:** Gateway 只经 PluginHost/Product/Graph 装配。插件 root 管理有硬上限的 owner 集合；每个 owner 保留配置和原 adapter，generation 保留模块 snapshot。原 adapter 仍是请求、claim、session 的唯一事实源。

**Tech Stack:** Salts CHTTP/CNet、CSTL、TurboFlow transactional Product ABI、TinyTest、CMake user presets。

**Spec:** https://github.com/qigao/turbo-flow/issues/71

## Global Constraints

- 唯一 discovery export 为 `turbo_flow_plugin_get_api`，三种 kind 原子注册。
- Gateway 不链接 `TurboFlow::CHTTPAdapter`，不调用具体 adapter API。
- 未知/缺失/类型/范围/协议配置在 native client/server 创建之前失败。
- 不引入 C、CMake、协议、TLS、静态链接或直接调用 fallback。
- owner 生命周期在 Gateway 控制线程串行；client poll 由 generation 驱动，server 使用原有 CHTTP owner 线程。
- 所有 borrowed TLS 配置由插件 owner 持有至原 adapter 销毁；host allocator 分配的存储只交回 host deallocate。
- 新功能只在三类 provider 完整并通过验证后开放，不提交对用户可见的占位 provider。

## 现状与风险

事实：`io/cnet/src/turbo_flow_cnet_plugin.c` 已示范 transactional provider、bounded root、EXTERNAL_POLL 与 sole export。CHTTP 当前只有 adapter DLL，没有 provider。

HIGH：`io/chttp/src/turbo_flow_chttp.c` 的 stop 对 ETIMEDOUT 无限重试，且未调用 `turbo_flow_adapter_report_stop_status`。generation 的有限期失败退出不能建立在这条路径上。先在本分支修正，不另拆前置 issue。

事实：`flow_plugin_generation.c` 顺序为 owner quiesce、Flow stop、owner drain、owner shutdown、Graph destroy/detach、owner destroy、snapshot release。shutdown 早于 registry detach，不能提前释放 adapter。destroy 为 void，因此此前必须验证所有 native owner 已停且所有 Flow claim 已终结。

MED：原生 stop 失败可能仍保留回调。Flow claim 必须先结算，而 slot/owner 必须保留到显式 stop 重试成功；迟到回调不得再次结算或增加 completed 计数。

## Task 1: 有界 client stop 与失败保留

Files: `io/chttp/src/turbo_flow_chttp.c`, `io/chttp/tests/test_chttp_adapter.c`, `io/chttp/tests/CMakeLists.txt`。

Interfaces: 保持现有公开函数；使用 `turbo_flow_adapter_report_stop_status(flow, status)` 上报，具体 owner snapshot 为 FAILED，下一次 `turbo_flow_stop(flow)` 重试。

- [x] 增加测试专用编译目标，替换 `chttp_async_client_stop`，一次返回 ETIMEDOUT；生产 DLL 不含替换。
- [x] 使用真实 deferred request，在 stop 前确认 active_requests=1。断言第一次 Flow stop 返回 ETIMEDOUT、publication_calls=1、owner destroy=EBUSY；第二次显式 stop 成功，计数不变。
- [x] 运行故障测试，确认当前实现错误地自动重试/返回成功。
- [x] 删除无限重试；失败时终结残余 Flow claims，保留 native owner/slot storage，迟到 native completion 识别已清空 claim，不重复结算；上报 exact stop error。
- [x] 运行故障目标与现有 H1/H2 client 回归；检查 stop 后再次 start 保持既有 EALREADY，不复用陈旧请求。新运行必须装配新 owner/generation。

```powershell
cmake --build --preset win-dev-user --target test_chttp_client_stop_fault test_chttp_adapter
ctest --preset win-dev-user -R 'test_chttp_(client_stop_fault|adapter)$' --output-on-failure
```

## Task 2: 完整三类配置及 DLL owner

Create: `io/chttp/src/turbo_flow_chttp_plugin_internal.h`, `turbo_flow_chttp_plugin_config.c`, `turbo_flow_chttp_plugin.c`。
Modify: `io/chttp/CMakeLists.txt`。
Test: `io/chttp/tests/test_chttp_plugin.c`。

Interfaces: 沿用 `turbo_flow_plugin_transactional_adapter_provider_v1_t` 的 preflight/materialize 和 `turbo_flow_plugin_product_owner_publish`；kind 精确为 `chttp.client`, `chttp.server`, `chttp.websocket_server`。

- [x] 先写 PluginHost loading/catalog 测试：三种 kind 同时存在，重复注册导致整个注册事务失败。Task 3 追加不同模块身份的 kind 冲突测试。
- [x] 建立 schema_version=1 的严格 resolved-config reader；字段分 common network/TLS、client request、server route/deferred、WebSocket frame/session 四组。所有容量和 timeout 为显式数值，类型不转换、不自动补全。
- [x] client 必须配置 connection_uri/authority/target/method/protocol、请求/响应/header 界限、max_attempts/idempotent/retry_delay/overall_timeout、TLS 引用/ALPN、poll budget。
- [x] server 必须配置 bind/route、H1 或显式 H1+H2 策略、H2 stream/HPACK/input/output 界限、TLS 引用/ALPN、deferred response/body/message 限额、状态码和 stop timeout。原生 server enable_http2 表示同时接受 H1/H2，不能把它宣传为 H2-only。
- [x] WebSocket 同时要求 session/frame/message/buffer 硬界限。检查 source 与 terminal stage 引用同一 adapter；client 只能用于 async transform stage。
- [x] preflight 完成所有字段、交叉容量、URI、协议/TLS/ALPN 校验后，materialize 才创建 profile/adapter。复制需要越过 resolved lifetime 的所有字符串。
- [x] root 使用有上限的 CSTL owner 集合；失败按逆序回收。注册后不能单独 unregister 时，由 generation 既有 rollback/detach 处理，绝不释放 registry 仍借用的 ctx。
- [x] client owner poll 调用原 adapter poll；server/WebSocket owner quiesce 调用 #83/#85 接口。未启动 owner 允许无副作用退出；已运行 owner 仅在原 snapshot 证明停止/无残余后通过 drain/shutdown。
- [x] sole-export DLL 私有依赖 adapter/Product/CHTTP；Windows CRT 与 TLS/profile 存储均在创建者边界释放。

Task 2 在 `65b0efc` 完成限定复审；配置词法缺口已由 RED/GREEN 测试修复。`3bd9fc4` 完整 Debug/ASan 52/52，修复后 focused plugin 13 项通过；Task 3 仍需最终双配置交付验证。client 增加原 owner mutex 管理的 quiesce/resume，不复制 admission 状态。未编译 Graph 的 terminal outgoing 仍由 compile 校验，装配失败由 generation 回收，配置字段提前失败的约束不变。

## Task 3: Gateway 测试与交付门禁

Files: `io/chttp/tests/test_chttp_plugin.c`, `io/chttp/tests/CMakeLists.txt`, `tests/install_consumer/main.c`, install consumer CMake 与检查脚本，CHTTP README/ADR。

- [x] Gateway-style consumer 仅链接 PluginHost/Product/Graph，通过 catalog/generation 使用三类能力；网络对端 fixture 可链接原生 CHTTP，但不得调用具体 Flow adapter。
- [x] 用真实 H1/H2 server 验证 client 输出、H2 多请求共享 session 与取消隔离；用原生 client 验证 deferred H1/H2 server 与 H1/RFC8441 WebSocket echo。
- [x] 遍历未知/缺失/错误类型/零容量/溢出/不匹配协议/TLS/ALPN，断言 create 前失败，原 Flow 所有权不转移。
- [x] 测试第二个 owner 装配失败回滚；外部 run/claim/callback 按契约持 caller lease，验证 teardown fast-fail；native Source 已接受请求与 WebSocket session 无额外 lease 时，经真实 owner vtable 首次 quiesce 返回 exact timeout/busy 并保留 generation/module，显式重试后终结及 owner/root 释放各一次。
- [x] 验证 DLL sole export/dependency table、安装路径动态加载、Debug/Release profile 隔离。运行 focused repeats、完整 CTest 与 install 两套门禁。
- [x] 独立审查后创建 PR 关联 #71；逐项核对 issue 验收再勾选。无实测性能数据不得宣称吞吐提升。

Task 3 在 `8c47222` 的本地验证：Debug/ASan 与 Release 各完整构建、CTest 52/52、
插件 focused 连续 10 次以及各自 install preset 通过。新增独立安装消费者，
插件测试合计 17 项，包含三类 H2/TLS 实际往返、共享连接计数、
native peer 单流取消、ACTIVE run/deferred 请求保活和真实 provider 的
allocator 故障/注册冲突 fixture。取消来源为 peer，不代表新增 Gateway client cancel API；
emit claim 的在途证据来自 accepted native 请求与尚未 terminal 的 run，
没有额外公开 claim 计数。最终审查与交付结果见下文。

Task 3 fix round 1：新增无 caller lease 的 native Source/session teardown 失败保留与重试，
fixture 在 adapter quiesce 边界注入首次超时/忙错误，仍经过真实插件 owner vtable；
caller lease fast-fail 与 owner teardown 分别提供证据。
安装 gate 精确要求 `salts_chttp-2.dll`，拒绝无版本名与其他 ABI；
独立 consumer 目录复制到 build 后编译。插件 focused 与安装 consumer 已通过，
该修复已通过限定复审，并纳入主线程最终双 profile 回归。

## 最终审查与交付

[PR #87](https://github.com/qigao/turbo-flow/pull/87) 关联 #71。全分支审查发现并修复了三类
adapter 启动失败后无法销毁 generation 的缺口：只有 native 未取得所有权或已确认清理成功时
使用 STOPPED，last_status 保留启动错误；cleanup/stop 失败仍持有 native storage 时保持 FAILED。
没有新增公开 API，也没有放宽任意 FAILED。真实占用端口与 client init 故障三条 RED 在修复后通过，
原 stop-failed 显式重试和 exact-once 测试继续通过；最终限定复审无新增问题。

最终代码 `d890f0b` 经主线程独立验证：Debug/ASan 完整 build、CTest 52/52（56.40 秒）、
插件连续 10 次（23.26 秒）和 install preset 均通过；Release 完整 build、CTest 52/52
（47.51 秒）、插件连续 10 次（10.08 秒）和 install preset 均通过。插件测试共 22 项。
完整 CTest 包括从独立复制目录构建的 installed Gateway、sole export、ABI 2、CRT 及缺失依赖门禁。
实测范围为 Windows amd64；未声称 Linux/macOS、完整 mTLS 矩阵或性能 benchmark 已验证。

## 兼容、迁移与回滚

现有 Graph DSL 保持；Gateway 部署增加统一插件及其动态依赖。旧直接 adapter 用户不自动改写配置，新 plugin 配置无旧格式 fallback。先停 admission 并结算旧 generation，再切换 catalog binding；失败保留旧模块/失败 owner，不卸载仍被引用的代码。回滚部署也必须先停止并销毁新 generation，不启用静态替代实现。

Windows 所有上述 CMake/CTest 命令在 VsDevCmd.bat 的 amd64 环境下执行。最终完整命令为两套 `cmake --build --preset <profile>`、`ctest --preset <profile> --output-on-failure`、`cmake --build --preset install-<profile>`。
