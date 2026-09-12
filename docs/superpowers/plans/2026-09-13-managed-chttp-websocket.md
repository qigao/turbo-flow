# Managed CHTTP WebSocket Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将现有 CHTTP WebSocket server 完整接入 managed Source/Sink 观察与控制。

**Architecture:** 在现有 owner/mutex/session/frame slots 上使用已公开的 atomic managed async-terminal registration。managed counters 记录 publication 契约，native counters 保持原义；pending publications 只是稳定快照的 accounting barrier，不是另一份会话事实源。控制仍使用现有 quiesce/resume 与原生 close admission，失败不回滚已发生的 admission 状态变化。

**Tech Stack:** C11、Salts/CFlow、standalone CHttp、既有 xxHash、TinyTest、Windows CMake presets、生产插件 DLL。

**Spec:** [TurboFlow #113](https://github.com/qigao/turbo-flow/issues/113)，继承 [#28](https://github.com/qigao/turbo-flow/issues/28)，基于已合并 [#112](https://github.com/qigao/turbo-flow/pull/112)。离线副本在本计划 SDD workspace 的 `issue-body.md`。

## Global Constraints

- No C/CMake fallback or new public API.
- Keep the same native owner, mutex, session/frame slots, plugin ABI and HTTP/WebSocket wire/configuration.
- One managed SOURCE|SINK, IO_TRANSPORT/CONNECTION, only QUIESCE|RESUME; capability flags0.
- Existing PROTOCOL_DATA/OPAQUE profile, application/octet-stream, declared CHTTPWebSocketCommand and CHTTPWebSocketEvent Frame/v1 schemas.
- Resource domain 为 IO_TRANSPORT；input/output content domain 为 PROTOCOL_PATTERN，以满足既有 profile/domain 校验。
- Bounded deterministic identity: short adapter name unchanged, long name uses existing XXH3-128 convention; UID prefix chttp-websocket:.
- Managed accepted counts successful publish handoff, completed counts every accepted publication terminal (including graph/send admission error); rejected counts only pre-publication failures. Preserve all existing native counters.
- Pending-admission accounting begins under the same lock as successful frame reservation. Snapshot returns EBUSY until make/publish outcome accounting commits, including callback-before-publish-return.
- queue_depth0 (no separate owner queue), queue_capacity=frame_capacity, in_flight=occupied admitted frames; saturation means full frame capacity, not merely quiesced.
- Existing host command serialization remains required. No implicit retry or rollback reopening sessions.
- 所有实现与审查 subagents 使用用户指定 Sol；禁止 worker 再派子代理。
- 只在已验证 worktree `.worktrees/issue-109-standalone-chttp` 的 `feat/managed-chttp-websocket` 工作；不创建另一个 worktree、不触碰根目录用户文件。
- worker 只运行 focused/adjacent native gates；主线程负责 full CTest 与 external SDK install/installed-consumer。所有 native 命令串行，等待终态，不修改 SDK 路径或添加 CMake override/fallback。
- `.codegraph/` 和 `.superpowers/` 是本地索引/证据，不提交、不 force-add；产品改动通过 apply_patch。
- 不推送或合并本分支；交付审查和验证结果后等待对应授权。

## 架构分析与影响

事实：WebSocket owner 已有 session/frame 容量、generation、原生 close retry 和 async publication lifecycle；`chttp_websocket_fault_support.c` 编译真实 owner TU，可隔离 close 失败。HTTP server 已示范 managed atomic registration、描述符、有限 identity 和 generation 校验。生产 DLL 与 installed consumer 已有 WebSocket generation/lease 流程。

选择：在现有 owner 添加 managed identity/counters/admission generation/pending accounting，并注册 callbacks。拒绝另建 session registry（双事实源）、新增公开 managed API（现有 aggregate 足够）、改变 native quiesce 为纯标记（破坏既有 close 行为）。不增加队列、线程或独立 provider 生命周期。

HIGH：callback 可能在 publish 返回前释放 slot；不能由 accepted-completed 推导 occupancy，也不能延迟 native release。snapshot 在 pending 非零时 EBUSY；稳定后在 owner 锁内校验 frames、每 session frame 计数、active sessions，与 managed completed <= accepted。不用饱和累计值推导活动资源。snapshot 校验可扫描有界 slots；应优先 O(frame_capacity + session_capacity) 时间、O(1) 或既有有界 owner 存储；若采用嵌套扫描，说明小容量依据与上限，不能无声引入无界二次成本。

HIGH：quiesce 的状态提交先于原生 close。真实 RUNNING -> QUIESCED 必须 generation+1，之后 close 失败仍保留 generation/state 和存活 session；新 key/current generation 才显式重试，旧 key 重放错误。原生 resume 不复活旧 closing session。snapshot 在 QUIESCED 且任意 session/frame 存在时为 DRAINING，全部归零才 QUIESCENT。

MED：既有 WebSocket generation 将新增一个可枚举 managed boundary，观察可能暂时 EBUSY。public 签名/layout/wire/config 不变；native control 的 generation 溢出以 ERANGE 拒绝真实 transition，并在公开注释说明。无持久化迁移，失败注册全回滚；部署仍是同一插件 DLL。回退改动只能整体 revert 本特性提交并重建，绝不新增 runtime fallback。

验证范围：真实 H1/H2 frame drain；故障 seam 验证不易调度的 publication/close 窗口；生产 DLL 与外部 SDK consumer 分别证明真实加载/ABI/lease。测试不声称 peer delivery、durable settlement 或 exactly-once。#28 人工闭环、#75 host services、#29 分布式 checkpoint 不在本切片内。

### Task 1: 完整 managed WebSocket owner、控制、测试与消费链

**Files:**
- Modify: `io/chttp/src/turbo_flow_chttp_websocket_server.c` — 唯一生产状态 owner；注册、metadata、descriptor、snapshot、计数、控制 generation。
- Modify: `io/chttp/include/turbo_flow_chttp.h` — 仅现有 quiesce/resume 注释，明确 ERANGE 与失败状态；不改声明/layout。
- Test: `io/chttp/tests/test_chttp_websocket_adapter.c` — 真实公开 API、注册、capacity、H1/H2 drain。
- Test: `io/chttp/tests/chttp_websocket_fault_support.c`、`io/chttp/tests/test_chttp_websocket_close_fault.c` — 真实 TU 的 test-only seam、确定性失败/顺序/invariant 测试。
- Modify if necessary: `io/chttp/tests/CMakeLists.txt` — fault target 私有 xxHash include 和 test-only defines；不引入公开依赖或自制 helper。
- Test: `io/chttp/tests/test_chttp_plugin.c` — 实际生产 DLL managed control/observation。
- Test: `tests/install_chttp_plugin_consumer/main.c` — installed WebSocket managed boundary/control，保留 generation lease 验证。
- Modify: `docs/MANAGED_BOUNDARIES.md` — 表明 WebSocket 实际支持、限制、generation/错误重试和计数。

**Interfaces:**
- Consumes: 已公开 `turbo_flow_managed_async_terminal_registration_t` / `turbo_flow_register_managed_async_terminal_adapter()`，`turbo_flow_managed_boundary_provider_ops_t`，`turbo_flow_resource_command()`；参照 HTTP server 的初始化和 callback signatures。
- Produces: 保持原 `turbo_flow_chttp_websocket_server_register()`、`quiesce()`、`resume()`、`snapshot()`、`destroy()` 签名；该 adapter 注册后可由既有 managed API 枚举一个 owner。
- 不修改 core command history；现有实现已在 provider 返回错误后重读 metadata 并缓存错误结果。

**Binding execution context:** 此任务继承上方 Global Constraints；生成 task brief 时必须一并提供。读 `AGENTS.md` 与适用 chttp/cflow/memory-design-protocols/plugin-system/tinytest/cmake-presets/TDD skills 和必需 references，维护 CodeGraph，至少阅读真实 owner、测试 gate、HTTP managed reference 三处。不得把 fault seam 当生产 DLL 证据。

- [ ] **Step 1: 写注册/描述符 RED，先不改生产代码。**

复用现有注册 fixture，最小可观测断言如下（放在成功注册、尚未销毁的真实 flow 上）：

```c
check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
turbo_flow_managed_boundary_descriptor_t managed_descriptor =
    TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &managed_descriptor), SALTS_OK);
check_equal(managed_descriptor.role_flags,
            (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_SOURCE | TURBO_FLOW_MANAGED_BOUNDARY_SINK));
```

添加 input/output schema、version、profile/encoding、UID/owner、command mask、capabilities 的独立 literal 期望。实际 `descriptor` 字段名沿公开头，不造 API。运行仅 websocket adapter 测试；预期 count 实际0/期望1。记录命令、失败行与退出码；编译错误不算 RED。

- [ ] **Step 2: 先写/运行 accounting 与 control RED，再最小实现完整 provider。**

在既有 H1/H2 gate 测试中，发布成功且 gate 持有时，稳定 snapshot 应 accepted1/completed0/in_flight1；quiesce 后 DRAINING，resume 后 RUNNING 且旧 session 仍 closing；释放 gate 后原 frame 正常送出，随后1013，稳定 completed1/in_flight0。对 capacity1 和 capacityN 填满/第N+1帧拒绝分别测 backpressure；capacity0 注册 EINVAL。追加未满 frame capacity 的 quiesce 不应误报 full。

每组新语义先看真实失败再实现；不要只用 count RED 为后续未经测试的语义背书。可以按 register/descriptor、accounting/snapshot、generation/control 分小 RED/GREEN 波次，但只交付完整可测试 Task1。

注册聚合骨架（值和 callback 字段沿现有 HTTP server，所有 callbacks 必须完整实现后暴露）：

```c
turbo_flow_managed_async_terminal_registration_t registration =
    TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
registration.adapter_name = server->adapter_name;
registration.owner_name = server->managed_owner;
registration.ctx = server;
/* adapter_ops / async_ops / schema / boundary_ops 使用当前真实实现。 */
status = turbo_flow_register_managed_async_terminal_adapter(config->flow, &registration);
```

owner 增加 bounded char identity、generation 初值1、饱和 managed accepted/completed/rejected 和 pending_publications。XXH3-128 使用既有依赖，长名称格式与 HTTP server 一致；不要截断导致碰撞。失败 cleanup 不遗留 adapter/resource 或悬挂 provider。

managed item 只是一项入站 event publication；handshake/session 拒绝和 outbound command 不是另一个 managed item。报告评估中的 `chttp-websocket-server:` 不采用，以 issue 指定 `chttp-websocket:` 为准。

会计顺序必须如此（示意逻辑嵌入现有锁，不复制另一份 lifecycle）：

```c
/* 同一次 frame reserve 成功的临界区： */
++server->pending_publications;
/* publish 成功返回的临界区： */
websocket_counter_increment(&server->managed_accepted);
--server->pending_publications;
/* publication callback 的现有 release 临界区，不受 terminal status 成败影响： */
websocket_counter_increment(&server->managed_completed);
```

make/publish 失败必须在 release frame 的同一临界区 pending--、managed_rejected++，不计 managed_completed。reserve/invalid-event 失败各计一次 managed_rejected；不可在 release 与外层 error 分支重复计。native counters 保留原更新点与含义。pending 不应饱和或绕回：它受已保留 frame 和同步 publish 处理界限约束，明确证明/检查边界。

snapshot 先在锁内拒绝 pending>0 为 EBUSY，再验证稳定 occupancy/session 不变量，错误 EPROTO；成功一次性填充输出。REGISTERED/STOPPED/DETACHED 等映射沿既有 managed 状态，FAILED 不伪装 RUNNING。resource metadata 与 snapshot generation 来自同一 owner。

不可校验 `pending_publications <= in_flight_frames`：callback-before-return 可使 pending1、occupied0，这是合法瞬态。只在 pending0 的稳定窗口做占用一致性检查；pending 本身的上界依据 callback dispatch 串行和固定容量推导，不从已提前释放的 frame 数推导。

- [ ] **Step 3: 写并验证失败 close / generation RED，然后共享原生与 managed 控制。**

扩展真实 close fault fixture，使用已有 injected ENOBUFS：

```c
command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
command.expected_generation = managed.generation;
check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOBUFS);
check_equal(result.generation_after, command.expected_generation + 1u);
check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOBUFS);
```

command target/key 必须按 fixture 初始化为真实 UID/非空唯一 key；断言 close_calls 第一次1、同 key 仍1；stable snapshot DRAINING、active session1、generation已推进。新 key 但旧 generation 拒绝且不 close；新 key+当前 generation 重试 succeeds、calls2；同态再 quiesce 不推进 generation。delayed completion 不隐式重试失败 close。deadline、unsupported mask、REGISTERED/STOPPED 控制、native/managed 交错、same-state no-op、overflow 前置拒绝都要覆盖。

实现将 native 与 managed 操作共享 owner-lock admission transition，managed expected generation 在 owner 锁内再次核验；锁外执行 native close，不能持锁 I/O。真实状态改变先验证 UINT64_MAX，再 state 更新和 generation++；之后 close error 原样返回，保留已提交状态。same-state retry 不递增，resume 不清旧 session closing/close_on_drain。控制 lane 仍要求串行，不声称 core history 任意并发安全。

- [ ] **Step 4: 用真实 TU 测 deterministic admission/error/invariants。**

test-only wrappers 只拦真实不可调度边界，放 fault support/test，不加生产测试 API：

```c
check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EBUSY);
/* 在 reserve 成功后的 make 阶段，以及真实 completion 先于 publish 返回时分别执行。 */
```

publication wrapper 应调用真实 publish 并用真实 callback 完成/阻塞返回，或者隔离真实 owner 的 reserve/make/callback/publish-result helper 驱动顺序；不能直接填 expected snapshot 或绕开生产 helper。make allocation fail、publish admission fail、terminal graph/send error 分别断言 managed rejected/completed/native counters 的区别与 slot 回收。将 counters 初始化 UINT64_MAX-1 后调用真实增量路径验证饱和，不以手写同公式作断言。

通过隔离 TU 将真实 slots/session 计数置成矛盾，snapshot EPROTO，恢复后 teardown；不把测试破坏状态留给生产 lifecycle。pending 检测必须覆盖 reserve 后 make 之前，非仅 publish 调用前。

注册补充：duplicate adapter/managed owner/UID、resource/adapter capacity 失败均验证无部分注册且 out owner NULL；多个独立长名称 deterministic 不串 owner。calloc失败覆盖 owner/slots 分配和 cleanup；fault 编译目标如需 xxHash 仅 private include。测试容量使用命名常量且真实 admission，而非直接伪造 full 状态。

- [ ] **Step 5: 实际 DLL 和 installed consumer 加断言，交主线程先测旧 SDK RED。**

`test_chttp_plugin.c` 的真实 H1/H2/TLS WebSocket roundtrip 中检查 count1、descriptor 与 managed QUIESCE/RESUME，保留实际 payload、1013关闭、host busy/lease。避免把现有 close-client 顺序无意变成二次 peer close 假成功。

installed consumer 原有 `kind == 1u` 检查扩展到 WebSocket（不是 client）：

```c
if (kind == 1u || kind == 2u) {
  /* 现有真实 managed descriptor/snapshot/command 检查，按 kind 区分诊断。 */
}
```

继续只链接 PluginHost/Product/Graph，不直接链接具体 CHTTP adapter 绕过 DLL。保留 lease acquire、generation/host destroy EBUSY、release 和最终 clean destroy。consumer 修改完成后通知主线程；worker 不 install。主线程先以当前已安装 SDK889fe7e运行新 consumer，预期 WS count0/期望1，再于全部审查通过后用候选 SDK GREEN。

- [ ] **Step 6: 更新文档与公开注释，自审、focused/adjacent GREEN、提交。**

文档明确可枚举双向 owner、schema/context 所有权、queue非独立队列、临时 EBUSY、draining含active sessions、错误 key replay/new-key retry、native admission非delivery。公开注释仅补 ERANGE/约束，勿调整 API。

worker 串行运行以下 release/debug focused gates；每条命令前调用既有 VsDevCmd，随后等待 native 终态：

```text
cmake --build --preset win-release-user --target test_chttp_websocket_adapter test_chttp_websocket_close_fault test_chttp_plugin
ctest --preset win-release-user -R "^(test_chttp_websocket_adapter|test_chttp_websocket_close_fault|test_chttp_plugin)$" --output-on-failure
cmake --build --preset win-dev-user --target test_chttp_websocket_adapter test_chttp_websocket_close_fault test_chttp_plugin
ctest --preset win-dev-user -R "^(test_chttp_websocket_adapter|test_chttp_websocket_close_fault|test_chttp_plugin)$" --output-on-failure
```

相邻回归用 `ctest --preset <preset> -N` 核实后选 HTTP adapter/managed owner/resource command/async terminal 相关测试，先 build 对应 target；不得杜撰 preset 或省略编译步骤。记录测试名称、数量、命令、退出码、RED/GREEN输出到 ignored task report。自审 git diff/check；只 add 明确产品/test/doc 文件，提交，不 force-add scratch。

- [ ] **Step 7: 主线程 task review、full/installed gate、final branch review。**

独立 Sol task review同时出 spec+quality verdict；需要修复时交原 implementer并 scoped re-review。主线程在无 worker native进程时跑 release/debug complete build和CTest，不能把focused当full。按已授权既有 SDK安装目录/presets执行候选install；从 consumer目录按 `chttp-plugin-consumer-win-release-user` 和 `chttp-plugin-consumer-win-dev-user` configure/build/test。不覆盖 backup、不引入 roots/flags 覆盖。

final broad Sol review覆盖整个分支与最终验证；只一次final fix wave加scoped re-review。最终报告准确区分本地Windows/Debug-ASan/真实DLL/故障seam/installed证据，Linux未运行则明言。不将 #113 尚未验证项目勾完；不自动推送或 merge。
