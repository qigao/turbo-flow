# Managed CHTTP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 deferred HTTP server 注册为可查询、可 quiesce/resume 的 managed Source/Sink，覆盖 embedded 和 DLL generation 两条使用路径。

**Architecture:** 复用现有 atomic managed async-terminal registration 和 resource-command dispatcher。现有 server owner/mutex/slots 继续拥有生命周期与请求事实；新增私有 managed 观察计数，不改变 native snapshot 的原有诊断语义。管理读取不推进业务状态，控制命令在 owner 锁内改变 admission。

**Tech Stack:** C11、Salts、CFlow、独立 Chttp、现有 PluginHost、TinyTest、CMake user presets。

**Spec:** https://github.com/qigao/turbo-flow/issues/110 ，父项 #28；本计划细化该 issue，不包含 #75 新公开 ABI。

## Global Constraints

- 所有新子代理使用 `gpt-5.6-sol`，不得再派子代理；主控安排独立审查。
- 不增加任何 C/CMake fallback、旧 DLL/API、依赖搜索降级或手工 DLL 复制路径。
- 不改变公开头文件布局、函数签名、opcode、HTTP wire、配置格式、DLL 入口或部署方式。
- command mask 仅 `QUIESCE|RESUME`；不声明 demand-aware、replayable、durable-settlement 或 manual-review 能力。
- 同一个 server owner、同一 mutex、同一槽数组是唯一状态归属；不增加第二 registry 或独立推进的业务副本。
- native `admitted_requests/completed_requests/rejected_requests` 的计数点保持不变。
- 源码修改使用 apply_patch；先完成行为 RED，随后最小实现、GREEN、相邻回归、独立审查。
- 不 push、merge、关闭父 issue 或写外部 SDK；构建串行、等待已有 handle 终态。

## 影响与取舍

`MED / 事实`：现有 server 使用普通 async-terminal registration，因此没有 managed boundary；改为 atomic aggregate 后 enumeration 增加一项，这是 #110 的预期能力。

`MED / 事实`：publish 成功早于 native deferred handle attach；成功 cancel 也会释放请求但不增加 native successful-reply counter。managed 计数不能投影 native 三个计数，否则会漏终态或重复拒绝。

选择 owner 内的私有 admitted 标志与饱和累计计数，而不是让 core 推断 HTTP 语义或修改公开 native snapshot。增加每槽一个标志、每 owner 三个计数与稳定 identity/generation；无新增热路径分配。snapshot 扫描槽计数为 O(request_capacity)，空间 O(1)；不通过累计计数差推导 in-flight，避免饱和后失真。

控制层复用已有命令历史、deadline、expected generation 和 idempotency，owner 锁内还必须核对 generation，避免直接 native admission 操作插入主机检查与实际执行之间。无新 HTTP 重试语义。恢复方式是回退本功能提交并重新构建，不是运行时保留旧注册路径。

## 文件归属

- `io/chttp/src/turbo_flow_chttp_server.c`：identity、私有计数、管理 callbacks、注册和 admission 状态迁移。
- `io/chttp/tests/test_chttp_server_adapter.c`：真实 H1/H2 管理闭环、计数与注册边界回归。
- `io/chttp/tests/test_chttp_plugin.c`：真实 DLL/generation 查询、控制与 owner pin 生命周期验证。
- `io/chttp/tests/CMakeLists.txt` 与仅测试故障支持文件：仅在 defer/native terminal 确定性故障需要隔离编译时使用；遵循既有 fault-support 方式，使用原生 CMake 命令。
- `io/chttp/CMakeLists.txt`：仅在 bounded identity 需要时添加现有 xxhash 的私有依赖入口；不导出新依赖。
- `docs/MANAGED_BOUNDARIES.md`：已实现能力、计数、状态、并发与边界说明。

### Task 1: 原子注册并实现 HTTP managed Source/Sink 闭环

**Files:** `io/chttp/src/turbo_flow_chttp_server.c`、`io/chttp/tests/test_chttp_server_adapter.c`、`io/chttp/tests/test_chttp_plugin.c`、`io/chttp/tests/CMakeLists.txt`、`io/chttp/CMakeLists.txt`、`tests/install_chttp_plugin_consumer/main.c`、`docs/MANAGED_BOUNDARIES.md`；必要时新增 `io/chttp/tests/chttp_server_defer_fault_support.c`、`io/chttp/tests/test_chttp_server_defer_fault.c` 及测试私有头。生产代码优先留在现有 server owner，不机械拆分或重构其他协议。

**Interfaces:**

- Consumes: `turbo_flow_register_managed_async_terminal_adapter(turbo_flow_t *, const turbo_flow_managed_async_terminal_registration_t *)`。
- Consumes: `turbo_flow_managed_boundary_descriptor_at()`、`turbo_flow_managed_boundary_snapshot_at()`、`turbo_flow_resource_command()`，具体签名及 INIT 宏以 `turbo_flow/include/turbo_flow.h` 为准。
- Produces: `turbo_flow_chttp_server_register()` 原有输出 server，同时在同一次成功事务中注册一项 managed boundary。无新增公开符号。

- [ ] **Step 1: 检查上下文并编写首个注册 RED。**

阅读 server handler/publication_complete/finish_reply/register/set_admission、现有 H1/H2 quiesce 测试、CNet managed registration 和 core aggregate rollback 测试。同步 CodeGraph；至少阅读三处真实实现/调用点。复用现有真实 server fixture 注册成功后加入以下断言，不用 mock 代替这一行为：

```c
turbo_flow_managed_boundary_descriptor_t descriptor =
    TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
check_equal(descriptor.role_flags,
            (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_SOURCE | TURBO_FLOW_MANAGED_BOUNDARY_SINK));
check_equal(descriptor.command_flags,
            (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE |
                       TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME));
check_equal(descriptor.capability_flags, (uint32_t)0u);
```

在 VsDevCmd 环境执行：

```text
cmake --build --preset win-release-user --target test_chttp_server_adapter
ctest --preset win-release-user -R ^test_chttp_server_adapter$ --output-on-failure
```

预期真实 RED 是 count 0 而不是 1，不是编译/依赖失败；记录完整命令与失败断言。

- [ ] **Step 2: 实现私有 identity 与 atomic registration。**

owner 增加固定大小 owner/uid 数组和初始非零 generation（命名常量 1）。短名称原样作为 owner；长名称复用既有 XXH3-128 bounded identity 算法，UID 前缀为 `chttp-server:`。不能 include CNet 私有模块头；可在 server 内做单一私有初始化函数，xxhash 入口遵循本仓库既有依赖。

descriptor domain 为 `TURBO_FLOW_DOMAIN_IO_TRANSPORT`，kind 为 `TURBO_FLOW_RESOURCE_CONNECTION`，input/output 使用已存在 HTTP response/request body profiles 与 opaque `application/octet-stream` 字节语义（不是从实际 Content-Type 猜 encoding）。两侧必须通过 `turbo_flow_content_descriptor_declare_schema()` 声明 schema：input 为 `CHTTPServerResponse` / `Body` / version 1，output 为 `CHTTPServerRequest` / `Body` / version 1；允许空 body，transport context 仍遵循既有 request-context 生命周期。检查 content initializer 和 schema 声明返回值，勿复制 HTTP header 格式到 core。callbacks 使用当前 provider ops 类型；metadata 与 snapshot 均检查 size/version、同锁复制 owner 事实。

注册使用现有结构体，不做先 adapter 再 resource 的非原子调用：

```c
turbo_flow_managed_async_terminal_registration_t registration =
    TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
registration.adapter_name = config->adapter_name;
registration.adapter_ops = &adapter_ops;
registration.async_ops = &async_ops;
registration.schema = &schema;
registration.owner_name = server->managed_owner;
registration.boundary_ops = &boundary_ops;
registration.ctx = server;
status = turbo_flow_register_managed_async_terminal_adapter(config->flow, &registration);
```

字段 `managed_owner` 属于本任务新增私有 owner；失败使用原 cleanup 路径，out_server 保持 NULL，不调用已提交 owner 的 shutdown。测试重复 adapter、重复资源 UID 的事务失败及重试；预填充 provider 只允许放在测试中。

- [ ] **Step 3: 先写计数语义 RED，再实现私有槽结算。**

增加 slot `managed_admitted` 与 owner `managed_accepted/managed_completed/managed_rejected` 饱和 uint64 计数。计数只在 owner mutex 下更新；不要增加另一份 native 业务状态。代码顺序必须满足：

```c
/* publish_async succeeded; the slot cannot retire before defer is resolved. */
salts_mutex_lock(&server->mutex);
slot->managed_admitted = true;
chttp_server_adapter_counter_increment(&server->managed_accepted);
salts_mutex_unlock(&server->mutex);
```

放在 publish 成功后、native defer 调用前。审核 publication callback 可同步或并发先到的情况：在 deferred_attached/defer_failed 之前不得释放此槽。所有终态 reset 对 admitted 标志结算一次并清除，reserve/copy/publish 前失败仅计 managed rejected。defer 失败已属于 accepted，后续释放计 completed，不计 managed rejected。原 `release_unadmitted` 同时被 post-publish defer failure 调用，必须按 flag 区分，不能按函数名决定计数。

在已有 buffer-exhaust/cancel 测试中断言 native admitted=1、completed=0 仍成立，managed 则 accepted=1、completed=1、rejected=0、in_flight=0。增加确定性 defer failure 测试，证明 native rejected 与 managed accepted/completed 的分歧；采用测试隔离 native seam，禁止生产故障开关。reply/cancel 均未终态时保留槽、completed 不增加。覆盖 reply ENOENT/EALREADY、cancel OK/ENOENT；同一槽复用无重复结算。

snapshot 不存在独立 owner 等待队列，queue_depth 为 0，capacity 来自 slot_count；in_flight 扫描 occupied && managed_admitted。demand/lag 为 0，backpressured 仅在槽满时为 1，不能把 quiesced 当作容量耗尽。计数饱和不回绕，in_flight 不用 accepted-completed 推导。若任一槽 occupied && !managed_admitted，保守返回 EBUSY，覆盖 publish 已成功但 managed admission 尚未提交的窗口；稳定矛盾（未占用却 admitted、扫描占用与 active_requests 不同）返回 EPROTO，不当作可重试 busy。这是本 owner 的容量与一致性契约，不声称 core 强制 queue_depth+in_flight<=capacity。

故障测试采用 `tf_chttp_server_defer_fault_support` 隔离编译实际 server TU，将 native `chttp_server_response_defer` 私有替换为 `chttp_test_server_response_defer`。如需命中 admission 窗口，在相同测试 TU 私有替换 `turbo_flow_publish_async`，wrapper 真实 publish 成功后停在原子 gate；主测试查询确定得到 EBUSY，放行后得到 accepted=1/in_flight=1。所有 test-only helper 声明置于测试目录；不得从生产公开头导出。使用真实 publication callback，不以固定成功返回值替代数据路径。

- [ ] **Step 4: 写控制 RED 并实现 owner 原子 admission 命令。**

真实 H1/H2 请求通过现有 gate 保持在处理中，新增 managed 命令闭环，保留原 native quiesce/resume 测试。按以下契约：

```c
turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
command.expected_generation = snapshot.generation;
memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
memcpy(command.idempotency_key, "http-quiesce-1", sizeof("http-quiesce-1"));
check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
```

owner callback 检查 ctx/flow/command、kind，并在同一 mutex 内检查 expected_generation、允许状态以及 generation 溢出，再改变 admission。direct native quiesce/resume 与 managed 命令复用同一锁内状态迁移；无真实改变不增加 generation，真实变化增加一次，UINT64_MAX 时返回 ERANGE 且不修改状态。主机命令 lane 的现有线程约束不扩大为并发 dispatcher 保证。

state 映射：REGISTERED/STARTING/RUNNING/STOPPING/STOPPED/FAILED 对应 managed；QUIESCED 有占用槽为 DRAINING、无槽为 QUIESCENT；DETACHED 为 STOPPED。generation/observed_generation 来自同一个已提交 admission generation，不因只读快照、请求计数或后台 drain 增加。

测试 accepted 请求继续完成、新请求返回原 unavailable 状态；quiescent 后 resume 使用同一端口；相同 key replay 不重复改变状态；旧 generation 返回 EBUSY；过期 deadline 返回 ETIMEDOUT；未声明命令返回 ENOTSUP；native 直接 quiesce/resume 的 generation 与 managed 查询一致；停止期间返回现有 ESHUTDOWN。用测试隔离方式验证溢出和 owner 锁内二次 generation 检查，不暴露 setter。

- [ ] **Step 5: DLL generation 与资源边界回归。**

在 `test_chttp_plugin.c` 通过真实 `TURBO_FLOW_CHTTP_PLUGIN_PATH` 加载 server，取得 generation_flow 后用同一 managed API 查询/控制；不得用 embedded 注册代替 DLL 路径。accepted 请求保持期间 host destroy 仍 EBUSY，generation quiesce 超时仍保留 owner，释放请求后 teardown 成功且 completion 一次。复用现有 gate/cleanup fixture；保持失败清理能释放 gate，避免失败断言挂死。

测试 255 字符和更长有效 adapter name、重复注册失败时数量不变和 out_server=NULL、同名重建 identity 一致、不同长名称 identity 不同。core aggregate 所有权测试继续覆盖 shutdown 精确一次，不修改 core 公共契约。

在 `tests/install_chttp_plugin_consumer/main.c` 的真实 server kind start 后增加 managed count/descriptor/snapshot 与 QUIESCE/RESUME 检查（空请求时 QUIESCENT，恢复 RUNNING）。保持现有 PluginHost、Product、Graph 链接，不新增 CHTTPAdapter/CHttp 原生链接；仍由 argv 的实际已安装 DLL 提供 owner。错误走原 cleanup，不增加 embedded 路径。主控在候选安装之后跑既有版本化消费者 preset；子代理先报告该安装门禁待主控执行，不可对旧 SDK 的必然 RED 自动降级。

- [ ] **Step 6: 文档、GREEN 与提交。**

更新 `docs/MANAGED_BOUNDARIES.md` 的 CHTTP server 专节及迁移列表，写清 command mask、状态映射、slot 容量、managed/native 计数区别、generation、读取/控制线程约束和 terminal 不等于远端消费/持久化的边界。

在 VsDevCmd 环境串行执行以下命令（新增测试 target 时明确追加相应 target/filter；不猜 preset）：

```text
cmake --preset win-release-user
cmake --build --preset win-release-user --target test_chttp_server_adapter test_chttp_plugin
ctest --preset win-release-user -R "^(test_chttp_server_adapter|test_chttp_plugin)$" --output-on-failure
cmake --preset win-dev-user
cmake --build --preset win-dev-user --target test_chttp_server_adapter test_chttp_plugin
ctest --preset win-dev-user -R "^(test_chttp_server_adapter|test_chttp_plugin)$" --output-on-failure
```

再构建并运行两 profile `chttp` 标签/名称相邻测试及 managed aggregate/core command 测试，准确名称先查询 `ctest -N`。主控安排完整构建/测试作为最终门禁；子代理不得 install 外部 SDK。记录测试范围/数字、未覆盖平台、RED/GREEN、每条需求落点及任何限制。自审 diff，`git diff --check` 后仅提交任务文件，提交信息 `feat(chttp): expose managed deferred server boundary`。
