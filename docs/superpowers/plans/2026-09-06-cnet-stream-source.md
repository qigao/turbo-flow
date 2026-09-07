# CNet Stream Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提供一个无 legacy/fallback、由 downstream demand 驱动的 TCP/TLS/Pipe CNet stream source owner，把 borrowed receive 安全地送入 TurboFlow Reactive run。

**Architecture:** 新增 `TurboFlow::CNetAdapter` 可选 target；opaque owner 在同一 caller thread 上串行驱动 CNet client、manual CFlow Scheduler、managed-message Publisher 与 TurboFlow run。每次 demand 最多登记一个 receive，callback 内完成唯一的 borrowed-to-owning copy，callback 返回后才由 Scheduler 把 message move 进 Graph。

**Tech Stack:** C11、Salts::CNet、Salts::CFlow Reactive、CMeta interface、Salts mem_buffer、TurboFlow::Graph、TinyTest、CMake Presets

**Spec:** `io/cnet/ADR_CNET_STREAM_SOURCE.md`

## Global Constraints

- 不恢复 `turbo_flow_coronet_*`、TurboNet/TurboHttp target、旧 transport alias、兼容层或 fallback。
- `TurboFlow::Graph`、`TurboFlow::Config`、`TurboFlow::Product` 不得链接 `Salts::CNet`；只有 `TurboFlow::CNetAdapter` 链接。
- 所有 source API 由同一 owner thread 串行调用；manual Scheduler admission 与 drive 不并发。
- 每个 owner 同时最多一个 CNet receive 和一个 owning message；容量满 fail fast。
- CNet callback 中只复制 borrowed view 和记录终态，不调用 Graph、不阻塞、不销毁 owner。
- TDD 顺序严格为 RED → GREEN → REFACTOR；每个新行为先观察预期失败。

---

### Task 1: 公开 ABI、独立 target 与 fail-fast open

**Files:**
- Create: `io/cnet/include/turbo_flow_cnet.h`
- Create: `io/cnet/src/turbo_flow_cnet_stream_source.c`
- Create: `io/cnet/CMakeLists.txt`
- Create: `io/cnet/tests/CMakeLists.txt`
- Create: `io/cnet/tests/test_cnet_stream_source.c`
- Create: `io/cnet/tests/cnet_stream_source_header_cpp.cpp`
- Modify: `CMakeLists.txt`
- Modify: `cmake/TurboFlowConfig.cmake.in`

**Interfaces:**
- Consumes: `turbo_flow_run_open/request/snapshot/cancel/close`, `cnet_client_init/connect/poll/stop/destroy`, `cflow_scheduler_manual_init_with_capacity`。
- Produces: spec 中六个 `turbo_flow_cnet_stream_source_*` 函数、V1 config/snapshot/state，以及安装 target `TurboFlow::CNetAdapter`。

- [ ] **Step 1: 写失败的 ABI/validation 测试**

  测试包含新头文件，检查 config/snapshot init 的 size/version，C++ TU 可包含；调用 open 验证 NULL、zero sizes、非 started flow、`udp://`、message ID 0、`max_message_bytes > client.receive_buffer_bytes` 均返回明确错误且 `out_source` 保持 NULL。

- [ ] **Step 2: 运行测试确认 RED**

  Run: `cmake --build --preset win-dev-user --target test_cnet_stream_source --parallel`

  Expected: configure/build 因 `turbo_flow_cnet.h` 或 target 尚不存在而失败；不是测试拼写错误。

- [ ] **Step 3: 实现最小公开契约和事务式 open cleanup**

  Header 定义 opaque owner、`TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION == 1`、size/version config/snapshot。Source 先纯校验，再按 Scheduler → CNet → Publisher/run → connect 初始化；任一步失败按反序释放且不发布 partial owner。Scheme 仅接受 `tcp://`、`tls://`、`pipe://`；TLS config 只允许 `tls://`。

- [ ] **Step 4: 运行 focused 测试确认 GREEN**

  Run: `cmake --build --preset win-dev-user --target test_cnet_stream_source --parallel`

  Run: `ctest --preset win-dev-user -R '^test_cnet_stream_source$' --output-on-failure`

- [ ] **Step 5: Commit**

  Run: `git add CMakeLists.txt cmake/TurboFlowConfig.cmake.in io/cnet`

  Run: `git commit -m "feat(cnet): add stream source owner contract"`

### Task 2: Demand-driven TCP receive and owning message handoff

**Files:**
- Modify: `io/cnet/src/turbo_flow_cnet_stream_source.c`
- Modify: `io/cnet/tests/test_cnet_stream_source.c`

**Interfaces:**
- Consumes: Task 1 owner API、CNet loopback listener/client、`mem_get_buffer`、`turbo_flow_msg_move`。
- Produces: zero-demand suppression、one-demand/one-receive、callback-return-after-copy 行为。

- [ ] **Step 1: 写真实 TCP loopback 失败测试**

  用 CNet listener 和 server-side CNet client 建立 loopback；先发送 `"first"` 且不 request，反复 poll 后 Graph probe 仍为 0。request 1 后只收到 `"first"`；再发送 `"second"`，没有第二次 demand 时仍为 1；第二次 request 后 payload/ID 顺序为 `first_id`、`first_id + 1`。

- [ ] **Step 2: 运行确认 RED**

  Run: `ctest --preset win-dev-user -R '^test_cnet_stream_source$' --output-on-failure`

  Expected: Graph probe 在 demand 后仍未收到消息，因为 Publisher resume/on_receive 尚未实现。

- [ ] **Step 3: 实现 managed Publisher 与 owner poll**

  `resume` 只在正 demand 下调用 `cnet_receive(..., 1)`；WAIT waitable 保存单个 waker。`on_receive` 校验 handle/kind/size/inflight，取得 bounded mem_buffer、复制并设置 used，构造 message 后 wake。`poll` 按 Scheduler → CNet → Scheduler 顺序执行，每段最多 `scheduler_max_steps_per_poll`；Publisher 通过 `turbo_flow_msg_move` 构造 output。

- [ ] **Step 4: 运行 focused + flow_run 回归确认 GREEN**

  Run: `ctest --preset win-dev-user -R '^(test_cnet_stream_source|test_flow_run)$' --output-on-failure`

- [ ] **Step 5: Commit**

  Run: `git add io/cnet/src/turbo_flow_cnet_stream_source.c io/cnet/tests/test_cnet_stream_source.c`

  Run: `git commit -m "feat(cnet): drive stream receive from reactive demand"`

### Task 3: Exact errors, terminal wake and stop/drain

**Files:**
- Modify: `io/cnet/src/turbo_flow_cnet_stream_source.c`
- Modify: `io/cnet/tests/test_cnet_stream_source.c`

**Interfaces:**
- Consumes: Task 2 state machine、CNet state/error callbacks。
- Produces: exact adapter snapshot status、remote DONE、FAILED、STOPPED、idempotence checks。

- [ ] **Step 1: 写 terminal/negative 失败测试**

  覆盖 refused connection 保存 CNet status/native stage；remote close 唤醒 run 并完成；超过 `max_message_bytes` 进入 `SALTS_EMSGSIZE`；stop 取消 run 并 drain；stop 前 destroy 返回 `SALTS_EBUSY`；stop 后 request/poll 返回 shutdown；重复 stop 返回 `SALTS_EALREADY`。

- [ ] **Step 2: 运行确认 RED**

  Run: `ctest --preset win-dev-user -R '^test_cnet_stream_source$' --output-on-failure`

  Expected: 至少 exact snapshot、oversize 或重复生命周期断言失败。

- [ ] **Step 3: 实现 terminal 与 cleanup 状态机**

  CNet 是连接事实源；callback 复制 portable error fields 并 wake value/terminal waker。FAILED 不阻止 stop。Stop 固定执行 run cancel/close、CNet stop/destroy、Scheduler shutdown/drain/destroy，并清理 ready message；destroy 只释放已 STOPPED owner。

- [ ] **Step 4: 运行 focused 测试确认 GREEN**

  Run: `ctest --preset win-dev-user -R '^(test_cnet_stream_source|test_flow_run|test_turbo_flow)$' --output-on-failure`

- [ ] **Step 5: Commit**

  Run: `git add io/cnet/src/turbo_flow_cnet_stream_source.c io/cnet/tests/test_cnet_stream_source.c`

  Run: `git commit -m "fix(cnet): preserve terminal errors and drain ownership"`

### Task 4: TLS/Pipe coverage and architecture documentation

**Files:**
- Modify: `io/cnet/tests/test_cnet_stream_source.c`
- Create: `io/cnet/tests/stream_source_pipe_fixture.h`
- Modify: `io/cnet/ADR_CNET_STREAM_SOURCE.md`

**Interfaces:**
- Consumes: stable source owner API。
- Produces: fail-closed TLS、platform Pipe coverage 与更新后的可选 adapter boundary 文档。

- [ ] **Step 1: 写 TLS/Pipe 失败测试**

  验证未配置 TLS 有界存储时同步 `SALTS_ENOTSUP`，无效 CA 同步失败且不发布 owner；CNet 自身负责完整证书/hostname 协议测试，本适配层不复制其测试证书。平台支持时用本地 Pipe peer 验证 zero-demand 与 demand handoff；缺失 Pipe 保留原始连接错误，不得改走 TCP。

- [ ] **Step 2: 运行确认 RED**

  Run: `ctest --preset win-dev-user -R '^test_cnet_stream_source$' --output-on-failure`

  Expected: 尚未覆盖的 TLS/Pipe lifecycle 或 URI policy 断言失败。

- [ ] **Step 3: 只修正 source owner 的 TLS/Pipe 差异并同步文档**

  保持同一状态机，不创建 transport strategy 层；仅把 TLS config 传给 CNet connect，并保留 Pipe/平台原始状态。ADR 将边界明确为 Graph target 外的可选 CNetAdapter target。

- [ ] **Step 4: 运行 focused 测试确认 GREEN**

  Run: `ctest --preset win-dev-user -R '^(test_cnet_stream_source|test_protocol_.*)$' --output-on-failure`

- [ ] **Step 5: Commit**

  Run: `git add io/cnet`

  Run: `git commit -m "test(cnet): cover TLS and Pipe source transports"`

### Task 5: 安装消费、全量验证与 issue/PR

**Files:**
- Modify: `tests/install_consumer/CMakeLists.txt`
- Modify: `tests/install_consumer/main.c`
- Modify: `tests/install_consumer/run.cmake`

**Interfaces:**
- Consumes: installed `TurboFlow::CNetAdapter`。
- Produces: C/C++ package consumer proof、可复验构建日志、stacked PR。

- [ ] **Step 1: 写安装消费失败测试**

  Consumer 链接 `TurboFlow::CNetAdapter`，包含 `turbo_flow_cnet.h`，构造 config/snapshot init，并从 C++ TU 验证 opaque ABI；不启动网络。

- [ ] **Step 2: 运行确认 RED**

  Run: `cmake --build --preset win-release-user --target install --parallel`

  Run: `ctest --preset win-release-user -R '^test_turbo_flow_install_consumer$' --output-on-failure`

  Expected: 安装 export/header/依赖传播尚未完整时 consumer configure 或 link 失败。

- [ ] **Step 3: 补齐 export 与 consumer，不改变核心 target 依赖**

  把 `tf_cnet_adapter` 加入根 export list；安装 `turbo_flow_cnet.h`；package config 继续只 `find_dependency(Salts)`，由 exported target 传播 `Salts::CNet`。

- [ ] **Step 4: 全量验证**

  Run: `cmake --fresh --preset win-release-user`

  Run: `cmake --build --preset win-release-user --parallel`

  Run: `ctest --preset win-release-user --output-on-failure`

  Run: `cmake --fresh --preset win-dev-user`

  Run: `cmake --build --preset win-dev-user --parallel`

  Run: `ctest --preset win-dev-user -R '^(test_cnet_stream_source|test_flow_run|test_turbo_flow|test_turbo_flow_install_consumer)$' --output-on-failure`

  Run: `codegraph sync .`

  Run: `git diff --check`

  Run: `rg.exe -n "TurboNet|TurboHttp|CoroNet|turbo_flow_coronet|fallback" CMakeLists.txt cmake io/cnet turbo_flow/include turbo_flow/src tests`

  Expected: 所有测试通过；扫描没有旧实现/API/alias/fallback（设计文档中的禁止说明除外）；Graph/Config/Product target 不含 `Salts::CNet`。

- [ ] **Step 5: Commit、push、创建 stacked PR 并更新 issues**

  Run: `git add tests/install_consumer CMakeLists.txt io/cnet docs turbo_flow ingress README.md`

  Run: `git commit -m "build(cnet): export stream source adapter"`

  Run: `git push -u origin feat/issue-18-cnet-stream-source`

  PR base 在 #17 merge 前使用 `refactor/issue-4-reactive-runs`；body 写明 `Closes #18`、依赖 #17、RED/GREEN 与 Release/Debug/ASan/install consumer 证据。#18 评论记录验证命令；#5 只勾选 stream source 子任务，保持 open。
