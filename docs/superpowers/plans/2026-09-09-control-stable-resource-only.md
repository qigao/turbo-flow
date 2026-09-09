# Control stable-resource-only Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for implementation and independent review.

**Goal:** 完成 #95 的 control 子阶段，删除控制 DSL 对旧 adapter command 的载荷依赖和运行时 fallback。

**Architecture:** 控制 DSL 保持文本语法，解析为指针无关的资源命令意图；唯一执行路径为稳定资源 UID/generation -> turbo_flow_resource_command。provider 保持状态主事实源；不增加 registry、wrapper 或接口别名。

**Tech Stack:** C11、Lemon、TinyTest、Windows Debug/ASan 与 Release CMake user presets。

**Spec:** 用户已批准“删除所有旧dll/接口，不需要任何形式的fallback（c,cmake)”并要求继续；https://github.com/qigao/turbo-flow/issues/95 。本计划只推进 control，discovery 与旧 adapter API 的完整删除仍在 #95。

## Global Constraints

- 包保持 2.0.0；用户批准 breaking C 接口迁移，控制命令结构不保留旧 adapter 字段或兼容别名。文本语法和其他控制动作保持不变。
- 替换 turbo_flow_control_command_t.adapter 为 turbo_flow_resource_command_kind_t resource_kind 和 int endpoint_port；保留现有 endpoint_host/endpoint_path 拥有字符串数组。旧 adapter 类型仍被 discovery 使用，本阶段不删除其定义/回调/API，不宣称全部清完。
- 缺少稳定可命令 connection provider 返回 SALTS_ENOENT；重复匹配/错误 metadata 返回原有错误；provider 执行错误直接传播，不调用旧 callback。
- 控制入口 STARTED 检查、条件求值、资源 generation/idempotency/历史和池 resize 行为保持；不修改 CFlow/Salts、外部 SDK、CMake 依赖或插件 ABI。
- 使用已有隔离 worktree。代码检索只用 rg.exe/fd.exe；apply_patch 编辑；报告/.codegraph/.superpowers 不提交。

## 影响与迁移

HIGH/事实：旧 control 结构含借用 endpoint 指针及 legacy adapter payload，flow_control.c 找不到 stable provider 会直接调用旧 callback，绕过资源命令 generation/history 检查。用户批准移除该路径。调用方改用 resource_kind/endpoint_port 并重编译；不迁移数据，文本配置不变。

选择最小控制边界清理；“仅删除 fallback 但继续使用 legacy payload”留下接口依赖，“同步机械迁移 discovery”忽略有界 command history 在补偿期间耗尽风险。后者须独立设计预检/容量/补偿语义，继续跟踪 #95。源码回滚仅通过显式撤销提交，不提供运行时降级。

### Task 1: Remove legacy control payload and fallback

**Files:** Modify turbo_flow/include/turbo_flow.h, turbo_flow/include/turbo_flow_control.h, turbo_flow/src/flow_control.c, turbo_flow/src/flow_control_internal.h, turbo_flow/parser/control_grammar.y, turbo_flow/tests/test_flow_control.c；按实际引用更新当前控制文档和调用点，不修改 build 生成的 parser 文件。

**Interfaces:** 消费 turbo_flow_resource_command_t、stable provider metadata 与既有 dispatcher；产出仅修改 control command 的 resource_kind/endpoint_port 字段，保留 target、endpoint_host/path 和其他字段。

- [ ] Step 1: 添加真实 RED 行为测试：以现有 control_started_flow_ex(&adapter,0) 注册只有旧 callback 的 adapter，执行 adapter mock quiesce 必须 SALTS_ENOENT，adapter.commands == 0，resource_commands == 0。当前 baseline 将返回成功并调用旧 callback。先构建 test_flow_control 并运行其 CTest，记录失败输出。

```c
check_equal(turbo_flow_control(flow, "adapter mock quiesce", sizeof("adapter mock quiesce") - 1u), SALTS_ENOENT);
check_equal(adapter.commands, 0);
check_equal(adapter.resource_commands, 0);
```

- [ ] Step 2: 用 resource_kind/endpoint_port 替换旧嵌套 payload；grammar 和 setter 直接用 TURBO_FLOW_RESOURCE_COMMAND_QUIESCE/RESUME/REPLACE_ENDPOINT；删除 adapter.size 设置及解析后指针修复。验证 resource_kind 仅允许上述三个值（不允许 RESIZE_POOL 走 adapter 动作），endpoint 字符串/端口验证保持严格。
- [ ] Step 3: execute_action 只查找稳定 connection provider，`if (rc != SALTS_OK) return rc;` 后初始化资源命令并执行；删除旧 kind 映射及 fallback。原 control success fixtures 改为 stable provider；保留仅旧回调 fixture 专门证明不会回退。
- [ ] Step 4: 增加/保留边界回归：连续 quiesce/resume 读取新 generation；replace endpoint 经 parse、结构体值复制、清空原结构后 execute，owner 收到正确 host/path/port；没有 provider 失败且无回调；非法 resource_kind（0 和 RESIZE_POOL）与非法 endpoint 失败且无副作用；现有条件 true/false、facts 错误、provider EIO、pool resize 等继续验证。模拟 owner 是真实 Graph dispatcher 边界探针，不 mock dispatcher。
- [ ] Step 5: 公共 control 文档说明唯一稳定资源路径、错误和结构迁移；rg 验证 control 源/grammar/头不引用 legacy command，其他实际旧入口保留于 #95，不能标记 issue 完成。
- [ ] Step 6: Debug/Release configure/build，先 control/discovery/resource_document 聚焦测试，再全量 CTest（包括安装消费者）。git diff --check；自审并显式提交源码/测试/docs，scratch report 仅本地。命令使用下例，两profile分别执行。

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user && cmake --build --preset win-dev-user && ctest --preset win-dev-user --output-on-failure'
```

Release 使用 win-release-user。RED/GREEN 最小目标 test_flow_control；测试过滤 `^(test_flow_control|test_flow_discovery|test_flow_resource_document)$`。先查当前声明/测试至少三处，已有 CodeGraph sync 成功，若新增文件再 sync。
