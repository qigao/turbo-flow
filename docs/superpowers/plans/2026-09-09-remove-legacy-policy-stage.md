# Remove legacy Policy stage bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for implementation and independent review.

**Goal:** 完成 #95 的一个独立清理步骤：删除按 stage 名绑定 Policy 的旧入口及仅被它使用的 stage/resources 注册桥，保留既有 rules.apply operation 行为。

**Architecture:** 复用 turbo_flow_rule_register_data_operation 的单一 operation/resource/module 注册事务与 flow_rule_data_stage 执行函数。processor 仍由调用方拥有，Graph 持有借用，生命周期不变；不新增包装层或替代 registry。

**Tech Stack:** C11、CFlow、TinyTest、现有 Windows user presets。

**Spec:** 用户明确删除所有旧 DLL/接口、无 C/CMake fallback；https://github.com/qigao/turbo-flow/issues/95 。#73/#93 的真正 DLL typed-operation 装配仍在后续，不借此宣称引擎迁移完成。

## Global Constraints

- 完全删除 turbo_flow_rule_register_data_stage 和 turbo_flow_register_stage_with_resources 的公开声明、实现与消费者；不留 alias、wrapper、stub、导出或 C/CMake fallback。
- 保留 turbo_flow_rule_register_data_operation、rules.apply、flow_rule_data_stage 及其 quota、facts、route/drop/error、资源文档语义；不删除普通 turbo_flow_register_stage_ex，此入口属于 #95 后续独立迁移。
- 保持包版本2.0.0及插件ABI2.0；本次不改变跨DLL结构布局，不修改外部SDK、依赖、presets，也不引入新依赖/测试框架。
- processor由caller拥有，flow停止并销毁后才能销毁processor；operation注册失败复用既有事务回滚，不新增独立事实源。
- 先实际安装导出负向测试RED，再删除实现；验证最小相关测试、相邻回归、双profile完整CTest。所有编辑apply_patch；只用rg.exe/fd.exe检索，不提交scratch/.codegraph。

## 影响与边界

HIGH/事实：flow_policy.c 的旧 data_stage 注册只调用 flow_core.c 的 stage_with_resources；全仓实际调用只有四个Policy测试和当前文档。现代 data_operation 已实现规则模块/原语/operation注册及失败回滚。

HIGH/迁移：调用方由 stage名注册改为 resource名注册，DSL节点必须显式写 operation rules.apply resource rules.test。旧函数不再可编译/链接；不保留自动改写或运行时降级。DSL语法本身不变，现有使用现代绑定的配置无需改变。

选择直接删除旧桥并迁移消费者；备选保留forwarder违反用户要求；重写所有普通stage或先实现完整引擎DLL会混入可独立交付任务。本轮不改变算法、配额、错误码与processor状态归属。恢复只能通过Git/完整旧版本部署，不提供fallback。

### Task 1: Remove Policy stage bridge and migrate behavior coverage atomically

**Files:** Modify turbo_flow/src/flow_policy.c, turbo_flow/src/flow_core.c, turbo_flow/include/turbo_flow_policy.h, turbo_flow/include/turbo_flow.h, turbo_flow/tests/test_flow_policy.c, turbo_flow/POLICY_PRIMITIVES.md, tests/install_consumer/run.cmake。当前文档若发现其他真实引用只做对应更新；历史计划不机械清理。不新增生产文件或公开API。

**Interfaces:** 使用既有 int turbo_flow_rule_register_data_operation(turbo_flow_t *flow, const char *resource_name, turbo_flow_rule_processor_t *processor)。Graph借用processor直到destroy；传入资源名与DSL完全一致。

- [ ] Step 1: 在既有安装consumer的dumpbin Graph导出检查中扩展退休API集合，保留现有adapter_command检查。实际安装二进制不得暴露两个旧函数，现代policy operation导出必须存在。执行安装consumer得到真实RED（当前Graph仍导出旧入口）；这是用户指定的安装ABI边界，不以grep源码代替。

```cmake
foreach(retired_export IN ITEMS turbo_flow_adapter_command turbo_flow_rule_register_data_stage turbo_flow_register_stage_with_resources)
  if(graph_export_output MATCHES "[ \t]${retired_export}([ \t\r\n]|$)")
    message(FATAL_ERROR "Graph still exports retired API: ${retired_export}")
  endif()
endforeach()
```

- [ ] Step 2: 四个既有Policy测试由旧调用和裸stage改为显式rules.apply/resource。保留原有fan-out selected1/skipped0、DROP sink0、unknown route EPROTO且不下发、pooled execution在compile拒绝的断言；如错误诊断阶段更早，核对现代路径后按错误语义调整，不删测试。原metadata/document/evaluations检查继续有效。

```c
/* DSL node */
"stage rules operation rules.apply resource rules.test\n"
/* pooled negative node */
"stage rules operation rules.apply resource rules.test exec thread workers 1\n"
check_equal(turbo_flow_rule_register_data_operation(flow, "rules.test", processor), SALTS_OK);
```

- [ ] Step 3: 删除两个旧函数的完整实现和公开声明（含专用注释）。flow_rule_data_stage仍被现代operation调用，必须保留；不新增转发函数，也不顺手删除还被其他API使用的stage/resource类型。运行policy/resource_document和相邻rulesforge/control回归，确认编译、真实路由/错误/资源查询行为不变。
- [ ] Step 4: 更新POLICY_PRIMITIVES文档到唯一rules.apply资源绑定，说明旧两个API删除、调用方显式迁移和借用生命周期。不将内建Policy expression evaluator等同于外部RulesForge；后者DLL化仍由#73/#93跟踪。旧不完整C示例改为指向实际可运行的test_flow_policy集成测试并展示对应完整DSL；不增加未编译的伪完整程序。修正相邻存储说明：实际actions是有界thread-local数组，不是栈数组，不改变实现。
- [ ] Step 5: 全仓有效C/header与当前文档核对两个旧入口无消费者，历史计划及安装负向检查的名称允许保留。运行两profileconfigure/fullbuild，先聚焦，再全量CTest（包含安装consumer）各一次，确认旧导出不存在且modern导出存在。保留实际exitcode、失败原因及任何环境warning。git diff --check，显式提交作用域文件；完整报告只写scratch。

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user && cmake --build --preset win-dev-user && ctest --preset win-dev-user -R "^(test_flow_policy|test_flow_resource_document|test_flow_rulesforge|test_flow_control)$" --output-on-failure && ctest --preset win-dev-user --output-on-failure'
```

Release用win-release-user。RED安装测试过滤 ^test_turbo_flow_install_consumer$；测试只写build下现有stage，不修改外部SDK。
