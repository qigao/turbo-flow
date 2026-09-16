# Typed operation ABI 3 contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for the bounded contract task and independent review.

**Goal:** 将 #93 的已批准 ABI 3.0 决策落实为可直接实施、无未决生命周期语义的精确 typed-operation 契约及实现计划。

**Architecture:** 复用 PluginHost 注册事务/snapshot、Graph operation provider 与 retained projection owner。先完成 ABI/状态机设计审查，再改公开头和运行时；不能以补齐 callback 表为由暴露不可执行 capability。

**Tech Stack:** C11/C++ public ABI、CMeta descriptors、CFlow/Graph、CSTL、TinyTest、Windows Debug/Release user presets。

**Spec:** https://github.com/qigao/turbo-flow/issues/93 和 docs/architecture/typed-operation-plugins.md；用户已确认统一 ABI 3.0、拒绝全部旧 ABI/短结构并同步重编译仓内插件。旧文档兼容条款已被用户指令覆盖。

## Global Constraints

- shared plugin ABI 必须统一为 3.0，旧 ABI 1.x/2.x、未来不支持版本及短结构均明确拒绝；没有旧布局 fallback、保留槽位或新旧双路。
- 单一注册事实源仍为 flow_plugin.c 的有界 catalog；注册首错粘住，包含吞错在内的整模块失败全部回滚，snapshot 持有对应 DLL leases。
- Graph 不依赖 PluginHost 或具体引擎。执行只使用 compile/materialize 缓存的函数指针和 ctx，不逐消息查 catalog/符号，不另造执行器。
- 精确说明 typed 输入/结果、schema 语义兼容、输入借用期限、结果独立 context、session/inflight/owner/snapshot 生命周期；worker 不修改控制面非原子租约。
- 在所有 preflight 成功之前不转移 Graph、不创建有副作用的实例；失败不发布半初始化输出，清理失败必须可诊断且保留安全 owner/lease，不能用强制 free 隐藏。
- 配额单位、上限、乘加溢出、实际执行保障与取消观察点必须完整；不能用调用后耗时检查冒充 deadline，不能把原生 DLL 白名单当作沙箱。
- Source/Sink settlement、terminal、durable review 的事实源不变。不能把 session-dependent 结果伪装为独立结果；不得破坏 message 原输入/descriptor 的失败原子性。
- 本设计任务不修改生产 C/H、测试、CMake、presets、外部 SDK，不运行引擎安装。#93 execution admission gate 暂保持；#73 真实引擎 DLL/旧入口/Core 必需链接删除保持开放。
- 只编辑当前 worktree；apply_patch 写文档，rg.exe/fd.exe 检索；不得提交 .codegraph 或 scratch。文档区分事实/推论和 HIGH/MED/LOW，不声明未实现功能已完成。

### Task 1: Specify exact operation ABI and executable implementation plan

**Files:** Modify docs/architecture/typed-operation-plugins.md; Create docs/architecture/typed-operation-abi3.md; Create docs/superpowers/plans/2026-09-09-typed-operation-abi3-runtime.md.

**Interfaces:** 本任务产物是新的精确 C 契约设计和实现任务，不直接发布 C API。必须在新设计文档给出完整 struct/enum/typedef/function 签名、数值标签及尺寸/version 初始化策略；实现计划引用这些精确名字，不允许空占位或未定义类型。

- [ ] 阅读 public plugin/operation/generation/projection headers、flow_plugin.c 注册/snapshot、flow_plugin_generation.c admission/materialize/destroy、flow_plugin_projection.c、Graph message projection/descriptor实现、实际 fixture/tests。至少三个真实实现/调用点；不要只从旧设计推导新协议。
- [ ] 先回答候选方案：同步借用输入→有界独立结果 vs 异步可取消 invocation；现有 Graph provider/emit/keyed接口适配能力；一个 message 只有一个 projection 时如何提交 typed decision sidecar 且不丢原 descriptor/payload/settlement。选择最小完整可执行边界，明确对不支持请求的 preflight 错误，不写宽泛承诺。不得为了消除 ENOTSUP 门禁假称 owner线程/硬deadline/异步已支持。
- [ ] 在 typed-operation-abi3.md 定义 exact descriptor、factory和执行vtable、输入输出/错误、权限与quota、注册callback及snapshot查询；确保 metadata/callback来源属于被pin模块，CMeta跨TU以语义比较而非地址相等。定义catalog identity/version冲突规则、schema匹配发生阶段和事务失败回滚顺序。
- [ ] 写出对象/状态/线程/容量矩阵；逐个说明 factory失败、异常输出别名/NULL、结果绑定失败、ownerbusy/retry、cancel与complete竞争、Graph compile失败和generation先销毁的可接受状态。若现有Graph缺少安全原子提交接口，明确最小新增接口及完整生命周期，而不是暗中依赖私有布局或另建状态事实源。
- [ ] 同步旧设计的 ABI 1.3历史与兼容段落：历史证据明确标历史，当前ABI2.0、批准目标ABI3.0；删除未来方案保留旧ABI的承诺。区分当前schema/projection已实现、operation尚未执行，以及本设计选定能力与#73后续真实引擎验收。
- [ ] 按 writing-plans skill 产出 runtime 实现计划，任务按可独立验证边界拆分；完整精确签名/数值/错误/文件/RED-GREEN测试场景、命令和期望结果。至少包含：ABI拒绝、注册重复/容量/中途失败/吞错、真实DLL generation/Graph路径、preflight不转移Graph、schema/effect/permission/quota/thread/cancel拒绝、inflight/clone/lease/cleanup、安装消费者唯一导出、双profile fullbuild/CTest。中间不可执行路径不开放capability，不保留兜底。
- [ ] 自审接口命名/类型/所有权跨任务一致性、所有要求的验收映射、无占位。git diff --check，显式提交三个任务文档。完整报告只写指定scratch，说明实际读取文件证据、关键决定/依据/风险、尚未实施边界及git状态；不虚报测试。

本合同设计需先经过任务审查，再由根代理采用具体 runtime 计划推进代码；设计不是功能验收，不单独关闭 #93。
