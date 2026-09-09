# Typed-operation DLL 边界

状态：设计决策已获用户确认；实现与验收仍在进行。本文件不是功能完成声明。

跟踪：[TurboFlow #73](https://github.com/qigao/turbo-flow/issues/73)，父任务
[#63](https://github.com/qigao/turbo-flow/issues/63)，数据处理闭环
[#28](https://github.com/qigao/turbo-flow/issues/28)。

## 背景与证据

以 `eb8a445ac7832b6839c748994c5a75f93b63ca1b` 为实现基线：

- `turbo_flow/include/turbo_flow_plugin.h` 的 ABI 为 1.3；注册类别覆盖
  adapter/resource/protocol/business，没有 schema/typed operation。
- `turbo_flow/src/flow_plugin.c` 已实现有界 CSTL catalog、注册失败回滚、
  immutable snapshot 与 DLL lease；不应再创建第二套插件注册事实源。
- `turbo_flow/include/turbo_flow.h` 已有 operation provider；
  `turbo_flow_domain.h` 已有 operation 的 domain、scope、effects 相关运行约束。
- `turbo_flow/src/flow_rulesforge.c` 将 RulesForge session 执行接到现有 provider，
  但注册逻辑直接访问 Graph 内部注册表；不能原封不动迁入独立 DLL。
- `turbo_flow/CMakeLists.txt` 与 `cmake/TurboFlowConfig.cmake.in` 将 RulesForge
  传播到 Graph、Product、PluginHost 和聚合库消费闭包。

用户于 2026-09-09 明确批准扩展公开插件 ABI、移除旧 RulesForge 直接入口及
Core 必需链接，改为显式 DLL 加载且无 fallback。既有 DSL 的 source/stage
拓扑语法保持不变；引擎资源配置和直接调用消费者需要显式迁移。

## 方案比较与选择

| 方案 | 影响 | 决策 |
|---|---|---|
| 只将现有 RulesForge 桥接包装成 DLL | 保留内部 Graph 耦合，不能验证 typed 契约及 lease | 不采用 |
| 新建独立脚本调度器与 operation 注册表 | 重复 CFlow 执行和 PluginHost 状态，增加取消与热更新事实源 | 不采用 |
| 扩展现有 catalog，装配后绑定现有 Graph provider | 复用事务、compile、run 与 terminal 所有权；新增版本化边界 | 采用 |

这是插件化和分层图执行的组合，不是新的图算法或解释器。CFlow 继续负责图执行；
引擎 DLL 只负责受约束的 typed 输入到 typed 结果转换。

## 契约分层

1. **Schema 注册**：size/ABI-version 包装不可变 CMeta 元数据，携带稳定 schema
   identity、版本、storage type 及所有权说明。跨 DLL/TU 用 `cmeta_type_equal`
   判断类型兼容，禁止以地址相等作为语义条件。
2. **Operation 注册**：声明 operation identity/version、输入/输出 schema、effects、
   权限需求、支持的配额和取消模式、owner/threading 约束，以及版本化工厂与执行 vtable。
   描述符不是一个可任意 eval 的字符串入口。
3. **装配 binding**：host 根据已解析配置选择准确的 operation/resource，验证资源
   artifact 和执行要求，再创建 opaque owner，连接既有 Graph operation provider。
   新引擎执行接口不接收可任意修改的 Graph、Gateway 或 registry 指针。
4. **执行**：host 适配层读取已验证 typed projection，调用缓存的执行 vtable，
   校验结果后提交 bounded typed decision sidecar。DLL 不能通过 message 获取网络、
   数据库或原输入 settlement 的可写 owner。

Host 的注册配置和 callback 表采用尾部扩展并增加 ABI minor；新增类别只在完整
size-prefix 与版本都满足时可用。旧 ABI 插件的原有能力仍按其原契约加载，
但不能借此获得新能力。旧 Host 遇到要求新类别的插件必须拒绝。
非 size-version 化的 CMeta 描述符布局不得修改。

注册期间一旦出现非法 descriptor、重复 identity/version、容量不足或 capability
声明不一致，本次模块的所有新条目回滚；即使 DLL 吞掉 add 回调错误，host 仍返回
首个错误。不得留下部分 schema 或 operation 对其他 generation 可见。

## 所有权与线程协议

| 对象 | 唯一 owner | 生命周期边界 |
|---|---|---|
| 注册数组与 snapshot | PluginHost | snapshot 引用归零后释放数组与模块 lease |
| DLL descriptor、vtable、静态 metadata | DLL | 所有引用该 metadata 的对象销毁前不可卸载 |
| 已编译 binding 与执行策略 | generation/Graph | 不可变，数据面不再查询 catalog 或动态符号 |
| 原生 KB、script module、session/instance | 对应引擎 owner | 按引擎原生销毁 API 释放，不跨 CRT free |
| 在途输入、输出和 terminal | 当前 run | 调用完成或取消完成后恰好释放一次 |
| durable review 与 disposition | 原 durable owner | operation 只返回决策，不能独立提交事实状态 |

控制面创建、lease 变更和销毁保持现有 caller-serialized 约束。数据面 worker
不得直接修改控制线程的非原子计数；跨线程完成先经既有 owner/执行机制回到规定边界。
未声明线程安全的 session/instance 不跨 worker 共享。不能仅凭 KB/module 被声明
immutable，就推导其原生 runtime 或全局初始化可并发。

输入 view 默认只借用到调用返回；异步保留必须显式 retain，并纳入 run/module lease。
结果若含 DLL-owned metadata 或 destroy callback，消息及 clone 都必须持有对应 lease；
不能只保护 Graph 存活期间，也不能在 generation 销毁后留下失效回调。

`turbo_flow_projection.h` 的显式 retained owner 接收 immutable payload、跨线程
clone/destroy 和 independent context 三项完整声明；不满足时直接拒绝。Graph
维护唯一的 count/byte reservation 事实源，clone 在调用 DLL 前预留额度，失败产生的
临时值在 DLL destroy 完成后归还额度。每份结果固定按 provider 的
`max_result_bytes` 收费；它是可信上界声明，不是 malloc 拦截或 native sandbox。

`turbo_flow_plugin_projection_owner_create` 是 PluginHost 到 Graph 的薄桥接：
控制线程 retain 一个 live catalog snapshot，复制 config/schema wrapper，worker
只转发 clone/destroy 到原始独立 context。可信 host binding 必须保证 snapshot
包含所有 callback/metadata 所属模块及依赖；这不是 DLL operation registration
capability，也不替代 #73 的 catalog 来源校验。Graph 不反向依赖 PluginHost。

独立结果 owner 可越过 generation；**依赖 session 的结果必须在 session 销毁前
drain**，不能把 session 指针伪装成 independent context。owner stop 关闭新 bind/clone
准入，已接受调用可完成；控制线程须禁止后续 API 进入并等待全部 worker API 返回，
再销毁 owner。计数归零不足以证明所有裸 owner 使用者已静止。context release
失败保留 owner 和 snapshot 供重试；成功才释放 snapshot，因此顺序为 payload
destroy 完成 → context release 成功 → snapshot 释放 → 模块 destroy/unload。
原 borrowed projection 和 descriptor 生命周期契约保持不变，clear_projection
不会自动延长 descriptor 的寿命。

真实 DLL 集成测试覆盖 clone/move/clear、失败临时值、context 重试、callback 屏障
和 generation 先销毁；Debug/ASan 用于检测内存错误，不证明没有数据竞争。

关闭顺序为：停止 admission → 等待已接受调用完成或完成取消 → 清空输出引用 →
销毁 session/instance → 销毁 KB/module → 释放 generation snapshot → 卸载 DLL。
quiesce/drain 失败保留 owner 与 lease，返回明确失败；禁止强杀线程或提前释放。

## 配额、权限与错误

artifact、输入、输出、并发调用和 session 数量分别定义配置容量与计量单位；
检查乘加溢出。CPU 步数、规则触发数、deadline、保留内存不是可互换的额度。
host 只有在原生契约足以兑现要求时才允许装配；调用结束后检查耗时不能被称为超时保障。

权限需求由 provider 声明，授权来自 host 配置/策略，插件不得自行扩大授权。
不提供任意宿主函数名解析、插件间直接调用或不受限的 I/O callback。
同进程 DLL 的能力白名单不是 OS 安全沙箱，也不提供原生崩溃隔离；不得用
signal/longjmp 恢复原生内存破坏。要求更强隔离的部署不能被静默接受。

失败输出不提交到 message；原始错误保留 operation identity、阶段和引擎状态。
decision 为 REVIEW/RETRY/REJECT 等建议，不代表已执行持久化或外部副作用。
真正的 approve/retry/replay 和审计仍走 #28/#75 的原 owner 命令。

## 引擎边界

### RulesForge

DLL 仅通过 `turbo_flow_plugin_get_api` 发现；引擎头和 native handle 留在插件内部。
KB artifact 在激活前按显式版本、内容校验及容量加载，不能在逐消息热路径读取或编译。
全局 init/cleanup 必须核实多 host、多 generation 与其他原生消费者的兼容性；
不能让一个插件 owner cleanup 另一个 owner 仍使用的 runtime。

安装版的 `ruleforge_session_fire_all_rules` 只提供 max_rules。
[RulesForge #6](https://github.com/qigao/RulesForge/issues/6) 跟踪有界执行、取消、
预算耗尽状态与 session 生命周期。其完成前不能宣称 RulesForge 支持这些完整能力。
缺失保障的请求应在 preflight 拒绝；这一拒绝不等于 #73 的完整取消/配额验收已完成。

### TurboScript

安装头 `turbo_script/debug/include/turbo_script.h` 已有 Host ABI 1：owner-thread
调用、immutable module、隔离 instance、max_steps 和 interrupt callback。
因此不能继续将 TurboScript 认定为“无公开 Host ABI”。接入前仍须验证 allocator、
callback 权限、模块 import、执行后端、配额覆盖和取消观察点。

TurboScript 使用独立 plugin ID 和 artifact，既不是 RulesForge 缺失时的替代品，
也不把 JIT 不支持自动切换成 interpreter。未通过该引擎自己的验收前不暴露可用能力。

## 文件归属与迁移顺序

- `turbo_flow/include/turbo_flow_plugin_operation.h`：新类别的公共 size/version 契约；
  不包含 RulesForge 或 TurboScript 头。
- `turbo_flow/src/flow_plugin.c`：扩展现有事务与 snapshot，不新建第二套模块 registry。
- `turbo_flow/src/flow_plugin_generation.c`：operation preflight、绑定与 owner 生命周期。
- `turbo_flow/tests/`：真实 DLL fixture、C/C++ ABI、重复注册和回滚、lease、权限/类型拒绝。
- `plugins/rulesforge/`：原生 KB/session 与 typed decision 转换、配置和插件测试。
- `turbo_flow/CMakeLists.txt`、`cmake/TurboFlowConfig.cmake.in`：删除 Core 引擎闭包。
- 既有 RulesForge 测试迁到 DLL 消费边界，随后删除 `flow_rulesforge.c` 和旧公开头的
  直接入口；不得以长期 compatibility target 保留旧路径。

先交付可独立验证的注册/lease 契约，再接入 generation，随后完成引擎 DLL 和
安装依赖解耦。删除旧入口与消费者迁移必须在同一可构建提交中完成。
中间提交不能将未实现的 operation 作为可执行 capability 暴露。

回滚方式是部署完整的已验证旧版本及其匹配配置，或保留尚未被替换的旧 generation；
不是新版本自动调用 legacy engine。新 generation preflight 失败不触碰旧 generation；
materialize 后失败按 owner 逆序清理，清理无法安全完成时保留可诊断的 failed 状态。

## 验证门槛与性能边界

- C11/C++ 头、旧/新 size-prefix、错误 ABI、未知 capability、重复 identity、容量
  0/1/N/N+1、注册中途失败和被插件吞掉的错误。
- 相同 CMeta 类型的不同 TU/DLL 地址可兼容；相同名字但不兼容 identity/布局必须拒绝。
- missing schema、operation/version、权限、artifact、配额和线程模型在 start 前失败。
- 调用、session、run、message clone、owner 任一 lease 存活时不可卸载；失败与取消
  不能产生第二个 terminal 或遗留半初始化输出。
- 独立安装消费者只链接 PluginHost/Product/Graph；其链接与运行依赖不含 RulesForge。
  单独加载引擎 DLL 后验证其原生依赖，缺失依赖即失败，不搜索其他 profile SDK。
- 单一导出、Debug/Release CRT、显式依赖根、Debug/ASan 和 Release 全量 CTest、
  focused repeats 以及两种配置的安装消费测试。
- 热路径不进行 registry/symbol lookup；仍需一次缓存 vtable 调用，这是用户要求的
  DLL 边界，优先于仓库通用的“热路径禁止间接调用”建议。不能声称完全 direct-call。
  不以架构变化宣称性能提升；吞吐、尾延迟和原生 session 分配成本由 #11 实测。

编译期 catalog 解析可以复用现有有界线性查找；其成本随 provider 数量和 binding
数量增长，不进入逐消息路径。需要优化时先以实际容量和 profiling 证明瓶颈。
完整 #73 验收必须逐项对照 issue，不能以通用 catalog 测试替代真实引擎、安装和
执行契约证据。
