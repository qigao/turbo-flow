# Transport-independent business graph implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task.

**Goal:** 让多种 Source 复用同一业务处理图，并纠正文档与 issue 中的协议/业务结算耦合。

**Architecture:** 复用 CFlow Graph、RulesForge provider 和 managed projection。Source/Sink
承担外部协议适配；RulesForge/TurboScript 承担业务处理；原生 owner 保持协议状态唯一归属。

**Tech Stack:** C、Salts/CFlow、RulesForge、TinyTest、CMake presets、GitHub issues。

## 1. 设计与任务边界

- 新增 `docs/architecture/transport-independent-business-graph.md`，覆盖状态、错误、迁移与验证。
- 更新 README 与 protocol 文档，明确 codec 原始帧尚不是统一业务对象。
- 修改 #73/#74/#115/#116；Flowie #22 保留为可选增强，不再阻塞普通 Source/Sink。

## 2. 图边界回归

- 在 `turbo_flow/tests/test_flow_rulesforge.c` 扩展真实 JSON provider 测试：
  四个 Source 汇入同一 rulesforge 节点，经同一 dispatch 到同一 sink。
- 每个来源发送成年对象和未成年对象，验证规则命中与过滤不依赖来源，并保留 projection 不被替换的断言。
- 先运行已有测试基线，再运行新增回归；这是既有能力的特征测试，不虚构生产修复的 RED/GREEN。
- 使用 `cmake --build --preset win-release-user --target test_flow_rulesforge`
  和相同 test preset 的精确名称过滤；随后运行 protocol/Graph 相邻回归。

## 3. 不兼容迁移与统一接收存储

用户已确认与旧结构/数据不做任何兼容。旧 MqttSink 只是无 I/O mapper，因此删除完整 API、
DLL、package component、protocol-derived topic 和测试；不以新 mapper 延续伪 Sink。
新数据存储使用独立命名空间，不删除用户旧数据库。

用户补充 Source DLL 接收后先存内存/数据库，再进行业务图处理。先落实统一 inbox 的
写入/领取/完成契约与宿主装配，再连接真实 Source/Sink；旧 MQTT mapper 删除只是子任务。
优先核对已有 TurboDB outbox Source、managed projection 与 DLL host service 的复用边界。
RED/GREEN 必须覆盖存储失败不执行图、内存/数据库显式选择、容量、claim、旧 schema 拒绝。
当前尚未实现 inbox 或新 mapper，不以已有图边界测试替代这些验收。

## 4. 交付

- 用 GitHub issue 记录实现状态与仍未完成的真实客户端/引擎 DLL 工作。
- 检查 diff 和定向测试输出；只声明实际验证的范围，不推送、合并或覆盖已安装 SDK。

## 5. 配置驱动 DLL 装配

- 在 resolved config 增加有序 plugins: [{id, version, path}] 投影；拒绝未知字段、
  重复 ID、相对路径与超限字符串。
- 在 PluginHost 增加从不可变配置创建宿主的入口；仍只使用唯一 discovery export 和 root
  vtable，且在 plugin load() 前精确核验 ID/版本。
- 多 DLL 中后续模块失败时逆序回滚已加载模块；不保留半装配 catalog，不搜索替代 DLL。
- 用 config/PluginHost 单测覆盖投影、身份不匹配前不执行回调及多模块回滚；安装消费者覆盖
  安装后的公开入口。
