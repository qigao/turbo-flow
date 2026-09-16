# ADR: Projection Fields in Graph Route Expressions

## 状态

已采纳。公开 C ABI 只增加 size-versioned projection-expression 注册结构和注册函数；
现有 DSL、message wire、projection ownership 和内置 `msg.*` 字段保持不变。

## 背景

TurboFlow 条件边已经使用 typed expression engine，但此前只在 graph 编译和执行时提供
`msg.*` 内置字段。DataBind/RulesForge 可以把 schema-bound projection 附加到
`turbo_flow_msg_t`，表达式引擎也支持 `parsed.*` 外部字段，两者尚未在 graph 边界连接。

把 `DataBindObject` 或 RulesForge session 直接放入 core 会扩散第三方类型、错误码和生命周期；
在运行期重新解析 schema 或编译表达式则会增加不受限开销并破坏 compile-time fail fast。

## 决策

flow registry 接受 immutable projection-expression registration：

```text
provider schema identity + parsed field table + read-only getter
        -> flow-owned copied registry
        -> graph compile resolves parsed.* to stable field ids
        -> route evaluation selects getter by message projection schema
```

provider 持有 getter context；flow 复制 schema identity 和字段路径。message 继续独占 projection，
getter 仅在一次同步求值期间借用它，不得 retain 或修改。compile 后 registry 不可变，因此并发
publish 只读 registry，不增加锁或分配。

同一个 flow 内共享 path 的注册必须使用相同 type 和 field id，同一个 field id 也只能表示一个
path。冲突、未知字段、缺失 projection、schema 不匹配、getter 错误和返回类型不一致均 fail fast，
不回退到 payload 字符串解析。

## 候选方案

- RulesForge 写 `msg.flags`：兼容且开销最低，但丢失字段级 graph 可读性，保留为简单判定路径。
- core 直接依赖 DataBind：可自动反射字段，但把第三方对象和 ABI 扩散到 graph core，拒绝。
- 每条消息动态编译 schema/表达式：支持任意 schema，但引入热路径分配和不确定延迟，拒绝。

## 影响

- 架构：DataBind/RulesForge 适配器负责 schema 到 expression scalar 的映射；core 只依赖 opaque
  projection 和 typed getter。
- 接口：provider 在 compile 前注册字段；reset 保留 registry 时继续有效，否则清理。
- 状态：projection 仍以 payload 为事实源的派生视图，不产生第二业务事实源。
- 错误：schema/字段冲突在注册或 compile 阶段失败，运行期 projection/getter 问题作为 route
  evaluation error 传播。
- 性能：compile 合并字段表需要 O(SF) 临时空间；publish 查找 schema 为 O(S)，字段读取 O(1)
  加 provider getter。S 为注册 schema 数，F 为字段数；运行期无新增堆分配。
- 迁移：现有 `msg.flags` graph 不变；需要字段路由的 provider 增加一次注册即可。
- 回滚：删除 projection registration 和 `parsed.*` 条件边即可恢复到 `msg.*` 路由，无数据迁移。

## 验证

最小验证覆盖：已注册字段成功编译、RulesForge 工作消息按 `parsed.age` 分流、未命中条件边成功
终止、未知字段编译失败，以及 projection/getter 错误保持 fail fast。
