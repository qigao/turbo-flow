# ADR: Config、Graph 与产品装配边界

## 状态

已采纳。

## 背景

历史聚合 target 同时承担配置解析、Graph runtime、provider 装配和协议产品适配，导致数据处理核心
反向知道 wire pattern、session 与 acknowledgement 语义。这样既扩大公开 ABI，也让独立产品无法只
把 TurboFlow 当作可选的数据处理引擎。

## 决策

依赖保持单向：

```text
TurboFlow::Config <--- TurboFlow::Graph <--- TurboFlow::Product
                              ^
                              |
                    external product adapter
```

- `Config` 只解析和校验配置，不创建 runtime 资源。
- `Graph` 拥有 DSL、plan、message、operation、执行器和通用 settlement contract。
- `Product` 按 resolved config 装配本仓库 provider；装配失败必须销毁本次 generation。
- 外部产品拥有 wire protocol、连接、session、peer、ack、重连和持久化协议状态。
- 外部 adapter 可调用 Graph；Graph 不包含任何具体协议产品头文件、target 或 owner registry。
- `TurboFlow::Flow` 仅为现有消费者保留聚合 ABI，新代码选择最小 target。

## 影响与验证

公开协议专用 message sidecar 和 pattern helper 已移除。`turbo_flow_msg_t` 只携带 payload、通用执行
元数据、content descriptor 与可选 typed projection。同步和异步 publish 只报告 graph 执行状态。

验证至少覆盖：Config-only 链接、Graph 构建与测试、Product 装配、安装包消费测试，以及仓库级
扫描确认没有外部产品源码、构建 target 或包依赖。
