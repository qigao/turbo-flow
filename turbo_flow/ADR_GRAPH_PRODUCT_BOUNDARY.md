# ADR: Config、Graph 与产品装配边界

## 状态

已采纳。

## 背景

历史聚合 target 同时承担配置解析、Graph runtime、provider 装配和协议产品适配，导致数据处理核心
反向知道 wire pattern、session 与 acknowledgement 语义。这样既扩大公开 ABI，也让独立产品无法只
把 TurboFlow 当作可选的数据处理引擎。

## 决策

设备协议 codec/runtime 归属仓库可选的 `ingress/protocol` 集成层。它只依赖
`TurboFlow::Graph`，且不把 MQTT 作为固定中间格式；网络 listener 由仓库外的
CNet/CHTTP 宿主适配层拥有，MQTT 仅可作为 Graph 后的可选 Sink。

依赖保持单向：

```text
TurboFlow::Config <--- TurboFlow::Graph <--- TurboFlow::Product
                              ^
                              |
                    external product adapter

TurboFlow::Config + parsed Graph + PluginHost snapshot
                              |
                              v
                 transactional Graph generation
                              |
                              v
                  DLL resource/adapter vtables
```

- `Config` 只解析和校验配置，不创建 runtime 资源。
- `Graph` 拥有 DSL、plan、message、operation、执行器和通用 settlement contract。
- `Product` 的旧 provider registry 仅供显式 embedded consumer；Gateway 不以它作为 fallback。
- `PluginHost` generation 在消费 parsed Graph 前完成 catalog/ref/capacity/preflight 校验；消费后任何
  materialize/compile 失败都先销毁整张 Graph 以完成 registry shutdown/detach，再逆序销毁已转移
  owner。
- generation lease 覆盖 CFlow run、pending claim 与 callback；owner 逆序退休完成后才释放 DLL snapshot。
- external-poll owner 由 generation 的控制线程入口轮转推进；一次调用只有一个 owner 获得总等待预算，
  其余 owner 零等待。未声明 external-poll 的旧 v1.0 prefix 只参与生命周期，不创建隐藏 worker。
- ABI minor 3 以 root `TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL` 让旧 host 在调用 materialize 前拒绝新
  progress provider；新 provider 必须用 `turbo_flow_plugin_product_owner_publish()` 尊重 caller
  预置的 owner buffer 容量，禁止跨 DLL 整结构盲写。
- 外部产品拥有 wire protocol、连接、session、peer、ack、重连和持久化协议状态。
- 外部 adapter 可调用 Graph；Graph 不包含任何具体协议产品头文件、target 或 owner registry。
- `TurboFlow::Flow` 仅为现有消费者保留聚合 ABI，新代码选择最小 target。

## 影响与验证

公开协议专用 message sidecar 和 pattern helper 已移除。`turbo_flow_msg_t` 只携带 payload、通用执行
元数据、content descriptor 与可选 typed projection。同步和异步 publish 只报告 graph 执行状态。

验证至少覆盖：Config-only 链接、Graph 构建与测试、Product 装配、安装包消费测试，以及仓库级
扫描确认没有外部产品源码、构建 target 或包依赖。
