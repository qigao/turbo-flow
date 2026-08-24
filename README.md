# TurboFlow

TurboFlow 是基于有向 graph 的可配置数据处理器。仓库只拥有 Graph DSL、编译与执行、配置解析、
typed projection、调度、可观测性，以及可选的通用存储和网络 adapter。

消息队列等完整产品及其控制面、业务 session、peer 和重连事实不属于本仓库。
这些产品可以调用 TurboFlow，但 TurboFlow 不反向依赖它们。

`ingress/protocol` 是独立于产品的可选协议前端：OCPP、JT/T 808、GB/T 32960、CoAP
等协议通过 CoroNet 完成分帧与校验后直接进入 Graph。MQTT 只是可选 Sink，不是内部消息格式。

## 构建边界

| CMake target | 职责 |
| --- | --- |
| `TurboFlow::Config` | 解析并校验产品配置，生成只读 resolved config |
| `TurboFlow::Graph` | Graph DSL、编译、执行和通用 operation/adapter API |
| `TurboFlow::Product` | 用 resolved config 装配 Graph 与本仓库 adapters |
| `TurboFlow::Flow` | 兼容聚合 target；新代码优先链接最小 target |
| `TurboFlow::ProtocolIngress` | 可选 protocol codec/runtime，不依赖 MQTT broker |
| `TurboFlow::ProtocolIngressGraph` | 将中立协议消息投递到 `TurboFlow::Graph` |
| `TurboFlow::ProtocolIngressCoroNet` | 可选 Socket/TLS/WS 协议监听与有界 session runtime |
| `TurboFlow::MqttSink` | 批量映射中立消息；不拥有 codec/session 或任何 I/O connection |

所有构建开关只在 `CMakeOptions.cmake` 声明。不得在子目录新增隐藏 option，也不得把外部产品源码、
协议状态机或安装组件重新并入本仓库。

## Windows 验证

```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user --parallel
ctest --preset win-release-user --output-on-failure
```

安装后的 `TurboFlowConfig.cmake` 导出上述 targets 以及本仓库实际构建的 adapters；不会查找或导出
外部协议产品组件。

## 形式化模型

[形式化模型规范](docs/FORMAL_FLOW_MODEL.md) 与 [Lean 验证入口](formal/README.md) 描述核心路由、fan-in 与生命周期的抽象证明。它不是对 C 源码、编译器输出或并发内存模型的 refinement proof。
