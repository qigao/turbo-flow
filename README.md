# TurboFlow

TurboFlow 是基于有向 graph 的可配置数据处理器。仓库只拥有 Graph DSL、编译与执行、配置解析、
typed projection、调度、可观测性，以及可选的存储、网络和 gateway adapter。

协议 server、消息队列产品及其 wire protocol、session、peer、ack 和重连状态不属于本仓库。
这些产品可以把数据归一化为 `turbo_flow_msg_t` 后调用 TurboFlow，但 TurboFlow 不反向依赖它们。

## 构建边界

| CMake target | 职责 |
| --- | --- |
| `TurboFlow::Config` | 解析并校验产品配置，生成只读 resolved config |
| `TurboFlow::Graph` | Graph DSL、编译、执行和通用 operation/adapter API |
| `TurboFlow::Product` | 用 resolved config 装配 Graph 与本仓库 adapters |
| `TurboFlow::Flow` | 兼容聚合 target；新代码优先链接最小 target |

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
