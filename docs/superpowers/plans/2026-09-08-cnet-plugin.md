# CNet 统一 Provider DLL 实施计划（GitHub #69）

## 目标与不变量

- Gateway 仅链接 `TurboFlow::PluginHost`/`Product`/`Graph`，通过显式路径和唯一入口
  `turbo_flow_plugin_get_api` 加载 CNet 能力；缺 DLL、入口、kind 或配置均 fail fast，
  不回退到 `TurboFlow::CNetAdapter`、静态实现或替代 transport。
- 一个根 DLL 原子注册六个事务式 adapter kind：三个 Source 与三个 Sink。
- PluginHost snapshot 是模块租约事实源；Graph generation 是运行期 owner 事实源；CNet
  plugin root 只维护有界 owner 索引，不独立推进业务状态。
- preflight 只做纯配置/容量/平台能力验证。connect/listen/bind、Flow start 和 demand
  只能发生在 generation materialize/Graph start 的既定边界，失败按 generation 逆序回滚。
- CNet、CFlow demand/backpressure、异步 terminal settlement 和 stop/drain 语义保持不变。

## 架构选择

采用“根插件 + 六个 typed factory + 每实例 owner vtable”的分层：

1. 根插件由 Host allocator 创建，持有固定上限的 CSTL owner collection；根 vtable 只管理
   注册与模块级生命周期。
2. 每个 provider 的 `preflight` 从 resolved adapter view 读取严格 v1 schema，检查字段全集、
   类型、范围、字段依赖、显式 backend 支持性，不触碰 socket。
3. `materialize` 再读取同一 schema 并生成同 DLL owner。Sink 复用既有 register API；Source
   注册 Managed Source wrapper，实际 CNet open 在该 adapter 的 `start` 回调中完成。
4. Source 的 CNet open 公共实现增加精确 stage/managed-run 入口；原 embedded API 继续走普通
   `turbo_flow_run_open`，DLL 路径走 `turbo_flow_managed_source_run_open`。Managed run 始终由
   Flow 关闭，Source 只清空借用句柄，避免二次 close。
5. owner `poll` 调用既有 CNet Source/Sink poll；generation 提供每轮唯一阻塞预算和公平轮转。
   owner `quiesce` 关闭插件侧控制 admission，Graph stop 负责现有 adapter stop/drain，owner
   `shutdown` 验证已停止，Graph destroy 触发 adapter shutdown/detach，最后 owner `destroy`
   在 DLL/CRT 分配侧销毁句柄与存储。

## 配置契约

- 所有 kind 使用顶层、版本化 `schema_version: 1`，字段名全集按 kind 固定；未知、缺失、
  错类型和越界字段在 preflight 报结构化路径。
- backend 必须显式为 `iocp`、`epoll`、`io_uring` 或 `kqueue`，且本机不支持即
  `SALTS_ENOTSUP`；不提供 `auto`。
- TCP/TLS/Pipe、listener、UDP、KCP/secure-KCP 选择均由显式 mode/URI 与配套字段表达。
  容量、bytes、session、scheduler/actor step、timeout、TLS 文件引用、ALPN、packet/FEC/PSK
  均显式提供；空引用只在 schema 明确允许“关闭该能力”时有效。
- 外部格式只在 provider 适配层转换成 `cnet_*_config`，CNet 类型不扩散到 Gateway 配置层。

## 实施步骤（TDD）

- [x] 新增 DLL 加载/六 kind 原子注册测试并确认 RED。
- [x] 新增 strict-schema 表驱动测试：正确 TCP/TLS/Pipe/listener/UDP/packet 配置，以及缺失、
      未知、错类型、越界、依赖冲突、backend 不支持；确认 preflight 无网络副作用。
- [x] 为三个 Source 增加 managed-run open 路径与直接/managed 生命周期回归测试。
- [x] 实现根插件、CSTL owner collection、六 provider 与 owner vtable；补 capacity
      0/1/N/N+1、materialize 中途失败、逆序唯一销毁、snapshot/owner lease 阻止卸载测试。
- [x] 增加 TCP 与 UDP 代表性 loopback，验证 demand、poll、terminal settlement、stop/drain。
- [x] 增加安装态 C/C++ Gateway consumer：不请求、不链接 `TurboFlow::CNetAdapter`，显式加载
      安装目录中的 provider DLL；缺 DLL/坏路径不走任何替代路径。
- [x] 检查 provider export table 仅有 canonical root symbol，dependency/CRT 表符合动态边界。
- [x] 运行 focused repeat、现有 CNet adapter 回归、Debug/ASan 全量、Release 全量、
      `install-win-dev-user`、安装态消费者与 `git diff --check`。

## 兼容性、成本与验证范围

- 公开影响：新增 provider DLL、六个 kind 字符串与 Source managed-open C ABI；既有 embedded
  `TurboFlow::CNetAdapter` API/行为不删除。Gateway 部署必须同时携带 provider DLL 及其动态
  CNetAdapter/CNet 依赖，这是明确的部署变化，不提供旧路径兼容分支。
- 状态影响：网络句柄仍由具体 CNet adapter 独占；plugin root 仅跟踪 owner，generation
  独占生命周期推进；任何失败保留第一个错误，未完全 drain 的 generation 可重试且模块不卸载。
- 性能影响：热路径只增加 generation 每轮一次 vtable 间接调用；消息与 CNet buffer 路径不
  增加复制。owner collection 在 load 时按硬上限 reserve，运行中不增长越界。
- 回滚：Provider DLL 是新增产物；若回滚该功能，移除 DLL/安装消费者和新增 kind 即可，既有
  embedded API 与测试仍独立存在。不能以运行时 fallback 作为回滚手段。
