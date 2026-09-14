# 协议无关的业务数据流

## 决策与范围

2026-09-13：采用可配置 Source DLL → 统一业务 schema → 接收存储（内存/数据库）
→ RulesForge/TurboScript 业务图 → 可配置 Sink DLL。
这是目标架构；已有 transport adapter、protocol codec 和 operation ABI 不等于全部客户端
provider 或脚本引擎已交付。本次迁移由 [#117](https://github.com/qigao/turbo-flow/issues/117)
跟踪；客户端与引擎实现缺口仍由 #73、#74、#115、#116 跟踪。

```text
HTTP / WS / socket+codec / MQTT / FlowMQ
                  ↓ Source DLL 解码、校验、归一化
             同一版本的业务 schema
                  ↓
       接收存储：有界内存 或 TurboDB inbox
                  ↓ 按 demand 领取已接纳记录
       同一份 RulesForge / TurboScript 业务图
                  ↓
           业务结果 + 显式目的地
                  ↓ Sink DLL 编码、发送
         HTTP / WS / socket / MQTT / FlowMQ
```

Source 表示接收，Sink 表示发送，二者与 client/server 身份无关。同一个连接可以同时提供
两种角色，但连接、重连与协议状态只能由原生 owner 推进。Gateway 是装配和控制宿主；
它的 socket+binary parser 是应用协议 Source，encoder+socket 是对应 Sink。

MQTT/socket/HTTP/WS 的两种角色均通过配置选择 DLL provider 和资源实例，由统一 PluginHost
vtable 装配；同一个 DLL 可注册多个端点能力，不要求每个角色各占一个 DLL。
配置显式绑定 Source、schema、接收存储、业务图和 Sink 目的地。
缺少所选 DLL、schema、存储能力或容量立即失败，不选择另一协议、内存后备或静态实现。

### 配置加载与 vtable 边界

~~~yaml
version: 1
plugins:
  - id: turbo-flow.cnet
    version: 1.0.0
    path: C:/turbo-flow/plugins/turbo_flow_cnet_plugin.dll
  - id: turbo-flow.chttp
    version: 1.0.0
    path: C:/turbo-flow/plugins/turbo_flow_chttp_plugin.dll
adapters:
  socket.input:
    kind: cnet.stream_source
    config: { ... }
  http.output:
    kind: chttp.client
    config: { ... }
~~~

plugins 是有序 DLL 清单。每项只接受 id/version/path，其中 path 必须是绝对路径；
不接受候选目录、fallback DLL、静态 target 名或协议别名。配置解析生成不可变借用 view，
turbo_flow_plugin_host_create_configured() 创建新的有界宿主并逐项加载。宿主只查找
turbo_flow_plugin_get_api，取得版本化 root vtable 后，必须在调用 DLL 的 load() 前
精确核验配置 ID、版本以及 root ABI。provider 注册和实际能力仍由该 vtable 完成，配置不传递
函数地址或 DLL 内对象。

多 DLL 装配是一个启动事务：任一 open/symbol/ABI/身份/load/register 失败，已经提交的前序
DLL 必须逆序 quiesce、shutdown、destroy、unload，不能发布半配置 PluginHost。若回滚回调本身
失败，返回该可重试宿主和明确生命周期错误；调用方不得继续创建 generation。空 plugins
只表示当前配置不需要外部 provider；一旦 Graph 引用了未注册 kind，preflight 直接失败，
不会寻找同名系统 DLL 或链接进程内实现。

不选择“每种协议复制一张业务图”，因为会造成业务规则分叉；不选择 MQTT 作为内部总线格式，
因为会让 topic/QoS 成为所有业务的必需字段。复用 CFlow 的现有图编译、执行和调度，不新增执行器。

## 数据、处理与目的地

- Source 负责 wire 解码、输入校验、身份认证上下文及到业务 schema 的显式映射。
  codec 输出的原始 wire frame + metadata 只是适配层数据，尚不等于业务对象。
- 同一种业务的输入必须具有相同 schema 身份和版本；禁止按来源放宽类型校验。
  wire 编码相同不是 schema 相同的证明，来源名称也不属于业务类型身份。
- RulesForge/TurboScript 表达中间的业务转换、判定、路由、查询和存储命令。
  引擎通过受限、类型化 host capability 使用 TurboDB 等能力；不自建数据库、不持有裸连接。
  transport codec 不承载业务规则，普通网络回调不成为另一个业务执行入口。
- Sink 接收业务输出并使用显式绑定的目的地。输入来自 HTTP 或 MQTT 不应自动改变目标 topic、
  endpoint 或 peer。业务确需按渠道路由时，必须在规则中显式表达，而不是由 mapper 偷偷拼接。
- 请求响应的 reply handle 属于可选适配上下文，不能要求所有业务结果回原连接；
  需要回包的 Sink 验证 handle 的 owner/generation，不能把陈旧会话当作默认目的地。

## 状态、所有权与失败

### 先存储，再处理

实现与安装态验收由 [#118](https://github.com/qigao/turbo-flow/issues/118) 跟踪。

接收存储是统一的 inbox 能力。Source 校验并归一化后提交给宿主装配的存储边界，
只有存储接纳成功，记录才可供业务图领取；各协议不各自实现业务入库逻辑。
这一步保管待处理输入，领域数据的计算、查询、更新仍由 RulesForge/TurboScript 表达。

| 显式选择 | 接纳完成点 | 故障边界 |
| --- | --- | --- |
| 有界内存 | 存储 owner 已取得完整记录所有权 | 进程崩溃不保证恢复 |
| TurboDB inbox | 接收记录事务已提交 | 按声明的 claim/replay 契约恢复，不代表外部发送 exactly-once |

数据库失败不得自动改用内存，也不得绕过存储启动业务图。数据库路径的内存缓存只是已提交记录的
派生视图，不能独立推进消费状态。若配置要求“确认前持久接纳”，须证明原生 Source 支持该
确认时机；不支持则拒绝此保证，不把普通 MQTT ACK 等同于 inbox 提交。

存储 owner 唯一管理记录、claim 与完成状态；Graph 持有不可变输入 lease，业务输出是独立结果。
初始并发契约是多 Source 有界交接、单一串行存储 owner；图内并行使用既有调度器，
worker 不直接修改存储游标。记录 ID、schema/version、correlation、处理状态是受约束字段；
持久记录不保存进程指针、DLL 地址或裸会话句柄。

记录数、总字节、单记录字节和在途 claim 都有配置上限，数据库保留数据同样受配额约束。
只处理已接纳记录；图完成与 Sink 投递完成分别记录。图失败保留明确失败状态，重试须显式配置；
发送结果未知不得盲目重发。持久重放使用原记录身份和新 run，拒绝旧 schema，不自动转换旧布局。
关停先关闭 Source 接纳，再排空在途工作、归还 claim，最后销毁存储与协议 owner。

事实：现有 `io/turbodb/include/turbo_flow_turbodb.h`、
`io/turbodb/src/turbo_flow_turbodb_outbox.c` 及其测试提供 fetch/claim/settlement 的复用点，
但不证明统一 inbox 写入端、内存/数据库双实现或真实 Source DLL 装配已经完成。
具体存储接口和配置仍需实施验证，不提前暴露未实现 API。

### 协议与业务完成

协议会话/ACK 属于原生协议 owner；Graph 的 run/demand 属于执行层；业务事实属于领域事务。
规则会话是声明作用域内的工作状态，managed snapshot 只是只读投影，不独立推进这些状态。
跨异步边界的 borrowed 数据必须在回调返回前转为有界 owned/retained 数据，不能保存裸 socket。
复用已有 managed projection 和 generation lease；没有测量，不宣称零拷贝或性能提升。

必须分别报告：Source 接纳、Graph 完成、Sink 接纳、发送完成、协议确认、业务事务提交。
普通 MQTT Source 不要求等业务 Graph 或领域数据库事务完成后才允许原生 ACK。延迟业务结算属于显式增强能力，
不是基本接收/发送的前置条件，也不是运行时降级路径。原协议明确要求业务响应的路径仍保留其
pending/reply 契约。没有真实证据不得宣称持久交付或端到端 exactly-once。

Source 的交接队列按消息数/字节有界；demand=0 禁止向图继续发布，但不等于停止协议控制流。
满额、解析失败、缺少 schema/provider/权限必须明确报错；不无限缓存、丢弃后报成功或切换旧实现。
已接受工作在 quiesce/drain 中保留唯一 owner；超时/取消有唯一终态。
外部副作用不能回滚时报告失败阶段和已知/未知提交状态，只有显式幂等命令或补偿策略允许重试。

## 影响与迁移

旧 `TurboFlow::MqttSink` 仅映射 topic，没有连接和发送 owner，不能作为 Source/Sink DLL。
用户已明确批准不兼容旧结构/数据，因此直接删除该 API、DLL、package component、
ProtocolIngress 依赖及协议衍生 topic 行为，不提供替代 mapper、转换器或 C/CMake fallback。
真实 MQTT Source/Sink 由 #115 的客户端 provider 交付。

现有 RulesForge JSON provider 可在一张图中复用；先增加多 Source、同一业务节点和同一 Sink 的
边界回归，再删除旧 mapper，最后分别实现真实 Flowie/FlowMQ 和 Gateway 安装态链路。
客户端 SDK 依赖仅进入具体 provider，Graph/Core 不反向依赖完整产品。
#73 继续承担真正的引擎 DLL 与受控业务能力，不把测试 fixture 算作已实现插件。

部署替换先停止新接纳、排空旧 generation，再启用通过 preflight 的新 generation。
失败不发布半初始化实例；回滚只能整体恢复已验证部署包，不在进程内退回旧接口。
新实现不兼容旧结构、数据、配置或 DLL，不提供转换器。部署使用新配置和新存储命名空间。
拒绝旧数据不等于删除用户已有数据库；本次不执行数据删除或覆盖安装。

## 验证与验收

1. 相同业务对象分别经 HTTP/WS/socket/MQTT 适配边界进入，同一规则产生相同业务结果并到达
   同一 Sink；改变业务内容应改变结果，改变来源不应改变结果。
2. malformed/schema/version/权限错误在边界失败；协议 framing、ACK 与连接隔离原有测试继续通过。
3. 安装消费者确认旧 MqttSink component 不再导出，真实 MQTT provider 使用统一插件 ABI。
4. 每种真实 Source/Sink 分别验证网络、队列满、断线、取消、迟到回调、drain 和卸载。
5. 内存/数据库分别验证先接纳再执行、存储失败不启动图、满额拒绝、claim 唯一、
   崩溃恢复范围、旧 schema 拒绝及关停所有权；数据库失败不走内存路径。

仅用名为 http/ws/socket/mqtt 的 Graph Source 做测试，只证明图边界复用，
不证明四种网络协议已经接通；真实客户端链路仍由各自 issue 验收。
