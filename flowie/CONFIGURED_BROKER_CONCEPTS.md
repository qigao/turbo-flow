# Flowie 配置式 Broker 概念与术语

## 1. 定义范围

本文规定 Flowie 服务端文档、配置和架构讨论中的术语。这里的“配置模式”不是一个运行时布尔
开关，而是一种产品装配方式：`flowie_server` 从 provider registry 获取宿主能力，从 YAML
创建具名实例，再用 `.flow` Graph 连接这些实例。

只有这一完整的配置式 broker 才是本文所说的“基于 TurboFlow 的典型消息处理应用”：

```text
Flowie MQTT protocol/session owner
  + product provider registry   # 宿主编译并注入哪些能力
  + YAML resolved config        # 创建哪些 endpoint/channel/adapter 实例
  + TurboFlow Graph             # source/stage/sink 的拓扑和处理顺序
  = configured Flowie broker
```

以下三种使用方式不能混称：

| 使用方式 | Flowie 提供什么 | 是否是配置式 TurboFlow 应用 |
|---|---|---|
| 协议库 | MQTT parser、packet view、编码与校验 | 否；没有运行时或 Graph |
| 嵌入式 endpoint | 由调用方注册和持有的 MQTT endpoint primitive | 不一定；产品拓扑由调用方负责 |
| `flowie_server` 配置式 broker | YAML、Graph、provider registry、endpoint/session owner 的完整装配 | 是 |

嵌入式 endpoint 可以成为另一个 TurboFlow 应用的一部分，但 endpoint 本身不代表已经存在
data source、data sink、持久化和业务处理拓扑。

### 可交付产品组合

| 产品 | 消息 handoff | MQTT session / retained | PUBLISH 业务记录 | MQTT ACK 边界 |
|---|---|---|---|---|
| `dev` | bounded memory | memory | memory | accepted |
| `smb` | not used | PostgreSQL record store | PostgreSQL outbox | durable |

`dev` 用于开发和单机验证。`smb` 的 MQTT endpoint 直接把 admitted PUBLISH 写入 PostgreSQL
outbox；只有 INSERT 事务 COMMIT 后才完成 `durable` settlement 并生成 QoS ACK。独立 outbox
source 回放消息，经 Policy 和 MQTT delivery Graph 成功后删除记录；处理失败或进程退出会保留记录，
因此该链路提供 at-least-once delivery，不会退回内存。outbox 同时保存消息 type/flags 与可序列化
protocol origin；live route capability 不会进入数据库，回放由当前 endpoint owner 重新执行 fan-out。

对应示例位于 `examples/products/flowie-dev.*` 与 `examples/products/flowie-smb.*`。SMB 示例
中的 `conninfo` 是部署模板；生产环境必须通过受限配置或 secret provider 注入真实凭据。

## 2. 规范术语

### Provider

`provider` 是 composition root 视角的实现提供者。它把一种受信任的原生能力注册到产品宿主，
例如 `turbo_flow_product_adapter_provider_t`、TurboFlow Policy resource provider、auth provider
factory 或 ACL policy provider factory。Provider 决定“这个宿主能够创建什么”，不决定消息
在 Graph 中从哪里流向哪里。

文档不得单独使用含义不明的 `data provider`。必须根据实际接口写成以下术语之一：

- **adapter provider**：根据 YAML `adapters.<name>.kind` 创建 Graph adapter；
- **resource provider**：根据 channel 创建 TurboFlow Policy 等 Graph resource；
- **auth provider**：验证 credential，返回 principal 或拒绝；
- **policy provider**：提供版本化 ACL bundle；
- **facts provider**：把 `turbo_flow_msg_t` 投影为 TurboFlow Policy facts；
- **external data service**：Redis、PostgreSQL、HTTP 服务等进程外系统。

这些 provider 接口用途不同，不能因为名称中都有 `provider` 就互换。

### Backend

`backend` 是某个 provider 或 store 内部选择的具体实现，例如 FlowStore 的 `memory`/`redis`，
record store 的 `redis`/`postgresql`，或 auth provider 的 `https`。Backend 是实现细节，不是 Graph
角色，也不自动成为 data source 或 data sink。

### Adapter

`adapter` 是 Graph 可引用的具名边界。它把外部协议或存储格式转换为
`turbo_flow_msg_t`，或把 `turbo_flow_msg_t` 交给外部系统。YAML 创建 adapter 实例，`.flow`
决定该实例在拓扑中的位置。

Adapter 应保持为薄边界：负责格式转换、生命周期和错误映射，不拥有 MQTT session 规则或
业务路由策略。

### Data source、data sink 与 transform

这三个词是 Graph 视角的角色，不是 provider 类型：

| Graph 角色 | 定义 | 所有权结果 |
|---|---|---|
| **data source** | 把外部事件转换为 `turbo_flow_msg_t` 并发布到 Graph | 发布成功后按 source 契约转移或保留消息所有权 |
| **data sink** | 消费 `turbo_flow_msg_t` 并产生外部副作用 | 成功只证明该 sink 的契约完成 |
| **transform** | 消费消息并产生修改后的消息或投影 | 输出继续由 Graph 管理 |

同一个 adapter 实现可以支持多个角色。例如 HTTP server 可以接收 request 并发送 reply，
Flowie endpoint 可以发布 admitted PUBLISH 并接收 MQTT fan-out packet。实际允许的角色由
adapter schema 与 Graph 节点共同约束，不能从 `kind` 名称猜测。

Data sink 也不等于 durable sink。只有 adapter 契约明确规定事务已提交，并且 settlement 绑定
到该提交边界时，成功结果才能称为 `durable`。

### Channel 与 resource

`channel` 是 YAML 中具名、带 owner 和容量/连接配置的共享资源。Graph operation 可以通过
`resource <channel-name>` 引用 TurboFlow Policy RuleSet 等资源。Channel 不是消息，也不是
Graph edge。

### `turbo_flow_msg_t`

`turbo_flow_msg_t` 是 Graph 的统一消息 envelope。Provider 内部的 MQTT frame、
HTTP request view、Redis entry view 等短生命周期对象不能直接跨异步边界；进入 Graph 前必须
转换为满足所有权契约的 `turbo_flow_msg_t`。

### FlowStore

FlowStore 独立于 Flowie，按 State、Index、Log、TimeSeries 保存类型化事实。内存与 Redis
backend 不能同时作为同一状态的写事实源；bitmap 仅可作为可重建的整数查询索引。FlowStore
提交成功只证明对应 store 契约完成，不能替代另一个业务 sink 的事务。

### Session owner 与 session store

FlowStore MQTT fact facade 是 MQTT session、subscription、packet ID、inflight QoS、Will 和
retained 状态的唯一事实源。Flowie session owner 只持有从 facade 重建的单 owner 工作缓存；
`session_store` 是该 facade 绑定的 Record backend，不是用户 Graph 中的 data sink。

普通 PUBLISH 的网络帧和解析缓冲是传输临时态，不会成为 session 事实；需要保留的 MQTT 事件
必须显式写入 FlowStore Log/Series 或其他已声明的业务存储，不能由 Flowie 内部容器偷偷保存。

### TurboFlow Policy

TurboFlow Policy 是可选的 Graph processor/resource。Facts provider 从 `turbo_flow_msg_t` 提取 MQTT topic、
QoS、payload 等受限视图，RuleSet 执行过滤、转换和路由。Policy 不是 data source、data
sink 或 auth provider，也不能推进 MQTT session 状态或生成协议 ACK。

## 3. 同一对象在不同视角下的名称

同一个外部系统会因观察层级不同而有不同叫法。应先说明视角，再使用术语：

| 观察视角 | Redis 示例 | 正确含义 |
|---|---|---|
| 部署/基础设施 | Redis service | 进程外 external data service |
| Provider | Redis adapter provider | 宿主可根据 `kind: redis` 注册 adapter |
| Store backend | FlowStore Redis provider | Hash/Set/Stream/TimeSeries 的远端事实实现 |
| Graph source | Redis Stream source adapter | 从 Stream 读取并发布 `turbo_flow_msg_t` |
| Graph sink | Redis SET/Stream sink adapter | 消费 `turbo_flow_msg_t` 并执行外部写入 |
| 协议持久化 | Redis record-store backend | 保存 session/retained record，不进入 Graph |
| 认证系统内部 | auth service 的用户数据库 | Flowie 不直接访问，也不由 Graph 配置 |

因此“Redis provider”“Redis storage”不足以表达设计。必须写明它是 FlowStore backend、record-store
backend、Graph source/sink adapter，还是认证服务内部数据库。

## 4. 配置式 Broker 的职责分配

### Product provider registry

Composition root 注册当前二进制允许的 adapter/resource/auth/policy provider。未注册的
adapter `kind` 在 product preflight 失败；Graph 引用但没有 provider 的 resource `kind` 在
assembly 失败；auth/policy backend 必须精确匹配一个已注册 factory。Registry 是宿主能力
白名单，不保存业务拓扑。

### YAML

YAML 定义部署实例和边界条件：endpoint、transport、channel、backend、容量、超时、secret
reference、RuleSet 和 adapter 配置。YAML 不定义 Graph edge，也不隐式选择业务输出。

### `.flow` Graph

Graph 定义 source、operation、transform、sink、分支以及 save/store/after-process 的顺序。
Data source 和 data sink 只有被 Graph 显式引用后才属于当前 broker 数据路径。

### Flowie protocol/session owner

Protocol owner 在 Graph 之前完成 framing、协议校验、认证、ACL 和 inflight admission；在 Graph
或 settlement 边界完成后决定 MQTT ACK。CONNECT、AUTH、SUBSCRIBE 等控制事务不因为存在
Graph 就自动成为业务消息。

## 5. 标准数据路径

```text
network bytes
  -> Flowie endpoint parser
  -> auth provider / ACL policy provider
  -> session owner admission
  -> turbo_flow_msg_t
  -> optional TurboFlow Policy / transforms
  -> explicit business data sink and/or MQTT fan-out
  -> settlement result
  -> session owner emits protocol ACK
```

必须保持以下不变量：

1. Session owner 是 MQTT 状态的唯一写 owner；Graph 不直接修改 session。
2. Auth/policy provider 属于安全控制路径，不是业务 data source。
3. FlowStore 只表达类型化事实存储，不替代协议 owner 或业务 Graph。
4. Graph admission、处理完成和 durable commit 是三个不同边界。
5. 未出现在 Graph 中的 adapter 不参与消息处理；Flowie 不猜测或自动添加存储节点。
6. `--check` 证明配置解析、provider 装配和 Graph 编译成功；某些 fail-fast provider（例如
   PostgreSQL record store）还会在创建时验证连接，但这仍不等于持续可用或端到端持久性测试。

## 6. 配置示例的阅读方式

仓库默认示例应按以下顺序阅读：

1. [flowie.yml](examples/flowie.yml)：查看允许的实例、backend 和资源上限；
2. [flowie.flow](examples/flowie.flow)：查看实际 source/sink 和处理顺序；
3. [服务端使用指南](SERVER_GUIDE.md)：查看启动、安全与 settlement 操作要求；
4. [消息与 Graph 边界 ADR](../turbo_flow/ADR_MESSAGE_GRAPH_BOUNDARY.md)：查看消息所有权契约。
