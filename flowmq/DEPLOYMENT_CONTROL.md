# FlowMQ deployment control and rolling upgrade contract

状态：failure-domain membership owner、stable logical route election、route fencing、snapshot
split-brain classification、mixed-version compatibility evaluator，以及 durable management typed
reconcile 已实现。它们不改变 FMQ v2 frame、TFMP/1 wire 或 REQ/REP 同步状态机。

## 1. 决策背景

graph data plane 只应传输业务消息。registry、成员租约、broker 选择、滚动升级判断与 crash-window
reconcile 都会跨越进程或版本，若把这些状态放入 graph adapter，会产生多个事实源，并让 route、连接和
业务拓扑互相反向修改。

因此部署控制分成三类 owner：

| Owner | 主事实源 | 公开边界 |
| --- | --- | --- |
| Deployment controller | member lease、logical route primary、route generation | `turbo_flow_fmq_deployment_*` |
| Compatibility evaluator | 两个 release manifest 与可选 gateway manifest | `turbo_flow_fmq_compatibility_evaluate()` |
| TFMP operation owner | durable command、recovery-required、terminal result | `turbo_flow_tfmp_management_service_*` |

Discovery controller 仍只把已决议 snapshot 投影到固定 adapter slots；它不参与 membership 或 election。
Observe 只能导出上述 owner 的派生指标，不能写回 registry、route 或 operation。

## 2. Failure-domain controller

### 2.1 状态与身份

- `authority_id` 是管理域稳定名字。
- `authority_epoch` 由宿主的单一 authority/election 服务单调分配。controller 不自行猜测或回退 epoch。
- controller 创建时用 TurboUtils UUIDv7 生成 `authority_incarnation_id`；熵失败直接返回。
- member 使用稳定 `member_id` 加进程级 `incarnation_id`。旧 incarnation 不能 heartbeat、drain 或 leave
  新 incarnation。
- registry mutation 必须携带精确 `expected_registry_version`，因此一个 owner lane 上不存在 lost update。

宿主通过既有严格管理 REQ/REP 传递 pointer-free membership command 和 snapshot；本模块不新增 wire
kind，也不编码 socket 指针、CoroNet route 或本地 monotonic deadline。snapshot 只携带相对
`lease_remaining_ms`，该值不得跨 snapshot 比较。

### 2.2 Election 与 fencing

每个 member 绑定一个 failure domain 和 stable logical route。READY incumbent 在租约有效时保持 primary，
避免新成员上线造成无谓抢占。incumbent drain、leave 或 lease expiry 后：

1. 优先选择不同 failure domain 的 READY candidate；
2. 再按 priority 降序；
3. 同 priority 按 member ID 字典序，保证确定性。

primary、primary incarnation、endpoint 或可用性变化都会推进 `route_generation`。远端 peer 必须把
`authority_epoch + authority_incarnation_id + logical_route + route_generation + member incarnation`
作为 fencing token；旧 token 不能覆盖新 route。没有 candidate 时 route 仍保留并返回
`TURBO_ENOTCONN`，后续恢复继续推进同一个 generation。

同 authority epoch 的不同 authority incarnation，或同 registry version 的不同 normalized content，
由 snapshot comparator 判为 `SPLIT_BRAIN`。更低 epoch/version 判为 `STALE`。更高 epoch 是宿主已完成
fencing 后的新 authority；本地 controller 不替宿主实现分布式 consensus。

### 2.3 复杂度、线程与失败状态

成员与 route 各有显式上限（默认/最大 256）。控制面 apply/election 为 `O(m)`，snapshot 排序为
`O(m log m)`；这些路径不在数据热路径。所有调用由宿主串行化，不创建线程或 timer。

命令先校验 authority、version、capacity 与 generation 上限，再更新事实源。错误不推进 registry
version。lease tick 在移除成员前预检 route generation，避免部分过期。宿主可从 normalized snapshot
持久化/复制 registry；跨进程写 authority epoch 必须由外部强一致 election/fencing 服务负责。

## 3. Rolling upgrade

每个 release manifest 显式声明：

- FMQ wire min/max；当前 release 只能声明 v2；
- TFMP major 与 minor range；major 不兼容，minor 只能增加 optional field/kind/capability；
- shared management store 的 read/write minor range；
- YAML schema read range；
- rollout 必需 capability bits。

evaluator 只计算共同窗口，不改变 decoder：

- wire、TFMP、store、YAML 都有共同窗口时为 `DIRECT`；
- wire/TFMP 无共同窗口，但显式 dual-stack gateway 分别覆盖两端时为 `GATEWAY`；
- shared store 或 YAML 无共同版本时 gateway 也不能补救，必须 `STOP_THE_WORLD`；
- 任一 release 缺少发布所需 capability 时为 `INCOMPATIBLE`。

例如旧 release 只写 TFMS/1.0，新 release 可读 1.0/1.1 且可配置写 1.0，则 mixed rollout 固定写
1.0；若新 release 只能写 1.1，而旧 release 不能读 1.1，必须停机迁移。若引入新的 FMQ major，只有
真实 v2/v3 gateway 或停机升级两条路径；不得让当前 v2 decoder 接受 v1/v3。

## 4. Durable side-effect reconcile

operation store 与 Flow/resource mutation 不构成分布式事务。durable execution 的顺序为：

1. 原子提交 `ACCEPTED`；
2. claim 后原子提交 `RUNNING`；
3. 执行目标 mutation；
4. 原子提交 terminal operation 与可选 event outbox。

进程可能在 2 与 4 之间退出。恢复时 `RUNNING/CANCEL_REQUESTED` 保持 recovery-required，不自动改写为
失败，也不盲目重放。owner 在 recovery 清空前不能进入 READY。

每种可 durable command 必须有 typed inspector：

- FLOW_PAUSE/RESUME/DRAIN 从 runtime snapshot 核验目标 admission/drain 状态；
- POOL_RESIZE 从 pool UID、kind、parallelism 与 generation 核验；
- RESOURCE_QUIESCE/RESUME 和 ENDPOINT_REPLACE 必须由 resource owner 注入
  `turbo_flow_tfmp_reconcile_binding_t`，否则 command descriptor 不宣告 durable，该请求返回
  unsupported capability。

inspector 只返回三种结论：

- `APPLIED`：直接提交成功，不重复 mutation；
- `NOT_APPLIED`：只有原 expected generation 仍成立时，使用同 operation UUID 作为 idempotency key
  执行一次 goal-state command；
- `CONFLICT`：状态已前进到其他值，提交 terminal conflict，不覆盖新状态。

inspect/store 错误保留 recovery-required，调用方显式重试。若重放后再次在 terminal commit 前退出，
下一 owner 仍先 inspect；已生效结果会走 APPLIED，因此不会重复副作用。

## 5. 候选方案与取舍

| 方案 | 优点 | 风险 | 结论 |
| --- | --- | --- | --- |
| 把 membership 放进 graph stage | 配置项少 | data/control 事实源混合，split-brain 无边界 | 不选 |
| 每个 adapter 自行选 broker | 局部实现简单 | logical route 多主，无法统一 fencing | 不选 |
| controller 内实现分布式 consensus | 单包看似完整 | 重复造高风险共识/存储基础设施 | 不选；authority epoch 由宿主强一致服务提供 |
| 放宽 v2 decoder 做兼容 | 不需要 gateway | 非法 frame 被误接收，当前 wire 契约失真 | 禁止 |
| crash 后自动重放 RUNNING | 恢复快 | 非幂等副作用重复执行 | 禁止 |
| typed inspect + generation-checked goal state | 状态归属清楚，可重试 | 每类 resource 必须实现 inspector | 选择 |

## 6. 迁移与回滚

1. 先部署 compatibility manifest 检查，只报告结果，不改变流量。
2. 部署 controller 并让 discovery 继续消费旧 registry；对比 normalized snapshot。
3. authority 服务分配新 epoch 后切换 discovery source；异常时恢复旧 epoch 的只读 snapshot，不能让旧
   controller 继续接受 mutation。
4. durable management 先配置 inspector，再开放相应 command 的 durable capability。回滚到旧版本前，
   compatibility evaluator 必须确认 shared store writer 仍使用旧版本可读 minor。

验证至少覆盖：跨 failure-domain failover、旧 member incarnation、lease expiry、同 version split-brain、
v2/v3 gateway/stop 路径、shared store writer gap、mutation 已生效 crash window、无 inspector 的 durable
拒绝，以及 inspector 恢复不重复 command。

## 7. Release gate

本协议的 fencing、rolling manifest 和 durable reconcile 测试属于统一 `fmq-release`/`fmq-chaos`
测试集。准确命令、真实 Redis 前置条件和性能趋势规则只在
[RELEASE_GATE.md](RELEASE_GATE.md) 维护，不能用 Disabled live suite 代替发布验证。
