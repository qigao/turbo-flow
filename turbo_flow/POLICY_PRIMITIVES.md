# I/O 与内容规则原语

## 决策背景

TurboFlow 已经分别拥有有界 Queue、FMQ HWM、条件 route、adapter retry、executor
placement 和资源 snapshot。它们解决的是不同维度的问题，但局部实现若继续复制，会让
message/byte 计数、停止唤醒和目标选择产生不同错误语义。

本决策影响 `turbo_flow`、`io/common`、FMQ 和 Queue，因此明确采用分层策略，而不增加一个
同时管理队列、连接、路由和执行器的万能 policy vtable。

## 选择

### 1. Admission/budget

`tf_io_budget_t` 是 `io/common` 内部原语，唯一拥有一组已接受工作的 message/byte 计数。
一次 acquire 在同一临界区同时检查并提交两个计数，避免 count 已增加但 byte reservation
失败后再回滚的可观察中间态。

- `TF_IO_ADMISSION_FAIL`：容量不足立即返回 `TURBO_ENOSPC`。
- `TF_IO_ADMISSION_BLOCK`：等待容量；owner close 后返回 `TURBO_ESHUTDOWN`；有限 deadline
  到期返回 `TURBO_ETIMEDOUT`。
- `close` 只关闭新准入并唤醒等待者，不伪造已接受工作的完成。
- `release` 由实际持有 request/frame 的 owner 调用；计数不足返回 `TURBO_ERANGE`。
- `drain` 只等待计数归零，不关闭 transport，也不释放 payload。

`drop_oldest` 不属于 budget。它必须由拥有消息容器与析构责任的 Queue/FMQ memory queue
实现，否则一个纯计数器无法安全选择和销毁被丢弃对象。

### 2. Ordering 与 selection

FIFO 是容器的出队顺序；Round Robin 是从一组候选目标中选择下一个目标。两者不是同类
策略，也不共享枚举。

- Queue 继续通过 `push_back/pop_front` 保证 FIFO，并拥有 requeue/drop 的消息生命周期。
- `tf_round_robin_t` 只产生 `[0, candidate_count)` 索引，不拥有 peer vector，不决定候选是否
  healthy，也不改变连接状态。
- 候选集合与 eligibility 仍由 FMQ session owner 管理。

### 3. Versioned rule program

`turbo_flow_rule_processor_t` 复用已编译的 TurboFlow expression，不引入第二套 DSL。processor
深拷贝 schema identity，编译 predicate，并深拷贝 pointer-free action template。

- program/action ABI 分别使用 `TURBO_FLOW_RULE_PROGRAM_ABI_V1` 和
  `TURBO_FLOW_RULE_ACTION_ABI_V1`；规则 action 仍然不是任意 callback。标准
  `rules.apply` 若使用 schema 字段，则通过显式的 `turbo_flow_rule_facts_provider_fn` /
  `facts_provider_ctx` 适配器提供 typed facts，不由 RulesForge 读取 opaque payload。
- `FIRST_MATCH` 只返回首条匹配 action；`ALL_MATCHES` 按声明顺序返回所有匹配 action。
  evaluation 只读同一个 immutable facts snapshot，不在规则之间修改 message。
- instruction、time、program memory、output action count 和 output bytes 均有显式 quota；
  schema identity、facts type 或 quota 不匹配时 fail fast。
- data program 只返回 mutate-private、route、drop、batch-key、retry-class、dead-letter。
  `turbo_flow_rule_register_data_stage()` 把纯 evaluator 注册成 inline stage，runtime 验证 action，
  并通过 message-owned typed decision sidecar 驱动 route/terminal completion。
- `turbo_flow_rule_register_data_operation()` 注册标准 `rules.apply` operation 和 `RuleSet`
  resource，使规则节点可以在满足 Message 类型契约的 DAG 位置复用；schema-backed operation
  要求 facts provider，provider 错误沿 operation boundary 传播。已有 message projection
  可以通过 `turbo_flow_rule_projection_facts_provider()` 与 host-owned materializer 接入，
  核心只传递 opaque projection 和 schema identity，不解释第三方 projection 内存。
- control program 只返回 pointer-free `turbo_flow_resource_command_t` proposal；host 必须调用
  `turbo_flow_rule_authorize_command()` 校验 command kind、target UID 和 observed generation，
  再自行调用 owner command dispatcher。规则求值本身不执行 command。
- processor 是 caller-owned immutable program；flow 必须在 processor 销毁前停止并销毁。
- strict YAML 将 RuleSet 建模为 channel resource，而不是 I/O adapter。profile 可通过
  `turbo_flow_resolved_config_profile_channel()` 引用它，host 再调用
  `turbo_flow_rule_processor_create_resolved()` 注入领域 schema/facts provider。配置必须包含
  `resource_uid`、`owner_name`、`mode` 和非空 `rules`；每条规则使用 `when`、`action` 及 action
  所需的 `key/value/mask/status`。支持 route、drop、batch_key、retry_class、dead_letter、
  mutate_type、mutate_flags、mutate_status；未知字段或不完整 action 在 graph compile 前失败。

YAML 示例：

```yaml
profiles:
  app:
    rule_set: mqtt.routing
channels:
  mqtt.routing:
    kind: rule_set
    config:
      resource_uid: rule-set:mqtt.routing
      owner_name: mqtt.routing
      mode: first_match
      max_output_actions: 16
      rules:
        - when: mqtt.topic == "application/output"
          action: route
          key: application_output
        - when: "true"
          action: route
          key: mqtt_fanout
```

示例：

```c
turbo_flow_rule_action_t priority = TURBO_FLOW_RULE_ACTION_INIT;
priority.kind = TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE;
priority.private_field = TURBO_FLOW_RULE_PRIVATE_MSG_FLAGS;
priority.value = 1u;
priority.mask = 1u;

turbo_flow_rule_t rules[] = {
    {"msg.type == 7 && msg.payload == \"urgent\"", 0, priority},
};
turbo_flow_rule_processor_config_t cfg = TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT;
cfg.resource_uid = "rule-set:classify";
cfg.owner_name = "classify";
cfg.rules = rules;
cfg.rule_count = 1;

turbo_flow_rule_processor_t *processor = NULL;
int rc = turbo_flow_rule_processor_create(&cfg, &processor, NULL);
if (rc == TURBO_OK) {
  rc = turbo_flow_rule_register_data_stage(flow, "classify", processor, NULL);
}
/* stop/destroy flow before destroying processor */
```

## 候选方案

1. 单一 `policy_ops` vtable：拒绝。它会混合容量、顺序、目标选择、内容动作和 lifecycle，
   ownership 与错误恢复无法保持单一事实源。
2. 所有行为都加入 Queue enum：拒绝。Queue full policy 无法表达 peer selection；Round Robin
   也无法表达 payload ownership。
3. 为内容规则新建 DSL：拒绝。现有 expression 已提供 typed message/schema field、编译期类型
   检查和无分配 evaluate。

## 性能与复杂度

- budget acquire/release：时间 `O(1)`、空间 `O(1)`；第一版用一个 mutex 保证复合计数正确性。
  在 profiling 证明竞争占总耗时超过 20% 前，不改成多原子补偿或无锁状态机。
- Round Robin：时间 `O(1)`、空间 `O(1)`。
- rule evaluate：`n` 为 rule 数，最坏时间 `O(n)`；output 使用 caller-owned bounded array，
  graph stage 使用 64-entry 固定栈数组，不在消息热路径分配。

## 迁移、兼容与回滚

- FMQ 的默认 HWM、linger 配置与 `TURBO_ENOSPC` 行为不变；内部计数与 drain 改由 budget
  提供，connection snapshot 从同一 budget 读取。新增显式 `block` admission 与 deadline；
  deadline 到期以 `TURBO_ETIMEDOUT` 发出 HWM event，stop 通过 close 唤醒等待者。
- FMQ PUSH 的 peer 集合与 wire protocol 不变，仅把局部 cursor 替换为通用 selector。
- Queue API 与 FIFO/full policy 不变。
- 旧 rule action callback/context API 已删除；rule 用户必须迁移为 typed action template。
  普通 graph stage callback 和 conditional route 不受影响。
- 若 FMQ 接入出现回归，可只回滚 FMQ 对 common primitive 的调用，不改变公开配置或 wire
  format；processor 也是独立新增文件，不影响未注册它的 graph。

## 验证范围

- common：message/byte 原子提交、BLOCK close interruption、Round Robin 序列。
- processor：FIRST/ALL、固定 facts、schema identity/type、五类 quota、data stage route/drop、
  unknown route、control authorization/stale generation。
- FMQ：现有 HWM、linger、PUSH/PULL Round Robin 与 connection snapshot 回归。
