# Graph 与内容规则原语

## 决策背景

TurboFlow 已经分别拥有有界 Queue、条件 route、adapter retry、executor placement 和资源
snapshot。它们解决的是不同维度的问题，但局部实现若继续复制，会让停止唤醒、状态迁移和
目标选择产生不同错误语义。

本决策只约束 Graph 与内容规则。外部 CNet/CHTTP adapter 独立拥有连接、传输准入、HWM 与
协议状态；本仓库不提供共享 I/O policy 实现，也不增加同时管理队列、连接、路由和执行器的
万能 policy vtable。

## 选择

### Versioned rule program

`turbo_flow_rule_processor_t` 复用已编译的 TurboFlow expression，不引入第二套 DSL。processor
深拷贝 schema identity，编译 predicate，并深拷贝 pointer-free action template。

- program/action ABI 分别使用 `TURBO_FLOW_RULE_PROGRAM_ABI_V1` 和
  `TURBO_FLOW_RULE_ACTION_ABI_V1`；规则 action 仍然不是任意 callback。标准
  `rules.apply` 若使用 schema 字段，则通过显式的 `turbo_flow_rule_facts_provider_fn` /
  `facts_provider_ctx` 适配器提供 typed facts，不由 Policy processor 读取 opaque payload。
- `FIRST_MATCH` 只返回首条匹配 action；`ALL_MATCHES` 按声明顺序返回所有匹配 action。
  evaluation 只读同一个 immutable facts snapshot，不在规则之间修改 message。
- instruction、time、program memory、output action count 和 output bytes 均有显式 quota；
  schema identity、facts type 或 quota 不匹配时 fail fast。
- data program 只返回 mutate-private、route、drop、batch-key、retry-class、dead-letter。
  `turbo_flow_rule_register_data_stage()` 把纯 evaluator 注册成 inline stage，runtime 验证 action，
  并通过 message-owned typed decision sidecar 驱动 route/terminal completion。
- route、batch-key 和 retry-class 是单值决策；同一次 `ALL_MATCHES` 产生重复单值
  action 时返回 `SALTS_EPROTO`，message 和 decision 保持不变，不使用隐式的
  last-write-wins 规则。
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

### Graph 与 RulesForge 的判定边界

TurboFlow Graph 是流程和状态事实源：它拥有节点顺序、边选择、执行状态、retry/reject 和
completion。普通 `route ... when ...` 适合对当前上游输出的 TurboFlow message metadata
做一次 BOOL 判定；predicate 为 false 只过滤该 edge，不构成执行失败。`msg.type`、
`msg.flags` 和 `msg.status` 可直接读取，但 Graph 不猜测 opaque payload 的 schema，也不会
因为配置中出现 `when` 而自动调用 RulesForge。

RulesForge 是数据规则求值边界：领域字段先由 DataBind/schema materializer 形成 immutable
typed facts snapshot，再由显式注册的 `rules.apply` operation 或 data stage 求值。规则输出
是受 quota 约束的 typed decision sidecar，Graph 只消费其中的 route/drop/mutation 等决策并
继续流转。facts provider、schema identity、类型或 quota 错误沿 operation boundary 返回
失败；Observer、日志或 route predicate 不得吞掉这些错误。

因此，一个节点需要查看或修改任意业务数据时，应显式放置 RulesForge 节点；只需根据
TurboFlow 状态选择后继 edge 时，使用 Graph route。二者共享同一条 message 生命周期，但
不共享第二份业务状态。

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
if (rc == SALTS_OK) {
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

- rule evaluate：`n` 为 rule 数，最坏时间 `O(n)`；output 使用 caller-owned bounded array，
  graph stage 使用 64-entry 固定栈数组，不在消息热路径分配。

## 迁移、兼容与回滚

- 外部消息 adapter 的 peer 集合、wire protocol、HWM、drain 与 deadline 由其 CNet/CHTTP
  owner 保持，不进入 Graph Core；缺少所需 owner 时 fail fast，不回退到仓库内实现。
- Queue API 与 FIFO/full policy 不变。
- 旧 rule action callback/context API 已删除；rule 用户必须迁移为 typed action template。
  普通 graph stage callback 和 conditional route 不受影响。
- processor 是独立能力，不影响未注册它的 graph。

## 验证范围

- processor：FIRST/ALL、固定 facts、schema identity/type、五类 quota、data stage route/drop、
  unknown route、control authorization/stale generation。
- 外部 adapter：由 #5、#6、#7 对应实现覆盖 HWM、drain、selection 与 connection snapshot。
