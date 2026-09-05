#include "turbo_flow_policy.h"

#include "tinytest.h"
#include "salts_error.h"
#include "salts_str.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static turbo_flow_rule_action_t data_action(turbo_flow_rule_action_kind_t kind, const char *key) {
  turbo_flow_rule_action_t action = TURBO_FLOW_RULE_ACTION_INIT;
  action.kind = kind;
  if (key) memcpy(action.key, key, strlen(key) + 1u);
  return action;
}

static turbo_flow_rule_action_t mutate_flags(uint32_t value, uint32_t mask) {
  turbo_flow_rule_action_t action = data_action(TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE, NULL);
  action.private_field = TURBO_FLOW_RULE_PRIVATE_MSG_FLAGS;
  action.value = value;
  action.mask = mask;
  return action;
}

static turbo_flow_rule_action_t command_proposal(const char *uid, uint64_t generation) {
  turbo_flow_rule_action_t action = TURBO_FLOW_RULE_ACTION_INIT;
  action.kind = TURBO_FLOW_RULE_ACTION_PROPOSE_COMMAND;
  action.command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
  action.command.expected_generation = generation;
  action.command.parallelism = 4u;
  memcpy(action.command.target_uid, uid, strlen(uid) + 1u);
  memcpy(action.command.idempotency_key, "rule-resize-1", sizeof("rule-resize-1"));
  return action;
}

static turbo_flow_rule_processor_config_t data_config(const turbo_flow_rule_t *rules,
                                                      size_t rule_count) {
  turbo_flow_rule_processor_config_t config = TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT;
  config.resource_uid = "rule-set:test";
  config.owner_name = "rules";
  config.rules = rules;
  config.rule_count = rule_count;
  return config;
}

static int count_stage(turbo_flow_msg_t *message, void *ctx) {
  int *count = (int *)ctx;
  (void)message;
  *count += 1;
  return SALTS_OK;
}

typedef struct rule_operation_probe_s {
  int count;
  uint32_t flags;
} rule_operation_probe_t;

static int observe_rule_operation(turbo_flow_msg_t *message, void *ctx) {
  rule_operation_probe_t *probe = (rule_operation_probe_t *)ctx;
  probe->count += 1;
  probe->flags = message->flags;
  return SALTS_OK;
}

typedef struct rule_facts_provider_probe_s {
  int calls;
  turbo_flow_expr_value_t values[1];
} rule_facts_provider_probe_t;

static const turbo_flow_expr_schema_field_t RULE_PROVIDER_FIELDS[] = {
    {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 1u}};
static const turbo_flow_expr_schema_t RULE_PROVIDER_SCHEMA = {RULE_PROVIDER_FIELDS, 1u};

static int provide_rule_facts(const turbo_flow_msg_t *message,
                              const turbo_flow_expr_schema_t *schema,
                              const turbo_flow_expr_value_t **values_out, size_t *value_count_out,
                              void *ctx) {
  rule_facts_provider_probe_t *probe = (rule_facts_provider_probe_t *)ctx;
  if (!message || !schema || !schema->fields || schema->field_count != 1u ||
      strcmp(schema->fields[0].path, "group.level") != 0 ||
      schema->fields[0].type != TURBO_FLOW_EXPR_TYPE_I64 || !values_out || !value_count_out ||
      !probe) {
    return SALTS_EPROTO;
  }
  probe->calls += 1;
  probe->values[0].type = TURBO_FLOW_EXPR_TYPE_I64;
  probe->values[0].as.i64 = (int64_t)message->flags;
  *values_out = probe->values;
  *value_count_out = 1u;
  return SALTS_OK;
}

static int provide_invalid_rule_facts(const turbo_flow_msg_t *message,
                                      const turbo_flow_expr_schema_t *schema,
                                      const turbo_flow_expr_value_t **values_out,
                                      size_t *value_count_out, void *ctx) {
  (void)message;
  (void)schema;
  (void)values_out;
  (void)value_count_out;
  (void)ctx;
  return SALTS_EPROTO;
}

typedef struct rule_projection_value_s {
  int64_t group_level;
} rule_projection_value_t;

typedef struct rule_projection_materializer_probe_s {
  int calls;
  turbo_flow_expr_value_t values[1];
} rule_projection_materializer_probe_t;

static const turbo_flow_data_schema_t RULE_PROJECTION_SCHEMA = {sizeof(turbo_flow_data_schema_t),
                                                                TURBO_FLOW_DOMAIN_DATA,
                                                                TURBO_FLOW_DATA_ENCODING_JSON,
                                                                "rules.message",
                                                                "RuleMessage",
                                                                "rules.message.v1",
                                                                31u,
                                                                1u,
                                                                NULL};

static void destroy_rule_projection(void *ptr, void *ctx) {
  (void)ctx;
  free(ptr);
}

static int clone_rule_projection(const void *value, void *ctx, void **out) {
  rule_projection_value_t *copy;
  (void)ctx;
  if (!value || !out) return SALTS_EINVAL;
  copy = (rule_projection_value_t *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const rule_projection_value_t *)value;
  *out = copy;
  return SALTS_OK;
}

static int materialize_rule_projection(const void *projection,
                                       const turbo_flow_data_schema_t *projection_schema,
                                       const turbo_flow_expr_schema_t *rule_schema,
                                       const turbo_flow_expr_value_t **values_out,
                                       size_t *value_count_out, void *ctx) {
  const rule_projection_value_t *value = (const rule_projection_value_t *)projection;
  rule_projection_materializer_probe_t *probe = (rule_projection_materializer_probe_t *)ctx;
  if (!value || !projection_schema || !rule_schema || !rule_schema->fields ||
      rule_schema->field_count != 1u ||
      strcmp(projection_schema->schema_name, "rules.message") != 0 ||
      strcmp(projection_schema->type_name, "RuleMessage") != 0 ||
      strcmp(rule_schema->fields[0].path, "group.level") != 0 ||
      rule_schema->fields[0].type != TURBO_FLOW_EXPR_TYPE_I64 || !values_out || !value_count_out ||
      !probe) {
    return SALTS_EPROTO;
  }
  probe->calls += 1;
  probe->values[0].type = TURBO_FLOW_EXPR_TYPE_I64;
  probe->values[0].as.i64 = value->group_level;
  *values_out = probe->values;
  *value_count_out = 1u;
  return SALTS_OK;
}

spec("versioned rule program") {
  it("evaluates an absent optional schema fact as null") {
    const turbo_flow_expr_schema_field_t field = {"optional.value", TURBO_FLOW_EXPR_TYPE_STRING,
                                                  77u};
    const turbo_flow_expr_schema_t schema = {&field, 1u};
    const turbo_flow_rule_t rule = {"optional.value == null", 0u,
                                    data_action(TURBO_FLOW_RULE_ACTION_DROP, NULL)};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t action;
    turbo_flow_rule_result_t result = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_rule_facts_t facts = TURBO_FLOW_RULE_FACTS_INIT;
    turbo_flow_expr_value_t value = {TURBO_FLOW_EXPR_TYPE_NULL};
    turbo_flow_msg_t message;

    config.schema = &schema;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    turbo_flow_msg_init(&message);
    facts.message = &message;
    facts.schema = &schema;
    facts.values = &value;
    facts.value_count = 1u;
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, &action, 1u, &result),
                 SALTS_OK);
    check_equal(result.emitted, 1u);
    check_equal(action.kind, TURBO_FLOW_RULE_ACTION_DROP);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("rejects a rule program without stable resource identity") {
    turbo_flow_rule_processor_t *processor = NULL;
    const turbo_flow_rule_t rule = {"true", 0u, data_action(TURBO_FLOW_RULE_ACTION_DROP, NULL)};
    turbo_flow_rule_processor_config_t config = TURBO_FLOW_RULE_PROCESSOR_CONFIG_INIT;

    config.rules = &rule;
    config.rule_count = 1u;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_EINVAL);
    config.resource_uid = "rule-set:test";
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_EINVAL);
    check_null(processor);
  }

  it("creates a strict bounded RuleSet from a resolved YAML channel") {
    static const char yaml[] = "version: 1\n"
                               "profiles:\n"
                               "  app:\n"
                               "    rules: routing\n"
                               "channels:\n"
                               "  routing:\n"
                               "    kind: rule_set\n"
                               "    config:\n"
                               "      resource_uid: rule-set:routing\n"
                               "      owner_name: rules.routing\n"
                               "      mode: all_matches\n"
                               "      max_output_actions: 4\n"
                               "      rules:\n"
                               "        - when: group.level > 1\n"
                               "          action: route\n"
                               "          key: elevated\n"
                               "        - when: group.level < 0\n"
                               "          action: drop\n"
                               "adapters: {}\n";
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t actions[2];
    turbo_flow_rule_result_t result = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_rule_facts_t facts = TURBO_FLOW_RULE_FACTS_INIT;
    turbo_flow_expr_value_t values[1];
    turbo_flow_msg_t message;
    rule_facts_provider_probe_t probe = {0};

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &config_error),
                 SALTS_OK);
    check_equal(turbo_flow_rule_processor_create_resolved(
                     resolved, "routing", &RULE_PROVIDER_SCHEMA, provide_rule_facts, &probe,
                     &processor, &config_error),
                 SALTS_OK);
    turbo_flow_msg_init(&message);
    values[0].type = TURBO_FLOW_EXPR_TYPE_I64;
    values[0].as.i64 = 3;
    facts.message = &message;
    facts.schema = &RULE_PROVIDER_SCHEMA;
    facts.values = values;
    facts.value_count = 1u;
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, actions, 2u, &result),
                 SALTS_OK);
    check_equal(result.emitted, 1u);
    check_equal(actions[0].kind, TURBO_FLOW_RULE_ACTION_ROUTE);
    check_equal(actions[0].key, "elevated");
    turbo_flow_rule_processor_destroy(processor);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("rejects unknown RuleSet fields and incomplete actions at the channel boundary") {
    static const char unknown[] =
        "version: 1\nchannels:\n  routing:\n    kind: rule_set\n    config:\n"
        "      resource_uid: rule-set:routing\n      owner_name: rules.routing\n"
        "      mode: first_match\n      surprise: true\n"
        "      rules:\n        - when: \"true\"\n          action: drop\nadapters: {}\n";
    static const char incomplete[] =
        "version: 1\nchannels:\n  routing:\n    kind: rule_set\n    config:\n"
        "      resource_uid: rule-set:routing\n      owner_name: rules.routing\n"
        "      mode: first_match\n"
        "      rules:\n        - when: \"true\"\n          action: route\nadapters: {}\n";
    const char *documents[] = {unknown, incomplete};
    const size_t lengths[] = {sizeof(unknown) - 1u, sizeof(incomplete) - 1u};
    for (size_t i = 0u; i < sizeof(documents) / sizeof(documents[0]); ++i) {
      turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_resolved_config_t *resolved = NULL;
      turbo_flow_rule_processor_t *processor = NULL;
      check_equal(
          turbo_flow_config_resolve_yaml(documents[i], lengths[i], &resolved, &config_error),
          SALTS_OK);
      check_equal(turbo_flow_rule_processor_create_resolved(resolved, "routing", NULL, NULL, NULL,
                                                             &processor, &config_error),
                   SALTS_EINVAL);
      check_null(processor);
      check_contains(config_error.path, "$.channels.routing.config");
      turbo_flow_resolved_config_destroy(resolved);
    }
  }

  it("emits deterministic typed actions from one immutable facts snapshot") {
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t actions[2];
    turbo_flow_rule_result_t first = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_rule_result_t second = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_msg_t message;
    const turbo_flow_rule_t rules[] = {
        {"msg.type == 7", 0u, mutate_flags(1u, 1u)},
        {"msg.flags == 0", 0u, data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "cold")}};
    turbo_flow_rule_processor_config_t config = data_config(rules, 2u);
    turbo_flow_rule_facts_t facts = TURBO_FLOW_RULE_FACTS_INIT;

    config.mode = TURBO_FLOW_RULE_ALL_MATCHES;
    turbo_flow_msg_init(&message);
    message.type = 7u;
    facts.message = &message;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_not_null(processor);
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, actions, 2u, &first),
                 SALTS_OK);
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, actions, 2u, &second),
                 SALTS_OK);
    check_equal(first.evaluated, 2u);
    check_equal(first.matched, 2u);
    check_equal(first.emitted, 2u);
    check_equal(second.evaluated, first.evaluated);
    check_equal(second.matched, first.matched);
    check_equal(second.emitted, first.emitted);
    check_equal(actions[0].kind, TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE);
    check_equal(actions[1].kind, TURBO_FLOW_RULE_ACTION_ROUTE);
    check_equal(actions[1].key, "cold");
    check_equal(message.flags, 0u);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("applies validated data actions atomically to the private message copy") {
    turbo_flow_msg_t message;
    turbo_flow_rule_data_decision_t decision = TURBO_FLOW_RULE_DATA_DECISION_INIT;
    turbo_flow_rule_action_t actions[] = {
        mutate_flags(5u, 7u), data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "priority"),
        data_action(TURBO_FLOW_RULE_ACTION_BATCH_KEY, "group-7"),
        data_action(TURBO_FLOW_RULE_ACTION_RETRY_CLASS, "transient")};
    turbo_flow_rule_action_t status_action =
        data_action(TURBO_FLOW_RULE_ACTION_MUTATE_PRIVATE, NULL);
    turbo_flow_rule_action_t conflict[] = {data_action(TURBO_FLOW_RULE_ACTION_DROP, NULL),
                                           data_action(TURBO_FLOW_RULE_ACTION_DEAD_LETTER, NULL)};

    conflict[1].status = SALTS_EIO;
    status_action.private_field = TURBO_FLOW_RULE_PRIVATE_MSG_STATUS;
    status_action.status = SALTS_EIO;
    turbo_flow_msg_init(&message);
    message.flags = 8u;
    check_equal(turbo_flow_rule_apply_data_actions(&message, actions, 4u, &decision), SALTS_OK);
    check_equal(message.flags, 13u);
    check_equal(decision.route, "priority");
    check_equal(decision.batch_key, "group-7");
    check_equal(decision.retry_class, "transient");
    check_equal(turbo_flow_rule_apply_data_actions(&message, &status_action, 1u, &decision),
                 SALTS_OK);
    check_equal(message.status, SALTS_EIO);
    check_equal(turbo_flow_rule_apply_data_actions(&message, conflict, 2u, &decision),
                 SALTS_EPROTO);
    check_equal(message.flags, 13u);
  }

  it("rejects conflicting single-valued data decisions atomically") {
    turbo_flow_msg_t message;
    turbo_flow_rule_data_decision_t decision = TURBO_FLOW_RULE_DATA_DECISION_INIT;
    const turbo_flow_rule_action_t route_conflict[] = {
        data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "primary"),
        data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "secondary")};
    const turbo_flow_rule_action_t batch_conflict[] = {
        data_action(TURBO_FLOW_RULE_ACTION_BATCH_KEY, "first"),
        data_action(TURBO_FLOW_RULE_ACTION_BATCH_KEY, "second")};
    const turbo_flow_rule_action_t retry_conflict[] = {
        data_action(TURBO_FLOW_RULE_ACTION_RETRY_CLASS, "fast"),
        data_action(TURBO_FLOW_RULE_ACTION_RETRY_CLASS, "slow")};

    turbo_flow_msg_init(&message);
    message.flags = 17u;
    memcpy(decision.route, "stable", sizeof("stable"));
    check_equal(turbo_flow_rule_apply_data_actions(&message, route_conflict, 2u, &decision),
                SALTS_EPROTO);
    check_equal(turbo_flow_rule_apply_data_actions(&message, batch_conflict, 2u, &decision),
                SALTS_EPROTO);
    check_equal(turbo_flow_rule_apply_data_actions(&message, retry_conflict, 2u, &decision),
                SALTS_EPROTO);
    check_equal(message.flags, 17u);
    check_equal(decision.route, "stable");
  }

  it("routes graph fan-out through the typed data-rule stage") {
    static const char source[] = "source input\n"
                                 "stage rules\n"
                                 "stage selected\n"
                                 "stage skipped\n"
                                 "stage main {\n"
                                 "  input -> rules -> [selected, skipped]\n"
                                 "}\n";
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    int selected = 0;
    int skipped = 0;
    const turbo_flow_rule_t rule = {"msg.type == 9", 0u,
                                    data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "selected")};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_stage(flow, "rules", processor, NULL), SALTS_OK);
    check_equal(turbo_flow_resource_metadata_count(flow), 1u);
    check_equal(turbo_flow_register_stage_ex(flow, "selected", count_stage, &selected, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "skipped", count_stage, &skipped, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    message.type = 9u;
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(selected, 1);
    check_equal(skipped, 0);
    check_equal(message.data_decision.route, "");
    {
      turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
      turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
      tstr json;
      check_equal(turbo_flow_resource_metadata_at(flow, 0u, &metadata), SALTS_OK);
      check_equal(metadata.domain, TURBO_FLOW_DOMAIN_RULES);
      check_equal(metadata.kind, TURBO_FLOW_RESOURCE_RULE_SET);
      check_equal(metadata.uid, config.resource_uid);
      check_equal(metadata.owner_name, config.owner_name);
      check_equal(
          turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
          SALTS_OK);
      check_equal(document.schema->type_name, "RuleSetStatus");
      json =
          tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
      check_not_null(json);
      check_not_null(strstr(json, "\"evaluations\":\"1\""));
      check_null(strstr(json, "msg.type"));
      check_null(strstr(json, "selected"));
      tstr_free(json);
      turbo_flow_resource_document_cleanup(&document);
    }
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("binds rules.apply to a resource primitive from any graph node") {
    static const char source[] = "source input\n"
                                 "stage apply operation rules.apply resource rules.test\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> apply -> sink\n"
                                 "}\n";
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    rule_operation_probe_t probe = {0};
    const turbo_flow_stage_plan_t *stage;
    const turbo_flow_operation_descriptor_t *operation;
    const turbo_flow_rule_t rule = {"msg.type == 7", 0u, mutate_flags(4u, 4u)};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_operation(flow, "rules.test", processor), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", observe_rule_operation, &probe, NULL),
                 SALTS_OK);
    check_not_null(turbo_flow_find_primitive(flow, "rules.test"));
    check_not_null(turbo_flow_find_operation(flow, TURBO_FLOW_RULE_APPLY_OPERATION));
    check_not_null(turbo_flow_find_module(flow, TURBO_FLOW_RULE_MODULE));
    check_equal(
        turbo_flow_operation_provider_module(flow, TURBO_FLOW_RULE_APPLY_OPERATION, "rules.test"),
        TURBO_FLOW_RULE_MODULE);
    check_equal(turbo_flow_compile(flow), SALTS_OK);

    stage = turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "apply"));
    operation = turbo_flow_stage_operation_at(flow, (size_t)turbo_flow_find_stage(flow, "apply"));
    check_not_null(stage);
    check_not_null(operation);
    check_equal(stage->operation_name, TURBO_FLOW_RULE_APPLY_OPERATION);
    check_equal(stage->resource_name, "rules.test");
    check_equal(operation->resource_type, TURBO_FLOW_RULE_SET_TYPE);
    check_equal(operation->domain, TURBO_FLOW_DOMAIN_RULES);
    check_true((operation->flags & TURBO_FLOW_OPERATION_BRIDGE) != 0u);
    check_equal(stage->mutability, TURBO_FLOW_STAGE_MUTATES_PRIVATE);
    check_true((stage->effects & TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION) != 0u);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    message.type = 7u;
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.count, 1);
    check_equal(probe.flags, 4u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("materializes typed schema facts before rules.apply") {
    static const char source[] = "source input\n"
                                 "stage apply operation rules.apply resource rules.test\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> apply -> sink\n"
                                 "}\n";
    const turbo_flow_expr_schema_field_t field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    const turbo_flow_expr_schema_t schema = {&field, 1u};
    const turbo_flow_rule_t rule = {"group.level > 3", 0u, mutate_flags(8u, 8u)};
    rule_facts_provider_probe_t facts_probe = {0};
    rule_operation_probe_t operation_probe = {0};
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;

    config.schema = &schema;
    config.facts_provider = provide_rule_facts;
    config.facts_provider_ctx = &facts_probe;
    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_operation(flow, "rules.test", processor), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", observe_rule_operation, &operation_probe, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    message.flags = 7u;
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(facts_probe.calls, 1);
    check_equal(operation_probe.count, 1);
    check_equal(operation_probe.flags, 15u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("materializes rules.apply facts from an attached typed projection") {
    static const char source[] = "source input\n"
                                 "stage apply operation rules.apply resource rules.test\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> apply -> sink\n"
                                 "}\n";
    const turbo_flow_expr_schema_field_t field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    const turbo_flow_expr_schema_t schema = {&field, 1u};
    const turbo_flow_rule_t rule = {"group.level > 3", 0u, mutate_flags(8u, 8u)};
    turbo_flow_rule_projection_provider_t projection_provider =
        TURBO_FLOW_RULE_PROJECTION_PROVIDER_INIT;
    rule_projection_materializer_probe_t materializer_probe = {0};
    rule_operation_probe_t operation_probe = {0};
    rule_projection_value_t *projection = (rule_projection_value_t *)malloc(sizeof(*projection));
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;

    check_not_null(projection);
    projection->group_level = 7;
    projection_provider.materialize = materialize_rule_projection;
    projection_provider.ctx = &materializer_probe;
    config.schema = &schema;
    config.facts_provider = turbo_flow_rule_projection_facts_provider;
    config.facts_provider_ctx = &projection_provider;
    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_operation(flow, "rules.test", processor), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", observe_rule_operation, &operation_probe, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_msg_bind_projection(&message, &RULE_PROJECTION_SCHEMA, projection,
                                                clone_rule_projection, destroy_rule_projection,
                                                NULL),
                 SALTS_OK);
    projection = NULL;
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(materializer_probe.calls, 1);
    check_equal(operation_probe.count, 1);
    check_equal(operation_probe.flags, 8u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
    free(projection);
  }

  it("rejects opaque messages at the projection facts boundary") {
    const turbo_flow_expr_schema_field_t field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    const turbo_flow_expr_schema_t schema = {&field, 1u};
    turbo_flow_rule_projection_provider_t projection_provider =
        TURBO_FLOW_RULE_PROJECTION_PROVIDER_INIT;
    rule_projection_materializer_probe_t materializer_probe = {0};
    const turbo_flow_expr_value_t *values = NULL;
    size_t value_count = 0u;
    turbo_flow_msg_t message;

    projection_provider.materialize = materialize_rule_projection;
    projection_provider.ctx = &materializer_probe;
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_rule_projection_facts_provider(&message, &schema, &values, &value_count,
                                                           &projection_provider),
                 SALTS_ENOENT);
    check_null(values);
    check_equal(value_count, 0u);
    check_equal(materializer_probe.calls, 0);
    turbo_flow_msg_cleanup(&message);
  }

  it("rejects schema-backed rules.apply without a facts provider") {
    const turbo_flow_expr_schema_field_t field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    const turbo_flow_expr_schema_t schema = {&field, 1u};
    const turbo_flow_rule_t rule = {"group.level > 3", 0u, mutate_flags(8u, 8u)};
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);
    turbo_flow_t *flow = turbo_flow_create();

    config.schema = &schema;
    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_operation(flow, "rules.test", processor),
                 SALTS_EINVAL);
    check_equal(turbo_flow_primitive_count(flow), 0u);
    check_equal(turbo_flow_operation_count(flow), 0u);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("propagates facts provider errors from rules.apply") {
    static const char source[] = "source input\n"
                                 "stage apply operation rules.apply resource rules.test\n"
                                 "stage main {\n"
                                 "  input -> apply\n"
                                 "}\n";
    const turbo_flow_expr_schema_field_t field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    const turbo_flow_expr_schema_t schema = {&field, 1u};
    const turbo_flow_rule_t rule = {"group.level > 3", 0u, mutate_flags(8u, 8u)};
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;

    config.schema = &schema;
    config.facts_provider = provide_invalid_rule_facts;
    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_operation(flow, "rules.test", processor), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    message.flags = 7u;
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_EPROTO);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("stops normal downstream release when a data rule drops the message") {
    static const char source[] = "source input\n"
                                 "stage rules\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rules -> sink\n"
                                 "}\n";
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    int sink = 0;
    const turbo_flow_rule_t rule = {"true", 0u, data_action(TURBO_FLOW_RULE_ACTION_DROP, NULL)};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_stage(flow, "rules", processor, NULL), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", count_stage, &sink, NULL), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(sink, 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("fails fast when a data rule selects an unknown downstream route") {
    static const char source[] = "source input\n"
                                 "stage rules\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rules -> sink\n"
                                 "}\n";
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    int sink = 0;
    const turbo_flow_rule_t rule = {"true", 0u,
                                    data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "missing")};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_stage(flow, "rules", processor, NULL), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "sink", count_stage, &sink, NULL), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_EPROTO);
    check_equal(sink, 0);
    check_contains(turbo_flow_last_error(flow)->message, "unknown downstream route");
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("rejects pooled execution for an inline data-rule evaluator") {
    static const char source[] = "source input\n"
                                 "stage rules exec thread workers 1\n"
                                 "stage main {\n"
                                 "  input -> rules\n"
                                 "}\n";
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_rule_t rule = {"true", 0u, data_action(TURBO_FLOW_RULE_ACTION_DROP, NULL)};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    check_not_null(flow);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, source, strlen(source)), SALTS_OK);
    check_equal(turbo_flow_rule_register_data_stage(flow, "rules", processor, NULL), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_EINVAL);
    check_contains(turbo_flow_last_error(flow)->message, "inline executor");
    turbo_flow_destroy(flow);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("rejects unknown fields and fact type mismatches") {
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_error_t error;
    turbo_flow_rule_action_t action;
    turbo_flow_rule_result_t result = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_expr_schema_field_t field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    turbo_flow_expr_schema_t schema = {&field, 1u};
    turbo_flow_expr_value_t value;
    turbo_flow_rule_facts_t facts = {sizeof(facts), NULL, &schema, &value, 1u};
    const turbo_flow_rule_t unknown = {"group.unknown == 1", 0u,
                                       data_action(TURBO_FLOW_RULE_ACTION_DROP, NULL)};
    const turbo_flow_rule_t typed = {"group.level > 3", 0u,
                                     data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "high")};
    turbo_flow_rule_processor_config_t config = data_config(&unknown, 1u);

    memset(&error, 0, sizeof(error));
    config.schema = &schema;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, &error), SALTS_ENOENT);
    check_null(processor);
    config.rules = &typed;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, &error), SALTS_OK);
    memset(&value, 0, sizeof(value));
    value.type = TURBO_FLOW_EXPR_TYPE_STRING;
    value.as.string = vstr_from_cstr("wrong");
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, &action, 1u, &result),
                 SALTS_EPROTO);
    check_equal(result.evaluated, 1u);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("requires evaluation facts to match the compiled schema identity") {
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t action;
    turbo_flow_expr_schema_field_t compiled_field = {"group.level", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    turbo_flow_expr_schema_field_t wrong_field = {"group.rank", TURBO_FLOW_EXPR_TYPE_I64, 9u};
    turbo_flow_expr_schema_t compiled_schema = {&compiled_field, 1u};
    turbo_flow_expr_schema_t wrong_schema = {&wrong_field, 1u};
    turbo_flow_expr_value_t value = {TURBO_FLOW_EXPR_TYPE_I64, {.i64 = 7}};
    turbo_flow_rule_facts_t facts = {sizeof(facts), NULL, &wrong_schema, &value, 1u};
    const turbo_flow_rule_t rule = {"group.level > 3", 0u,
                                    data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "high")};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    config.schema = &compiled_schema;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, &action, 1u, NULL),
                 SALTS_EPROTO);
    facts.schema = &compiled_schema;
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, &action, 1u, NULL),
                 SALTS_OK);
    check_equal(action.key, "high");
    turbo_flow_rule_processor_destroy(processor);
  }

  it("enforces instruction memory time and output quotas") {
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t actions[2];
    turbo_flow_rule_result_t result = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_rule_facts_t facts = TURBO_FLOW_RULE_FACTS_INIT;
    const turbo_flow_rule_t rules[] = {
        {"true", 0u, data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "one")},
        {"true", 0u, data_action(TURBO_FLOW_RULE_ACTION_ROUTE, "two")}};
    turbo_flow_rule_processor_config_t config = data_config(rules, 2u);

    config.limits.max_instructions = 1u;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_ENOSPC);
    config.limits = (turbo_flow_rule_limits_t)TURBO_FLOW_RULE_LIMITS_DEFAULT;
    config.limits.max_memory_bytes = sizeof(turbo_flow_rule_processor_config_t);
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_ENOSPC);
    config.limits = (turbo_flow_rule_limits_t)TURBO_FLOW_RULE_LIMITS_DEFAULT;
    config.limits.max_output_actions = 1u;
    config.mode = TURBO_FLOW_RULE_ALL_MATCHES;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, actions, 2u, &result),
                 SALTS_ENOSPC);
    check_equal(result.emitted, 1u);
    turbo_flow_rule_processor_destroy(processor);
    processor = NULL;
    config.limits = (turbo_flow_rule_limits_t)TURBO_FLOW_RULE_LIMITS_DEFAULT;
    config.limits.max_time_ns = 1u;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, actions, 2u, &result),
                 SALTS_ETIMEDOUT);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("returns control proposals for host authorization without dispatching them") {
    static const char uid[] = "pool:parse:thread";
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t proposal;
    turbo_flow_rule_result_t result = TURBO_FLOW_RULE_RESULT_INIT;
    turbo_flow_rule_facts_t facts = TURBO_FLOW_RULE_FACTS_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_rule_authority_t authority = TURBO_FLOW_RULE_AUTHORITY_INIT;
    const turbo_flow_rule_t rule = {"true", 0u, command_proposal(uid, 12u)};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    config.domain = TURBO_FLOW_RULE_CONTROL;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_OK);
    check_equal(turbo_flow_rule_processor_evaluate(processor, &facts, &proposal, 1u, &result),
                 SALTS_OK);
    authority.allowed_command_mask = UINT32_C(1) << TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
    authority.observed_generation = 11u;
    memcpy(authority.target_uid, uid, sizeof(uid));
    check_equal(turbo_flow_rule_authorize_command(&proposal, &authority, &command), SALTS_EBUSY);
    authority.observed_generation = 12u;
    authority.allowed_command_mask = 0u;
    check_equal(turbo_flow_rule_authorize_command(&proposal, &authority, &command), SALTS_EPERM);
    authority.allowed_command_mask = UINT32_C(1) << TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
    check_equal(turbo_flow_rule_authorize_command(&proposal, &authority, &command), SALTS_OK);
    check_equal(command.kind, TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL);
    check_equal(command.target_uid, uid);
    check_equal(command.expected_generation, 12u);
    turbo_flow_rule_processor_destroy(processor);
  }

  it("rejects cross-domain and unversioned actions before program creation") {
    turbo_flow_rule_processor_t *processor = NULL;
    turbo_flow_rule_action_t proposal = command_proposal("pool:parse:thread", 1u);
    turbo_flow_rule_t rule = {"true", 0u, proposal};
    turbo_flow_rule_processor_config_t config = data_config(&rule, 1u);

    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_EINVAL);
    config.domain = TURBO_FLOW_RULE_CONTROL;
    rule.action.abi_version = 0u;
    check_equal(turbo_flow_rule_processor_create(&config, &processor, NULL), SALTS_EINVAL);
    check_null(processor);
  }
}
