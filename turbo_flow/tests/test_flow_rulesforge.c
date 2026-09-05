#include "turbo_flow_rulesforge.h"
#include "turbo_flow_expr.h"

#include "tinytest.h"

#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

enum {
  RULESFORGE_MATCHED_FLAG = 1u << 1,
  RULESFORGE_OTHER_FLAG = 1u << 4,
  RULESFORGE_AGE_FIELD = 1u
};

typedef struct rulesforge_mock_context_s {
  int callback_count;
  int matched;
} rulesforge_mock_context_t;

typedef struct rulesforge_sink_probe_s {
  int count;
  uint32_t flags;
  turbo_flow_data_evaluation_status_t rule_status;
  uint32_t rule_match_count;
  int rule_error;
} rulesforge_sink_probe_t;

typedef struct rulesforge_projection_s {
  int64_t age;
} rulesforge_projection_t;

typedef struct rulesforge_projection_lifecycle_s {
  atomic_int clones;
  atomic_int destroys;
} rulesforge_projection_lifecycle_t;

typedef struct rulesforge_worker_probe_s {
  atomic_int count;
  atomic_llong age;
} rulesforge_worker_probe_t;

typedef struct rulesforge_dispatch_probe_s {
  atomic_int count;
} rulesforge_dispatch_probe_t;

static const turbo_flow_data_schema_t RULESFORGE_PROJECTION_SCHEMA = {
    sizeof(turbo_flow_data_schema_t),
    TURBO_FLOW_DOMAIN_DATA,
    TURBO_FLOW_DATA_ENCODING_TBE,
    "rulesforge.test",
    "Applicant",
    "RulesForge.TestProjection",
    1u,
    1u,
    NULL};

static const turbo_flow_data_schema_t RULESFORGE_DATABIND_SCHEMA = {
    sizeof(turbo_flow_data_schema_t),
    TURBO_FLOW_DOMAIN_DATA,
    TURBO_FLOW_DATA_ENCODING_TBE,
    "rulesforge.provider.test",
    "Applicant",
    TURBO_FLOW_RULEFORGE_DATABIND_PROJECTION_TYPE,
    2u,
    1u,
    NULL};

static const turbo_flow_expr_schema_field_t RULESFORGE_PROJECTION_FIELDS[] = {
    {"parsed.age", TURBO_FLOW_EXPR_TYPE_I64, RULESFORGE_AGE_FIELD}};

static const turbo_flow_expr_schema_t RULESFORGE_EXPR_SCHEMA = {
    RULESFORGE_PROJECTION_FIELDS,
    sizeof(RULESFORGE_PROJECTION_FIELDS) / sizeof(RULESFORGE_PROJECTION_FIELDS[0])};

static int rulesforge_projection_clone(const void *value, void *ctx, void **out) {
  rulesforge_projection_lifecycle_t *lifecycle = (rulesforge_projection_lifecycle_t *)ctx;
  rulesforge_projection_t *copy;
  if (!value || !out) return SALTS_EINVAL;
  *out = NULL;
  copy = (rulesforge_projection_t *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const rulesforge_projection_t *)value;
  *out = copy;
  if (lifecycle) atomic_fetch_add_explicit(&lifecycle->clones, 1, memory_order_relaxed);
  return SALTS_OK;
}

static void rulesforge_projection_destroy(void *value, void *ctx) {
  rulesforge_projection_lifecycle_t *lifecycle = (rulesforge_projection_lifecycle_t *)ctx;
  if (lifecycle) atomic_fetch_add_explicit(&lifecycle->destroys, 1, memory_order_relaxed);
  free(value);
}

static int rulesforge_projection_read(const void *value, uint32_t field_id,
                                      turbo_flow_expr_value_t *out, void *ctx) {
  const rulesforge_projection_t *projection = (const rulesforge_projection_t *)value;
  (void)ctx;
  if (!projection || !out) return SALTS_EINVAL;
  if (field_id != RULESFORGE_AGE_FIELD) return SALTS_ENOENT;
  out->type = TURBO_FLOW_EXPR_TYPE_I64;
  out->as.i64 = projection->age;
  return SALTS_OK;
}

static int rulesforge_bind_age_with_lifecycle(turbo_flow_msg_t *message, int64_t age,
                                              rulesforge_projection_lifecycle_t *lifecycle) {
  rulesforge_projection_t *projection =
      (rulesforge_projection_t *)malloc(sizeof(*projection));
  int rc;
  if (!projection) return SALTS_ENOMEM;
  projection->age = age;
  rc = turbo_flow_msg_bind_projection(message, &RULESFORGE_PROJECTION_SCHEMA, projection,
                                      rulesforge_projection_clone, rulesforge_projection_destroy,
                                      lifecycle);
  if (rc != SALTS_OK) free(projection);
  return rc;
}

static int rulesforge_bind_age(turbo_flow_msg_t *message, int64_t age) {
  return rulesforge_bind_age_with_lifecycle(message, age, NULL);
}

static int rulesforge_mock_callback(turbo_flow_msg_t *message, const char *resource_name,
                                    void *ctx) {
  rulesforge_mock_context_t *rules = (rulesforge_mock_context_t *)ctx;

  if (!message || !resource_name || !ctx || resource_name[0] == '\0') return SALTS_EINVAL;
  if (rules->matched) {
    message->flags |= RULESFORGE_MATCHED_FLAG;
  } else {
    message->flags &= ~(uint32_t)RULESFORGE_MATCHED_FLAG;
  }
  if (turbo_flow_rulesforge_set_result(message, rules->matched ? 1u : 0u, SALTS_OK) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  rules->callback_count++;
  return SALTS_OK;
}

static int rulesforge_sink_counter(turbo_flow_msg_t *message, void *ctx) {
  rulesforge_sink_probe_t *sink = (rulesforge_sink_probe_t *)ctx;
  sink->count++;
  sink->flags = message->flags;
  sink->rule_status = message->data_decision.evaluation_status;
  sink->rule_match_count = message->data_decision.match_count;
  sink->rule_error = message->data_decision.evaluation_error;
  return SALTS_OK;
}

static int rulesforge_worker_projection_probe(turbo_flow_msg_t *message, void *ctx) {
  rulesforge_worker_probe_t *worker = (rulesforge_worker_probe_t *)ctx;
  const turbo_flow_data_schema_t *schema = NULL;
  const rulesforge_projection_t *projection;

  if (!message || !worker) return SALTS_EINVAL;
  projection =
      (const rulesforge_projection_t *)turbo_flow_msg_projection(message, &schema);
  if (!projection || schema != &RULESFORGE_PROJECTION_SCHEMA) return SALTS_EPROTO;
  atomic_store_explicit(&worker->age, (long long)projection->age, memory_order_relaxed);
  atomic_fetch_add_explicit(&worker->count, 1, memory_order_relaxed);
  return SALTS_OK;
}

static int rulesforge_stage_counter(turbo_flow_msg_t *message, void *ctx) {
  rulesforge_dispatch_probe_t *probe = (rulesforge_dispatch_probe_t *)ctx;
  if (!message || !probe) return SALTS_EINVAL;
  atomic_fetch_add_explicit(&probe->count, 1, memory_order_relaxed);
  return SALTS_OK;
}

spec("rulesforge bridge operation") {
  it("registers rulesforge.apply and executes callback from graph stage") {
    static const char source[] = "source input\n"
                                 "stage rulesforge operation rulesforge.apply resource rules.test\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rulesforge -> sink\n"
                                 "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    rulesforge_mock_context_t rules = {0, 1};
    rulesforge_sink_probe_t sink = {0};
    turbo_flow_rulesforge_data_operation_registration_t registration =
        TURBO_FLOW_RULEFORGE_DATA_OPERATION_REGISTRATION_INIT;

    registration.resource_name = "rules.test";
    registration.fn = rulesforge_mock_callback;
    registration.callback_ctx = &rules;
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, source, sizeof(source) - 1u), SALTS_OK);
    check_equal(turbo_flow_rulesforge_register_data_operation(flow, &registration), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", rulesforge_sink_counter, &sink, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(rules.callback_count, 1);
    check_equal(sink.count, 1);
    check_equal(sink.flags, RULESFORGE_MATCHED_FLAG);
    check_equal(sink.rule_status, TURBO_FLOW_DATA_MATCHED);
    check_equal(sink.rule_match_count, 1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
  }

  it("filters unmatched data through a conditional graph route") {
    static const char source[] = "source input\n"
                                 "stage rulesforge operation rulesforge.apply resource rules.test\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rulesforge\n"
                                 "  route rulesforge -> sink when msg.rule_matched\n"
                                 "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t matched_message;
    turbo_flow_msg_t unmatched_message;
    rulesforge_mock_context_t rules = {0, 1};
    rulesforge_sink_probe_t sink = {0};
    turbo_flow_rulesforge_data_operation_registration_t registration =
        TURBO_FLOW_RULEFORGE_DATA_OPERATION_REGISTRATION_INIT;

    registration.resource_name = "rules.test";
    registration.fn = rulesforge_mock_callback;
    registration.callback_ctx = &rules;
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, source, sizeof(source) - 1u), SALTS_OK);
    check_equal(turbo_flow_rulesforge_register_data_operation(flow, &registration), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", rulesforge_sink_counter, &sink, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&matched_message);
    matched_message.flags = RULESFORGE_OTHER_FLAG;
    check_equal(turbo_flow_publish(flow, "input", &matched_message), SALTS_OK);
    check_equal(rules.callback_count, 1);
    check_equal(sink.count, 1);
    check_equal(sink.flags, RULESFORGE_MATCHED_FLAG | RULESFORGE_OTHER_FLAG);
    check_equal(sink.rule_status, TURBO_FLOW_DATA_MATCHED);

    rules.matched = 0;
    turbo_flow_msg_init(&unmatched_message);
    check_equal(turbo_flow_publish(flow, "input", &unmatched_message), SALTS_OK);
    check_equal(rules.callback_count, 2);
    check_equal(sink.count, 1);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&unmatched_message);
    turbo_flow_msg_cleanup(&matched_message);
    turbo_flow_destroy(flow);
  }

  it("routes RulesForge projection fields through parsed schema expressions") {
    static const char source[] = "source input\n"
                                 "stage rulesforge operation rulesforge.apply resource rules.test\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rulesforge\n"
                                 "  route rulesforge -> sink when parsed.age >= 18\n"
                                 "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t adult_message;
    turbo_flow_msg_t minor_message;
    rulesforge_mock_context_t rules = {0, 1};
    rulesforge_sink_probe_t sink = {0};
    turbo_flow_rulesforge_data_operation_registration_t operation =
        TURBO_FLOW_RULEFORGE_DATA_OPERATION_REGISTRATION_INIT;
    turbo_flow_expr_projection_registration_t projection =
        TURBO_FLOW_EXPR_PROJECTION_REGISTRATION_INIT;

    operation.resource_name = "rules.test";
    operation.fn = rulesforge_mock_callback;
    operation.callback_ctx = &rules;
    projection.projection_schema = &RULESFORGE_PROJECTION_SCHEMA;
    projection.expr_schema = &RULESFORGE_EXPR_SCHEMA;
    projection.read_field = rulesforge_projection_read;
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, source, sizeof(source) - 1u), SALTS_OK);
    check_equal(turbo_flow_rulesforge_register_data_operation(flow, &operation), SALTS_OK);
    check_equal(turbo_flow_register_expr_projection(flow, &projection), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", rulesforge_sink_counter, &sink, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&adult_message);
    check_equal(rulesforge_bind_age(&adult_message, 21), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &adult_message), SALTS_OK);
    check_equal(sink.count, 1);
    check_equal(sink.rule_status, TURBO_FLOW_DATA_MATCHED);
    check_equal(sink.rule_match_count, 1u);

    turbo_flow_msg_init(&minor_message);
    check_equal(rulesforge_bind_age(&minor_message, 17), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &minor_message), SALTS_OK);
    check_equal(sink.count, 1);
    check_equal(rules.callback_count, 2);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&minor_message);
    turbo_flow_msg_cleanup(&adult_message);
    turbo_flow_destroy(flow);
  }

  it("keeps projection ownership valid through worker-pool disruptor routing") {
    static const char source[] = "source input\n"
                                 "stage rulesforge operation rulesforge.apply resource rules.test\n"
                                 "stage dispatch worker 1 capacity 64\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rulesforge -> dispatch\n"
                                 "  route dispatch -> sink when parsed.age >= 18\n"
                                 "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t adult_message;
    turbo_flow_msg_t minor_message;
    const rulesforge_projection_t *original_projection;
    const turbo_flow_stage_plan_t *dispatch_plan;
    rulesforge_mock_context_t rules = {0, 0};
    rulesforge_sink_probe_t sink = {0};
    rulesforge_projection_lifecycle_t lifecycle;
    rulesforge_worker_probe_t worker;
    turbo_flow_rulesforge_data_operation_registration_t operation =
        TURBO_FLOW_RULEFORGE_DATA_OPERATION_REGISTRATION_INIT;
    turbo_flow_expr_projection_registration_t projection =
        TURBO_FLOW_EXPR_PROJECTION_REGISTRATION_INIT;
    int clone_count;

    atomic_init(&lifecycle.clones, 0);
    atomic_init(&lifecycle.destroys, 0);
    atomic_init(&worker.count, 0);
    atomic_init(&worker.age, 0);
    operation.resource_name = "rules.test";
    operation.fn = rulesforge_mock_callback;
    operation.callback_ctx = &rules;
    projection.projection_schema = &RULESFORGE_PROJECTION_SCHEMA;
    projection.expr_schema = &RULESFORGE_EXPR_SCHEMA;
    projection.read_field = rulesforge_projection_read;
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, source, sizeof(source) - 1u), SALTS_OK);
    check_equal(turbo_flow_rulesforge_register_data_operation(flow, &operation), SALTS_OK);
    check_equal(turbo_flow_register_expr_projection(flow, &projection), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "dispatch", rulesforge_worker_projection_probe, &worker,
                                     NULL),
        SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", rulesforge_sink_counter, &sink, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    dispatch_plan =
        turbo_flow_stage_at(flow, (size_t)turbo_flow_find_stage(flow, "dispatch"));
    check_not_null(dispatch_plan);
    check_equal(dispatch_plan->data_strategy, TURBO_FLOW_DATA_WORKER_POOL);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&adult_message);
    check_equal(rulesforge_bind_age_with_lifecycle(&adult_message, 21, &lifecycle), SALTS_OK);
    original_projection =
        (const rulesforge_projection_t *)turbo_flow_msg_projection(&adult_message, NULL);
    check_not_null(original_projection);
    check_equal(turbo_flow_publish(flow, "input", &adult_message), SALTS_OK);
    check_equal(atomic_load_explicit(&worker.count, memory_order_relaxed), 1);
    check_equal((int)atomic_load_explicit(&worker.age, memory_order_relaxed), 21);
    check_equal(sink.count, 1);
    check_equal((const void *)turbo_flow_msg_projection(&adult_message, NULL),
                (const void *)original_projection);
    check_equal((int)original_projection->age, 21);

    turbo_flow_msg_init(&minor_message);
    check_equal(rulesforge_bind_age_with_lifecycle(&minor_message, 17, &lifecycle), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &minor_message), SALTS_OK);
    check_equal(atomic_load_explicit(&worker.count, memory_order_relaxed), 2);
    check_equal((int)atomic_load_explicit(&worker.age, memory_order_relaxed), 17);
    check_equal(sink.count, 1);
    check_equal(rules.callback_count, 2);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    clone_count = atomic_load_explicit(&lifecycle.clones, memory_order_relaxed);
    check_greater_equal(clone_count, 2);
    check_equal(atomic_load_explicit(&lifecycle.destroys, memory_order_relaxed), clone_count);
    turbo_flow_msg_cleanup(&minor_message);
    turbo_flow_msg_cleanup(&adult_message);
    check_equal(atomic_load_explicit(&lifecycle.destroys, memory_order_relaxed), clone_count + 2);
    turbo_flow_destroy(flow);
  }

  it("processes owned DataBind objects with an isolated RulesForge provider") {
    static const char schema_text[] =
        "schema TurboFlowRules [id(2), version(1), byte_order(little)]; "
        "message Applicant { int64 age; }";
    static const char source[] = "source input\n"
                                 "stage rulesforge operation rulesforge.apply resource rules.test\n"
                                 "stage dispatch worker 1 capacity 64\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rulesforge -> dispatch\n"
                                 "  route dispatch -> sink when msg.rule_matched\n"
                                 "}\n";
    char rfl[2048];
    char *schema_path = tt_make_temp_file("turbo_flow_rulesforge", ".schema");
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t adult_message;
    turbo_flow_msg_t minor_message;
    ruleforge_knowledge_base_t kb = NULL;
    ruleforge_data_bind_object_t object = NULL;
    turbo_flow_rulesforge_databind_provider_t provider =
        TURBO_FLOW_RULEFORGE_DATABIND_PROVIDER_INIT;
    rulesforge_sink_probe_t sink = {0};
    rulesforge_dispatch_probe_t dispatch;
    int rfl_len;
    size_t i;

    check_not_null(schema_path);
    for (i = 0; schema_path[i] != '\0'; ++i) {
      if (schema_path[i] == '\\') schema_path[i] = '/';
    }
    check_equal(tt_write_file(schema_path, schema_text, sizeof(schema_text) - 1u), 0);
    rfl_len = snprintf(rfl, sizeof(rfl),
                       "import \"%s\";\n"
                       "rule \"Adult applicant\"\n"
                       "when\n"
                       "  Applicant(age >= 18)\n"
                       "then\n"
                       "end\n",
                       schema_path);
    check_greater(rfl_len, 0);
    check_less(rfl_len, (int)sizeof(rfl));
    check_equal(ruleforge_init(), RULES_FORGE_OK);
    check_equal(ruleforge_kb_create(&kb), RULES_FORGE_OK);
    check_equal(ruleforge_kb_load_drl(kb, rfl), RULES_FORGE_OK);

    provider.knowledge_base = kb;
    provider.matched_flag = RULESFORGE_MATCHED_FLAG;
    provider.max_rules = 16;
    atomic_init(&dispatch.count, 0);
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, source, sizeof(source) - 1u), SALTS_OK);
    check_equal(
        turbo_flow_rulesforge_register_databind_provider(flow, "rules.test", &provider),
        SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "dispatch", rulesforge_stage_counter, &dispatch,
                                     NULL),
        SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", rulesforge_sink_counter, &sink, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&adult_message);
    adult_message.flags = RULESFORGE_OTHER_FLAG;
    check_equal(ruleforge_data_bind_object_from_json(schema_path, "Applicant", "{\"age\":21}",
                                                      sizeof("{\"age\":21}") - 1u, &object),
                 RULES_FORGE_OK);
    check_equal(turbo_flow_rulesforge_bind_databind_object(
                     &adult_message, &RULESFORGE_DATABIND_SCHEMA, object),
                 SALTS_OK);
    object = NULL;
    check_equal(turbo_flow_publish(flow, "input", &adult_message), SALTS_OK);
    check_equal(sink.count, 1);
    check_equal(sink.flags, RULESFORGE_MATCHED_FLAG | RULESFORGE_OTHER_FLAG);
    check_equal(sink.rule_status, TURBO_FLOW_DATA_MATCHED);
    check_equal(sink.rule_match_count, 1u);
    check_equal(sink.rule_error, SALTS_OK);

    turbo_flow_msg_init(&minor_message);
    check_equal(ruleforge_data_bind_object_from_json(schema_path, "Applicant", "{\"age\":17}",
                                                      sizeof("{\"age\":17}") - 1u, &object),
                 RULES_FORGE_OK);
    check_equal(turbo_flow_rulesforge_bind_databind_object(
                     &minor_message, &RULESFORGE_DATABIND_SCHEMA, object),
                 SALTS_OK);
    object = NULL;
    check_equal(turbo_flow_publish(flow, "input", &minor_message), SALTS_OK);
    check_equal(sink.count, 1);
    check_equal(atomic_load_explicit(&dispatch.count, memory_order_relaxed), 2);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&minor_message);
    turbo_flow_msg_cleanup(&adult_message);
    turbo_flow_destroy(flow);
    check_equal(ruleforge_kb_destroy(kb), RULES_FORGE_OK);
    check_equal(ruleforge_cleanup(), RULES_FORGE_OK);
    check_equal(tt_remove_file(schema_path), 0);
    free(schema_path);
  }

  it("processes raw JSON payload without replacing an existing projection") {
    static const char schema_text[] =
        "schema TurboFlowJsonRules [id(3), version(1), byte_order(little)]; "
        "message Applicant { int64 age; }";
    static const char source[] = "source input\n"
                                 "stage rulesforge operation rulesforge.apply resource rules.json\n"
                                 "stage dispatch worker 1 capacity 64\n"
                                 "stage sink\n"
                                 "stage main {\n"
                                 "  input -> rulesforge -> dispatch\n"
                                 "  route dispatch -> sink when msg.rule_matched\n"
                                 "}\n";
    char rfl[2048];
    char *schema_path = tt_make_temp_file("turbo_flow_rulesforge_json", ".schema");
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    ruleforge_knowledge_base_t kb = NULL;
    turbo_flow_rulesforge_json_provider_t provider =
        TURBO_FLOW_RULEFORGE_JSON_PROVIDER_INIT;
    rulesforge_sink_probe_t sink = {0};
    rulesforge_dispatch_probe_t dispatch;
    int rfl_len;
    size_t i;

    check_not_null(schema_path);
    for (i = 0; schema_path[i] != '\0'; ++i) {
      if (schema_path[i] == '\\') schema_path[i] = '/';
    }
    check_equal(tt_write_file(schema_path, schema_text, sizeof(schema_text) - 1u), 0);
    rfl_len = snprintf(rfl, sizeof(rfl),
                       "import \"%s\";\n"
                       "rule \"Adult JSON applicant\"\n"
                       "when\n"
                       "  Applicant(age >= 18)\n"
                       "then\n"
                       "end\n",
                       schema_path);
    check_greater(rfl_len, 0);
    check_less(rfl_len, (int)sizeof(rfl));
    check_equal(ruleforge_init(), RULES_FORGE_OK);
    check_equal(ruleforge_kb_create(&kb), RULES_FORGE_OK);
    check_equal(ruleforge_kb_load_drl(kb, rfl), RULES_FORGE_OK);

    provider.knowledge_base = kb;
    provider.fact_type = "Applicant";
    provider.matched_flag = RULESFORGE_MATCHED_FLAG;
    provider.max_rules = 16;
    atomic_init(&dispatch.count, 0);
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, source, sizeof(source) - 1u), SALTS_OK);
    check_equal(turbo_flow_rulesforge_register_json_provider(flow, "rules.json", &provider),
                 SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "dispatch", rulesforge_stage_counter, &dispatch, NULL),
        SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "sink", rulesforge_sink_counter, &sink, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.flags = RULESFORGE_OTHER_FLAG;
    message.owned_payload = tstr_dup("{\"age\":21}");
    check_not_null(message.owned_payload);
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(rulesforge_bind_age(&message, 99), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(sink.count, 1);
    check_equal(sink.flags, RULESFORGE_MATCHED_FLAG | RULESFORGE_OTHER_FLAG);
    check_equal(sink.rule_status, TURBO_FLOW_DATA_MATCHED);
    check_equal(sink.rule_match_count, 1u);
    check_equal(sink.rule_error, SALTS_OK);
    check_equal(atomic_load_explicit(&dispatch.count, memory_order_relaxed), 1);
    check_not_null(turbo_flow_msg_projection(&message, NULL));

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    turbo_flow_destroy(flow);
    check_equal(ruleforge_kb_destroy(kb), RULES_FORGE_OK);
    check_equal(ruleforge_cleanup(), RULES_FORGE_OK);
    check_equal(tt_remove_file(schema_path), 0);
    free(schema_path);
  }
}
