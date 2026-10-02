#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow_provider.h"

#include <string.h>

typedef struct scoped_probe_s {
  int starts;
  int consumes;
  const char *last_stage;
} scoped_probe_t;

static int scoped_start(
    void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  scoped_probe_t *probe = (scoped_probe_t *)ctx;
  (void)flow;
  if (!probe || !stage) return SALTS_EINVAL;
  ++probe->starts;
  probe->last_stage = stage->name;
  return SALTS_OK;
}

static int scoped_consume(
    void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
    turbo_flow_msg_t *message) {
  scoped_probe_t *probe = (scoped_probe_t *)ctx;
  (void)flow;
  if (!probe || !stage || !message) return SALTS_EINVAL;
  ++probe->consumes;
  probe->last_stage = stage->name;
  return SALTS_OK;
}

static turbo_flow_adapter_schema_t sink_schema(void) {
  turbo_flow_adapter_schema_t schema;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  return schema;
}

static turbo_flow_adapter_schema_t dual_schema(void) {
  turbo_flow_adapter_schema_t schema;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  return schema;
}

static turbo_flow_adapter_ops_t sink_ops(void) {
  turbo_flow_adapter_ops_t ops;
  memset(&ops, 0, sizeof(ops));
  ops.consume = scoped_consume;
  return ops;
}

static turbo_flow_adapter_ops_t dual_ops(void) {
  turbo_flow_adapter_ops_t ops = sink_ops();
  ops.start = scoped_start;
  return ops;
}

static int register_scoped(
    turbo_flow_t *flow, const char *identity,
    const char *const *stages, size_t stage_count,
    const turbo_flow_adapter_ops_t *ops,
    const turbo_flow_adapter_schema_t *schema,
    scoped_probe_t *probe) {
  turbo_flow_provider_adapter_registration_v1_t registration =
      TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT;
  registration.provider_identity = identity;
  registration.stage_names = stages;
  registration.stage_count = stage_count;
  registration.adapter_ops = ops;
  registration.schema = schema;
  registration.ctx = probe;
  return turbo_flow_provider_adapter_register(flow, &registration);
}

spec("provider-scoped adapter stage binding") {
  it("materializes two independent registrations for one provider identity") {
    static const char *src =
        "source input\n"
        "stage a adapter fixture.provider\n"
        "stage b adapter fixture.provider\n"
        "stage main {\n"
        "  input -> [a, b]\n"
        "}\n";
    const char *stage_a[] = {"a"};
    const char *stage_b[] = {"b"};
    scoped_probe_t a = {0};
    scoped_probe_t b = {0};
    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    int a_index;
    int b_index;
    const flow_adapter_registration_t *a_adapter;
    const flow_adapter_registration_t *b_adapter;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        register_scoped(
            flow, "fixture.provider", stage_a, 1u, &ops, &schema, &a),
        SALTS_OK);
    check_equal(
        register_scoped(
            flow, "fixture.provider", stage_b, 1u, &ops, &schema, &b),
        SALTS_OK);

    check_null(turbo_flow_find_adapter_schema(flow, "fixture.provider"));
    check_equal(turbo_flow_adapter_count(flow), (size_t)2u);
    check_equal(turbo_flow_compile(flow), SALTS_OK);

    a_index = turbo_flow_find_stage(flow, "a");
    b_index = turbo_flow_find_stage(flow, "b");
    check_true(a_index >= 0);
    check_true(b_index >= 0);
    a_adapter = flow_adapter_for_compiled_stage(flow, (uint32_t)a_index);
    b_adapter = flow_adapter_for_compiled_stage(flow, (uint32_t)b_index);
    check_not_null(a_adapter);
    check_not_null(b_adapter);
    check_true(a_adapter != b_adapter);
    if (a_adapter) check_true(a_adapter->ctx == &a);
    if (b_adapter) check_true(b_adapter->ctx == &b);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(a.consumes, 1);
    check_equal(b.consumes, 1);
    check_equal(a.last_stage, "a");
    check_equal(b.last_stage, "b");
    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    /* Parsed-stage binding is not retained across reset even when registries are. */
    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_not_equal(turbo_flow_compile(flow), SALTS_OK);

    turbo_flow_destroy(flow);
  }

  it("binds one provider registration to a source and terminal stage") {
    static const char *src =
        "source ingress adapter fixture.dual\n"
        "stage egress adapter fixture.dual\n"
        "stage main {\n"
        "  ingress -> egress\n"
        "}\n";
    const char *stages[] = {"ingress", "egress"};
    scoped_probe_t probe = {0};
    turbo_flow_adapter_ops_t ops = dual_ops();
    turbo_flow_adapter_schema_t schema = dual_schema();
    turbo_flow_t *flow = turbo_flow_create();
    int ingress_index;
    int egress_index;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        register_scoped(
            flow, "fixture.dual", stages, 2u, &ops, &schema, &probe),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);

    ingress_index = turbo_flow_find_stage(flow, "ingress");
    egress_index = turbo_flow_find_stage(flow, "egress");
    check_true(ingress_index >= 0);
    check_true(egress_index >= 0);
    check_true(
        flow_adapter_for_compiled_stage(flow, (uint32_t)ingress_index) ==
        flow_adapter_for_compiled_stage(flow, (uint32_t)egress_index));

    turbo_flow_destroy(flow);
  }

  it("rejects duplicate and mismatched stage binding without registry drift") {
    static const char *src =
        "stage a adapter fixture.provider\n"
        "stage wrong adapter fixture.other\n";
    const char *stage_a[] = {"a"};
    const char *stage_wrong[] = {"wrong"};
    scoped_probe_t first = {0};
    scoped_probe_t second = {0};
    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_t *flow = turbo_flow_create();
    size_t adapter_count;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        register_scoped(
            flow, "fixture.provider", stage_a, 1u, &ops, &schema, &first),
        SALTS_OK);
    adapter_count = turbo_flow_adapter_count(flow);

    check_equal(
        register_scoped(
            flow, "fixture.provider", stage_a, 1u, &ops, &schema, &second),
        SALTS_EALREADY);
    check_equal(turbo_flow_adapter_count(flow), adapter_count);

    check_equal(
        register_scoped(
            flow, "fixture.provider", stage_wrong, 1u, &ops, &schema, &second),
        SALTS_EPROTO);
    check_equal(turbo_flow_adapter_count(flow), adapter_count);

    turbo_flow_destroy(flow);
  }

  it("preserves legacy global name-based registration") {
    static const char *src =
        "source input\n"
        "stage sink adapter legacy.adapter\n"
        "stage main { input -> sink }\n";
    scoped_probe_t probe = {0};
    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(
        turbo_flow_register_adapter_ex(
            flow, "legacy.adapter", &ops, &probe, &schema),
        SALTS_OK);
    check_not_null(turbo_flow_find_adapter_schema(flow, "legacy.adapter"));
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);

    turbo_flow_destroy(flow);
  }
}
