#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow_provider.h"

#include <string.h>

typedef struct scoped_probe_s {
  int starts;
  int consumes;
  int shutdowns;
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

static void scoped_shutdown(void *ctx) {
  scoped_probe_t *probe = (scoped_probe_t *)ctx;
  if (probe) ++probe->shutdowns;
}

static int scoped_terminal_submit(
    void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
    const turbo_flow_msg_t *message,
    turbo_flow_async_terminal_claim_t *claim) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)message;
  (void)claim;
  return SALTS_OK;
}

static int fail_async_terminal_bind(
    void *ctx, flow_registration_checkpoint_t checkpoint) {
  (void)ctx;
  return checkpoint == FLOW_REGISTRATION_ASYNC_TERMINAL_BIND
             ? SALTS_EIO
             : SALTS_OK;
}

typedef struct scoped_boundary_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
} scoped_boundary_t;

typedef struct scoped_managed_owner_s {
  scoped_probe_t probe;
  scoped_boundary_t boundary;
} scoped_managed_owner_t;

static void scoped_boundary_init(scoped_boundary_t *boundary) {
  memset(boundary, 0, sizeof(*boundary));
  boundary->metadata =
      (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  boundary->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  boundary->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(boundary->metadata.uid, "connection:scoped", sizeof("connection:scoped"));
  memcpy(boundary->metadata.owner_name, "scoped.owner", sizeof("scoped.owner"));
  boundary->metadata.generation = 1u;
  boundary->metadata.observed_generation = 1u;

  boundary->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)
          TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  boundary->descriptor.domain = boundary->metadata.domain;
  boundary->descriptor.kind = boundary->metadata.kind;
  boundary->descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  memcpy(boundary->descriptor.uid, boundary->metadata.uid,
         strlen(boundary->metadata.uid) + 1u);
  memcpy(boundary->descriptor.owner_name, boundary->metadata.owner_name,
         strlen(boundary->metadata.owner_name) + 1u);
  check_equal(
      turbo_flow_content_descriptor_init(
          &boundary->descriptor.input,
          TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
          TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA,
          TURBO_FLOW_DATA_ENCODING_JSON,
          "application/json", "scoped-input"),
      SALTS_OK);
  check_equal(
      turbo_flow_content_descriptor_declare_schema(
          &boundary->descriptor.input, "ScopedBoundary", "Input", 1u),
      SALTS_OK);

  boundary->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)
          TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  memcpy(boundary->snapshot.uid, boundary->metadata.uid,
         strlen(boundary->metadata.uid) + 1u);
  boundary->snapshot.generation = 1u;
  boundary->snapshot.observed_generation = 1u;
  boundary->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
}

static int scoped_boundary_metadata(
    void *ctx, turbo_flow_resource_metadata_t *out) {
  scoped_managed_owner_t *owner = (scoped_managed_owner_t *)ctx;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = owner->boundary.metadata;
  return SALTS_OK;
}

static int scoped_boundary_descriptor(
    void *ctx, turbo_flow_managed_boundary_descriptor_t *out) {
  scoped_managed_owner_t *owner = (scoped_managed_owner_t *)ctx;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = owner->boundary.descriptor;
  return SALTS_OK;
}

static int scoped_boundary_snapshot(
    void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  scoped_managed_owner_t *owner = (scoped_managed_owner_t *)ctx;
  if (!owner || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = owner->boundary.snapshot;
  return SALTS_OK;
}

static void scoped_managed_owner_shutdown(void *ctx) {
  scoped_managed_owner_t *owner = (scoped_managed_owner_t *)ctx;
  if (owner) ++owner->probe.shutdowns;
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
    check_equal(turbo_flow_adapter_count(flow), (size_t)0u);
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

  it("rolls back async-terminal registration without taking ctx ownership") {
    static const char *src =
        "stage sink adapter fixture.async\n";
    const char *stages[] = {"sink"};
    scoped_probe_t probe = {0};
    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_async_terminal_adapter_ops_t async_ops =
        TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_provider_adapter_registration_v1_t registration =
        TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT;
    turbo_flow_t *flow = turbo_flow_create();
    flow_stage_plan_impl_t *stage;
    int stage_index;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    ops.shutdown = scoped_shutdown;
    async_ops.submit = scoped_terminal_submit;
    registration.provider_identity = "fixture.async";
    registration.stage_names = stages;
    registration.stage_count = 1u;
    registration.adapter_ops = &ops;
    registration.async_terminal_ops = &async_ops;
    registration.schema = &schema;
    registration.ctx = &probe;

    flow->registration_fault.before_commit = fail_async_terminal_bind;
    check_equal(
        turbo_flow_provider_adapter_register(flow, &registration),
        SALTS_EIO);
    check_equal(turbo_flow_adapter_count(flow), (size_t)0u);
    check_equal(probe.shutdowns, 0);

    stage_index = turbo_flow_find_stage(flow, "sink");
    check_true(stage_index >= 0);
    stage = stage_index >= 0
                ? (flow_stage_plan_impl_t *)vec_at(
                      &flow->stages, (size_t)stage_index)
                : NULL;
    check_not_null(stage);
    if (stage) check_false(stage->provider_adapter_bound);

    flow->registration_fault.before_commit = NULL;
    check_equal(
        turbo_flow_provider_adapter_register(flow, &registration),
        SALTS_OK);
    check_equal(turbo_flow_adapter_count(flow), (size_t)1u);
    if (stage) check_true(stage->provider_adapter_bound);
    check_equal(probe.shutdowns, 0);

    turbo_flow_destroy(flow);
    check_equal(probe.shutdowns, 1);
  }

  it("drops scoped managed boundary ownership on keep-registry reset") {
    static const char *src =
        "stage sink adapter fixture.managed\n";
    const char *stages[] = {"sink"};
    scoped_managed_owner_t owner;
    scoped_managed_owner_t ordinary;
    turbo_flow_resource_provider_ops_t ordinary_ops =
        TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
    turbo_flow_resource_metadata_t remaining =
        TURBO_FLOW_RESOURCE_METADATA_INIT;

    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_managed_boundary_provider_ops_t boundary_ops =
        TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
    turbo_flow_provider_adapter_registration_v1_t registration =
        TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&owner, 0, sizeof(owner));
    memset(&ordinary, 0, sizeof(ordinary));
    scoped_boundary_init(&owner.boundary);
    scoped_boundary_init(&ordinary.boundary);
    memcpy(ordinary.boundary.metadata.uid, "connection:ordinary",
           sizeof("connection:ordinary"));
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    ops.shutdown = scoped_managed_owner_shutdown;
    boundary_ops.resource.metadata = scoped_boundary_metadata;
    boundary_ops.descriptor = scoped_boundary_descriptor;
    boundary_ops.snapshot = scoped_boundary_snapshot;

    registration.provider_identity = "fixture.managed";
    registration.stage_names = stages;
    registration.stage_count = 1u;
    registration.adapter_ops = &ops;
    registration.schema = &schema;
    registration.managed_owner_name = owner.boundary.metadata.owner_name;
    registration.managed_boundary_ops = &boundary_ops;
    registration.ctx = &owner;

    owner.boundary.descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SOURCE;
    check_equal(
        turbo_flow_provider_adapter_register(flow, &registration),
        SALTS_EPROTO);
    check_equal(turbo_flow_adapter_count(flow), (size_t)0u);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(owner.probe.shutdowns, 0);

    owner.boundary.descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
    check_equal(
        turbo_flow_provider_adapter_register(flow, &registration),
        SALTS_OK);

    ordinary_ops.metadata = scoped_boundary_metadata;
    check_equal(
        turbo_flow_register_resource_provider(
            flow, ordinary.boundary.metadata.owner_name,
            &ordinary_ops, &ordinary),
        SALTS_OK);

    check_equal(turbo_flow_adapter_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)2u);
    check_equal(owner.probe.shutdowns, 0);

    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
    check_equal(turbo_flow_adapter_count(flow), (size_t)0u);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    check_equal(turbo_flow_resource_metadata_count(flow), (size_t)1u);
    check_equal(turbo_flow_resource_metadata_at(flow, 0u, &remaining), SALTS_OK);
    check_equal(remaining.uid, "connection:ordinary");
    check_equal(remaining.owner_name, "scoped.owner");
    check_equal(owner.probe.shutdowns, 1);

    turbo_flow_destroy(flow);
    check_equal(owner.probe.shutdowns, 1);
  }

  it("keeps provider deployment resources out of primitive resolution") {
    static const char *provider_src =
        "source input\n"
        "stage sink adapter fixture.provider {\n"
        "  resource deployment_main\n"
        "}\n"
        "stage main {\n"
        "  input -> sink\n"
        "}\n";
    static const char *legacy_src =
        "source input\n"
        "stage sink adapter legacy.adapter {\n"
        "  resource deployment_main\n"
        "}\n"
        "stage main {\n"
        "  input -> sink\n"
        "}\n";
    const char *stages[] = {"sink"};
    scoped_probe_t scoped = {0};
    scoped_probe_t legacy = {0};
    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_t *provider_flow = turbo_flow_create();
    turbo_flow_t *legacy_flow = turbo_flow_create();
    int provider_parse = SALTS_ENOMEM;
    int provider_register = SALTS_ENOMEM;
    int provider_compile = SALTS_ENOMEM;
    int legacy_register = SALTS_ENOMEM;
    int legacy_parse = SALTS_ENOMEM;
    int legacy_compile = SALTS_ENOMEM;

    if (provider_flow) {
      provider_parse =
          turbo_flow_parse_string(
              provider_flow, provider_src, strlen(provider_src));
      if (provider_parse == SALTS_OK)
        provider_register =
            register_scoped(
                provider_flow, "fixture.provider", stages, 1u,
                &ops, &schema, &scoped);
      if (provider_register == SALTS_OK)
        provider_compile = turbo_flow_compile(provider_flow);
    }
    if (legacy_flow) {
      legacy_register =
          turbo_flow_register_adapter_ex(
              legacy_flow, "legacy.adapter", &ops, &legacy, &schema);
      if (legacy_register == SALTS_OK)
        legacy_parse =
            turbo_flow_parse_string(
                legacy_flow, legacy_src, strlen(legacy_src));
      if (legacy_parse == SALTS_OK)
        legacy_compile = turbo_flow_compile(legacy_flow);
    }

    turbo_flow_destroy(provider_flow);
    turbo_flow_destroy(legacy_flow);

    check_not_null(provider_flow);
    check_not_null(legacy_flow);
    check_equal(provider_parse, SALTS_OK);
    check_equal(provider_register, SALTS_OK);
    check_equal(provider_compile, SALTS_OK);
    check_equal(legacy_register, SALTS_OK);
    check_equal(legacy_parse, SALTS_OK);
    check_equal(legacy_compile, SALTS_EINVAL);
  }

  it("preserves legacy global name-based registration") {
    static const char *src =
        "source input\n"
        "stage sink adapter legacy.adapter\n"
        "stage main {\n"
        "  input -> sink\n"
        "}\n";
    scoped_probe_t probe = {0};
    turbo_flow_adapter_ops_t ops = sink_ops();
    turbo_flow_adapter_schema_t schema = sink_schema();
    turbo_flow_t *flow = turbo_flow_create();
    int register_status = SALTS_ENOMEM;
    int parse_one_status = SALTS_ENOMEM;
    int compile_one_status = SALTS_ENOMEM;
    int reset_status = SALTS_ENOMEM;
    size_t adapter_count_after_reset = 0u;
    int schema_present_after_reset = 0;
    int parse_two_status = SALTS_ENOMEM;
    int compile_two_status = SALTS_ENOMEM;

    if (flow) {
      register_status = turbo_flow_register_adapter_ex(
          flow, "legacy.adapter", &ops, &probe, &schema);
      parse_one_status = turbo_flow_parse_string(flow, src, strlen(src));
      if (parse_one_status == SALTS_OK)
        compile_one_status = turbo_flow_compile(flow);
      if (compile_one_status == SALTS_OK)
        reset_status = turbo_flow_reset(flow, 1);
      if (reset_status == SALTS_OK) {
        adapter_count_after_reset = turbo_flow_adapter_count(flow);
        schema_present_after_reset =
            turbo_flow_find_adapter_schema(flow, "legacy.adapter") != NULL;
        parse_two_status = turbo_flow_parse_string(flow, src, strlen(src));
        if (parse_two_status == SALTS_OK)
          compile_two_status = turbo_flow_compile(flow);
      }
      turbo_flow_destroy(flow);
    }

    check_not_null(flow);
    check_equal(register_status, SALTS_OK);
    check_equal(parse_one_status, SALTS_OK);
    check_equal(compile_one_status, SALTS_OK);
    check_equal(reset_status, SALTS_OK);
    check_equal(adapter_count_after_reset, (size_t)1u);
    check_true(schema_present_after_reset);
    check_equal(parse_two_status, SALTS_OK);
    check_equal(compile_two_status, SALTS_OK);
  }
}
