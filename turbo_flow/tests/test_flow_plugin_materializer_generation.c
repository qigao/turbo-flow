#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_operation.h"
#include "../../tests/flow_operation_fixture.h"

#include <cmeta/function.h>
#include <cmeta/type_select.h>
#include <fmt.h>
#include <stdlib.h>
#include <string.h>
#include <tinytest.h>

FunctionDecl(value, int, materializer_operation_semantic,
    (int, value, CMETA_PARAM_IN));
int materializer_operation_semantic(int value) {
  return value * 2;
}

static int materializer_register_reflected_operation(
    turbo_flow_t *flow, const char *name) {
  turbo_flow_operation_descriptor_t operation = {0};
  turbo_flow_operation_port_binding_t ports[2];
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

  operation.size = sizeof(operation);
  operation.name = name;
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  operation.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;

  ports[0] = (turbo_flow_operation_port_binding_t)
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  ports[0].port_index = 0u;
  ports[0].domain = TURBO_FLOW_DOMAIN_DATA;
  ports[0].direction = TURBO_FLOW_OPERATION_PORT_INPUT;
  ports[0].value_kind = TURBO_FLOW_OPERATION_VALUE_PARAMETER;
  ports[0].storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  ports[0].parameter_index = 0u;
  ports[0].data = &cmeta_data_int;

  ports[1] = (turbo_flow_operation_port_binding_t)
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  ports[1].port_index = 0u;
  ports[1].domain = TURBO_FLOW_DOMAIN_DATA;
  ports[1].direction = TURBO_FLOW_OPERATION_PORT_OUTPUT;
  ports[1].value_kind = TURBO_FLOW_OPERATION_VALUE_RETURN;
  ports[1].storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  ports[1].parameter_index = SIZE_MAX;
  ports[1].data = &cmeta_data_int;

  registration.operation = &operation;
  registration.function = FunctionMeta(materializer_operation_semantic);
  registration.abi = FunctionAbi(materializer_operation_semantic);
  registration.ports = ports;
  registration.port_count = 2u;
  registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;
  return turbo_flow_register_reflected_operation(flow, &registration);
}

static int materializer_noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return SALTS_OK;
}

static int materializer_publish_claim(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage,
                                      turbo_flow_msg_t *message) {
  (void)flow;
  (void)stage;
  return turbo_flow_publish((turbo_flow_t *)ctx, "input", message);
}

static const char yaml_good[] = "version: 1\nadapters: {}\n";

#define OPERATION_POLICY                                                                           \
  "  policy { plugin \"fixture.operation\", version 1, max_inflight 1, max_input_bytes 8, "        \
  "max_result_bytes 8, max_retained_bytes 32, max_steps 2 }\n"

static const char graph_text[] = "source input\n"
                                 "stage calculate operation fixture.double {\n" OPERATION_POLICY
                                 "  materializer { plugin \"fixture.materializer.operation\" }\n"
                                 "}\n"
                                 "stage main {\n"
                                 "  input -> calculate\n"
                                 "}\n";

static const char yaml_two_consumers[] = "version: 1\nadapters: {}\n";

static const char graph_two_consumers[] =
    "source input\n"
    "stage calculate operation fixture.double {\n" OPERATION_POLICY
    "  materializer { plugin \"fixture.materializer.operation\" }\n"
    "}\n"
    "stage decide operation fixture.other {\n" OPERATION_POLICY
    "  materializer { plugin \"fixture.materializer.operation\" }\n"
    "}\n"
    "stage main {\n"
    "  input -> calculate\n"
    "  calculate -> decide\n"
    "}\n";

typedef struct materializer_generation_test_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_plugin_result_domain_t *domain;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_plugin_generation_t *generation;
  turbo_flow_plugin_generation_t *cleanup;
  turbo_flow_t *flow;
  turbo_flow_config_error_t error;
} materializer_generation_test_t;

static int open_test_with_operation(materializer_generation_test_t *t,
                                    const char *operation_path,
                                    const char *materializer_path,
                                    const char *yaml, const char *graph,
                                    size_t result_capacity, int register_other) {
  turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  int rc;
  memset(t, 0, sizeof(*t));
  t->error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  hc.module_capacity = 2u;
  hc.schema_capacity = 4u;
  hc.operation_capacity = 2u;
  hc.materializer_capacity = 2u;
  rc = turbo_flow_plugin_host_create(&hc, &t->host, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(t->host, operation_path, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(t->host, materializer_path, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(t->host, &t->snapshot, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_result_domain_create(t->snapshot, result_capacity, &t->domain, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &t->resolved, &t->error);
  if (rc != SALTS_OK) return rc;
  t->flow = turbo_flow_create();
  if (!t->flow) return SALTS_ENOMEM;
  rc = materializer_register_reflected_operation(t->flow, "fixture.double");
  if (rc != SALTS_OK) return rc;
  if (register_other) {
    rc = materializer_register_reflected_operation(t->flow, "fixture.other");
    if (rc != SALTS_OK) return rc;
  }
  return turbo_flow_parse_string(t->flow, graph, strlen(graph));
}

static int open_test(materializer_generation_test_t *t, const char *materializer_path,
                     const char *yaml) {
  return open_test_with_operation(t, FLOW_OPERATION_OK, materializer_path, yaml,
                                  graph_text, 1u, 0);
}

static int create_generation(materializer_generation_test_t *t) {
  turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  return turbo_flow_plugin_generation_create(t->snapshot, t->resolved, &t->flow, &gc, t->domain,
                                             &t->generation, &t->cleanup, &t->error);
}

static void close_test(materializer_generation_test_t *t) {
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
  if (t->cleanup) {
    (void)turbo_flow_plugin_generation_destroy(t->cleanup, 0u, &ce);
    t->cleanup = NULL;
  }
  if (t->generation) {
    (void)turbo_flow_plugin_generation_destroy(t->generation, 0u, &ce);
    t->generation = NULL;
  }
  if (t->flow) turbo_flow_destroy(t->flow);
  if (t->domain) (void)turbo_flow_plugin_result_domain_destroy(t->domain, &pe);
  if (t->resolved) turbo_flow_resolved_config_destroy(t->resolved);
  if (t->snapshot) turbo_flow_plugin_catalog_snapshot_destroy(t->snapshot);
  if (t->host) (void)turbo_flow_plugin_host_destroy(t->host, 0u, &pe);
  memset(t, 0, sizeof(*t));
}

static void replace_once(char *out, size_t capacity, const char *source,
                         const char *from, const char *to) {
  const char *at = strstr(source, from);
  size_t prefix;
  check_not_null(at);
  prefix = (size_t)(at - source);
  check_less(prefix + strlen(to) + strlen(at + strlen(from)), capacity);
  memcpy(out, source, prefix);
  strcpy(out + prefix, to);
  strcpy(out + prefix + strlen(to), at + strlen(from));
}

static void materializer_message(turbo_flow_msg_t *msg, const char *payload,
                                 const char *schema_name, uint32_t schema_version) {
  turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  turbo_flow_msg_init(msg);
  msg->owned_payload = tstr_dup(payload);
  check_not_null(msg->owned_payload);
  msg->payload = tstr_to_v(msg->owned_payload);
  check_equal(turbo_flow_content_descriptor_init(
                  &descriptor, TURBO_FLOW_DOMAIN_DATA,
                  TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_OPAQUE,
                  "application/octet-stream", "materializer-test"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(
                  &descriptor, schema_name, "Integer", schema_version),
              SALTS_OK);
  check_equal(turbo_flow_msg_copy_content_descriptor(msg, &descriptor), SALTS_OK);
}

static void materializer_durable_identity(turbo_flow_msg_t *msg) {
  static const char source_id[] = "durable-source";
  static const char admission_id[] = "durable-admission";
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  identity.source_id = vstr_from_buf(source_id, sizeof(source_id) - 1u);
  identity.admission_id = vstr_from_buf(admission_id, sizeof(admission_id) - 1u);
  identity.source_sequence = 1u;
  check_equal(turbo_flow_msg_set_durable_identity(msg, &identity), SALTS_OK);
}

static void graph_max_inflight(char *out, size_t capacity, uint32_t max_inflight) {
  char replacement[64];
  int n = snprintf(replacement, sizeof(replacement), "max_inflight %u", max_inflight);
  check_true(n > 0 && (size_t)n < sizeof(replacement));
  replace_once(out, capacity, graph_text, "max_inflight 1", replacement);
}

spec("generation materializer preflight") {
  it("allows an exact provider-private operation resource without a Graph primitive") {
    static const char graph[] =
        "source input\n"
        "stage decide operation fixture.private resource rules.private\n"
        "stage main {\n"
        "  input -> decide\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    flow_test_operation_t operation =
        flow_test_operation_init("fixture.private", materializer_noop_stage, NULL);

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    operation.provider.resource_name = "rules.private";
    check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
    check_null(turbo_flow_find_primitive(flow, "rules.private"));
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    turbo_flow_destroy(flow);

    flow = turbo_flow_create();
    operation = flow_test_operation_init("fixture.private", materializer_noop_stage, NULL);
    operation.provider.resource_name = "rules.other";
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
    check_not_equal(turbo_flow_compile(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("compiles one exact materializer binding before Graph start") {
    materializer_generation_test_t t;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_not_null(t.generation);
    check_null(t.cleanup);
    close_test(&t);
  }

  it("rejects a missing configured materializer plugin before materialization") {
    materializer_generation_test_t t;
    char graph[4096];
    replace_once(graph, sizeof(graph), graph_text, "fixture.materializer.operation",
                 "fixture.materializer.missing");
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                         yaml_good, graph, 1u, 0),
                SALTS_OK);
    check_equal(create_generation(&t), SALTS_ENOENT);
    check_equal(t.error.path, "$.generation.materializers[0].plugin");
    check_not_null(t.flow);
    check_null(t.generation);
    close_test(&t);
  }

  it("rejects a materializer whose schema wrapper does not match operation input") {
    materializer_generation_test_t t;
    check_equal(open_test(&t, FLOW_MATERIALIZER_MISMATCH, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_EPROTO);
    check_equal(strcmp(t.error.path, "$.generation.materializers[0].schema"), 0);
    check_null(t.generation);
    close_test(&t);
  }

  it("rejects a selected plugin that does not publish the canonical input schema") {
    materializer_generation_test_t t;
    char graph[4096];
    replace_once(graph, sizeof(graph), graph_text, "fixture.materializer.operation",
                 "fixture.materializer.unused");
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_UNUSED, yaml_good,
                                         graph, 1u, 0),
                SALTS_OK);
    check_equal(create_generation(&t), SALTS_ENOENT);
    check_equal(t.error.path, "$.generation.materializers[0].plugin");
    check_not_null(t.flow);
    check_null(t.generation);
    close_test(&t);
  }

  it("shares one schema-level materializer across multiple operation consumers") {
    materializer_generation_test_t t;
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_TWO_NAMES,
                                         FLOW_MATERIALIZER_OPERATION,
                                         yaml_two_consumers, graph_two_consumers,
                                         2u, 1), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_not_null(t.generation);
    check_null(t.cleanup);
    close_test(&t);
  }

  it("materializes durable canonical bytes at the typed-operation boundary") {
    static const char intake_graph[] =
        "source raw\n"
        "buffer intake provider flow.durable.memory resource intake_store\n"
        "stage forward adapter forward\n"
        "stage main {\n"
        "  raw -> intake -> forward\n"
        "}\n";
    materializer_generation_test_t t;
    turbo_flow_t *flow;
    turbo_flow_t *intake = turbo_flow_create();
    turbo_flow_msg_t msg;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_memory_config_t memory = turbo_flow_inbox_memory_config_default();
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_durable_buffer_binding_config_t config =
        TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_adapter_ops_t forwarding = {0};
    check_not_null(intake);
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    flow = turbo_flow_plugin_generation_flow(t.generation);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    /* Only the real buffer claim grants materialization provenance. Forward its
     * borrowed message to the generation while the claim and both Flows are live. */
    check_equal(turbo_flow_parse_string(intake, intake_graph, sizeof(intake_graph) - 1u), SALTS_OK);
    forwarding.consume = materializer_publish_claim;
    check_equal(turbo_flow_register_adapter(intake, "forward", &forwarding, flow), SALTS_OK);
    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    config.resource_name = "intake_store";
    config.inbox = &inbox;
    check_equal(turbo_flow_durable_buffer_bind(intake, &config, &binding), SALTS_OK);
    check_equal(turbo_flow_compile(intake), SALTS_OK);
    check_equal(turbo_flow_start(intake), SALTS_OK);

    /* Four bytes are required by the fixture materializer; choose a value whose
     * little-endian int representation remains defined when the fixture doubles it. */
    materializer_message(&msg, "!!!!", "cmeta.int.data", 1u);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    materializer_durable_identity(&msg);
    check_equal(turbo_flow_publish(intake, "raw", &msg), SALTS_OK);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_durable_buffer_quiesce(binding), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_drain(binding, 1000u), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.completed, 1u);
    check_equal(snapshot.failed, 0u);
    check_equal(turbo_flow_stop(intake), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(binding), SALTS_OK);
    turbo_flow_destroy(intake);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    close_test(&t);
  }

  it("rejects public durable identity without internal claim provenance") {
    materializer_generation_test_t t;
    turbo_flow_t *flow;
    turbo_flow_msg_t msg;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    flow = turbo_flow_plugin_generation_flow(t.generation);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    materializer_message(&msg, "ABCD", "cmeta.int.data", 1u);
    materializer_durable_identity(&msg);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    check_equal(turbo_flow_publish(flow, "input", &msg), SALTS_ENOTSUP);

    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    close_test(&t);
  }

  it("rejects non-durable raw bytes instead of bypassing the durable boundary") {
    materializer_generation_test_t t;
    turbo_flow_t *flow;
    turbo_flow_msg_t msg;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    flow = turbo_flow_plugin_generation_flow(t.generation);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    materializer_message(&msg, "ABCD", "cmeta.int.data", 1u);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    check_equal(turbo_flow_publish(flow, "input", &msg), SALTS_ENOTSUP);

    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    close_test(&t);
  }

  it("materializes canonical payload bytes into an exact typed projection") {
    materializer_generation_test_t t;
    turbo_flow_msg_t msg;
    const turbo_flow_plugin_materializer_binding_t *binding = NULL;
    const turbo_flow_data_schema_t *schema = NULL;
    const cmeta_data_desc *data = NULL;
    const void *value;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materializer_count(t.generation), (size_t)1);
    check_equal(turbo_flow_plugin_generation_materializer_at(t.generation, 0u, &binding), SALTS_OK);
    check_not_null(binding);

    materializer_message(&msg, "ABCD", "cmeta.int.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_OK);
    value = turbo_flow_msg_projection(&msg, &schema);
    data = turbo_flow_msg_projection_data(&msg);
    check_not_null(value);
    check_not_null(schema);
    check_not_null(data);
    check_equal(memcmp(value, "ABCD", sizeof(int)), 0);
    check_equal(strcmp(schema->schema_name, "cmeta.int.data"), 0);
    check_true(cmeta_type_equal(data->storage_type, CMETA_TYPEOF(int)));
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_EBUSY);

    turbo_flow_msg_cleanup(&msg);
    close_test(&t);
  }

  it("enforces bounded projection capacity across message clones and recovers quota") {
    materializer_generation_test_t t;
    turbo_flow_msg_t msg, clone, second;
    const turbo_flow_plugin_materializer_binding_t *binding = NULL;
    char yaml[4096];
    graph_max_inflight(yaml, sizeof(yaml), 2u);
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                         yaml_good, yaml, 1u, 0),
                SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materializer_at(t.generation, 0u, &binding), SALTS_OK);

    materializer_message(&msg, "ABCD", "cmeta.int.data", 1u);
    turbo_flow_msg_init(&clone);
    turbo_flow_msg_init(&second);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&clone, &msg), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&second, &msg), SALTS_ENOSPC);
    turbo_flow_msg_cleanup(&clone);
    check_equal(turbo_flow_msg_clone(&second, &msg), SALTS_OK);

    turbo_flow_msg_cleanup(&second);
    turbo_flow_msg_cleanup(&msg);
    close_test(&t);
  }

  it("rejects descriptor mismatch oversized payload and callback failure without leaking quota") {
    materializer_generation_test_t t;
    turbo_flow_msg_t msg;
    const turbo_flow_plugin_materializer_binding_t *binding = NULL;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materializer_at(t.generation, 0u, &binding), SALTS_OK);

    materializer_message(&msg, "ABCD", "other.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_EPROTO);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    turbo_flow_msg_cleanup(&msg);

    materializer_message(&msg, "ABCDE", "cmeta.int.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_ENOSPC);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    turbo_flow_msg_cleanup(&msg);

    materializer_message(&msg, "ABC", "cmeta.int.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_EINVAL);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    turbo_flow_msg_cleanup(&msg);

    materializer_message(&msg, "ABCD", "cmeta.int.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_OK);
    turbo_flow_msg_cleanup(&msg);
    close_test(&t);
  }

  it("blocks generation retirement while a materialized projection is alive") {
    materializer_generation_test_t t;
    turbo_flow_msg_t msg;
    const turbo_flow_plugin_materializer_binding_t *binding = NULL;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materializer_at(t.generation, 0u, &binding), SALTS_OK);
    materializer_message(&msg, "ABCD", "cmeta.int.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &msg, &t.error), SALTS_OK);

    check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0u, &t.error), SALTS_EBUSY);
    check_not_null(t.generation);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0u, &t.error), SALTS_OK);
    t.generation = NULL;
    close_test(&t);
  }

  it("rejects invalid materializer handle lookups") {
    materializer_generation_test_t t;
    const turbo_flow_plugin_materializer_binding_t *binding =
        (const turbo_flow_plugin_materializer_binding_t *)(uintptr_t)1u;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materializer_at(t.generation, 1u, &binding),
                SALTS_EINVAL);
    check_null(binding);
    check_equal(turbo_flow_plugin_generation_materializer_at(NULL, 0u, &binding),
                SALTS_EINVAL);
    close_test(&t);
  }

  it("preserves typed-operation generations when no materializer is selected") {
    materializer_generation_test_t t;
    char graph[4096];
    replace_once(graph, sizeof(graph), graph_text,
                 "  materializer { plugin \"fixture.materializer.operation\" }\n", "");
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                         yaml_good, graph, 1u, 0),
                SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_not_null(t.generation);
    check_equal(turbo_flow_plugin_generation_materializer_count(t.generation), (size_t)0u);
    close_test(&t);
  }

  it("rejects obsolete YAML materializer selection instead of falling back") {
    materializer_generation_test_t t;
    char yaml[4096];
    (void)fmt(yaml, sizeof(yaml), "{}{}", yaml_good,
              "materializer_bindings:\n"
              "  - plugin: fixture.materializer.operation\n"
              "    schema: cmeta.int.data\n"
              "    schema_version: 1\n"
              "    encoding: opaque\n");
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml), SALTS_OK);
    check_equal(create_generation(&t), SALTS_ENOTSUP);
    check_equal(t.error.path, "$.materializer_bindings");
    check_not_null(t.flow);
    check_null(t.generation);
    check_null(t.cleanup);
    close_test(&t);
  }

  it("validates optional materializer records through the generated MessagePlan") {
    static const char *const invalid[] = {
        "  materializer {}\n", "  materializer { plugin 3 }\n", "  materializer { plugin \"\" }\n",
        "  materializer { plugin \"fixture.materializer.operation\", schema \"private\" }\n",
        "  unexpected true\n"};
    for (size_t i = 0u; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
      materializer_generation_test_t t;
      char graph[4096];
      replace_once(graph, sizeof(graph), graph_text,
                   "  materializer { plugin \"fixture.materializer.operation\" }\n", invalid[i]);
      check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                           yaml_good, graph, 1u, 0),
                  SALTS_OK);
      check_equal(create_generation(&t), SALTS_EPROTO);
      check_contains(t.error.path, "$.stages.calculate");
      check_not_null(t.flow);
      check_null(t.generation);
      check_null(t.cleanup);
      close_test(&t);
    }
  }

  it("rejects a materializer selection without a bound typed-operation consumer") {
    static const char orphan[] = "stage orphan operation fixture.unbound {\n"
                                 "  materializer { plugin \"fixture.materializer.operation\" }\n"
                                 "}\n"
                                 "stage main {\n";
    materializer_generation_test_t t;
    char graph[4096];
    replace_once(graph, sizeof(graph), graph_text, "stage main {\n", orphan);
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                         yaml_good, graph, 1u, 0),
                SALTS_OK);
    check_equal(create_generation(&t), SALTS_ENOTSUP);
    check_equal(t.error.path, "$.stages.orphan.materializer");
    check_not_null(t.flow);
    check_null(t.generation);
    close_test(&t);
  }

  it("bounds the selected plugin identity before catalog admission") {
    for (size_t length = TURBO_FLOW_PLUGIN_ID_MAX; length <= TURBO_FLOW_PLUGIN_ID_MAX + 1u;
         ++length) {
      materializer_generation_test_t t;
      char plugin[TURBO_FLOW_PLUGIN_ID_MAX + 2u];
      char graph[4096];
      memset(plugin, 'a', length);
      plugin[length] = '\0';
      replace_once(graph, sizeof(graph), graph_text, "fixture.materializer.operation", plugin);
      check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                           yaml_good, graph, 1u, 0),
                  SALTS_OK);
      check_equal(create_generation(&t),
                  length == TURBO_FLOW_PLUGIN_ID_MAX ? SALTS_ENOENT : SALTS_EPROTO);
      check_not_null(t.flow);
      check_null(t.generation);
      check_null(t.cleanup);
      close_test(&t);
    }
  }

  it("requires consistent selection for stages sharing one operation session") {
    static const char shared_graph[] =
        "source input\n"
        "stage first operation fixture.double {\n" OPERATION_POLICY
        "  materializer { plugin \"fixture.materializer.operation\" }\n"
        "}\n"
        "stage second operation fixture.double {\n" OPERATION_POLICY
        "  materializer { plugin \"fixture.materializer.operation\" }\n"
        "}\n"
        "stage main {\n  input -> first -> second\n}\n";
    static const char conflicting[] = "stage first operation fixture.double {\n" OPERATION_POLICY;
    for (unsigned conflict = 0u; conflict < 4u; ++conflict) {
      materializer_generation_test_t t;
      char graph[4096];
      if (conflict == 1u)
        replace_once(graph, sizeof(graph), shared_graph,
                     "stage first operation fixture.double {\n" OPERATION_POLICY
                     "  materializer { plugin \"fixture.materializer.operation\" }\n",
                     conflicting);
      else if (conflict == 2u)
        replace_once(graph, sizeof(graph), shared_graph,
                     "stage second operation fixture.double {\n" OPERATION_POLICY
                     "  materializer { plugin \"fixture.materializer.operation\" }\n",
                     "stage second operation fixture.double {\n" OPERATION_POLICY);
      else if (conflict == 3u)
        replace_once(graph, sizeof(graph), shared_graph, "fixture.materializer.operation",
                     "fixture.materializer.other");
      else (void)fmt(graph, sizeof(graph), "{}", shared_graph);
      check_equal(open_test_with_operation(&t, FLOW_OPERATION_OK, FLOW_MATERIALIZER_OPERATION,
                                           yaml_good, graph, 1u, 0),
                  SALTS_OK);
      check_equal(create_generation(&t), conflict ? SALTS_EPROTO : SALTS_OK);
      if (conflict) {
        check_equal(t.error.path, "$.stages.second.materializer");
        check_not_null(t.flow);
        check_null(t.generation);
      } else {
        check_equal(turbo_flow_plugin_generation_materializer_count(t.generation), (size_t)1u);
      }
      close_test(&t);
    }
  }

  it("reserves projection capacity only for consumers selecting this materializer") {
    materializer_generation_test_t t;
    const turbo_flow_plugin_materializer_binding_t *binding = NULL;
    turbo_flow_msg_t first, second;
    char graph[4096];
    replace_once(graph, sizeof(graph), graph_two_consumers,
                 "stage decide operation fixture.other {\n" OPERATION_POLICY
                 "  materializer { plugin \"fixture.materializer.operation\" }\n",
                 "stage decide operation fixture.other {\n" OPERATION_POLICY);
    check_equal(open_test_with_operation(&t, FLOW_OPERATION_TWO_NAMES, FLOW_MATERIALIZER_OPERATION,
                                         yaml_two_consumers, graph, 2u, 1),
                SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materializer_count(t.generation), (size_t)1u);
    check_equal(turbo_flow_plugin_generation_materializer_at(t.generation, 0u, &binding), SALTS_OK);
    materializer_message(&first, "ABCD", "cmeta.int.data", 1u);
    materializer_message(&second, "EFGH", "cmeta.int.data", 1u);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &first, &t.error), SALTS_OK);
    check_equal(turbo_flow_plugin_materializer_materialize(binding, &second, &t.error),
                SALTS_ENOSPC);
    turbo_flow_msg_cleanup(&first);
    turbo_flow_msg_cleanup(&second);
    close_test(&t);
  }
}
