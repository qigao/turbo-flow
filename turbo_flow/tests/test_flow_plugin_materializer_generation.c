#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_operation.h"
#include "../src/flow_projection_owner_internal.h"
#include "../../tests/flow_operation_fixture.h"

#include <cmeta/type_select.h>
#include <stdlib.h>
#include <string.h>
#include <tinytest.h>

static int materializer_noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return SALTS_OK;
}

static const char yaml_good[] =
    "version: 1\n"
    "operation_bindings:\n"
    "  - operation: fixture.double\n"
    "    plugin: fixture.operation\n"
    "    version: 1\n"
    "    input_schema: cmeta.int.data\n"
    "    input_schema_version: 1\n"
    "    output_schema: cmeta.int.data\n"
    "    output_schema_version: 1\n"
    "    execution: inline\n"
    "    threading: thread_safe\n"
    "    cancellation: none\n"
    "    permissions: []\n"
    "    max_inflight: 1\n"
    "    max_input_bytes: 8\n"
    "    max_result_bytes: 8\n"
    "    max_retained_bytes: 32\n"
    "    max_steps: 2\n"
    "    deadline_ms: 0\n"
    "materializer_bindings:\n"
    "  - plugin: fixture.materializer.operation\n"
    "    schema: cmeta.int.data\n"
    "    schema_version: 1\n"
    "    encoding: opaque\n"
    "adapters: {}\n";

static const char graph_text[] =
    "source input\n"
    "stage calculate operation fixture.double\n"
    "stage main {\n"
    "  input -> calculate\n"
    "}\n";

static const char yaml_two_consumers[] =
    "version: 1\n"
    "operation_bindings:\n"
    "  - operation: fixture.double\n"
    "    plugin: fixture.operation\n"
    "    version: 1\n"
    "    input_schema: cmeta.int.data\n"
    "    input_schema_version: 1\n"
    "    output_schema: cmeta.int.data\n"
    "    output_schema_version: 1\n"
    "    execution: inline\n"
    "    threading: thread_safe\n"
    "    cancellation: none\n"
    "    permissions: []\n"
    "    max_inflight: 1\n"
    "    max_input_bytes: 8\n"
    "    max_result_bytes: 8\n"
    "    max_retained_bytes: 32\n"
    "    max_steps: 2\n"
    "    deadline_ms: 0\n"
    "  - operation: fixture.other\n"
    "    plugin: fixture.operation\n"
    "    version: 1\n"
    "    input_schema: cmeta.int.data\n"
    "    input_schema_version: 1\n"
    "    output_schema: cmeta.int.data\n"
    "    output_schema_version: 1\n"
    "    execution: inline\n"
    "    threading: thread_safe\n"
    "    cancellation: none\n"
    "    permissions: []\n"
    "    max_inflight: 1\n"
    "    max_input_bytes: 8\n"
    "    max_result_bytes: 8\n"
    "    max_retained_bytes: 32\n"
    "    max_steps: 2\n"
    "    deadline_ms: 0\n"
    "materializer_bindings:\n"
    "  - plugin: fixture.materializer.operation\n"
    "    schema: cmeta.int.data\n"
    "    schema_version: 1\n"
    "    encoding: opaque\n"
    "adapters: {}\n";

static const char graph_two_consumers[] =
    "source input\n"
    "stage calculate operation fixture.double\n"
    "stage decide operation fixture.other\n"
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
  turbo_flow_operation_descriptor_t operation = {0};
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
  operation.size = sizeof(operation);
  operation.name = "fixture.double";
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = "Message";
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = "Message";
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  operation.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  rc = turbo_flow_register_operation(t->flow, &operation);
  if (rc != SALTS_OK) return rc;
  if (register_other) {
    operation.name = "fixture.other";
    rc = turbo_flow_register_operation(t->flow, &operation);
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

static void yaml_max_inflight(char *out, size_t capacity, uint32_t max_inflight) {
  char replacement[64];
  int n = snprintf(replacement, sizeof(replacement), "max_inflight: %u", max_inflight);
  check_true(n > 0 && (size_t)n < sizeof(replacement));
  replace_once(out, capacity, yaml_good, "max_inflight: 1", replacement);
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
    char yaml[4096];
    replace_once(yaml, sizeof(yaml), yaml_good, "fixture.materializer.operation",
                 "fixture.materializer.missing");
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml), SALTS_OK);
    check_equal(create_generation(&t), SALTS_ENOENT);
    check_equal(strcmp(t.error.path, "$.materializer_bindings[0].plugin"), 0);
    check_null(t.generation);
    close_test(&t);
  }

  it("rejects a materializer whose schema wrapper does not match operation input") {
    materializer_generation_test_t t;
    check_equal(open_test(&t, FLOW_MATERIALIZER_MISMATCH, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_EPROTO);
    check_equal(strcmp(t.error.path, "$.materializer_bindings[0].schema"), 0);
    check_null(t.generation);
    close_test(&t);
  }

  it("rejects a configured materializer with no typed-operation consumer") {
    materializer_generation_test_t t;
    char yaml[4096];
    {
      char intermediate[4096];
      replace_once(intermediate, sizeof(intermediate), yaml_good,
                   "plugin: fixture.materializer.operation",
                   "plugin: fixture.materializer.unused");
      replace_once(yaml, sizeof(yaml), intermediate,
                   "    schema: cmeta.int.data\n",
                   "    schema: fixture.unused.data\n");
    }
    check_equal(open_test(&t, FLOW_MATERIALIZER_UNUSED, yaml), SALTS_OK);
    check_equal(create_generation(&t), SALTS_EPROTO);
    check_equal(strcmp(t.error.path, "$.materializer_bindings[0].schema"), 0);
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
    materializer_generation_test_t t;
    turbo_flow_t *flow;
    turbo_flow_msg_t msg;
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml_good), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    flow = turbo_flow_plugin_generation_flow(t.generation);
    check_not_null(flow);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    /* Four bytes are required by the fixture materializer; choose a value whose
     * little-endian int representation remains defined when the fixture doubles it. */
    materializer_message(&msg, "!!!!", "cmeta.int.data", 1u);
    materializer_durable_identity(&msg);
    check_equal(flow_msg_mark_durable_claim(&msg), SALTS_OK);
    check_null(turbo_flow_msg_projection(&msg, NULL));
    check_equal(turbo_flow_publish(flow, "input", &msg), SALTS_OK);

    turbo_flow_msg_cleanup(&msg);
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
    yaml_max_inflight(yaml, sizeof(yaml), 2u);
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml), SALTS_OK);
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

  it("preserves existing typed-operation generations when no materializer binding is configured") {
    materializer_generation_test_t t;
    char yaml[4096];
    const char *start = strstr(yaml_good, "materializer_bindings:");
    const char *end = strstr(yaml_good, "adapters: {}");
    size_t prefix;
    check_not_null(start);
    check_not_null(end);
    prefix = (size_t)(start - yaml_good);
    memcpy(yaml, yaml_good, prefix);
    strcpy(yaml + prefix, end);
    check_equal(open_test(&t, FLOW_MATERIALIZER_OPERATION, yaml), SALTS_OK);
    check_equal(create_generation(&t), SALTS_OK);
    check_not_null(t.generation);
    close_test(&t);
  }
}
