#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_operation.h"
#include "turbo_flow_projection.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tinytest.h>

static const char graph_text[] =
    "source input\n"
    "stage calculate operation fixture.double\n"
    "stage main {\n"
    "  input -> calculate\n"
    "}\n";

typedef struct runtime_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_plugin_result_domain_t *domain;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_plugin_generation_t *generation;
  turbo_flow_plugin_generation_t *cleanup;
  turbo_flow_t *flow;
  turbo_flow_config_error_t error;
} runtime_t;

static int runtime_open(runtime_t *t, const char *materializer_path, size_t capacity) {
  turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_operation_descriptor_t operation = {0};
  char yaml[2048];
  int written;
  int rc;
  memset(t, 0, sizeof(*t));
  t->error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  hc.module_capacity = 2u;
  hc.schema_capacity = 4u;
  hc.operation_capacity = 2u;
  hc.materializer_capacity = 2u;
  rc = turbo_flow_plugin_host_create(&hc, &t->host, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(t->host, FLOW_OPERATION_OK, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(t->host, materializer_path, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(t->host, &t->snapshot, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_result_domain_create(t->snapshot, 1u, &t->domain, &pe);
  if (rc != SALTS_OK) return rc;
  written = snprintf(
      yaml, sizeof(yaml),
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
      "    capacity: %zu\n"
      "adapters: {}\n",
      capacity);
  if (written <= 0 || (size_t)written >= sizeof(yaml)) return SALTS_ERANGE;
  rc = turbo_flow_config_resolve_yaml(yaml, (size_t)written, &t->resolved, &t->error);
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
  rc = turbo_flow_parse_string(t->flow, graph_text, sizeof(graph_text) - 1u);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_generation_create(t->snapshot, t->resolved, &t->flow, &gc,
                                           t->domain, &t->generation, &t->cleanup, &t->error);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_plugin_generation_materializer_count(t->generation) == 1u
             ? SALTS_OK
             : SALTS_EPROTO;
}

static int runtime_close(runtime_t *t) {
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
  int rc;
  if (t->cleanup) {
    rc = turbo_flow_plugin_generation_destroy(t->cleanup, 0u, &ce);
    if (rc != SALTS_OK) return rc;
    t->cleanup = NULL;
  }
  if (t->generation) {
    rc = turbo_flow_plugin_generation_destroy(t->generation, 0u, &ce);
    if (rc != SALTS_OK) return rc;
    t->generation = NULL;
  }
  if (t->flow) {
    turbo_flow_destroy(t->flow);
    t->flow = NULL;
  }
  if (t->domain) {
    rc = turbo_flow_plugin_result_domain_destroy(t->domain, &pe);
    if (rc != SALTS_OK) return rc;
    t->domain = NULL;
  }
  if (t->resolved) {
    turbo_flow_resolved_config_destroy(t->resolved);
    t->resolved = NULL;
  }
  if (t->snapshot) {
    turbo_flow_plugin_catalog_snapshot_destroy(t->snapshot);
    t->snapshot = NULL;
  }
  if (t->host) {
    rc = turbo_flow_plugin_host_destroy(t->host, 0u, &pe);
    if (rc != SALTS_OK) return rc;
    t->host = NULL;
  }
  return SALTS_OK;
}

static int message_prepare(turbo_flow_msg_t *msg, const void *payload, size_t payload_size,
                           const char *schema_name, const char *type_name,
                           uint32_t schema_version, turbo_flow_data_encoding_t encoding) {
  turbo_flow_content_descriptor_t content = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  int rc;
  turbo_flow_msg_init(msg);
  msg->owned_payload = tstr_from_v(vstr_from_buf(payload, payload_size));
  if (!msg->owned_payload) return SALTS_ENOMEM;
  msg->payload = tstr_to_v(msg->owned_payload);
  rc = turbo_flow_content_descriptor_init(&content, TURBO_FLOW_DOMAIN_DATA,
                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC, encoding,
                                          "application/octet-stream", "materializer-runtime");
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(&content, schema_name, type_name,
                                                    schema_version);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_msg_copy_content_descriptor(msg, &content);
}

static int message_prepare_int(turbo_flow_msg_t *msg, int value) {
  return message_prepare(msg, &value, sizeof(value), "cmeta.int.data", "Integer", 1u,
                         TURBO_FLOW_DATA_ENCODING_OPAQUE);
}

spec("compiled materializer runtime") {
  it("materializes canonical bytes into a message-owned exact typed projection") {
    runtime_t t;
    turbo_flow_msg_t message;
    const turbo_flow_data_schema_t *schema = NULL;
    const int encoded = 42;
    const int *native;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 2u), SALTS_OK);
    check_equal(message_prepare_int(&message, encoded), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_OK);
    native = (const int *)turbo_flow_msg_projection(&message, &schema);
    check_not_null(native);
    check_equal(*native, encoded);
    check_not_null(schema);
    check_equal(strcmp(schema->schema_name, "cmeta.int.data"), 0);
    check_true(turbo_flow_msg_projection_data(&message) != NULL);
    check_equal(message.payload.len, sizeof(encoded));
    check_equal(*(const int *)message.payload.data, encoded);
    turbo_flow_msg_cleanup(&message);
    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("rejects wrong schema version encoding and encoded-byte overflow before binding") {
    runtime_t t;
    turbo_flow_msg_t message;
    uint64_t wide = UINT64_C(0x0102030405060708);
    int encoded = 7;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 2u), SALTS_OK);

    check_equal(message_prepare(&message, &encoded, sizeof(encoded), "fixture.other", "Integer", 1u,
                                TURBO_FLOW_DATA_ENCODING_OPAQUE), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_EPROTO);
    check_null(turbo_flow_msg_projection(&message, NULL));
    turbo_flow_msg_cleanup(&message);

    check_equal(message_prepare(&message, &encoded, sizeof(encoded), "cmeta.int.data", "Integer", 2u,
                                TURBO_FLOW_DATA_ENCODING_OPAQUE), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_EPROTO);
    turbo_flow_msg_cleanup(&message);

    check_equal(message_prepare(&message, &encoded, sizeof(encoded), "cmeta.int.data", "Integer", 1u,
                                TURBO_FLOW_DATA_ENCODING_JSON), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_EPROTO);
    turbo_flow_msg_cleanup(&message);

    check_equal(message_prepare(&message, &wide, sizeof(wide), "cmeta.int.data", "Integer", 1u,
                                TURBO_FLOW_DATA_ENCODING_OPAQUE), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_ENOSPC);
    check_equal(strcmp(t.error.path, "$.materializer_bindings[0].payload"), 0);
    turbo_flow_msg_cleanup(&message);

    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("defines encoded payload 0/1/N/N+1 boundaries") {
    runtime_t t;
    turbo_flow_msg_t message;
    turbo_flow_content_descriptor_t content = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    unsigned char one = 0u;
    unsigned char n_plus_one[sizeof(int) + 1u] = {0};
    int exact = 17;

    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 2u), SALTS_OK);

    turbo_flow_msg_init(&message);
    check_equal(turbo_flow_content_descriptor_init(
                    &content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                    TURBO_FLOW_DATA_ENCODING_OPAQUE, "application/octet-stream",
                    "materializer-runtime"),
                SALTS_OK);
    check_equal(turbo_flow_content_descriptor_declare_schema(
                    &content, "cmeta.int.data", "Integer", 1u),
                SALTS_OK);
    check_equal(turbo_flow_msg_copy_content_descriptor(&message, &content), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(
                    t.generation, 0u, &message, &t.error),
                SALTS_EINVAL);
    check_null(turbo_flow_msg_projection(&message, NULL));
    turbo_flow_msg_cleanup(&message);

    check_equal(message_prepare(&message, &one, 1u, "cmeta.int.data", "Integer", 1u,
                                TURBO_FLOW_DATA_ENCODING_OPAQUE), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(
                    t.generation, 0u, &message, &t.error),
                SALTS_EINVAL);
    check_null(turbo_flow_msg_projection(&message, NULL));
    turbo_flow_msg_cleanup(&message);

    check_equal(message_prepare(&message, &exact, sizeof(exact), "cmeta.int.data", "Integer", 1u,
                                TURBO_FLOW_DATA_ENCODING_OPAQUE), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(
                    t.generation, 0u, &message, &t.error),
                SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), exact);
    turbo_flow_msg_cleanup(&message);

    check_equal(message_prepare(&message, n_plus_one, sizeof(n_plus_one),
                                "cmeta.int.data", "Integer", 1u,
                                TURBO_FLOW_DATA_ENCODING_OPAQUE), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(
                    t.generation, 0u, &message, &t.error),
                SALTS_ENOSPC);
    check_equal(strcmp(t.error.path, "$.materializer_bindings[0].payload"), 0);
    check_null(turbo_flow_msg_projection(&message, NULL));
    turbo_flow_msg_cleanup(&message);

    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("enforces configured N/N+1 projection capacity and releases quota on cleanup") {
    runtime_t t;
    turbo_flow_msg_t a, b, c;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 2u), SALTS_OK);
    check_equal(message_prepare_int(&a, 1), SALTS_OK);
    check_equal(message_prepare_int(&b, 2), SALTS_OK);
    check_equal(message_prepare_int(&c, 3), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &a, &t.error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &b, &t.error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &c, &t.error),
                SALTS_ENOSPC);
    check_equal(strcmp(t.error.path, "$.materializer_bindings[0].capacity"), 0);
    turbo_flow_msg_cleanup(&a);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &c, &t.error),
                SALTS_OK);
    turbo_flow_msg_cleanup(&b);
    turbo_flow_msg_cleanup(&c);
    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("charges clones against capacity and blocks generation retirement while projections live") {
    runtime_t t;
    turbo_flow_msg_t source, clone, later;
    turbo_flow_config_error_t destroy_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 2u), SALTS_OK);
    check_equal(message_prepare_int(&source, 11), SALTS_OK);
    turbo_flow_msg_init(&clone);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &source, &t.error),
                SALTS_OK);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0u, &destroy_error), SALTS_EBUSY);
    check_equal(strcmp(destroy_error.path, "$.generation.materializers"), 0);

    check_equal(message_prepare_int(&later, 12), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &later, &t.error),
                SALTS_EBUSY);
    turbo_flow_msg_cleanup(&later);

    turbo_flow_msg_cleanup(&clone);
    turbo_flow_msg_cleanup(&source);
    check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0u, &destroy_error), SALTS_OK);
    t.generation = NULL;
    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("rejects clone when the configured projection capacity is already full") {
    runtime_t t;
    turbo_flow_msg_t source, clone;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 1u), SALTS_OK);
    check_equal(message_prepare_int(&source, 21), SALTS_OK);
    turbo_flow_msg_init(&clone);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &source, &t.error),
                SALTS_OK);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_ENOSPC);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_msg_cleanup(&source);
    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("releases quota after a materializer callback failure") {
    runtime_t t;
    turbo_flow_msg_t first, second;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_CALLBACK_FAIL, 1u), SALTS_OK);
    check_equal(message_prepare_int(&first, 31), SALTS_OK);
    check_equal(message_prepare_int(&second, 32), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &first, &t.error),
                SALTS_EIO);
    check_null(turbo_flow_msg_projection(&first, NULL));
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &second, &t.error),
                SALTS_EIO);
    check_null(turbo_flow_msg_projection(&second, NULL));
    turbo_flow_msg_cleanup(&first);
    turbo_flow_msg_cleanup(&second);
    check_equal(runtime_close(&t), SALTS_OK);
  }

  it("rejects invalid binding indexes and re-materialization") {
    runtime_t t;
    turbo_flow_msg_t message;
    check_equal(runtime_open(&t, FLOW_MATERIALIZER_OPERATION, 2u), SALTS_OK);
    check_equal(message_prepare_int(&message, 41), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 1u, &message, &t.error),
                SALTS_ENOENT);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_materialize_at(t.generation, 0u, &message, &t.error),
                SALTS_EALREADY);
    turbo_flow_msg_cleanup(&message);
    check_equal(runtime_close(&t), SALTS_OK);
  }
}
