#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_operation.h"

#include <stdlib.h>
#include <string.h>
#include <tinytest.h>

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

static int open_test(materializer_generation_test_t *t, const char *materializer_path,
                     const char *yaml) {
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
  rc = turbo_flow_plugin_host_load(t->host, FLOW_OPERATION_OK, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(t->host, materializer_path, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(t->host, &t->snapshot, &pe);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_result_domain_create(t->snapshot, 1u, &t->domain, &pe);
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
  return turbo_flow_parse_string(t->flow, graph_text, sizeof(graph_text) - 1u);
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

spec("generation materializer preflight") {
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
