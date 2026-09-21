#include "tinytest.h"
#include "turbo_flow_resolved_config.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char operation_fields[] =
    "    operation: fixture.double\n"
    "    plugin: fixture.operation\n"
    "    version: 1\n"
    "    input_schema: cmeta.int.data\n"
    "    input_schema_version: 1\n"
    "    output_schema: cmeta.int.data\n"
    "    output_schema_version: 1\n"
    "    permissions: []\n"
    "    execution: inline\n"
    "    threading: thread_safe\n"
    "    cancellation: none\n"
    "    max_inflight: 1\n"
    "    max_input_bytes: 64\n"
    "    max_result_bytes: 64\n"
    "    max_retained_bytes: 64\n"
    "    max_steps: 8\n"
    "    deadline_ms: 0\n";

static const char materializer_fields[] =
    "    operation: fixture.double\n"
    "    plugin: fixture.materializer\n"
    "    schema: cmeta.int.data\n"
    "    schema_version: 1\n"
    "    encoding: json\n"
    "    max_encoded_bytes: 256\n";

static char *document(const char *materializer) {
  const char *a = "version: 1\noperation_bindings:\n  -\n";
  const char *b = "materializer_bindings:\n  -\n";
  const char *c = "adapters: {}\n";
  size_t n = strlen(a) + strlen(operation_fields) + strlen(b) + strlen(materializer) +
             strlen(c) + 1u;
  char *out = (char *)malloc(n);
  if (out) sprintf(out, "%s%s%s%s%s", a, operation_fields, b, materializer, c);
  return out;
}

static char *replace_once(const char *input, const char *from, const char *to) {
  const char *at = strstr(input, from);
  size_t n;
  char *out;
  if (!at) return NULL;
  n = strlen(input) - strlen(from) + strlen(to) + 1u;
  out = (char *)malloc(n);
  if (out)
    sprintf(out, "%.*s%s%s", (int)(at - input), input, to, at + strlen(from));
  return out;
}

static void rejected(const char *yaml, int status, const char *path) {
  turbo_flow_resolved_config_t *config =
      (turbo_flow_resolved_config_t *)(uintptr_t)1u;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &config, &error), status);
  check_null(config);
  check_equal(error.status, status);
  check_equal(error.path, path);
}

spec("materializer binding configuration") {
  it("projects one exact operation materializer binding") {
    char *yaml = document(materializer_fields);
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_materializer_binding_view_t view =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    size_t count = 0u;

    check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &config, &error), SALTS_OK);
    check_equal(turbo_flow_resolved_config_materializer_binding_count(config, &count), SALTS_OK);
    check_equal(count, (size_t)1);
    check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 0u, &view), SALTS_OK);
    check_equal(strcmp(view.operation, "fixture.double"), 0);
    check_null(view.resource);
    check_equal(strcmp(view.plugin, "fixture.materializer"), 0);
    check_equal(strcmp(view.schema, "cmeta.int.data"), 0);
    check_equal(view.schema_version, 1u);
    check_equal(strcmp(view.encoding, "json"), 0);
    check_equal(view.max_encoded_bytes, (size_t)256);

    turbo_flow_resolved_config_destroy(config);
    free(yaml);
  }

  it("allows absent and empty materializer binding lists") {
    const char *docs[] = {
        "version: 1\nadapters: {}\n",
        "version: 1\nmaterializer_bindings: []\nadapters: {}\n"};
    for (size_t i = 0u; i < sizeof(docs) / sizeof(docs[0]); ++i) {
      turbo_flow_resolved_config_t *config = NULL;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      size_t count = 99u;
      check_equal(turbo_flow_config_resolve_yaml(docs[i], strlen(docs[i]), &config, &error),
                  SALTS_OK);
      check_equal(turbo_flow_resolved_config_materializer_binding_count(config, &count), SALTS_OK);
      check_equal(count, (size_t)0);
      turbo_flow_resolved_config_destroy(config);
    }
  }

  it("rejects malformed lists fields encoding and bounds") {
    char *base = document(materializer_fields);
    char *yaml;
    rejected("version: 1\nmaterializer_bindings: null\nadapters: {}\n", SALTS_EINVAL,
             "$.materializer_bindings");
    rejected("version: 1\nmaterializer_bindings: {}\nadapters: {}\n", SALTS_EINVAL,
             "$.materializer_bindings");
    rejected("version: 1\nmaterializer_bindings: [text]\nadapters: {}\n", SALTS_EINVAL,
             "$.materializer_bindings[0]");

    yaml = replace_once(base, "    max_encoded_bytes: 256\n",
                       "    max_encoded_bytes: 256\n    surprise: true\n");
    rejected(yaml, SALTS_EINVAL, "$.materializer_bindings[0].surprise");
    free(yaml);

    yaml = replace_once(base, "    encoding: json\n", "    encoding: yaml\n");
    rejected(yaml, SALTS_EINVAL, "$.materializer_bindings[0].encoding");
    free(yaml);

    yaml = replace_once(base, "    max_encoded_bytes: 256\n", "    max_encoded_bytes: 0\n");
    rejected(yaml, SALTS_EINVAL, "$.materializer_bindings[0].max_encoded_bytes");
    free(yaml);

    yaml = replace_once(base, "    schema_version: 1\n", "    schema_version: 0\n");
    rejected(yaml, SALTS_EINVAL, "$.materializer_bindings[0].schema_version");
    free(yaml);
    free(base);
  }

  it("requires one exact target operation binding and matching input schema") {
    char *base = document(materializer_fields);
    char *yaml;

    yaml = replace_once(base, "operation: fixture.double", "operation: fixture.missing");
    rejected(yaml, SALTS_ENOENT, "$.materializer_bindings[0].operation");
    free(yaml);

    yaml = replace_once(base, "    schema: cmeta.int.data\n",
                       "    schema: operation.Other.data\n");
    rejected(yaml, SALTS_EPROTO, "$.materializer_bindings[0].schema");
    free(yaml);

    yaml = replace_once(base, "    schema_version: 1\n", "    schema_version: 2\n");
    rejected(yaml, SALTS_EPROTO, "$.materializer_bindings[0].schema");
    free(yaml);
    free(base);
  }

  it("uses operation and resource as the unique target identity") {
    const char *yaml =
        "version: 1\n"
        "channels:\n"
        "  rules:\n"
        "    kind: fixture.resource\n"
        "    config: {}\n"
        "operation_bindings:\n"
        "  -\n"
        "    operation: fixture.double\n"
        "    resource: rules\n"
        "    plugin: fixture.operation\n"
        "    version: 1\n"
        "    input_schema: cmeta.int.data\n"
        "    input_schema_version: 1\n"
        "    output_schema: cmeta.int.data\n"
        "    output_schema_version: 1\n"
        "    permissions: []\n"
        "    execution: inline\n"
        "    threading: thread_safe\n"
        "    cancellation: none\n"
        "    max_inflight: 1\n"
        "    max_input_bytes: 64\n"
        "    max_result_bytes: 64\n"
        "    max_retained_bytes: 64\n"
        "    max_steps: 8\n"
        "    deadline_ms: 0\n"
        "materializer_bindings:\n"
        "  -\n"
        "    operation: fixture.double\n"
        "    resource: rules\n"
        "    plugin: fixture.materializer\n"
        "    schema: cmeta.int.data\n"
        "    schema_version: 1\n"
        "    encoding: json\n"
        "    max_encoded_bytes: 256\n"
        "adapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_materializer_binding_view_t view =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;

    check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &config, &error), SALTS_OK);
    check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 0u, &view), SALTS_OK);
    check_equal(strcmp(view.resource, "rules"), 0);
    turbo_flow_resolved_config_destroy(config);
  }

  it("rejects duplicate materializers for one operation resource target") {
    char *base = document(materializer_fields);
    const char *second = strstr(base, "  -\n    operation: fixture.double\n");
    size_t n = strlen(base) + strlen(second) + 1u;
    char *duplicate = (char *)malloc(n);
    const char *adapters = strstr(base, "adapters: {}\n");
    size_t prefix = (size_t)(adapters - base);
    check_not_null(duplicate);
    sprintf(duplicate, "%.*s%s%s", (int)prefix, base,
            "  -\n"
            "    operation: fixture.double\n"
            "    plugin: fixture.materializer.second\n"
            "    schema: cmeta.int.data\n"
            "    schema_version: 1\n"
            "    encoding: json\n"
            "    max_encoded_bytes: 128\n",
            adapters);
    rejected(duplicate, SALTS_EALREADY, "$.materializer_bindings[1].operation");
    free(duplicate);
    free(base);
  }

  it("validates projection arguments and missing indices") {
    char *yaml = document(materializer_fields);
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_materializer_binding_view_t view =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    size_t count = 0u;

    check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &config, &error), SALTS_OK);
    check_equal(turbo_flow_resolved_config_materializer_binding_count(NULL, &count), SALTS_EINVAL);
    check_equal(turbo_flow_resolved_config_materializer_binding_count(config, NULL), SALTS_EINVAL);
    check_equal(turbo_flow_resolved_config_materializer_binding_at(NULL, 0u, &view), SALTS_EINVAL);
    check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 0u, NULL), SALTS_EINVAL);
    view.size = sizeof(view) - 1u;
    check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 0u, &view), SALTS_EINVAL);
    view = (turbo_flow_resolved_materializer_binding_view_t)
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 1u, &view), SALTS_ENOENT);

    turbo_flow_resolved_config_destroy(config);
    free(yaml);
  }
}
