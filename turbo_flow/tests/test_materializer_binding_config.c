#include "turbo_flow_resolved_config.h"

#include <stdio.h>
#include <string.h>
#include <tinytest.h>

static void rejected(const char *yaml, int status, const char *path) {
  turbo_flow_resolved_config_t *config = NULL;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &config, &error), status);
  check_null(config);
  check_equal(strcmp(error.path, path), 0);
}

spec("materializer binding config") {
  it("accepts and projects one explicit materializer binding") {
    static const char yaml[] =
        "version: 1\n"
        "materializer_bindings:\n"
        "  - plugin: fixture.materializer\n"
        "    schema: cmeta.int.data\n"
        "    schema_version: 1\n"
        "    encoding: opaque\n"
        "adapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_materializer_binding_view_t view =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    size_t count = 99u;
    size_t capacity = 0u;
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                SALTS_OK);
    check_equal(turbo_flow_resolved_config_materializer_binding_count(config, &count), SALTS_OK);
    check_equal(count, (size_t)1);
    check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 0u, &view), SALTS_OK);
    check_equal(strcmp(view.plugin, "fixture.materializer"), 0);
    check_equal(strcmp(view.schema, "cmeta.int.data"), 0);
    check_equal(view.schema_version, 1u);
    check_equal(view.encoding, TURBO_FLOW_CONFIG_MATERIALIZER_OPAQUE);
    check_equal(turbo_flow_resolved_config_materializer_binding_capacity(config, 0u, &capacity),
                SALTS_OK);
    check_equal(capacity, (size_t)TURBO_FLOW_CONFIG_MATERIALIZER_DEFAULT_CAPACITY);
    turbo_flow_resolved_config_destroy(config);
  }

  it("treats missing or empty materializer bindings as zero bindings") {
    static const char *const docs[] = {
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

  it("rejects malformed fields and unsupported encodings") {
    rejected("version: 1\nmaterializer_bindings: {}\nadapters: {}\n", SALTS_EINVAL,
             "$.materializer_bindings");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 1\n    encoding: opaque\n    surprise: true\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].surprise");
    rejected("version: 1\nmaterializer_bindings:\n  - schema: sample\n"
             "    schema_version: 1\n    encoding: opaque\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].plugin");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n"
             "    schema_version: 1\n    encoding: opaque\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].schema");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    encoding: opaque\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].schema_version");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 1\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].encoding");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 1\n    encoding: binary\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].encoding");
  }

  it("accepts explicit bounded capacity and rejects invalid capacity values") {
    static const char yaml[] =
        "version: 1\nmaterializer_bindings:\n"
        "  - plugin: fixture\n    schema: sample\n"
        "    schema_version: 1\n    encoding: opaque\n"
        "    capacity: 2\nadapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t capacity = 0u;
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                SALTS_OK);
    check_equal(turbo_flow_resolved_config_materializer_binding_capacity(config, 0u, &capacity),
                SALTS_OK);
    check_equal(capacity, (size_t)2);
    check_equal(turbo_flow_resolved_config_materializer_binding_capacity(NULL, 0u, &capacity),
                SALTS_EINVAL);
    check_equal(turbo_flow_resolved_config_materializer_binding_capacity(config, 0u, NULL),
                SALTS_EINVAL);
    check_equal(turbo_flow_resolved_config_materializer_binding_capacity(config, 1u, &capacity),
                SALTS_ENOENT);
    turbo_flow_resolved_config_destroy(config);

    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 1\n    encoding: opaque\n    capacity: 0\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].capacity");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 1\n    encoding: opaque\n    capacity: 1.5\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].capacity");
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 1\n    encoding: opaque\n    capacity: 1048577\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].capacity");
  }

  it("accepts every canonical encoding") {
    static const char *const names[] = {"tbe", "json", "csv", "xml", "utf8", "opaque"};
    for (size_t i = 0u; i < sizeof(names) / sizeof(names[0]); ++i) {
      char yaml[512];
      turbo_flow_resolved_config_t *config = NULL;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_resolved_materializer_binding_view_t view =
          TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
      int written = snprintf(yaml, sizeof(yaml),
                             "version: 1\nmaterializer_bindings:\n"
                             "  - plugin: fixture\n    schema: sample\n"
                             "    schema_version: 1\n    encoding: %s\nadapters: {}\n",
                             names[i]);
      check_true(written > 0 && (size_t)written < sizeof(yaml));
      check_equal(turbo_flow_config_resolve_yaml(yaml, (size_t)written, &config, &error), SALTS_OK);
      check_equal(turbo_flow_resolved_config_materializer_binding_at(config, 0u, &view), SALTS_OK);
      check_equal(view.encoding, (turbo_flow_config_materializer_encoding_t)i);
      turbo_flow_resolved_config_destroy(config);
    }
  }

  it("rejects duplicate schema version and encoding tuples") {
    static const char yaml[] =
        "version: 1\nmaterializer_bindings:\n"
        "  - plugin: fixture.one\n    schema: cmeta.int.data\n"
        "    schema_version: 1\n    encoding: opaque\n"
        "  - plugin: fixture.two\n    schema: cmeta.int.data\n"
        "    schema_version: 1\n    encoding: opaque\nadapters: {}\n";
    rejected(yaml, SALTS_EALREADY, "$.materializer_bindings[1].schema");
  }

  it("rejects invalid schema versions and invalid view arguments") {
    rejected("version: 1\nmaterializer_bindings:\n  - plugin: fixture\n    schema: sample\n"
             "    schema_version: 0\n    encoding: opaque\nadapters: {}\n",
             SALTS_EINVAL, "$.materializer_bindings[0].schema_version");
    {
      static const char yaml[] =
          "version: 1\nmaterializer_bindings:\n"
          "  - plugin: fixture\n    schema: sample\n"
          "    schema_version: 1\n    encoding: opaque\nadapters: {}\n";
      turbo_flow_resolved_config_t *config = NULL;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_resolved_materializer_binding_view_t view =
          TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
      size_t count = 0u;
      check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                  SALTS_OK);
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
    }
  }
}
