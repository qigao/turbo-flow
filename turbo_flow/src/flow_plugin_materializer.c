#include "flow_plugin_operation_internal.h"

#include <stdio.h>
#include <string.h>

static int materializer_error(turbo_flow_config_error_t *error, size_t index,
                              const char *field, int status, const char *message) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(path, sizeof(path), "$.materializer_bindings[%zu]%s%s", index,
                   field ? "." : "", field ? field : "");
    (void)snprintf(error->path, sizeof(error->path), "%s", path);
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "materializer binding failed");
  }
  return status;
}

static int same_nullable(const char *a, const char *b) {
  return (!a && !b) || (a && b && strcmp(a, b) == 0);
}

static int materializer_encoding(const char *encoding, turbo_flow_data_encoding_t *out) {
  if (!encoding || !out) return SALTS_EINVAL;
  if (strcmp(encoding, "tbe") == 0) *out = TURBO_FLOW_DATA_ENCODING_TBE;
  else if (strcmp(encoding, "json") == 0) *out = TURBO_FLOW_DATA_ENCODING_JSON;
  else if (strcmp(encoding, "csv") == 0) *out = TURBO_FLOW_DATA_ENCODING_CSV;
  else if (strcmp(encoding, "xml") == 0) *out = TURBO_FLOW_DATA_ENCODING_XML;
  else if (strcmp(encoding, "utf8") == 0) *out = TURBO_FLOW_DATA_ENCODING_UTF8;
  else if (strcmp(encoding, "opaque") == 0) *out = TURBO_FLOW_DATA_ENCODING_OPAQUE;
  else return SALTS_EINVAL;
  return SALTS_OK;
}

static flow_plugin_operation_binding_t *materializer_target(
    vec_t *bindings, const turbo_flow_resolved_materializer_binding_view_t *view) {
  if (!bindings || !view) return NULL;
  for (size_t i = 0u; i < vec_size(bindings); ++i) {
    flow_plugin_operation_binding_t *binding =
        (flow_plugin_operation_binding_t *)vec_at(bindings, i);
    if (binding && same_nullable(binding->request.operation_name, view->operation) &&
        same_nullable(binding->request.resource_name, view->resource))
      return binding;
  }
  return NULL;
}

int flow_plugin_materializers_prepare(turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                      const turbo_flow_resolved_config_t *resolved,
                                      vec_t *bindings, turbo_flow_config_error_t *error) {
  turbo_flow_plugin_materializer_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
  size_t count = 0u;
  int rc;

  if (!snapshot || !resolved || !bindings || !error || error->size < sizeof(*error))
    return materializer_error(error, 0u, NULL, SALTS_EINVAL,
                              "invalid materializer preflight arguments");

  rc = turbo_flow_resolved_config_materializer_binding_count(resolved, &count);
  if (rc != SALTS_OK)
    return materializer_error(error, 0u, "config", rc,
                              "materializer binding configuration is unavailable");
  if (count == 0u) return SALTS_OK;

  rc = turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog);
  if (rc != SALTS_OK)
    return materializer_error(error, 0u, "catalog", rc,
                              "materializer catalog is unavailable");

  for (size_t i = 0u; i < count; ++i) {
    turbo_flow_resolved_materializer_binding_view_t view =
        TURBO_FLOW_RESOLVED_MATERIALIZER_BINDING_VIEW_INIT;
    const turbo_flow_plugin_materializer_catalog_entry_v1_t *selected = NULL;
    flow_plugin_operation_binding_t *target;
    turbo_flow_data_encoding_t encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    int plugin_found = 0, schema_found = 0, version_found = 0;

    rc = turbo_flow_resolved_config_materializer_binding_at(resolved, i, &view);
    if (rc != SALTS_OK)
      return materializer_error(error, i, "config", rc,
                                "materializer binding projection failed");

    target = materializer_target(bindings, &view);
    if (!target)
      return materializer_error(error, i, "operation", SALTS_EPROTO,
                                "compiled operation binding is missing");

    rc = materializer_encoding(view.encoding, &encoding);
    if (rc != SALTS_OK)
      return materializer_error(error, i, "encoding", rc,
                                "materializer encoding is invalid");

    for (size_t j = 0u; j < catalog.count; ++j) {
      const turbo_flow_plugin_materializer_catalog_entry_v1_t *entry = &catalog.entries[j];
      if (!entry->plugin_id || strcmp(entry->plugin_id, view.plugin) != 0) continue;
      plugin_found = 1;
      if (!entry->materializer.schema.schema_name ||
          strcmp(entry->materializer.schema.schema_name, view.schema) != 0)
        continue;
      schema_found = 1;
      if (entry->materializer.schema.schema_version != view.schema_version) continue;
      version_found = 1;
      if (entry->materializer.schema.encoding != encoding) continue;
      selected = entry;
      break;
    }

    if (!selected)
      return materializer_error(error, i,
                                !plugin_found ? "plugin"
                                : !schema_found ? "schema"
                                : !version_found ? "schema_version"
                                                 : "encoding",
                                SALTS_EINVAL,
                                "configured materializer is not present in the catalog");

    if (target->has_materializer)
      return materializer_error(error, i, "operation", SALTS_EALREADY,
                                "operation already has a compiled materializer");

    if (view.max_encoded_bytes > selected->materializer.max_encoded_bytes)
      return materializer_error(error, i, "max_encoded_bytes", SALTS_ENOSPC,
                                "configured encoded bound exceeds provider capability");

    if (selected->materializer.native_bytes > target->request.limits.max_input_bytes)
      return materializer_error(error, i, "max_encoded_bytes", SALTS_ENOSPC,
                                "materialized native value exceeds operation input budget");

    target->materializer_native_schema = selected->materializer.schema;
    target->materializer_native_schema.encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    rc = turbo_flow_data_schema_match(
        &target->materializer_native_schema, selected->materializer.data,
        target->operation.input.projection, target->operation.input.data);
    if (rc != SALTS_OK)
      return materializer_error(error, i, "schema", rc,
                                "materialized native schema does not match operation input");

    target->materializer = selected->materializer;
    target->materializer_max_encoded_bytes = view.max_encoded_bytes;
    target->has_materializer = 1;
  }
  return SALTS_OK;
}

size_t flow_plugin_materializer_binding_count(const vec_t *bindings) {
  size_t count = 0u;
  if (!bindings) return 0u;
  for (size_t i = 0u; i < vec_size(bindings); ++i) {
    const flow_plugin_operation_binding_t *binding =
        (const flow_plugin_operation_binding_t *)vec_at_const(bindings, i);
    if (binding && binding->has_materializer) ++count;
  }
  return count;
}
