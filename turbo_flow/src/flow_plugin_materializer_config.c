#include "flow_plugin_operation_internal.h"
#include "flow_provider_config_internal.h"
#include "operation_stage_config_native.h"

#include <fmt.h>
#include <string.h>

static int materializer_config_error(turbo_flow_config_error_t *error, int status,
                                     const char *stage, const char *field, const char *message) {
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  error->status = status;
  (void)fmt(error->path, sizeof(error->path), "$.stages.{}{}{}", stage, field ? "." : "",
            field ? field : "");
  (void)fmt(error->message, sizeof(error->message), "{}", message);
  return status;
}

static int materializer_stage_selection(const flow_stage_plan_impl_t *stage, char *plugin,
                                        size_t capacity, turbo_flow_config_error_t *error) {
  flow_provider_typed_config_t config = {0};
  DataBindMessagePlanDiagnostic diagnostic = DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  int rc, cleanup;
  plugin[0] = '\0';
  if (vec_size(&stage->provider_config_literals) == 0u) return SALTS_OK;
  rc =
      flow_provider_typed_config_bind(stage, FlowOperationStageConfig_codec_create,
                                      OperationStageConfig_native_artifact(), &config, &diagnostic);
  if (rc != SALTS_OK)
    return materializer_config_error(
        error, rc, stage->name, diagnostic.schema_field[0] ? diagnostic.schema_field : NULL,
        diagnostic.message[0] ? diagnostic.message : "operation stage config binding failed");
  if (config.value_bytes != sizeof(OperationStageConfig_t)) {
    rc = SALTS_EPROTO;
  } else {
    const OperationStageConfig_t *typed = config.value;
    if ((typed->_presence[OperationStageConfig_OPTIONAL_materializer / 8u] &
         (1u << (OperationStageConfig_OPTIONAL_materializer % 8u))) != 0u) {
      size_t length = typed->materializer.plugin ? tstr_len(typed->materializer.plugin) : 0u;
      if (!length || length >= capacity || memchr(typed->materializer.plugin, '\0', length))
        rc = SALTS_EPROTO;
      else memcpy(plugin, typed->materializer.plugin, length + 1u);
    }
  }
  cleanup = flow_provider_typed_config_destroy(&config);
  if (rc == SALTS_OK) rc = cleanup;
  if (rc != SALTS_OK)
    return materializer_config_error(error, rc, stage->name, "materializer",
                                     "materializer selection or native config cleanup failed");
  return SALTS_OK;
}

static int same_name(const char *left, const char *right) {
  return left && right ? strcmp(left, right) == 0 : left == right;
}

int flow_plugin_materializer_configure(const turbo_flow_t *flow, vec_t *operations,
                                       turbo_flow_config_error_t *error) {
  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage = vec_at_const(&flow->stages, i);
    flow_plugin_operation_binding_t *operation = NULL;
    char plugin[TURBO_FLOW_PLUGIN_ID_MAX + 1u];
    int rc;
    if (!stage->operation_name || stage->adapter_name || stage->is_source || stage->is_buffer)
      continue;
    for (size_t j = 0u; j < vec_size(operations); ++j) {
      flow_plugin_operation_binding_t *candidate = vec_at(operations, j);
      if (same_name(stage->operation_name, candidate->request.operation_name) &&
          same_name(stage->resource_name, candidate->request.resource_name)) {
        operation = candidate;
        break;
      }
    }
    if (!operation) {
      if (vec_size(&stage->provider_config_literals) != 0u)
        return materializer_config_error(error, SALTS_ENOTSUP, stage->name, NULL,
                                         "stage config requires a bound typed operation");
      continue;
    }
    rc = materializer_stage_selection(stage, plugin, sizeof(plugin), error);
    if (rc != SALTS_OK) return rc;
    /* The existing operation+resource binding owns one shared session/budget. */
    if (operation->materializer_selection_seen &&
        strcmp(operation->materializer_plugin, plugin) != 0)
      return materializer_config_error(
          error, SALTS_EPROTO, stage->name, "materializer",
          "stages sharing one operation binding must select the same materializer");
    memcpy(operation->materializer_plugin, plugin, strlen(plugin) + 1u);
    operation->materializer_selection_seen = 1;
  }
  return SALTS_OK;
}
