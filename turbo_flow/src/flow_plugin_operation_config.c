#include "flow_plugin_operation_internal.h"
#include "flow_provider_config_internal.h"
#include "operation_stage_config_native.h"
#include "turbo_flow_stl_error_internal.h"

#include <fmt.h>
#include <string.h>

typedef struct operation_stage_selection_s {
  const flow_stage_plan_impl_t *stage;
  char plugin[TURBO_FLOW_PLUGIN_ID_MAX + 1u];
  char materializer[TURBO_FLOW_PLUGIN_ID_MAX + 1u];
  uint32_t version;
  turbo_flow_plugin_operation_limits_v3_t limits;
} operation_stage_selection_t;

static int config_error(turbo_flow_config_error_t *error, int status, const char *stage,
                        const char *field, const char *message) {
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  error->status = status;
  (void)fmt(error->path, sizeof(error->path), "$.stages.{}{}{}", stage, field ? "." : "",
            field ? field : "");
  (void)fmt(error->message, sizeof(error->message), "{}", message);
  return status;
}

static int copy_plugin(char *out, size_t capacity, const tstr value) {
  size_t length = value ? tstr_len(value) : 0u;
  if (!length || length >= capacity || memchr(value, '\0', length)) return SALTS_EPROTO;
  memcpy(out, value, length + 1u);
  return SALTS_OK;
}

static int stage_selection(const flow_stage_plan_impl_t *stage,
                           operation_stage_selection_t *selection,
                           turbo_flow_config_error_t *error) {
  flow_provider_typed_config_t config = {0};
  DataBindMessagePlanDiagnostic diagnostic = DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  int rc, cleanup;
  memset(selection, 0, sizeof(*selection));
  selection->stage = stage;
  turbo_flow_plugin_operation_limits_v3_init(&selection->limits);
  if (vec_size(&stage->provider_config_literals) == 0u) return SALTS_OK;
  rc =
      flow_provider_typed_config_bind(stage, FlowOperationStageConfig_codec_create,
                                      OperationStageConfig_native_artifact(), &config, &diagnostic);
  if (rc != SALTS_OK)
    return config_error(
        error, rc, stage->name, diagnostic.schema_field[0] ? diagnostic.schema_field : NULL,
        diagnostic.message[0] ? diagnostic.message : "operation stage config binding failed");
  if (config.value_bytes != sizeof(OperationStageConfig_t)) {
    rc = SALTS_EPROTO;
  } else {
    const OperationStageConfig_t *typed = config.value;
    if ((typed->_presence[OperationStageConfig_OPTIONAL_policy / 8u] &
         (1u << (OperationStageConfig_OPTIONAL_policy % 8u))) != 0u) {
      const OperationPolicy_t *policy = &typed->policy;
      rc = copy_plugin(selection->plugin, sizeof(selection->plugin), policy->plugin);
      selection->version = policy->version;
      selection->limits.max_inflight = policy->max_inflight;
      selection->limits.max_input_bytes = policy->max_input_bytes;
      selection->limits.max_result_bytes = policy->max_result_bytes;
      selection->limits.max_retained_bytes = policy->max_retained_bytes;
      selection->limits.max_steps = policy->max_steps;
    }
    if (rc == SALTS_OK && (typed->_presence[OperationStageConfig_OPTIONAL_materializer / 8u] &
                           (1u << (OperationStageConfig_OPTIONAL_materializer % 8u))) != 0u)
      rc = copy_plugin(selection->materializer, sizeof(selection->materializer),
                       typed->materializer.plugin);
  }
  cleanup = flow_provider_typed_config_destroy(&config);
  if (rc == SALTS_OK) rc = cleanup;
  if (rc != SALTS_OK)
    return config_error(error, rc, stage->name, NULL,
                        "operation selection or native config cleanup failed");
  if (selection->materializer[0] && !selection->plugin[0])
    return config_error(error, SALTS_ENOTSUP, stage->name, "materializer",
                        "materializer requires an explicit typed operation policy");
  return SALTS_OK;
}

static int same_name(const char *left, const char *right) {
  return left && right ? strcmp(left, right) == 0 : left == right;
}

static int same_policy(const operation_stage_selection_t *left,
                       const operation_stage_selection_t *right) {
  return strcmp(left->plugin, right->plugin) == 0 && left->version == right->version &&
         left->limits.max_inflight == right->limits.max_inflight &&
         left->limits.max_input_bytes == right->limits.max_input_bytes &&
         left->limits.max_result_bytes == right->limits.max_result_bytes &&
         left->limits.max_retained_bytes == right->limits.max_retained_bytes &&
         left->limits.max_steps == right->limits.max_steps;
}

int flow_plugin_operation_configure(const turbo_flow_t *flow, vec_t *operations,
                                    turbo_flow_config_error_t *error) {
  vec_t selections = {0};
  size_t count = 0u;
  int rc = turbo_flow_stl_error(vec_init_bytes(&selections, sizeof(operation_stage_selection_t),
                                               _Alignof(turbo_flow_max_align_t),
                                               vec_size(&flow->stages)));
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0u; i < vec_size(&flow->stages); ++i) {
    const flow_stage_plan_impl_t *stage = vec_at_const(&flow->stages, i);
    operation_stage_selection_t selection;
    int found = 0;
    if (!stage->operation_name || stage->adapter_name) continue;
    if (stage->is_source || stage->is_buffer) {
      if (vec_size(&stage->provider_config_literals)) {
        rc = config_error(error, SALTS_ENOTSUP, stage->name, NULL,
                          "typed operation policy requires an operation stage");
        goto done;
      }
      continue;
    }
    rc = stage_selection(stage, &selection, error);
    if (rc != SALTS_OK) goto done;
    /* One operation+resource owns one session/budget, including stages omitting policy. */
    for (size_t j = 0u; j < vec_size(&selections); ++j) {
      const operation_stage_selection_t *previous = vec_at_const(&selections, j);
      if (!same_name(stage->operation_name, previous->stage->operation_name) ||
          !same_name(stage->resource_name, previous->stage->resource_name))
        continue;
      found = 1;
      if (!same_policy(previous, &selection)) {
        rc = config_error(error, SALTS_EPROTO, stage->name, "policy",
                          "stages sharing one operation binding must declare the same policy");
        goto done;
      }
      if (strcmp(previous->materializer, selection.materializer) != 0) {
        rc = config_error(error, SALTS_EPROTO, stage->name, "materializer",
                          "stages sharing one operation binding must select the same materializer");
        goto done;
      }
      break;
    }
    if (found) continue;
    if (selection.plugin[0] && ++count > FLOW_PLUGIN_OPERATION_MAX_BINDINGS) {
      rc = config_error(error, SALTS_ENOSPC, stage->name, "policy", "too many operation bindings");
      goto done;
    }
    rc = turbo_flow_stl_error(vec_push(&selections, &selection));
    if (rc != SALTS_OK) goto done;
  }
  /* Allocate exactly the selected binding count before initializing atomics/mutexes. */
  rc = turbo_flow_stl_error(vec_init_bytes(operations, sizeof(flow_plugin_operation_binding_t),
                                           _Alignof(turbo_flow_max_align_t), count));
  if (rc != SALTS_OK) goto done;
  rc = turbo_flow_stl_error(vec_reserve(operations, count));
  if (rc != SALTS_OK) goto done;
  for (size_t i = 0u; i < vec_size(&selections); ++i) {
    const operation_stage_selection_t *selection = vec_at_const(&selections, i);
    flow_plugin_operation_binding_t binding = {0};
    if (!selection->plugin[0]) continue;
    memcpy(binding.plugin, selection->plugin, sizeof(binding.plugin));
    memcpy(binding.materializer_plugin, selection->materializer,
           sizeof(binding.materializer_plugin));
    binding.version = selection->version;
    turbo_flow_plugin_operation_request_v3_init(&binding.request);
    binding.request.operation_name = selection->stage->operation_name;
    binding.request.resource_name = selection->stage->resource_name;
    binding.request.limits = selection->limits;
    rc = turbo_flow_stl_error(vec_push(operations, &binding));
    if (rc != SALTS_OK) goto done;
  }
done:
  vec_destroy(&selections);
  return rc;
}
