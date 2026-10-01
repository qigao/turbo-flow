#include "flow_provider_instance_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct flow_compiled_provider_instance_s {
  size_t stage_index;
  turbo_flow_provider_binding_t *provider_binding;
  turbo_flow_resource_binding_t *resource_binding;
  flow_provider_typed_config_t typed_config;
  turbo_flow_provider_contract_v1_t contract;
  turbo_flow_provider_config_view_v1_t config_view;
  turbo_flow_provider_resource_view_v1_t resource_view;
  turbo_flow_provider_instance_v1_t instance;
  turbo_flow_runtime_owner owner;
  int preflight_complete;
  int owner_live;
};

static int compiled_provider_error(
    turbo_flow_config_error_t *error,
    int status,
    const char *stage_name,
    const char *field,
    const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.stages.%s%s%s",
                   stage_name ? stage_name : "",
                   field && field[0] ? "." : "",
                   field ? field : "");
    (void)snprintf(error->message, sizeof(error->message), "%s",
                   message ? message : "provider instance binding failed");
  }
  return status;
}

static int compiled_provider_resource_required(
    const turbo_flow_provider_resource_requirement_v1_t *resource) {
  return resource && resource->size == sizeof(*resource) &&
         resource->contract_id && resource->contract_id[0] &&
         resource->contract_version != 0u &&
         cmeta_interface_desc_valid(resource->expected_interface);
}

static int compiled_provider_config_error(
    turbo_flow_config_error_t *error,
    int status,
    const char *stage_name,
    const DataBindMessagePlanDiagnostic *diagnostic) {
  const char *field = NULL;
  const char *message = "typed provider config binding failed";
  if (diagnostic &&
      diagnostic->size == sizeof(*diagnostic) &&
      diagnostic->abi_version == DATA_BIND_MESSAGE_PLAN_ABI_VERSION) {
    if (diagnostic->schema_field[0]) field = diagnostic->schema_field;
    if (diagnostic->message[0]) message = diagnostic->message;
  }
  return compiled_provider_error(
      error, status, stage_name, field, message);
}

static int compiled_provider_cleanup(
    flow_compiled_provider_instance_t *compiled) {
  int first = SALTS_OK;
  int rc;

  if (!compiled) return SALTS_EINVAL;
  if (compiled->owner_live) return SALTS_EBUSY;

  if (compiled->typed_config.artifact || compiled->typed_config.plan ||
      compiled->typed_config.storage_allocation ||
      compiled->typed_config.workspace) {
    rc = flow_provider_typed_config_destroy(&compiled->typed_config);
    if (rc != SALTS_OK && first == SALTS_OK) first = rc;
  }

  if (compiled->resource_binding) {
    rc = turbo_flow_resource_binding_release(&compiled->resource_binding);
    if (rc != SALTS_OK && first == SALTS_OK) first = rc;
  }

  if (compiled->provider_binding) {
    rc = turbo_flow_provider_binding_release(&compiled->provider_binding);
    if (rc != SALTS_OK && first == SALTS_OK) first = rc;
  }

  return first;
}

static int compiled_provider_fail(
    flow_compiled_provider_instance_t *compiled,
    flow_compiled_provider_instance_t **out,
    turbo_flow_config_error_t *error,
    int original_status,
    const char *stage_name,
    const char *message) {
  int cleanup;
  cleanup = compiled_provider_cleanup(compiled);
  if (cleanup != SALTS_OK) {
    if (out) *out = compiled;
    return compiled_provider_error(
        error, cleanup, stage_name, NULL,
        "provider instance cleanup failed after binding rejection");
  }
  free(compiled);
  if (out) *out = NULL;
  if (!error || error->size != sizeof(*error) || error->status == SALTS_OK)
    compiled_provider_error(
        error, original_status, stage_name, NULL, message);
  return original_status;
}

int flow_compiled_provider_instance_prepare(
    turbo_flow_t *flow,
    size_t stage_index,
    const turbo_flow_provider_resolver_v1_t *provider_resolver,
    const turbo_flow_resource_resolver_v1_t *resource_resolver,
    flow_compiled_provider_instance_t **out,
    turbo_flow_config_error_t *error) {
  const flow_stage_plan_impl_t *stage;
  flow_compiled_provider_instance_t *compiled;
  flow_provider_typed_config_view_t config_view;
  DataBindMessagePlanDiagnostic diagnostic =
      DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  int requires_resource;
  int rc;

  if (out) *out = NULL;
  if (!flow || !provider_resolver || !out)
    return compiled_provider_error(
        error, SALTS_EINVAL, NULL, NULL,
        "invalid compiled provider arguments");

  stage = (const flow_stage_plan_impl_t *)vec_at_const(
      &flow->stages, stage_index);
  if (!stage || !stage->name || !stage->adapter_name ||
      !stage->adapter_name[0])
    return compiled_provider_error(
        error, SALTS_EPROTO, stage ? stage->name : NULL, "provider",
        "stage has no canonical provider identity");

  compiled =
      (flow_compiled_provider_instance_t *)calloc(1u, sizeof(*compiled));
  if (!compiled)
    return compiled_provider_error(
        error, SALTS_ENOMEM, stage->name, NULL,
        "compiled provider allocation failed");

  compiled->stage_index = stage_index;
  compiled->contract =
      (turbo_flow_provider_contract_v1_t)TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  compiled->config_view =
      (turbo_flow_provider_config_view_v1_t)
          TURBO_FLOW_PROVIDER_CONFIG_VIEW_V1_INIT;
  compiled->resource_view =
      (turbo_flow_provider_resource_view_v1_t)
          TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
  compiled->instance =
      (turbo_flow_provider_instance_v1_t)TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;

  rc = turbo_flow_provider_binding_acquire(
      provider_resolver, stage->adapter_name,
      &compiled->provider_binding, error);
  if (rc != SALTS_OK) {
    if (compiled->provider_binding) {
      *out = compiled;
      return rc;
    }
    free(compiled);
    return rc;
  }

  rc = turbo_flow_provider_binding_contract(
      compiled->provider_binding, &compiled->contract);
  if (rc != SALTS_OK)
    return compiled_provider_fail(
        compiled, out, error, rc, stage->name,
        "provider contract could not be borrowed");

  rc = flow_provider_typed_config_bind(
      stage,
      compiled->contract.config.codec_factory,
      compiled->contract.config.message_artifact,
      &compiled->typed_config,
      &diagnostic);
  if (rc != SALTS_OK) {
    compiled_provider_config_error(
        error, rc, stage->name, &diagnostic);
    return compiled_provider_fail(
        compiled, out, error, rc, stage->name,
        "typed provider config binding failed");
  }

  rc = flow_provider_typed_config_view(
      &compiled->typed_config, &config_view);
  if (rc != SALTS_OK)
    return compiled_provider_fail(
        compiled, out, error, rc, stage->name,
        "typed provider config view is unavailable");

  compiled->config_view.type_name = config_view.type_name;
  compiled->config_view.data = config_view.data;
  compiled->config_view.value = config_view.value;
  compiled->config_view.value_bytes = config_view.value_bytes;

  requires_resource =
      compiled_provider_resource_required(&compiled->contract.resource);
  if (requires_resource) {
    if (!stage->resource_name || !stage->resource_name[0])
      return compiled_provider_fail(
          compiled, out, error, SALTS_EPROTO, stage->name,
          "provider requires an explicit deployment resource");
    if (!resource_resolver)
      return compiled_provider_fail(
          compiled, out, error, SALTS_EINVAL, stage->name,
          "deployment resource resolver is required");

    rc = turbo_flow_resource_binding_acquire(
        resource_resolver, stage->resource_name,
        &compiled->contract.resource,
        &compiled->resource_binding, error);
    if (rc != SALTS_OK) {
      if (compiled->resource_binding) {
        *out = compiled;
        return rc;
      }
      return compiled_provider_fail(
          compiled, out, error, rc, stage->name,
          "deployment resource binding failed");
    }

    rc = turbo_flow_resource_binding_view(
        compiled->resource_binding, &compiled->resource_view);
    if (rc != SALTS_OK)
      return compiled_provider_fail(
          compiled, out, error, rc, stage->name,
          "deployment resource view is unavailable");
  } else if (stage->resource_name && stage->resource_name[0]) {
    return compiled_provider_fail(
        compiled, out, error, SALTS_EPROTO, stage->name,
        "stage declares a resource but provider contract does not accept one");
  }

  compiled->instance.instance_name = stage->name;
  compiled->instance.config = compiled->config_view;
  compiled->instance.resource =
      requires_resource ? &compiled->resource_view : NULL;

  rc = turbo_flow_provider_binding_preflight(
      compiled->provider_binding, &compiled->instance, error);
  if (rc != SALTS_OK)
    return compiled_provider_fail(
        compiled, out, error, rc, stage->name,
        "provider preflight rejected the compiled instance");

  compiled->preflight_complete = 1;
  *out = compiled;
  return SALTS_OK;
}

int flow_compiled_provider_instance_view(
    const flow_compiled_provider_instance_t *compiled,
    const turbo_flow_provider_instance_v1_t **out) {
  if (out) *out = NULL;
  if (!compiled || !out || !compiled->preflight_complete ||
      !compiled->provider_binding ||
      !compiled->instance.instance_name ||
      !compiled->instance.instance_name[0])
    return SALTS_EINVAL;
  *out = &compiled->instance;
  return SALTS_OK;
}

int flow_compiled_provider_instance_materialize(
    flow_compiled_provider_instance_t *compiled,
    turbo_flow_t *flow,
    turbo_flow_config_error_t *error) {
  turbo_flow_runtime_owner owner = {0};
  int rc;

  if (!compiled || !flow || !compiled->preflight_complete ||
      !compiled->provider_binding)
    return compiled_provider_error(
        error, SALTS_EINVAL,
        compiled ? compiled->instance.instance_name : NULL, NULL,
        "invalid compiled provider materialization arguments");
  if (compiled->owner_live)
    return compiled_provider_error(
        error, SALTS_EALREADY, compiled->instance.instance_name, NULL,
        "compiled provider instance is already materialized");

  rc = turbo_flow_provider_binding_materialize(
      compiled->provider_binding, flow, &compiled->instance, &owner, error);
  if (rc != SALTS_OK) {
    if (!error || error->size != sizeof(*error) || error->status == SALTS_OK)
      compiled_provider_error(
          error, rc, compiled->instance.instance_name, NULL,
          "provider materialization failed");
    return rc;
  }

  compiled->owner = owner;
  compiled->owner_live = 1;
  if (!turbo_flow_runtime_owner_contract_valid(&compiled->owner))
    return compiled_provider_error(
        error, SALTS_EPROTO, compiled->instance.instance_name, NULL,
        "provider returned an invalid runtime owner");

  return SALTS_OK;
}

int flow_compiled_provider_instance_owner(
    flow_compiled_provider_instance_t *compiled,
    turbo_flow_runtime_owner **out) {
  if (out) *out = NULL;
  if (!compiled || !out || !compiled->owner_live)
    return SALTS_EINVAL;
  if (!turbo_flow_runtime_owner_contract_valid(&compiled->owner))
    return SALTS_EPROTO;
  *out = &compiled->owner;
  return SALTS_OK;
}

int flow_compiled_provider_instance_owner_destroy(
    flow_compiled_provider_instance_t *compiled) {
  if (!compiled || !compiled->owner_live) return SALTS_EINVAL;
  if (!turbo_flow_runtime_owner_contract_valid(&compiled->owner))
    return SALTS_EPROTO;

  turbo_flow_runtime_owner_destroy(&compiled->owner);
  memset(&compiled->owner, 0, sizeof(compiled->owner));
  compiled->owner_live = 0;
  return SALTS_OK;
}

int flow_compiled_provider_instance_release(
    flow_compiled_provider_instance_t **compiled_io) {
  flow_compiled_provider_instance_t *compiled;
  int rc;

  if (!compiled_io || !*compiled_io) return SALTS_EINVAL;
  compiled = *compiled_io;
  rc = compiled_provider_cleanup(compiled);
  if (compiled->resource_binding || compiled->provider_binding)
    return rc != SALTS_OK ? rc : SALTS_EBUSY;

  free(compiled);
  *compiled_io = NULL;
  return rc;
}
