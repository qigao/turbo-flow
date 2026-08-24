#include "turbo_flow_product.h"

#include "flow_config_internal.h"

#include "turbo_error.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

static int flow_product_error(turbo_flow_config_error_t *error, int status, const char *path,
                              const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s", path ? path : "$");
    (void)snprintf(error->message, sizeof(error->message), "%s", message ? message : "error");
  }
  return status;
}

static int flow_product_runtime_integer(const json_value_t *value, size_t maximum,
                                        size_t *result) {
  double number;
  size_t converted;
  if (!value || !result || turbo_json_type(value) != TURBO_JSON_NUMBER) return TURBO_EPROTO;
  number = turbo_json_number(value);
  if (!isfinite(number) || number < 1.0 || number > (double)maximum) return TURBO_EPROTO;
  converted = (size_t)number;
  if ((double)converted != number) return TURBO_EPROTO;
  *result = converted;
  return TURBO_OK;
}

int turbo_flow_resolved_config_runtime_ingress(const turbo_flow_resolved_config_t *config,
                                               turbo_flow_async_ingress_config_t *ingress) {
  turbo_flow_async_ingress_config_t resolved = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  json_value_t *runtime;
  json_value_t *ingress_value;
  json_value_t *workers;
  json_value_t *capacity;
  json_value_t *max_message_bytes;
  json_value_t *max_inflight_bytes;
  size_t output_size;
  size_t value;
  if (!config || !config->document || !ingress ||
      ingress->size < TURBO_FLOW_ASYNC_INGRESS_CONFIG_V1_SIZE ||
      (ingress->size > TURBO_FLOW_ASYNC_INGRESS_CONFIG_V1_SIZE &&
       ingress->size < sizeof(*ingress)))
    return TURBO_EINVAL;
  output_size = ingress->size;
  runtime = turbo_json_object_get(config->document, "runtime");
  ingress_value = runtime ? turbo_json_object_get(runtime, "ingress") : NULL;
  workers = ingress_value ? turbo_json_object_get(ingress_value, "workers") : NULL;
  capacity = ingress_value ? turbo_json_object_get(ingress_value, "capacity") : NULL;
  max_message_bytes =
      ingress_value ? turbo_json_object_get(ingress_value, "max_message_bytes") : NULL;
  max_inflight_bytes =
      ingress_value ? turbo_json_object_get(ingress_value, "max_inflight_bytes") : NULL;
  if (!runtime || turbo_json_type(runtime) != TURBO_JSON_OBJECT || !ingress_value ||
      turbo_json_type(ingress_value) != TURBO_JSON_OBJECT ||
      flow_product_runtime_integer(workers, TURBO_FLOW_ASYNC_INGRESS_MAX_WORKERS, &value) !=
          TURBO_OK)
    return TURBO_EPROTO;
  resolved.workers = (uint32_t)value;
  if (flow_product_runtime_integer(capacity, TURBO_FLOW_ASYNC_INGRESS_MAX_CAPACITY, &value) !=
      TURBO_OK)
    return TURBO_EPROTO;
  resolved.queue_capacity = value;
  if (flow_product_runtime_integer(max_message_bytes,
                                   TURBO_FLOW_ASYNC_INGRESS_MAX_MESSAGE_BYTES, &value) != TURBO_OK)
    return TURBO_EPROTO;
  resolved.max_message_bytes = value;
  if (flow_product_runtime_integer(max_inflight_bytes,
                                   TURBO_FLOW_ASYNC_INGRESS_MAX_INFLIGHT_BYTES, &value) != TURBO_OK)
    return TURBO_EPROTO;
  resolved.max_inflight_bytes = value;
  if (resolved.max_message_bytes > resolved.max_inflight_bytes) return TURBO_EPROTO;
  if (output_size >= sizeof(*ingress)) {
    *ingress = resolved;
  } else {
    ingress->workers = resolved.workers;
    ingress->queue_capacity = resolved.queue_capacity;
  }
  return TURBO_OK;
}

static const turbo_flow_product_adapter_provider_t *
flow_product_adapter_provider(const turbo_flow_product_provider_registry_t *registry,
                              const char *kind) {
  if (!registry || !kind) return NULL;
  for (size_t i = 0u; i < registry->adapter_provider_count; ++i) {
    const turbo_flow_product_adapter_provider_t *provider = &registry->adapter_providers[i];
    if (provider->kind && strcmp(provider->kind, kind) == 0) return provider;
  }
  return NULL;
}

static const turbo_flow_product_resource_provider_t *
flow_product_resource_provider(const turbo_flow_product_provider_registry_t *registry,
                               const char *kind) {
  if (!registry || !kind) return NULL;
  for (size_t i = 0u; i < registry->resource_provider_count; ++i) {
    const turbo_flow_product_resource_provider_t *provider = &registry->resource_providers[i];
    if (provider->kind && strcmp(provider->kind, kind) == 0) return provider;
  }
  return NULL;
}

static int flow_product_registry_validate(const turbo_flow_product_provider_registry_t *registry,
                                          turbo_flow_config_error_t *error) {
  if (!registry || registry->size < sizeof(*registry) ||
      (registry->adapter_provider_count > 0u && !registry->adapter_providers) ||
      (registry->resource_provider_count > 0u && !registry->resource_providers)) {
    return flow_product_error(error, TURBO_EINVAL, "$.providers",
                             "invalid product provider registry");
  }
  for (size_t i = 0u; i < registry->adapter_provider_count; ++i) {
    const turbo_flow_product_adapter_provider_t *provider = &registry->adapter_providers[i];
    if (provider->size < sizeof(*provider) || !provider->kind || !provider->kind[0] ||
        !provider->register_adapter) {
      return flow_product_error(error, TURBO_EINVAL, "$.providers.adapters",
                               "invalid adapter provider");
    }
    for (size_t previous = 0u; previous < i; ++previous) {
      if (strcmp(provider->kind, registry->adapter_providers[previous].kind) == 0) {
        return flow_product_error(error, TURBO_EALREADY, "$.providers.adapters",
                                 "duplicate adapter provider kind");
      }
    }
  }
  for (size_t i = 0u; i < registry->resource_provider_count; ++i) {
    const turbo_flow_product_resource_provider_t *provider = &registry->resource_providers[i];
    if (provider->size < sizeof(*provider) || !provider->kind || !provider->kind[0] ||
        !provider->register_resource) {
      return flow_product_error(error, TURBO_EINVAL, "$.providers.resources",
                               "invalid resource provider");
    }
    for (size_t previous = 0u; previous < i; ++previous) {
      if (strcmp(provider->kind, registry->resource_providers[previous].kind) == 0) {
        return flow_product_error(error, TURBO_EALREADY, "$.providers.resources",
                                 "duplicate resource provider kind");
      }
    }
  }
  return TURBO_OK;
}

int turbo_flow_product_preflight(const turbo_flow_resolved_config_t *config,
                                 const turbo_flow_product_provider_registry_t *registry,
                                 turbo_flow_config_error_t *error) {
  json_value_t *adapters;
  int rc;
  if (!config || !config->document || !error || error->size < sizeof(*error)) return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = flow_product_registry_validate(registry, error);
  if (rc != TURBO_OK) return rc;
  adapters = turbo_json_object_get(config->document, "adapters");
  if (!adapters || turbo_json_type(adapters) != TURBO_JSON_OBJECT)
    return flow_product_error(error, TURBO_EPROTO, "$.adapters",
                              "resolved adapter map is invalid");
  for (size_t i = 0u; i < turbo_json_object_size(adapters); ++i) {
    const char *name = turbo_json_object_key(adapters, i);
    json_value_t *adapter = turbo_json_object_value(adapters, i);
    json_value_t *kind = adapter ? turbo_json_object_get(adapter, "kind") : NULL;
    const char *kind_name =
        kind && turbo_json_type(kind) == TURBO_JSON_STRING ? turbo_json_string(kind) : NULL;
    char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
    if (!name || !name[0] || !kind_name || !kind_name[0])
      return flow_product_error(error, TURBO_EPROTO, "$.adapters",
                               "resolved adapter identity is invalid");
    if (!flow_product_adapter_provider(registry, kind_name)) {
      (void)snprintf(path, sizeof(path), "$.adapters.%s.kind", name);
      return flow_product_error(error, TURBO_ENOTSUP, path,
                                "adapter kind has no product provider");
    }
  }
  return TURBO_OK;
}

static int flow_product_channel_kind(const turbo_flow_resolved_config_t *config,
                                     const char *resource_name, const char **kind_name,
                                     turbo_flow_config_error_t *error) {
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  if (kind_name) *kind_name = NULL;
  if (!config || !config->document || !resource_name || !resource_name[0] || !kind_name)
    return TURBO_EINVAL;
  channels = turbo_json_object_get(config->document, "channels");
  channel = channels ? turbo_json_object_get(channels, resource_name) : NULL;
  (void)snprintf(path, sizeof(path), "$.channels.%s", resource_name);
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT)
    return flow_product_error(error, TURBO_ENOENT, path,
                             "Graph resource does not resolve to a configured channel");
  kind = turbo_json_object_get(channel, "kind");
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING || !turbo_json_string(kind)[0])
    return flow_product_error(error, TURBO_EPROTO, path, "resolved channel kind is invalid");
  *kind_name = turbo_json_string(kind);
  return TURBO_OK;
}

static int flow_product_stage_reference_seen(const turbo_flow_t *flow, size_t stage_index,
                                             int resource, const char *name) {
  if (!name) return 1;
  for (size_t i = 0u; i < stage_index; ++i) {
    const turbo_flow_stage_plan_t *previous = turbo_flow_stage_at(flow, i);
    const char *previous_name =
        previous ? (resource ? previous->resource_name : previous->adapter_name) : NULL;
    if (previous_name && strcmp(previous_name, name) == 0) return 1;
  }
  return 0;
}

static int flow_product_provider_error(turbo_flow_config_error_t *error, int status,
                                       const char *scope, const char *name) {
  char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
  if (error && error->size >= sizeof(*error) && error->status != TURBO_OK) return status;
  (void)snprintf(path, sizeof(path), "$.%s.%s", scope, name);
  return flow_product_error(error, status, path, "product provider registration failed");
}

int turbo_flow_product_assemble_graph(turbo_flow_t *flow,
                                      const turbo_flow_resolved_config_t *config,
                                      const turbo_flow_product_provider_registry_t *registry,
                                      turbo_flow_config_error_t *error) {
  size_t stage_count;
  int rc;
  if (!flow || !config || !error || error->size < sizeof(*error)) return TURBO_EINVAL;
  if (turbo_flow_state(flow) != TURBO_FLOW_STATE_PARSED)
    return flow_product_error(error, TURBO_EINVAL, "$.graph",
                             "product assembly requires a parsed Graph");
  rc = turbo_flow_product_preflight(config, registry, error);
  if (rc != TURBO_OK) return rc;
  stage_count = turbo_flow_stage_count(flow);

  for (size_t i = 0u; i < stage_count; ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    const char *resource_name = stage ? stage->resource_name : NULL;
    const char *adapter_name = stage ? stage->adapter_name : NULL;
    const char *kind = NULL;
    if (!stage)
      return flow_product_error(error, TURBO_EPROTO, "$.graph", "invalid Graph stage");
    if (resource_name && !flow_product_stage_reference_seen(flow, i, 1, resource_name)) {
      rc = flow_product_channel_kind(config, resource_name, &kind, error);
      if (rc != TURBO_OK) return rc;
      if (!flow_product_resource_provider(registry, kind)) {
        char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
        (void)snprintf(path, sizeof(path), "$.channels.%s.kind", resource_name);
        return flow_product_error(error, TURBO_ENOTSUP, path,
                                 "Graph resource kind has no product provider");
      }
    }
    if (adapter_name && !flow_product_stage_reference_seen(flow, i, 0, adapter_name)) {
      turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
      rc = turbo_flow_resolved_config_adapter(config, adapter_name, &view);
      if (rc != TURBO_OK) {
        char path[TURBO_FLOW_CONFIG_PATH_MAX + 1u];
        (void)snprintf(path, sizeof(path), "$.adapters.%s", adapter_name);
        return flow_product_error(error, rc, path,
                                 "Graph adapter does not resolve to configured adapter");
      }
    }
  }

  for (size_t i = 0u; i < stage_count; ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    const char *resource_name = stage ? stage->resource_name : NULL;
    const char *kind = NULL;
    const turbo_flow_product_resource_provider_t *provider;
    if (!resource_name || flow_product_stage_reference_seen(flow, i, 1, resource_name)) continue;
    rc = flow_product_channel_kind(config, resource_name, &kind, error);
    if (rc != TURBO_OK) return rc;
    provider = flow_product_resource_provider(registry, kind);
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    rc = provider->register_resource(provider->ctx, flow, config, resource_name, error);
    if (rc != TURBO_OK) return flow_product_provider_error(error, rc, "channels", resource_name);
  }

  for (size_t i = 0u; i < stage_count; ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    const char *adapter_name = stage ? stage->adapter_name : NULL;
    turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
    const turbo_flow_product_adapter_provider_t *provider;
    if (!adapter_name || flow_product_stage_reference_seen(flow, i, 0, adapter_name)) continue;
    rc = turbo_flow_resolved_config_adapter(config, adapter_name, &view);
    if (rc != TURBO_OK) return flow_product_provider_error(error, rc, "adapters", adapter_name);
    provider = flow_product_adapter_provider(registry, view.kind);
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    rc = provider->register_adapter(provider->ctx, flow, config, adapter_name, error);
    if (rc != TURBO_OK) return flow_product_provider_error(error, rc, "adapters", adapter_name);
  }
  return TURBO_OK;
}
