#include "flow_redis_storage_internal.h"

#include "turbo_error.h"

#include <stdlib.h>

typedef struct flow_redis_storage_instance_s {
  turbo_flow_storage_model_t model;
  union {
    turbo_flow_record_store_t record;
    turbo_flow_state_store_t *state;
    turbo_flow_index_store_t *index;
    turbo_flow_log_store_t *log;
  } service;
} flow_redis_storage_instance_t;

static int flow_redis_storage_options(
    const turbo_flow_storage_backend_open_request_t *request,
    const turbo_flow_redis_storage_backend_options_t **out) {
  const turbo_flow_redis_storage_backend_options_t *options;
  if (out) *out = NULL;
  if (!request || !out || !request->options ||
      request->options_size < sizeof(turbo_flow_redis_storage_backend_options_t)) {
    return TURBO_EINVAL;
  }
  options = (const turbo_flow_redis_storage_backend_options_t *)request->options;
  if (options->size < sizeof(*options) ||
      options->version != TURBO_FLOW_REDIS_STORAGE_BACKEND_OPTIONS_VERSION || !options->config) {
    return TURBO_EINVAL;
  }
  *out = options;
  return TURBO_OK;
}

static int flow_redis_storage_open(
    void *ctx, const turbo_flow_storage_backend_open_request_t *request,
    turbo_flow_storage_backend_service_t *service, turbo_flow_config_error_t *error) {
  const turbo_flow_redis_storage_backend_options_t *options = NULL;
  flow_redis_storage_instance_t *instance;
  int rc = TURBO_EINVAL;
  (void)ctx;
  if (!request || !service || service->size < sizeof(*service) ||
      service->abi_version != TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  instance = (flow_redis_storage_instance_t *)calloc(1u, sizeof(*instance));
  if (!instance) return TURBO_ENOMEM;
  instance->model = request->model;
  switch (request->model) {
  case TURBO_FLOW_STORAGE_MODEL_RECORD:
    instance->service.record = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
    if (request->resolved && request->channel_name && !request->options) {
      rc = flow_redis_record_store_create_resolved(
          request->resolved, request->channel_name, &instance->service.record, error);
    } else if (flow_redis_storage_options(request, &options) == TURBO_OK &&
               options->config_size == sizeof(turbo_flow_redis_record_store_config_t) &&
               !options->limits) {
      rc = flow_redis_record_store_create(
          (const turbo_flow_redis_record_store_config_t *)options->config,
          &instance->service.record);
    }
    if (rc == TURBO_OK) service->instance = &instance->service.record;
    break;
  case TURBO_FLOW_STORAGE_MODEL_STATE:
    if (flow_redis_storage_options(request, &options) == TURBO_OK &&
        options->config_size == sizeof(turbo_flow_redis_record_store_config_t) &&
        options->limits) {
      rc = flow_redis_state_store_create(
          (const turbo_flow_redis_record_store_config_t *)options->config, options->limits,
          &instance->service.state);
    }
    if (rc == TURBO_OK) service->instance = instance->service.state;
    break;
  case TURBO_FLOW_STORAGE_MODEL_INDEX:
    if (flow_redis_storage_options(request, &options) == TURBO_OK &&
        options->config_size == sizeof(turbo_flow_redis_index_store_config_t) &&
        options->limits) {
      rc = flow_redis_index_store_create(
          (const turbo_flow_redis_index_store_config_t *)options->config, options->limits,
          &instance->service.index);
    }
    if (rc == TURBO_OK) service->instance = instance->service.index;
    break;
  case TURBO_FLOW_STORAGE_MODEL_LOG:
    if (flow_redis_storage_options(request, &options) == TURBO_OK &&
        options->config_size == sizeof(turbo_flow_redis_log_store_config_t) &&
        options->limits) {
      rc = flow_redis_log_store_create(
          (const turbo_flow_redis_log_store_config_t *)options->config, options->limits,
          &instance->service.log);
    }
    if (rc == TURBO_OK) service->instance = instance->service.log;
    break;
  default:
    rc = TURBO_ENOTSUP;
    break;
  }
  if (rc != TURBO_OK) {
    free(instance);
    return rc;
  }
  service->model = request->model;
  service->owner = instance;
  return TURBO_OK;
}

static void flow_redis_storage_close(void *ctx,
                                     turbo_flow_storage_backend_service_t *service) {
  flow_redis_storage_instance_t *instance;
  (void)ctx;
  if (!service || !service->owner) return;
  instance = (flow_redis_storage_instance_t *)service->owner;
  switch (service->model) {
  case TURBO_FLOW_STORAGE_MODEL_RECORD:
    flow_redis_record_store_destroy(&instance->service.record);
    break;
  case TURBO_FLOW_STORAGE_MODEL_STATE:
    (void)turbo_flow_state_store_close(instance->service.state);
    turbo_flow_state_store_destroy(instance->service.state);
    break;
  case TURBO_FLOW_STORAGE_MODEL_INDEX:
    (void)turbo_flow_index_store_close(instance->service.index);
    turbo_flow_index_store_destroy(instance->service.index);
    break;
  case TURBO_FLOW_STORAGE_MODEL_LOG:
    (void)turbo_flow_log_store_close(instance->service.log);
    turbo_flow_log_store_destroy(instance->service.log);
    break;
  default:
    return;
  }
  service->instance = NULL;
  service->owner = NULL;
  free(instance);
}

static const turbo_flow_storage_backend_plugin_api_t FLOW_REDIS_STORAGE_BACKEND_API = {
    sizeof(turbo_flow_storage_backend_plugin_api_t),
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
    "redis",
    TURBO_FLOW_STORAGE_CAP_RECORD | TURBO_FLOW_STORAGE_CAP_STATE |
        TURBO_FLOW_STORAGE_CAP_INDEX | TURBO_FLOW_STORAGE_CAP_LOG,
    NULL,
    flow_redis_storage_open,
    flow_redis_storage_close};

const turbo_flow_storage_backend_plugin_api_t *turbo_flow_redis_storage_backend_api(void) {
  return &FLOW_REDIS_STORAGE_BACKEND_API;
}

const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_plugin_get_api(void) {
  return &FLOW_REDIS_STORAGE_BACKEND_API;
}
