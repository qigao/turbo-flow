#include "flow_sqlite_storage_internal.h"

#include "turbo_error.h"

#include <stdlib.h>

typedef struct flow_sqlite_storage_instance_s {
  turbo_flow_record_store_t record;
} flow_sqlite_storage_instance_t;

static int flow_sqlite_storage_open(void *ctx,
                                    const turbo_flow_storage_backend_open_request_t *request,
                                    turbo_flow_storage_backend_service_t *service,
                                    turbo_flow_config_error_t *error) {
  flow_sqlite_storage_instance_t *instance;
  int rc;
  (void)ctx;
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION || !service ||
      service->size < sizeof(*service) ||
      service->abi_version != TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION)
    return TURBO_EINVAL;
  if (request->model != TURBO_FLOW_STORAGE_MODEL_RECORD) return TURBO_ENOTSUP;
  instance = (flow_sqlite_storage_instance_t *)calloc(1u, sizeof(*instance));
  if (!instance) return TURBO_ENOMEM;
  instance->record = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  if (request->resolved && request->channel_name && !request->options) {
    rc = turbo_flow_sqlite_record_store_create_resolved(request->resolved, request->channel_name,
                                                        &instance->record, error);
  } else if (!request->resolved && request->options &&
             request->options_size >= sizeof(turbo_flow_sqlite_storage_backend_options_t)) {
    const turbo_flow_sqlite_storage_backend_options_t *options =
        (const turbo_flow_sqlite_storage_backend_options_t *)request->options;
    if (options->size < sizeof(*options) ||
        options->version != TURBO_FLOW_SQLITE_STORAGE_BACKEND_OPTIONS_VERSION || !options->config) {
      rc = TURBO_EINVAL;
    } else {
      rc = turbo_flow_sqlite_record_store_create(options->config, &instance->record);
    }
  } else {
    rc = TURBO_EINVAL;
  }
  if (rc != TURBO_OK) {
    free(instance);
    return rc;
  }
  service->model = TURBO_FLOW_STORAGE_MODEL_RECORD;
  service->instance = &instance->record;
  service->owner = instance;
  return TURBO_OK;
}

static void flow_sqlite_storage_close(void *ctx, turbo_flow_storage_backend_service_t *service) {
  flow_sqlite_storage_instance_t *instance;
  (void)ctx;
  if (!service || service->model != TURBO_FLOW_STORAGE_MODEL_RECORD || !service->owner) return;
  instance = (flow_sqlite_storage_instance_t *)service->owner;
  (void)turbo_flow_sqlite_record_store_close(&instance->record);
  service->instance = NULL;
  service->owner = NULL;
  free(instance);
}

static const turbo_flow_storage_backend_plugin_api_t FLOW_SQLITE_STORAGE_BACKEND_API = {
    sizeof(turbo_flow_storage_backend_plugin_api_t),
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
    "sqlite",
    TURBO_FLOW_STORAGE_CAP_RECORD,
    NULL,
    flow_sqlite_storage_open,
    flow_sqlite_storage_close};

const turbo_flow_storage_backend_plugin_api_t *turbo_flow_sqlite_storage_backend_api(void) {
  return &FLOW_SQLITE_STORAGE_BACKEND_API;
}

const turbo_flow_storage_backend_plugin_api_t *turbo_flow_storage_backend_plugin_get_api(void) {
  return &FLOW_SQLITE_STORAGE_BACKEND_API;
}
