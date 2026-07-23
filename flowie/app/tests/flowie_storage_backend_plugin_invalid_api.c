#include "turbo_flow_storage_backend.h"
#include "turbo_error.h"

/* Deliberately malformed: the loader must reject an API without close(). */
static int flowie_storage_plugin_invalid_open(
    void *ctx, const turbo_flow_storage_backend_open_request_t *request,
    turbo_flow_storage_backend_service_t *service, turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)request;
  (void)service;
  (void)error;
  return TURBO_ENOTSUP;
}

static const turbo_flow_storage_backend_plugin_api_t FLOWIE_STORAGE_PLUGIN_INVALID_API = {
    sizeof(turbo_flow_storage_backend_plugin_api_t),
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
    "invalid-storage-plugin",
    TURBO_FLOW_STORAGE_CAP_RECORD,
    NULL,
    flowie_storage_plugin_invalid_open,
    NULL};

CXX_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_plugin_get_api(void) {
  return &FLOWIE_STORAGE_PLUGIN_INVALID_API;
}
