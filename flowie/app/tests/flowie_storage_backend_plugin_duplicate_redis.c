#include "turbo_flow_storage_backend.h"
#include "turbo_error.h"

static int flowie_storage_plugin_duplicate_open(
    void *ctx, const turbo_flow_storage_backend_open_request_t *request,
    turbo_flow_storage_backend_service_t *service, turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)request;
  (void)service;
  (void)error;
  return TURBO_ENOTSUP;
}

static void flowie_storage_plugin_duplicate_close(
    void *ctx, turbo_flow_storage_backend_service_t *service) {
  (void)ctx;
  (void)service;
}

static const turbo_flow_storage_backend_plugin_api_t FLOWIE_STORAGE_PLUGIN_DUPLICATE_API = {
    sizeof(turbo_flow_storage_backend_plugin_api_t),
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
    "redis",
    TURBO_FLOW_STORAGE_CAP_RECORD,
    NULL,
    flowie_storage_plugin_duplicate_open,
    flowie_storage_plugin_duplicate_close};

CXX_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_plugin_get_api(void) {
  return &FLOWIE_STORAGE_PLUGIN_DUPLICATE_API;
}
