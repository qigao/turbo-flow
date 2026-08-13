#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_storage_backend.h"

#include <stdlib.h>

typedef struct test_backend_context_s {
  size_t opens;
  size_t closes;
  int fail_open;
} test_backend_context_t;

static int test_backend_open(void *ctx,
                             const turbo_flow_storage_backend_open_request_t *request,
                             turbo_flow_storage_backend_service_t *service,
                             turbo_flow_config_error_t *error) {
  test_backend_context_t *context = (test_backend_context_t *)ctx;
  int *instance;
  (void)error;
  if (!context || !request || !service) return TURBO_EINVAL;
  instance = (int *)malloc(sizeof(*instance));
  if (!instance) return TURBO_ENOMEM;
  *instance = 42;
  context->opens++;
  service->model = request->model;
  service->instance = instance;
  service->owner = instance;
  if (context->fail_open) return TURBO_EIO;
  return TURBO_OK;
}

static void test_backend_close(void *ctx, turbo_flow_storage_backend_service_t *service) {
  test_backend_context_t *context = (test_backend_context_t *)ctx;
  if (!context || !service) return;
  free(service->owner);
  service->instance = NULL;
  service->owner = NULL;
  context->closes++;
}

spec("storage backend registry") {
  it("selects by backend and capability while retaining active owners") {
    test_backend_context_t context = {0};
    turbo_flow_storage_backend_plugin_api_t api = {
        sizeof(turbo_flow_storage_backend_plugin_api_t),
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
        "test",
        TURBO_FLOW_STORAGE_CAP_RECORD,
        &context,
        test_backend_open,
        test_backend_close};
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
    void *service = NULL;

    check_int_eq(turbo_flow_storage_backend_registry_create(2u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &api), TURBO_EALREADY);
    check_ptr_eq(turbo_flow_storage_backend_registry_find(registry, "test"), &api);
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(
                     registry, "test", &request, &owner, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_service(
                     owner, TURBO_FLOW_STORAGE_MODEL_RECORD, &service),
                 TURBO_OK);
    check_int_eq(*(const int *)service, 42);
    check_str_eq(turbo_flow_storage_backend_owner_backend(owner), "test");
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_EBUSY);
    turbo_flow_storage_backend_owner_destroy(owner);
    check_size_eq(context.opens, 1u);
    check_size_eq(context.closes, 1u);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
  }

  it("rejects unsupported models before calling a plugin") {
    test_backend_context_t context = {0};
    turbo_flow_storage_backend_plugin_api_t api = {
        sizeof(turbo_flow_storage_backend_plugin_api_t),
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
        "record-only",
        TURBO_FLOW_STORAGE_CAP_RECORD,
        &context,
        test_backend_open,
        test_backend_close};
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;

    request.model = TURBO_FLOW_STORAGE_MODEL_SERIES;
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(
                     registry, "record-only", &request, &owner, NULL),
                 TURBO_ENOTSUP);
    check_null(owner);
    check_size_eq(context.opens, 0u);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
  }

  it("cleans a service partially initialized by a failed open") {
    test_backend_context_t context = {0};
    turbo_flow_storage_backend_plugin_api_t api = {
        sizeof(turbo_flow_storage_backend_plugin_api_t),
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
        "failing-open",
        TURBO_FLOW_STORAGE_CAP_RECORD,
        &context,
        test_backend_open,
        test_backend_close};
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;

    context.fail_open = 1;
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(
                     registry, "failing-open", &request, &owner, NULL),
                 TURBO_EIO);
    check_null(owner);
    check_size_eq(context.opens, 1u);
    check_size_eq(context.closes, 1u);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
  }

  it("enforces ABI shape and bounded registration") {
    test_backend_context_t context = {0};
    turbo_flow_storage_backend_plugin_api_t api = {
        sizeof(turbo_flow_storage_backend_plugin_api_t),
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
        TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
        "bounded",
        TURBO_FLOW_STORAGE_CAP_RECORD,
        &context,
        test_backend_open,
        test_backend_close};
    turbo_flow_storage_backend_plugin_api_t invalid = api;
    turbo_flow_storage_backend_plugin_api_t second = api;
    turbo_flow_storage_backend_registry_t *registry = NULL;

    invalid.backend = "invalid";
    invalid.close = NULL;
    second.backend = "second";
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &invalid), TURBO_EINVAL);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &api), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(registry, &second), TURBO_ENOSPC);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
  }
}
