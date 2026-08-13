#ifndef PGSQL_STORAGE_TEST_HELPERS_H
#define PGSQL_STORAGE_TEST_HELPERS_H

#include "turbo_error.h"
#include "turbo_flow_pgsql_storage_backend.h"

#include <string.h>

typedef struct pgsql_test_storage_s {
  turbo_flow_storage_backend_registry_t *registry;
  turbo_flow_storage_backend_owner_t *owner;
} pgsql_test_storage_t;

static int pgsql_test_record_store_open(
    const turbo_flow_pgsql_record_store_config_t *config,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, pgsql_test_storage_t *storage,
    turbo_flow_config_error_t *error) {
  turbo_flow_storage_backend_open_request_t request =
      TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
  turbo_flow_pgsql_storage_backend_options_t options =
      TURBO_FLOW_PGSQL_STORAGE_BACKEND_OPTIONS_INIT;
  turbo_flow_storage_backend_registry_t *registry = NULL;
  turbo_flow_storage_backend_owner_t *owner = NULL;
  void *service = NULL;
  int rc;

  if (!out || !storage) return TURBO_EINVAL;
  *out = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  storage->registry = NULL;
  storage->owner = NULL;
  rc = turbo_flow_storage_backend_registry_create(1u, &registry);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_registry_register(
      registry, turbo_flow_pgsql_storage_backend_api());
  if (rc != TURBO_OK) goto fail;
  request.model = TURBO_FLOW_STORAGE_MODEL_RECORD;
  if (resolved) {
    request.resolved = resolved;
    request.channel_name = channel_name;
  } else {
    options.config = config;
    request.options = &options;
    request.options_size = sizeof(options);
  }
  rc = turbo_flow_storage_backend_owner_create_registered(registry, "postgresql", &request,
                                                          &owner, error);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_flow_storage_backend_owner_service(owner, TURBO_FLOW_STORAGE_MODEL_RECORD, &service);
  if (rc != TURBO_OK) goto fail;
  *out = *(const turbo_flow_record_store_t *)service;
  storage->registry = registry;
  storage->owner = owner;
  return TURBO_OK;

fail:
  turbo_flow_storage_backend_owner_destroy(owner);
  (void)turbo_flow_storage_backend_registry_destroy(registry);
  return rc;
}

static void pgsql_test_record_store_close(pgsql_test_storage_t *storage) {
  if (!storage) return;
  turbo_flow_storage_backend_owner_destroy(storage->owner);
  (void)turbo_flow_storage_backend_registry_destroy(storage->registry);
  storage->owner = NULL;
  storage->registry = NULL;
}

#endif
