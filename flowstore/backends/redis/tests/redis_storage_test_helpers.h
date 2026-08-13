#ifndef REDIS_STORAGE_TEST_HELPERS_H
#define REDIS_STORAGE_TEST_HELPERS_H

#include "turbo_error.h"
#include "turbo_flow_redis_storage_backend.h"

#include <string.h>

typedef struct redis_test_storage_entry_s {
  const void *key;
  turbo_flow_storage_backend_registry_t *registry;
  turbo_flow_storage_backend_owner_t *owner;
} redis_test_storage_entry_t;

static redis_test_storage_entry_t redis_test_storage_entries[64];

static int redis_test_storage_open_common(
    turbo_flow_storage_model_t model, const void *config, size_t config_size,
    const turbo_flow_store_limits_t *limits, const turbo_flow_resolved_config_t *resolved,
    const char *channel_name, turbo_flow_storage_backend_owner_t **owner_out,
    turbo_flow_storage_backend_registry_t **registry_out, turbo_flow_config_error_t *error_out) {
  turbo_flow_storage_backend_registry_t *registry = NULL;
  turbo_flow_storage_backend_owner_t *owner = NULL;
  turbo_flow_storage_backend_open_request_t request =
      TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
  turbo_flow_redis_storage_backend_options_t options =
      TURBO_FLOW_REDIS_STORAGE_BACKEND_OPTIONS_INIT;
  turbo_flow_config_error_t local_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_config_error_t *error = error_out ? error_out : &local_error;
  int rc;

  if (!owner_out || !registry_out) return TURBO_EINVAL;
  *owner_out = NULL;
  *registry_out = NULL;
  rc = turbo_flow_storage_backend_registry_create(1u, &registry);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_registry_register(
      registry, turbo_flow_redis_storage_backend_api());
  if (rc != TURBO_OK) goto fail;
  request.model = model;
  request.resolved = resolved;
  request.channel_name = channel_name;
  if (resolved) {
    request.options = NULL;
    request.options_size = 0u;
  } else {
    options.config = config;
    options.config_size = config_size;
    options.limits = limits;
    request.options = &options;
    request.options_size = sizeof(options);
  }
  rc = turbo_flow_storage_backend_owner_create_registered(registry, "redis", &request, &owner,
                                                          error);
  if (rc != TURBO_OK) goto fail;
  *owner_out = owner;
  *registry_out = registry;
  return TURBO_OK;

fail:
  turbo_flow_storage_backend_owner_destroy(owner);
  (void)turbo_flow_storage_backend_registry_destroy(registry);
  return rc;
}

static int redis_test_storage_track(const void *key,
                                    turbo_flow_storage_backend_registry_t *registry,
                                    turbo_flow_storage_backend_owner_t *owner) {
  for (size_t i = 0u; i < sizeof(redis_test_storage_entries) /
                              sizeof(redis_test_storage_entries[0]); ++i) {
    if (!redis_test_storage_entries[i].key) {
      redis_test_storage_entries[i].key = key;
      redis_test_storage_entries[i].registry = registry;
      redis_test_storage_entries[i].owner = owner;
      return TURBO_OK;
    }
  }
  return TURBO_ENOSPC;
}

static int redis_test_storage_open_pointer(
    turbo_flow_storage_model_t model, const void *config, size_t config_size,
    const turbo_flow_store_limits_t *limits, void **out) {
  turbo_flow_storage_backend_owner_t *owner = NULL;
  turbo_flow_storage_backend_registry_t *registry = NULL;
  void *service = NULL;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = redis_test_storage_open_common(model, config, config_size, limits, NULL, NULL, &owner,
                                      &registry, NULL);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_owner_service(owner, model, &service);
  if (rc == TURBO_OK) rc = redis_test_storage_track(service, registry, owner);
  if (rc != TURBO_OK) {
    turbo_flow_storage_backend_owner_destroy(owner);
    (void)turbo_flow_storage_backend_registry_destroy(registry);
    return rc;
  }
  *out = service;
  return TURBO_OK;
}

static int redis_test_storage_open_record(
    const void *config, size_t config_size, const turbo_flow_resolved_config_t *resolved,
    const char *channel_name, turbo_flow_record_store_t *out, turbo_flow_config_error_t *error) {
  turbo_flow_storage_backend_owner_t *owner = NULL;
  turbo_flow_storage_backend_registry_t *registry = NULL;
  void *service = NULL;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  rc = redis_test_storage_open_common(TURBO_FLOW_STORAGE_MODEL_RECORD, config, config_size, NULL,
                                      resolved, channel_name, &owner, &registry, error);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_storage_backend_owner_service(owner, TURBO_FLOW_STORAGE_MODEL_RECORD, &service);
  if (rc == TURBO_OK) {
    *out = *(const turbo_flow_record_store_t *)service;
    rc = redis_test_storage_track(out, registry, owner);
  }
  if (rc != TURBO_OK) {
    turbo_flow_storage_backend_owner_destroy(owner);
    (void)turbo_flow_storage_backend_registry_destroy(registry);
  }
  return rc;
}

static int redis_test_storage_destroy(const void *key) {
  if (!key) return TURBO_EINVAL;
  for (size_t i = 0u; i < sizeof(redis_test_storage_entries) /
                              sizeof(redis_test_storage_entries[0]); ++i) {
    redis_test_storage_entry_t *entry = &redis_test_storage_entries[i];
    if (entry->key == key) {
      turbo_flow_storage_backend_owner_destroy(entry->owner);
      (void)turbo_flow_storage_backend_registry_destroy(entry->registry);
      memset(entry, 0, sizeof(*entry));
      return TURBO_OK;
    }
  }
  return TURBO_EINVAL;
}

#define redis_test_state_store_open(config, limits, out)                                  \
  redis_test_storage_open_pointer(TURBO_FLOW_STORAGE_MODEL_STATE, (config),                \
                                  sizeof(turbo_flow_redis_record_store_config_t), (limits), \
                                  (void **)(out))
#define redis_test_index_store_open(config, limits, out)                                  \
  redis_test_storage_open_pointer(TURBO_FLOW_STORAGE_MODEL_INDEX, (config),                \
                                  sizeof(turbo_flow_redis_index_store_config_t), (limits), \
                                  (void **)(out))
#define redis_test_log_store_open(config, limits, out)                                    \
  redis_test_storage_open_pointer(TURBO_FLOW_STORAGE_MODEL_LOG, (config),                 \
                                  sizeof(turbo_flow_redis_log_store_config_t), (limits),    \
                                  (void **)(out))
#define redis_test_record_store_open(config, out)                                         \
  redis_test_storage_open_record((config), sizeof(turbo_flow_redis_record_store_config_t), \
                                 NULL, NULL, (out), NULL)
#define redis_test_record_store_open_resolved(resolved, channel, out)                    \
  redis_test_storage_open_record(NULL, 0u, (resolved), (channel), (out), NULL)
#define redis_test_record_store_open_resolved_ex(resolved, channel, out, error)            \
  redis_test_storage_open_record(NULL, 0u, (resolved), (channel), (out), (error))

#endif
