#ifndef FLOWIE_WORKER_RUNTIME_INTERNAL_H
#define FLOWIE_WORKER_RUNTIME_INTERNAL_H

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_security.h"
#include "turbo_flow_storage_backend.h"

#include <stddef.h>

typedef struct flowie_worker_runtime_s flowie_worker_runtime_t;

typedef struct flowie_worker_runtime_config_s {
  size_t size;
  const char *profile;
  const char *config_path;
  const char *graph_path;
  turbo_flow_storage_backend_registry_t *storage_backends;
  /** Product-owned source/sink adapter providers beyond Flowie's core adapters. */
  const turbo_flow_product_adapter_provider_t *adapter_providers;
  size_t adapter_provider_count;
  const turbo_flow_security_auth_provider_factory_t *const *auth_provider_factories;
  size_t auth_provider_factory_count;
  const turbo_flow_security_policy_provider_factory_t *const *policy_provider_factories;
  size_t policy_provider_factory_count;
} flowie_worker_runtime_config_t;

#define FLOWIE_WORKER_RUNTIME_CONFIG_INIT                                                          \
  {sizeof(flowie_worker_runtime_config_t), "flowie", NULL, NULL, NULL, NULL, 0u, NULL, 0u, NULL,   \
   0u}

typedef enum flowie_worker_error_detail_e {
  FLOWIE_WORKER_ERROR_NONE = 0,
  FLOWIE_WORKER_ERROR_CONFIG,
  FLOWIE_WORKER_ERROR_FLOW
} flowie_worker_error_detail_t;

typedef struct flowie_worker_error_s {
  size_t size;
  const char *operation;
  int status;
  flowie_worker_error_detail_t detail;
  turbo_flow_config_error_t config;
  turbo_flow_error_t flow;
} flowie_worker_error_t;

#define FLOWIE_WORKER_ERROR_INIT                                                                   \
  {                                                                                                \
    sizeof(flowie_worker_error_t), NULL, TURBO_OK, FLOWIE_WORKER_ERROR_NONE,                       \
        TURBO_FLOW_CONFIG_ERROR_INIT, {                                                            \
      TURBO_OK, 0u, 0u, {0}                                                                        \
    }                                                                                              \
  }

/** Build one prepared worker from exactly one resolved config and graph. */
int flowie_worker_runtime_create(const flowie_worker_runtime_config_t *config, flowie_worker_runtime_t **out,
                                 flowie_worker_error_t *error);

/** Start/stop the prepared Flow generation. Lifecycle calls are caller-serialized. */
int flowie_worker_runtime_start(flowie_worker_runtime_t *runtime, flowie_worker_error_t *error);
int flowie_worker_runtime_stop(flowie_worker_runtime_t *runtime, flowie_worker_error_t *error);

/** Stop if necessary, release the entire configuration generation, and invalidate runtime. */
int flowie_worker_runtime_destroy(flowie_worker_runtime_t *runtime, flowie_worker_error_t *error);

#endif /* FLOWIE_WORKER_RUNTIME_INTERNAL_H */
