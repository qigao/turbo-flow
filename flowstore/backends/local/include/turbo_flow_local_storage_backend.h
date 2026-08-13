#ifndef TURBO_FLOW_LOCAL_STORAGE_BACKEND_H
#define TURBO_FLOW_LOCAL_STORAGE_BACKEND_H

#include "turbo_flow_storage_backend.h"
#include "turbo_flow_series_store.h"
#include "turbo_flow_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_VERSION 1u

typedef struct turbo_flow_local_storage_backend_options_s {
  size_t size;
  uint32_t version;
  const turbo_flow_store_limits_t *limits;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_records;
  const turbo_flow_series_config_t *series_config;
} turbo_flow_local_storage_backend_options_t;

#define TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_INIT                                          \
  {sizeof(turbo_flow_local_storage_backend_options_t),                                         \
   TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_VERSION, NULL, 0u, 0u, 0u, 0u, NULL}

/**
 * Return the builtin volatile local storage backend function table.
 *
 * The backend owns process-local state through the common storage owner ABI. Its Record service is
 * atomic but not durable and must not be used for a durable external session-store binding.
 */
CXX_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_local_storage_backend_api(void);

#ifdef __cplusplus
}
#endif

#endif
