#ifndef TURBO_FLOW_PGSQL_STORAGE_BACKEND_H
#define TURBO_FLOW_PGSQL_STORAGE_BACKEND_H

#include "turbo_flow_storage_backend.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_PGSQL_STORAGE_BACKEND_OPTIONS_VERSION 1u

typedef struct turbo_flow_pgsql_record_store_config_s turbo_flow_pgsql_record_store_config_t;

typedef struct turbo_flow_pgsql_storage_backend_options_s {
  size_t size;
  uint32_t version;
  const turbo_flow_pgsql_record_store_config_t *config;
} turbo_flow_pgsql_storage_backend_options_t;

#define TURBO_FLOW_PGSQL_STORAGE_BACKEND_OPTIONS_INIT                                              \
  {sizeof(turbo_flow_pgsql_storage_backend_options_t),                                             \
   TURBO_FLOW_PGSQL_STORAGE_BACKEND_OPTIONS_VERSION, NULL}

/** Return PostgreSQL's limited RECORD storage backend function table. */
CXX_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_pgsql_storage_backend_api(void);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PGSQL_STORAGE_BACKEND_H */
