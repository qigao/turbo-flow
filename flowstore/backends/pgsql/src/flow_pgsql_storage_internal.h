#ifndef FLOW_PGSQL_STORAGE_INTERNAL_H
#define FLOW_PGSQL_STORAGE_INTERNAL_H

#include "turbo_flow_pgsql_storage_backend.h"
#include "turbo_flow_pgsql.h"

#if defined(__GNUC__) || defined(__clang__)
  #define FLOW_PGSQL_INTERNAL_API __attribute__((visibility("hidden")))
#else
  #define FLOW_PGSQL_INTERNAL_API
#endif

FLOW_PGSQL_INTERNAL_API int
flow_pgsql_record_store_create(const turbo_flow_pgsql_record_store_config_t *config,
                               turbo_flow_record_store_t *out);
FLOW_PGSQL_INTERNAL_API int flow_pgsql_record_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, turbo_flow_config_error_t *error);
FLOW_PGSQL_INTERNAL_API void flow_pgsql_record_store_destroy(turbo_flow_record_store_t *store);

#endif /* FLOW_PGSQL_STORAGE_INTERNAL_H */
