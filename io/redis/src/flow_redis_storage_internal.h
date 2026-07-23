#ifndef FLOW_REDIS_STORAGE_INTERNAL_H
#define FLOW_REDIS_STORAGE_INTERNAL_H

#include "turbo_flow_redis_storage_backend.h"
#include "turbo_flow_index_store.h"
#include "turbo_flow_log_store.h"
#include "turbo_flow_redis.h"
#include "turbo_flow_state_store.h"

#if defined(__GNUC__) || defined(__clang__)
  #define FLOW_REDIS_INTERNAL_API __attribute__((visibility("hidden")))
#else
  #define FLOW_REDIS_INTERNAL_API
#endif

FLOW_REDIS_INTERNAL_API int
flow_redis_record_store_create(const turbo_flow_redis_record_store_config_t *config,
                               turbo_flow_record_store_t *out);
FLOW_REDIS_INTERNAL_API int flow_redis_record_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, turbo_flow_config_error_t *error);
FLOW_REDIS_INTERNAL_API void flow_redis_record_store_destroy(turbo_flow_record_store_t *store);

FLOW_REDIS_INTERNAL_API int
flow_redis_state_store_create(const turbo_flow_redis_record_store_config_t *config,
                              const turbo_flow_store_limits_t *limits,
                              turbo_flow_state_store_t **out);
FLOW_REDIS_INTERNAL_API int
flow_redis_index_store_create(const turbo_flow_redis_index_store_config_t *config,
                              const turbo_flow_store_limits_t *limits,
                              turbo_flow_index_store_t **out);
FLOW_REDIS_INTERNAL_API int
flow_redis_log_store_create(const turbo_flow_redis_log_store_config_t *config,
                            const turbo_flow_store_limits_t *limits,
                            turbo_flow_log_store_t **out);

#endif /* FLOW_REDIS_STORAGE_INTERNAL_H */
