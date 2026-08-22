#ifndef TURBO_FLOW_REDIS_STORAGE_BACKEND_H
#define TURBO_FLOW_REDIS_STORAGE_BACKEND_H

#include "turbo_flow_redis.h"
#include "turbo_flow_storage_backend.h"
#include "turbo_flow_store.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_REDIS_STORAGE_BACKEND_OPTIONS_VERSION 1u

typedef struct turbo_flow_redis_record_store_config_s turbo_flow_redis_record_store_config_t;

#define TURBO_FLOW_REDIS_INDEX_DEFAULT_MAX_NAME_SIZE 256u
#define TURBO_FLOW_REDIS_INDEX_DEFAULT_MAX_MEMBER_SIZE (1024u * 1024u)

typedef struct turbo_flow_redis_index_store_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  const char *key;
  size_t max_index_name_size;
  size_t max_member_size;
  turbo_flow_redis_connection_config_t connection;
} turbo_flow_redis_index_store_config_t;

#define TURBO_FLOW_REDIS_LOG_DEFAULT_MAX_OPERATION_RECORDS 4096u
#define TURBO_FLOW_REDIS_LOG_MAX_OPERATION_RECORDS 65535u

typedef struct turbo_flow_redis_log_store_config_s {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  int database;
  uint32_t timeout_ms;
  const char *key;
  size_t max_operation_records;
  turbo_flow_redis_connection_config_t connection;
} turbo_flow_redis_log_store_config_t;

/**
 * Direct-open options for Redis storage services.
 *
 * config points to the config matching request.model. limits is required for State, Index, and
 * Log and must be NULL for Record. All pointers are borrowed only during open().
 */
typedef struct turbo_flow_redis_storage_backend_options_s {
  size_t size;
  uint32_t version;
  const void *config;
  size_t config_size;
  const turbo_flow_store_limits_t *limits;
} turbo_flow_redis_storage_backend_options_t;

#define TURBO_FLOW_REDIS_STORAGE_BACKEND_OPTIONS_INIT                                              \
  {sizeof(turbo_flow_redis_storage_backend_options_t),                                             \
   TURBO_FLOW_REDIS_STORAGE_BACKEND_OPTIONS_VERSION, NULL, 0u, NULL}

/** Return Redis's limited RECORD|STATE|INDEX|LOG storage backend function table. */
TURBO_FLOW_C_API const turbo_flow_storage_backend_plugin_api_t *turbo_flow_redis_storage_backend_api(void);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_REDIS_STORAGE_BACKEND_H */
