#ifndef TURBO_FLOW_SQLITE_STORAGE_BACKEND_H
#define TURBO_FLOW_SQLITE_STORAGE_BACKEND_H

#include "turbo_flow_record_store.h"
#include "turbo_flow_storage_backend.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SQLITE_STORAGE_BACKEND_OPTIONS_VERSION 1u
#define TURBO_FLOW_SQLITE_RECORD_STORE_CONFIG_VERSION 1u
#define TURBO_FLOW_SQLITE_RECORD_STORE_NAMESPACE_MAX 255u
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_KEY_SIZE (128u * 1024u)
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE (2u * 1024u * 1024u)
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE 256u
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_RECORDS 4096u
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BYTES (64u * 1024u * 1024u)
#define TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_ITEM_BYTES (2u * 1024u * 1024u)

typedef struct turbo_flow_sqlite_record_store_config_s {
  size_t size;
  uint32_t version;
  const char *database_path;
  const char *namespace_name;
  int busy_timeout_ms;
  size_t max_records;
  size_t max_bytes;
  size_t max_item_bytes;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
} turbo_flow_sqlite_record_store_config_t;

#define TURBO_FLOW_SQLITE_RECORD_STORE_CONFIG_INIT                                                 \
  {sizeof(turbo_flow_sqlite_record_store_config_t),                                                \
   TURBO_FLOW_SQLITE_RECORD_STORE_CONFIG_VERSION,                                                  \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0,                                                                                              \
   TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_RECORDS,                                             \
   TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BYTES,                                               \
   TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_ITEM_BYTES,                                          \
   TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_KEY_SIZE,                                            \
   TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE,                                          \
   TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE}

typedef struct turbo_flow_sqlite_storage_backend_options_s {
  size_t size;
  uint32_t version;
  const turbo_flow_sqlite_record_store_config_t *config;
} turbo_flow_sqlite_storage_backend_options_t;

#define TURBO_FLOW_SQLITE_STORAGE_BACKEND_OPTIONS_INIT                                             \
  {sizeof(turbo_flow_sqlite_storage_backend_options_t),                                            \
   TURBO_FLOW_SQLITE_STORAGE_BACKEND_OPTIONS_VERSION, NULL}

/**
 * Open a caller-serialized SQLite RecordStore.
 *
 * The store copies the path and namespace and owns one SQLite connection until
 * turbo_flow_sqlite_record_store_close(). A `:memory:` path is process-local, is visible only
 * through that owner, is destroyed on close, and does not advertise the durable capability.
 * File paths remain durable. Keys and values are binary-safe.
 */
TURBO_FLOW_C_API int
turbo_flow_sqlite_record_store_create(const turbo_flow_sqlite_record_store_config_t *config,
                                      turbo_flow_record_store_t *out);

/** Create from one strict `kind: record_store`, `backend: sqlite` resolved YAML channel. */
TURBO_FLOW_C_API int turbo_flow_sqlite_record_store_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    turbo_flow_record_store_t *out, turbo_flow_config_error_t *error);

/** Close the owned SQLite connection and reset the RecordStore view. */
TURBO_FLOW_C_API int turbo_flow_sqlite_record_store_close(turbo_flow_record_store_t *store);

/** Return the builtin SQLite RECORD storage backend function table. */
TURBO_FLOW_C_API const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_sqlite_storage_backend_api(void);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_SQLITE_STORAGE_BACKEND_H */
