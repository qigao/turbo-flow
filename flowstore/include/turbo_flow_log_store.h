#ifndef TURBO_FLOW_LOG_STORE_H
#define TURBO_FLOW_LOG_STORE_H

#include "turbo_buffer.h"
#include "turbo_flow_store.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_log_store_s turbo_flow_log_store_t;

/** Memory stores have one mutable owner and are not thread-safe. */

typedef struct turbo_flow_log_record_s {
  size_t size;
  uint32_t abi_version;
  uint64_t cursor;
  uint64_t timestamp_ms;
  mem_buffer_t *payload;
} turbo_flow_log_record_t;

#define TURBO_FLOW_LOG_RECORD_INIT                                                                 \
  {sizeof(turbo_flow_log_record_t), TURBO_FLOW_STORE_ABI_VERSION, 0u, 0u, NULL}

CXX_C_API int turbo_flow_log_store_create_memory(const turbo_flow_store_limits_t *limits,
                                                 turbo_flow_log_store_t **out);
CXX_C_API int turbo_flow_log_store_close(turbo_flow_log_store_t *store);
CXX_C_API void turbo_flow_log_store_destroy(turbo_flow_log_store_t *store);
CXX_C_API int turbo_flow_log_store_append(turbo_flow_log_store_t *store, uint64_t timestamp_ms,
                                          turbo_flow_store_bytes_t payload, uint64_t *cursor);
/** cursor == 0 starts at the current head. A trimmed cursor returns TURBO_ERANGE. */
CXX_C_API int turbo_flow_log_store_read(turbo_flow_log_store_t *store, uint64_t cursor,
                                        turbo_flow_log_record_t *records, size_t capacity,
                                        size_t *count);
CXX_C_API int turbo_flow_log_store_trim_before_cursor(turbo_flow_log_store_t *store,
                                                      uint64_t cursor, size_t *trimmed);
CXX_C_API int turbo_flow_log_store_trim_before_time(turbo_flow_log_store_t *store,
                                                    uint64_t timestamp_ms, size_t *trimmed);
CXX_C_API int turbo_flow_log_store_bounds(turbo_flow_log_store_t *store, uint64_t *head,
                                          uint64_t *tail);
CXX_C_API int turbo_flow_log_store_stats(const turbo_flow_log_store_t *store,
                                         turbo_flow_store_stats_t *out);
CXX_C_API void turbo_flow_log_record_cleanup(turbo_flow_log_record_t *record);

#ifdef __cplusplus
}
#endif

#endif
