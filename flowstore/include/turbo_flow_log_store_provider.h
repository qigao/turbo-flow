#ifndef TURBO_FLOW_LOG_STORE_PROVIDER_H
#define TURBO_FLOW_LOG_STORE_PROVIDER_H

#include "turbo_flow_log_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_LOG_STORE_PROVIDER_API_VERSION 1u

typedef struct turbo_flow_log_store_provider_ops_s {
  size_t size;
  uint32_t api_version;
  int (*close)(void *ctx);
  void (*destroy)(void *ctx);
  int (*append)(void *ctx, uint64_t timestamp_ms, turbo_flow_store_bytes_t payload,
                uint64_t *cursor);
  int (*read)(void *ctx, uint64_t cursor, turbo_flow_log_record_t *records, size_t capacity,
              size_t *count);
  int (*trim_before_cursor)(void *ctx, uint64_t cursor, size_t *trimmed);
  int (*trim_before_time)(void *ctx, uint64_t timestamp_ms, size_t *trimmed);
  int (*bounds)(void *ctx, uint64_t *head, uint64_t *tail);
  int (*stats)(const void *ctx, turbo_flow_store_stats_t *out);
} turbo_flow_log_store_provider_ops_t;

#define TURBO_FLOW_LOG_STORE_PROVIDER_OPS_INIT                                                     \
  {sizeof(turbo_flow_log_store_provider_ops_t),                                                    \
   TURBO_FLOW_LOG_STORE_PROVIDER_API_VERSION,                                                      \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

CXX_C_API int turbo_flow_log_store_create_provider(const turbo_flow_log_store_provider_ops_t *ops,
                                                   void *ctx, turbo_flow_log_store_t **out);

#ifdef __cplusplus
}
#endif

#endif
