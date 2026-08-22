#ifndef TURBO_FLOW_SERIES_STORE_PROVIDER_H
#define TURBO_FLOW_SERIES_STORE_PROVIDER_H

#include "turbo_flow_series_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SERIES_STORE_PROVIDER_API_VERSION 1u

typedef struct turbo_flow_series_store_provider_ops_s {
  size_t size;
  uint32_t api_version;
  int (*close)(void *ctx);
  void (*destroy)(void *ctx);
  int (*append)(void *ctx, turbo_flow_store_bytes_t series,
                const turbo_flow_series_sample_t *sample);
  int (*range)(void *ctx, turbo_flow_store_bytes_t series, uint64_t start_ms, uint64_t end_ms,
               turbo_flow_series_sample_t *samples, size_t capacity, size_t *count);
  int (*aggregate)(void *ctx, turbo_flow_store_bytes_t series, uint64_t start_ms, uint64_t end_ms,
                   turbo_flow_series_aggregate_t aggregate, double *value, size_t *sample_count);
  int (*trim_before)(void *ctx, turbo_flow_store_bytes_t series, uint64_t timestamp_ms,
                     size_t *trimmed);
  int (*stats)(const void *ctx, turbo_flow_store_stats_t *out);
} turbo_flow_series_store_provider_ops_t;

#define TURBO_FLOW_SERIES_STORE_PROVIDER_OPS_INIT                                                  \
  {sizeof(turbo_flow_series_store_provider_ops_t),                                                 \
   TURBO_FLOW_SERIES_STORE_PROVIDER_API_VERSION,                                                   \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

TURBO_FLOW_C_API int
turbo_flow_series_store_create_provider(const turbo_flow_series_store_provider_ops_t *ops,
                                        void *ctx, turbo_flow_series_store_t **out);

#ifdef __cplusplus
}
#endif

#endif
