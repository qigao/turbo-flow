#ifndef TURBO_FLOW_INDEX_STORE_PROVIDER_H
#define TURBO_FLOW_INDEX_STORE_PROVIDER_H

#include "turbo_flow_index_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_INDEX_STORE_PROVIDER_API_VERSION 1u

typedef struct turbo_flow_index_store_provider_ops_s {
  size_t size;
  uint32_t api_version;
  int (*close)(void *ctx);
  void (*destroy)(void *ctx);
  int (*add)(void *ctx, turbo_flow_store_bytes_t index, turbo_flow_store_bytes_t member);
  int (*remove)(void *ctx, turbo_flow_store_bytes_t index, turbo_flow_store_bytes_t member);
  int (*contains)(void *ctx, turbo_flow_store_bytes_t index, turbo_flow_store_bytes_t member,
                  int *out);
  int (*count)(void *ctx, turbo_flow_store_bytes_t index, size_t *out);
  int (*visit)(void *ctx, turbo_flow_store_bytes_t index, turbo_flow_index_visit_fn visit,
               void *visit_ctx);
  int (*intersection_count)(void *ctx, const turbo_flow_store_bytes_t *indices, size_t index_count,
                            size_t *out);
  int (*stats)(const void *ctx, turbo_flow_store_stats_t *out);
} turbo_flow_index_store_provider_ops_t;

#define TURBO_FLOW_INDEX_STORE_PROVIDER_OPS_INIT                                                   \
  {sizeof(turbo_flow_index_store_provider_ops_t),                                                  \
   TURBO_FLOW_INDEX_STORE_PROVIDER_API_VERSION,                                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

CXX_C_API int
turbo_flow_index_store_create_provider(const turbo_flow_index_store_provider_ops_t *ops, void *ctx,
                                       turbo_flow_index_store_t **out);

#ifdef __cplusplus
}
#endif

#endif
