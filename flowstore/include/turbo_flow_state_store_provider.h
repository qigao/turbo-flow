#ifndef TURBO_FLOW_STATE_STORE_PROVIDER_H
#define TURBO_FLOW_STATE_STORE_PROVIDER_H

#include "turbo_flow_state_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_STATE_STORE_PROVIDER_API_VERSION 1u

/** Provider callbacks own ctx and must serialize it according to their documented thread model. */
typedef struct turbo_flow_state_store_provider_ops_s {
  size_t size;
  uint32_t api_version;
  int (*close)(void *ctx);
  void (*destroy)(void *ctx);
  int (*put)(void *ctx, turbo_flow_store_bytes_t key, turbo_flow_store_bytes_t value,
             uint64_t expected_revision, uint64_t *new_revision);
  int (*get)(void *ctx, turbo_flow_store_bytes_t key, turbo_flow_state_record_t *out);
  int (*remove)(void *ctx, turbo_flow_store_bytes_t key, uint64_t expected_revision);
  int (*visit)(void *ctx, turbo_flow_state_visit_fn visit, void *visit_ctx);
  int (*stats)(const void *ctx, turbo_flow_store_stats_t *out);
} turbo_flow_state_store_provider_ops_t;

#define TURBO_FLOW_STATE_STORE_PROVIDER_OPS_INIT                                                   \
  {sizeof(turbo_flow_state_store_provider_ops_t),                                                  \
   TURBO_FLOW_STATE_STORE_PROVIDER_API_VERSION,                                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * Bind an initialized provider context to the public StateStore facade.
 * Ownership of ctx transfers only on success; destroy is called exactly once by store destroy.
 */
CXX_C_API int
turbo_flow_state_store_create_provider(const turbo_flow_state_store_provider_ops_t *ops, void *ctx,
                                       turbo_flow_state_store_t **out);

#ifdef __cplusplus
}
#endif

#endif
