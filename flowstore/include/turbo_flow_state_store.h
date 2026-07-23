#ifndef TURBO_FLOW_STATE_STORE_H
#define TURBO_FLOW_STATE_STORE_H

#include "turbo_buffer.h"
#include "turbo_flow_store.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_state_store_s turbo_flow_state_store_t;

/** Memory stores have one mutable owner and are not thread-safe. */

typedef struct turbo_flow_state_record_s {
  size_t size;
  uint32_t abi_version;
  uint64_t revision;
  mem_buffer_t *value;
} turbo_flow_state_record_t;

#define TURBO_FLOW_STATE_RECORD_INIT                                                               \
  {sizeof(turbo_flow_state_record_t), TURBO_FLOW_STORE_ABI_VERSION, 0u, NULL}

/** Borrowed visit data is valid only during the callback. The callback must not mutate the store.
 */
typedef int (*turbo_flow_state_visit_fn)(void *ctx, turbo_flow_store_bytes_t key,
                                         turbo_flow_store_bytes_t value, uint64_t revision);

CXX_C_API int turbo_flow_state_store_create_memory(const turbo_flow_store_limits_t *limits,
                                                   turbo_flow_state_store_t **out);
CXX_C_API int turbo_flow_state_store_close(turbo_flow_state_store_t *store);
CXX_C_API void turbo_flow_state_store_destroy(turbo_flow_state_store_t *store);

/**
 * expected_revision == 0 creates an absent key; otherwise it must equal the current revision.
 * Revision mismatch returns TURBO_EBUSY without changing state.
 */
CXX_C_API int turbo_flow_state_store_put(turbo_flow_state_store_t *store,
                                         turbo_flow_store_bytes_t key,
                                         turbo_flow_store_bytes_t value, uint64_t expected_revision,
                                         uint64_t *new_revision);
CXX_C_API int turbo_flow_state_store_get(turbo_flow_state_store_t *store,
                                         turbo_flow_store_bytes_t key,
                                         turbo_flow_state_record_t *out);
CXX_C_API int turbo_flow_state_store_remove(turbo_flow_state_store_t *store,
                                            turbo_flow_store_bytes_t key,
                                            uint64_t expected_revision);
CXX_C_API int turbo_flow_state_store_visit(turbo_flow_state_store_t *store,
                                           turbo_flow_state_visit_fn visit, void *ctx);
CXX_C_API int turbo_flow_state_store_stats(const turbo_flow_state_store_t *store,
                                           turbo_flow_store_stats_t *out);
CXX_C_API void turbo_flow_state_record_cleanup(turbo_flow_state_record_t *record);

#ifdef __cplusplus
}
#endif

#endif
