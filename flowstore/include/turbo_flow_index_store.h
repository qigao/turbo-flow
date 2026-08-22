#ifndef TURBO_FLOW_INDEX_STORE_H
#define TURBO_FLOW_INDEX_STORE_H

#include "turbo_flow_store.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_index_store_s turbo_flow_index_store_t;

/** Memory stores have one mutable owner and are not thread-safe. */

/** Borrowed member data is valid only during the callback; the callback must not mutate the store.
 */
typedef int (*turbo_flow_index_visit_fn)(void *ctx, turbo_flow_store_bytes_t member);

TURBO_FLOW_C_API int turbo_flow_index_store_create_memory(const turbo_flow_store_limits_t *limits,
                                                   turbo_flow_index_store_t **out);
TURBO_FLOW_C_API int turbo_flow_index_store_close(turbo_flow_index_store_t *store);
TURBO_FLOW_C_API void turbo_flow_index_store_destroy(turbo_flow_index_store_t *store);
TURBO_FLOW_C_API int turbo_flow_index_store_add(turbo_flow_index_store_t *store,
                                         turbo_flow_store_bytes_t index,
                                         turbo_flow_store_bytes_t member);
TURBO_FLOW_C_API int turbo_flow_index_store_remove(turbo_flow_index_store_t *store,
                                            turbo_flow_store_bytes_t index,
                                            turbo_flow_store_bytes_t member);
TURBO_FLOW_C_API int turbo_flow_index_store_contains(turbo_flow_index_store_t *store,
                                              turbo_flow_store_bytes_t index,
                                              turbo_flow_store_bytes_t member, int *out);
TURBO_FLOW_C_API int turbo_flow_index_store_count(turbo_flow_index_store_t *store,
                                           turbo_flow_store_bytes_t index, size_t *out);
TURBO_FLOW_C_API int turbo_flow_index_store_visit(turbo_flow_index_store_t *store,
                                           turbo_flow_store_bytes_t index,
                                           turbo_flow_index_visit_fn visit, void *ctx);
/** Count members present in every named index; query time is O(min_members * index_count). */
TURBO_FLOW_C_API int turbo_flow_index_store_intersection_count(turbo_flow_index_store_t *store,
                                                        const turbo_flow_store_bytes_t *indices,
                                                        size_t index_count, size_t *out);
TURBO_FLOW_C_API int turbo_flow_index_store_stats(const turbo_flow_index_store_t *store,
                                           turbo_flow_store_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif
