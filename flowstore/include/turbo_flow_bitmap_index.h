#ifndef TURBO_FLOW_BITMAP_INDEX_H
#define TURBO_FLOW_BITMAP_INDEX_H

#include "turbo_flow_export.h"
#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Derived uint64 membership index backed by CRoaring.
 *
 * This index is not a fact store: callers must be able to rebuild it from State/Index facts.
 * Instances have one mutable owner and are not thread-safe.
 */
typedef struct turbo_flow_bitmap_index_s turbo_flow_bitmap_index_t;

/** Create an index with a non-zero hard cardinality limit. */
TURBO_FLOW_C_API int turbo_flow_bitmap_index_create(size_t max_members,
                                             turbo_flow_bitmap_index_t **out);
TURBO_FLOW_C_API void turbo_flow_bitmap_index_destroy(turbo_flow_bitmap_index_t *index);

/** Idempotently add/remove one integer member. */
TURBO_FLOW_C_API int turbo_flow_bitmap_index_add(turbo_flow_bitmap_index_t *index, uint64_t member);
TURBO_FLOW_C_API int turbo_flow_bitmap_index_remove(turbo_flow_bitmap_index_t *index, uint64_t member);

TURBO_FLOW_C_API int turbo_flow_bitmap_index_contains(const turbo_flow_bitmap_index_t *index,
                                                uint64_t member, int *out);
TURBO_FLOW_C_API int turbo_flow_bitmap_index_count(const turbo_flow_bitmap_index_t *index, size_t *out);

/** Select the zero-based member rank in ascending order; an invalid rank returns TURBO_ERANGE. */
TURBO_FLOW_C_API int turbo_flow_bitmap_index_select(const turbo_flow_bitmap_index_t *index, size_t rank,
                                              uint64_t *out);

#ifdef __cplusplus
}
#endif

#endif
