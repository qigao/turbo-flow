#include "turbo_flow_bitmap_index.h"

#include "roaring.h"
#include "turbo_error.h"

#include <stdlib.h>

struct turbo_flow_bitmap_index_s {
  roaring64_bitmap_t *bitmap;
  size_t max_members;
};

int turbo_flow_bitmap_index_create(size_t max_members, turbo_flow_bitmap_index_t **out) {
  turbo_flow_bitmap_index_t *index;
  if (!out || max_members == 0u) return TURBO_EINVAL;
  *out = NULL;
  index = (turbo_flow_bitmap_index_t *)calloc(1u, sizeof(*index));
  if (!index) return TURBO_ENOMEM;
  index->bitmap = roaring64_bitmap_create();
  if (!index->bitmap) {
    free(index);
    return TURBO_ENOMEM;
  }
  index->max_members = max_members;
  *out = index;
  return TURBO_OK;
}

void turbo_flow_bitmap_index_destroy(turbo_flow_bitmap_index_t *index) {
  if (!index) return;
  roaring64_bitmap_free(index->bitmap);
  free(index);
}

int turbo_flow_bitmap_index_add(turbo_flow_bitmap_index_t *index, uint64_t member) {
  if (!index || !index->bitmap) return TURBO_EINVAL;
  if (roaring64_bitmap_contains(index->bitmap, member)) return TURBO_OK;
  if (roaring64_bitmap_get_cardinality(index->bitmap) >= (uint64_t)index->max_members)
    return TURBO_ENOSPC;
  roaring64_bitmap_add(index->bitmap, member);
  return roaring64_bitmap_contains(index->bitmap, member) ? TURBO_OK : TURBO_ENOMEM;
}

int turbo_flow_bitmap_index_remove(turbo_flow_bitmap_index_t *index, uint64_t member) {
  if (!index || !index->bitmap) return TURBO_EINVAL;
  roaring64_bitmap_remove(index->bitmap, member);
  return TURBO_OK;
}

int turbo_flow_bitmap_index_contains(const turbo_flow_bitmap_index_t *index, uint64_t member,
                                     int *out) {
  if (!index || !index->bitmap || !out) return TURBO_EINVAL;
  *out = roaring64_bitmap_contains(index->bitmap, member) ? 1 : 0;
  return TURBO_OK;
}

int turbo_flow_bitmap_index_count(const turbo_flow_bitmap_index_t *index, size_t *out) {
  uint64_t count;
  if (!index || !index->bitmap || !out) return TURBO_EINVAL;
  count = roaring64_bitmap_get_cardinality(index->bitmap);
  if (count > (uint64_t)SIZE_MAX) return TURBO_ERANGE;
  *out = (size_t)count;
  return TURBO_OK;
}

int turbo_flow_bitmap_index_select(const turbo_flow_bitmap_index_t *index, size_t rank,
                                   uint64_t *out) {
  if (!index || !index->bitmap || !out) return TURBO_EINVAL;
  return roaring64_bitmap_select(index->bitmap, (uint64_t)rank, out) ? TURBO_OK : TURBO_ERANGE;
}
