#include "flow_store_internal.h"

#include <limits.h>
#include <string.h>

int turbo_flow_store_limits_validate(const turbo_flow_store_limits_t *limits, int allow_trim) {
  if (!limits || limits->size < sizeof(*limits) ||
      limits->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  if (limits->max_records == 0u || limits->max_bytes == 0u || limits->max_item_bytes == 0u ||
      limits->initial_records > limits->max_records || limits->max_item_bytes > limits->max_bytes) {
    return TURBO_EINVAL;
  }
  if (limits->full_policy != TURBO_FLOW_STORE_FULL_REJECT &&
      (!allow_trim || limits->full_policy != TURBO_FLOW_STORE_FULL_TRIM_OLDEST)) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int flow_store_bytes_validate(turbo_flow_store_bytes_t bytes, int allow_empty) {
  if ((!allow_empty && bytes.size == 0u) || (bytes.size > 0u && !bytes.data)) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int flow_store_size_add(size_t left, size_t right, size_t *out) {
  if (!out || left > SIZE_MAX - right) return TURBO_EFBIG;
  *out = left + right;
  return TURBO_OK;
}

int flow_store_buffer_copy(turbo_flow_store_bytes_t source, mem_buffer_t **out) {
  mem_buffer_t *buffer;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = flow_store_bytes_validate(source, 1);
  if (rc != TURBO_OK || source.size == 0u) return rc;
  buffer = mem_get_buffer(mem_global(), source.size);
  if (!buffer) return TURBO_ENOMEM;
  memcpy(mem_buffer_data(buffer), source.data, source.size);
  mem_set_used(buffer, source.size);
  *out = buffer;
  return TURBO_OK;
}

flow_store_key_t flow_store_key_from_bytes(turbo_flow_store_bytes_t bytes) {
  flow_store_key_t key;
  key.data = bytes.data;
  key.size = bytes.size;
  return key;
}

flow_store_key_t flow_store_key_from_buffer(const mem_buffer_t *buffer) {
  flow_store_key_t key;
  key.data = buffer ? (const uint8_t *)mem_buffer_const_data(buffer) : NULL;
  key.size = buffer ? mem_buffer_used(buffer) : 0u;
  return key;
}

size_t flow_store_key_hash(const void *key, size_t key_size, void *ctx) {
  const flow_store_key_t *view = (const flow_store_key_t *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(view->data, view->size, NULL);
}

bool flow_store_key_equal(const void *left, const void *right, size_t key_size, void *ctx) {
  const flow_store_key_t *a = (const flow_store_key_t *)left;
  const flow_store_key_t *b = (const flow_store_key_t *)right;
  (void)key_size;
  (void)ctx;
  return a->size == b->size && (a->size == 0u || memcmp(a->data, b->data, a->size) == 0);
}

int flow_store_stats_write(turbo_flow_store_stats_t *stats, size_t records, size_t bytes) {
  if (!stats) return TURBO_EINVAL;
  stats->records = records;
  stats->bytes = bytes;
  if (records > stats->peak_records) stats->peak_records = records;
  if (bytes > stats->peak_bytes) stats->peak_bytes = bytes;
  return TURBO_OK;
}

int flow_store_stats_copy(const turbo_flow_store_stats_t *source, turbo_flow_store_stats_t *out) {
  size_t output_size;
  if (!source || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  output_size = out->size;
  *out = *source;
  out->size = output_size;
  return TURBO_OK;
}
