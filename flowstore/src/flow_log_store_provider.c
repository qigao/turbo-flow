#include "turbo_flow_log_store_provider.h"

#include <stdlib.h>

struct turbo_flow_log_store_s {
  turbo_flow_log_store_provider_ops_t ops;
  void *ctx;
  int closed;
};

int turbo_flow_log_store_create_provider(const turbo_flow_log_store_provider_ops_t *ops, void *ctx,
                                         turbo_flow_log_store_t **out) {
  turbo_flow_log_store_t *store;
  if (!ops || ops->size < sizeof(*ops) ||
      ops->api_version != TURBO_FLOW_LOG_STORE_PROVIDER_API_VERSION || !ctx || !out ||
      !ops->close || !ops->destroy || !ops->append || !ops->read || !ops->trim_before_cursor ||
      !ops->trim_before_time || !ops->bounds || !ops->stats) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  store = (turbo_flow_log_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->ops = *ops;
  store->ctx = ctx;
  *out = store;
  return TURBO_OK;
}

int turbo_flow_log_store_close(turbo_flow_log_store_t *store) {
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  rc = store->ops.close(store->ctx);
  if (rc == TURBO_OK) store->closed = 1;
  return rc;
}

void turbo_flow_log_store_destroy(turbo_flow_log_store_t *store) {
  if (!store) return;
  store->ops.destroy(store->ctx);
  free(store);
}

int turbo_flow_log_store_append(turbo_flow_log_store_t *store, uint64_t timestamp_ms,
                                turbo_flow_store_bytes_t payload, uint64_t *cursor) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.append(store->ctx, timestamp_ms, payload, cursor);
}

int turbo_flow_log_store_read(turbo_flow_log_store_t *store, uint64_t cursor,
                              turbo_flow_log_record_t *records, size_t capacity, size_t *count) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.read(store->ctx, cursor, records, capacity, count);
}

int turbo_flow_log_store_trim_before_cursor(turbo_flow_log_store_t *store, uint64_t cursor,
                                            size_t *trimmed) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.trim_before_cursor(store->ctx, cursor, trimmed);
}

int turbo_flow_log_store_trim_before_time(turbo_flow_log_store_t *store, uint64_t timestamp_ms,
                                          size_t *trimmed) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.trim_before_time(store->ctx, timestamp_ms, trimmed);
}

int turbo_flow_log_store_bounds(turbo_flow_log_store_t *store, uint64_t *head, uint64_t *tail) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.bounds(store->ctx, head, tail);
}

int turbo_flow_log_store_stats(const turbo_flow_log_store_t *store, turbo_flow_store_stats_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.stats(store->ctx, out);
}

void turbo_flow_log_record_cleanup(turbo_flow_log_record_t *record) {
  if (!record) return;
  mem_buffer_release(record->payload);
  record->payload = NULL;
  record->cursor = 0u;
  record->timestamp_ms = 0u;
}
