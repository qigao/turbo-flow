#include "turbo_flow_series_store_provider.h"

#include <stdlib.h>

struct turbo_flow_series_store_s {
  turbo_flow_series_store_provider_ops_t ops;
  void *ctx;
  int closed;
};

int turbo_flow_series_store_create_provider(const turbo_flow_series_store_provider_ops_t *ops,
                                            void *ctx, turbo_flow_series_store_t **out) {
  turbo_flow_series_store_t *store;
  if (!ops || ops->size < sizeof(*ops) ||
      ops->api_version != TURBO_FLOW_SERIES_STORE_PROVIDER_API_VERSION || !ctx || !out ||
      !ops->close || !ops->destroy || !ops->append || !ops->range || !ops->aggregate ||
      !ops->trim_before || !ops->stats) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  store = (turbo_flow_series_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->ops = *ops;
  store->ctx = ctx;
  *out = store;
  return TURBO_OK;
}

int turbo_flow_series_store_close(turbo_flow_series_store_t *store) {
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  rc = store->ops.close(store->ctx);
  if (rc == TURBO_OK) store->closed = 1;
  return rc;
}

void turbo_flow_series_store_destroy(turbo_flow_series_store_t *store) {
  if (!store) return;
  store->ops.destroy(store->ctx);
  free(store);
}

int turbo_flow_series_store_append(turbo_flow_series_store_t *store,
                                   turbo_flow_store_bytes_t series,
                                   const turbo_flow_series_sample_t *sample) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.append(store->ctx, series, sample);
}

int turbo_flow_series_store_range(turbo_flow_series_store_t *store, turbo_flow_store_bytes_t series,
                                  uint64_t start_ms, uint64_t end_ms,
                                  turbo_flow_series_sample_t *samples, size_t capacity,
                                  size_t *count) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.range(store->ctx, series, start_ms, end_ms, samples, capacity, count);
}

int turbo_flow_series_store_aggregate(turbo_flow_series_store_t *store,
                                      turbo_flow_store_bytes_t series, uint64_t start_ms,
                                      uint64_t end_ms, turbo_flow_series_aggregate_t aggregate,
                                      double *value, size_t *sample_count) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.aggregate(store->ctx, series, start_ms, end_ms, aggregate, value,
                              sample_count);
}

int turbo_flow_series_store_trim_before(turbo_flow_series_store_t *store,
                                        turbo_flow_store_bytes_t series, uint64_t timestamp_ms,
                                        size_t *trimmed) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.trim_before(store->ctx, series, timestamp_ms, trimmed);
}

int turbo_flow_series_store_stats(const turbo_flow_series_store_t *store,
                                  turbo_flow_store_stats_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.stats(store->ctx, out);
}
