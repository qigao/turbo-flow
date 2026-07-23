#include "turbo_flow_index_store_provider.h"

#include <stdlib.h>

struct turbo_flow_index_store_s {
  turbo_flow_index_store_provider_ops_t ops;
  void *ctx;
  int closed;
};

int turbo_flow_index_store_create_provider(const turbo_flow_index_store_provider_ops_t *ops,
                                           void *ctx, turbo_flow_index_store_t **out) {
  turbo_flow_index_store_t *store;
  if (!ops || ops->size < sizeof(*ops) ||
      ops->api_version != TURBO_FLOW_INDEX_STORE_PROVIDER_API_VERSION || !ctx || !out ||
      !ops->close || !ops->destroy || !ops->add || !ops->remove || !ops->contains || !ops->count ||
      !ops->visit || !ops->intersection_count || !ops->stats) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  store = (turbo_flow_index_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->ops = *ops;
  store->ctx = ctx;
  *out = store;
  return TURBO_OK;
}

int turbo_flow_index_store_close(turbo_flow_index_store_t *store) {
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  rc = store->ops.close(store->ctx);
  if (rc == TURBO_OK) store->closed = 1;
  return rc;
}

void turbo_flow_index_store_destroy(turbo_flow_index_store_t *store) {
  if (!store) return;
  store->ops.destroy(store->ctx);
  free(store);
}

int turbo_flow_index_store_add(turbo_flow_index_store_t *store, turbo_flow_store_bytes_t index,
                               turbo_flow_store_bytes_t member) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.add(store->ctx, index, member);
}

int turbo_flow_index_store_remove(turbo_flow_index_store_t *store, turbo_flow_store_bytes_t index,
                                  turbo_flow_store_bytes_t member) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.remove(store->ctx, index, member);
}

int turbo_flow_index_store_contains(turbo_flow_index_store_t *store, turbo_flow_store_bytes_t index,
                                    turbo_flow_store_bytes_t member, int *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.contains(store->ctx, index, member, out);
}

int turbo_flow_index_store_count(turbo_flow_index_store_t *store, turbo_flow_store_bytes_t index,
                                 size_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.count(store->ctx, index, out);
}

int turbo_flow_index_store_visit(turbo_flow_index_store_t *store, turbo_flow_store_bytes_t index,
                                 turbo_flow_index_visit_fn visit, void *ctx) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.visit(store->ctx, index, visit, ctx);
}

int turbo_flow_index_store_intersection_count(turbo_flow_index_store_t *store,
                                              const turbo_flow_store_bytes_t *indices,
                                              size_t index_count, size_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.intersection_count(store->ctx, indices, index_count, out);
}

int turbo_flow_index_store_stats(const turbo_flow_index_store_t *store,
                                 turbo_flow_store_stats_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.stats(store->ctx, out);
}
