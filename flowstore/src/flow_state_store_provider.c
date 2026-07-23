#include "turbo_flow_state_store_provider.h"

#include <stdlib.h>

struct turbo_flow_state_store_s {
  turbo_flow_state_store_provider_ops_t ops;
  void *ctx;
  int closed;
};

int turbo_flow_state_store_create_provider(const turbo_flow_state_store_provider_ops_t *ops,
                                           void *ctx, turbo_flow_state_store_t **out) {
  turbo_flow_state_store_t *store;
  if (!ops || ops->size < sizeof(*ops) ||
      ops->api_version != TURBO_FLOW_STATE_STORE_PROVIDER_API_VERSION || !ctx || !out ||
      !ops->close || !ops->destroy || !ops->put || !ops->get || !ops->remove || !ops->visit ||
      !ops->stats) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  store = (turbo_flow_state_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->ops = *ops;
  store->ctx = ctx;
  *out = store;
  return TURBO_OK;
}

int turbo_flow_state_store_close(turbo_flow_state_store_t *store) {
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  rc = store->ops.close(store->ctx);
  if (rc == TURBO_OK) store->closed = 1;
  return rc;
}

void turbo_flow_state_store_destroy(turbo_flow_state_store_t *store) {
  if (!store) return;
  store->ops.destroy(store->ctx);
  free(store);
}

int turbo_flow_state_store_put(turbo_flow_state_store_t *store, turbo_flow_store_bytes_t key,
                               turbo_flow_store_bytes_t value, uint64_t expected_revision,
                               uint64_t *new_revision) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.put(store->ctx, key, value, expected_revision, new_revision);
}

int turbo_flow_state_store_get(turbo_flow_state_store_t *store, turbo_flow_store_bytes_t key,
                               turbo_flow_state_record_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.get(store->ctx, key, out);
}

int turbo_flow_state_store_remove(turbo_flow_state_store_t *store, turbo_flow_store_bytes_t key,
                                  uint64_t expected_revision) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.remove(store->ctx, key, expected_revision);
}

int turbo_flow_state_store_visit(turbo_flow_state_store_t *store, turbo_flow_state_visit_fn visit,
                                 void *ctx) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.visit(store->ctx, visit, ctx);
}

int turbo_flow_state_store_stats(const turbo_flow_state_store_t *store,
                                 turbo_flow_store_stats_t *out) {
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  return store->ops.stats(store->ctx, out);
}

void turbo_flow_state_record_cleanup(turbo_flow_state_record_t *record) {
  if (!record || record->size < sizeof(*record)) return;
  mem_buffer_release(record->value);
  *record = (turbo_flow_state_record_t)TURBO_FLOW_STATE_RECORD_INIT;
}
