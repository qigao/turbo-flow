#include "flow_store_internal.h"
#include "turbo_flow_state_store.h"
#include "turbo_flow_state_store_provider.h"

#include <stdlib.h>

typedef struct flow_state_entry_s {
  mem_buffer_t *key;
  mem_buffer_t *value;
  uint64_t revision;
} flow_state_entry_t;

typedef struct flow_state_memory_s {
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  turbo_hash_map_t entries;
  int closed;
} flow_state_memory_t;

static flow_state_entry_t *flow_state_find(flow_state_memory_t *store,
                                           turbo_flow_store_bytes_t key) {
  flow_store_key_t view = flow_store_key_from_bytes(key);
  flow_state_entry_t **entry = (flow_state_entry_t **)turbo_hash_map_get(&store->entries, &view);
  return entry ? *entry : NULL;
}

static void flow_state_entry_destroy(flow_state_entry_t *entry) {
  if (!entry) return;
  mem_buffer_release(entry->key);
  mem_buffer_release(entry->value);
  free(entry);
}

static int flow_state_memory_close(void *ctx);
static void flow_state_memory_destroy(void *ctx);
static int flow_state_memory_put(void *ctx, turbo_flow_store_bytes_t key,
                                 turbo_flow_store_bytes_t value, uint64_t expected_revision,
                                 uint64_t *new_revision);
static int flow_state_memory_get(void *ctx, turbo_flow_store_bytes_t key,
                                 turbo_flow_state_record_t *out);
static int flow_state_memory_remove(void *ctx, turbo_flow_store_bytes_t key,
                                    uint64_t expected_revision);
static int flow_state_memory_visit(void *ctx, turbo_flow_state_visit_fn visit, void *visit_ctx);
static int flow_state_memory_stats(const void *ctx, turbo_flow_store_stats_t *out);

static const turbo_flow_state_store_provider_ops_t FLOW_STATE_MEMORY_OPS = {
    sizeof(turbo_flow_state_store_provider_ops_t),
    TURBO_FLOW_STATE_STORE_PROVIDER_API_VERSION,
    flow_state_memory_close,
    flow_state_memory_destroy,
    flow_state_memory_put,
    flow_state_memory_get,
    flow_state_memory_remove,
    flow_state_memory_visit,
    flow_state_memory_stats};

int turbo_flow_state_store_create_memory(const turbo_flow_store_limits_t *limits,
                                         turbo_flow_state_store_t **out) {
  flow_state_memory_t *store;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 0);
  if (rc != TURBO_OK) return rc;
  store = (flow_state_memory_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  rc = turbo_hash_map_init(&store->entries, sizeof(flow_store_key_t), sizeof(flow_state_entry_t *),
                           flow_store_key_hash, flow_store_key_equal, NULL);
  if (rc == TURBO_OK && limits->initial_records > 0u) {
    rc = turbo_hash_map_reserve(&store->entries, limits->initial_records);
  }
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&store->entries);
    free(store);
    return rc;
  }
  rc = turbo_flow_state_store_create_provider(&FLOW_STATE_MEMORY_OPS, store, out);
  if (rc != TURBO_OK) flow_state_memory_destroy(store);
  return rc;
}

static int flow_state_memory_close(void *ctx) {
  flow_state_memory_t *store = (flow_state_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_state_memory_destroy(void *ctx) {
  flow_state_memory_t *store = (flow_state_memory_t *)ctx;
  size_t slot;
  if (!store) return;
  for (slot = 0u; slot < turbo_hash_map_capacity(&store->entries); ++slot) {
    flow_state_entry_t *const *entry =
        (flow_state_entry_t *const *)turbo_hash_map_value_at_const(&store->entries, slot);
    if (entry) flow_state_entry_destroy(*entry);
  }
  turbo_hash_map_destroy(&store->entries);
  free(store);
}

static int flow_state_memory_put(void *ctx, turbo_flow_store_bytes_t key,
                                 turbo_flow_store_bytes_t value, uint64_t expected_revision,
                                 uint64_t *new_revision) {
  flow_state_memory_t *store = (flow_state_memory_t *)ctx;
  flow_state_entry_t *entry;
  mem_buffer_t *new_value = NULL;
  size_t item_bytes = 0u;
  size_t next_bytes = 0u;
  int rc;
  if (!store || !new_revision) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(key, 0);
  if (rc == TURBO_OK) rc = flow_store_bytes_validate(value, 1);
  if (rc == TURBO_OK) rc = flow_store_size_add(key.size, value.size, &item_bytes);
  if (rc != TURBO_OK) return rc;
  if (item_bytes > store->limits.max_item_bytes) {
    store->stats.rejects++;
    return TURBO_EFBIG;
  }

  entry = flow_state_find(store, key);
  if (entry) {
    if (expected_revision == 0u || expected_revision != entry->revision) {
      store->stats.conflicts++;
      return TURBO_EBUSY;
    }
    if (entry->revision == UINT64_MAX) return TURBO_ERANGE;
    next_bytes = store->stats.bytes - mem_buffer_used(entry->value);
    rc = flow_store_size_add(next_bytes, value.size, &next_bytes);
    if (rc != TURBO_OK || next_bytes > store->limits.max_bytes) {
      store->stats.rejects++;
      return rc == TURBO_OK ? TURBO_ENOSPC : rc;
    }
    rc = flow_store_buffer_copy(value, &new_value);
    if (rc != TURBO_OK) return rc;
    mem_buffer_release(entry->value);
    entry->value = new_value;
    entry->revision++;
    store->stats.writes++;
    flow_store_stats_write(&store->stats, store->stats.records, next_bytes);
    *new_revision = entry->revision;
    return TURBO_OK;
  }

  if (expected_revision != 0u) {
    store->stats.conflicts++;
    return TURBO_EBUSY;
  }
  if (store->stats.records >= store->limits.max_records ||
      flow_store_size_add(store->stats.bytes, item_bytes, &next_bytes) != TURBO_OK ||
      next_bytes > store->limits.max_bytes) {
    store->stats.rejects++;
    return TURBO_ENOSPC;
  }

  entry = (flow_state_entry_t *)calloc(1u, sizeof(*entry));
  if (!entry) return TURBO_ENOMEM;
  rc = flow_store_buffer_copy(key, &entry->key);
  if (rc == TURBO_OK) rc = flow_store_buffer_copy(value, &entry->value);
  if (rc == TURBO_OK) {
    flow_store_key_t owned_key = flow_store_key_from_buffer(entry->key);
    entry->revision = 1u;
    rc = turbo_hash_map_put(&store->entries, &owned_key, &entry);
  }
  if (rc != TURBO_OK) {
    flow_state_entry_destroy(entry);
    return rc;
  }
  store->stats.writes++;
  flow_store_stats_write(&store->stats, store->stats.records + 1u, next_bytes);
  *new_revision = 1u;
  return TURBO_OK;
}

static int flow_state_memory_get(void *ctx, turbo_flow_store_bytes_t key,
                                 turbo_flow_state_record_t *out) {
  flow_state_memory_t *store = (flow_state_memory_t *)ctx;
  flow_state_entry_t *entry;
  turbo_flow_store_bytes_t value;
  size_t output_size;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  rc = flow_store_bytes_validate(key, 0);
  if (rc != TURBO_OK) return rc;
  entry = flow_state_find(store, key);
  store->stats.queries++;
  if (!entry) return TURBO_ENOENT;
  value.data = (const uint8_t *)mem_buffer_const_data(entry->value);
  value.size = mem_buffer_used(entry->value);
  output_size = out->size;
  turbo_flow_state_record_cleanup(out);
  *out = (turbo_flow_state_record_t)TURBO_FLOW_STATE_RECORD_INIT;
  out->size = output_size;
  out->revision = entry->revision;
  rc = flow_store_buffer_copy(value, &out->value);
  return rc;
}

static int flow_state_memory_remove(void *ctx, turbo_flow_store_bytes_t key,
                                    uint64_t expected_revision) {
  flow_state_memory_t *store = (flow_state_memory_t *)ctx;
  flow_store_key_t view;
  flow_state_entry_t *entry;
  int rc;
  if (!store || expected_revision == 0u) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(key, 0);
  if (rc != TURBO_OK) return rc;
  entry = flow_state_find(store, key);
  if (!entry) return TURBO_ENOENT;
  if (entry->revision != expected_revision) {
    store->stats.conflicts++;
    return TURBO_EBUSY;
  }
  view = flow_store_key_from_bytes(key);
  rc = turbo_hash_map_remove(&store->entries, &view, NULL);
  if (rc != TURBO_OK) return rc;
  store->stats.writes++;
  flow_store_stats_write(&store->stats, store->stats.records - 1u,
                         store->stats.bytes - mem_buffer_used(entry->key) -
                             mem_buffer_used(entry->value));
  flow_state_entry_destroy(entry);
  return TURBO_OK;
}

static int flow_state_memory_visit(void *ctx, turbo_flow_state_visit_fn visit, void *visit_ctx) {
  flow_state_memory_t *store = (flow_state_memory_t *)ctx;
  size_t slot;
  if (!store || !visit) return TURBO_EINVAL;
  store->stats.queries++;
  for (slot = 0u; slot < turbo_hash_map_capacity(&store->entries); ++slot) {
    flow_state_entry_t *const *entry_ptr =
        (flow_state_entry_t *const *)turbo_hash_map_value_at_const(&store->entries, slot);
    turbo_flow_store_bytes_t key;
    turbo_flow_store_bytes_t value;
    int rc;
    if (!entry_ptr) continue;
    key.data = (const uint8_t *)mem_buffer_const_data((*entry_ptr)->key);
    key.size = mem_buffer_used((*entry_ptr)->key);
    value.data = (const uint8_t *)mem_buffer_const_data((*entry_ptr)->value);
    value.size = mem_buffer_used((*entry_ptr)->value);
    rc = visit(visit_ctx, key, value, (*entry_ptr)->revision);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_state_memory_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  const flow_state_memory_t *store = (const flow_state_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  return flow_store_stats_copy(&store->stats, out);
}
