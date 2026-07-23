#include "flow_redis_storage_internal.h"
#include "turbo_flow_store_redis.h"

#include "flow_redis_internal.h"
#include "turbo_flow_state_store_provider.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_redis_state_store_s {
  turbo_flow_record_store_t records;
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  int closed;
} flow_redis_state_store_t;

typedef struct flow_redis_state_scan_s {
  turbo_flow_store_bytes_t target;
  turbo_flow_state_record_t *record;
  turbo_flow_state_visit_fn visit;
  void *visit_ctx;
  size_t records;
  size_t bytes;
  size_t found_value_size;
  uint64_t found_revision;
  int found;
} flow_redis_state_scan_t;

static int flow_redis_state_size_add(size_t left, size_t right, size_t *out) {
  if (!out || left > SIZE_MAX - right) return TURBO_EFBIG;
  *out = left + right;
  return TURBO_OK;
}

static int flow_redis_state_buffer_copy(const uint8_t *data, size_t size, mem_buffer_t **out) {
  mem_buffer_t *buffer;
  if (!out || (size > 0u && !data)) return TURBO_EINVAL;
  *out = NULL;
  if (size == 0u) return TURBO_OK;
  buffer = mem_get_buffer(mem_global(), size);
  if (!buffer) return TURBO_ENOMEM;
  memcpy(mem_buffer_data(buffer), data, size);
  mem_set_used(buffer, size);
  *out = buffer;
  return TURBO_OK;
}

static int flow_redis_state_scan_record(void *ctx, const turbo_flow_record_view_t *record) {
  flow_redis_state_scan_t *scan = (flow_redis_state_scan_t *)ctx;
  size_t item_bytes;
  int rc;
  if (!scan || !record || record->size < sizeof(*record)) return TURBO_EPROTO;
  rc = flow_redis_state_size_add(record->key_size, record->value_size, &item_bytes);
  if (rc == TURBO_OK) rc = flow_redis_state_size_add(scan->bytes, item_bytes, &scan->bytes);
  if (rc != TURBO_OK) return rc;
  scan->records++;
  if (scan->visit) {
    turbo_flow_store_bytes_t key = {record->key, record->key_size};
    turbo_flow_store_bytes_t value = {record->value, record->value_size};
    rc = scan->visit(scan->visit_ctx, key, value, record->revision);
    if (rc != TURBO_OK) return rc;
  }
  if (scan->target.size != record->key_size ||
      memcmp(scan->target.data, record->key, record->key_size) != 0) {
    return TURBO_OK;
  }
  scan->found = 1;
  scan->found_value_size = record->value_size;
  scan->found_revision = record->revision;
  if (scan->record) {
    const size_t output_size = scan->record->size;
    turbo_flow_state_record_cleanup(scan->record);
    *scan->record = (turbo_flow_state_record_t)TURBO_FLOW_STATE_RECORD_INIT;
    scan->record->size = output_size;
    scan->record->revision = record->revision;
    rc = flow_redis_state_buffer_copy(record->value, record->value_size, &scan->record->value);
  }
  return rc;
}

static int flow_redis_state_scan(flow_redis_state_store_t *store, turbo_flow_store_bytes_t target,
                                 turbo_flow_state_record_t *record, turbo_flow_state_visit_fn visit,
                                 void *visit_ctx, flow_redis_state_scan_t *out) {
  flow_redis_state_scan_t scan;
  int rc;
  if (!store || !out) return TURBO_EINVAL;
  memset(&scan, 0, sizeof(scan));
  scan.target = target;
  scan.record = record;
  scan.visit = visit;
  scan.visit_ctx = visit_ctx;
  rc = store->records.scan(store->records.ctx, flow_redis_state_scan_record, &scan);
  if (rc != TURBO_OK) {
    store->stats.backend_errors++;
    return rc;
  }
  *out = scan;
  return TURBO_OK;
}

static int flow_redis_state_key_validate(const flow_redis_state_store_t *store,
                                         turbo_flow_store_bytes_t key) {
  if (!store || !key.data || key.size == 0u) return TURBO_EINVAL;
  return key.size <= store->records.max_key_size ? TURBO_OK : TURBO_EFBIG;
}

static int flow_redis_state_close(void *ctx) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_redis_state_destroy(void *ctx) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  if (!store) return;
  flow_redis_record_store_destroy(&store->records);
  free(store);
}

static int flow_redis_state_put(void *ctx, turbo_flow_store_bytes_t key,
                                turbo_flow_store_bytes_t value, uint64_t expected_revision,
                                uint64_t *new_revision) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t scan;
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  size_t item_bytes;
  size_t next_bytes;
  int rc;
  if (!store || !new_revision || (value.size > 0u && !value.data)) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_redis_state_key_validate(store, key);
  if (rc != TURBO_OK) return rc;
  if (value.size > store->records.max_value_size ||
      flow_redis_state_size_add(key.size, value.size, &item_bytes) != TURBO_OK ||
      item_bytes > store->limits.max_item_bytes) {
    store->stats.rejects++;
    return TURBO_EFBIG;
  }
  rc = flow_redis_state_scan(store, key, NULL, NULL, NULL, &scan);
  if (rc != TURBO_OK) return rc;
  if ((scan.found && expected_revision == 0u) || (!scan.found && expected_revision != 0u)) {
    store->stats.conflicts++;
    return TURBO_EBUSY;
  }
  if (scan.found) {
    if (scan.found_revision != expected_revision) {
      store->stats.conflicts++;
      return TURBO_EBUSY;
    }
    if (scan.found_revision == TURBO_FLOW_RECORD_REVISION_MAX) return TURBO_ERANGE;
    next_bytes = scan.bytes - key.size - scan.found_value_size;
  } else {
    if (scan.records >= store->limits.max_records) {
      store->stats.rejects++;
      return TURBO_ENOSPC;
    }
    next_bytes = scan.bytes;
  }
  rc = flow_redis_state_size_add(next_bytes, item_bytes, &next_bytes);
  if (rc != TURBO_OK || next_bytes > store->limits.max_bytes) {
    store->stats.rejects++;
    return rc == TURBO_OK ? TURBO_ENOSPC : rc;
  }
  mutation.key = key.data;
  mutation.key_size = key.size;
  mutation.expected_revision = expected_revision;
  mutation.next_revision = expected_revision + 1u;
  mutation.value = value.data;
  mutation.value_size = value.size;
  rc = store->records.commit(store->records.ctx, &mutation, 1u);
  if (rc == TURBO_EBUSY) store->stats.conflicts++;
  if (rc != TURBO_OK) {
    if (rc != TURBO_EBUSY) store->stats.backend_errors++;
    return rc;
  }
  store->stats.writes++;
  store->stats.records = scan.found ? scan.records : scan.records + 1u;
  store->stats.bytes = next_bytes;
  if (store->stats.records > store->stats.peak_records)
    store->stats.peak_records = store->stats.records;
  if (next_bytes > store->stats.peak_bytes) store->stats.peak_bytes = next_bytes;
  *new_revision = mutation.next_revision;
  return TURBO_OK;
}

static int flow_redis_state_get(void *ctx, turbo_flow_store_bytes_t key,
                                turbo_flow_state_record_t *out) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t lookup;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  rc = flow_redis_state_key_validate(store, key);
  if (rc != TURBO_OK) return rc;
  memset(&lookup, 0, sizeof(lookup));
  lookup.target = key;
  lookup.record = out;
  rc = flow_redis_record_store_get(&store->records, key.data, key.size,
                                   flow_redis_state_scan_record, &lookup);
  store->stats.queries++;
  if (rc != TURBO_OK && rc != TURBO_ENOENT) store->stats.backend_errors++;
  return rc;
}

static int flow_redis_state_remove(void *ctx, turbo_flow_store_bytes_t key,
                                   uint64_t expected_revision) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t scan;
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  int rc;
  if (!store || expected_revision == 0u) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_redis_state_key_validate(store, key);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_state_scan(store, key, NULL, NULL, NULL, &scan);
  if (rc != TURBO_OK) return rc;
  if (!scan.found) return TURBO_ENOENT;
  if (scan.found_revision != expected_revision) {
    store->stats.conflicts++;
    return TURBO_EBUSY;
  }
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = key.data;
  mutation.key_size = key.size;
  mutation.expected_revision = expected_revision;
  mutation.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  rc = store->records.commit(store->records.ctx, &mutation, 1u);
  if (rc == TURBO_EBUSY) store->stats.conflicts++;
  if (rc != TURBO_OK) {
    if (rc != TURBO_EBUSY) store->stats.backend_errors++;
    return rc;
  }
  store->stats.writes++;
  store->stats.records = scan.records - 1u;
  store->stats.bytes = scan.bytes - key.size - scan.found_value_size;
  return TURBO_OK;
}

static int flow_redis_state_visit(void *ctx, turbo_flow_state_visit_fn visit, void *visit_ctx) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t scan;
  int rc;
  if (!store || !visit) return TURBO_EINVAL;
  rc = flow_redis_state_scan(store, (turbo_flow_store_bytes_t)TURBO_FLOW_STORE_BYTES_INIT, NULL,
                             visit, visit_ctx, &scan);
  store->stats.queries++;
  return rc;
}

static int flow_redis_state_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t scan;
  size_t output_size;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  rc = flow_redis_state_scan(store, (turbo_flow_store_bytes_t)TURBO_FLOW_STORE_BYTES_INIT, NULL,
                             NULL, NULL, &scan);
  if (rc != TURBO_OK) return rc;
  store->stats.records = scan.records;
  store->stats.bytes = scan.bytes;
  if (scan.records > store->stats.peak_records) store->stats.peak_records = scan.records;
  if (scan.bytes > store->stats.peak_bytes) store->stats.peak_bytes = scan.bytes;
  output_size = out->size;
  *out = store->stats;
  out->size = output_size;
  return TURBO_OK;
}

static const turbo_flow_state_store_provider_ops_t FLOW_REDIS_STATE_OPS = {
    sizeof(turbo_flow_state_store_provider_ops_t),
    TURBO_FLOW_STATE_STORE_PROVIDER_API_VERSION,
    flow_redis_state_close,
    flow_redis_state_destroy,
    flow_redis_state_put,
    flow_redis_state_get,
    flow_redis_state_remove,
    flow_redis_state_visit,
    flow_redis_state_stats};

int flow_redis_state_store_create(const turbo_flow_redis_record_store_config_t *config,
                                  const turbo_flow_store_limits_t *limits,
                                  turbo_flow_state_store_t **out) {
  flow_redis_state_store_t *store;
  size_t max_item_bytes;
  const size_t max_key_size = config && config->max_record_key_size
                                  ? config->max_record_key_size
                                  : TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE;
  const size_t max_value_size = config && config->max_value_size
                                    ? config->max_value_size
                                    : TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE;
  int rc;
  if (!config || !limits || !out) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 0);
  if (rc != TURBO_OK) return rc;
  if (flow_redis_state_size_add(max_key_size, max_value_size, &max_item_bytes) != TURBO_OK ||
      limits->max_records != config->max_records || limits->max_item_bytes > max_item_bytes) {
    return TURBO_EINVAL;
  }
  store = (flow_redis_state_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->records = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  rc = flow_redis_record_store_create(config, &store->records);
  if (rc == TURBO_OK)
    rc = turbo_flow_state_store_create_provider(&FLOW_REDIS_STATE_OPS, store, out);
  if (rc != TURBO_OK) flow_redis_state_destroy(store);
  return rc;
}
