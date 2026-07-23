#include "flow_store_internal.h"
#include "turbo_deque.h"
#include "turbo_flow_log_store.h"
#include "turbo_flow_log_store_provider.h"

#include <stdlib.h>

typedef struct flow_log_entry_s {
  uint64_t cursor;
  uint64_t timestamp_ms;
  mem_buffer_t *payload;
} flow_log_entry_t;

typedef struct flow_log_memory_s {
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  turbo_deque_t entries;
  uint64_t next_cursor;
  uint64_t last_timestamp_ms;
  int closed;
} flow_log_memory_t;

static size_t flow_log_payload_size(const flow_log_entry_t *entry) {
  return entry->payload ? mem_buffer_used(entry->payload) : 0u;
}

static void flow_log_trim_front(flow_log_memory_t *store, size_t count) {
  while (count-- > 0u) {
    flow_log_entry_t entry;
    if (turbo_deque_pop_front(&store->entries, &entry) != TURBO_OK) return;
    store->stats.records--;
    store->stats.bytes -= flow_log_payload_size(&entry);
    store->stats.trims++;
    mem_buffer_release(entry.payload);
  }
}

static int flow_log_plan_append(flow_log_memory_t *store, uint64_t timestamp_ms,
                                size_t payload_size, size_t *trim_count) {
  size_t size = turbo_deque_size(&store->entries);
  size_t remove_count = 0u;
  size_t remaining_bytes = store->stats.bytes;
  size_t next_bytes;
  uint64_t retention_floor = 0u;
  if (store->limits.retention_ms > 0u && timestamp_ms > store->limits.retention_ms) {
    retention_floor = timestamp_ms - store->limits.retention_ms;
  }
  while (remove_count < size) {
    const flow_log_entry_t *entry =
        (const flow_log_entry_t *)turbo_deque_at_const(&store->entries, remove_count);
    if (entry->timestamp_ms >= retention_floor) break;
    remaining_bytes -= flow_log_payload_size(entry);
    remove_count++;
  }
  if (flow_store_size_add(remaining_bytes, payload_size, &next_bytes) != TURBO_OK) {
    return TURBO_ENOSPC;
  }
  while ((size - remove_count + 1u > store->limits.max_records ||
          next_bytes > store->limits.max_bytes) &&
         store->limits.full_policy == TURBO_FLOW_STORE_FULL_TRIM_OLDEST && remove_count < size) {
    const flow_log_entry_t *entry =
        (const flow_log_entry_t *)turbo_deque_at_const(&store->entries, remove_count);
    remaining_bytes -= flow_log_payload_size(entry);
    next_bytes = remaining_bytes + payload_size;
    remove_count++;
  }
  if (size - remove_count + 1u > store->limits.max_records ||
      next_bytes > store->limits.max_bytes) {
    return TURBO_ENOSPC;
  }
  *trim_count = remove_count;
  return TURBO_OK;
}

static int flow_log_memory_close(void *ctx);
static void flow_log_memory_destroy(void *ctx);
static int flow_log_memory_append(void *ctx, uint64_t timestamp_ms,
                                  turbo_flow_store_bytes_t payload, uint64_t *cursor);
static int flow_log_memory_read(void *ctx, uint64_t cursor, turbo_flow_log_record_t *records,
                                size_t capacity, size_t *count);
static int flow_log_memory_trim_before_cursor(void *ctx, uint64_t cursor, size_t *trimmed);
static int flow_log_memory_trim_before_time(void *ctx, uint64_t timestamp_ms, size_t *trimmed);
static int flow_log_memory_bounds(void *ctx, uint64_t *head, uint64_t *tail);
static int flow_log_memory_stats(const void *ctx, turbo_flow_store_stats_t *out);

static const turbo_flow_log_store_provider_ops_t FLOW_LOG_MEMORY_OPS = {
    sizeof(turbo_flow_log_store_provider_ops_t),
    TURBO_FLOW_LOG_STORE_PROVIDER_API_VERSION,
    flow_log_memory_close,
    flow_log_memory_destroy,
    flow_log_memory_append,
    flow_log_memory_read,
    flow_log_memory_trim_before_cursor,
    flow_log_memory_trim_before_time,
    flow_log_memory_bounds,
    flow_log_memory_stats};

int turbo_flow_log_store_create_memory(const turbo_flow_store_limits_t *limits,
                                       turbo_flow_log_store_t **out) {
  flow_log_memory_t *store;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 1);
  if (rc != TURBO_OK) return rc;
  store = (flow_log_memory_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  store->next_cursor = 1u;
  rc = turbo_deque_init(&store->entries, sizeof(flow_log_entry_t));
  if (rc == TURBO_OK && limits->initial_records > 0u) {
    rc = turbo_deque_reserve(&store->entries, limits->initial_records);
  }
  if (rc != TURBO_OK) {
    turbo_deque_destroy(&store->entries);
    free(store);
    return rc;
  }
  rc = turbo_flow_log_store_create_provider(&FLOW_LOG_MEMORY_OPS, store, out);
  if (rc != TURBO_OK) flow_log_memory_destroy(store);
  return rc;
}

static int flow_log_memory_close(void *ctx) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_log_memory_destroy(void *ctx) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  if (!store) return;
  flow_log_trim_front(store, turbo_deque_size(&store->entries));
  turbo_deque_destroy(&store->entries);
  free(store);
}

static int flow_log_memory_append(void *ctx, uint64_t timestamp_ms,
                                  turbo_flow_store_bytes_t payload, uint64_t *cursor) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  flow_log_entry_t entry;
  size_t trim_count;
  size_t final_size;
  int rc;
  if (!store || !cursor) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(payload, 1);
  if (rc != TURBO_OK) return rc;
  if (payload.size > store->limits.max_item_bytes) {
    store->stats.rejects++;
    return TURBO_EFBIG;
  }
  if (timestamp_ms < store->last_timestamp_ms) {
    return TURBO_ERANGE;
  }
  if (store->next_cursor == UINT64_MAX) return TURBO_ERANGE;
  rc = flow_log_plan_append(store, timestamp_ms, payload.size, &trim_count);
  if (rc != TURBO_OK) {
    store->stats.rejects++;
    return rc;
  }
  entry.cursor = store->next_cursor;
  entry.timestamp_ms = timestamp_ms;
  entry.payload = NULL;
  rc = flow_store_buffer_copy(payload, &entry.payload);
  if (rc != TURBO_OK) return rc;
  final_size = turbo_deque_size(&store->entries) - trim_count + 1u;
  if (turbo_deque_capacity(&store->entries) < final_size) {
    rc = turbo_deque_reserve(&store->entries, final_size);
  }
  if (rc != TURBO_OK) {
    mem_buffer_release(entry.payload);
    return rc;
  }
  flow_log_trim_front(store, trim_count);
  rc = turbo_deque_push_back(&store->entries, &entry);
  if (rc != TURBO_OK) {
    store->stats.backend_errors++;
    mem_buffer_release(entry.payload);
    return rc;
  }
  store->next_cursor++;
  store->last_timestamp_ms = timestamp_ms;
  store->stats.writes++;
  flow_store_stats_write(&store->stats, store->stats.records + 1u,
                         store->stats.bytes + payload.size);
  *cursor = entry.cursor;
  return TURBO_OK;
}

static int flow_log_memory_read(void *ctx, uint64_t cursor, turbo_flow_log_record_t *records,
                                size_t capacity, size_t *count) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  size_t start = 0u;
  size_t written = 0u;
  size_t size;
  int rc = TURBO_OK;
  if (!store || !count || (capacity > 0u && !records)) return TURBO_EINVAL;
  *count = 0u;
  size = turbo_deque_size(&store->entries);
  store->stats.queries++;
  if (size == 0u || capacity == 0u) return TURBO_OK;
  if (cursor != 0u) {
    const flow_log_entry_t *head =
        (const flow_log_entry_t *)turbo_deque_front_const(&store->entries);
    if (cursor < head->cursor) return TURBO_ERANGE;
    while (start < size) {
      const flow_log_entry_t *entry =
          (const flow_log_entry_t *)turbo_deque_at_const(&store->entries, start);
      if (entry->cursor >= cursor) break;
      start++;
    }
  }
  while (start < size && written < capacity) {
    const flow_log_entry_t *entry =
        (const flow_log_entry_t *)turbo_deque_at_const(&store->entries, start++);
    turbo_flow_store_bytes_t payload;
    size_t output_size;
    if (records[written].size < sizeof(records[written]) ||
        records[written].abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
      rc = TURBO_EINVAL;
      break;
    }
    output_size = records[written].size;
    turbo_flow_log_record_cleanup(&records[written]);
    records[written] = (turbo_flow_log_record_t)TURBO_FLOW_LOG_RECORD_INIT;
    records[written].size = output_size;
    records[written].cursor = entry->cursor;
    records[written].timestamp_ms = entry->timestamp_ms;
    payload.data = entry->payload ? (const uint8_t *)mem_buffer_const_data(entry->payload) : NULL;
    payload.size = flow_log_payload_size(entry);
    rc = flow_store_buffer_copy(payload, &records[written].payload);
    if (rc != TURBO_OK) break;
    written++;
  }
  if (rc != TURBO_OK) {
    size_t i;
    for (i = 0u; i < written; ++i)
      turbo_flow_log_record_cleanup(&records[i]);
    return rc;
  }
  *count = written;
  return TURBO_OK;
}

static int flow_log_memory_trim_before_cursor(void *ctx, uint64_t cursor, size_t *trimmed) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  size_t count = 0u;
  if (!store || !trimmed) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  while (count < turbo_deque_size(&store->entries)) {
    const flow_log_entry_t *entry =
        (const flow_log_entry_t *)turbo_deque_at_const(&store->entries, count);
    if (entry->cursor >= cursor) break;
    count++;
  }
  flow_log_trim_front(store, count);
  if (count > 0u) store->stats.writes++;
  *trimmed = count;
  return TURBO_OK;
}

static int flow_log_memory_trim_before_time(void *ctx, uint64_t timestamp_ms, size_t *trimmed) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  size_t count = 0u;
  if (!store || !trimmed) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  while (count < turbo_deque_size(&store->entries)) {
    const flow_log_entry_t *entry =
        (const flow_log_entry_t *)turbo_deque_at_const(&store->entries, count);
    if (entry->timestamp_ms >= timestamp_ms) break;
    count++;
  }
  flow_log_trim_front(store, count);
  if (count > 0u) store->stats.writes++;
  *trimmed = count;
  return TURBO_OK;
}

static int flow_log_memory_bounds(void *ctx, uint64_t *head, uint64_t *tail) {
  flow_log_memory_t *store = (flow_log_memory_t *)ctx;
  const flow_log_entry_t *first;
  const flow_log_entry_t *last;
  if (!store || !head || !tail) return TURBO_EINVAL;
  store->stats.queries++;
  first = (const flow_log_entry_t *)turbo_deque_front_const(&store->entries);
  last = (const flow_log_entry_t *)turbo_deque_back_const(&store->entries);
  *head = first ? first->cursor : store->next_cursor;
  *tail = last ? last->cursor : 0u;
  return TURBO_OK;
}

static int flow_log_memory_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  const flow_log_memory_t *store = (const flow_log_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  return flow_store_stats_copy(&store->stats, out);
}
