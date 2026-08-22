#include "flow_store_internal.h"
#include "turbo_flow_stl_adapter.h"
#include "turbo_flow_series_store.h"
#include "turbo_flow_series_store_provider.h"

#include <math.h>
#include <stdlib.h>

typedef struct flow_series_group_s {
  mem_buffer_t *name;
  turbo_flow_series_value_kind_t kind;
  turbo_deque_t samples;
} flow_series_group_t;

typedef struct flow_series_memory_s {
  turbo_flow_series_config_t config;
  turbo_flow_store_stats_t stats;
  turbo_hash_map_t groups;
  int closed;
} flow_series_memory_t;

static flow_series_group_t *flow_series_find_group(flow_series_memory_t *store,
                                                   turbo_flow_store_bytes_t series) {
  flow_store_key_t key = flow_store_key_from_bytes(series);
  flow_series_group_t **group = (flow_series_group_t **)turbo_hash_map_get(&store->groups, &key);
  return group ? *group : NULL;
}

static void flow_series_group_destroy(flow_series_group_t *group) {
  if (!group) return;
  turbo_deque_destroy(&group->samples);
  mem_buffer_release(group->name);
  free(group);
}

static int flow_series_value_validate(const turbo_flow_series_value_t *value) {
  if (!value) return TURBO_EINVAL;
  switch (value->kind) {
  case TURBO_FLOW_SERIES_BOOL:
    return value->as.boolean == 0 || value->as.boolean == 1 ? TURBO_OK : TURBO_EINVAL;
  case TURBO_FLOW_SERIES_INT64:
    return TURBO_OK;
  case TURBO_FLOW_SERIES_DOUBLE:
    return isfinite(value->as.real) ? TURBO_OK : TURBO_EINVAL;
  default:
    return TURBO_EINVAL;
  }
}

static double flow_series_value_as_double(const turbo_flow_series_value_t *value) {
  switch (value->kind) {
  case TURBO_FLOW_SERIES_BOOL:
    return (double)value->as.boolean;
  case TURBO_FLOW_SERIES_INT64:
    return (double)value->as.integer;
  case TURBO_FLOW_SERIES_DOUBLE:
    return value->as.real;
  default:
    return 0.0;
  }
}

static void flow_series_pop_front(flow_series_memory_t *store, flow_series_group_t *group) {
  turbo_flow_series_sample_t ignored;
  if (turbo_deque_pop_front(&group->samples, &ignored) != TURBO_OK) return;
  store->stats.records--;
  store->stats.bytes -= sizeof(turbo_flow_series_sample_t);
  store->stats.trims++;
}

static int flow_series_remove_empty_group(flow_series_memory_t *store, flow_series_group_t *group) {
  flow_store_key_t key;
  int rc;
  if (!group || !turbo_deque_empty(&group->samples)) return TURBO_OK;
  key = flow_store_key_from_buffer(group->name);
  rc = turbo_hash_map_remove(&store->groups, &key, NULL);
  if (rc != TURBO_OK) return rc;
  store->stats.bytes -= mem_buffer_used(group->name);
  flow_series_group_destroy(group);
  return TURBO_OK;
}

static flow_series_group_t *flow_series_oldest_group(flow_series_memory_t *store) {
  flow_series_group_t *oldest = NULL;
  size_t slot;
  for (slot = 0u; slot < turbo_hash_map_capacity(&store->groups); ++slot) {
    flow_series_group_t *const *candidate =
        (flow_series_group_t *const *)turbo_hash_map_value_at_const(&store->groups, slot);
    const turbo_flow_series_sample_t *candidate_sample;
    const turbo_flow_series_sample_t *oldest_sample;
    if (!candidate || turbo_deque_empty(&(*candidate)->samples)) continue;
    candidate_sample =
        (const turbo_flow_series_sample_t *)turbo_deque_front_const(&(*candidate)->samples);
    oldest_sample =
        oldest ? (const turbo_flow_series_sample_t *)turbo_deque_front_const(&oldest->samples)
               : NULL;
    if (!oldest_sample || candidate_sample->timestamp_ms < oldest_sample->timestamp_ms) {
      oldest = *candidate;
    }
  }
  return oldest;
}

static int flow_series_memory_close(void *ctx);
static void flow_series_memory_destroy(void *ctx);
static int flow_series_memory_append(void *ctx, turbo_flow_store_bytes_t series,
                                     const turbo_flow_series_sample_t *sample);
static int flow_series_memory_range(void *ctx, turbo_flow_store_bytes_t series, uint64_t start_ms,
                                    uint64_t end_ms, turbo_flow_series_sample_t *samples,
                                    size_t capacity, size_t *count);
static int flow_series_memory_aggregate(void *ctx, turbo_flow_store_bytes_t series,
                                        uint64_t start_ms, uint64_t end_ms,
                                        turbo_flow_series_aggregate_t aggregate, double *value,
                                        size_t *sample_count);
static int flow_series_memory_trim_before(void *ctx, turbo_flow_store_bytes_t series,
                                          uint64_t timestamp_ms, size_t *trimmed);
static int flow_series_memory_stats(const void *ctx, turbo_flow_store_stats_t *out);

static const turbo_flow_series_store_provider_ops_t FLOW_SERIES_MEMORY_OPS = {
    sizeof(turbo_flow_series_store_provider_ops_t),
    TURBO_FLOW_SERIES_STORE_PROVIDER_API_VERSION,
    flow_series_memory_close,
    flow_series_memory_destroy,
    flow_series_memory_append,
    flow_series_memory_range,
    flow_series_memory_aggregate,
    flow_series_memory_trim_before,
    flow_series_memory_stats};

int turbo_flow_series_store_create_memory(const turbo_flow_series_config_t *config,
                                          turbo_flow_series_store_t **out) {
  flow_series_memory_t *store;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_STORE_ABI_VERSION ||
      (config->duplicate_policy != TURBO_FLOW_SERIES_DUPLICATE_REJECT &&
       config->duplicate_policy != TURBO_FLOW_SERIES_DUPLICATE_KEEP_FIRST &&
       config->duplicate_policy != TURBO_FLOW_SERIES_DUPLICATE_KEEP_LAST)) {
    return TURBO_EINVAL;
  }
  rc = turbo_flow_store_limits_validate(&config->limits, 1);
  if (rc != TURBO_OK) return rc;
  if (config->limits.max_item_bytes < sizeof(turbo_flow_series_sample_t)) {
    return TURBO_EINVAL;
  }
  store = (flow_series_memory_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->config = *config;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  rc = turbo_hash_map_init(&store->groups, sizeof(flow_store_key_t), sizeof(flow_series_group_t *),
                           flow_store_key_hash, flow_store_key_equal, NULL);
  if (rc != TURBO_OK) {
    free(store);
    return rc;
  }
  rc = turbo_flow_series_store_create_provider(&FLOW_SERIES_MEMORY_OPS, store, out);
  if (rc != TURBO_OK) flow_series_memory_destroy(store);
  return rc;
}

static int flow_series_memory_close(void *ctx) {
  flow_series_memory_t *store = (flow_series_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_series_memory_destroy(void *ctx) {
  flow_series_memory_t *store = (flow_series_memory_t *)ctx;
  size_t slot;
  if (!store) return;
  for (slot = 0u; slot < turbo_hash_map_capacity(&store->groups); ++slot) {
    flow_series_group_t *const *group =
        (flow_series_group_t *const *)turbo_hash_map_value_at_const(&store->groups, slot);
    if (group) flow_series_group_destroy(*group);
  }
  turbo_hash_map_destroy(&store->groups);
  free(store);
}

static int flow_series_memory_append(void *ctx, turbo_flow_store_bytes_t series,
                                     const turbo_flow_series_sample_t *sample) {
  flow_series_memory_t *store = (flow_series_memory_t *)ctx;
  flow_series_group_t *group;
  size_t item_bytes = 0u;
  size_t target_size;
  size_t next_records;
  size_t next_bytes;
  int created = 0;
  int rc;
  if (!store || !sample) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(series, 0);
  if (rc == TURBO_OK) rc = flow_series_value_validate(&sample->value);
  if (rc == TURBO_OK) {
    rc = flow_store_size_add(series.size, sizeof(turbo_flow_series_sample_t), &item_bytes);
  }
  if (rc != TURBO_OK) return rc;
  if (item_bytes > store->config.limits.max_item_bytes) {
    store->stats.rejects++;
    return TURBO_EFBIG;
  }

  group = flow_series_find_group(store, series);
  if (group && !turbo_deque_empty(&group->samples)) {
    turbo_flow_series_sample_t *last =
        (turbo_flow_series_sample_t *)turbo_deque_back(&group->samples);
    if (sample->value.kind != group->kind) return TURBO_EINVAL;
    if (sample->timestamp_ms < last->timestamp_ms) return TURBO_ERANGE;
    if (sample->timestamp_ms == last->timestamp_ms) {
      if (store->config.duplicate_policy == TURBO_FLOW_SERIES_DUPLICATE_REJECT) {
        return TURBO_EALREADY;
      }
      if (store->config.duplicate_policy == TURBO_FLOW_SERIES_DUPLICATE_KEEP_FIRST) {
        return TURBO_OK;
      }
      *last = *sample;
      store->stats.writes++;
      return TURBO_OK;
    }
  } else if (group && sample->value.kind != group->kind) {
    return TURBO_EINVAL;
  }

  target_size = group ? turbo_deque_size(&group->samples) : 0u;
  if (target_size < store->config.limits.max_records) {
    if (group && turbo_deque_capacity(&group->samples) < target_size + 1u) {
      rc = turbo_deque_reserve(&group->samples, target_size + 1u);
      if (rc != TURBO_OK) return rc;
    }
  }

  if (store->config.limits.full_policy == TURBO_FLOW_STORE_FULL_REJECT) {
    size_t retention_count = 0u;
    size_t retained_records = store->stats.records;
    size_t retained_bytes = store->stats.bytes;
    if (group && store->config.limits.retention_ms > 0u &&
        sample->timestamp_ms > store->config.limits.retention_ms) {
      uint64_t floor = sample->timestamp_ms - store->config.limits.retention_ms;
      while (retention_count < turbo_deque_size(&group->samples)) {
        const turbo_flow_series_sample_t *old =
            (const turbo_flow_series_sample_t *)turbo_deque_at_const(&group->samples,
                                                                     retention_count);
        if (old->timestamp_ms >= floor) break;
        retention_count++;
      }
      retained_records -= retention_count;
      retained_bytes -= retention_count * sizeof(turbo_flow_series_sample_t);
    }
    next_records = retained_records + 1u;
    next_bytes = retained_bytes + sizeof(turbo_flow_series_sample_t) + (group ? 0u : series.size);
    if (next_records > store->config.limits.max_records ||
        next_bytes > store->config.limits.max_bytes) {
      store->stats.rejects++;
      return TURBO_ENOSPC;
    }
  } else if (series.size + sizeof(turbo_flow_series_sample_t) > store->config.limits.max_bytes) {
    store->stats.rejects++;
    return TURBO_ENOSPC;
  }

  if (!group) {
    flow_store_key_t key;
    group = (flow_series_group_t *)calloc(1u, sizeof(*group));
    if (!group) return TURBO_ENOMEM;
    group->kind = sample->value.kind;
    rc = flow_store_buffer_copy(series, &group->name);
    if (rc == TURBO_OK) rc = turbo_deque_init(&group->samples, sizeof(*sample));
    if (rc == TURBO_OK) rc = turbo_deque_reserve(&group->samples, 1u);
    if (rc == TURBO_OK) {
      key = flow_store_key_from_buffer(group->name);
      rc = turbo_hash_map_put(&store->groups, &key, &group);
    }
    if (rc != TURBO_OK) {
      flow_series_group_destroy(group);
      return rc;
    }
    created = 1;
    store->stats.bytes += series.size;
  }

  if (store->config.limits.retention_ms > 0u &&
      sample->timestamp_ms > store->config.limits.retention_ms) {
    uint64_t floor = sample->timestamp_ms - store->config.limits.retention_ms;
    const turbo_flow_series_sample_t *front =
        (const turbo_flow_series_sample_t *)turbo_deque_front_const(&group->samples);
    while (front && front->timestamp_ms < floor) {
      flow_series_pop_front(store, group);
      front = (const turbo_flow_series_sample_t *)turbo_deque_front_const(&group->samples);
    }
  }

  if (store->config.limits.full_policy == TURBO_FLOW_STORE_FULL_TRIM_OLDEST) {
    while (store->stats.records + 1u > store->config.limits.max_records ||
           store->stats.bytes + sizeof(*sample) > store->config.limits.max_bytes) {
      flow_series_group_t *oldest = flow_series_oldest_group(store);
      if (!oldest) {
        if (created) {
          flow_series_remove_empty_group(store, group);
        }
        store->stats.rejects++;
        return TURBO_ENOSPC;
      }
      flow_series_pop_front(store, oldest);
      if (oldest != group && turbo_deque_empty(&oldest->samples)) {
        rc = flow_series_remove_empty_group(store, oldest);
        if (rc != TURBO_OK) return rc;
      }
    }
  }

  rc = turbo_deque_push_back(&group->samples, sample);
  if (rc != TURBO_OK) {
    store->stats.backend_errors++;
    if (created) flow_series_remove_empty_group(store, group);
    return rc;
  }
  store->stats.writes++;
  flow_store_stats_write(&store->stats, store->stats.records + 1u,
                         store->stats.bytes + sizeof(*sample));
  return TURBO_OK;
}

static int flow_series_memory_range(void *ctx, turbo_flow_store_bytes_t series, uint64_t start_ms,
                                    uint64_t end_ms, turbo_flow_series_sample_t *samples,
                                    size_t capacity, size_t *count) {
  flow_series_memory_t *store = (flow_series_memory_t *)ctx;
  flow_series_group_t *group;
  size_t i;
  size_t written = 0u;
  int rc;
  if (!store || !count || start_ms > end_ms || (capacity > 0u && !samples)) {
    return TURBO_EINVAL;
  }
  rc = flow_store_bytes_validate(series, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_series_find_group(store, series);
  store->stats.queries++;
  if (!group) return TURBO_ENOENT;
  for (i = 0u; i < turbo_deque_size(&group->samples) && written < capacity; ++i) {
    const turbo_flow_series_sample_t *sample =
        (const turbo_flow_series_sample_t *)turbo_deque_at_const(&group->samples, i);
    if (sample->timestamp_ms < start_ms) continue;
    if (sample->timestamp_ms > end_ms) break;
    samples[written++] = *sample;
  }
  *count = written;
  return TURBO_OK;
}

static int flow_series_memory_aggregate(void *ctx, turbo_flow_store_bytes_t series,
                                        uint64_t start_ms, uint64_t end_ms,
                                        turbo_flow_series_aggregate_t aggregate, double *value,
                                        size_t *sample_count) {
  flow_series_memory_t *store = (flow_series_memory_t *)ctx;
  flow_series_group_t *group;
  size_t i;
  size_t count = 0u;
  double result = 0.0;
  int rc;
  if (!store || !value || !sample_count || start_ms > end_ms ||
      aggregate < TURBO_FLOW_SERIES_AGGREGATE_COUNT ||
      aggregate > TURBO_FLOW_SERIES_AGGREGATE_AVG) {
    return TURBO_EINVAL;
  }
  rc = flow_store_bytes_validate(series, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_series_find_group(store, series);
  store->stats.queries++;
  if (!group) return TURBO_ENOENT;
  for (i = 0u; i < turbo_deque_size(&group->samples); ++i) {
    const turbo_flow_series_sample_t *sample =
        (const turbo_flow_series_sample_t *)turbo_deque_at_const(&group->samples, i);
    double current;
    if (sample->timestamp_ms < start_ms) continue;
    if (sample->timestamp_ms > end_ms) break;
    current = flow_series_value_as_double(&sample->value);
    if (count == 0u || (aggregate == TURBO_FLOW_SERIES_AGGREGATE_MIN && current < result) ||
        (aggregate == TURBO_FLOW_SERIES_AGGREGATE_MAX && current > result)) {
      result = current;
    } else if (aggregate == TURBO_FLOW_SERIES_AGGREGATE_SUM ||
               aggregate == TURBO_FLOW_SERIES_AGGREGATE_AVG) {
      result += current;
    }
    count++;
  }
  if (aggregate == TURBO_FLOW_SERIES_AGGREGATE_COUNT) result = (double)count;
  if (aggregate == TURBO_FLOW_SERIES_AGGREGATE_AVG && count > 0u) result /= (double)count;
  *value = result;
  *sample_count = count;
  return TURBO_OK;
}

static int flow_series_memory_trim_before(void *ctx, turbo_flow_store_bytes_t series,
                                          uint64_t timestamp_ms, size_t *trimmed) {
  flow_series_memory_t *store = (flow_series_memory_t *)ctx;
  flow_series_group_t *group;
  size_t count = 0u;
  int rc;
  if (!store || !trimmed) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(series, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_series_find_group(store, series);
  if (!group) return TURBO_ENOENT;
  while (!turbo_deque_empty(&group->samples)) {
    const turbo_flow_series_sample_t *sample =
        (const turbo_flow_series_sample_t *)turbo_deque_front_const(&group->samples);
    if (sample->timestamp_ms >= timestamp_ms) break;
    flow_series_pop_front(store, group);
    count++;
  }
  if (turbo_deque_empty(&group->samples)) {
    rc = flow_series_remove_empty_group(store, group);
    if (rc != TURBO_OK) return rc;
  }
  if (count > 0u) store->stats.writes++;
  *trimmed = count;
  return TURBO_OK;
}

static int flow_series_memory_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  const flow_series_memory_t *store = (const flow_series_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  return flow_store_stats_copy(&store->stats, out);
}
