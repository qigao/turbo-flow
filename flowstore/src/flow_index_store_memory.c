#include "flow_store_internal.h"
#include "turbo_flow_index_store.h"
#include "turbo_flow_index_store_provider.h"

#include <stdlib.h>

typedef struct flow_index_group_s {
  mem_buffer_t *name;
  turbo_hash_map_t members;
} flow_index_group_t;

typedef struct flow_index_memory_s {
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  turbo_hash_map_t groups;
  int closed;
} flow_index_memory_t;

static flow_index_group_t *flow_index_find_group(flow_index_memory_t *store,
                                                 turbo_flow_store_bytes_t index) {
  flow_store_key_t key = flow_store_key_from_bytes(index);
  flow_index_group_t **group = (flow_index_group_t **)turbo_hash_map_get(&store->groups, &key);
  return group ? *group : NULL;
}

static void flow_index_group_destroy(flow_index_group_t *group) {
  size_t slot;
  if (!group) return;
  for (slot = 0u; slot < turbo_hash_map_capacity(&group->members); ++slot) {
    mem_buffer_t *const *member =
        (mem_buffer_t *const *)turbo_hash_map_value_at_const(&group->members, slot);
    if (member) mem_buffer_release(*member);
  }
  turbo_hash_map_destroy(&group->members);
  mem_buffer_release(group->name);
  free(group);
}

static int flow_index_memory_close(void *ctx);
static void flow_index_memory_destroy(void *ctx);
static int flow_index_memory_add(void *ctx, turbo_flow_store_bytes_t index,
                                 turbo_flow_store_bytes_t member);
static int flow_index_memory_remove(void *ctx, turbo_flow_store_bytes_t index,
                                    turbo_flow_store_bytes_t member);
static int flow_index_memory_contains(void *ctx, turbo_flow_store_bytes_t index,
                                      turbo_flow_store_bytes_t member, int *out);
static int flow_index_memory_count(void *ctx, turbo_flow_store_bytes_t index, size_t *out);
static int flow_index_memory_visit(void *ctx, turbo_flow_store_bytes_t index,
                                   turbo_flow_index_visit_fn visit, void *visit_ctx);
static int flow_index_memory_intersection_count(void *ctx, const turbo_flow_store_bytes_t *indices,
                                                size_t index_count, size_t *out);
static int flow_index_memory_stats(const void *ctx, turbo_flow_store_stats_t *out);

static const turbo_flow_index_store_provider_ops_t FLOW_INDEX_MEMORY_OPS = {
    sizeof(turbo_flow_index_store_provider_ops_t),
    TURBO_FLOW_INDEX_STORE_PROVIDER_API_VERSION,
    flow_index_memory_close,
    flow_index_memory_destroy,
    flow_index_memory_add,
    flow_index_memory_remove,
    flow_index_memory_contains,
    flow_index_memory_count,
    flow_index_memory_visit,
    flow_index_memory_intersection_count,
    flow_index_memory_stats};

int turbo_flow_index_store_create_memory(const turbo_flow_store_limits_t *limits,
                                         turbo_flow_index_store_t **out) {
  flow_index_memory_t *store;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 0);
  if (rc != TURBO_OK) return rc;
  store = (flow_index_memory_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  rc = turbo_hash_map_init(&store->groups, sizeof(flow_store_key_t), sizeof(flow_index_group_t *),
                           flow_store_key_hash, flow_store_key_equal, NULL);
  if (rc != TURBO_OK) {
    free(store);
    return rc;
  }
  rc = turbo_flow_index_store_create_provider(&FLOW_INDEX_MEMORY_OPS, store, out);
  if (rc != TURBO_OK) flow_index_memory_destroy(store);
  return rc;
}

static int flow_index_memory_close(void *ctx) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_index_memory_destroy(void *ctx) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  size_t slot;
  if (!store) return;
  for (slot = 0u; slot < turbo_hash_map_capacity(&store->groups); ++slot) {
    flow_index_group_t *const *group =
        (flow_index_group_t *const *)turbo_hash_map_value_at_const(&store->groups, slot);
    if (group) flow_index_group_destroy(*group);
  }
  turbo_hash_map_destroy(&store->groups);
  free(store);
}

static int flow_index_memory_add(void *ctx, turbo_flow_store_bytes_t index,
                                 turbo_flow_store_bytes_t member) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  flow_index_group_t *group;
  flow_store_key_t member_key;
  mem_buffer_t *owned_member = NULL;
  size_t item_bytes = 0u;
  size_t next_bytes = 0u;
  int created_group = 0;
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(index, 0);
  if (rc == TURBO_OK) rc = flow_store_bytes_validate(member, 0);
  if (rc == TURBO_OK) rc = flow_store_size_add(index.size, member.size, &item_bytes);
  if (rc != TURBO_OK) return rc;
  if (item_bytes > store->limits.max_item_bytes) {
    store->stats.rejects++;
    return TURBO_EFBIG;
  }
  group = flow_index_find_group(store, index);
  if (group) {
    member_key = flow_store_key_from_bytes(member);
    if (turbo_hash_map_contains(&group->members, &member_key)) return TURBO_EALREADY;
    item_bytes = member.size;
  }
  if (store->stats.records >= store->limits.max_records ||
      flow_store_size_add(store->stats.bytes, item_bytes, &next_bytes) != TURBO_OK ||
      next_bytes > store->limits.max_bytes) {
    store->stats.rejects++;
    return TURBO_ENOSPC;
  }

  if (!group) {
    flow_store_key_t group_key;
    group = (flow_index_group_t *)calloc(1u, sizeof(*group));
    if (!group) return TURBO_ENOMEM;
    rc = flow_store_buffer_copy(index, &group->name);
    if (rc == TURBO_OK) {
      rc = turbo_hash_map_init(&group->members, sizeof(flow_store_key_t), sizeof(mem_buffer_t *),
                               flow_store_key_hash, flow_store_key_equal, NULL);
    }
    if (rc == TURBO_OK) {
      group_key = flow_store_key_from_buffer(group->name);
      rc = turbo_hash_map_put(&store->groups, &group_key, &group);
    }
    if (rc != TURBO_OK) {
      flow_index_group_destroy(group);
      return rc;
    }
    created_group = 1;
  }

  rc = flow_store_buffer_copy(member, &owned_member);
  if (rc == TURBO_OK) {
    member_key = flow_store_key_from_buffer(owned_member);
    rc = turbo_hash_map_put(&group->members, &member_key, &owned_member);
  }
  if (rc != TURBO_OK) {
    mem_buffer_release(owned_member);
    if (created_group) {
      flow_store_key_t group_key = flow_store_key_from_bytes(index);
      turbo_hash_map_remove(&store->groups, &group_key, NULL);
      flow_index_group_destroy(group);
    }
    return rc;
  }
  store->stats.writes++;
  flow_store_stats_write(&store->stats, store->stats.records + 1u, next_bytes);
  return TURBO_OK;
}

static int flow_index_memory_remove(void *ctx, turbo_flow_store_bytes_t index,
                                    turbo_flow_store_bytes_t member) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  flow_index_group_t *group;
  flow_store_key_t member_key;
  mem_buffer_t *owned_member = NULL;
  size_t next_bytes;
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_store_bytes_validate(index, 0);
  if (rc == TURBO_OK) rc = flow_store_bytes_validate(member, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_index_find_group(store, index);
  if (!group) return TURBO_ENOENT;
  member_key = flow_store_key_from_bytes(member);
  rc = turbo_hash_map_remove(&group->members, &member_key, &owned_member);
  if (rc != TURBO_OK) return rc;
  next_bytes = store->stats.bytes - mem_buffer_used(owned_member);
  mem_buffer_release(owned_member);
  if (turbo_hash_map_empty(&group->members)) {
    flow_store_key_t group_key = flow_store_key_from_bytes(index);
    rc = turbo_hash_map_remove(&store->groups, &group_key, NULL);
    if (rc != TURBO_OK) return rc;
    next_bytes -= mem_buffer_used(group->name);
    flow_index_group_destroy(group);
  }
  store->stats.writes++;
  flow_store_stats_write(&store->stats, store->stats.records - 1u, next_bytes);
  return TURBO_OK;
}

static int flow_index_memory_contains(void *ctx, turbo_flow_store_bytes_t index,
                                      turbo_flow_store_bytes_t member, int *out) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  flow_index_group_t *group;
  flow_store_key_t key;
  int rc;
  if (!store || !out) return TURBO_EINVAL;
  rc = flow_store_bytes_validate(index, 0);
  if (rc == TURBO_OK) rc = flow_store_bytes_validate(member, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_index_find_group(store, index);
  key = flow_store_key_from_bytes(member);
  *out = group && turbo_hash_map_contains(&group->members, &key);
  store->stats.queries++;
  return TURBO_OK;
}

static int flow_index_memory_count(void *ctx, turbo_flow_store_bytes_t index, size_t *out) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  flow_index_group_t *group;
  int rc;
  if (!store || !out) return TURBO_EINVAL;
  rc = flow_store_bytes_validate(index, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_index_find_group(store, index);
  *out = group ? turbo_hash_map_size(&group->members) : 0u;
  store->stats.queries++;
  return TURBO_OK;
}

static int flow_index_memory_visit(void *provider_ctx, turbo_flow_store_bytes_t index,
                                   turbo_flow_index_visit_fn visit, void *visit_ctx) {
  flow_index_memory_t *store = (flow_index_memory_t *)provider_ctx;
  flow_index_group_t *group;
  size_t slot;
  int rc;
  if (!store || !visit) return TURBO_EINVAL;
  rc = flow_store_bytes_validate(index, 0);
  if (rc != TURBO_OK) return rc;
  group = flow_index_find_group(store, index);
  store->stats.queries++;
  if (!group) return TURBO_ENOENT;
  for (slot = 0u; slot < turbo_hash_map_capacity(&group->members); ++slot) {
    mem_buffer_t *const *owned =
        (mem_buffer_t *const *)turbo_hash_map_value_at_const(&group->members, slot);
    turbo_flow_store_bytes_t member;
    if (!owned) continue;
    member.data = (const uint8_t *)mem_buffer_const_data(*owned);
    member.size = mem_buffer_used(*owned);
    rc = visit(visit_ctx, member);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_index_memory_intersection_count(void *ctx, const turbo_flow_store_bytes_t *indices,
                                                size_t index_count, size_t *out) {
  flow_index_memory_t *store = (flow_index_memory_t *)ctx;
  flow_index_group_t *smallest = NULL;
  size_t i;
  size_t slot;
  size_t result = 0u;
  if (!store || !indices || index_count == 0u || !out) return TURBO_EINVAL;
  for (i = 0u; i < index_count; ++i) {
    flow_index_group_t *group;
    int rc = flow_store_bytes_validate(indices[i], 0);
    if (rc != TURBO_OK) return rc;
    group = flow_index_find_group(store, indices[i]);
    if (!group) {
      *out = 0u;
      store->stats.queries++;
      return TURBO_OK;
    }
    if (!smallest ||
        turbo_hash_map_size(&group->members) < turbo_hash_map_size(&smallest->members)) {
      smallest = group;
    }
  }
  for (slot = 0u; slot < turbo_hash_map_capacity(&smallest->members); ++slot) {
    const flow_store_key_t *member_key =
        (const flow_store_key_t *)turbo_hash_map_key_at(&smallest->members, slot);
    int present = 1;
    if (!member_key) continue;
    for (i = 0u; i < index_count; ++i) {
      flow_index_group_t *group = flow_index_find_group(store, indices[i]);
      if (group != smallest && !turbo_hash_map_contains(&group->members, member_key)) {
        present = 0;
        break;
      }
    }
    result += (size_t)present;
  }
  *out = result;
  store->stats.queries++;
  return TURBO_OK;
}

static int flow_index_memory_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  const flow_index_memory_t *store = (const flow_index_memory_t *)ctx;
  if (!store) return TURBO_EINVAL;
  return flow_store_stats_copy(&store->stats, out);
}
