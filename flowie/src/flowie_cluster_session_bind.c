#include "flowie_cluster_session_bind_internal.h"

#include "flowie_cluster_lifecycle_dispatch_internal.h"
#include "flowie_cluster_peer_wire_internal.h"
#include "flowie_topic_index_internal.h"
#include "monocypher.h"
#include "turbo_hash.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_SESSION_FACT_MAGIC[4] = {'T', 'F', 'S', 'E'};
static const uint8_t FLOWIE_CLUSTER_SESSION_BOUND_EVENT_MAGIC[4] = {'T', 'F', 'B', 'E'};
static const uint8_t FLOWIE_CLUSTER_SESSION_UPDATED_EVENT_MAGIC[4] = {'T', 'F', 'U', 'E'};
static const uint8_t FLOWIE_CLUSTER_SESSION_CONNECTION_LOST_EVENT_MAGIC[4] = {'T', 'F', 'L', 'E'};
static const uint8_t FLOWIE_CLUSTER_SESSION_TAKEN_OVER_EVENT_MAGIC[4] = {'T', 'F', 'T', 'E'};
static const uint8_t FLOWIE_CLUSTER_SESSION_BIND_REPLY_MAGIC[4] = {'T', 'F', 'B', 'R'};
static const uint8_t FLOWIE_CLUSTER_SESSION_LIFECYCLE_COMMAND_DOMAIN[] =
    "flowie.cluster.session.lifecycle.command.v1";

typedef struct flowie_cluster_session_entry_s {
  tstr_t client_id;
  tstr_t bind_payload;
  tstr_t edge_node_id;
  tstr_v key;
  flowie_session_owner_t *owner;
  turbo_flow_security_principal_t principal;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t edge_action_sequence;
  uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
} flowie_cluster_session_entry_t;

typedef struct flowie_cluster_session_edge_binding_s {
  tstr_v edge_node_id;
  const uint8_t *edge_boot_id;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t action_sequence;
} flowie_cluster_session_edge_binding_t;

typedef struct flowie_cluster_subscription_member_s {
  flowie_cluster_session_entry_t *entry;
  uint64_t session_id;
  uint8_t qos;
  uint8_t no_local;
  uint8_t retain_as_published;
  uint32_t subscription_identifier;
} flowie_cluster_subscription_member_t;

typedef struct flowie_cluster_subscription_group_s {
  tstr_t filter;
  turbo_vec_t members;
  turbo_flow_pattern_selector_t selector;
  uint8_t shared;
} flowie_cluster_subscription_group_t;

typedef struct flowie_cluster_subscription_index_s {
  flowie_topic_index_t topics;
  turbo_vec_t groups;
  turbo_hash_map_t filter_index;
  int initialized;
} flowie_cluster_subscription_index_t;

typedef struct flowie_cluster_publish_target_owned_s {
  flowie_cluster_session_entry_t *entry;
  flowie_session_snapshot_t snapshot;
  turbo_vec_t subscription_identifiers;
  uint8_t qos;
  uint8_t retain_as_published;
  tstr_v shared_filter;
} flowie_cluster_publish_target_owned_t;

typedef struct flowie_cluster_session_plan_s {
  struct flowie_cluster_session_bind_s *bind;
  flowie_cluster_session_entry_t *entry;
  flowie_session_owner_t *expected_owner;
  flowie_session_owner_t *staged;
  turbo_flow_security_principal_t principal;
  tstr_t bind_payload;
  tstr_t edge_node_id;
  tstr_t outbox_payload;
  tstr_t reply;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  flowie_cluster_peer_owner_complete_fn complete;
  void *completion_ctx;
  int new_entry;
  int publish;
  int persist_event;
  int update_binding;
  int reindex;
  int staged_index_initialized;
  uint32_t event_type;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t edge_action_sequence;
  uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_subscription_index_t staged_index;
} flowie_cluster_session_plan_t;

struct flowie_cluster_session_bind_s {
  size_t max_sessions;
  size_t max_bind_payload_size;
  size_t max_fact_value_size;
  size_t max_event_payload_size;
  flowie_session_config_t session;
  uint64_t next_session_id;
  uint8_t security_enabled;
  flowie_cluster_session_fact_submit_fn submit;
  void *submit_ctx;
  flowie_cluster_session_now_fn now;
  void *now_ctx;
  flowie_cluster_session_self_fence_fn self_fence;
  void *self_fence_ctx;
  turbo_hash_map_t sessions;
  flowie_cluster_subscription_index_t subscription_index;
  int sessions_initialized;
  atomic_int inflight;
};

struct flowie_cluster_session_recovery_s {
  flowie_cluster_session_bind_t *bind;
  turbo_hash_map_t sessions;
  turbo_hash_map_t session_ids;
  uint64_t next_session_id;
  int sessions_initialized;
  int session_ids_initialized;
  int published;
  int status;
};

struct flowie_cluster_session_delivery_plan_s {
  flowie_cluster_session_bind_t *bind;
  flowie_cluster_session_entry_t *entry;
  flowie_session_owner_t *expected_owner;
  flowie_session_owner_t *staged;
  flowie_cluster_pgsql_fact_command_t command;
  flowie_cluster_pgsql_fact_mutation_t mutation;
  flowie_cluster_pgsql_event_dedupe_t dedupe;
  tstr_t packet;
  tstr_t fact_value;
  tstr_t action;
  uint64_t next_action_sequence;
  int publish_owner;
  int publish_action_sequence;
};

struct flowie_cluster_session_lifecycle_plan_s {
  flowie_cluster_session_bind_t *bind;
  flowie_cluster_session_entry_t *entry;
  flowie_session_owner_t *expected_owner;
  flowie_session_owner_t *staged;
  flowie_cluster_lifecycle_action_t action;
  flowie_cluster_pgsql_fact_command_t command;
  flowie_cluster_pgsql_fact_mutation_t mutation;
  tstr_t packet;
  tstr_t fact_value;
  tstr_t event_payload;
  flowie_cluster_subscription_index_t staged_index;
  int staged_index_initialized;
};

static int flowie_cluster_session_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  if (!bytes) return 0;
  for (size_t index = 0u; index < size; ++index)
    combined |= bytes[index];
  return combined != 0u;
}

static size_t flowie_cluster_session_key_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *client_id = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(client_id->data, client_id->len, NULL);
}

static bool flowie_cluster_session_key_equal(const void *left, const void *right, size_t key_size,
                                             void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static int flowie_cluster_subscription_is_shared(flowie_mqtt_span_t filter) {
  static const uint8_t prefix[] = "$share/";
  return filter.size > sizeof(prefix) - 1u && memcmp(filter.data, prefix, sizeof(prefix) - 1u) == 0;
}

static void
flowie_cluster_subscription_group_destroy(flowie_cluster_subscription_group_t *group) {
  if (!group) return;
  turbo_vec_destroy(&group->members);
  tstr_freep(&group->filter);
  memset(group, 0, sizeof(*group));
}

static void
flowie_cluster_subscription_index_destroy(flowie_cluster_subscription_index_t *index) {
  if (!index || !index->initialized) return;
  for (size_t slot = 0u; slot < turbo_vec_size(&index->groups); ++slot) {
    flowie_cluster_subscription_group_t *group =
        (flowie_cluster_subscription_group_t *)turbo_vec_at(&index->groups, slot);
    flowie_cluster_subscription_group_destroy(group);
  }
  flowie_topic_index_destroy(&index->topics);
  turbo_hash_map_destroy(&index->filter_index);
  turbo_vec_destroy(&index->groups);
  memset(index, 0, sizeof(*index));
}

static flowie_cluster_subscription_group_t *flowie_cluster_subscription_group_find(
    flowie_cluster_subscription_index_t *index, flowie_mqtt_span_t filter) {
  tstr_v key;
  const size_t *slot;
  if (!index || !index->initialized || !filter.data || filter.size == 0u) return NULL;
  key = tstr_v_from_buf((const char *)filter.data, filter.size);
  slot = (const size_t *)turbo_hash_map_get_const(&index->filter_index, &key);
  return slot ? (flowie_cluster_subscription_group_t *)turbo_vec_at(&index->groups, *slot) : NULL;
}

static const flowie_cluster_subscription_group_t *flowie_cluster_subscription_group_find_const(
    const flowie_cluster_subscription_index_t *index, flowie_mqtt_span_t filter) {
  return flowie_cluster_subscription_group_find((flowie_cluster_subscription_index_t *)index,
                                                filter);
}

static int flowie_cluster_subscription_index_init(flowie_cluster_subscription_index_t *index) {
  int rc;
  if (!index) return TURBO_EINVAL;
  memset(index, 0, sizeof(*index));
  rc = turbo_vec_init(&index->groups, sizeof(flowie_cluster_subscription_group_t));
  if (rc != TURBO_OK) return rc;
  rc = turbo_hash_map_init(&index->filter_index, sizeof(tstr_v), sizeof(size_t),
                           flowie_cluster_session_key_hash, flowie_cluster_session_key_equal, NULL);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&index->groups);
    return rc;
  }
  rc = flowie_topic_index_init(&index->topics);
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&index->filter_index);
    turbo_vec_destroy(&index->groups);
    return rc;
  }
  index->initialized = 1;
  return TURBO_OK;
}

static int flowie_cluster_subscription_group_add(
    flowie_cluster_subscription_index_t *index,
    const flowie_cluster_subscription_index_t *previous, flowie_mqtt_span_t filter,
    flowie_cluster_subscription_group_t **out) {
  flowie_cluster_subscription_group_t created;
  const flowie_cluster_subscription_group_t *old_group;
  flowie_cluster_subscription_group_t *stored;
  tstr_v key;
  size_t slot;
  int rc;
  if (!index || !index->initialized || !filter.data || filter.size == 0u || !out)
    return TURBO_EINVAL;
  memset(&created, 0, sizeof(created));
  created.filter = tstr_new_len(filter.data, filter.size);
  if (!created.filter) return TURBO_ENOMEM;
  rc = turbo_vec_init(&created.members, sizeof(flowie_cluster_subscription_member_t));
  if (rc != TURBO_OK) {
    tstr_free(created.filter);
    return rc;
  }
  created.shared = (uint8_t)flowie_cluster_subscription_is_shared(filter);
  rc = turbo_flow_pattern_selector_init(&created.selector);
  if (rc != TURBO_OK) {
    flowie_cluster_subscription_group_destroy(&created);
    return rc;
  }
  old_group = flowie_cluster_subscription_group_find_const(previous, filter);
  if (old_group) {
    const uint_fast64_t cursor =
        atomic_load_explicit(&old_group->selector.cursor, memory_order_relaxed);
    atomic_store_explicit(&created.selector.cursor, cursor, memory_order_relaxed);
  }
  rc = turbo_vec_push(&index->groups, &created);
  if (rc != TURBO_OK) {
    flowie_cluster_subscription_group_destroy(&created);
    return rc;
  }
  slot = turbo_vec_size(&index->groups) - 1u;
  stored = (flowie_cluster_subscription_group_t *)turbo_vec_at(&index->groups, slot);
  if (!stored || !stored->filter) return TURBO_EPROTO;
  key = tstr_to_v(stored->filter);
  rc = turbo_hash_map_put(&index->filter_index, &key, &slot);
  if (rc != TURBO_OK) return rc;
  *out = stored;
  return TURBO_OK;
}

static int flowie_cluster_subscription_index_add_session(
    flowie_cluster_subscription_index_t *index,
    const flowie_cluster_subscription_index_t *previous, flowie_cluster_session_entry_t *entry,
    flowie_session_owner_t *owner) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  int rc;
  if (!index || !entry || !owner) return TURBO_EINVAL;
  rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc != TURBO_OK) return rc;
  for (size_t slot = 0u; slot < snapshot.subscription_count; ++slot) {
    flowie_session_subscription_t subscription = FLOWIE_SESSION_SUBSCRIPTION_INIT;
    flowie_cluster_subscription_group_t *group;
    flowie_cluster_subscription_member_t member;
    rc = flowie_session_owner_subscription_at(owner, slot, &subscription);
    if (rc != TURBO_OK) return rc;
    group = flowie_cluster_subscription_group_find(index, subscription.filter);
    if (!group) {
      rc = flowie_cluster_subscription_group_add(index, previous, subscription.filter, &group);
      if (rc != TURBO_OK) return rc;
    }
    memset(&member, 0, sizeof(member));
    member.entry = entry;
    member.session_id = snapshot.session_id;
    member.qos = subscription.qos;
    member.no_local = subscription.no_local;
    member.retain_as_published = subscription.retain_as_published;
    member.subscription_identifier = subscription.subscription_identifier;
    rc = turbo_vec_push(&group->members, &member);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flowie_cluster_subscription_index_build(
    flowie_cluster_subscription_index_t *out, turbo_hash_map_t *sessions,
    const flowie_cluster_subscription_index_t *previous, flowie_cluster_session_entry_t *replacement,
    flowie_session_owner_t *replacement_owner, int append_replacement) {
  int rc;
  if (!out || !sessions ||
      (append_replacement && (!replacement || !replacement_owner)))
    return TURBO_EINVAL;
  rc = flowie_cluster_subscription_index_init(out);
  if (rc != TURBO_OK) return rc;
  for (size_t slot = 0u; slot < turbo_hash_map_capacity(sessions); ++slot) {
    flowie_cluster_session_entry_t **value =
        (flowie_cluster_session_entry_t **)turbo_hash_map_value_at(sessions, slot);
    flowie_cluster_session_entry_t *entry = value ? *value : NULL;
    flowie_session_owner_t *owner;
    if (!entry) continue;
    if (entry == replacement && !replacement_owner) continue;
    owner = entry == replacement ? replacement_owner : entry->owner;
    rc = flowie_cluster_subscription_index_add_session(out, previous, entry, owner);
    if (rc != TURBO_OK) goto fail;
  }
  if (append_replacement) {
    rc = flowie_cluster_subscription_index_add_session(out, previous, replacement,
                                                       replacement_owner);
    if (rc != TURBO_OK) goto fail;
  }
  for (size_t slot = 0u; slot < turbo_vec_size(&out->groups); ++slot) {
    flowie_cluster_subscription_group_t *group =
        (flowie_cluster_subscription_group_t *)turbo_vec_at(&out->groups, slot);
    flowie_mqtt_span_t filter;
    if (!group || !group->filter) {
      rc = TURBO_EPROTO;
      goto fail;
    }
    filter = (flowie_mqtt_span_t){(const uint8_t *)group->filter, tstr_len(group->filter)};
    rc = flowie_topic_index_insert(&out->topics, filter, slot);
    if (rc != TURBO_OK) goto fail;
  }
  return TURBO_OK;

fail:
  flowie_cluster_subscription_index_destroy(out);
  return rc;
}

static flowie_cluster_session_entry_t *
flowie_cluster_session_find(const flowie_cluster_session_bind_t *bind,
                            flowie_mqtt_span_t client_id) {
  tstr_v key = tstr_v_from_buf((const char *)client_id.data, client_id.size);
  flowie_cluster_session_entry_t *const *found;
  if (!bind || !bind->sessions_initialized || !client_id.data || client_id.size == 0u) return NULL;
  found = (flowie_cluster_session_entry_t *const *)turbo_hash_map_get_const(&bind->sessions, &key);
  return found ? *found : NULL;
}

static void flowie_cluster_session_entry_destroy(flowie_cluster_session_entry_t *entry) {
  if (!entry) return;
  flowie_session_owner_destroy(entry->owner);
  tstr_freep(&entry->client_id);
  tstr_freep(&entry->bind_payload);
  tstr_freep(&entry->edge_node_id);
  free(entry);
}

static void flowie_cluster_session_map_destroy(turbo_hash_map_t *sessions) {
  if (!sessions) return;
  for (size_t index = 0u; index < turbo_hash_map_capacity(sessions); ++index) {
    flowie_cluster_session_entry_t **entry =
        (flowie_cluster_session_entry_t **)turbo_hash_map_value_at(sessions, index);
    if (entry) flowie_cluster_session_entry_destroy(*entry);
  }
  turbo_hash_map_destroy(sessions);
}

static int flowie_cluster_session_map_init(turbo_hash_map_t *sessions, size_t capacity) {
  int rc;
  if (!sessions || capacity == 0u) return TURBO_EINVAL;
  rc = turbo_hash_map_init(sessions, sizeof(tstr_v), sizeof(flowie_cluster_session_entry_t *),
                           flowie_cluster_session_key_hash, flowie_cluster_session_key_equal, NULL);
  if (rc != TURBO_OK) return rc;
  rc = turbo_hash_map_reserve(sessions, capacity);
  if (rc != TURBO_OK) turbo_hash_map_destroy(sessions);
  return rc;
}

static void flowie_cluster_session_plan_destroy(flowie_cluster_session_plan_t *plan) {
  if (!plan) return;
  if (plan->staged_index_initialized)
    flowie_cluster_subscription_index_destroy(&plan->staged_index);
  flowie_session_owner_destroy(plan->staged);
  if (plan->new_entry) flowie_cluster_session_entry_destroy(plan->entry);
  tstr_freep(&plan->bind_payload);
  tstr_freep(&plan->edge_node_id);
  tstr_freep(&plan->outbox_payload);
  tstr_freep(&plan->reply);
  free(plan);
}

static int flowie_cluster_session_parse_status(int rc) {
  if (rc == FLOWIE_MQTT_PARSE_INVALID_ARGUMENT) return TURBO_EINVAL;
  if (rc == FLOWIE_MQTT_PARSE_TOO_LARGE) return TURBO_EMSGSIZE;
  if (rc == FLOWIE_MQTT_PARSE_NO_MEMORY) return TURBO_ENOMEM;
  return TURBO_EPROTO;
}

static int flowie_cluster_session_reply_encode(const flowie_session_connect_result_t *decision,
                                               size_t maximum, tstr_t *out) {
  size_t written = 0u;
  uint32_t flags;
  uint8_t *bytes;
  int rc;
  if (!decision || !out || maximum <= FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE)
    return TURBO_EINVAL;
  *out = tstr_new_len(NULL, maximum);
  if (!*out) return TURBO_ENOMEM;
  bytes = (uint8_t *)*out;
  rc = flowie_mqtt_control_packet_encode(
      &decision->reply, bytes + FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE,
      maximum - FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    tstr_freep(out);
    return flowie_cluster_session_parse_status(rc);
  }
  flags = (decision->accepted ? 1u : 0u) | (decision->close_after_reply ? 2u : 0u) |
          (decision->session_present ? 4u : 0u) | ((uint32_t)decision->reply.version << 8u);
  memcpy(bytes, FLOWIE_CLUSTER_SESSION_BIND_REPLY_MAGIC,
         sizeof(FLOWIE_CLUSTER_SESSION_BIND_REPLY_MAGIC));
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(
      bytes + 8u, (uint32_t)(FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE + written));
  flowie_cluster_peer_wire_write_u32(bytes + 12u, flags);
  flowie_cluster_peer_wire_write_u64(bytes + 16u, decision->route.owner_instance_id);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, decision->route.session_id);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, decision->route.session_generation);
  written += FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE;
  if (!tstr_set_len_checked(*out, written)) {
    tstr_freep(out);
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int
flowie_cluster_session_principal_same_owner(const turbo_flow_security_principal_t *left,
                                            const turbo_flow_security_principal_t *right) {
  return left && right && strcmp(left->principal_id, right->principal_id) == 0 &&
         strcmp(left->principal_type, right->principal_type) == 0 &&
         strcmp(left->domain_id, right->domain_id) == 0;
}

static int flowie_cluster_session_size_add(size_t *total, size_t value) {
  if (!total || value > SIZE_MAX - *total) return TURBO_ERANGE;
  *total += value;
  return TURBO_OK;
}

static int flowie_cluster_session_fact_encode(const flowie_cluster_session_bind_t *bind,
                                              tstr_v bind_payload,
                                              const flowie_session_owner_t *owner,
                                              const flowie_cluster_session_edge_binding_t *binding,
                                              tstr_t *out) {
  size_t owner_size = 0u;
  size_t total = FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE;
  uint8_t *bytes;
  int rc;
  if (!bind || !bind_payload.data || bind_payload.len == 0u || !owner || !binding ||
      (binding->edge_node_id.len != 0u &&
       (!binding->edge_node_id.data || !binding->edge_boot_id || binding->connection_id == 0u ||
        binding->connection_generation == 0u ||
        !flowie_cluster_session_nonzero(binding->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))) ||
      (binding->edge_node_id.len == 0u &&
       (binding->action_sequence != 0u || binding->connection_id != 0u ||
        binding->connection_generation != 0u || binding->edge_boot_id != NULL)) ||
      binding->edge_node_id.len > UINT16_MAX || !out)
    return TURBO_EINVAL;
  *out = NULL;
  rc = flowie_session_owner_record_encode(owner, NULL, 0u, &owner_size);
  if (rc != TURBO_ENOSPC || owner_size == 0u) return rc == TURBO_OK ? TURBO_EPROTO : rc;
  rc = flowie_cluster_session_size_add(&total, bind_payload.len);
  if (rc == TURBO_OK) rc = flowie_cluster_session_size_add(&total, owner_size);
  if (rc == TURBO_OK) rc = flowie_cluster_session_size_add(&total, binding->edge_node_id.len);
  if (rc != TURBO_OK) return rc;
  if (total > bind->max_fact_value_size || total > UINT32_MAX || bind_payload.len > UINT32_MAX ||
      owner_size > UINT32_MAX)
    return TURBO_EMSGSIZE;
  *out = tstr_new_len(NULL, total);
  if (!*out) return TURBO_ENOMEM;
  bytes = (uint8_t *)*out;
  memcpy(bytes, FLOWIE_CLUSTER_SESSION_FACT_MAGIC, sizeof(FLOWIE_CLUSTER_SESSION_FACT_MAGIC));
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_FACT_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  flowie_cluster_peer_wire_write_u32(bytes + 12u, (uint32_t)bind_payload.len);
  flowie_cluster_peer_wire_write_u32(bytes + 16u, (uint32_t)owner_size);
  flowie_cluster_peer_wire_write_u32(bytes + 20u, bind->security_enabled ? 1u : 0u);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, binding->action_sequence);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, binding->connection_id);
  flowie_cluster_peer_wire_write_u64(bytes + 40u, binding->connection_generation);
  flowie_cluster_peer_wire_write_u16(bytes + 48u, (uint16_t)binding->edge_node_id.len);
  flowie_cluster_peer_wire_write_u16(bytes + 50u, 0u);
  flowie_cluster_peer_wire_write_u32(bytes + 52u, 0u);
  if (binding->edge_boot_id)
    memcpy(bytes + 56u, binding->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  else
    memset(bytes + 56u, 0, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  memcpy(bytes + FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE, bind_payload.data, bind_payload.len);
  rc = flowie_session_owner_record_encode(
      owner, bytes + FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE + bind_payload.len, owner_size,
      &owner_size);
  if (rc == TURBO_OK && binding->edge_node_id.len != 0u)
    memcpy(bytes + FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE + bind_payload.len + owner_size,
           binding->edge_node_id.data, binding->edge_node_id.len);
  if (rc != TURBO_OK) tstr_freep(out);
  return rc;
}

int flowie_cluster_session_fact_route_decode(
    const flowie_cluster_session_bind_config_t *config,
    const flowie_cluster_pgsql_fact_record_t *record,
    flowie_cluster_session_fact_route_view_t *out) {
  flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_owner_t *owner = NULL;
  flowie_cluster_session_fact_route_view_t route = FLOWIE_CLUSTER_SESSION_FACT_ROUTE_VIEW_INIT;
  const uint8_t *bytes;
  const uint8_t *bind_payload;
  const uint8_t *owner_record;
  const uint8_t *edge_node_id;
  size_t value_size;
  size_t bind_size;
  size_t owner_size;
  size_t edge_node_id_size;
  uint32_t flags;
  int rc;
  if (!config || flowie_cluster_session_bind_config_validate(config) != TURBO_OK || !record ||
      record->size < sizeof(*record) || record->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      record->key_kind != FLOWIE_CLUSTER_KEY_SESSION ||
      record->owner_epoch == 0u || record->revision == 0u || !record->key || !record->value ||
      tstr_len(record->key) == 0u || tstr_len(record->key) > FLOWIE_CLUSTER_KEY_MAX ||
      tstr_len(record->value) > config->max_fact_value_size || !out ||
      out->size != sizeof(*out) || out->abi_version != FLOWIE_CLUSTER_SESSION_BIND_ABI_V1)
    return TURBO_EINVAL;
  value_size = tstr_len(record->value);
  if (value_size <= FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE) return TURBO_EPROTO;
  bytes = (const uint8_t *)record->value;
  if (memcmp(bytes, FLOWIE_CLUSTER_SESSION_FACT_MAGIC, sizeof(FLOWIE_CLUSTER_SESSION_FACT_MAGIC)) !=
          0 ||
      flowie_cluster_peer_wire_read_u16(bytes + 4u) != FLOWIE_CLUSTER_SESSION_FACT_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) != FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != value_size)
    return TURBO_EPROTO;
  bind_size = flowie_cluster_peer_wire_read_u32(bytes + 12u);
  owner_size = flowie_cluster_peer_wire_read_u32(bytes + 16u);
  flags = flowie_cluster_peer_wire_read_u32(bytes + 20u);
  route.connection_id = flowie_cluster_peer_wire_read_u64(bytes + 32u);
  route.connection_generation = flowie_cluster_peer_wire_read_u64(bytes + 40u);
  edge_node_id_size = flowie_cluster_peer_wire_read_u16(bytes + 48u);
  route.edge_boot_id = bytes + 56u;
  if (bind_size == 0u || bind_size > config->max_bind_payload_size || owner_size == 0u ||
      bind_size > value_size - FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE ||
      owner_size > value_size - FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE - bind_size ||
      edge_node_id_size !=
          value_size - FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE - bind_size - owner_size ||
      edge_node_id_size == 0u || edge_node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      flags != (config->security_enabled ? 1u : 0u) ||
      flowie_cluster_peer_wire_read_u16(bytes + 50u) != 0u ||
      flowie_cluster_peer_wire_read_u32(bytes + 52u) != 0u || route.connection_id == 0u ||
      route.connection_generation == 0u ||
      !flowie_cluster_session_nonzero(route.edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EPROTO;
  bind_payload = bytes + FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE;
  owner_record = bind_payload + bind_size;
  edge_node_id = owner_record + owner_size;
  if (memchr(edge_node_id, '\0', edge_node_id_size)) return TURBO_EPROTO;
  rc = flowie_cluster_peer_connect_bind_decode(bind_payload, bind_size,
                                               config->max_bind_payload_size, &decoded);
  if (rc != TURBO_OK) return rc;
  if (decoded.security_enabled != config->security_enabled ||
      decoded.connect.client_id.size != tstr_len(record->key) ||
      memcmp(decoded.connect.client_id.data, record->key, tstr_len(record->key)) != 0)
    return TURBO_EPROTO;
  rc = flowie_session_owner_record_restore(
      &config->session,
      (flowie_mqtt_span_t){(const uint8_t *)record->key, tstr_len(record->key)}, record->revision,
      owner_record, owner_size, &owner);
  if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(owner, &snapshot);
  if (rc == TURBO_OK &&
      (snapshot.resource_generation != record->revision || snapshot.session_id == 0u ||
       snapshot.session_generation == 0u || snapshot.client_id.size != tstr_len(record->key) ||
       memcmp(snapshot.client_id.data, record->key, tstr_len(record->key)) != 0))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    route.client_id =
        (flowie_mqtt_span_t){(const uint8_t *)record->key, tstr_len(record->key)};
    route.edge_node_id = tstr_v_from_buf((const char *)edge_node_id, edge_node_id_size);
    route.session_id = snapshot.session_id;
    route.session_generation = snapshot.session_generation;
    *out = route;
  }
  flowie_session_owner_destroy(owner);
  return rc;
}

static int flowie_cluster_session_event_encode(const flowie_cluster_session_bind_t *bind,
                                               const flowie_cluster_session_plan_t *plan,
                                               const flowie_cluster_peer_frame_t *command,
                                               const flowie_session_snapshot_t *before,
                                               const flowie_session_snapshot_t *snapshot,
                                               tstr_t *out) {
  tstr_v edge_node_id;
  const uint8_t *edge_boot_id;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint32_t event_type = plan ? plan->event_type : 0u;
  uint8_t mqtt_version = 0u;
  size_t total = FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE;
  uint8_t *bytes;
  int rc;
  if (!bind || !plan || !command || !before || !snapshot || !out ||
      (event_type != FLOWIE_CLUSTER_SESSION_EVENT_BOUND &&
       event_type != FLOWIE_CLUSTER_SESSION_EVENT_UPDATED &&
       event_type != FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST &&
       event_type != FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER))
    return TURBO_EINVAL;
  *out = NULL;
  edge_node_id = command->source_node_id;
  edge_boot_id = command->source_boot_id;
  connection_id = command->connection_id;
  connection_generation = command->connection_generation;
  if (event_type == FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER) {
    if (!plan->entry || !plan->entry->edge_node_id || !before->active ||
        !flowie_mqtt_version_is_supported(before->version) || plan->entry->connection_id == 0u ||
        plan->entry->connection_generation == 0u)
      return TURBO_EPROTO;
    edge_node_id = tstr_to_v(plan->entry->edge_node_id);
    edge_boot_id = plan->entry->edge_boot_id;
    connection_id = plan->entry->connection_id;
    connection_generation = plan->entry->connection_generation;
    mqtt_version = (uint8_t)before->version;
  }
  rc = flowie_cluster_session_size_add(&total, edge_node_id.len);
  if (rc != TURBO_OK) return rc;
  if (total > bind->max_event_payload_size || total > UINT32_MAX || edge_node_id.len > UINT16_MAX)
    return TURBO_EMSGSIZE;
  *out = tstr_new_len(NULL, total);
  if (!*out) return TURBO_ENOMEM;
  bytes = (uint8_t *)*out;
  memcpy(bytes,
         event_type == FLOWIE_CLUSTER_SESSION_EVENT_BOUND ? FLOWIE_CLUSTER_SESSION_BOUND_EVENT_MAGIC
         : event_type == FLOWIE_CLUSTER_SESSION_EVENT_UPDATED
             ? FLOWIE_CLUSTER_SESSION_UPDATED_EVENT_MAGIC
         : event_type == FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST
             ? FLOWIE_CLUSTER_SESSION_CONNECTION_LOST_EVENT_MAGIC
             : FLOWIE_CLUSTER_SESSION_TAKEN_OVER_EVENT_MAGIC,
         sizeof(FLOWIE_CLUSTER_SESSION_BOUND_EVENT_MAGIC));
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  bytes[12] = mqtt_version;
  memset(bytes + 13u, 0, 3u);
  flowie_cluster_peer_wire_write_u64(bytes + 16u, connection_id);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, connection_generation);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, snapshot->session_id);
  flowie_cluster_peer_wire_write_u64(bytes + 40u, snapshot->session_generation);
  memcpy(bytes + 48u, edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  flowie_cluster_peer_wire_write_u16(bytes + 64u, (uint16_t)edge_node_id.len);
  flowie_cluster_peer_wire_write_u16(bytes + 66u, 0u);
  memcpy(bytes + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, edge_node_id.data,
         edge_node_id.len);
  return TURBO_OK;
}

int flowie_cluster_session_bind_config_validate(
    const flowie_cluster_session_bind_config_t *config) {
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_SESSION_BIND_ABI_V1 || config->max_sessions == 0u ||
      config->max_bind_payload_size <= FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE ||
      config->max_bind_payload_size > UINT32_MAX ||
      config->max_fact_value_size < FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE ||
      config->max_fact_value_size > UINT32_MAX ||
      config->max_event_payload_size < FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE ||
      config->max_event_payload_size > UINT32_MAX ||
      config->session.size != sizeof(config->session) ||
      config->session.abi_version != FLOWIE_SESSION_INTERNAL_ABI_V1 ||
      config->session.owner_instance_id == 0u || config->session.session_id != 0u ||
      config->session.max_subscriptions > FLOWIE_SESSION_INTERNAL_MAX_SUBSCRIPTIONS ||
      config->session.max_inflight > UINT16_MAX || config->first_session_id == UINT64_MAX ||
      config->security_enabled > 1u || !config->submit || !config->now || !config->self_fence)
    return TURBO_EINVAL;
  rc = turbo_flow_protocol_settlement_policy_validate(&config->session.settlement);
  return rc == TURBO_OK ? TURBO_OK : TURBO_EINVAL;
}

int flowie_cluster_session_bind_create(const flowie_cluster_session_bind_config_t *config,
                                       flowie_cluster_session_bind_t **out) {
  flowie_cluster_session_bind_t *bind;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = flowie_cluster_session_bind_config_validate(config);
  if (rc != TURBO_OK) return rc;
  bind = (flowie_cluster_session_bind_t *)calloc(1u, sizeof(*bind));
  if (!bind) return TURBO_ENOMEM;
  bind->max_sessions = config->max_sessions;
  bind->max_bind_payload_size = config->max_bind_payload_size;
  bind->max_fact_value_size = config->max_fact_value_size;
  bind->max_event_payload_size = config->max_event_payload_size;
  bind->session = config->session;
  bind->next_session_id = config->first_session_id;
  bind->security_enabled = config->security_enabled;
  bind->submit = config->submit;
  bind->submit_ctx = config->submit_ctx;
  bind->now = config->now;
  bind->now_ctx = config->now_ctx;
  bind->self_fence = config->self_fence;
  bind->self_fence_ctx = config->self_fence_ctx;
  atomic_init(&bind->inflight, 0);
  rc = flowie_cluster_session_map_init(&bind->sessions, bind->max_sessions);
  if (rc == TURBO_OK) {
    bind->sessions_initialized = 1;
    rc = flowie_cluster_subscription_index_init(&bind->subscription_index);
  }
  if (rc != TURBO_OK) {
    (void)flowie_cluster_session_bind_destroy(bind);
    return rc;
  }
  *out = bind;
  return TURBO_OK;
}

int flowie_cluster_session_bind_destroy(flowie_cluster_session_bind_t *bind) {
  if (!bind) return TURBO_OK;
  if (atomic_load_explicit(&bind->inflight, memory_order_acquire)) return TURBO_EBUSY;
  if (bind->sessions_initialized) {
    flowie_cluster_session_map_destroy(&bind->sessions);
  }
  flowie_cluster_subscription_index_destroy(&bind->subscription_index);
  free(bind);
  return TURBO_OK;
}

static int flowie_cluster_session_recovery_decode(flowie_cluster_session_recovery_t *recovery,
                                                  const turbo_flow_record_view_t *record,
                                                  flowie_cluster_session_entry_t **out,
                                                  uint64_t *session_id_out) {
  flowie_cluster_session_bind_t *bind;
  flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
  flowie_session_config_t session_config;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_cluster_session_entry_t *entry = NULL;
  const uint8_t *bytes;
  const uint8_t *bind_payload;
  const uint8_t *owner_record;
  const uint8_t *edge_node_id;
  size_t header_size;
  size_t bind_size;
  size_t owner_size;
  size_t edge_node_id_size = 0u;
  uint64_t edge_action_sequence = 0u;
  uint64_t connection_id = 0u;
  uint64_t connection_generation = 0u;
  const uint8_t *edge_boot_id = NULL;
  uint16_t version;
  uint32_t flags;
  int rc;
  if (out) *out = NULL;
  if (session_id_out) *session_id_out = 0u;
  if (!recovery || !(bind = recovery->bind) || !record || record->size < sizeof(*record) ||
      !record->key || record->key_size == 0u || record->key_size > UINT16_MAX || !record->value ||
      record->value_size <= FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1 ||
      record->value_size > bind->max_fact_value_size || record->revision == 0u ||
      record->revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX || !out || !session_id_out)
    return TURBO_EINVAL;
  bytes = record->value;
  version = flowie_cluster_peer_wire_read_u16(bytes + 4u);
  header_size = flowie_cluster_peer_wire_read_u16(bytes + 6u);
  if (memcmp(bytes, FLOWIE_CLUSTER_SESSION_FACT_MAGIC, sizeof(FLOWIE_CLUSTER_SESSION_FACT_MAGIC)) !=
          0 ||
      ((version == FLOWIE_CLUSTER_SESSION_FACT_VERSION_V1 &&
        header_size != FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1) ||
       (version == FLOWIE_CLUSTER_SESSION_FACT_VERSION &&
        header_size != FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE) ||
       (version != FLOWIE_CLUSTER_SESSION_FACT_VERSION_V1 &&
        version != FLOWIE_CLUSTER_SESSION_FACT_VERSION)) ||
      header_size >= record->value_size ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != record->value_size)
    return TURBO_EPROTO;
  bind_size = flowie_cluster_peer_wire_read_u32(bytes + 12u);
  owner_size = flowie_cluster_peer_wire_read_u32(bytes + 16u);
  flags = flowie_cluster_peer_wire_read_u32(bytes + 20u);
  if (version == FLOWIE_CLUSTER_SESSION_FACT_VERSION) {
    edge_action_sequence = flowie_cluster_peer_wire_read_u64(bytes + 24u);
    connection_id = flowie_cluster_peer_wire_read_u64(bytes + 32u);
    connection_generation = flowie_cluster_peer_wire_read_u64(bytes + 40u);
    edge_node_id_size = flowie_cluster_peer_wire_read_u16(bytes + 48u);
    edge_boot_id = bytes + 56u;
    if (flowie_cluster_peer_wire_read_u16(bytes + 50u) != 0u ||
        flowie_cluster_peer_wire_read_u32(bytes + 52u) != 0u ||
        (edge_node_id_size == 0u &&
         (edge_action_sequence != 0u || connection_id != 0u || connection_generation != 0u ||
          flowie_cluster_session_nonzero(edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))) ||
        (edge_node_id_size != 0u &&
         (connection_id == 0u || connection_generation == 0u ||
          !flowie_cluster_session_nonzero(edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))))
      return TURBO_EPROTO;
  }
  if (bind_size == 0u || bind_size > bind->max_bind_payload_size || owner_size == 0u ||
      bind_size > record->value_size - header_size ||
      owner_size > record->value_size - header_size - bind_size ||
      edge_node_id_size != record->value_size - header_size - bind_size - owner_size ||
      flags != (bind->security_enabled ? 1u : 0u))
    return TURBO_EPROTO;
  bind_payload = bytes + header_size;
  owner_record = bind_payload + bind_size;
  edge_node_id = owner_record + owner_size;
  rc = flowie_cluster_peer_connect_bind_decode(bind_payload, bind_size, bind->max_bind_payload_size,
                                               &decoded);
  if (rc != TURBO_OK) return rc;
  if (decoded.security_enabled != bind->security_enabled ||
      decoded.connect.client_id.size != record->key_size ||
      memcmp(decoded.connect.client_id.data, record->key, record->key_size) != 0)
    return TURBO_EPROTO;
  session_config = bind->session;
  session_config.session_id = 1u;
  entry = (flowie_cluster_session_entry_t *)calloc(1u, sizeof(*entry));
  if (!entry) return TURBO_ENOMEM;
  entry->client_id = tstr_new_len(record->key, record->key_size);
  entry->bind_payload = tstr_new_len(bind_payload, bind_size);
  entry->edge_node_id = edge_node_id_size != 0u ? tstr_new_len(edge_node_id, edge_node_id_size)
                                               : NULL;
  if (!entry->client_id || !entry->bind_payload ||
      (edge_node_id_size != 0u && !entry->edge_node_id)) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  entry->key = tstr_to_v(entry->client_id);
  entry->principal = decoded.principal;
  entry->connection_id = connection_id;
  entry->connection_generation = connection_generation;
  entry->edge_action_sequence = edge_action_sequence;
  if (edge_boot_id) memcpy(entry->edge_boot_id, edge_boot_id, sizeof(entry->edge_boot_id));
  rc = flowie_session_owner_record_restore(
      &session_config, (flowie_mqtt_span_t){record->key, record->key_size}, record->revision,
      owner_record, owner_size, &entry->owner);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_session_owner_snapshot(entry->owner, &snapshot);
  if (rc != TURBO_OK) goto fail;
  if (snapshot.active || snapshot.owner_instance_id != bind->session.owner_instance_id ||
      snapshot.session_id == 0u || snapshot.resource_generation != record->revision ||
      snapshot.version != decoded.connect.version || snapshot.client_id.size != record->key_size ||
      memcmp(snapshot.client_id.data, record->key, record->key_size) != 0) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  *session_id_out = snapshot.session_id;
  *out = entry;
  return TURBO_OK;

fail:
  flowie_cluster_session_entry_destroy(entry);
  return rc;
}

int flowie_cluster_session_recovery_create(flowie_cluster_session_bind_t *bind,
                                           flowie_cluster_session_recovery_t **out) {
  flowie_cluster_session_recovery_t *recovery;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!bind || !bind->sessions_initialized) return TURBO_EINVAL;
  if (atomic_load_explicit(&bind->inflight, memory_order_acquire) ||
      !turbo_hash_map_empty(&bind->sessions))
    return TURBO_EBUSY;
  recovery = (flowie_cluster_session_recovery_t *)calloc(1u, sizeof(*recovery));
  if (!recovery) return TURBO_ENOMEM;
  recovery->bind = bind;
  recovery->next_session_id = bind->next_session_id;
  rc = flowie_cluster_session_map_init(&recovery->sessions, bind->max_sessions);
  if (rc != TURBO_OK) {
    free(recovery);
    return rc;
  }
  recovery->sessions_initialized = 1;
  rc = turbo_hash_map_init(&recovery->session_ids, sizeof(uint64_t), sizeof(uint8_t), NULL, NULL,
                           NULL);
  if (rc == TURBO_OK) {
    recovery->session_ids_initialized = 1;
    rc = turbo_hash_map_reserve(&recovery->session_ids, bind->max_sessions);
  }
  if (rc != TURBO_OK) {
    flowie_cluster_session_recovery_destroy(recovery);
    return rc;
  }
  *out = recovery;
  return TURBO_OK;
}

int flowie_cluster_session_recovery_visit(void *ctx, const turbo_flow_record_view_t *record) {
  flowie_cluster_session_recovery_t *recovery = (flowie_cluster_session_recovery_t *)ctx;
  flowie_cluster_session_entry_t *entry = NULL;
  uint64_t session_id = 0u;
  const uint8_t present = 1u;
  int rc;
  if (!recovery || recovery->published || !recovery->sessions_initialized ||
      !recovery->session_ids_initialized)
    return TURBO_EINVAL;
  if (recovery->status != TURBO_OK) return recovery->status;
  if (turbo_hash_map_size(&recovery->sessions) >= recovery->bind->max_sessions) {
    recovery->status = TURBO_ENOSPC;
    return recovery->status;
  }
  rc = flowie_cluster_session_recovery_decode(recovery, record, &entry, &session_id);
  if (rc != TURBO_OK) {
    recovery->status = rc;
    return rc;
  }
  if (turbo_hash_map_contains(&recovery->sessions, &entry->key) ||
      turbo_hash_map_contains(&recovery->session_ids, &session_id)) {
    flowie_cluster_session_entry_destroy(entry);
    recovery->status = TURBO_EPROTO;
    return recovery->status;
  }
  rc = turbo_hash_map_put(&recovery->sessions, &entry->key, &entry);
  if (rc != TURBO_OK) {
    flowie_cluster_session_entry_destroy(entry);
    recovery->status = rc;
    return rc;
  }
  rc = turbo_hash_map_put(&recovery->session_ids, &session_id, &present);
  if (rc != TURBO_OK) {
    (void)turbo_hash_map_remove(&recovery->sessions, &entry->key, NULL);
    flowie_cluster_session_entry_destroy(entry);
    recovery->status = rc;
    return rc;
  }
  if (session_id > recovery->next_session_id) recovery->next_session_id = session_id;
  return TURBO_OK;
}

int flowie_cluster_session_recovery_publish(flowie_cluster_session_recovery_t *recovery) {
  flowie_cluster_session_bind_t *bind;
  flowie_cluster_subscription_index_t staged_index;
  flowie_cluster_subscription_index_t previous_index;
  turbo_hash_map_t previous;
  int rc;
  if (!recovery || recovery->published || !recovery->sessions_initialized ||
      !recovery->session_ids_initialized || !(bind = recovery->bind))
    return TURBO_EINVAL;
  if (recovery->status != TURBO_OK) return recovery->status;
  if (atomic_load_explicit(&bind->inflight, memory_order_acquire) ||
      !turbo_hash_map_empty(&bind->sessions))
    return TURBO_EBUSY;
  memset(&staged_index, 0, sizeof(staged_index));
  rc = flowie_cluster_subscription_index_build(&staged_index, &recovery->sessions,
                                               &bind->subscription_index, NULL, NULL, 0);
  if (rc != TURBO_OK) return rc;
  previous = bind->sessions;
  previous_index = bind->subscription_index;
  bind->sessions = recovery->sessions;
  bind->subscription_index = staged_index;
  bind->next_session_id = recovery->next_session_id;
  memset(&recovery->sessions, 0, sizeof(recovery->sessions));
  recovery->sessions_initialized = 0;
  recovery->published = 1;
  turbo_hash_map_destroy(&recovery->session_ids);
  recovery->session_ids_initialized = 0;
  flowie_cluster_session_map_destroy(&previous);
  flowie_cluster_subscription_index_destroy(&previous_index);
  return TURBO_OK;
}

void flowie_cluster_session_recovery_destroy(flowie_cluster_session_recovery_t *recovery) {
  if (!recovery) return;
  if (recovery->sessions_initialized) flowie_cluster_session_map_destroy(&recovery->sessions);
  if (recovery->session_ids_initialized) turbo_hash_map_destroy(&recovery->session_ids);
  free(recovery);
}

int flowie_cluster_session_bind_recover_pgsql(flowie_cluster_session_bind_t *bind,
                                              flowie_cluster_pgsql_fact_store_t *store,
                                              flowie_cluster_pgsql_lease_worker_t *lease_worker) {
  flowie_cluster_pgsql_lease_snapshot_t lease = FLOWIE_CLUSTER_PGSQL_LEASE_SNAPSHOT_INIT;
  flowie_cluster_pgsql_fact_scan_t scan = FLOWIE_CLUSTER_PGSQL_FACT_SCAN_INIT;
  flowie_cluster_session_recovery_t *recovery = NULL;
  int rc;
  if (!bind || !store || !lease_worker) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_lease_worker_snapshot(lease_worker, &lease);
  if (rc != TURBO_OK) return rc;
  if (lease.state != FLOWIE_CLUSTER_SHARD_RECOVERING) return TURBO_EBUSY;
  rc = flowie_cluster_session_recovery_create(bind, &recovery);
  if (rc != TURBO_OK) return rc;
  scan.owner = lease.owner;
  scan.key_kind = FLOWIE_CLUSTER_KEY_SESSION;
  scan.max_records = bind->max_sessions;
  rc =
      flowie_cluster_pgsql_fact_scan(store, &scan, flowie_cluster_session_recovery_visit, recovery);
  if (rc == TURBO_OK) rc = flowie_cluster_session_recovery_publish(recovery);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_lease_worker_activate(lease_worker, &lease.owner);
  flowie_cluster_session_recovery_destroy(recovery);
  return rc;
}

static int flowie_cluster_session_plan_finalize(void *ctx, int durable_status,
                                                tstr_t *reply_payload) {
  flowie_cluster_session_plan_t *plan = (flowie_cluster_session_plan_t *)ctx;
  flowie_cluster_session_bind_t *bind;
  flowie_session_owner_t *previous = NULL;
  tstr_t previous_bind_payload = NULL;
  tstr_t previous_edge_node_id = NULL;
  flowie_cluster_subscription_index_t previous_index;
  int replace_index = 0;
  int rc = durable_status;
  if (!plan || !reply_payload || *reply_payload) return TURBO_EINVAL;
  bind = plan->bind;
  memset(&previous_index, 0, sizeof(previous_index));
  if (rc == TURBO_OK && plan->publish) {
    if (plan->reindex && !plan->staged_index_initialized) rc = TURBO_EPROTO;
    if (rc == TURBO_OK && plan->new_entry) {
      plan->entry->owner = plan->staged;
      plan->staged = NULL;
      plan->entry->principal = plan->principal;
      rc = turbo_hash_map_put(&bind->sessions, &plan->entry->key, &plan->entry);
      if (rc == TURBO_OK) {
        plan->new_entry = 0;
      } else {
        plan->staged = plan->entry->owner;
        plan->entry->owner = NULL;
      }
    } else if (rc == TURBO_OK && plan->entry->owner != plan->expected_owner) {
      rc = TURBO_EBUSY;
    } else if (rc == TURBO_OK) {
      previous = plan->entry->owner;
      plan->entry->owner = plan->staged;
      plan->staged = NULL;
      plan->entry->principal = plan->principal;
    }
    if (rc == TURBO_OK && plan->update_binding) {
      previous_bind_payload = plan->entry->bind_payload;
      previous_edge_node_id = plan->entry->edge_node_id;
      plan->entry->bind_payload = plan->bind_payload;
      plan->bind_payload = NULL;
      plan->entry->edge_node_id = plan->edge_node_id;
      plan->edge_node_id = NULL;
      plan->entry->connection_id = plan->connection_id;
      plan->entry->connection_generation = plan->connection_generation;
      plan->entry->edge_action_sequence = plan->edge_action_sequence;
      memcpy(plan->entry->edge_boot_id, plan->edge_boot_id, sizeof(plan->entry->edge_boot_id));
    }
    if (rc == TURBO_OK && plan->reindex) {
      previous_index = bind->subscription_index;
      bind->subscription_index = plan->staged_index;
      memset(&plan->staged_index, 0, sizeof(plan->staged_index));
      plan->staged_index_initialized = 0;
      replace_index = 1;
    }
    if (rc != TURBO_OK) bind->self_fence(bind->self_fence_ctx, rc);
  }
  if (rc == TURBO_OK) {
    *reply_payload = plan->reply;
    plan->reply = NULL;
  }
  flowie_session_owner_destroy(previous);
  if (replace_index) flowie_cluster_subscription_index_destroy(&previous_index);
  tstr_free(previous_bind_payload);
  tstr_free(previous_edge_node_id);
  flowie_cluster_session_plan_destroy(plan);
  atomic_store_explicit(&bind->inflight, 0, memory_order_release);
  return rc;
}

static void flowie_cluster_session_fact_complete(
    void *ctx, const uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE], int status) {
  flowie_cluster_session_plan_t *plan = (flowie_cluster_session_plan_t *)ctx;
  flowie_cluster_session_bind_t *bind;
  int rc;
  if (!plan) return;
  bind = plan->bind;
  if (!command_id || memcmp(command_id, plan->command_id, sizeof(plan->command_id)) != 0) {
    status = TURBO_EPROTO;
    bind->self_fence(bind->self_fence_ctx, status);
  }
  rc = plan->complete(plan->completion_ctx, status, flowie_cluster_session_plan_finalize, plan);
  if (rc == TURBO_OK) return;
  bind->self_fence(bind->self_fence_ctx, rc);
  flowie_cluster_session_plan_destroy(plan);
  atomic_store_explicit(&bind->inflight, 0, memory_order_release);
}

static int
flowie_cluster_session_prepare_entry(flowie_cluster_session_bind_t *bind,
                                     const flowie_cluster_peer_connect_bind_view_t *decoded,
                                     flowie_cluster_session_plan_t *plan) {
  flowie_session_config_t session_config;
  flowie_cluster_session_entry_t *entry =
      flowie_cluster_session_find(bind, decoded->connect.client_id);
  if (entry) {
    plan->entry = entry;
    plan->expected_owner = entry->owner;
    return TURBO_OK;
  }
  if (turbo_hash_map_size(&bind->sessions) >= bind->max_sessions) return TURBO_ENOSPC;
  if (bind->next_session_id == UINT64_MAX) return TURBO_ERANGE;
  entry = (flowie_cluster_session_entry_t *)calloc(1u, sizeof(*entry));
  if (!entry) return TURBO_ENOMEM;
  entry->client_id = tstr_new_len(decoded->connect.client_id.data, decoded->connect.client_id.size);
  if (!entry->client_id) {
    free(entry);
    return TURBO_ENOMEM;
  }
  entry->key = tstr_to_v(entry->client_id);
  session_config = bind->session;
  session_config.session_id = ++bind->next_session_id;
  plan->staged = flowie_session_owner_create(&session_config);
  if (!plan->staged) {
    flowie_cluster_session_entry_destroy(entry);
    return TURBO_ENOMEM;
  }
  plan->entry = entry;
  plan->new_entry = 1;
  return TURBO_OK;
}

static int flowie_cluster_session_binding_same(const flowie_cluster_session_entry_t *entry,
                                               const flowie_cluster_session_plan_t *plan) {
  return entry && plan && entry->edge_node_id && plan->edge_node_id &&
         entry->connection_id == plan->connection_id &&
         entry->connection_generation == plan->connection_generation &&
         tstr_len(entry->edge_node_id) == tstr_len(plan->edge_node_id) &&
         memcmp(entry->edge_node_id, plan->edge_node_id, tstr_len(entry->edge_node_id)) == 0 &&
         memcmp(entry->edge_boot_id, plan->edge_boot_id, sizeof(entry->edge_boot_id)) == 0;
}

static int flowie_cluster_session_prepare_decision(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_peer_connect_bind_view_t *decoded,
    flowie_cluster_session_plan_t *plan, flowie_session_connect_result_t *decision) {
  flowie_session_snapshot_t current = FLOWIE_SESSION_SNAPSHOT_INIT;
  int rc;
  if (!plan->new_entry && bind->security_enabled &&
      !flowie_cluster_session_principal_same_owner(&plan->entry->principal, &decoded->principal)) {
    decision->reply.type = FLOWIE_MQTT_PACKET_CONNACK;
    decision->reply.version = decoded->connect.version;
    decision->reply.reason_code =
        decoded->connect.version == FLOWIE_MQTT_VERSION_5 ? UINT8_C(0x87) : UINT8_C(0x05);
    decision->close_after_reply = 1u;
    return TURBO_OK;
  }
  if (!plan->new_entry) {
    rc = flowie_session_owner_snapshot(plan->entry->owner, &current);
    if (rc != TURBO_OK) return rc;
    plan->staged = flowie_session_owner_clone(plan->entry->owner);
    if (!plan->staged) return TURBO_ENOMEM;
  }
  rc = !plan->new_entry && current.active
           ? flowie_session_owner_connect_takeover(plan->staged, &decoded->connect, decision)
           : flowie_session_owner_connect(plan->staged, &decoded->connect, decision);
  if (rc != TURBO_OK || !decision->accepted) return rc;
  plan->reindex = 1;
  if (!plan->new_entry && current.active && !flowie_cluster_session_binding_same(plan->entry, plan))
    plan->event_type = FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER;
  {
    size_t expired_count = 0u;
    uint64_t now = bind->now(bind->now_ctx);
    if (now == 0u) return TURBO_EIO;
    rc = flowie_session_owner_delivery_expire(plan->staged, now, &expired_count);
  }
  if (rc == TURBO_OK) plan->publish = 1;
  return rc;
}

static int flowie_cluster_session_submit(flowie_cluster_session_plan_t *plan,
                                         const flowie_cluster_peer_frame_t *peer_command) {
  flowie_cluster_session_bind_t *bind;
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
  flowie_cluster_pgsql_fact_command_t command = FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  tstr_t fact_value = NULL;
  tstr_t event_payload = NULL;
  tstr_v durable_event = {NULL, 0u};
  int event_only;
  int rc;
  tstr_v bind_payload;
  flowie_cluster_session_edge_binding_t edge_binding;
  if (!plan || !peer_command || !plan->staged || (!plan->publish && !plan->persist_event))
    return TURBO_EINVAL;
  bind = plan->bind;
  if (plan->reindex) {
    rc = flowie_cluster_subscription_index_build(
        &plan->staged_index, &bind->sessions, &bind->subscription_index, plan->entry, plan->staged,
        plan->new_entry);
    if (rc != TURBO_OK) return rc;
    plan->staged_index_initialized = 1;
  }
  bind_payload =
      plan->update_binding ? tstr_to_v(plan->bind_payload) : tstr_to_v(plan->entry->bind_payload);
  memset(&edge_binding, 0, sizeof(edge_binding));
  if (plan->update_binding) {
    edge_binding.edge_node_id = tstr_to_v(plan->edge_node_id);
    edge_binding.edge_boot_id = plan->edge_boot_id;
    edge_binding.connection_id = plan->connection_id;
    edge_binding.connection_generation = plan->connection_generation;
    edge_binding.action_sequence = plan->edge_action_sequence;
  } else if (plan->entry) {
    edge_binding.edge_node_id = plan->entry->edge_node_id ? tstr_to_v(plan->entry->edge_node_id)
                                                         : (tstr_v){NULL, 0u};
    edge_binding.edge_boot_id = plan->entry->edge_node_id ? plan->entry->edge_boot_id : NULL;
    edge_binding.connection_id = plan->entry->connection_id;
    edge_binding.connection_generation = plan->entry->connection_generation;
    edge_binding.action_sequence = plan->entry->edge_action_sequence;
  }
  event_only = plan->persist_event && !plan->publish;
  if (!plan->new_entry) {
    rc = flowie_session_owner_snapshot(plan->expected_owner, &before);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_session_owner_snapshot(plan->staged, &after);
  if (rc != TURBO_OK) return rc;
  if (!after.client_id.data || after.client_id.size == 0u ||
      after.resource_generation > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
      (plan->publish && after.resource_generation <= before.resource_generation) ||
      (event_only && after.resource_generation != before.resource_generation))
    return TURBO_EPROTO;
  if (plan->publish)
    rc = flowie_cluster_session_fact_encode(bind, bind_payload, plan->staged, &edge_binding,
                                            &fact_value);
  else
    rc = TURBO_OK;
  if (rc == TURBO_OK && plan->persist_event) {
    if (plan->event_type != FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH || !plan->outbox_payload)
      rc = TURBO_EPROTO;
    durable_event = tstr_to_v(plan->outbox_payload);
  } else if (rc == TURBO_OK) {
    rc = flowie_cluster_session_event_encode(bind, plan, peer_command, &before, &after,
                                             &event_payload);
    durable_event = tstr_to_v(event_payload);
  }
  if (rc != TURBO_OK) goto done;
  mutation.write_kind = event_only ? FLOWIE_CLUSTER_PGSQL_EVENT_ONLY
                                   : FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT;
  mutation.shard_key = after.client_id.data;
  mutation.shard_key_size = after.client_id.size;
  mutation.record.kind = TURBO_FLOW_RECORD_PUT;
  mutation.record.key = after.client_id.data;
  mutation.record.key_size = after.client_id.size;
  if (plan->publish) {
    mutation.record.expected_revision = before.resource_generation;
    mutation.record.next_revision = after.resource_generation;
    mutation.record.value = (const uint8_t *)fact_value;
    mutation.record.value_size = tstr_len(fact_value);
  } else {
    mutation.record.expected_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
    mutation.record.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
    mutation.record.value = NULL;
    mutation.record.value_size = 0u;
  }
  mutation.event_type = plan->event_type;
  mutation.event_payload = (const uint8_t *)durable_event.data;
  mutation.event_payload_size = durable_event.len;
  memcpy(command.command_id, plan->command_id, sizeof(command.command_id));
  rc = flowie_cluster_owner_token_init(&command.owner, peer_command->shard_id,
                                       peer_command->owner_epoch, peer_command->target_node_id.data,
                                       peer_command->target_node_id.len,
                                       peer_command->target_boot_id);
  if (rc != TURBO_OK) goto done;
  command.mutations = &mutation;
  command.mutation_count = 1u;
  rc = bind->submit(bind->submit_ctx, &command, flowie_cluster_session_fact_complete, plan);

done:
  tstr_free(fact_value);
  tstr_free(event_payload);
  return rc;
}

static int flowie_cluster_session_bind_execute_connect(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_peer_frame_t *command,
    flowie_cluster_peer_owner_complete_fn complete, void *completion_ctx) {
  flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
  flowie_session_connect_result_t decision = FLOWIE_SESSION_CONNECT_RESULT_INIT;
  flowie_cluster_session_plan_t *plan = NULL;
  int rc;
  if (command->operation != FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND || !command->payload.data)
    return TURBO_EINVAL;
  rc = flowie_cluster_peer_connect_bind_decode(command->payload.data, command->payload.len,
                                               bind->max_bind_payload_size, &decoded);
  if (rc != TURBO_OK) goto fail;
  if (decoded.security_enabled != bind->security_enabled) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  plan = (flowie_cluster_session_plan_t *)calloc(1u, sizeof(*plan));
  if (!plan) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  plan->bind = bind;
  plan->principal = decoded.principal;
  plan->bind_payload = tstr_new_len(command->payload.data, command->payload.len);
  plan->edge_node_id = tstr_new_len(command->source_node_id.data, command->source_node_id.len);
  if (!plan->bind_payload || !plan->edge_node_id) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  plan->connection_id = command->connection_id;
  plan->connection_generation = command->connection_generation;
  memcpy(plan->edge_boot_id, command->source_boot_id, sizeof(plan->edge_boot_id));
  plan->update_binding = 1;
  plan->event_type = FLOWIE_CLUSTER_SESSION_EVENT_BOUND;
  plan->complete = complete;
  plan->completion_ctx = completion_ctx;
  memcpy(plan->command_id, command->correlation_id, sizeof(plan->command_id));
  rc = flowie_cluster_session_prepare_entry(bind, &decoded, plan);
  if (rc == TURBO_OK) rc = flowie_cluster_session_prepare_decision(bind, &decoded, plan, &decision);
  if (rc == TURBO_OK)
    plan->edge_action_sequence =
        !plan->new_entry && flowie_cluster_session_binding_same(plan->entry, plan)
            ? plan->entry->edge_action_sequence
            : 0u;
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_reply_encode(&decision, bind->max_bind_payload_size, &plan->reply);
  if (rc != TURBO_OK) goto fail;
  if (!decision.accepted) {
    rc = complete(completion_ctx, TURBO_OK, flowie_cluster_session_plan_finalize, plan);
    if (rc == TURBO_OK) return TURBO_OK;
    bind->self_fence(bind->self_fence_ctx, rc);
    goto fail;
  }
  rc = flowie_cluster_session_submit(plan, command);
  if (rc == TURBO_OK) return TURBO_OK;

fail:
  flowie_cluster_session_plan_destroy(plan);
  atomic_store_explicit(&bind->inflight, 0, memory_order_release);
  return rc;
}

static int flowie_cluster_session_action_encode(flowie_cluster_session_bind_t *bind,
                                                flowie_mqtt_version_t version,
                                                const flowie_mqtt_control_packet_t *control,
                                                int close_after_send,
                                                turbo_flow_protocol_settlement_point_t settlement,
                                                tstr_t *out) {
  flowie_mqtt_span_t packet = {NULL, 0u};
  uint8_t *encoded = NULL;
  size_t capacity;
  size_t written = 0u;
  int rc;
  if (!bind || !out || bind->max_bind_payload_size <= FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE)
    return TURBO_EINVAL;
  capacity = bind->max_bind_payload_size - FLOWIE_CLUSTER_PEER_MQTT_REPLY_HEADER_SIZE;
  if (control) {
    encoded = (uint8_t *)malloc(capacity);
    if (!encoded) return TURBO_ENOMEM;
    rc = flowie_mqtt_control_packet_encode(control, encoded, capacity, &written);
    if (rc != FLOWIE_MQTT_PARSE_OK) {
      free(encoded);
      return flowie_cluster_session_parse_status(rc);
    }
    packet = (flowie_mqtt_span_t){encoded, written};
  }
  rc = flowie_cluster_peer_mqtt_reply_encode(version, packet, close_after_send, settlement,
                                             capacity, out);
  free(encoded);
  return rc;
}

static int flowie_cluster_session_binding_require(const flowie_cluster_session_entry_t *entry,
                                                  const flowie_cluster_peer_frame_t *command,
                                                  flowie_mqtt_version_t mqtt_version,
                                                  int require_active,
                                                  flowie_session_snapshot_t *snapshot) {
  int rc;
  if (!entry || !command || !snapshot) return TURBO_EINVAL;
  rc = flowie_session_owner_snapshot(entry->owner, snapshot);
  if (rc != TURBO_OK) return rc;
  if ((require_active && !snapshot->active) || snapshot->version != mqtt_version ||
      !entry->edge_node_id || entry->connection_id != command->connection_id ||
      entry->connection_generation != command->connection_generation ||
      tstr_len(entry->edge_node_id) != command->source_node_id.len ||
      memcmp(entry->edge_node_id, command->source_node_id.data, command->source_node_id.len) != 0 ||
      memcmp(entry->edge_boot_id, command->source_boot_id, sizeof(entry->edge_boot_id)) != 0)
    return TURBO_EBUSY;
  return TURBO_OK;
}

static int flowie_cluster_session_plan_prepare(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_peer_frame_t *command,
    flowie_mqtt_span_t client_id, flowie_mqtt_version_t mqtt_version,
    flowie_cluster_peer_owner_complete_fn complete, void *completion_ctx,
    flowie_cluster_session_plan_t **out) {
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_cluster_session_entry_t *entry;
  flowie_cluster_session_plan_t *plan;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  entry = flowie_cluster_session_find(bind, client_id);
  if (!entry) return TURBO_ENOENT;
  rc = flowie_cluster_session_binding_require(entry, command, mqtt_version, 1, &snapshot);
  if (rc != TURBO_OK) return rc;
  plan = (flowie_cluster_session_plan_t *)calloc(1u, sizeof(*plan));
  if (!plan) return TURBO_ENOMEM;
  plan->bind = bind;
  plan->entry = entry;
  plan->expected_owner = entry->owner;
  plan->staged = flowie_session_owner_clone(entry->owner);
  plan->principal = entry->principal;
  plan->complete = complete;
  plan->completion_ctx = completion_ctx;
  plan->event_type = FLOWIE_CLUSTER_SESSION_EVENT_UPDATED;
  memcpy(plan->command_id, command->correlation_id, sizeof(plan->command_id));
  if (!plan->staged) {
    flowie_cluster_session_plan_destroy(plan);
    return TURBO_ENOMEM;
  }
  *out = plan;
  return TURBO_OK;
}

static int
flowie_cluster_session_prepare_subscribe(flowie_cluster_session_plan_t *plan,
                                         const flowie_cluster_peer_mqtt_command_view_t *decoded) {
  flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
  flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
  flowie_mqtt_subscription_view_t subscription;
  flowie_session_subscribe_result_t result = FLOWIE_SESSION_SUBSCRIBE_RESULT_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  tstr_t reasons = NULL;
  size_t index = 0u;
  int rc;
  rc = flowie_mqtt_subscribe_parse(&decoded->packet, &subscribe);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  reasons = tstr_new_len(NULL, subscribe.entry_count);
  if (!reasons) return TURBO_ENOMEM;
  rc = flowie_mqtt_subscription_iterator_init(&decoded->packet, &subscribe, &iterator);
  while (rc == FLOWIE_MQTT_PARSE_OK && (rc = flowie_mqtt_subscription_iterator_next(
                                            &iterator, &subscription)) == FLOWIE_MQTT_PARSE_OK) {
    if (index >= subscribe.entry_count) {
      rc = FLOWIE_MQTT_PARSE_MALFORMED;
      break;
    }
    reasons[index++] = (char)subscription.qos;
  }
  if (rc != FLOWIE_MQTT_PARSE_NEED_MORE || index != subscribe.entry_count) {
    tstr_free(reasons);
    return TURBO_EPROTO;
  }
  rc = flowie_session_owner_subscribe(plan->staged, &decoded->packet, &subscribe, &result);
  if (rc == TURBO_ENOSPC && decoded->mqtt_version != FLOWIE_MQTT_VERSION_3_1) {
    memset(reasons, decoded->mqtt_version == FLOWIE_MQTT_VERSION_5 ? 0x97 : 0x80,
           subscribe.entry_count);
    flowie_session_owner_destroy(plan->staged);
    plan->staged = NULL;
    rc = TURBO_OK;
  }
  if (rc == TURBO_OK && result.changed) {
    plan->publish = 1;
    plan->reindex = 1;
  }
  if (rc == TURBO_OK) {
    reply.version = decoded->mqtt_version;
    reply.type = FLOWIE_MQTT_PACKET_SUBACK;
    reply.packet_id = subscribe.packet_id;
    reply.reason_codes = (flowie_mqtt_span_t){(const uint8_t *)reasons, subscribe.entry_count};
    rc = flowie_cluster_session_action_encode(plan->bind, decoded->mqtt_version, &reply, 0,
                                              plan->publish
                                                  ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                                                  : (turbo_flow_protocol_settlement_point_t)0,
                                              &plan->reply);
  }
  tstr_free(reasons);
  return rc;
}

static int flowie_cluster_session_prepare_publish(
    flowie_cluster_session_plan_t *plan, const flowie_cluster_peer_frame_t *command,
    const flowie_cluster_peer_mqtt_command_view_t *decoded) {
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  flowie_session_publish_begin_result_t begin = FLOWIE_SESSION_PUBLISH_BEGIN_RESULT_INIT;
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  turbo_flow_protocol_settlement_request_t settlement =
      TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  turbo_flow_protocol_settlement_point_t requested;
  uint64_t accepted_at_epoch_seconds;
  int rc;
  if (!plan || !command || !decoded) return TURBO_EINVAL;
  accepted_at_epoch_seconds = plan->bind->now(plan->bind->now_ctx);
  if (accepted_at_epoch_seconds == 0u) return TURBO_EIO;
  rc = flowie_mqtt_publish_parse(&decoded->packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  rc = flowie_session_owner_publish_begin(plan->staged, &publish, &begin);
  if (rc != TURBO_OK) return rc;
  requested = publish.qos == 0u   ? plan->bind->session.settlement.qos0
              : publish.qos == 1u ? plan->bind->session.settlement.qos1
                                  : plan->bind->session.settlement.qos2;
  if (begin.has_ack) ack = begin.ack;
  if (begin.admit_graph && publish.qos != 0u) {
    plan->publish = 1;
    if (requested == TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED) {
      settlement.message = begin.message.metadata;
      settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED;
      settlement.status = TURBO_OK;
      rc = flowie_session_owner_route(plan->staged, &route);
      if (rc == TURBO_OK)
        rc = flowie_session_owner_publish_settle(plan->staged, &route, &settlement, &ack);
      if (rc != TURBO_OK) return rc;
    }
  }
  if (begin.admit_graph) {
    rc = flowie_session_owner_snapshot(plan->staged, &snapshot);
    if (rc == TURBO_OK)
      rc = flowie_cluster_publish_event_encode(
          decoded->mqtt_version, requested, command->connection_id,
          command->connection_generation, snapshot.session_id, snapshot.session_generation,
          accepted_at_epoch_seconds, command->source_node_id, command->source_boot_id,
          decoded->client_id,
          decoded->packet.packet, plan->bind->max_event_payload_size, &plan->outbox_payload);
    if (rc != TURBO_OK) return rc;
    plan->persist_event = 1;
    plan->event_type = FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH;
  }
  if (ack.kind != FLOWIE_SESSION_ACK_NONE) {
    rc = flowie_session_ack_control_packet(&ack, decoded->mqtt_version, &reply);
    if (rc != TURBO_OK) return rc;
    return flowie_cluster_session_action_encode(
        plan->bind, decoded->mqtt_version, &reply, 0,
        plan->persist_event ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                            : (turbo_flow_protocol_settlement_point_t)0,
        &plan->reply);
  }
  return flowie_cluster_session_action_encode(
      plan->bind, decoded->mqtt_version, NULL, 0,
      plan->persist_event ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                          : (turbo_flow_protocol_settlement_point_t)0,
      &plan->reply);
}

static int
flowie_cluster_session_prepare_unsubscribe(flowie_cluster_session_plan_t *plan,
                                           const flowie_cluster_peer_mqtt_command_view_t *decoded) {
  flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
  flowie_session_unsubscribe_result_t result = FLOWIE_SESSION_UNSUBSCRIBE_RESULT_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  tstr_t reasons = NULL;
  int rc = flowie_mqtt_unsubscribe_parse(&decoded->packet, &unsubscribe);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  if (decoded->mqtt_version == FLOWIE_MQTT_VERSION_5) {
    reasons = tstr_new_len(NULL, unsubscribe.filter_count);
    if (!reasons) return TURBO_ENOMEM;
  }
  rc = flowie_session_owner_unsubscribe(plan->staged, &decoded->packet, &unsubscribe,
                                        (uint8_t *)reasons, reasons ? unsubscribe.filter_count : 0u,
                                        &result);
  if (rc == TURBO_OK && result.changed) {
    plan->publish = 1;
    plan->reindex = 1;
  }
  if (rc == TURBO_OK) {
    reply.version = decoded->mqtt_version;
    reply.type = FLOWIE_MQTT_PACKET_UNSUBACK;
    reply.packet_id = unsubscribe.packet_id;
    reply.reason_codes = result.reason_codes;
    rc = flowie_cluster_session_action_encode(plan->bind, decoded->mqtt_version, &reply, 0,
                                              plan->publish
                                                  ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                                                  : (turbo_flow_protocol_settlement_point_t)0,
                                              &plan->reply);
  }
  tstr_free(reasons);
  return rc;
}

static int
flowie_cluster_session_prepare_ack(flowie_cluster_session_plan_t *plan,
                                   const flowie_cluster_peer_mqtt_command_view_t *decoded) {
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  int rc = flowie_session_owner_snapshot(plan->staged, &before);
  if (rc == TURBO_OK) rc = flowie_mqtt_control_packet_parse(&decoded->packet, &control);
  if (rc == FLOWIE_MQTT_PARSE_OK && decoded->packet.type == FLOWIE_MQTT_PACKET_PUBREL) {
    rc = flowie_session_owner_route(plan->staged, &route);
    if (rc == TURBO_OK)
      rc = flowie_session_owner_qos2_release(plan->staged, &route, control.packet_id, &ack);
    if (rc == TURBO_ENOENT) {
      ack.kind = FLOWIE_SESSION_ACK_PUBCOMP;
      ack.packet_id = control.packet_id;
      ack.reason_code = decoded->mqtt_version == FLOWIE_MQTT_VERSION_5 ? 0x92u : 0u;
      rc = TURBO_OK;
    }
  } else if (rc == FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_session_owner_delivery_ack(plan->staged, &decoded->packet, &ack);
    if (rc == TURBO_ENOENT) rc = TURBO_EPROTO;
  } else if (rc != TURBO_OK) {
    rc = TURBO_EPROTO;
  }
  if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(plan->staged, &after);
  if (rc == TURBO_OK && after.resource_generation != before.resource_generation) plan->publish = 1;
  if (rc == TURBO_OK && ack.kind != FLOWIE_SESSION_ACK_NONE) {
    rc = flowie_session_ack_control_packet(&ack, decoded->mqtt_version, &reply);
    if (rc == TURBO_OK)
      rc = flowie_cluster_session_action_encode(plan->bind, decoded->mqtt_version, &reply, 0,
                                                plan->publish
                                                    ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                                                    : (turbo_flow_protocol_settlement_point_t)0,
                                                &plan->reply);
  } else if (rc == TURBO_OK) {
    rc = flowie_cluster_session_action_encode(plan->bind, decoded->mqtt_version, NULL, 0,
                                              plan->publish
                                                  ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                                                  : (turbo_flow_protocol_settlement_point_t)0,
                                              &plan->reply);
  }
  return rc;
}

static int
flowie_cluster_session_prepare_disconnect(flowie_cluster_session_plan_t *plan,
                                          const flowie_cluster_peer_mqtt_command_view_t *decoded) {
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_mqtt_control_packet_view_t disconnect = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  int rc = flowie_session_owner_snapshot(plan->staged, &before);
  if (rc == TURBO_OK &&
      flowie_mqtt_control_packet_parse(&decoded->packet, &disconnect) != FLOWIE_MQTT_PARSE_OK)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_session_owner_disconnect(plan->staged, &disconnect);
  if (rc == TURBO_OK) rc = flowie_session_owner_close(plan->staged);
  if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(plan->staged, &after);
  if (rc == TURBO_OK && after.resource_generation != before.resource_generation) plan->publish = 1;
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_action_encode(plan->bind, decoded->mqtt_version, NULL, 1,
                                              plan->publish
                                                  ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                                                  : (turbo_flow_protocol_settlement_point_t)0,
                                              &plan->reply);
  return rc;
}

static int flowie_cluster_session_bind_execute_packet(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_peer_frame_t *command,
    flowie_cluster_peer_owner_complete_fn complete, void *completion_ctx) {
  flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
  flowie_cluster_session_plan_t *plan = NULL;
  int rc = flowie_cluster_peer_mqtt_command_decode(command->operation, command->payload.data,
                                                   command->payload.len,
                                                   bind->max_bind_payload_size, &decoded);
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_plan_prepare(bind, command, decoded.client_id,
                                             decoded.mqtt_version, complete, completion_ctx,
                                             &plan);
  if (rc == TURBO_OK) {
    switch (command->operation) {
    case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH:
      rc = flowie_cluster_session_prepare_publish(plan, command, &decoded);
      break;
    case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE:
      rc = flowie_cluster_session_prepare_subscribe(plan, &decoded);
      break;
    case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE:
      rc = flowie_cluster_session_prepare_unsubscribe(plan, &decoded);
      break;
    case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK:
      rc = flowie_cluster_session_prepare_ack(plan, &decoded);
      break;
    case FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT:
      rc = flowie_cluster_session_prepare_disconnect(plan, &decoded);
      break;
    default:
      rc = TURBO_ENOTSUP;
      break;
    }
  }
  if (rc == TURBO_OK && (plan->publish || plan->persist_event))
    rc = flowie_cluster_session_submit(plan, command);
  else if (rc == TURBO_OK)
    rc = complete(completion_ctx, TURBO_OK, flowie_cluster_session_plan_finalize, plan);
  if (rc == TURBO_OK) return TURBO_OK;
  flowie_cluster_session_plan_destroy(plan);
  atomic_store_explicit(&bind->inflight, 0, memory_order_release);
  return rc;
}

static int flowie_cluster_session_bind_execute_publish_settle(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_peer_frame_t *command,
    flowie_cluster_peer_owner_complete_fn complete, void *completion_ctx) {
  flowie_cluster_peer_publish_settle_view_t decoded =
      FLOWIE_CLUSTER_PEER_PUBLISH_SETTLE_VIEW_INIT;
  flowie_cluster_session_plan_t *plan = NULL;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  flowie_session_ack_intent_t ack = FLOWIE_SESSION_ACK_INTENT_INIT;
  flowie_mqtt_control_packet_t reply = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  flowie_mqtt_version_t mqtt_version;
  int rc = flowie_cluster_peer_publish_settle_decode(
      command->payload.data, command->payload.len, bind->max_bind_payload_size, &decoded);
  mqtt_version = (flowie_mqtt_version_t)decoded.settlement.message.protocol_version;
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_plan_prepare(bind, command, decoded.client_id, mqtt_version,
                                             complete, completion_ctx, &plan);
  if (rc == TURBO_OK) rc = flowie_session_owner_route(plan->staged, &route);
  if (rc == TURBO_OK && route.session_generation != decoded.settlement.message.session_generation)
    rc = TURBO_EBUSY;
  if (rc == TURBO_OK)
    rc = flowie_session_owner_publish_settle(plan->staged, &route, &decoded.settlement, &ack);
  if (rc == TURBO_OK) rc = flowie_session_ack_control_packet(&ack, mqtt_version, &reply);
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_action_encode(
        bind, mqtt_version, &reply, 0, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, &plan->reply);
  if (rc == TURBO_OK) {
    plan->publish = 1;
    rc = flowie_cluster_session_submit(plan, command);
  }
  if (rc == TURBO_OK) return TURBO_OK;
  flowie_cluster_session_plan_destroy(plan);
  atomic_store_explicit(&bind->inflight, 0, memory_order_release);
  return rc;
}

static int flowie_cluster_session_bind_execute_connection_lost(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_peer_frame_t *command,
    flowie_cluster_peer_owner_complete_fn complete, void *completion_ctx) {
  flowie_cluster_peer_connection_lost_view_t decoded =
      FLOWIE_CLUSTER_PEER_CONNECTION_LOST_VIEW_INIT;
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_cluster_session_entry_t *entry;
  flowie_cluster_session_plan_t *plan = NULL;
  int rc = flowie_cluster_peer_connection_lost_decode(command->payload.data, command->payload.len,
                                                      bind->max_bind_payload_size, &decoded);
  if (rc != TURBO_OK) goto fail;
  entry = flowie_cluster_session_find(bind, decoded.client_id);
  if (!entry) {
    rc = TURBO_ENOENT;
    goto fail;
  }
  rc = flowie_cluster_session_binding_require(entry, command, decoded.mqtt_version, 0, &before);
  if (rc != TURBO_OK) goto fail;
  plan = (flowie_cluster_session_plan_t *)calloc(1u, sizeof(*plan));
  if (!plan) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  plan->bind = bind;
  plan->entry = entry;
  plan->expected_owner = entry->owner;
  plan->principal = entry->principal;
  plan->complete = complete;
  plan->completion_ctx = completion_ctx;
  plan->event_type = FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST;
  memcpy(plan->command_id, command->correlation_id, sizeof(plan->command_id));
  if (before.active) {
    plan->staged = flowie_session_owner_clone(entry->owner);
    if (!plan->staged) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    rc = flowie_session_owner_close(plan->staged);
    if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(plan->staged, &after);
    if (rc == TURBO_OK && after.resource_generation != before.resource_generation)
      plan->publish = 1;
  }
  if (rc == TURBO_OK)
    rc = flowie_cluster_session_action_encode(bind, decoded.mqtt_version, NULL, 0,
                                              plan->publish
                                                  ? TURBO_FLOW_PROTOCOL_SETTLE_DURABLE
                                                  : (turbo_flow_protocol_settlement_point_t)0,
                                              &plan->reply);
  if (rc == TURBO_OK && plan->publish) rc = flowie_cluster_session_submit(plan, command);
  else if (rc == TURBO_OK)
    rc = complete(completion_ctx, TURBO_OK, flowie_cluster_session_plan_finalize, plan);
  if (rc == TURBO_OK) return TURBO_OK;

fail:
  flowie_cluster_session_plan_destroy(plan);
  atomic_store_explicit(&bind->inflight, 0, memory_order_release);
  return rc;
}

int flowie_cluster_session_bind_execute_async(void *user_data,
                                              const flowie_cluster_peer_frame_t *command,
                                              flowie_cluster_peer_owner_complete_fn complete,
                                              void *completion_ctx) {
  flowie_cluster_session_bind_t *bind = (flowie_cluster_session_bind_t *)user_data;
  int rc;
  if (!bind || !command || !complete || !completion_ctx ||
      command->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND || !command->payload.data ||
      !command->source_node_id.data || command->source_node_id.len == 0u ||
      command->source_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || !command->target_node_id.data ||
      command->target_node_id.len == 0u ||
      command->target_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX ||
      !flowie_cluster_session_nonzero(command->source_boot_id, sizeof(command->source_boot_id)) ||
      !flowie_cluster_session_nonzero(command->target_boot_id, sizeof(command->target_boot_id)) ||
      !flowie_cluster_session_nonzero(command->correlation_id, sizeof(command->correlation_id)))
    return TURBO_EINVAL;
  if (atomic_exchange_explicit(&bind->inflight, 1, memory_order_acq_rel) != 0) return TURBO_EBUSY;
  if (command->operation == FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND)
    return flowie_cluster_session_bind_execute_connect(bind, command, complete, completion_ctx);
  if (command->operation == FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST)
    return flowie_cluster_session_bind_execute_connection_lost(bind, command, complete,
                                                               completion_ctx);
  if (command->operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE)
    return flowie_cluster_session_bind_execute_publish_settle(bind, command, complete,
                                                              completion_ctx);
  rc = flowie_cluster_session_bind_execute_packet(bind, command, complete, completion_ctx);
  return rc;
}

int flowie_cluster_session_bind_snapshot(const flowie_cluster_session_bind_t *bind,
                                         flowie_mqtt_span_t client_id,
                                         flowie_session_snapshot_t *snapshot,
                                         turbo_flow_security_principal_t *principal) {
  flowie_cluster_session_entry_t *entry;
  int rc;
  if (!bind || !snapshot) return TURBO_EINVAL;
  entry = flowie_cluster_session_find(bind, client_id);
  if (!entry) return TURBO_ENOENT;
  rc = flowie_session_owner_snapshot(entry->owner, snapshot);
  if (rc == TURBO_OK && principal) *principal = entry->principal;
  return rc;
}

static void flowie_cluster_publish_targets_destroy(turbo_vec_t *targets) {
  if (!targets) return;
  for (size_t slot = 0u; slot < turbo_vec_size(targets); ++slot) {
    flowie_cluster_publish_target_owned_t *target =
        (flowie_cluster_publish_target_owned_t *)turbo_vec_at(targets, slot);
    if (target) turbo_vec_destroy(&target->subscription_identifiers);
  }
  turbo_vec_destroy(targets);
}

static int flowie_cluster_publish_target_identifier_add(
    flowie_cluster_publish_target_owned_t *target, uint32_t identifier) {
  if (!target) return TURBO_EINVAL;
  if (identifier == 0u) return TURBO_OK;
  for (size_t slot = 0u; slot < turbo_vec_size(&target->subscription_identifiers); ++slot) {
    const uint32_t *existing =
        (const uint32_t *)turbo_vec_at_const(&target->subscription_identifiers, slot);
    if (existing && *existing == identifier) return TURBO_OK;
  }
  return turbo_vec_push(&target->subscription_identifiers, &identifier);
}

static int flowie_cluster_publish_target_add(
    turbo_vec_t *targets, turbo_hash_map_t *ordinary_index,
    const flowie_cluster_subscription_member_t *member, tstr_v shared_filter, int merge) {
  flowie_cluster_publish_target_owned_t *existing = NULL;
  flowie_cluster_publish_target_owned_t created;
  const size_t *found;
  size_t slot;
  int rc;
  if (!targets || !member || !member->entry || (merge && !ordinary_index)) return TURBO_EINVAL;
  if (merge) {
    found = (const size_t *)turbo_hash_map_get_const(ordinary_index, &member->session_id);
    if (found) {
      existing = (flowie_cluster_publish_target_owned_t *)turbo_vec_at(targets, *found);
      if (!existing || existing->entry != member->entry) return TURBO_EPROTO;
    }
  }
  if (existing) {
    if (member->qos > existing->qos) existing->qos = member->qos;
    if (member->retain_as_published) existing->retain_as_published = 1u;
    return flowie_cluster_publish_target_identifier_add(existing,
                                                        member->subscription_identifier);
  }
  memset(&created, 0, sizeof(created));
  created.snapshot = (flowie_session_snapshot_t)FLOWIE_SESSION_SNAPSHOT_INIT;
  created.entry = member->entry;
  created.qos = member->qos;
  created.retain_as_published = member->retain_as_published;
  created.shared_filter = shared_filter;
  rc = flowie_session_owner_snapshot(member->entry->owner, &created.snapshot);
  if (rc != TURBO_OK || created.snapshot.session_id != member->session_id)
    return rc == TURBO_OK ? TURBO_EPROTO : rc;
  rc = turbo_vec_init(&created.subscription_identifiers, sizeof(uint32_t));
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_publish_target_identifier_add(&created, member->subscription_identifier);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&created.subscription_identifiers);
    return rc;
  }
  rc = turbo_vec_push(targets, &created);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&created.subscription_identifiers);
    return rc;
  }
  if (!merge) return TURBO_OK;
  slot = turbo_vec_size(targets) - 1u;
  rc = turbo_hash_map_put(ordinary_index, &member->session_id, &slot);
  if (rc == TURBO_OK) return TURBO_OK;
  {
    flowie_cluster_publish_target_owned_t removed;
    memset(&removed, 0, sizeof(removed));
    if (turbo_vec_swap_remove(targets, slot, &removed) == TURBO_OK)
      turbo_vec_destroy(&removed.subscription_identifiers);
  }
  return rc;
}

static int flowie_cluster_publish_select_group(
    flowie_cluster_subscription_group_t *group, uint64_t publisher_session_id,
    turbo_vec_t *targets, turbo_hash_map_t *ordinary_index) {
  turbo_flow_pattern_selection_iterator_t selection = TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  const flowie_cluster_subscription_member_t *selected = NULL;
  size_t member_slot;
  int rc;
  if (!group || !targets || !ordinary_index) return TURBO_EINVAL;
  rc = turbo_flow_pattern_selection_begin(&group->selector, TURBO_FLOW_PATTERN_SELECT_FAN_OUT,
                                          turbo_vec_size(&group->members), &selection);
  if (rc == TURBO_ENOENT) return TURBO_OK;
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_pattern_selection_next(&selection, &member_slot)) == TURBO_OK) {
    const flowie_cluster_subscription_member_t *member =
        (const flowie_cluster_subscription_member_t *)turbo_vec_at_const(&group->members,
                                                                         member_slot);
    if (!member || !member->entry || !member->entry->owner) return TURBO_EPROTO;
    if (member->no_local && member->session_id == publisher_session_id) continue;
    if (group->shared) {
      if (!selected || member->session_id < selected->session_id) selected = member;
      continue;
    }
    rc = flowie_cluster_publish_target_add(targets, ordinary_index, member, (tstr_v){NULL, 0u}, 1);
    if (rc != TURBO_OK) return rc;
  }
  if (rc != TURBO_ENOENT) return rc;
  return selected ? flowie_cluster_publish_target_add(targets, ordinary_index, selected,
                                                      tstr_to_v(group->filter), 0)
                  : TURBO_OK;
}

int flowie_cluster_session_bind_match_publish(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_publish_event_view_t *event,
    flowie_cluster_session_publish_target_visit_fn visit, void *visit_ctx, size_t *target_count) {
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  turbo_vec_t matched_groups;
  turbo_vec_t targets;
  turbo_hash_map_t ordinary_index;
  size_t visited = 0u;
  int matched_initialized = 0;
  int targets_initialized = 0;
  int ordinary_initialized = 0;
  int rc;
  if (target_count) *target_count = 0u;
  if (!bind || !event || event->size != sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION || !visit || !target_count ||
      event->publish.packet.type != FLOWIE_MQTT_PACKET_PUBLISH)
    return TURBO_EINVAL;
  if (atomic_load_explicit(&bind->inflight, memory_order_acquire)) return TURBO_EBUSY;
  rc = flowie_mqtt_publish_parse(&event->publish.packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  rc = turbo_vec_init(&matched_groups, sizeof(size_t));
  if (rc != TURBO_OK) return rc;
  matched_initialized = 1;
  rc = turbo_vec_init(&targets, sizeof(flowie_cluster_publish_target_owned_t));
  if (rc != TURBO_OK) goto done;
  targets_initialized = 1;
  rc = turbo_hash_map_init(&ordinary_index, sizeof(uint64_t), sizeof(size_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) goto done;
  ordinary_initialized = 1;
  rc = turbo_hash_map_reserve(&ordinary_index, bind->max_sessions);
  if (rc != TURBO_OK) goto done;
  rc = flowie_topic_index_match(&bind->subscription_index.topics, publish.topic, &matched_groups);
  if (rc != TURBO_OK) goto done;
  for (size_t match_slot = 0u; match_slot < turbo_vec_size(&matched_groups); ++match_slot) {
    const size_t *group_slot = (const size_t *)turbo_vec_at_const(&matched_groups, match_slot);
    flowie_cluster_subscription_group_t *group =
        group_slot ? (flowie_cluster_subscription_group_t *)turbo_vec_at(
                         &bind->subscription_index.groups, *group_slot)
                   : NULL;
    int topic_matches = 0;
    flowie_mqtt_span_t filter;
    if (!group || !group->filter) {
      rc = TURBO_EPROTO;
      goto done;
    }
    filter = (flowie_mqtt_span_t){(const uint8_t *)group->filter, tstr_len(group->filter)};
    rc = flowie_mqtt_topic_matches(filter, publish.topic, &topic_matches);
    if (rc != FLOWIE_MQTT_PARSE_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (!topic_matches) continue;
    rc = flowie_cluster_publish_select_group(group, event->session_id, &targets, &ordinary_index);
    if (rc != TURBO_OK) goto done;
  }
  for (size_t target_slot = 0u; target_slot < turbo_vec_size(&targets); ++target_slot) {
    const flowie_cluster_publish_target_owned_t *target =
        (const flowie_cluster_publish_target_owned_t *)turbo_vec_at_const(&targets, target_slot);
    flowie_cluster_session_publish_target_t view = FLOWIE_CLUSTER_SESSION_PUBLISH_TARGET_INIT;
    size_t identifier_count;
    if (!target || !target->entry || !target->entry->client_id) {
      rc = TURBO_EPROTO;
      goto done;
    }
    identifier_count = turbo_vec_size(&target->subscription_identifiers);
    view.client_id = (flowie_mqtt_span_t){(const uint8_t *)target->entry->client_id,
                                          tstr_len(target->entry->client_id)};
    view.mqtt_version = target->snapshot.version;
    view.session_id = target->snapshot.session_id;
    view.session_generation = target->snapshot.session_generation;
    view.resource_generation = target->snapshot.resource_generation;
    view.session_expiry_interval = target->snapshot.session_expiry_interval;
    view.active = target->snapshot.active;
    view.qos = publish.qos < target->qos ? publish.qos : target->qos;
    view.retain_as_published = target->retain_as_published;
    view.edge_node_id = target->snapshot.active && target->entry->edge_node_id
                            ? tstr_to_v(target->entry->edge_node_id)
                            : (tstr_v){NULL, 0u};
    view.edge_boot_id = target->snapshot.active ? target->entry->edge_boot_id : NULL;
    view.connection_id = target->snapshot.active ? target->entry->connection_id : 0u;
    view.connection_generation =
        target->snapshot.active ? target->entry->connection_generation : 0u;
    view.subscription_identifier_count = identifier_count;
    view.subscription_identifiers =
        identifier_count != 0u
            ? (const uint32_t *)turbo_vec_at_const(&target->subscription_identifiers, 0u)
            : NULL;
    view.shared_filter = (flowie_mqtt_span_t){(const uint8_t *)target->shared_filter.data,
                                              target->shared_filter.len};
    rc = visit(visit_ctx, &view);
    if (rc != TURBO_OK) goto done;
    visited += 1u;
  }
  rc = TURBO_OK;

done:
  *target_count = visited;
  if (ordinary_initialized) turbo_hash_map_destroy(&ordinary_index);
  if (targets_initialized) flowie_cluster_publish_targets_destroy(&targets);
  if (matched_initialized) turbo_vec_destroy(&matched_groups);
  return rc;
}

int flowie_cluster_session_bind_reply_decode(const void *data, size_t data_size,
                                             size_t max_payload_size,
                                             flowie_cluster_session_bind_reply_view_t *out) {
  const uint8_t *bytes = (const uint8_t *)data;
  flowie_cluster_session_bind_reply_view_t decoded = FLOWIE_CLUSTER_SESSION_BIND_REPLY_VIEW_INIT;
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  size_t consumed = 0u;
  uint32_t flags;
  int rc;
  if (!bytes || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION ||
      max_payload_size <= FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE ||
      max_payload_size > UINT32_MAX)
    return TURBO_EINVAL;
  if (data_size > max_payload_size) return TURBO_EMSGSIZE;
  if (data_size <= FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE ||
      memcmp(bytes, FLOWIE_CLUSTER_SESSION_BIND_REPLY_MAGIC,
             sizeof(FLOWIE_CLUSTER_SESSION_BIND_REPLY_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + 4u) != FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) !=
          FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != data_size)
    return TURBO_EPROTO;
  flags = flowie_cluster_peer_wire_read_u32(bytes + 12u);
  if ((flags & ~UINT32_C(0x0000ff07)) != 0u ||
      !flowie_mqtt_version_is_supported((flowie_mqtt_version_t)((flags >> 8u) & 0xffu)))
    return TURBO_EPROTO;
  options.version = (flowie_mqtt_version_t)((flags >> 8u) & 0xffu);
  options.max_packet_size = max_payload_size - FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE;
  rc = flowie_mqtt_packet_parse(bytes + FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE,
                                data_size - FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE, &options,
                                &packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK ||
      consumed != data_size - FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE ||
      packet.type != FLOWIE_MQTT_PACKET_CONNACK)
    return TURBO_EPROTO;
  rc = flowie_mqtt_control_packet_parse(&packet, &control);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  decoded.accepted = (uint8_t)(flags & 1u);
  decoded.close_after_reply = (uint8_t)((flags >> 1u) & 1u);
  decoded.session_present = (uint8_t)((flags >> 2u) & 1u);
  decoded.route.protocol = TURBO_FLOW_PROTOCOL_MQTT;
  decoded.route.owner_instance_id = flowie_cluster_peer_wire_read_u64(bytes + 16u);
  decoded.route.session_id = flowie_cluster_peer_wire_read_u64(bytes + 24u);
  decoded.route.session_generation = flowie_cluster_peer_wire_read_u64(bytes + 32u);
  if (control.session_present != decoded.session_present ||
      (decoded.accepted &&
       (control.reason_code != 0u || decoded.route.owner_instance_id == 0u ||
        decoded.route.session_id == 0u || decoded.route.session_generation == 0u)) ||
      (!decoded.accepted &&
       (control.reason_code == 0u || decoded.session_present ||
        decoded.route.owner_instance_id != 0u || decoded.route.session_id != 0u ||
        decoded.route.session_generation != 0u)))
    return TURBO_EPROTO;
  decoded.packet = (flowie_mqtt_span_t){bytes + FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE,
                                        data_size - FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE};
  *out = decoded;
  return TURBO_OK;
}

static void flowie_cluster_session_delivery_plan_release(
    flowie_cluster_session_delivery_plan_t *plan) {
  if (!plan) return;
  flowie_session_owner_destroy(plan->staged);
  tstr_freep(&plan->packet);
  tstr_freep(&plan->fact_value);
  tstr_freep(&plan->action);
  if (plan->bind) atomic_store_explicit(&plan->bind->inflight, 0, memory_order_release);
  free(plan);
}

static int flowie_cluster_session_delivery_target_require(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_session_publish_target_t *target,
    flowie_cluster_session_entry_t **out, flowie_session_snapshot_t *snapshot) {
  flowie_cluster_session_entry_t *entry;
  int rc;
  if (!bind || !target || target->size != sizeof(*target) ||
      target->abi_version != FLOWIE_CLUSTER_SESSION_BIND_ABI_V1 || !target->client_id.data ||
      target->client_id.size == 0u || target->session_id == 0u ||
      target->session_generation == 0u || target->resource_generation == 0u || target->qos > 2u ||
      target->retain_as_published > 1u ||
      (target->shared_filter.size != 0u &&
       (!target->shared_filter.data || target->shared_filter.size > FLOWIE_CLUSTER_KEY_MAX ||
        !flowie_cluster_subscription_is_shared(target->shared_filter))) ||
      !out || !snapshot)
    return TURBO_EINVAL;
  entry = flowie_cluster_session_find(bind, target->client_id);
  if (!entry || !entry->owner) return TURBO_EBUSY;
  rc = flowie_session_owner_snapshot(entry->owner, snapshot);
  if (rc != TURBO_OK) return rc;
  if (snapshot->session_id != target->session_id ||
      snapshot->session_generation != target->session_generation ||
      snapshot->resource_generation != target->resource_generation ||
      snapshot->version != target->mqtt_version || snapshot->active != target->active)
    return TURBO_EBUSY;
  if (target->active &&
      (!entry->edge_node_id || !target->edge_node_id.data || target->edge_node_id.len == 0u ||
       !target->edge_boot_id || target->connection_id == 0u ||
       target->connection_generation == 0u || entry->connection_id != target->connection_id ||
       entry->connection_generation != target->connection_generation ||
       tstr_len(entry->edge_node_id) != target->edge_node_id.len ||
       memcmp(entry->edge_node_id, target->edge_node_id.data, target->edge_node_id.len) != 0 ||
       memcmp(entry->edge_boot_id, target->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0))
    return TURBO_EBUSY;
  *out = entry;
  return TURBO_OK;
}

static int flowie_cluster_session_delivery_plan_terminal(
    flowie_cluster_session_delivery_plan_t *plan) {
  plan->command.mutations = NULL;
  plan->command.mutation_count = 0u;
  return TURBO_OK;
}

int flowie_cluster_session_delivery_plan_create(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_broadcast_event_view_t *source,
    const uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE],
    const flowie_cluster_session_publish_target_t *target,
    flowie_cluster_session_delivery_plan_t **out) {
  flowie_cluster_session_delivery_plan_t *plan = NULL;
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  uint16_t packet_id = 0u;
  uint64_t expiry_at = 0u;
  uint64_t now;
  flowie_cluster_session_edge_binding_t edge_binding;
  int expired = 0;
  int reserved = 0;
  int rc;
  if (out) *out = NULL;
  if (!bind || !current_owner ||
      flowie_cluster_owner_token_require(current_owner, current_owner) != TURBO_OK || !source ||
      source->size != sizeof(*source) ||
      source->abi_version != FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION ||
      !flowie_cluster_session_nonzero(source->command_id, sizeof(source->command_id)) ||
      source->source_owner_epoch == 0u || !event_digest || !target || !out)
    return TURBO_EINVAL;
  if (atomic_exchange_explicit(&bind->inflight, 1, memory_order_acq_rel)) return TURBO_EBUSY;
  plan = (flowie_cluster_session_delivery_plan_t *)calloc(1u, sizeof(*plan));
  if (!plan) {
    atomic_store_explicit(&bind->inflight, 0, memory_order_release);
    return TURBO_ENOMEM;
  }
  plan->bind = bind;
  plan->command = (flowie_cluster_pgsql_fact_command_t)FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  plan->mutation =
      (flowie_cluster_pgsql_fact_mutation_t)FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
  plan->dedupe = (flowie_cluster_pgsql_event_dedupe_t)FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
  rc = flowie_cluster_session_delivery_target_require(bind, target, &plan->entry, &before);
  if (rc != TURBO_OK) goto fail;
  plan->expected_owner = plan->entry->owner;
  memcpy(plan->dedupe.source_command_id, source->command_id,
         sizeof(plan->dedupe.source_command_id));
  plan->dedupe.event_index = source->event_index;
  plan->dedupe.source_shard_id = source->source_shard_id;
  plan->dedupe.source_owner_epoch = source->source_owner_epoch;
  plan->dedupe.source_fact_revision = source->fact_revision;
  plan->dedupe.target_session_id = target->session_id;
  memcpy(plan->dedupe.event_digest, event_digest, sizeof(plan->dedupe.event_digest));
  plan->dedupe.shared_filter = target->shared_filter.data;
  plan->dedupe.shared_filter_size = target->shared_filter.size;
  rc = flowie_cluster_broadcast_target_command_id(event_digest, current_owner->shard_id,
                                                  target->session_id,
                                                  plan->command.command_id);
  if (rc != TURBO_OK) goto fail;
  plan->command.owner = *current_owner;
  plan->command.dedupe = &plan->dedupe;
  if (!target->active && (target->qos == 0u || target->session_expiry_interval == 0u)) {
    rc = flowie_cluster_session_delivery_plan_terminal(plan);
    goto done;
  }
  now = bind->now(bind->now_ctx);
  if (now == 0u) {
    rc = TURBO_EIO;
    goto fail;
  }
  if (target->qos != 0u || target->active) {
    plan->staged = flowie_session_owner_clone(plan->entry->owner);
    if (!plan->staged) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
  }
  if (target->qos != 0u) {
    rc = flowie_session_owner_delivery_reserve(plan->staged, target->qos, &packet_id);
    if (rc != TURBO_OK) goto fail;
    reserved = 1;
  }
  rc = flowie_cluster_delivery_packet_encode(
      &source->publish, target->mqtt_version, target->qos, target->retain_as_published, packet_id,
      target->subscription_identifiers, target->subscription_identifier_count, now,
      bind->max_event_payload_size, &plan->packet, &expiry_at, &expired);
  if (rc != TURBO_OK) goto fail;
  if (expired) {
    flowie_session_owner_destroy(plan->staged);
    plan->staged = NULL;
    rc = flowie_cluster_session_delivery_plan_terminal(plan);
    goto done;
  }
  if (target->qos != 0u) {
    rc = target->active
             ? flowie_session_owner_delivery_commit(
                   plan->staged, packet_id,
                   (flowie_mqtt_span_t){(const uint8_t *)plan->packet, tstr_len(plan->packet)},
                   expiry_at)
             : flowie_session_owner_delivery_commit_queued(
                   plan->staged, packet_id,
                   (flowie_mqtt_span_t){(const uint8_t *)plan->packet, tstr_len(plan->packet)},
                   expiry_at);
    if (rc != TURBO_OK) goto fail;
    reserved = 0;
  } else if (target->active) {
    rc = flowie_session_owner_touch(plan->staged);
    if (rc != TURBO_OK) goto fail;
  }
  if (target->active) {
    if (plan->entry->edge_action_sequence == UINT64_MAX) {
      rc = TURBO_ERANGE;
      goto fail;
    }
    plan->next_action_sequence = plan->entry->edge_action_sequence + 1u;
    rc = flowie_cluster_delivery_action_encode(
        target->edge_node_id, target->edge_boot_id, target->connection_id,
        target->connection_generation, target->session_id, target->session_generation,
        plan->next_action_sequence, target->mqtt_version,
        (flowie_mqtt_span_t){(const uint8_t *)plan->packet, tstr_len(plan->packet)},
        bind->max_event_payload_size, &plan->action);
    if (rc != TURBO_OK) goto fail;
    plan->publish_action_sequence = 1;
  }
  if (plan->staged) {
    rc = flowie_session_owner_snapshot(plan->staged, &after);
    if (rc != TURBO_OK || after.resource_generation <= before.resource_generation) {
      rc = rc == TURBO_OK ? TURBO_EPROTO : rc;
      goto fail;
    }
    memset(&edge_binding, 0, sizeof(edge_binding));
    edge_binding.edge_node_id =
        plan->entry->edge_node_id ? tstr_to_v(plan->entry->edge_node_id) : (tstr_v){NULL, 0u};
    edge_binding.edge_boot_id =
        plan->entry->edge_node_id ? plan->entry->edge_boot_id : NULL;
    edge_binding.connection_id = plan->entry->connection_id;
    edge_binding.connection_generation = plan->entry->connection_generation;
    edge_binding.action_sequence = target->active ? plan->next_action_sequence
                                                  : plan->entry->edge_action_sequence;
    rc = flowie_cluster_session_fact_encode(bind, tstr_to_v(plan->entry->bind_payload),
                                            plan->staged, &edge_binding, &plan->fact_value);
    if (rc != TURBO_OK) goto fail;
    plan->publish_owner = 1;
  }
  plan->mutation.shard_key = (const uint8_t *)plan->entry->client_id;
  plan->mutation.shard_key_size = tstr_len(plan->entry->client_id);
  plan->mutation.record.kind = TURBO_FLOW_RECORD_PUT;
  plan->mutation.record.key = (const uint8_t *)plan->entry->client_id;
  plan->mutation.record.key_size = tstr_len(plan->entry->client_id);
  plan->mutation.write_kind = target->active ? FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT
                                             : FLOWIE_CLUSTER_PGSQL_FACT_ONLY;
  plan->mutation.record.expected_revision = before.resource_generation;
  plan->mutation.record.next_revision = after.resource_generation;
  plan->mutation.record.value = (const uint8_t *)plan->fact_value;
  plan->mutation.record.value_size = tstr_len(plan->fact_value);
  if (target->active) {
    plan->mutation.event_type = FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE;
    plan->mutation.event_payload = (const uint8_t *)plan->action;
    plan->mutation.event_payload_size = tstr_len(plan->action);
  }
  plan->command.mutations = &plan->mutation;
  plan->command.mutation_count = 1u;
done:
  *out = plan;
  return TURBO_OK;

fail:
  if (reserved && plan->staged) (void)flowie_session_owner_delivery_cancel(plan->staged, packet_id);
  flowie_cluster_session_delivery_plan_release(plan);
  return rc;
}

const flowie_cluster_pgsql_fact_command_t *flowie_cluster_session_delivery_plan_command(
    const flowie_cluster_session_delivery_plan_t *plan) {
  return plan ? &plan->command : NULL;
}

int flowie_cluster_session_delivery_plan_finalize(flowie_cluster_session_delivery_plan_t **plan,
                                                  int durable_status) {
  flowie_cluster_session_delivery_plan_t *current;
  flowie_session_owner_t *previous = NULL;
  int rc;
  if (!plan || !(current = *plan)) return TURBO_EINVAL;
  *plan = NULL;
  if (durable_status == TURBO_EALREADY) {
    flowie_cluster_session_delivery_plan_release(current);
    return TURBO_OK;
  }
  rc = durable_status;
  if (rc == TURBO_OK &&
      ((!current->entry || current->entry->owner != current->expected_owner) ||
       (current->publish_action_sequence &&
        current->entry->edge_action_sequence + 1u != current->next_action_sequence))) {
    rc = TURBO_EBUSY;
    current->bind->self_fence(current->bind->self_fence_ctx, rc);
  }
  if (rc == TURBO_OK && current->publish_owner) {
    previous = current->entry->owner;
    current->entry->owner = current->staged;
    current->staged = NULL;
  }
  if (rc == TURBO_OK && current->publish_action_sequence)
    current->entry->edge_action_sequence = current->next_action_sequence;
  flowie_session_owner_destroy(previous);
  flowie_cluster_session_delivery_plan_release(current);
  return rc;
}

void flowie_cluster_session_delivery_plan_destroy(flowie_cluster_session_delivery_plan_t **plan) {
  flowie_cluster_session_delivery_plan_t *current;
  if (!plan || !(current = *plan)) return;
  *plan = NULL;
  flowie_cluster_session_delivery_plan_release(current);
}

static void flowie_cluster_session_lifecycle_plan_release(
    flowie_cluster_session_lifecycle_plan_t *plan) {
  if (!plan) return;
  if (plan->staged_index_initialized)
    flowie_cluster_subscription_index_destroy(&plan->staged_index);
  flowie_session_owner_destroy(plan->staged);
  tstr_freep(&plan->packet);
  tstr_freep(&plan->fact_value);
  tstr_freep(&plan->event_payload);
  if (plan->bind) atomic_store_explicit(&plan->bind->inflight, 0, memory_order_release);
  free(plan);
}

static int flowie_cluster_session_lifecycle_event_validate(
    const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_lifecycle_event_view_t *event, uint64_t now_epoch_seconds) {
  if (!current_owner || current_owner->size != sizeof(*current_owner) ||
      current_owner->abi_version != FLOWIE_CLUSTER_INTERNAL_ABI_V1 ||
      current_owner->owner_epoch == 0u || current_owner->node_id_size == 0u ||
      current_owner->node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      current_owner->node_id[current_owner->node_id_size] != '\0' ||
      !flowie_cluster_session_nonzero(current_owner->boot_id, sizeof(current_owner->boot_id)) ||
      !event || event->size != sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1 ||
      event->shard_id != current_owner->shard_id ||
      !flowie_cluster_session_nonzero(event->command_id, sizeof(event->command_id)) ||
      event->event_owner_epoch == 0u || event->expected_fact_revision == 0u ||
      event->created_at_epoch_seconds == 0u || event->connection_id == 0u ||
      event->connection_generation == 0u || event->session_id == 0u ||
      event->session_generation == 0u || !event->client_id.data || event->client_id.size == 0u ||
      !event->edge_node_id.data || event->edge_node_id.size == 0u ||
      event->edge_node_id.size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      memchr(event->edge_node_id.data, '\0', event->edge_node_id.size) ||
      !flowie_cluster_session_nonzero(event->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      now_epoch_seconds == 0u)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_session_lifecycle_command_id(
    const flowie_cluster_lifecycle_event_view_t *event,
    flowie_cluster_lifecycle_action_t action, uint64_t resource_generation,
    uint64_t session_id, uint8_t out[FLOWIE_CLUSTER_COMMAND_ID_SIZE]) {
  uint8_t identity[17];
  crypto_blake2b_ctx ctx;
  if (out) memset(out, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE);
  if (!event || (action != FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL &&
                 action != FLOWIE_CLUSTER_LIFECYCLE_ACTION_EXPIRE_SESSION) ||
      resource_generation == 0u || session_id == 0u || !out)
    return TURBO_EINVAL;
  identity[0] = (uint8_t)action;
  flowie_cluster_peer_wire_write_u64(identity + 1u, resource_generation);
  flowie_cluster_peer_wire_write_u64(identity + 9u, session_id);
  crypto_blake2b_init(&ctx, FLOWIE_CLUSTER_COMMAND_ID_SIZE);
  crypto_blake2b_update(&ctx, FLOWIE_CLUSTER_SESSION_LIFECYCLE_COMMAND_DOMAIN,
                        sizeof(FLOWIE_CLUSTER_SESSION_LIFECYCLE_COMMAND_DOMAIN) - 1u);
  crypto_blake2b_update(&ctx, event->command_id, sizeof(event->command_id));
  crypto_blake2b_update(&ctx, identity, sizeof(identity));
  crypto_blake2b_final(&ctx, out);
  crypto_wipe(&ctx, sizeof(ctx));
  crypto_wipe(identity, sizeof(identity));
  return TURBO_OK;
}

static int flowie_cluster_session_will_properties(
    const flowie_session_snapshot_t *snapshot, tstr_t *out) {
  flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  tstr_t filtered;
  size_t written = 0u;
  int rc;
  if (out) *out = NULL;
  if (!snapshot || !out) return TURBO_EINVAL;
  filtered = tstr_new_len(NULL, snapshot->will_properties.size);
  if (!filtered) return TURBO_ENOMEM;
  block.values = snapshot->will_properties;
  rc = flowie_mqtt_property_iterator_init(&block, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    tstr_free(filtered);
    return TURBO_EPROTO;
  }
  for (;;) {
    const uint8_t *begin = iterator.cursor;
    rc = flowie_mqtt_property_iterator_next(&iterator, &property);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK || !begin || iterator.cursor < begin) {
      tstr_free(filtered);
      return TURBO_EPROTO;
    }
    if (property.identifier == FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL) continue;
    memcpy(filtered + written, begin, (size_t)(iterator.cursor - begin));
    written += (size_t)(iterator.cursor - begin);
  }
  if (!tstr_set_len_checked(filtered, written)) {
    tstr_free(filtered);
    return TURBO_ERANGE;
  }
  *out = filtered;
  return TURBO_OK;
}

static int flowie_cluster_session_will_packet_encode(
    const flowie_session_snapshot_t *snapshot, size_t maximum, tstr_t *out) {
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  tstr_t properties = NULL;
  tstr_t packet = NULL;
  size_t capacity = 16u;
  size_t written = 0u;
  int rc;
  if (out) *out = NULL;
  if (!snapshot || !out || maximum == 0u || snapshot->active || !snapshot->has_will ||
      !snapshot->will_pending)
    return TURBO_EINVAL;
  rc = flowie_cluster_session_will_properties(snapshot, &properties);
  if (rc != TURBO_OK) return rc;
  if (snapshot->will_topic.size > SIZE_MAX - capacity) rc = TURBO_ERANGE;
  else capacity += snapshot->will_topic.size;
  if (rc == TURBO_OK && snapshot->will_payload.size > SIZE_MAX - capacity) rc = TURBO_ERANGE;
  else if (rc == TURBO_OK) capacity += snapshot->will_payload.size;
  if (rc == TURBO_OK && tstr_len(properties) > SIZE_MAX - capacity) rc = TURBO_ERANGE;
  else if (rc == TURBO_OK) capacity += tstr_len(properties);
  if (rc == TURBO_OK && capacity > maximum) rc = TURBO_EMSGSIZE;
  if (rc == TURBO_OK) {
    packet = tstr_new_len(NULL, capacity);
    if (!packet) rc = TURBO_ENOMEM;
  }
  if (rc == TURBO_OK) {
    publish.version = snapshot->version;
    publish.qos = snapshot->will_qos;
    publish.retain = snapshot->will_retain;
    publish.packet_id = snapshot->will_qos == 0u ? 0u : 1u;
    publish.topic = snapshot->will_topic;
    publish.payload = snapshot->will_payload;
    publish.properties =
        (flowie_mqtt_span_t){(const uint8_t *)properties, tstr_len(properties)};
    rc = flowie_mqtt_publish_packet_encode(&publish, (uint8_t *)packet, capacity, &written);
    if (rc != FLOWIE_MQTT_PARSE_OK || !tstr_set_len_checked(packet, written))
      rc = rc == FLOWIE_MQTT_PARSE_OK ? TURBO_ERANGE : flowie_cluster_session_parse_status(rc);
    else
      rc = TURBO_OK;
  }
  tstr_free(properties);
  if (rc != TURBO_OK) {
    tstr_free(packet);
    return rc;
  }
  *out = packet;
  return TURBO_OK;
}

int flowie_cluster_session_lifecycle_plan_create(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_lifecycle_event_view_t *event, uint64_t now_epoch_seconds,
    flowie_cluster_session_lifecycle_plan_t **out) {
  flowie_cluster_session_lifecycle_plan_t *plan = NULL;
  flowie_cluster_lifecycle_decision_t decision = FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
  flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
  flowie_cluster_session_edge_binding_t edge_binding;
  int rc;
  if (out) *out = NULL;
  if (!bind || !out) return TURBO_EINVAL;
  rc = flowie_cluster_session_lifecycle_event_validate(current_owner, event,
                                                       now_epoch_seconds);
  if (rc != TURBO_OK) return rc;
  if (atomic_exchange_explicit(&bind->inflight, 1, memory_order_acq_rel)) return TURBO_EBUSY;
  plan = (flowie_cluster_session_lifecycle_plan_t *)calloc(1u, sizeof(*plan));
  if (!plan) {
    atomic_store_explicit(&bind->inflight, 0, memory_order_release);
    return TURBO_ENOMEM;
  }
  plan->bind = bind;
  plan->command = (flowie_cluster_pgsql_fact_command_t)FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  plan->mutation =
      (flowie_cluster_pgsql_fact_mutation_t)FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
  plan->entry = flowie_cluster_session_find(bind, event->client_id);
  if (!plan->entry) {
    plan->action = FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE;
    *out = plan;
    return TURBO_OK;
  }
  plan->expected_owner = plan->entry->owner;
  rc = flowie_session_owner_snapshot(plan->entry->owner, &before);
  if (rc == TURBO_OK)
    rc = flowie_cluster_lifecycle_decide(event, &before, now_epoch_seconds, &decision);
  if (rc != TURBO_OK) goto fail;
  plan->action = decision.action;
  if (plan->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE ||
      plan->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT) {
    *out = plan;
    return TURBO_OK;
  }
  rc = flowie_cluster_session_lifecycle_command_id(
      event, plan->action, before.resource_generation, before.session_id,
      plan->command.command_id);
  if (rc != TURBO_OK) goto fail;
  plan->command.owner = *current_owner;
  plan->mutation.shard_key = (const uint8_t *)plan->entry->client_id;
  plan->mutation.shard_key_size = tstr_len(plan->entry->client_id);
  plan->mutation.record.key = (const uint8_t *)plan->entry->client_id;
  plan->mutation.record.key_size = tstr_len(plan->entry->client_id);
  plan->mutation.record.expected_revision = before.resource_generation;
  if (plan->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL) {
    rc = flowie_cluster_session_will_packet_encode(&before, bind->max_event_payload_size,
                                                   &plan->packet);
    if (rc != TURBO_OK) goto fail;
    plan->staged = flowie_session_owner_clone(plan->entry->owner);
    if (!plan->staged) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    rc = flowie_session_owner_will_complete(plan->staged);
    if (rc == TURBO_OK) rc = flowie_session_owner_snapshot(plan->staged, &after);
    if (rc == TURBO_OK && after.resource_generation <= before.resource_generation)
      rc = TURBO_EPROTO;
    if (rc == TURBO_OK) {
      memset(&edge_binding, 0, sizeof(edge_binding));
      edge_binding.edge_node_id = plan->entry->edge_node_id
                                      ? tstr_to_v(plan->entry->edge_node_id)
                                      : (tstr_v){NULL, 0u};
      edge_binding.edge_boot_id =
          plan->entry->edge_node_id ? plan->entry->edge_boot_id : NULL;
      edge_binding.connection_id = plan->entry->connection_id;
      edge_binding.connection_generation = plan->entry->connection_generation;
      edge_binding.action_sequence = plan->entry->edge_action_sequence;
      rc = flowie_cluster_session_fact_encode(bind, tstr_to_v(plan->entry->bind_payload),
                                              plan->staged, &edge_binding, &plan->fact_value);
    }
    if (rc == TURBO_OK)
      rc = flowie_cluster_publish_event_encode(
          before.version, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, event->connection_id,
          event->connection_generation, before.session_id, before.session_generation,
          now_epoch_seconds,
          tstr_v_from_buf((const char *)event->edge_node_id.data, event->edge_node_id.size),
          event->edge_boot_id, event->client_id,
          (flowie_mqtt_span_t){(const uint8_t *)plan->packet, tstr_len(plan->packet)},
          bind->max_event_payload_size, &plan->event_payload);
    if (rc != TURBO_OK) goto fail;
    plan->mutation.write_kind = FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT;
    plan->mutation.record.kind = TURBO_FLOW_RECORD_PUT;
    plan->mutation.record.next_revision = after.resource_generation;
    plan->mutation.record.value = (const uint8_t *)plan->fact_value;
    plan->mutation.record.value_size = tstr_len(plan->fact_value);
    plan->mutation.event_type = FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH;
    plan->mutation.event_payload = (const uint8_t *)plan->event_payload;
    plan->mutation.event_payload_size = tstr_len(plan->event_payload);
  } else if (plan->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_EXPIRE_SESSION) {
    rc = flowie_cluster_subscription_index_build(
        &plan->staged_index, &bind->sessions, &bind->subscription_index, plan->entry, NULL, 0);
    if (rc != TURBO_OK) goto fail;
    plan->staged_index_initialized = 1;
    plan->mutation.write_kind = FLOWIE_CLUSTER_PGSQL_FACT_ONLY;
    plan->mutation.record.kind = TURBO_FLOW_RECORD_DELETE;
    plan->mutation.record.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  } else {
    rc = TURBO_EPROTO;
    goto fail;
  }
  plan->command.mutations = &plan->mutation;
  plan->command.mutation_count = 1u;
  *out = plan;
  return TURBO_OK;

fail:
  flowie_cluster_session_lifecycle_plan_release(plan);
  return rc;
}

const flowie_cluster_pgsql_fact_command_t *flowie_cluster_session_lifecycle_plan_command(
    const flowie_cluster_session_lifecycle_plan_t *plan) {
  return plan && plan->command.mutation_count != 0u ? &plan->command : NULL;
}

int flowie_cluster_session_lifecycle_plan_finalize(
    flowie_cluster_session_lifecycle_plan_t **plan, int durable_status) {
  flowie_cluster_session_lifecycle_plan_t *current;
  flowie_cluster_subscription_index_t previous_index;
  flowie_session_owner_t *previous_owner = NULL;
  int rc;
  if (!plan || !(current = *plan)) return TURBO_EINVAL;
  *plan = NULL;
  if (current->command.mutation_count == 0u) {
    rc = current->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE ? TURBO_OK : TURBO_EBUSY;
    flowie_cluster_session_lifecycle_plan_release(current);
    return rc;
  }
  if (durable_status == TURBO_EALREADY) {
    current->bind->self_fence(current->bind->self_fence_ctx, durable_status);
    flowie_cluster_session_lifecycle_plan_release(current);
    return TURBO_EBUSY;
  }
  rc = durable_status;
  if (rc == TURBO_OK &&
      (!current->entry || current->entry->owner != current->expected_owner)) {
    rc = TURBO_EBUSY;
    current->bind->self_fence(current->bind->self_fence_ctx, rc);
  }
  if (rc == TURBO_OK && current->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL) {
    previous_owner = current->entry->owner;
    current->entry->owner = current->staged;
    current->staged = NULL;
    rc = TURBO_EBUSY;
  } else if (rc == TURBO_OK &&
             current->action == FLOWIE_CLUSTER_LIFECYCLE_ACTION_EXPIRE_SESSION) {
    rc = turbo_hash_map_remove(&current->bind->sessions, &current->entry->key, NULL);
    if (rc == TURBO_OK) {
      previous_index = current->bind->subscription_index;
      current->bind->subscription_index = current->staged_index;
      memset(&current->staged_index, 0, sizeof(current->staged_index));
      current->staged_index_initialized = 0;
      flowie_cluster_session_entry_destroy(current->entry);
      current->entry = NULL;
      flowie_cluster_subscription_index_destroy(&previous_index);
    } else {
      rc = TURBO_EPROTO;
      current->bind->self_fence(current->bind->self_fence_ctx, rc);
    }
  }
  flowie_session_owner_destroy(previous_owner);
  flowie_cluster_session_lifecycle_plan_release(current);
  return rc;
}

void flowie_cluster_session_lifecycle_plan_destroy(
    flowie_cluster_session_lifecycle_plan_t **plan) {
  flowie_cluster_session_lifecycle_plan_t *current;
  if (!plan || !(current = *plan)) return;
  *plan = NULL;
  flowie_cluster_session_lifecycle_plan_release(current);
}

int flowie_cluster_session_bind_pgsql_submit(void *ctx,
                                             const flowie_cluster_pgsql_fact_command_t *command,
                                             flowie_cluster_pgsql_fact_completion_fn completion,
                                             void *completion_ctx) {
  return flowie_cluster_pgsql_fact_worker_submit((flowie_cluster_pgsql_fact_worker_t *)ctx, command,
                                                 completion, completion_ctx);
}
