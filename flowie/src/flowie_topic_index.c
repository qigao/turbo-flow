#include "flowie_topic_index_internal.h"

#include "turbo_error.h"
#include "turbo_str.h"

#include <stdlib.h>
#include <string.h>

static size_t flowie_topic_key_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *token = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(token->data, token->len, NULL);
}

static bool flowie_topic_key_equal(const void *left, const void *right, size_t key_size,
                                   void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static void flowie_topic_node_destroy(flowie_topic_node_t *node) {
  if (!node) return;
  turbo_hash_map_destroy(&node->exact_children);
  turbo_vec_destroy(&node->terminal_entries);
  turbo_vec_destroy(&node->hash_entries);
  tstr_free(node->token);
  free(node);
}

void flowie_topic_index_destroy(flowie_topic_index_t *index) {
  if (!index || !index->initialized) return;
  for (size_t i = 0u; i < turbo_vec_size(&index->nodes); ++i) {
    flowie_topic_node_t **node = (flowie_topic_node_t **)turbo_vec_at(&index->nodes, i);
    if (!node || !*node) continue;
    flowie_topic_node_destroy(*node);
  }
  turbo_vec_destroy(&index->match_next);
  turbo_vec_destroy(&index->match_active);
  turbo_vec_destroy(&index->nodes);
  memset(index, 0, sizeof(*index));
}

static int flowie_topic_node_create(flowie_topic_index_t *index, flowie_topic_node_t *parent,
                                    tstr_v token, int plus_edge, flowie_topic_node_t **out) {
  flowie_topic_node_t *node;
  int rc;
  if (!index || !index->initialized || !out) return TURBO_EINVAL;
  *out = NULL;
  node = (flowie_topic_node_t *)calloc(1u, sizeof(*node));
  if (!node) return TURBO_ENOMEM;
  node->parent = parent;
  node->plus_edge = plus_edge;
  node->node_slot = FLOWIE_TOPIC_INDEX_NO_ENTRY;
  if (parent && !plus_edge) {
    node->token = tstr_dup_len(token.data, token.len);
    if (!node->token) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
  }
  rc = turbo_hash_map_init(&node->exact_children, sizeof(tstr_v), sizeof(flowie_topic_node_t *),
                           flowie_topic_key_hash, flowie_topic_key_equal, NULL);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_vec_init(&node->terminal_entries, sizeof(size_t));
  if (rc != TURBO_OK) goto fail;
  rc = turbo_vec_init(&node->hash_entries, sizeof(size_t));
  if (rc != TURBO_OK) goto fail;
  node->node_slot = turbo_vec_size(&index->nodes);
  rc = turbo_vec_push(&index->nodes, &node);
  if (rc != TURBO_OK) goto fail;
  *out = node;
  return TURBO_OK;

fail:
  flowie_topic_node_destroy(node);
  return rc;
}

int flowie_topic_index_init(flowie_topic_index_t *index) {
  int rc;
  if (!index) return TURBO_EINVAL;
  memset(index, 0, sizeof(*index));
  rc = turbo_vec_init(&index->nodes, sizeof(flowie_topic_node_t *));
  if (rc != TURBO_OK) return rc;
  rc = turbo_vec_init(&index->match_active, sizeof(flowie_topic_node_t *));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&index->nodes);
    return rc;
  }
  rc = turbo_vec_init(&index->match_next, sizeof(flowie_topic_node_t *));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&index->match_active);
    turbo_vec_destroy(&index->nodes);
    return rc;
  }
  index->initialized = 1;
  rc = flowie_topic_node_create(index, NULL, tstr_v_from_buf(NULL, 0u), 0, &index->root);
  if (rc != TURBO_OK) flowie_topic_index_destroy(index);
  return rc;
}

static flowie_mqtt_span_t flowie_topic_filter_inner(flowie_mqtt_span_t filter) {
  static const uint8_t prefix[] = "$share/";
  if (filter.size <= sizeof(prefix) - 1u || memcmp(filter.data, prefix, sizeof(prefix) - 1u) != 0)
    return filter;
  for (size_t i = sizeof(prefix) - 1u; i < filter.size; ++i) {
    if (filter.data[i] == '/')
      return (flowie_mqtt_span_t){filter.data + i + 1u, filter.size - i - 1u};
  }
  return (flowie_mqtt_span_t){NULL, 0u};
}

static int flowie_topic_exact_child(flowie_topic_index_t *index, flowie_topic_node_t *parent,
                                    tstr_v token, flowie_topic_node_t **out) {
  flowie_topic_node_t **found;
  flowie_topic_node_t *created;
  int rc;
  if (!index || !parent || !out) return TURBO_EINVAL;
  found = (flowie_topic_node_t **)turbo_hash_map_get(&parent->exact_children, &token);
  if (found) {
    *out = *found;
    return TURBO_OK;
  }
  rc = flowie_topic_node_create(index, parent, token, 0, &created);
  if (rc != TURBO_OK) return rc;
  token = tstr_to_v(created->token);
  rc = turbo_hash_map_put(&parent->exact_children, &token, &created);
  if (rc != TURBO_OK) {
    flowie_topic_node_t **slot =
        (flowie_topic_node_t **)turbo_vec_at(&index->nodes, created->node_slot);
    if (created->node_slot + 1u == turbo_vec_size(&index->nodes))
      (void)turbo_vec_pop(&index->nodes, NULL);
    else if (slot) *slot = NULL;
    flowie_topic_node_destroy(created);
    return rc;
  }
  *out = created;
  return TURBO_OK;
}

int flowie_topic_index_insert_bound(flowie_topic_index_t *index, flowie_mqtt_span_t filter,
                                    size_t entry_index, flowie_topic_index_binding_t *binding) {
  flowie_topic_node_t *node;
  size_t offset = 0u;
  if (!index || !index->root || !filter.data || filter.size == 0u || !binding) return TURBO_EINVAL;
  memset(binding, 0, sizeof(*binding));
  filter = flowie_topic_filter_inner(filter);
  if (!filter.data || filter.size == 0u) return TURBO_EPROTO;
  node = index->root;
  for (;;) {
    size_t end = offset;
    tstr_v token;
    int last;
    int rc;
    while (end < filter.size && filter.data[end] != '/')
      ++end;
    token = tstr_v_from_buf((const char *)filter.data + offset, end - offset);
    last = end == filter.size;
    if (token.len == 1u && token.data[0] == '#') {
      if (!last) return TURBO_EPROTO;
      rc = turbo_vec_push(&node->hash_entries, &entry_index);
      if (rc == TURBO_OK) {
        binding->node = node;
        binding->position = turbo_vec_size(&node->hash_entries) - 1u;
        binding->bucket = FLOWIE_TOPIC_BUCKET_HASH;
      }
      return rc;
    }
    if (token.len == 1u && token.data[0] == '+') {
      if (!node->plus_child) {
        rc = flowie_topic_node_create(index, node, tstr_v_from_buf(NULL, 0u), 1, &node->plus_child);
        if (rc != TURBO_OK) return rc;
      }
      node = node->plus_child;
    } else {
      flowie_topic_node_t *child = NULL;
      rc = flowie_topic_exact_child(index, node, token, &child);
      if (rc != TURBO_OK) return rc;
      node = child;
    }
    if (last) {
      rc = turbo_vec_push(&node->terminal_entries, &entry_index);
      if (rc == TURBO_OK) {
        binding->node = node;
        binding->position = turbo_vec_size(&node->terminal_entries) - 1u;
        binding->bucket = FLOWIE_TOPIC_BUCKET_TERMINAL;
      }
      return rc;
    }
    offset = end + 1u;
  }
}

int flowie_topic_index_insert(flowie_topic_index_t *index, flowie_mqtt_span_t filter,
                              size_t entry_index) {
  flowie_topic_index_binding_t ignored;
  return flowie_topic_index_insert_bound(index, filter, entry_index, &ignored);
}

static int flowie_topic_node_empty(const flowie_topic_node_t *node) {
  return node && !node->plus_child && turbo_hash_map_empty(&node->exact_children) &&
         turbo_vec_empty(&node->terminal_entries) && turbo_vec_empty(&node->hash_entries);
}

static int flowie_topic_index_prune(flowie_topic_index_t *index, flowie_topic_node_t *node) {
  if (!index || !node) return TURBO_EINVAL;
  while (node != index->root && flowie_topic_node_empty(node)) {
    flowie_topic_node_t *parent = node->parent;
    size_t removed_slot = node->node_slot;
    int rc;
    if (!parent || node->node_slot >= turbo_vec_size(&index->nodes)) return TURBO_EPROTO;
    if (node->plus_edge) {
      if (parent->plus_child != node) return TURBO_EPROTO;
      parent->plus_child = NULL;
    } else {
      tstr_v token = tstr_to_v(node->token);
      rc = turbo_hash_map_remove(&parent->exact_children, &token, NULL);
      if (rc != TURBO_OK) return TURBO_EPROTO;
    }
    {
      flowie_topic_node_t *const *slot =
          (flowie_topic_node_t *const *)turbo_vec_at_const(&index->nodes, removed_slot);
      if (!slot || *slot != node) return TURBO_EPROTO;
    }
    rc = turbo_vec_swap_remove(&index->nodes, removed_slot, NULL);
    if (rc != TURBO_OK) return rc;
    if (removed_slot < turbo_vec_size(&index->nodes)) {
      flowie_topic_node_t **moved =
          (flowie_topic_node_t **)turbo_vec_at(&index->nodes, removed_slot);
      if (!moved || !*moved) return TURBO_EPROTO;
      (*moved)->node_slot = removed_slot;
    }
    flowie_topic_node_destroy(node);
    node = parent;
  }
  return TURBO_OK;
}

int flowie_topic_index_remove(flowie_topic_index_t *index, flowie_topic_index_binding_t *binding,
                              size_t entry_index, size_t *moved_entry_index) {
  flowie_topic_node_t *node;
  turbo_vec_t *entries;
  const size_t *stored;
  const size_t *last;
  size_t moved = FLOWIE_TOPIC_INDEX_NO_ENTRY;
  int rc;
  if (moved_entry_index) *moved_entry_index = FLOWIE_TOPIC_INDEX_NO_ENTRY;
  if (!index || !index->root || !binding || !binding->node) return TURBO_EINVAL;
  node = binding->node;
  if (binding->bucket == FLOWIE_TOPIC_BUCKET_TERMINAL) entries = &node->terminal_entries;
  else if (binding->bucket == FLOWIE_TOPIC_BUCKET_HASH) entries = &node->hash_entries;
  else return TURBO_EINVAL;
  stored = (const size_t *)turbo_vec_at_const(entries, binding->position);
  if (!stored || *stored != entry_index) return TURBO_EPROTO;
  last = (const size_t *)turbo_vec_at_const(entries, turbo_vec_size(entries) - 1u);
  if (!last) return TURBO_EPROTO;
  if (binding->position + 1u != turbo_vec_size(entries)) moved = *last;
  rc = turbo_vec_swap_remove(entries, binding->position, NULL);
  if (rc != TURBO_OK) return rc;
  binding->node = NULL;
  binding->position = 0u;
  binding->bucket = FLOWIE_TOPIC_BUCKET_NONE;
  if (moved_entry_index) *moved_entry_index = moved;
  return flowie_topic_index_prune(index, node);
}

static int flowie_topic_entries_append(turbo_vec_t *matched, const turbo_vec_t *entries) {
  if (!matched || !entries) return TURBO_EINVAL;
  for (size_t i = 0u; i < turbo_vec_size(entries); ++i) {
    const size_t *entry = (const size_t *)turbo_vec_at_const(entries, i);
    int rc;
    if (!entry) return TURBO_EPROTO;
    rc = turbo_vec_push(matched, entry);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

int flowie_topic_index_match(flowie_topic_index_t *index, flowie_mqtt_span_t topic,
                             turbo_vec_t *matched) {
  turbo_vec_t *active;
  turbo_vec_t *next;
  size_t offset = 0u;
  int first_level = 1;
  int rc;
  if (!index || !index->root || !topic.data || topic.size == 0u || !matched) return TURBO_EINVAL;
  active = &index->match_active;
  next = &index->match_next;
  turbo_vec_clear(active);
  turbo_vec_clear(next);
  rc = turbo_vec_push(active, &index->root);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    size_t end = offset;
    tstr_v token;
    int last;
    while (end < topic.size && topic.data[end] != '/')
      ++end;
    token = tstr_v_from_buf((const char *)topic.data + offset, end - offset);
    last = end == topic.size;
    turbo_vec_clear(next);
    for (size_t i = 0u; i < turbo_vec_size(active); ++i) {
      flowie_topic_node_t *const *node =
          (flowie_topic_node_t *const *)turbo_vec_at_const(active, i);
      flowie_topic_node_t *const *exact;
      if (!node || !*node) {
        rc = TURBO_EPROTO;
        goto done;
      }
      if (!(first_level && topic.data[0] == '$')) {
        rc = flowie_topic_entries_append(matched, &(*node)->hash_entries);
        if (rc != TURBO_OK) goto done;
        if ((*node)->plus_child) {
          rc = turbo_vec_push(next, &(*node)->plus_child);
          if (rc != TURBO_OK) goto done;
        }
      }
      exact =
          (flowie_topic_node_t *const *)turbo_hash_map_get_const(&(*node)->exact_children, &token);
      if (exact && *exact) {
        rc = turbo_vec_push(next, exact);
        if (rc != TURBO_OK) goto done;
      }
    }
    if (last) break;
    {
      turbo_vec_t *swap = active;
      active = next;
      next = swap;
    }
    offset = end + 1u;
    first_level = 0;
    if (turbo_vec_size(active) == 0u) {
      turbo_vec_clear(next);
      break;
    }
  }
  for (size_t i = 0u; i < turbo_vec_size(next); ++i) {
    flowie_topic_node_t *const *node = (flowie_topic_node_t *const *)turbo_vec_at_const(next, i);
    if (!node || !*node) {
      rc = TURBO_EPROTO;
      goto done;
    }
    rc = flowie_topic_entries_append(matched, &(*node)->terminal_entries);
    if (rc == TURBO_OK) rc = flowie_topic_entries_append(matched, &(*node)->hash_entries);
    if (rc != TURBO_OK) goto done;
  }
  rc = TURBO_OK;

done:
  return rc;
}

static int flowie_topic_entries_visit(const turbo_vec_t *entries, flowie_topic_index_visit_fn visit,
                                      void *ctx) {
  if (!entries || !visit) return TURBO_EINVAL;
  for (size_t i = 0u; i < turbo_vec_size(entries); ++i) {
    const size_t *entry = (const size_t *)turbo_vec_at_const(entries, i);
    int rc;
    if (!entry) return TURBO_EPROTO;
    rc = visit(ctx, *entry);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flowie_topic_node_visit_end(const flowie_topic_node_t *node,
                                       flowie_topic_index_visit_fn visit, void *ctx) {
  int rc;
  if (!node) return TURBO_EPROTO;
  rc = flowie_topic_entries_visit(&node->terminal_entries, visit, ctx);
  return rc == TURBO_OK ? flowie_topic_entries_visit(&node->hash_entries, visit, ctx) : rc;
}

static int flowie_topic_index_visit_topic_node(const flowie_topic_node_t *node,
                                               flowie_mqtt_span_t topic, size_t offset,
                                               int first_level, flowie_topic_index_visit_fn visit,
                                               void *ctx) {
  flowie_topic_node_t *const *exact;
  size_t end = offset;
  tstr_v token;
  int last;
  int rc;
  while (end < topic.size && topic.data[end] != '/')
    ++end;
  token = tstr_v_from_buf((const char *)topic.data + offset, end - offset);
  last = end == topic.size;
  if (!(first_level && topic.data[0] == '$')) {
    rc = flowie_topic_entries_visit(&node->hash_entries, visit, ctx);
    if (rc != TURBO_OK) return rc;
  }
  exact = (flowie_topic_node_t *const *)turbo_hash_map_get_const(&node->exact_children, &token);
  if (exact && *exact) {
    rc = last ? flowie_topic_node_visit_end(*exact, visit, ctx)
              : flowie_topic_index_visit_topic_node(*exact, topic, end + 1u, 0, visit, ctx);
    if (rc != TURBO_OK) return rc;
  }
  if (node->plus_child && !(first_level && topic.data[0] == '$')) {
    rc = last ? flowie_topic_node_visit_end(node->plus_child, visit, ctx)
              : flowie_topic_index_visit_topic_node(node->plus_child, topic, end + 1u, 0, visit,
                                                    ctx);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

int flowie_topic_index_visit_topic(const flowie_topic_index_t *index, flowie_mqtt_span_t topic,
                                   flowie_topic_index_visit_fn visit, void *ctx) {
  if (!flowie_mqtt_topic_name_validate(topic)) return TURBO_EINVAL;
  return flowie_topic_index_visit_validated_topic(index, topic, visit, ctx);
}

int flowie_topic_index_visit_validated_topic(const flowie_topic_index_t *index,
                                             flowie_mqtt_span_t topic,
                                             flowie_topic_index_visit_fn visit, void *ctx) {
  if (!index || !index->initialized || !index->root || !visit || !topic.data || topic.size == 0u)
    return TURBO_EINVAL;
  return flowie_topic_index_visit_topic_node(index->root, topic, 0u, 1, visit, ctx);
}

static int flowie_topic_index_visit_containing_node(const flowie_topic_node_t *node,
                                                    flowie_mqtt_span_t requested, size_t offset,
                                                    int first_level,
                                                    flowie_topic_index_visit_fn visit, void *ctx) {
  flowie_topic_node_t *const *exact;
  size_t end = offset;
  tstr_v token;
  int requested_hash;
  int requested_plus;
  int requested_system;
  int last;
  int rc;
  while (end < requested.size && requested.data[end] != '/')
    ++end;
  token = tstr_v_from_buf((const char *)requested.data + offset, end - offset);
  last = end == requested.size;
  requested_hash = token.len == 1u && token.data[0] == '#';
  requested_plus = token.len == 1u && token.data[0] == '+';
  requested_system = first_level && token.len != 0u && token.data[0] == '$';
  if (!requested_system) {
    rc = flowie_topic_entries_visit(&node->hash_entries, visit, ctx);
    if (rc != TURBO_OK) return rc;
  }
  if (requested_hash) return TURBO_OK;
  if (!requested_plus) {
    exact = (flowie_topic_node_t *const *)turbo_hash_map_get_const(&node->exact_children, &token);
    if (exact && *exact) {
      rc = last ? flowie_topic_node_visit_end(*exact, visit, ctx)
                : flowie_topic_index_visit_containing_node(*exact, requested, end + 1u, 0, visit,
                                                           ctx);
      if (rc != TURBO_OK) return rc;
    }
  }
  if (node->plus_child && !requested_system) {
    rc = last ? flowie_topic_node_visit_end(node->plus_child, visit, ctx)
              : flowie_topic_index_visit_containing_node(node->plus_child, requested, end + 1u, 0,
                                                         visit, ctx);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

int flowie_topic_index_visit_containing_filters(const flowie_topic_index_t *index,
                                                flowie_mqtt_span_t requested_filter,
                                                flowie_topic_index_visit_fn visit, void *ctx) {
  if (!flowie_mqtt_topic_filter_validate(requested_filter)) return TURBO_EINVAL;
  return flowie_topic_index_visit_validated_containing_filters(index, requested_filter, visit, ctx);
}

int flowie_topic_index_visit_validated_containing_filters(const flowie_topic_index_t *index,
                                                          flowie_mqtt_span_t requested_filter,
                                                          flowie_topic_index_visit_fn visit,
                                                          void *ctx) {
  if (!index || !index->initialized || !index->root || !visit || !requested_filter.data ||
      requested_filter.size == 0u)
    return TURBO_EINVAL;
  requested_filter = flowie_topic_filter_inner(requested_filter);
  if (!requested_filter.data || requested_filter.size == 0u) return TURBO_EINVAL;
  return flowie_topic_index_visit_containing_node(index->root, requested_filter, 0u, 1, visit, ctx);
}
