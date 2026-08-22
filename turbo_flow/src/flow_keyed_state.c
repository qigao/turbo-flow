#include "flow_internal.h"

#include "turbo_flow_stl_adapter.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_keyed_state_entry_s {
  mem_buffer_t *key;
  mem_buffer_t *value;
  uint64_t revision;
  int present;
} flow_keyed_state_entry_t;

typedef enum flow_keyed_state_mutation_e {
  FLOW_KEYED_STATE_MUTATION_NONE = 0,
  FLOW_KEYED_STATE_MUTATION_PUT,
  FLOW_KEYED_STATE_MUTATION_DELETE
} flow_keyed_state_mutation_t;

struct turbo_flow_keyed_state_store_s {
  turbo_hash_map_t entries;
  turbo_mutex_t mutex;
  size_t max_entries;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_total_bytes;
  size_t current_bytes;
  size_t present_count;
  size_t application_key_size;
  uint64_t window_size_ns;
  uint64_t allowed_lateness_ns;
  uint64_t watermark_ns;
  int event_time;
  int watermark_initialized;
  int watermark_advancing;
  int bound;
  int initialized;
};

struct turbo_flow_keyed_state_s {
  turbo_flow_keyed_state_store_t *store;
  mem_buffer_t *key;
  mem_buffer_t *snapshot;
  mem_buffer_t *pending;
  uint64_t revision;
  int had_slot;
  int present;
  int active;
  int status;
  size_t key_prefix_size;
  uint64_t close_at_ns;
  int event_time;
  flow_keyed_state_mutation_t mutation;
};

#define FLOW_EVENT_TIME_KEY_PREFIX_SIZE sizeof(uint64_t)

static vstr flow_keyed_buffer_view(const mem_buffer_t *buffer) {
  if (!buffer) return vstr_from_buf(NULL, 0u);
  return vstr_from_buf(mem_buffer_const_data(buffer), mem_buffer_used(buffer));
}

static size_t flow_keyed_hash(const void *key, size_t key_size, void *ctx) {
  const vstr *view = (const vstr *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(view->data, view->len, NULL);
}

static bool flow_keyed_equal(const void *left, const void *right, size_t key_size, void *ctx) {
  const vstr *lhs = (const vstr *)left;
  const vstr *rhs = (const vstr *)right;
  (void)key_size;
  (void)ctx;
  return lhs->len == rhs->len && memcmp(lhs->data, rhs->data, lhs->len) == 0;
}

static mem_buffer_t *flow_keyed_copy(vstr value) {
  mem_buffer_t *buffer;
  const size_t allocation_size = value.len == 0u ? 1u : value.len;

  if (value.len > 0u && !value.data) return NULL;
  buffer = mem_get_buffer(mem_global(), allocation_size);
  if (!buffer) return NULL;
  if (value.len > 0u) memcpy(mem_buffer_data(buffer), value.data, value.len);
  buffer->used = value.len;
  return buffer;
}

static int flow_keyed_config_valid(const turbo_flow_keyed_state_store_config_t *config) {
  if (!config || config->size < sizeof(*config) || config->max_entries == 0u ||
      config->max_key_size == 0u || config->max_value_size == 0u ||
      config->max_total_bytes == 0u) {
    return 0;
  }
  if (config->max_entries > TURBO_FLOW_KEYED_STATE_MAX_ENTRIES ||
      config->max_key_size > TURBO_FLOW_KEYED_STATE_MAX_KEY_SIZE ||
      config->max_value_size > TURBO_FLOW_KEYED_STATE_MAX_VALUE_SIZE ||
      config->max_key_size > config->max_total_bytes ||
      config->max_value_size > config->max_total_bytes) {
    return 0;
  }
  return 1;
}

turbo_flow_keyed_state_store_t *turbo_flow_keyed_state_store_create(
    const turbo_flow_keyed_state_store_config_t *config) {
  turbo_flow_keyed_state_store_t *store;
  int rc;

  if (!flow_keyed_config_valid(config)) return NULL;
  store = (turbo_flow_keyed_state_store_t *)calloc(1u, sizeof(*store));
  if (!store) return NULL;
  rc = turbo_hash_map_init(&store->entries, sizeof(vstr), sizeof(flow_keyed_state_entry_t),
                           flow_keyed_hash, flow_keyed_equal, NULL);
  if (rc != TURBO_OK) {
    free(store);
    return NULL;
  }
  rc = turbo_hash_map_reserve(&store->entries, config->max_entries);
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&store->entries);
    free(store);
    return NULL;
  }
  turbo_mutex_init(&store->mutex);
  store->max_entries = config->max_entries;
  store->max_key_size = config->max_key_size;
  store->max_value_size = config->max_value_size;
  store->max_total_bytes = config->max_total_bytes;
  store->initialized = 1;
  return store;
}

turbo_flow_event_time_window_store_t *turbo_flow_event_time_window_store_create(
    const turbo_flow_event_time_window_store_config_t *config) {
  turbo_flow_keyed_state_store_config_t keyed_config = TURBO_FLOW_KEYED_STATE_STORE_CONFIG_INIT;
  turbo_flow_keyed_state_store_t *store;

  if (!config || config->size < sizeof(*config) || config->max_windows == 0u ||
      config->max_key_size == 0u || config->max_value_size == 0u ||
      config->max_total_bytes == 0u || config->window_size_ns == 0u ||
      config->max_key_size > TURBO_FLOW_KEYED_STATE_MAX_KEY_SIZE -
                                 FLOW_EVENT_TIME_KEY_PREFIX_SIZE) {
    return NULL;
  }
  keyed_config.max_entries = config->max_windows;
  keyed_config.max_key_size = config->max_key_size + FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  keyed_config.max_value_size = config->max_value_size;
  keyed_config.max_total_bytes = config->max_total_bytes;
  store = turbo_flow_keyed_state_store_create(&keyed_config);
  if (!store) return NULL;
  store->application_key_size = config->max_key_size;
  store->window_size_ns = config->window_size_ns;
  store->allowed_lateness_ns = config->allowed_lateness_ns;
  store->event_time = 1;
  return store;
}

void turbo_flow_event_time_window_store_destroy(turbo_flow_event_time_window_store_t *store) {
  turbo_flow_keyed_state_store_destroy(store);
}

uint64_t turbo_flow_event_time_window_watermark(
    const turbo_flow_event_time_window_store_t *store, int *initialized) {
  uint64_t watermark = 0u;
  int has_watermark = 0;
  if (store && store->initialized && store->event_time) {
    turbo_mutex_lock((turbo_mutex_t *)&store->mutex);
    watermark = store->watermark_ns;
    has_watermark = store->watermark_initialized;
    turbo_mutex_unlock((turbo_mutex_t *)&store->mutex);
  }
  if (initialized) *initialized = has_watermark;
  return watermark;
}

void turbo_flow_keyed_state_store_destroy(turbo_flow_keyed_state_store_t *store) {
  size_t slot;

  if (!store) return;
  if (store->initialized) {
    for (slot = 0u; slot < turbo_hash_map_capacity(&store->entries); ++slot) {
      flow_keyed_state_entry_t *entry =
          (flow_keyed_state_entry_t *)turbo_hash_map_value_at(&store->entries, slot);
      if (!entry) continue;
      mem_buffer_release(entry->value);
      mem_buffer_release(entry->key);
    }
    turbo_hash_map_destroy(&store->entries);
    turbo_mutex_destroy(&store->mutex);
  }
  free(store);
}

size_t turbo_flow_keyed_state_store_size(const turbo_flow_keyed_state_store_t *store) {
  size_t count;

  if (!store || !store->initialized) return 0u;
  turbo_mutex_lock((turbo_mutex_t *)&store->mutex);
  count = store->present_count;
  turbo_mutex_unlock((turbo_mutex_t *)&store->mutex);
  return count;
}

int flow_keyed_state_store_bind(turbo_flow_keyed_state_store_t *store) {
  int rc = TURBO_OK;
  if (!store || !store->initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&store->mutex);
  if (store->bound) rc = TURBO_EALREADY;
  else store->bound = 1;
  turbo_mutex_unlock(&store->mutex);
  return rc;
}

int flow_keyed_state_store_is_event_time(const turbo_flow_keyed_state_store_t *store) {
  return store && store->initialized && store->event_time;
}

void flow_keyed_state_store_unbind(turbo_flow_keyed_state_store_t *store) {
  if (!store || !store->initialized) return;
  turbo_mutex_lock(&store->mutex);
  store->bound = 0;
  turbo_mutex_unlock(&store->mutex);
}

void flow_keyed_state_store_reset(turbo_flow_keyed_state_store_t *store) {
  size_t slot;

  if (!store || !store->initialized) return;
  /* Runtime generation changes occur only while publish admission is closed. */
  for (slot = 0u; slot < turbo_hash_map_capacity(&store->entries); ++slot) {
    flow_keyed_state_entry_t *entry =
        (flow_keyed_state_entry_t *)turbo_hash_map_value_at(&store->entries, slot);
    if (!entry) continue;
    mem_buffer_release(entry->value);
    mem_buffer_release(entry->key);
  }
  turbo_hash_map_clear(&store->entries);
  store->current_bytes = 0u;
  store->present_count = 0u;
  store->watermark_ns = 0u;
  store->watermark_initialized = 0;
  store->watermark_advancing = 0;
}

vstr turbo_flow_keyed_state_key(const turbo_flow_keyed_state_t *state) {
  vstr key;
  if (!state || !state->active) return vstr_from_buf(NULL, 0u);
  key = flow_keyed_buffer_view(state->key);
  if (state->key_prefix_size > key.len) return vstr_from_buf(NULL, 0u);
  return vstr_from_buf(key.data + state->key_prefix_size, key.len - state->key_prefix_size);
}

int turbo_flow_keyed_state_get(const turbo_flow_keyed_state_t *state, vstr *value,
                               uint64_t *revision) {
  const mem_buffer_t *buffer;

  if (!value) return TURBO_EINVAL;
  *value = vstr_from_buf(NULL, 0u);
  if (!state || !state->active) return TURBO_EBUSY;
  if (state->mutation == FLOW_KEYED_STATE_MUTATION_DELETE ||
      (state->mutation == FLOW_KEYED_STATE_MUTATION_NONE && !state->present)) {
    return TURBO_ENOENT;
  }
  buffer = state->mutation == FLOW_KEYED_STATE_MUTATION_PUT ? state->pending : state->snapshot;
  if (!buffer) return TURBO_ENOENT;
  *value = flow_keyed_buffer_view(buffer);
  if (revision) *revision = state->revision;
  return TURBO_OK;
}

int turbo_flow_keyed_state_put(turbo_flow_keyed_state_t *state, vstr value) {
  mem_buffer_t *pending;

  if (!state || !state->active) return TURBO_EBUSY;
  if (state->status != TURBO_OK) return state->status;
  if ((value.len > 0u && !value.data) || value.len > state->store->max_value_size) {
    state->status = value.len > state->store->max_value_size ? TURBO_ENOSPC : TURBO_EINVAL;
    return state->status;
  }
  pending = flow_keyed_copy(value);
  if (!pending) {
    state->status = TURBO_ENOMEM;
    return state->status;
  }
  mem_buffer_release(state->pending);
  state->pending = pending;
  state->mutation = FLOW_KEYED_STATE_MUTATION_PUT;
  return TURBO_OK;
}

int turbo_flow_keyed_state_delete(turbo_flow_keyed_state_t *state) {
  if (!state || !state->active) return TURBO_EBUSY;
  if (state->status != TURBO_OK) return state->status;
  if (state->mutation == FLOW_KEYED_STATE_MUTATION_PUT && !state->present) {
    mem_buffer_release(state->pending);
    state->pending = NULL;
    state->mutation = FLOW_KEYED_STATE_MUTATION_NONE;
    return TURBO_OK;
  }
  if (!state->present || state->mutation == FLOW_KEYED_STATE_MUTATION_DELETE) {
    return TURBO_ENOENT;
  }
  mem_buffer_release(state->pending);
  state->pending = NULL;
  state->mutation = FLOW_KEYED_STATE_MUTATION_DELETE;
  return TURBO_OK;
}

static int flow_keyed_snapshot(turbo_flow_keyed_state_t *state) {
  vstr key = flow_keyed_buffer_view(state->key);
  flow_keyed_state_entry_t *entry;

  turbo_mutex_lock(&state->store->mutex);
  entry = (flow_keyed_state_entry_t *)turbo_hash_map_get(&state->store->entries, &key);
  if (entry) {
    state->had_slot = 1;
    state->present = entry->present;
    state->revision = entry->revision;
    if (entry->present) state->snapshot = mem_buffer_retain(entry->value);
  }
  turbo_mutex_unlock(&state->store->mutex);
  return TURBO_OK;
}

static int flow_keyed_conflict(const turbo_flow_keyed_state_t *state,
                               const flow_keyed_state_entry_t *entry) {
  if (!state->had_slot) return entry != NULL;
  return !entry || entry->revision != state->revision;
}

static int flow_keyed_event_time_gate_locked(const turbo_flow_keyed_state_t *state) {
  const turbo_flow_keyed_state_store_t *store = state->store;
  if (state->event_time && store->watermark_initialized &&
      store->watermark_ns >= state->close_at_ns) {
    return TURBO_ETIMEDOUT;
  }
  return TURBO_OK;
}

static int flow_keyed_commit_put(turbo_flow_keyed_state_t *state) {
  turbo_flow_keyed_state_store_t *store = state->store;
  vstr key = flow_keyed_buffer_view(state->key);
  flow_keyed_state_entry_t replacement;
  flow_keyed_state_entry_t *entry;
  mem_buffer_t *prepared_key = mem_buffer_retain(state->key);
  mem_buffer_t *prepared_value = mem_buffer_retain(state->pending);
  mem_buffer_t *old_value = NULL;
  mem_buffer_t *previous_value = NULL;
  size_t new_bytes;
  size_t old_value_bytes = 0u;
  int was_present = 0;
  int rc = TURBO_OK;

  if (!prepared_key || !prepared_value) {
    mem_buffer_release(prepared_key);
    mem_buffer_release(prepared_value);
    return TURBO_ENOMEM;
  }
  turbo_mutex_lock(&store->mutex);
  rc = flow_keyed_event_time_gate_locked(state);
  if (rc != TURBO_OK) goto unlock;
  entry = (flow_keyed_state_entry_t *)turbo_hash_map_get(&store->entries, &key);
  if (flow_keyed_conflict(state, entry)) {
    rc = TURBO_EBUSY;
    goto unlock;
  }
  if ((!entry && turbo_hash_map_size(&store->entries) >= store->max_entries) ||
      (entry && entry->revision == UINT64_MAX)) {
    rc = entry ? TURBO_ERANGE : TURBO_ENOSPC;
    goto unlock;
  }
  if (entry) {
    was_present = entry->present;
    previous_value = entry->value;
  }
  if (was_present) old_value_bytes = mem_buffer_used(previous_value);
  new_bytes = store->current_bytes - old_value_bytes;
  if (!entry) {
    if (new_bytes > SIZE_MAX - key.len) {
      rc = TURBO_ENOSPC;
      goto unlock;
    }
    new_bytes += key.len;
  }
  if (new_bytes > SIZE_MAX - mem_buffer_used(prepared_value) ||
      new_bytes + mem_buffer_used(prepared_value) > store->max_total_bytes) {
    rc = TURBO_ENOSPC;
    goto unlock;
  }
  new_bytes += mem_buffer_used(prepared_value);

  memset(&replacement, 0, sizeof(replacement));
  replacement.key = entry ? entry->key : prepared_key;
  replacement.value = prepared_value;
  replacement.revision = entry ? entry->revision + 1u : 1u;
  replacement.present = 1;
  rc = turbo_hash_map_put(&store->entries, &key, &replacement);
  if (rc != TURBO_OK) goto unlock;
  old_value = previous_value;
  if (!entry) prepared_key = NULL;
  prepared_value = NULL;
  if (!was_present) store->present_count += 1u;
  store->current_bytes = new_bytes;

unlock:
  turbo_mutex_unlock(&store->mutex);
  mem_buffer_release(old_value);
  mem_buffer_release(prepared_key);
  mem_buffer_release(prepared_value);
  return rc;
}

static int flow_keyed_commit_delete(turbo_flow_keyed_state_t *state) {
  turbo_flow_keyed_state_store_t *store = state->store;
  vstr key = flow_keyed_buffer_view(state->key);
  flow_keyed_state_entry_t replacement;
  flow_keyed_state_entry_t *entry;
  mem_buffer_t *old_value = NULL;
  int rc = TURBO_OK;

  turbo_mutex_lock(&store->mutex);
  rc = flow_keyed_event_time_gate_locked(state);
  if (rc != TURBO_OK) goto unlock;
  entry = (flow_keyed_state_entry_t *)turbo_hash_map_get(&store->entries, &key);
  if (flow_keyed_conflict(state, entry)) {
    rc = TURBO_EBUSY;
    goto unlock;
  }
  if (!entry || !entry->present) {
    rc = TURBO_ENOENT;
    goto unlock;
  }
  if (entry->revision == UINT64_MAX) {
    rc = TURBO_ERANGE;
    goto unlock;
  }
  replacement = *entry;
  replacement.value = NULL;
  replacement.revision += 1u;
  replacement.present = 0;
  old_value = entry->value;
  rc = turbo_hash_map_put(&store->entries, &key, &replacement);
  if (rc != TURBO_OK) {
    old_value = NULL;
    goto unlock;
  }
  store->current_bytes -= mem_buffer_used(old_value);
  store->present_count -= 1u;

unlock:
  turbo_mutex_unlock(&store->mutex);
  mem_buffer_release(old_value);
  return rc;
}

static int flow_keyed_validate_snapshot(const turbo_flow_keyed_state_t *state) {
  vstr key = flow_keyed_buffer_view(state->key);
  flow_keyed_state_entry_t *entry;
  int rc;

  turbo_mutex_lock(&state->store->mutex);
  rc = flow_keyed_event_time_gate_locked(state);
  if (rc == TURBO_OK) {
    entry = (flow_keyed_state_entry_t *)turbo_hash_map_get(&state->store->entries, &key);
    rc = flow_keyed_conflict(state, entry) ? TURBO_EBUSY : TURBO_OK;
  }
  turbo_mutex_unlock(&state->store->mutex);
  return rc;
}

static int flow_keyed_commit(turbo_flow_keyed_state_t *state) {
  switch (state->mutation) {
  case FLOW_KEYED_STATE_MUTATION_NONE:
    return flow_keyed_validate_snapshot(state);
  case FLOW_KEYED_STATE_MUTATION_PUT:
    return flow_keyed_commit_put(state);
  case FLOW_KEYED_STATE_MUTATION_DELETE:
    return flow_keyed_commit_delete(state);
  default:
    return TURBO_EINVAL;
  }
}

static int flow_keyed_state_open(turbo_flow_keyed_state_store_t *store,
                                 turbo_flow_key_selector_fn key_selector, void *key_ctx,
                                 const turbo_flow_msg_t *message,
                                 turbo_flow_keyed_state_t *state) {
  vstr selected = vstr_from_buf(NULL, 0u);
  int rc;

  if (!store || !store->initialized || !key_selector || !message || !state) return TURBO_EINVAL;
  rc = key_selector(message, &selected, key_ctx);
  if (rc != TURBO_OK) return rc;
  if (!selected.data || selected.len == 0u) return TURBO_EINVAL;
  if (selected.len > store->max_key_size) return TURBO_ENOSPC;

  memset(state, 0, sizeof(*state));
  state->store = store;
  state->status = TURBO_OK;
  state->key = flow_keyed_copy(selected);
  if (!state->key) return TURBO_ENOMEM;
  rc = flow_keyed_snapshot(state);
  if (rc == TURBO_OK) state->active = 1;
  else mem_buffer_release(state->key);
  return rc;
}

static int flow_keyed_state_close(turbo_flow_keyed_state_t *state, int callback_status) {
  int rc = callback_status;
  if (!state || !state->active) return TURBO_EBUSY;
  if (rc == TURBO_OK && state->status != TURBO_OK) rc = state->status;
  if (rc == TURBO_OK) rc = flow_keyed_commit(state);
  state->active = 0;
  mem_buffer_release(state->pending);
  mem_buffer_release(state->snapshot);
  mem_buffer_release(state->key);
  return rc;
}

int flow_keyed_state_execute(turbo_flow_keyed_state_store_t *store,
                             turbo_flow_key_selector_fn key_selector, void *key_ctx,
                             turbo_flow_keyed_stage_fn fn, void *ctx,
                             turbo_flow_msg_t *message) {
  turbo_flow_keyed_state_t state;
  int rc;

  if (!fn || !message) return TURBO_EINVAL;
  rc = flow_keyed_state_open(store, key_selector, key_ctx, message, &state);
  if (rc != TURBO_OK) return rc;
  return flow_keyed_state_close(&state, fn(message, &state, ctx));
}

int flow_keyed_state_execute_emitting(turbo_flow_keyed_state_store_t *store,
                                      turbo_flow_key_selector_fn key_selector, void *key_ctx,
                                      turbo_flow_keyed_emitting_stage_fn fn, void *ctx,
                                      const turbo_flow_msg_t *message,
                                      turbo_flow_emitter_t *emitter) {
  turbo_flow_keyed_state_t state;
  int rc;

  if (!fn || !message || !emitter) return TURBO_EINVAL;
  rc = flow_keyed_state_open(store, key_selector, key_ctx, message, &state);
  if (rc != TURBO_OK) return rc;
  rc = fn(message, &state, emitter, ctx);
  if (rc == TURBO_OK && emitter->status != TURBO_OK) rc = emitter->status;
  return flow_keyed_state_close(&state, rc);
}

typedef struct flow_event_time_candidate_s {
  mem_buffer_t *key;
  mem_buffer_t *value;
  uint64_t revision;
  uint64_t start_ns;
} flow_event_time_candidate_t;

static int flow_event_time_bounds(const turbo_flow_keyed_state_store_t *store,
                                  uint64_t timestamp_ns, uint64_t *start_ns,
                                  uint64_t *end_ns, uint64_t *close_at_ns) {
  uint64_t start;
  uint64_t end;

  if (!store || !store->event_time || !start_ns || !end_ns || !close_at_ns) {
    return TURBO_EINVAL;
  }
  start = (timestamp_ns / store->window_size_ns) * store->window_size_ns;
  if (start > UINT64_MAX - store->window_size_ns) return TURBO_ERANGE;
  end = start + store->window_size_ns;
  *start_ns = start;
  *end_ns = end;
  *close_at_ns = end > UINT64_MAX - store->allowed_lateness_ns
                     ? UINT64_MAX
                     : end + store->allowed_lateness_ns;
  return TURBO_OK;
}

static mem_buffer_t *flow_event_time_key_copy(uint64_t start_ns, vstr application_key) {
  mem_buffer_t *key;
  size_t size;

  if (!application_key.data || application_key.len == 0u ||
      application_key.len > SIZE_MAX - FLOW_EVENT_TIME_KEY_PREFIX_SIZE) {
    return NULL;
  }
  size = FLOW_EVENT_TIME_KEY_PREFIX_SIZE + application_key.len;
  key = mem_get_buffer(mem_global(), size);
  if (!key) return NULL;
  memcpy(mem_buffer_data(key), &start_ns, FLOW_EVENT_TIME_KEY_PREFIX_SIZE);
  memcpy((uint8_t *)mem_buffer_data(key) + FLOW_EVENT_TIME_KEY_PREFIX_SIZE,
         application_key.data, application_key.len);
  key->used = size;
  return key;
}

int flow_event_time_window_execute(turbo_flow_event_time_window_store_t *store,
                                   turbo_flow_key_selector_fn key_selector, void *key_ctx,
                                   turbo_flow_event_time_window_stage_fn fn, void *ctx,
                                   const turbo_flow_msg_t *message) {
  turbo_flow_keyed_state_t state;
  turbo_flow_event_time_window_t window = TURBO_FLOW_EVENT_TIME_WINDOW_INIT;
  vstr selected = vstr_from_buf(NULL, 0u);
  uint64_t close_at_ns;
  int rc;

  if (!store || !store->initialized || !store->event_time || !key_selector || !fn || !message) {
    return TURBO_EINVAL;
  }
  rc = key_selector(message, &selected, key_ctx);
  if (rc != TURBO_OK) return rc;
  if (!selected.data || selected.len == 0u) return TURBO_EINVAL;
  if (selected.len > store->application_key_size) return TURBO_ENOSPC;
  rc = flow_event_time_bounds(store, message->ts_ns, &window.start_ns, &window.end_ns,
                              &close_at_ns);
  if (rc != TURBO_OK) return rc;

  turbo_mutex_lock(&store->mutex);
  rc = store->watermark_initialized && store->watermark_ns >= close_at_ns
           ? TURBO_ETIMEDOUT
           : TURBO_OK;
  turbo_mutex_unlock(&store->mutex);
  if (rc != TURBO_OK) return rc;

  memset(&state, 0, sizeof(state));
  state.store = store;
  state.status = TURBO_OK;
  state.key_prefix_size = FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  state.close_at_ns = close_at_ns;
  state.event_time = 1;
  state.key = flow_event_time_key_copy(window.start_ns, selected);
  if (!state.key) return TURBO_ENOMEM;
  rc = flow_keyed_snapshot(&state);
  if (rc != TURBO_OK) {
    mem_buffer_release(state.key);
    return rc;
  }
  state.active = 1;
  window.key = selected;
  window.aggregate = state.present ? flow_keyed_buffer_view(state.snapshot)
                                   : vstr_from_buf(NULL, 0u);
  return flow_keyed_state_close(&state, fn(message, &window, &state, ctx));
}

static int flow_event_time_candidate_compare(const void *left, const void *right) {
  const flow_event_time_candidate_t *lhs = (const flow_event_time_candidate_t *)left;
  const flow_event_time_candidate_t *rhs = (const flow_event_time_candidate_t *)right;
  vstr lhs_key;
  vstr rhs_key;
  size_t common;
  int compared;

  if (lhs->start_ns < rhs->start_ns) return -1;
  if (lhs->start_ns > rhs->start_ns) return 1;
  lhs_key = flow_keyed_buffer_view(lhs->key);
  rhs_key = flow_keyed_buffer_view(rhs->key);
  lhs_key.data += FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  lhs_key.len -= FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  rhs_key.data += FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  rhs_key.len -= FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  common = lhs_key.len < rhs_key.len ? lhs_key.len : rhs_key.len;
  compared = memcmp(lhs_key.data, rhs_key.data, common);
  if (compared != 0) return compared;
  return lhs_key.len < rhs_key.len ? -1 : lhs_key.len > rhs_key.len ? 1 : 0;
}

static void flow_event_time_candidates_cleanup(turbo_vec_t *candidates) {
  for (size_t index = 0u; index < turbo_vec_size(candidates); ++index) {
    flow_event_time_candidate_t *candidate =
        (flow_event_time_candidate_t *)turbo_vec_at(candidates, index);
    if (!candidate) continue;
    mem_buffer_release(candidate->value);
    mem_buffer_release(candidate->key);
  }
  turbo_vec_destroy(candidates);
}

static int flow_event_time_close_candidate(
    turbo_flow_t *flow, uint32_t stage_index, turbo_flow_keyed_state_store_t *store,
    turbo_flow_event_time_window_close_fn close_fn, void *ctx, uint32_t max_outputs,
    flow_event_time_candidate_t *candidate, int *committed) {
  turbo_flow_event_time_window_t window = TURBO_FLOW_EVENT_TIME_WINDOW_INIT;
  turbo_flow_keyed_state_t state;
  turbo_flow_emitter_t emitter;
  vstr full_key;
  uint64_t close_at_ns;
  int rc;

  if (committed) *committed = 0;
  memset(&emitter, 0, sizeof(emitter));
  rc = flow_event_time_bounds(store, candidate->start_ns, &window.start_ns, &window.end_ns,
                              &close_at_ns);
  if (rc != TURBO_OK) return rc;
  (void)close_at_ns;
  full_key = flow_keyed_buffer_view(candidate->key);
  if (full_key.len <= FLOW_EVENT_TIME_KEY_PREFIX_SIZE) return TURBO_EPROTO;
  window.key = vstr_from_buf(full_key.data + FLOW_EVENT_TIME_KEY_PREFIX_SIZE,
                               full_key.len - FLOW_EVENT_TIME_KEY_PREFIX_SIZE);
  window.aggregate = flow_keyed_buffer_view(candidate->value);

  rc = flow_emitter_init(&emitter, max_outputs);
  if (rc != TURBO_OK) return rc;
  rc = close_fn(&window, &emitter, ctx);
  if (rc == TURBO_OK && emitter.status != TURBO_OK) rc = emitter.status;

  memset(&state, 0, sizeof(state));
  state.store = store;
  state.key = candidate->key;
  state.snapshot = candidate->value;
  state.revision = candidate->revision;
  state.had_slot = 1;
  state.present = 1;
  state.active = 1;
  state.status = TURBO_OK;
  state.key_prefix_size = FLOW_EVENT_TIME_KEY_PREFIX_SIZE;
  state.mutation = FLOW_KEYED_STATE_MUTATION_DELETE;
  candidate->key = NULL;
  candidate->value = NULL;
  rc = flow_keyed_state_close(&state, rc);
  flow_emitter_close(&emitter);
  if (rc == TURBO_OK) {
    if (committed) *committed = 1;
    for (size_t index = 0u; index < turbo_vec_size(&emitter.outputs); ++index) {
      turbo_flow_msg_t *output = (turbo_flow_msg_t *)turbo_vec_at(&emitter.outputs, index);
      rc = flow_run_message_from_stage(flow, stage_index, output);
      if (rc != TURBO_OK) break;
    }
  }
  flow_emitter_cleanup(&emitter);
  return rc;
}

int flow_event_time_window_advance(turbo_flow_t *flow, uint32_t stage_index,
                                   turbo_flow_event_time_window_store_t *store,
                                   turbo_flow_event_time_window_close_fn close_fn, void *ctx,
                                   uint32_t max_outputs, uint64_t watermark_ns,
                                   size_t *closed_windows) {
  turbo_vec_t candidates;
  int candidates_initialized = 0;
  int advancing = 0;
  int rc;

  if (closed_windows) *closed_windows = 0u;
  if (!flow || !store || !store->initialized || !store->event_time || !close_fn ||
      max_outputs == 0u || max_outputs > TURBO_FLOW_EMITTER_MAX_OUTPUTS) {
    return TURBO_EINVAL;
  }
  turbo_mutex_lock(&store->mutex);
  if (store->watermark_advancing) rc = TURBO_EBUSY;
  else if (store->watermark_initialized && watermark_ns < store->watermark_ns) rc = TURBO_EINVAL;
  else {
    store->watermark_advancing = 1;
    advancing = 1;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&store->mutex);
  if (rc != TURBO_OK) return rc;

  rc = turbo_vec_init(&candidates, sizeof(flow_event_time_candidate_t));
  if (rc != TURBO_OK) goto cleanup;
  candidates_initialized = 1;
  rc = turbo_vec_reserve(&candidates, store->max_entries);
  if (rc != TURBO_OK) goto cleanup;

  turbo_mutex_lock(&store->mutex);
  store->watermark_ns = watermark_ns;
  store->watermark_initialized = 1;
  for (size_t slot = 0u; slot < turbo_hash_map_capacity(&store->entries); ++slot) {
    flow_keyed_state_entry_t *entry =
        (flow_keyed_state_entry_t *)turbo_hash_map_value_at(&store->entries, slot);
    flow_event_time_candidate_t candidate;
    vstr key;
    uint64_t start_ns;
    uint64_t end_ns;
    uint64_t close_at_ns;

    if (!entry || !entry->present) continue;
    key = flow_keyed_buffer_view(entry->key);
    if (key.len <= FLOW_EVENT_TIME_KEY_PREFIX_SIZE) {
      rc = TURBO_EPROTO;
      break;
    }
    memcpy(&start_ns, key.data, FLOW_EVENT_TIME_KEY_PREFIX_SIZE);
    rc = flow_event_time_bounds(store, start_ns, &start_ns, &end_ns, &close_at_ns);
    if (rc != TURBO_OK) break;
    if (close_at_ns > watermark_ns) continue;
    memset(&candidate, 0, sizeof(candidate));
    candidate.key = mem_buffer_retain(entry->key);
    candidate.value = mem_buffer_retain(entry->value);
    candidate.revision = entry->revision;
    candidate.start_ns = start_ns;
    if (!candidate.key || !candidate.value ||
        turbo_vec_push(&candidates, &candidate) != TURBO_OK) {
      mem_buffer_release(candidate.value);
      mem_buffer_release(candidate.key);
      rc = TURBO_ENOMEM;
      break;
    }
  }
  turbo_mutex_unlock(&store->mutex);
  if (rc != TURBO_OK) goto cleanup;

  if (turbo_vec_size(&candidates) > 1u) {
    qsort(turbo_vec_data(&candidates), turbo_vec_size(&candidates),
          sizeof(flow_event_time_candidate_t), flow_event_time_candidate_compare);
  }
  for (size_t index = 0u; index < turbo_vec_size(&candidates); ++index) {
    flow_event_time_candidate_t *candidate =
        (flow_event_time_candidate_t *)turbo_vec_at(&candidates, index);
    int committed = 0;
    rc = flow_event_time_close_candidate(flow, stage_index, store, close_fn, ctx, max_outputs,
                                         candidate, &committed);
    if (committed && closed_windows) *closed_windows += 1u;
    if (rc != TURBO_OK) break;
  }

cleanup:
  if (candidates_initialized) flow_event_time_candidates_cleanup(&candidates);
  if (advancing) {
    turbo_mutex_lock(&store->mutex);
    store->watermark_advancing = 0;
    turbo_mutex_unlock(&store->mutex);
  }
  return rc;
}
