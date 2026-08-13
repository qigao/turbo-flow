#include "turbo_flow_store.h"

#include <stdlib.h>
#include <string.h>

struct turbo_flow_store_s {
  turbo_flow_record_store_t *backend;
};

typedef struct flow_store_get_context_s {
  const uint8_t *key;
  size_t key_size;
  turbo_flow_store_record_t *out;
  int found;
} flow_store_get_context_t;

static int flow_store_get_visit(void *ctx, const turbo_flow_record_view_t *source) {
  flow_store_get_context_t *get = (flow_store_get_context_t *)ctx;
  uint8_t *value = NULL;
  if (!get || !source || !source->key || source->key_size != get->key_size ||
      memcmp(source->key, get->key, get->key_size) != 0)
    return get ? TURBO_OK : TURBO_EINVAL;
  if (get->found || (source->value_size != 0u && !source->value)) return TURBO_EPROTO;
  if (source->value_size != 0u) {
    value = (uint8_t *)malloc(source->value_size);
    if (!value) return TURBO_ENOMEM;
    memcpy(value, source->value, source->value_size);
  }
  get->out->revision = source->revision;
  get->out->value = value;
  get->out->value_size = source->value_size;
  get->found = 1;
  return TURBO_OK;
}

static int flow_store_backend_valid(const turbo_flow_record_store_t *backend) {
  return backend && backend->size >= sizeof(*backend) &&
         backend->api_version == TURBO_FLOW_RECORD_STORE_API_VERSION && backend->ctx &&
         backend->scan && backend->commit &&
         (backend->capabilities & TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH) &&
         backend->max_key_size != 0u && backend->max_value_size != 0u &&
         backend->max_batch_size != 0u;
}

int turbo_flow_store_create(const turbo_flow_store_config_t *config, turbo_flow_store_t **out) {
  turbo_flow_store_t *store;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_STORE_ABI_VERSION || !flow_store_backend_valid(config->backend))
    return TURBO_EINVAL;
  store = (turbo_flow_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->backend = config->backend;
  *out = store;
  return TURBO_OK;
}

void turbo_flow_store_destroy(turbo_flow_store_t *store) { free(store); }

void turbo_flow_store_record_clear(turbo_flow_store_record_t *record) {
  if (!record) return;
  free(record->value);
  *record = (turbo_flow_store_record_t)TURBO_FLOW_STORE_RECORD_INIT;
}

int turbo_flow_store_commit(turbo_flow_store_t *store,
                            const turbo_flow_record_mutation_t *mutations,
                            size_t mutation_count) {
  if (!store || !mutations || mutation_count == 0u ||
      mutation_count > store->backend->max_batch_size)
    return TURBO_EINVAL;
  return store->backend->commit(store->backend->ctx, mutations, mutation_count);
}

int turbo_flow_store_put(turbo_flow_store_t *store, const uint8_t *key, size_t key_size,
                         uint64_t expected_revision, uint64_t next_revision,
                         const uint8_t *value, size_t value_size) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  if (!store || !key || key_size == 0u || key_size > store->backend->max_key_size ||
      (value_size != 0u && !value) || value_size > store->backend->max_value_size ||
      next_revision <= expected_revision || next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EINVAL;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.expected_revision = expected_revision;
  mutation.next_revision = next_revision;
  mutation.value = value;
  mutation.value_size = value_size;
  return turbo_flow_store_commit(store, &mutation, 1u);
}

int turbo_flow_store_get(turbo_flow_store_t *store, const uint8_t *key, size_t key_size,
                         turbo_flow_store_record_t *out) {
  flow_store_get_context_t context;
  int rc;
  if (!store || !key || key_size == 0u || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  turbo_flow_store_record_clear(out);
  context.key = key;
  context.key_size = key_size;
  context.out = out;
  context.found = 0;
  rc = turbo_flow_store_scan(store, flow_store_get_visit, &context);
  if (rc != TURBO_OK) {
    turbo_flow_store_record_clear(out);
    return rc;
  }
  return context.found ? TURBO_OK : TURBO_ENOENT;
}

int turbo_flow_store_delete(turbo_flow_store_t *store, const uint8_t *key, size_t key_size,
                            uint64_t expected_revision) {
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  if (!store || !key || key_size == 0u || key_size > store->backend->max_key_size ||
      expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT ||
      expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EINVAL;
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = key;
  mutation.key_size = key_size;
  mutation.expected_revision = expected_revision;
  return turbo_flow_store_commit(store, &mutation, 1u);
}

int turbo_flow_store_scan(turbo_flow_store_t *store, turbo_flow_record_visit_fn visit, void *ctx) {
  if (!store || !visit) return TURBO_EINVAL;
  return store->backend->scan(store->backend->ctx, visit, ctx);
}
