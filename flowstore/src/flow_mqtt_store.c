#include "turbo_flow_mqtt_store.h"

#include "turbo_error.h"

#include <stdlib.h>

struct turbo_flow_mqtt_store_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_record_store_t *backend;
};

static int flow_mqtt_store_valid(const turbo_flow_mqtt_store_t *store) {
  return store && store->size >= sizeof(*store) &&
         store->abi_version == TURBO_FLOW_MQTT_STORE_ABI_VERSION && store->backend &&
         store->backend->size >= sizeof(*store->backend) &&
         store->backend->api_version == TURBO_FLOW_RECORD_STORE_API_VERSION &&
         store->backend->ctx && store->backend->scan && store->backend->commit;
}

int turbo_flow_mqtt_store_create(turbo_flow_record_store_t *backend,
                                 turbo_flow_mqtt_store_t **out) {
  turbo_flow_mqtt_store_t *store;
  if (out) *out = NULL;
  if (!backend || !out || backend->size < sizeof(*backend) ||
      backend->api_version != TURBO_FLOW_RECORD_STORE_API_VERSION || !backend->ctx ||
      !backend->scan || !backend->commit || backend->max_key_size == 0u ||
      backend->max_value_size == 0u || backend->max_batch_size == 0u ||
      backend->max_records == 0u)
    return TURBO_EINVAL;
  store = (turbo_flow_mqtt_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->size = sizeof(*store);
  store->abi_version = TURBO_FLOW_MQTT_STORE_ABI_VERSION;
  store->backend = backend;
  *out = store;
  return TURBO_OK;
}

void turbo_flow_mqtt_store_destroy(turbo_flow_mqtt_store_t *store) { free(store); }

int turbo_flow_mqtt_store_scan(turbo_flow_mqtt_store_t *store,
                               turbo_flow_record_visit_fn visit, void *ctx) {
  if (!flow_mqtt_store_valid(store) || !visit) return TURBO_EINVAL;
  return store->backend->scan(store->backend->ctx, visit, ctx);
}

int turbo_flow_mqtt_store_commit(turbo_flow_mqtt_store_t *store,
                                 const turbo_flow_record_mutation_t *mutations,
                                 size_t mutation_count) {
  if (!flow_mqtt_store_valid(store) || !mutations || mutation_count == 0u ||
      mutation_count > store->backend->max_batch_size)
    return TURBO_EINVAL;
  return store->backend->commit(store->backend->ctx, mutations, mutation_count);
}

uint32_t turbo_flow_mqtt_store_capabilities(const turbo_flow_mqtt_store_t *store) {
  return flow_mqtt_store_valid(store) ? store->backend->capabilities : 0u;
}

size_t turbo_flow_mqtt_store_max_key_size(const turbo_flow_mqtt_store_t *store) {
  return flow_mqtt_store_valid(store) ? store->backend->max_key_size : 0u;
}

size_t turbo_flow_mqtt_store_max_value_size(const turbo_flow_mqtt_store_t *store) {
  return flow_mqtt_store_valid(store) ? store->backend->max_value_size : 0u;
}

size_t turbo_flow_mqtt_store_max_batch_size(const turbo_flow_mqtt_store_t *store) {
  return flow_mqtt_store_valid(store) ? store->backend->max_batch_size : 0u;
}

size_t turbo_flow_mqtt_store_max_records(const turbo_flow_mqtt_store_t *store) {
  return flow_mqtt_store_valid(store) ? store->backend->max_records : 0u;
}
