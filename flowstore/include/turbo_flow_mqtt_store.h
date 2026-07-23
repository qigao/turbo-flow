#ifndef TURBO_FLOW_MQTT_STORE_H
#define TURBO_FLOW_MQTT_STORE_H

#include "turbo_flow_record_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_MQTT_STORE_ABI_VERSION 1u

typedef struct turbo_flow_mqtt_store_s turbo_flow_mqtt_store_t;

/**
 * Flowie-facing fact store. The record backend is borrowed and remains owned by
 * the StorageBackend registry; callers never access its callbacks directly.
 */
CXX_C_API int turbo_flow_mqtt_store_create(turbo_flow_record_store_t *backend,
                                            turbo_flow_mqtt_store_t **out);
CXX_C_API void turbo_flow_mqtt_store_destroy(turbo_flow_mqtt_store_t *store);
CXX_C_API int turbo_flow_mqtt_store_scan(turbo_flow_mqtt_store_t *store,
                                         turbo_flow_record_visit_fn visit, void *ctx);
CXX_C_API int turbo_flow_mqtt_store_commit(turbo_flow_mqtt_store_t *store,
                                           const turbo_flow_record_mutation_t *mutations,
                                           size_t mutation_count);
CXX_C_API uint32_t turbo_flow_mqtt_store_capabilities(const turbo_flow_mqtt_store_t *store);
CXX_C_API size_t turbo_flow_mqtt_store_max_key_size(const turbo_flow_mqtt_store_t *store);
CXX_C_API size_t turbo_flow_mqtt_store_max_value_size(const turbo_flow_mqtt_store_t *store);
CXX_C_API size_t turbo_flow_mqtt_store_max_batch_size(const turbo_flow_mqtt_store_t *store);
CXX_C_API size_t turbo_flow_mqtt_store_max_records(const turbo_flow_mqtt_store_t *store);

#ifdef __cplusplus
}
#endif

#endif
