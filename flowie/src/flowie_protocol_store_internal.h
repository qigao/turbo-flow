#ifndef FLOWIE_PROTOCOL_STORE_INTERNAL_H
#define FLOWIE_PROTOCOL_STORE_INTERNAL_H

#include "turbo_flow_record_store.h"

typedef struct flowie_protocol_store_s flowie_protocol_store_t;

int flowie_protocol_store_create(turbo_flow_record_store_t *backend, flowie_protocol_store_t **out);
void flowie_protocol_store_destroy(flowie_protocol_store_t *store);
int flowie_protocol_store_scan(flowie_protocol_store_t *store, turbo_flow_record_visit_fn visit,
                               void *ctx);
int flowie_protocol_store_commit(flowie_protocol_store_t *store,
                                 const turbo_flow_record_mutation_t *mutations,
                                 size_t mutation_count);
uint32_t flowie_protocol_store_capabilities(const flowie_protocol_store_t *store);
size_t flowie_protocol_store_max_key_size(const flowie_protocol_store_t *store);
size_t flowie_protocol_store_max_value_size(const flowie_protocol_store_t *store);
size_t flowie_protocol_store_max_batch_size(const flowie_protocol_store_t *store);
size_t flowie_protocol_store_max_records(const flowie_protocol_store_t *store);

#endif
