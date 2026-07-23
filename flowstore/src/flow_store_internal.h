#ifndef TURBO_FLOW_STORE_INTERNAL_H
#define TURBO_FLOW_STORE_INTERNAL_H

#include "turbo_buffer.h"
#include "turbo_flow_store.h"
#include "turbo_hash.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct flow_store_key_s {
  const uint8_t *data;
  size_t size;
} flow_store_key_t;

int flow_store_bytes_validate(turbo_flow_store_bytes_t bytes, int allow_empty);
int flow_store_size_add(size_t left, size_t right, size_t *out);
int flow_store_buffer_copy(turbo_flow_store_bytes_t source, mem_buffer_t **out);
flow_store_key_t flow_store_key_from_bytes(turbo_flow_store_bytes_t bytes);
flow_store_key_t flow_store_key_from_buffer(const mem_buffer_t *buffer);
size_t flow_store_key_hash(const void *key, size_t key_size, void *ctx);
bool flow_store_key_equal(const void *left, const void *right, size_t key_size, void *ctx);
int flow_store_stats_write(turbo_flow_store_stats_t *stats, size_t records, size_t bytes);
int flow_store_stats_copy(const turbo_flow_store_stats_t *source, turbo_flow_store_stats_t *out);

#endif
