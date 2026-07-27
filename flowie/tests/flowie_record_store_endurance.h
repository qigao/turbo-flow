#ifndef FLOWIE_RECORD_STORE_ENDURANCE_H
#define FLOWIE_RECORD_STORE_ENDURANCE_H

#include "turbo_error.h"
#include "turbo_flow.h"

#include <stdint.h>
#include <string.h>

#define FLOWIE_RECORD_ENDURANCE_RECORDS 16u
#define FLOWIE_RECORD_ENDURANCE_BATCH_SIZE 4u
#define FLOWIE_RECORD_ENDURANCE_ROUNDS 32u
#define FLOWIE_RECORD_ENDURANCE_KEY_SIZE 8u
#define FLOWIE_RECORD_ENDURANCE_VALUE_SIZE 16u

typedef struct flowie_record_store_endurance_result_s {
  size_t successful_commits;
  size_t scans;
  size_t conflicts;
  size_t final_count;
  int durable;
} flowie_record_store_endurance_result_t;

typedef struct flowie_record_store_endurance_capture_s {
  const uint64_t *revisions;
  uint32_t seen;
  size_t count;
} flowie_record_store_endurance_capture_t;

static inline void flowie_record_store_endurance_u32(uint8_t *output, uint32_t value) {
  output[0] = (uint8_t)(value >> 24u);
  output[1] = (uint8_t)(value >> 16u);
  output[2] = (uint8_t)(value >> 8u);
  output[3] = (uint8_t)value;
}

static inline uint32_t flowie_record_store_endurance_load_u32(const uint8_t *input) {
  return ((uint32_t)input[0] << 24u) | ((uint32_t)input[1] << 16u) | ((uint32_t)input[2] << 8u) |
         (uint32_t)input[3];
}

static inline void flowie_record_store_endurance_key(uint8_t *key, size_t index) {
  key[0] = 0u;
  key[1] = (uint8_t)'e';
  key[2] = (uint8_t)'n';
  key[3] = (uint8_t)'d';
  flowie_record_store_endurance_u32(key + 4u, (uint32_t)index);
}

static inline void flowie_record_store_endurance_value(uint8_t *value, size_t index,
                                                       uint64_t revision) {
  value[0] = (uint8_t)'m';
  value[1] = (uint8_t)'q';
  value[2] = (uint8_t)'t';
  value[3] = 0u;
  flowie_record_store_endurance_u32(value + 4u, (uint32_t)index);
  flowie_record_store_endurance_u32(value + 8u, (uint32_t)(revision >> 32u));
  flowie_record_store_endurance_u32(value + 12u, (uint32_t)revision);
}

static inline int flowie_record_store_endurance_visit(void *ctx,
                                                      const turbo_flow_record_view_t *record) {
  flowie_record_store_endurance_capture_t *capture = (flowie_record_store_endurance_capture_t *)ctx;
  uint8_t expected_value[FLOWIE_RECORD_ENDURANCE_VALUE_SIZE];
  uint32_t bit;
  size_t index;
  if (!capture || !capture->revisions || !record || record->size < sizeof(*record) ||
      !record->key || record->key_size != FLOWIE_RECORD_ENDURANCE_KEY_SIZE || !record->value ||
      record->value_size != FLOWIE_RECORD_ENDURANCE_VALUE_SIZE || record->key[0] != 0u ||
      record->key[1] != (uint8_t)'e' || record->key[2] != (uint8_t)'n' ||
      record->key[3] != (uint8_t)'d')
    return TURBO_EPROTO;
  index = flowie_record_store_endurance_load_u32(record->key + 4u);
  if (index >= FLOWIE_RECORD_ENDURANCE_RECORDS || record->revision != capture->revisions[index])
    return TURBO_EPROTO;
  bit = UINT32_C(1) << index;
  if ((capture->seen & bit) != 0u) return TURBO_EPROTO;
  flowie_record_store_endurance_value(expected_value, index, record->revision);
  if (memcmp(record->value, expected_value, sizeof(expected_value)) != 0) return TURBO_EPROTO;
  capture->seen |= bit;
  ++capture->count;
  return TURBO_OK;
}

static inline int
flowie_record_store_endurance_scan(turbo_flow_record_store_t *store, const uint64_t *revisions,
                                   size_t expected_count,
                                   flowie_record_store_endurance_result_t *result) {
  flowie_record_store_endurance_capture_t capture = {revisions, 0u, 0u};
  int rc = store->scan(store->ctx, flowie_record_store_endurance_visit, &capture);
  ++result->scans;
  if (rc != TURBO_OK) return rc;
  return capture.count == expected_count ? TURBO_OK : TURBO_EPROTO;
}

static inline int
flowie_record_store_endurance_run(turbo_flow_record_store_t *store,
                                  flowie_record_store_endurance_result_t *result) {
  uint8_t keys[FLOWIE_RECORD_ENDURANCE_BATCH_SIZE][FLOWIE_RECORD_ENDURANCE_KEY_SIZE];
  uint8_t values[FLOWIE_RECORD_ENDURANCE_BATCH_SIZE][FLOWIE_RECORD_ENDURANCE_VALUE_SIZE];
  uint64_t revisions[FLOWIE_RECORD_ENDURANCE_RECORDS] = {0};
  turbo_flow_record_mutation_t mutations[FLOWIE_RECORD_ENDURANCE_BATCH_SIZE];
  int rc;
  if (!store || !result || !store->scan || !store->commit ||
      !(store->capabilities & TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH) ||
      store->max_key_size < FLOWIE_RECORD_ENDURANCE_KEY_SIZE ||
      store->max_value_size < FLOWIE_RECORD_ENDURANCE_VALUE_SIZE ||
      store->max_batch_size < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE ||
      store->max_records < FLOWIE_RECORD_ENDURANCE_RECORDS)
    return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  result->durable = (store->capabilities & TURBO_FLOW_RECORD_STORE_DURABLE) != 0u;
  rc = flowie_record_store_endurance_scan(store, revisions, 0u, result);
  if (rc != TURBO_OK) return rc;

  for (size_t base = 0u; base < FLOWIE_RECORD_ENDURANCE_RECORDS;
       base += FLOWIE_RECORD_ENDURANCE_BATCH_SIZE) {
    for (size_t offset = 0u; offset < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE; ++offset) {
      const size_t index = base + offset;
      mutations[offset] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
      flowie_record_store_endurance_key(keys[offset], index);
      flowie_record_store_endurance_value(values[offset], index, 1u);
      mutations[offset].kind = TURBO_FLOW_RECORD_PUT;
      mutations[offset].key = keys[offset];
      mutations[offset].key_size = sizeof(keys[offset]);
      mutations[offset].expected_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
      mutations[offset].next_revision = 1u;
      mutations[offset].value = values[offset];
      mutations[offset].value_size = sizeof(values[offset]);
    }
    rc = store->commit(store->ctx, mutations, FLOWIE_RECORD_ENDURANCE_BATCH_SIZE);
    if (rc != TURBO_OK) return rc;
    ++result->successful_commits;
    for (size_t offset = 0u; offset < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE; ++offset)
      revisions[base + offset] = 1u;
  }
  rc =
      flowie_record_store_endurance_scan(store, revisions, FLOWIE_RECORD_ENDURANCE_RECORDS, result);
  if (rc != TURBO_OK) return rc;

  for (size_t round = 0u; round < FLOWIE_RECORD_ENDURANCE_ROUNDS; ++round) {
    const size_t base =
        (round * FLOWIE_RECORD_ENDURANCE_BATCH_SIZE) % FLOWIE_RECORD_ENDURANCE_RECORDS;
    for (size_t offset = 0u; offset < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE; ++offset) {
      const size_t index = base + offset;
      const uint64_t next_revision = revisions[index] + 1u;
      mutations[offset] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
      flowie_record_store_endurance_key(keys[offset], index);
      flowie_record_store_endurance_value(values[offset], index, next_revision);
      mutations[offset].kind = TURBO_FLOW_RECORD_PUT;
      mutations[offset].key = keys[offset];
      mutations[offset].key_size = sizeof(keys[offset]);
      mutations[offset].expected_revision = revisions[index];
      mutations[offset].next_revision = next_revision;
      mutations[offset].value = values[offset];
      mutations[offset].value_size = sizeof(values[offset]);
    }
    rc = store->commit(store->ctx, mutations, FLOWIE_RECORD_ENDURANCE_BATCH_SIZE);
    if (rc != TURBO_OK) return rc;
    ++result->successful_commits;
    for (size_t offset = 0u; offset < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE; ++offset)
      ++revisions[base + offset];
    rc = flowie_record_store_endurance_scan(store, revisions, FLOWIE_RECORD_ENDURANCE_RECORDS,
                                            result);
    if (rc != TURBO_OK) return rc;

    if ((round + 1u) % 8u == 0u) {
      const size_t first_index = base;
      const size_t stale_index = base + 1u;
      for (size_t offset = 0u; offset < 2u; ++offset) {
        const size_t index = offset == 0u ? first_index : stale_index;
        const uint64_t next_revision = revisions[index] + 1u;
        mutations[offset] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
        flowie_record_store_endurance_key(keys[offset], index);
        flowie_record_store_endurance_value(values[offset], index, next_revision);
        mutations[offset].kind = TURBO_FLOW_RECORD_PUT;
        mutations[offset].key = keys[offset];
        mutations[offset].key_size = sizeof(keys[offset]);
        mutations[offset].expected_revision =
            offset == 0u ? revisions[index] : revisions[index] - 1u;
        mutations[offset].next_revision = next_revision;
        mutations[offset].value = values[offset];
        mutations[offset].value_size = sizeof(values[offset]);
      }
      rc = store->commit(store->ctx, mutations, 2u);
      if (rc != TURBO_EBUSY) return rc == TURBO_OK ? TURBO_EPROTO : rc;
      ++result->conflicts;
      rc = flowie_record_store_endurance_scan(store, revisions, FLOWIE_RECORD_ENDURANCE_RECORDS,
                                              result);
      if (rc != TURBO_OK) return rc;
    }
  }

  for (size_t base = 0u; base < FLOWIE_RECORD_ENDURANCE_RECORDS;
       base += FLOWIE_RECORD_ENDURANCE_BATCH_SIZE) {
    for (size_t offset = 0u; offset < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE; ++offset) {
      const size_t index = base + offset;
      mutations[offset] = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
      flowie_record_store_endurance_key(keys[offset], index);
      mutations[offset].kind = TURBO_FLOW_RECORD_DELETE;
      mutations[offset].key = keys[offset];
      mutations[offset].key_size = sizeof(keys[offset]);
      mutations[offset].expected_revision = revisions[index];
    }
    rc = store->commit(store->ctx, mutations, FLOWIE_RECORD_ENDURANCE_BATCH_SIZE);
    if (rc != TURBO_OK) return rc;
    ++result->successful_commits;
    for (size_t offset = 0u; offset < FLOWIE_RECORD_ENDURANCE_BATCH_SIZE; ++offset)
      revisions[base + offset] = TURBO_FLOW_RECORD_REVISION_ABSENT;
  }
  rc = flowie_record_store_endurance_scan(store, revisions, 0u, result);
  if (rc != TURBO_OK) return rc;
  result->final_count = 0u;
  return TURBO_OK;
}

#endif
