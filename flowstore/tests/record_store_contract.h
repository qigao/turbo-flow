#ifndef TURBO_FLOW_TEST_RECORD_STORE_CONTRACT_H
#define TURBO_FLOW_TEST_RECORD_STORE_CONTRACT_H

#include "turbo_error.h"
#include "turbo_flow.h"

#include <stdint.h>
#include <string.h>

typedef struct turbo_flow_test_record_store_contract_result_s {
  size_t restored_count;
  uint64_t restored_revision;
  int duplicate_create_status;
  int stale_update_status;
  int stale_delete_status;
  int empty_after_delete;
  size_t binary_wire_size;
  int binary_wire_equal;
} turbo_flow_test_record_store_contract_result_t;

typedef struct turbo_flow_test_record_store_contract_capture_s {
  size_t count;
  int valid;
} turbo_flow_test_record_store_contract_capture_t;

typedef struct turbo_flow_test_record_store_recovery_result_s {
  int timeout_status;
  int lost_reply_status;
  int retry_status;
  uint64_t revision_after_timeout;
  uint64_t revision_after_lost_reply;
  uint64_t revision_after_recovery;
} turbo_flow_test_record_store_recovery_result_t;

typedef struct turbo_flow_test_record_store_fault_s {
  turbo_flow_record_store_t *inner;
  int fail_before_commit;
  int lose_commit_reply;
} turbo_flow_test_record_store_fault_t;

typedef struct turbo_flow_test_record_store_revision_capture_s {
  const uint8_t *expected_key;
  size_t expected_key_size;
  uint64_t revision;
  size_t count;
} turbo_flow_test_record_store_revision_capture_t;

static inline int turbo_flow_test_record_store_fault_scan(void *ctx, turbo_flow_record_visit_fn visit,
                                                 void *visit_ctx) {
  turbo_flow_test_record_store_fault_t *fault = (turbo_flow_test_record_store_fault_t *)ctx;
  return fault && fault->inner && fault->inner->scan
             ? fault->inner->scan(fault->inner->ctx, visit, visit_ctx)
             : TURBO_EINVAL;
}

static inline int turbo_flow_test_record_store_fault_commit(void *ctx,
                                                   const turbo_flow_record_mutation_t *mutations,
                                                   size_t mutation_count) {
  turbo_flow_test_record_store_fault_t *fault = (turbo_flow_test_record_store_fault_t *)ctx;
  int rc;
  if (!fault || !fault->inner || !fault->inner->commit) return TURBO_EINVAL;
  if (fault->fail_before_commit) {
    fault->fail_before_commit = 0;
    return TURBO_ETIMEDOUT;
  }
  rc = fault->inner->commit(fault->inner->ctx, mutations, mutation_count);
  if (rc == TURBO_OK && fault->lose_commit_reply) {
    fault->lose_commit_reply = 0;
    return TURBO_EIO;
  }
  return rc;
}

static inline int turbo_flow_test_record_store_revision_visit(void *ctx,
                                                     const turbo_flow_record_view_t *record) {
  turbo_flow_test_record_store_revision_capture_t *capture =
      (turbo_flow_test_record_store_revision_capture_t *)ctx;
  if (!capture || !record || capture->count != 0u ||
      record->key_size != capture->expected_key_size ||
      memcmp(record->key, capture->expected_key, capture->expected_key_size) != 0)
    return TURBO_EPROTO;
  capture->revision = record->revision;
  ++capture->count;
  return TURBO_OK;
}

static inline int turbo_flow_test_record_store_recovery_revision(turbo_flow_record_store_t *store,
                                                        const uint8_t *key, size_t key_size,
                                                        uint64_t *revision_out) {
  turbo_flow_test_record_store_revision_capture_t capture = {key, key_size, 0u, 0u};
  int rc;
  if (!store || !revision_out) return TURBO_EINVAL;
  rc = store->scan(store->ctx, turbo_flow_test_record_store_revision_visit, &capture);
  if (rc != TURBO_OK) return rc;
  if (capture.count != 1u || capture.revision == 0u) return TURBO_EPROTO;
  *revision_out = capture.revision;
  return TURBO_OK;
}

static inline int turbo_flow_test_record_store_recovery_contract_run(
    turbo_flow_record_store_t *store, turbo_flow_test_record_store_recovery_result_t *result) {
  static const uint8_t key[] = {'r', 0u, 'e', 'c'};
  static const uint8_t values[][4] = {{'v', '1', 0u, 1u},
                                      {'v', '2', 0u, 2u},
                                      {'v', '3', 0u, 3u}};
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  turbo_flow_test_record_store_fault_t fault = {store, 0, 0};
  turbo_flow_record_store_t fault_store = TURBO_FLOW_RECORD_STORE_INIT;
  int rc;
  if (!store || !result || !store->scan || !store->commit || store->max_key_size < sizeof(key) ||
      store->max_value_size < sizeof(values[0]))
    return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  mutation.kind = TURBO_FLOW_RECORD_PUT;
  mutation.key = key;
  mutation.key_size = sizeof(key);
  mutation.expected_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  mutation.next_revision = 1u;
  mutation.value = values[0];
  mutation.value_size = sizeof(values[0]);
  rc = store->commit(store->ctx, &mutation, 1u);
  if (rc != TURBO_OK) return rc;

  fault_store.capabilities = store->capabilities;
  fault_store.max_key_size = store->max_key_size;
  fault_store.max_value_size = store->max_value_size;
  fault_store.max_batch_size = store->max_batch_size;
  fault_store.max_records = store->max_records;
  fault_store.ctx = &fault;
  fault_store.scan = turbo_flow_test_record_store_fault_scan;
  fault_store.commit = turbo_flow_test_record_store_fault_commit;
  mutation.expected_revision = 1u;
  mutation.next_revision = 2u;
  mutation.value = values[1];
  fault.fail_before_commit = 1;
  result->timeout_status = fault_store.commit(fault_store.ctx, &mutation, 1u);
  if (result->timeout_status != TURBO_ETIMEDOUT) return TURBO_EPROTO;
  rc = turbo_flow_test_record_store_recovery_revision(store, key, sizeof(key),
                                             &result->revision_after_timeout);
  if (rc != TURBO_OK || result->revision_after_timeout != 1u)
    return rc == TURBO_OK ? TURBO_EPROTO : rc;

  fault.lose_commit_reply = 1;
  result->lost_reply_status = fault_store.commit(fault_store.ctx, &mutation, 1u);
  if (result->lost_reply_status != TURBO_EIO) return TURBO_EPROTO;
  rc = turbo_flow_test_record_store_recovery_revision(store, key, sizeof(key),
                                             &result->revision_after_lost_reply);
  if (rc != TURBO_OK || result->revision_after_lost_reply != 2u)
    return rc == TURBO_OK ? TURBO_EPROTO : rc;
  result->retry_status = store->commit(store->ctx, &mutation, 1u);
  if (result->retry_status != TURBO_EBUSY) return TURBO_EPROTO;

  mutation.expected_revision = 2u;
  mutation.next_revision = 3u;
  mutation.value = values[2];
  rc = store->commit(store->ctx, &mutation, 1u);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_test_record_store_recovery_revision(store, key, sizeof(key),
                                             &result->revision_after_recovery);
  if (rc != TURBO_OK || result->revision_after_recovery != 3u)
    return rc == TURBO_OK ? TURBO_EPROTO : rc;

  mutation = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = key;
  mutation.key_size = sizeof(key);
  mutation.expected_revision = 3u;
  return store->commit(store->ctx, &mutation, 1u);
}

static inline int turbo_flow_test_record_store_contract_visit(void *ctx,
                                                     const turbo_flow_record_view_t *record) {
  static const uint8_t expected_key[] = {0u, 'm', 'q', 0xffu};
  static const uint8_t expected_value[] = {0x32u, 0x0au, 0x00u, 0x01u, 't', 0x00u,
                                           0x01u, 0x00u, 'v',   0x00u, '2', 0xfeu};
  turbo_flow_test_record_store_contract_capture_t *capture =
      (turbo_flow_test_record_store_contract_capture_t *)ctx;
  if (!capture || !record) return TURBO_EINVAL;
  ++capture->count;
  capture->valid = record->key_size == sizeof(expected_key) &&
                   memcmp(record->key, expected_key, sizeof(expected_key)) == 0 &&
                   record->revision == 2u && record->value_size == sizeof(expected_value) &&
                   memcmp(record->value, expected_value, sizeof(expected_value)) == 0;
  return capture->valid ? TURBO_OK : TURBO_EPROTO;
}

static inline int turbo_flow_test_record_store_contract_count(void *ctx,
                                                     const turbo_flow_record_view_t *record) {
  size_t *count = (size_t *)ctx;
  if (!count || !record) return TURBO_EINVAL;
  ++*count;
  return TURBO_OK;
}

static inline int turbo_flow_test_record_store_contract_run(
    turbo_flow_record_store_t *store, turbo_flow_test_record_store_contract_result_t *result) {
  static const uint8_t key[] = {0u, 'm', 'q', 0xffu};
  static const uint8_t first_value[] = {0x32u, 0x0au, 0x00u, 0x01u, 't', 0x00u,
                                        0x01u, 0x00u, 'v',   0x00u, '1', 0xfdu};
  static const uint8_t second_value[] = {0x32u, 0x0au, 0x00u, 0x01u, 't', 0x00u,
                                         0x01u, 0x00u, 'v',   0x00u, '2', 0xfeu};
  turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
  turbo_flow_test_record_store_contract_capture_t capture = {0};
  size_t count = 0u;
  int rc;
  if (!store || !result || !store->scan || !store->commit ||
      !(store->capabilities & TURBO_FLOW_RECORD_STORE_DURABLE) ||
      !(store->capabilities & TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH) ||
      store->max_key_size < sizeof(key) || store->max_value_size < sizeof(second_value) ||
      store->max_batch_size < 1u || store->max_records < 1u)
    return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  rc = store->scan(store->ctx, turbo_flow_test_record_store_contract_count, &count);
  if (rc != TURBO_OK || count != 0u) return rc == TURBO_OK ? TURBO_EBUSY : rc;

  mutation.kind = TURBO_FLOW_RECORD_PUT;
  mutation.key = key;
  mutation.key_size = sizeof(key);
  mutation.expected_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  mutation.next_revision = 1u;
  mutation.value = first_value;
  mutation.value_size = sizeof(first_value);
  rc = store->commit(store->ctx, &mutation, 1u);
  if (rc != TURBO_OK) return rc;
  result->duplicate_create_status = store->commit(store->ctx, &mutation, 1u);
  if (result->duplicate_create_status != TURBO_EBUSY) return TURBO_EPROTO;

  mutation.expected_revision = 1u;
  mutation.next_revision = 2u;
  mutation.value = second_value;
  mutation.value_size = sizeof(second_value);
  rc = store->commit(store->ctx, &mutation, 1u);
  if (rc != TURBO_OK) return rc;
  result->stale_update_status = store->commit(store->ctx, &mutation, 1u);
  if (result->stale_update_status != TURBO_EBUSY) return TURBO_EPROTO;

  rc = store->scan(store->ctx, turbo_flow_test_record_store_contract_visit, &capture);
  if (rc != TURBO_OK || capture.count != 1u || !capture.valid)
    return rc == TURBO_OK ? TURBO_EPROTO : rc;
  result->restored_count = capture.count;
  result->restored_revision = 2u;
  result->binary_wire_size = sizeof(second_value);
  result->binary_wire_equal = capture.valid;

  mutation = (turbo_flow_record_mutation_t)TURBO_FLOW_RECORD_MUTATION_INIT;
  mutation.kind = TURBO_FLOW_RECORD_DELETE;
  mutation.key = key;
  mutation.key_size = sizeof(key);
  mutation.expected_revision = 1u;
  mutation.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  result->stale_delete_status = store->commit(store->ctx, &mutation, 1u);
  if (result->stale_delete_status != TURBO_EBUSY) return TURBO_EPROTO;
  mutation.expected_revision = 2u;
  rc = store->commit(store->ctx, &mutation, 1u);
  if (rc != TURBO_OK) return rc;
  count = 0u;
  rc = store->scan(store->ctx, turbo_flow_test_record_store_contract_count, &count);
  if (rc != TURBO_OK || count != 0u) return rc == TURBO_OK ? TURBO_EPROTO : rc;
  result->empty_after_delete = 1;
  return TURBO_OK;
}

#endif

