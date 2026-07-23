#ifndef TURBO_FLOW_RECORD_STORE_H
#define TURBO_FLOW_RECORD_STORE_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_RECORD_STORE_API_VERSION 1u
#define TURBO_FLOW_RECORD_REVISION_ABSENT 0u
#define TURBO_FLOW_RECORD_REVISION_MAX INT64_MAX

typedef enum turbo_flow_record_store_capability_e {
  /** A successful commit has crossed the provider's durable commit boundary. */
  TURBO_FLOW_RECORD_STORE_DURABLE = 1u << 0,
  /** Every mutation in one commit succeeds or none becomes visible. */
  TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH = 1u << 1
} turbo_flow_record_store_capability_t;

/** Borrowed record view valid only for the duration of one scan callback. */
typedef struct turbo_flow_record_view_s {
  size_t size;
  const uint8_t *key;
  size_t key_size;
  uint64_t revision;
  const uint8_t *value;
  size_t value_size;
} turbo_flow_record_view_t;

#define TURBO_FLOW_RECORD_VIEW_INIT {sizeof(turbo_flow_record_view_t), NULL, 0u, 0u, NULL, 0u}

typedef enum turbo_flow_record_mutation_kind_e {
  TURBO_FLOW_RECORD_PUT = 1,
  TURBO_FLOW_RECORD_DELETE
} turbo_flow_record_mutation_kind_t;

/**
 * One optimistic mutation in an atomic record-store commit.
 *
 * PUT requires `next_revision > expected_revision`. DELETE requires an existing
 * `expected_revision` and leaves no revision behind. Revision zero always means
 * that the key must be absent. Keys in one batch must be unique.
 */
typedef struct turbo_flow_record_mutation_s {
  size_t size;
  turbo_flow_record_mutation_kind_t kind;
  const uint8_t *key;
  size_t key_size;
  uint64_t expected_revision;
  uint64_t next_revision;
  const uint8_t *value;
  size_t value_size;
} turbo_flow_record_mutation_t;

#define TURBO_FLOW_RECORD_MUTATION_INIT                                                            \
  {sizeof(turbo_flow_record_mutation_t), TURBO_FLOW_RECORD_PUT, NULL, 0u, 0u, 0u, NULL, 0u}

typedef int (*turbo_flow_record_visit_fn)(void *ctx, const turbo_flow_record_view_t *record);

/**
 * Bounded multi-key state store for durable state owners.
 *
 * The provider is already bound to one namespace. The caller owns the store and
 * serializes scan/commit unless the provider documents stronger concurrency.
 * scan visits a stable snapshot in provider-defined key order and propagates the
 * first visitor error. commit returns TURBO_EBUSY when any expected revision does
 * not match and must leave every record unchanged on all failures. No durable
 * provider may fall back to volatile memory.
 */
typedef struct turbo_flow_record_store_s {
  size_t size;
  uint32_t api_version;
  uint32_t capabilities;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_records;
  void *ctx;
  int (*scan)(void *ctx, turbo_flow_record_visit_fn visit, void *visit_ctx);
  int (*commit)(void *ctx, const turbo_flow_record_mutation_t *mutations, size_t mutation_count);
} turbo_flow_record_store_t;

#define TURBO_FLOW_RECORD_STORE_INIT                                                               \
  {sizeof(turbo_flow_record_store_t),                                                              \
   TURBO_FLOW_RECORD_STORE_API_VERSION,                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

#ifdef __cplusplus
}
#endif

#endif
