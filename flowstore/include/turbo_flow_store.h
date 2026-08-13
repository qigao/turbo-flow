#ifndef TURBO_FLOW_STORE_H
#define TURBO_FLOW_STORE_H

#include "platform.h"
#include "turbo_error.h"
#include "turbo_flow_record_store.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_STORE_ABI_VERSION 1u

typedef struct turbo_flow_store_s turbo_flow_store_t;

/**
 * Owns the FlowStore facade over one caller-owned, namespace-bound RecordStore backend.
 *
 * The backend remains borrowed and must outlive FlowStore. The caller serializes operations unless
 * its backend explicitly documents stronger concurrency guarantees.
 */
typedef struct turbo_flow_store_config_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_record_store_t *backend;
} turbo_flow_store_config_t;

#define TURBO_FLOW_STORE_CONFIG_INIT                                                              \
  {sizeof(turbo_flow_store_config_t), TURBO_FLOW_STORE_ABI_VERSION, NULL}

/** Caller-owned binary snapshot returned by turbo_flow_store_get(). */
typedef struct turbo_flow_store_record_s {
  size_t size;
  uint64_t revision;
  uint8_t *value;
  size_t value_size;
} turbo_flow_store_record_t;

#define TURBO_FLOW_STORE_RECORD_INIT {sizeof(turbo_flow_store_record_t), 0u, NULL, 0u}

typedef struct turbo_flow_store_bytes_s {
  const uint8_t *data;
  size_t size;
} turbo_flow_store_bytes_t;

#define TURBO_FLOW_STORE_BYTES_INIT {NULL, 0u}

typedef enum turbo_flow_store_full_policy_e {
  TURBO_FLOW_STORE_FULL_REJECT = 1,
  TURBO_FLOW_STORE_FULL_TRIM_OLDEST = 2
} turbo_flow_store_full_policy_t;

typedef struct turbo_flow_store_limits_s {
  size_t size;
  uint32_t abi_version;
  size_t initial_records;
  size_t max_records;
  size_t max_bytes;
  size_t max_item_bytes;
  uint64_t retention_ms;
  turbo_flow_store_full_policy_t full_policy;
} turbo_flow_store_limits_t;

#define TURBO_FLOW_STORE_LIMITS_INIT                                                               \
  {sizeof(turbo_flow_store_limits_t), TURBO_FLOW_STORE_ABI_VERSION, 0u, 0u, 0u, 0u, 0u,            \
   TURBO_FLOW_STORE_FULL_REJECT}

typedef struct turbo_flow_store_stats_s {
  size_t size;
  uint32_t abi_version;
  size_t records;
  size_t peak_records;
  size_t bytes;
  size_t peak_bytes;
  uint64_t writes;
  uint64_t queries;
  uint64_t rejects;
  uint64_t trims;
  uint64_t conflicts;
  uint64_t backend_errors;
} turbo_flow_store_stats_t;

#define TURBO_FLOW_STORE_STATS_INIT {sizeof(turbo_flow_store_stats_t), TURBO_FLOW_STORE_ABI_VERSION}

/** Validate common limits. State/Index callers must use FULL_REJECT. */
CXX_C_API int turbo_flow_store_limits_validate(const turbo_flow_store_limits_t *limits,
                                               int allow_trim);

/** Create the owning binary Record facade. Data format interpretation is intentionally external. */
CXX_C_API int turbo_flow_store_create(const turbo_flow_store_config_t *config,
                                      turbo_flow_store_t **out);
CXX_C_API void turbo_flow_store_destroy(turbo_flow_store_t *store);

/** Commit a caller-owned atomic batch of binary Record mutations. */
CXX_C_API int turbo_flow_store_commit(turbo_flow_store_t *store,
                                      const turbo_flow_record_mutation_t *mutations,
                                      size_t mutation_count);

/** Commit one binary Record PUT. Value may be empty but is never interpreted by FlowStore. */
CXX_C_API int turbo_flow_store_put(turbo_flow_store_t *store, const uint8_t *key, size_t key_size,
                                   uint64_t expected_revision, uint64_t next_revision,
                                   const uint8_t *value, size_t value_size);

/**
 * Copy one binary Record value into caller-owned output. TURBO_ENOENT means no matching key.
 * Call turbo_flow_store_record_clear() on every initialized output before reuse or release.
 */
CXX_C_API int turbo_flow_store_get(turbo_flow_store_t *store, const uint8_t *key, size_t key_size,
                                   turbo_flow_store_record_t *out);
CXX_C_API void turbo_flow_store_record_clear(turbo_flow_store_record_t *record);

/** Commit one binary Record DELETE at an existing revision. */
CXX_C_API int turbo_flow_store_delete(turbo_flow_store_t *store, const uint8_t *key,
                                      size_t key_size, uint64_t expected_revision);

/** Visit the backend's stable binary Record snapshot. Views are borrowed for each callback. */
CXX_C_API int turbo_flow_store_scan(turbo_flow_store_t *store, turbo_flow_record_visit_fn visit,
                                    void *ctx);

#ifdef __cplusplus
}
#endif

#endif
