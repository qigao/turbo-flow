#ifndef TURBO_FLOW_STORE_H
#define TURBO_FLOW_STORE_H

#include "platform.h"
#include "turbo_error.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_STORE_ABI_VERSION 1u

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

#ifdef __cplusplus
}
#endif

#endif
