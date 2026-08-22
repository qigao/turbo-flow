#ifndef TURBO_FLOW_SERIES_STORE_H
#define TURBO_FLOW_SERIES_STORE_H

#include "turbo_flow_store.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_series_store_s turbo_flow_series_store_t;

/** Memory stores have one mutable owner and are not thread-safe. */

typedef enum turbo_flow_series_value_kind_e {
  TURBO_FLOW_SERIES_BOOL = 1,
  TURBO_FLOW_SERIES_INT64 = 2,
  TURBO_FLOW_SERIES_DOUBLE = 3
} turbo_flow_series_value_kind_t;

typedef struct turbo_flow_series_value_s {
  turbo_flow_series_value_kind_t kind;
  union {
    int boolean;
    int64_t integer;
    double real;
  } as;
} turbo_flow_series_value_t;

typedef struct turbo_flow_series_sample_s {
  uint64_t timestamp_ms;
  turbo_flow_series_value_t value;
} turbo_flow_series_sample_t;

typedef enum turbo_flow_series_duplicate_policy_e {
  TURBO_FLOW_SERIES_DUPLICATE_REJECT = 1,
  TURBO_FLOW_SERIES_DUPLICATE_KEEP_FIRST = 2,
  TURBO_FLOW_SERIES_DUPLICATE_KEEP_LAST = 3
} turbo_flow_series_duplicate_policy_t;

typedef enum turbo_flow_series_aggregate_e {
  TURBO_FLOW_SERIES_AGGREGATE_COUNT = 1,
  TURBO_FLOW_SERIES_AGGREGATE_SUM = 2,
  TURBO_FLOW_SERIES_AGGREGATE_MIN = 3,
  TURBO_FLOW_SERIES_AGGREGATE_MAX = 4,
  TURBO_FLOW_SERIES_AGGREGATE_AVG = 5
} turbo_flow_series_aggregate_t;

typedef struct turbo_flow_series_config_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_store_limits_t limits;
  turbo_flow_series_duplicate_policy_t duplicate_policy;
} turbo_flow_series_config_t;

#define TURBO_FLOW_SERIES_CONFIG_INIT                                                              \
  {sizeof(turbo_flow_series_config_t), TURBO_FLOW_STORE_ABI_VERSION, TURBO_FLOW_STORE_LIMITS_INIT, \
   TURBO_FLOW_SERIES_DUPLICATE_REJECT}

TURBO_FLOW_C_API int turbo_flow_series_store_create_memory(const turbo_flow_series_config_t *config,
                                                    turbo_flow_series_store_t **out);
TURBO_FLOW_C_API int turbo_flow_series_store_close(turbo_flow_series_store_t *store);
TURBO_FLOW_C_API void turbo_flow_series_store_destroy(turbo_flow_series_store_t *store);
TURBO_FLOW_C_API int turbo_flow_series_store_append(turbo_flow_series_store_t *store,
                                             turbo_flow_store_bytes_t series,
                                             const turbo_flow_series_sample_t *sample);
TURBO_FLOW_C_API int turbo_flow_series_store_range(turbo_flow_series_store_t *store,
                                            turbo_flow_store_bytes_t series, uint64_t start_ms,
                                            uint64_t end_ms, turbo_flow_series_sample_t *samples,
                                            size_t capacity, size_t *count);
TURBO_FLOW_C_API int turbo_flow_series_store_aggregate(turbo_flow_series_store_t *store,
                                                turbo_flow_store_bytes_t series, uint64_t start_ms,
                                                uint64_t end_ms,
                                                turbo_flow_series_aggregate_t aggregate,
                                                double *value, size_t *sample_count);
TURBO_FLOW_C_API int turbo_flow_series_store_trim_before(turbo_flow_series_store_t *store,
                                                  turbo_flow_store_bytes_t series,
                                                  uint64_t timestamp_ms, size_t *trimmed);
TURBO_FLOW_C_API int turbo_flow_series_store_stats(const turbo_flow_series_store_t *store,
                                            turbo_flow_store_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif
