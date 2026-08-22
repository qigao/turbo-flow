#ifndef TURBO_FLOW_STORE_POLICY_H
#define TURBO_FLOW_STORE_POLICY_H

#include "turbo_flow_store.h"
#include "turbo_flow_storage_backend.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum turbo_flow_store_data_class_e {
  TURBO_FLOW_STORE_DATA_STATE = 1,
  TURBO_FLOW_STORE_DATA_MEMBERSHIP = 2,
  TURBO_FLOW_STORE_DATA_EVENT = 3,
  TURBO_FLOW_STORE_DATA_METRIC = 4
} turbo_flow_store_data_class_t;

typedef enum turbo_flow_store_model_e {
  TURBO_FLOW_STORE_MODEL_STATE = 1,
  TURBO_FLOW_STORE_MODEL_INDEX = 2,
  TURBO_FLOW_STORE_MODEL_LOG = 3,
  TURBO_FLOW_STORE_MODEL_TIME_SERIES = 4
} turbo_flow_store_model_t;

typedef enum turbo_flow_store_placement_e {
  TURBO_FLOW_STORE_PLACEMENT_MEMORY = 1,
  TURBO_FLOW_STORE_PLACEMENT_BACKEND = 2,
  TURBO_FLOW_STORE_PLACEMENT_REDIS = TURBO_FLOW_STORE_PLACEMENT_BACKEND
} turbo_flow_store_placement_t;

typedef struct turbo_flow_store_policy_s {
  size_t size;
  uint32_t abi_version;
  size_t local_max_records;
  size_t local_max_bytes;
  size_t local_max_item_bytes;
  uint64_t local_max_writes_per_second;
  uint64_t local_max_retention_ms;
  turbo_flow_storage_capabilities_t remote_capabilities;
} turbo_flow_store_policy_t;

#define TURBO_FLOW_STORE_POLICY_INIT                                                               \
  {sizeof(turbo_flow_store_policy_t), TURBO_FLOW_STORE_ABI_VERSION, 0u, 0u, 0u, 0u, 0u, 0u}

typedef struct turbo_flow_store_requirements_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_store_data_class_t data_class;
  size_t expected_records;
  size_t expected_bytes;
  size_t max_item_bytes;
  uint64_t writes_per_second;
  uint64_t retention_ms;
  int durable;
} turbo_flow_store_requirements_t;

#define TURBO_FLOW_STORE_REQUIREMENTS_INIT                                                         \
  {sizeof(turbo_flow_store_requirements_t),                                                        \
   TURBO_FLOW_STORE_ABI_VERSION,                                                                   \
   TURBO_FLOW_STORE_DATA_STATE,                                                                    \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0}

typedef struct turbo_flow_store_decision_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_store_model_t model;
  turbo_flow_store_placement_t placement;
} turbo_flow_store_decision_t;

#define TURBO_FLOW_STORE_DECISION_INIT                                                             \
  {sizeof(turbo_flow_store_decision_t), TURBO_FLOW_STORE_ABI_VERSION,                              \
   TURBO_FLOW_STORE_MODEL_STATE, TURBO_FLOW_STORE_PLACEMENT_MEMORY}

/** Pure routing decision. It does not allocate, connect, or silently fall back. */
TURBO_FLOW_C_API int turbo_flow_store_route(const turbo_flow_store_policy_t *policy,
                                     const turbo_flow_store_requirements_t *requirements,
                                     turbo_flow_store_decision_t *decision);

#ifdef __cplusplus
}
#endif

#endif
