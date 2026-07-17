#ifndef TURBO_FLOW_FMQ_COMPATIBILITY_H
#define TURBO_FLOW_FMQ_COMPATIBILITY_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_FMQ_COMPATIBILITY_NAME_MAX 63u
#define TURBO_FLOW_FMQ_CAPABILITY_BIT(id) (UINT64_C(1) << (id))

typedef struct turbo_flow_fmq_minor_range_s {
  uint16_t major;
  uint16_t minor_min;
  uint16_t minor_max;
} turbo_flow_fmq_minor_range_t;

#define TURBO_FLOW_FMQ_MINOR_RANGE_INIT {0u, 0u, 0u}

/** Read/write schema range for a shared durable fact source. */
typedef struct turbo_flow_fmq_store_range_s {
  uint16_t major;
  uint16_t read_minor_min;
  uint16_t read_minor_max;
  uint16_t write_minor_min;
  uint16_t write_minor_max;
} turbo_flow_fmq_store_range_t;

#define TURBO_FLOW_FMQ_STORE_RANGE_INIT {0u, 0u, 0u, 0u, 0u}

/** One immutable release/deployment capability manifest. */
typedef struct turbo_flow_fmq_compatibility_contract_s {
  size_t size;
  char release[TURBO_FLOW_FMQ_COMPATIBILITY_NAME_MAX + 1u];
  uint16_t fmq_wire_min;
  uint16_t fmq_wire_max;
  turbo_flow_fmq_minor_range_t tfmp;
  turbo_flow_fmq_store_range_t management_store;
  turbo_flow_fmq_minor_range_t yaml_schema;
  uint64_t capabilities;
} turbo_flow_fmq_compatibility_contract_t;

#define TURBO_FLOW_FMQ_COMPATIBILITY_CONTRACT_INIT                                                 \
  {sizeof(turbo_flow_fmq_compatibility_contract_t),                                                \
   {0},                                                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_FMQ_MINOR_RANGE_INIT,                                                                \
   TURBO_FLOW_FMQ_STORE_RANGE_INIT,                                                                \
   TURBO_FLOW_FMQ_MINOR_RANGE_INIT,                                                                \
   0u}

typedef enum turbo_flow_fmq_rollout_mode_e {
  TURBO_FLOW_FMQ_ROLLOUT_DIRECT = 1,
  TURBO_FLOW_FMQ_ROLLOUT_GATEWAY = 2,
  TURBO_FLOW_FMQ_ROLLOUT_STOP_THE_WORLD = 3,
  TURBO_FLOW_FMQ_ROLLOUT_INCOMPATIBLE = 4
} turbo_flow_fmq_rollout_mode_t;

typedef enum turbo_flow_fmq_compatibility_reason_e {
  TURBO_FLOW_FMQ_COMPATIBLE = 0,
  TURBO_FLOW_FMQ_MISSING_CAPABILITY = 1,
  TURBO_FLOW_FMQ_WIRE_GAP = 2,
  TURBO_FLOW_FMQ_TFMP_GAP = 3,
  TURBO_FLOW_FMQ_STORE_GAP = 4,
  TURBO_FLOW_FMQ_YAML_GAP = 5
} turbo_flow_fmq_compatibility_reason_t;

typedef struct turbo_flow_fmq_compatibility_result_s {
  size_t size;
  turbo_flow_fmq_rollout_mode_t mode;
  turbo_flow_fmq_compatibility_reason_t reason;
  uint16_t negotiated_wire;
  uint16_t negotiated_tfmp_minor;
  uint16_t negotiated_store_minor;
  uint16_t negotiated_yaml_minor;
  uint64_t missing_capabilities;
} turbo_flow_fmq_compatibility_result_t;

#define TURBO_FLOW_FMQ_COMPATIBILITY_RESULT_INIT                                                   \
  {sizeof(turbo_flow_fmq_compatibility_result_t),                                                  \
   TURBO_FLOW_FMQ_ROLLOUT_INCOMPATIBLE,                                                            \
   TURBO_FLOW_FMQ_COMPATIBLE,                                                                      \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/**
 * Evaluate a mixed-version rollout without changing either protocol decoder.
 *
 * `gateway` is optional and represents an explicitly deployed dual-stack
 * protocol adapter. It can bridge wire/TFMP gaps only; shared durable store and
 * YAML schema still require one common version understood by both releases.
 */
CXX_C_API int
turbo_flow_fmq_compatibility_evaluate(const turbo_flow_fmq_compatibility_contract_t *current,
                                      const turbo_flow_fmq_compatibility_contract_t *candidate,
                                      const turbo_flow_fmq_compatibility_contract_t *gateway,
                                      uint64_t required_capabilities,
                                      turbo_flow_fmq_compatibility_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_FMQ_COMPATIBILITY_H */
