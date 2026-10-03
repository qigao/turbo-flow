#ifndef TURBO_FLOW_CNET_SOURCE_CONTRACT_INTERNAL_H
#define TURBO_FLOW_CNET_SOURCE_CONTRACT_INTERNAL_H

#include "turbo_flow_provider.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum cnet_typed_source_kind_e {
  CNET_TYPED_SOURCE_STREAM = 1,
  CNET_TYPED_SOURCE_LISTENER = 2,
  CNET_TYPED_SOURCE_PACKET = 3
} cnet_typed_source_kind_t;

typedef struct cnet_typed_source_contract_s {
  size_t size;
  cnet_typed_source_kind_t kind;
  uint32_t packet_mode;
  size_t transport_capacity;
  size_t max_message_bytes;
  size_t scheduler_max_steps_per_poll;
} cnet_typed_source_contract_t;

#define CNET_TYPED_SOURCE_CONTRACT_INIT \
  {sizeof(cnet_typed_source_contract_t), (cnet_typed_source_kind_t)0, \
   0u, 0u, 0u, 0u}

/**
 * Project protocol-admission facts from one already-bound canonical CNet
 * provider config. The implementation requires the exact generated
 * Message/DataDesc contract and performs no field-name/schema lookup.
 */
int cnet_typed_source_contract(
    const turbo_flow_provider_config_view_v1_t *config,
    cnet_typed_source_contract_t *out,
    turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CNET_SOURCE_CONTRACT_INTERNAL_H */
