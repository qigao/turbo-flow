#ifndef TURBO_FLOW_DURABLE_MEMORY_RESOURCE_H
#define TURBO_FLOW_DURABLE_MEMORY_RESOURCE_H

#include <cmeta/interface.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_ID \
  "turbo_flow.durable.memory"

enum {
  TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_VERSION = 1u,
  TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS = UINT64_C(1) << 0
};

typedef struct turbo_flow_durable_memory_resource_view_s {
  size_t size;
  size_t max_records;
  size_t max_total_bytes;
  size_t max_record_bytes;
  size_t max_claims;
} turbo_flow_durable_memory_resource_view_t;

#define TURBO_FLOW_DURABLE_MEMORY_RESOURCE_VIEW_INIT \
  {sizeof(turbo_flow_durable_memory_resource_view_t), 0u, 0u, 0u, 0u}

#define TURBO_FLOW_DURABLE_MEMORY_RESOURCE_METHODS(X, I) \
  X(I, R1, int, snapshot, turbo_flow_durable_memory_resource_view_t *, view_out)

CMETA_INTERFACE(
    turbo_flow_durable_memory_resource,
    TURBO_FLOW_DURABLE_MEMORY_RESOURCE_METHODS);

static inline int turbo_flow_durable_memory_resource_view_valid(
    const turbo_flow_durable_memory_resource_view_t *view) {
  return view != NULL && view->size == sizeof(*view) &&
         view->max_records != 0u && view->max_total_bytes != 0u &&
         view->max_record_bytes != 0u && view->max_claims != 0u &&
         view->max_record_bytes <= view->max_total_bytes &&
         view->max_claims <= view->max_records;
}

#ifdef __cplusplus
}
#endif

#endif
