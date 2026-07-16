#ifndef TURBO_FLOW_CORONET_EXECUTION_H
#define TURBO_FLOW_CORONET_EXECUTION_H

#include "CoroNet/turbo_coro_thread_pool.h"
#include "turbo_error.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum turbo_flow_coronet_execution_kind_e {
  /** Adapter creates, drives, stops, and destroys a dedicated context. */
  TURBO_FLOW_CORONET_EXECUTION_PRIVATE = 1,
  /** Host drives and owns context for the complete adapter lifetime. */
  TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT,
  /** Adapter exclusively drives, stops, and destroys the supplied context. */
  TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT,
  /** Host pool drives and owns the stable context selected by lane. */
  TURBO_FLOW_CORONET_EXECUTION_POOL_LANE
} turbo_flow_coronet_execution_kind_t;

/** Immutable adapter execution placement, copied during registration. */
typedef struct turbo_flow_coronet_execution_binding_s {
  /** Must be at least sizeof(turbo_flow_coronet_execution_binding_t). */
  size_t size;
  turbo_flow_coronet_execution_kind_t kind;
  coro_context_t *context;
  coro_thread_pool_t *pool;
  uint32_t lane;
  uint32_t flags;
} turbo_flow_coronet_execution_binding_t;

/**
 * Validate field combinations and resolve a pool lane.
 *
 * @return TURBO_OK, TURBO_EINVAL for an invalid combination, or TURBO_ERANGE
 * for a lane not present in pool. This function does not take ownership.
 */
static inline int turbo_flow_coronet_execution_binding_validate(
    const turbo_flow_coronet_execution_binding_t *binding) {
  if (!binding || binding->size < sizeof(*binding) || binding->flags != 0u) return TURBO_EINVAL;
  switch (binding->kind) {
  case TURBO_FLOW_CORONET_EXECUTION_PRIVATE:
    return !binding->context && !binding->pool && binding->lane == 0u ? TURBO_OK : TURBO_EINVAL;
  case TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT:
  case TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT:
    return binding->context && !binding->pool && binding->lane == 0u ? TURBO_OK : TURBO_EINVAL;
  case TURBO_FLOW_CORONET_EXECUTION_POOL_LANE:
    if (binding->context || !binding->pool) return TURBO_EINVAL;
    return coro_thread_pool_get_context(binding->pool, (int)binding->lane) ? TURBO_OK
                                                                           : TURBO_ERANGE;
  default:
    return TURBO_EINVAL;
  }
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CORONET_EXECUTION_H */
