#ifndef TURBO_FLOW_PROVIDER_ADAPTER_H
#define TURBO_FLOW_PROVIDER_ADAPTER_H

#include "turbo_flow.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum { TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_VERSION = 1u };

/**
 * One provider-owned adapter materialization bound to exact parsed stages.
 *
 * provider_identity is the visible .flow adapter/provider identity. stage_names
 * are exact parsed stage/source instance names and are never rewritten into
 * registry aliases. A successful call creates one provider-scoped adapter
 * registration and binds every listed stage to that exact registration.
 *
 * At most one of async_terminal_ops/async_emit_ops may be supplied.
 * managed_owner_name and managed_boundary_ops are either both NULL or both
 * present. Callback code and ctx remain provider-owned until Graph registry
 * teardown, matching the existing adapter registration lifetime.
 */
typedef struct turbo_flow_provider_adapter_registration_v1_s {
  size_t size;
  uint32_t version;
  const char *provider_identity;
  const char *const *stage_names;
  size_t stage_count;
  const turbo_flow_adapter_ops_t *adapter_ops;
  const turbo_flow_async_terminal_adapter_ops_t *async_terminal_ops;
  const turbo_flow_async_emit_adapter_ops_t *async_emit_ops;
  const turbo_flow_adapter_schema_t *schema;
  const char *managed_owner_name;
  const turbo_flow_managed_boundary_provider_ops_t *managed_boundary_ops;
  void *ctx;
} turbo_flow_provider_adapter_registration_v1_t;

#define TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_V1_INIT \
  {sizeof(turbo_flow_provider_adapter_registration_v1_t), \
   TURBO_FLOW_PROVIDER_ADAPTER_REGISTRATION_VERSION, NULL, NULL, 0u, \
   NULL, NULL, NULL, NULL, NULL, NULL, NULL}

/**
 * Atomically register one provider-scoped adapter and bind exact stages.
 *
 * Provider-scoped registrations are intentionally excluded from legacy global
 * adapter-name lookup. Compile resolves an explicit stage binding first and
 * falls back to the legacy global registry only when no explicit binding exists.
 */
TURBO_FLOW_C_API int turbo_flow_provider_adapter_register(
    turbo_flow_t *flow,
    const turbo_flow_provider_adapter_registration_v1_t *registration);


/**
 * Attach transport-reply capability to one already-bound provider stage.
 *
 * The stage must already be bound by turbo_flow_provider_adapter_register().
 * This never consults the legacy global adapter-name registry.
 */
TURBO_FLOW_C_API int turbo_flow_provider_adapter_transport_reply_register(
    turbo_flow_t *flow, const char *stage_name,
    const turbo_flow_transport_reply_provider_ops_t *ops, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_ADAPTER_H */
