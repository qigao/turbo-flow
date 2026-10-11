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
 *
 * An explicit managed boundary must cover the schema's SOURCE/SINK roles.
 * When the schema includes SOURCE and that boundary is validated, the exact
 * source start callback may call turbo_flow_managed_source_run_open(). Only a
 * successful open moves the Publisher into the Flow-owned run. A SOURCE schema
 * alone, without a managed boundary, does not grant managed Source admission.
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

enum { TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_VERSION = 1u };

typedef enum turbo_flow_source_transport_kind_e {
  TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER = 1,
  TURBO_FLOW_SOURCE_TRANSPORT_UDP_PACKET,
  TURBO_FLOW_SOURCE_TRANSPORT_KCP_PACKET
} turbo_flow_source_transport_kind_t;

/**
 * Immutable admission limits supplied by a materialized network Source.
 *
 * Counts describe the transport owner, not a downstream protocol's frame count.
 * max_messages_per_poll bounds deliveries across all stages sharing that owner
 * during one external owner poll on its control thread. It must include buffered
 * deliveries before and after native progress; scheduler task counts alone are
 * not message counts. All three limits must be positive.
 *
 * This pointer-free value is copied into Graph storage. It neither retains a
 * provider/resource nor authorizes polling; their owners keep those lifetimes.
 */
typedef struct turbo_flow_source_transport_contract_s {
  size_t size;
  uint32_t version;
  turbo_flow_source_transport_kind_t transport;
  size_t session_capacity;
  size_t max_message_bytes;
  size_t max_messages_per_poll;
} turbo_flow_source_transport_contract_t;

#define TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT \
  {sizeof(turbo_flow_source_transport_contract_t), \
   TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_VERSION, \
   (turbo_flow_source_transport_kind_t)0, 0u, 0u, 0u}

/**
 * Copy one contract onto an already provider-bound Source stage before compile.
 * Registration is exclusive with Graph use, just like adapter registration.
 * Returns EINVAL for invalid limits/ABI or a non-Source, ENOENT for an unbound
 * stage, EALREADY for a second contract on the same owner, EBUSY after compile.
 * Rejection changes neither the binding nor its previous contract.
 */
TURBO_FLOW_C_API int turbo_flow_provider_adapter_source_transport_register(
    turbo_flow_t *flow, const char *stage_name,
    const turbo_flow_source_transport_contract_t *contract);

/**
 * Copy a Source's admission contract using its exact parsed stage name.
 * Available after materialization, including before compile/start. Never falls
 * back to the global adapter registry. Initialize out with CONTRACT_INIT.
 * Returns ENOENT for a missing/unbound stage, ENOTSUP for a bound Source without
 * this capability, EINVAL for an invalid output ABI or non-Source. On failure
 * out is unchanged. A successful copy stays valid after Graph reset/destroy.
 * Example: query(flow, "wire", &limits), then size pending claims to at least
 * limits.max_messages_per_poll with checked byte-budget multiplication.
 */
TURBO_FLOW_C_API int turbo_flow_provider_adapter_source_transport_query(
    const turbo_flow_t *flow, const char *stage_name,
    turbo_flow_source_transport_contract_t *out);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_ADAPTER_H */
