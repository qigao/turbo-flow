#ifndef TURBO_FLOW_PROVIDER_BINDING_H
#define TURBO_FLOW_PROVIDER_BINDING_H

#include "turbo_flow_diagnostic.h"
#include "turbo_flow_export.h"
#include "turbo_flow_provider.h"

#include <salts/component_plugin.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Deployment lookup result for one canonical .flow provider identity.
 *
 * component_identity is the exact Salts Component stable_id selected by
 * deployment configuration. module_identity is optional diagnostic text.
 * No Plugin registry/ref/lease is exposed here: module lifetime is pinned by
 * the generation-owned Component scope.
 */
typedef struct turbo_flow_provider_candidate_v1_s {
  size_t size;
  const char *module_identity;
  const char *component_identity;
} turbo_flow_provider_candidate_v2_t;

#define TURBO_FLOW_PROVIDER_CANDIDATE_V2_INIT \
  {sizeof(turbo_flow_provider_candidate_v2_t), NULL, NULL}

typedef int (*turbo_flow_provider_resolve_v2_fn)(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v2_t *out,
    turbo_flow_config_error_t *error);

/** Explicit deployment/control-plane alias resolver; never a runtime registry. */
typedef struct turbo_flow_provider_resolver_v1_s {
  size_t size;
  void *ctx;
  turbo_flow_provider_resolve_v2_fn resolve;
} turbo_flow_provider_resolver_v2_t;

#define TURBO_FLOW_PROVIDER_RESOLVER_V2_INIT \
  {sizeof(turbo_flow_provider_resolver_v2_t), NULL, NULL}

typedef struct turbo_flow_provider_binding_s turbo_flow_provider_binding_t;

/**
 * Resolve one exact provider-factory Interface from a generation-owned
 * Component scope.
 *
 * Success borrows provider metadata, DataBind artifacts, Interface vtable and
 * callbacks from component_scope. The binding owns no Plugin lease. The scope
 * must outlive the binding and every provider/session/result produced from it.
 * All failures leave *out NULL.
 */
TURBO_FLOW_C_API int turbo_flow_provider_binding_acquire(
    const salts_component_plugin_scope *component_scope,
    const turbo_flow_provider_resolver_v2_t *resolver,
    const char *provider_identity,
    turbo_flow_provider_binding_t **out,
    turbo_flow_config_error_t *error);

/** Borrow the provider's immutable typed config/resource contract. */
TURBO_FLOW_C_API int turbo_flow_provider_binding_contract(
    const turbo_flow_provider_binding_t *binding,
    turbo_flow_provider_contract_v1_t *out);

/** Invoke preflight through the retained provider Interface. */
TURBO_FLOW_C_API int turbo_flow_provider_binding_preflight(
    turbo_flow_provider_binding_t *binding,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_config_error_t *error);

/** Materialize one runtime owner through the retained provider Interface. */
TURBO_FLOW_C_API int turbo_flow_provider_binding_materialize(
    turbo_flow_provider_binding_t *binding,
    turbo_flow_t *flow,
    const turbo_flow_provider_instance_v1_t *instance,
    turbo_flow_runtime_owner *owner_out,
    turbo_flow_config_error_t *error);

/** Destroy the local binding. Module lifetime remains owned by Component scope. */
TURBO_FLOW_C_API int turbo_flow_provider_binding_release(
    turbo_flow_provider_binding_t **binding_io);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_BINDING_H */
