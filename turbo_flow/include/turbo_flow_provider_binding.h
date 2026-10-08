#ifndef TURBO_FLOW_PROVIDER_BINDING_H
#define TURBO_FLOW_PROVIDER_BINDING_H

#include "turbo_flow_diagnostic.h"
#include "turbo_flow_export.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Deployment lookup result for one canonical .flow provider identity.
 *
 * provider_identity is itself the Salts Plugin export_id. The resolver may only
 * select the loaded/started plugin ref that owns that exact export; it cannot
 * substitute a different export identity.
 *
 * module_identity is optional non-secret diagnostic text borrowed until the
 * enclosing acquire call returns. registry must outlive every acquired binding.
 */
typedef struct turbo_flow_provider_candidate_v1_s {
  size_t size;
  const char *module_identity;
  cmeta_plugin_registry *registry;
  cmeta_plugin_ref plugin;
} turbo_flow_provider_candidate_v1_t;

#define TURBO_FLOW_PROVIDER_CANDIDATE_V1_INIT \
  {sizeof(turbo_flow_provider_candidate_v1_t), NULL, NULL, {0u, 0u}}

typedef int (*turbo_flow_provider_resolve_fn)(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error);

/** Explicit deployment/control-plane resolver; never a process-global registry. */
typedef struct turbo_flow_provider_resolver_v1_s {
  size_t size;
  void *ctx;
  turbo_flow_provider_resolve_fn resolve;
} turbo_flow_provider_resolver_v1_t;

#define TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT \
  {sizeof(turbo_flow_provider_resolver_v1_t), NULL, NULL}

typedef struct turbo_flow_provider_binding_s turbo_flow_provider_binding_t;

/**
 * Resolve and retain one exact canonical provider-factory Interface export.
 *
 * Success owns one Salts Plugin lease until release. Provider metadata,
 * DataBind Message artifacts, Interface vtables and callbacks remain borrowed
 * from the provider DSO under that lease.
 *
 * Normal failures leave *out NULL. If rejection cleanup cannot return the
 * acquired lease, *out receives a cleanup binding for explicit release retry.
 */
TURBO_FLOW_C_API int turbo_flow_provider_binding_acquire(
    const turbo_flow_provider_resolver_v1_t *resolver,
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

/**
 * Release provider module lease and destroy binding.
 * On registry release failure, *binding_io remains live for explicit retry.
 */
TURBO_FLOW_C_API int turbo_flow_provider_binding_release(
    turbo_flow_provider_binding_t **binding_io);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_BINDING_H */
