#ifndef TURBO_FLOW_RESOURCE_H
#define TURBO_FLOW_RESOURCE_H

#include "turbo_flow_diagnostic.h"
#include "turbo_flow_export.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Resolver output for one named deployment resource.
 *
 * identity and export_id are stable non-secret diagnostics borrowed until the
 * enclosing turbo_flow_resource_binding_acquire() returns; the host copies them
 * immediately after resolve(). registry is deployment-manager owned and must outlive
 * every acquired TurboFlow resource binding. plugin must already be loaded and
 * STARTED; TurboFlow owns acquisition/release of the lease, not the resolver.
 */
typedef struct turbo_flow_resource_candidate_v1_s {
  size_t size;
  const char *identity;
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  const char *export_id;
} turbo_flow_resource_candidate_v1_t;

#define TURBO_FLOW_RESOURCE_CANDIDATE_V1_INIT \
  {sizeof(turbo_flow_resource_candidate_v1_t), NULL, NULL, {0u, 0u}, NULL}

typedef int (*turbo_flow_resource_resolve_fn)(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error);

/**
 * Explicit generation/control-plane resource resolver.
 *
 * The resolver maps one stable .flow resource name to a concrete Salts Plugin
 * ref/export. It never transfers a lease and must not synthesize an alternate
 * Interface when the requested exact contract is unavailable.
 */
typedef struct turbo_flow_resource_resolver_v1_s {
  size_t size;
  void *ctx;
  turbo_flow_resource_resolve_fn resolve;
} turbo_flow_resource_resolver_v1_t;

#define TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT \
  {sizeof(turbo_flow_resource_resolver_v1_t), NULL, NULL}

typedef struct turbo_flow_resource_binding_s turbo_flow_resource_binding_t;

/**
 * Resolve, acquire and exact-contract-check one deployment resource.
 *
 * Success owns one Salts Plugin lease until turbo_flow_resource_binding_release.
 * Only non-secret identity/export text is copied into the binding. The resource
 * Interface {self,vtable} remains plugin-owned and borrowed under the lease.
 *
 * Normal failures leave *out NULL. If exact-contract rejection succeeds but
 * cleanup of the just-acquired lease itself fails, the function returns that
 * cleanup error with *out non-NULL so the caller can explicitly retry release;
 * no force-unload path exists.
 */
TURBO_FLOW_C_API int turbo_flow_resource_binding_acquire(
    const turbo_flow_resource_resolver_v1_t *resolver,
    const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_binding_t **out,
    turbo_flow_config_error_t *error);

/** Borrow the exact provider-facing resource view while binding remains live. */
TURBO_FLOW_C_API int turbo_flow_resource_binding_view(
    const turbo_flow_resource_binding_t *binding,
    turbo_flow_provider_resource_view_v1_t *out);

/**
 * Release the Salts Plugin lease and destroy the binding.
 *
 * On release failure *binding_io remains owned/live for explicit retry.
 */
TURBO_FLOW_C_API int turbo_flow_resource_binding_release(
    turbo_flow_resource_binding_t **binding_io);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_RESOURCE_H */
