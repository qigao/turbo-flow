#ifndef TURBO_FLOW_PROVIDER_RESOLVER_H
#define TURBO_FLOW_PROVIDER_RESOLVER_H

#include "turbo_flow_diagnostic.h"
#include "turbo_flow_provider.h"

#include <salts/plugin.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * One explicit deployment mapping to an already-loaded Salts Plugin export.
 *
 * The resolver owns no lease. registry must outlive every generation that uses
 * this mapping. plugin is a stable registry ref by value. export_id and
 * identity are borrowed only for the resolver call; generation re-resolves the
 * export from the leased manifest and uses the manifest-owned export identity.
 *
 * identity is non-secret diagnostic identity only.
 */
typedef struct turbo_flow_provider_export_ref_v1_s {
  size_t size;
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  const char *export_id;
  const char *identity;
} turbo_flow_provider_export_ref_v1_t;

#define TURBO_FLOW_PROVIDER_EXPORT_REF_V1_INIT   {sizeof(turbo_flow_provider_export_ref_v1_t), NULL, {0u, 0u}, NULL, NULL}

static inline int
turbo_flow_provider_export_ref_valid(
    const turbo_flow_provider_export_ref_v1_t *ref) {
  return ref != NULL && ref->size == sizeof(*ref) &&
         ref->registry != NULL && salts_plugin_ref_valid(ref->plugin) &&
         ref->export_id != NULL && ref->export_id[0] != '\0' &&
         ref->identity != NULL && ref->identity[0] != '\0';
}

typedef int (*turbo_flow_resolve_provider_export_fn)(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_export_ref_v1_t *out,
    turbo_flow_config_error_t *error);

typedef int (*turbo_flow_resolve_resource_export_fn)(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_provider_export_ref_v1_t *out,
    turbo_flow_config_error_t *error);

/**
 * Generation-scoped deployment resolver.
 *
 * Provider resolution is mandatory. Resource resolution is optional because a
 * product may contain no provider requiring a deployment resource. If a
 * provider declares a resource requirement while resolve_resource is NULL, the
 * generation must fail preflight; it must not consult environment/PATH/defaults.
 *
 * Callbacks return only references to already-loaded Salts Plugin exports.
 * They never transfer a lease and never expose secret payload.
 */
typedef struct turbo_flow_provider_resolver_v1_s {
  size_t size;
  void *ctx;
  turbo_flow_resolve_provider_export_fn resolve_provider;
  turbo_flow_resolve_resource_export_fn resolve_resource;
} turbo_flow_provider_resolver_v1_t;

#define TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT   {sizeof(turbo_flow_provider_resolver_v1_t), NULL, NULL, NULL}

static inline int
turbo_flow_provider_resolver_valid(
    const turbo_flow_provider_resolver_v1_t *resolver) {
  return resolver != NULL && resolver->size == sizeof(*resolver) &&
         resolver->resolve_provider != NULL;
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_RESOLVER_H */
