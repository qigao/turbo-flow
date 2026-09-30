#ifndef TURBO_FLOW_DEPLOYMENT_RESOURCE_H
#define TURBO_FLOW_DEPLOYMENT_RESOURCE_H

#include "turbo_flow_diagnostic.h"
#include "turbo_flow_export.h"

#include <cmeta/interface.h>
#include <salts/plugin.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_DEPLOYMENT_RESOURCE_IDENTITY_MAX 255u

/**
 * Exact Salts Plugin Interface requirement declared by one provider instance.
 *
 * contract_id/expected_interface are immutable provider-owned metadata and are
 * valid while the provider's own plugin lease is retained by generation.
 */
typedef struct turbo_flow_deployment_resource_requirement_v1_s {
  size_t size;
  const char *contract_id;
  uint32_t contract_version;
  uint64_t required_capabilities;
  const cmeta_interface_desc *expected_interface;
} turbo_flow_deployment_resource_requirement_v1_t;

#define TURBO_FLOW_DEPLOYMENT_RESOURCE_REQUIREMENT_V1_INIT \
  {sizeof(turbo_flow_deployment_resource_requirement_v1_t), NULL, 0u, 0u, NULL}

/**
 * Caller-owned non-secret resolution result.
 *
 * The resolver writes fixed-size identity/export_id strings so generation never
 * retains resolver-borrowed text. registry is deployment-owned and must outlive
 * every generation that acquired a lease from it. plugin/export_id identify the
 * exact Salts Plugin INTERFACE export; this structure itself owns no lease.
 */
typedef struct turbo_flow_deployment_resource_reference_v1_s {
  size_t size;
  char identity[TURBO_FLOW_DEPLOYMENT_RESOURCE_IDENTITY_MAX + 1u];
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  char export_id[SALTS_PLUGIN_EXPORT_ID_MAX + 1u];
} turbo_flow_deployment_resource_reference_v1_t;

#define TURBO_FLOW_DEPLOYMENT_RESOURCE_REFERENCE_V1_INIT \
  {sizeof(turbo_flow_deployment_resource_reference_v1_t), {0}, NULL, {0u, 0u}, {0}}

/**
 * Resolve one explicit .flow resource name without side effects on a provider.
 *
 * The callback does not acquire a lease and never returns secret material.
 * Generation validates/acquires the returned Salts Plugin reference itself.
 */
typedef int (*turbo_flow_deployment_resource_resolve_fn)(
    void *ctx, const char *resource_name,
    const turbo_flow_deployment_resource_requirement_v1_t *requirement,
    turbo_flow_deployment_resource_reference_v1_t *reference_out,
    turbo_flow_config_error_t *error);

typedef struct turbo_flow_deployment_resource_resolver_v1_s {
  size_t size;
  void *ctx;
  turbo_flow_deployment_resource_resolve_fn resolve;
} turbo_flow_deployment_resource_resolver_v1_t;

#define TURBO_FLOW_DEPLOYMENT_RESOURCE_RESOLVER_V1_INIT \
  {sizeof(turbo_flow_deployment_resource_resolver_v1_t), NULL, NULL}

static inline int turbo_flow_deployment_resource_requirement_valid(
    const turbo_flow_deployment_resource_requirement_v1_t *requirement) {
  return requirement != NULL &&
         requirement->size == sizeof(*requirement) &&
         requirement->contract_id != NULL &&
         requirement->contract_id[0] != '\0' &&
         requirement->contract_version != 0u &&
         cmeta_interface_desc_valid(requirement->expected_interface);
}

static inline int turbo_flow_deployment_resource_reference_valid(
    const turbo_flow_deployment_resource_reference_v1_t *reference) {
  return reference != NULL &&
         reference->size == sizeof(*reference) &&
         reference->identity[0] != '\0' &&
         reference->registry != NULL &&
         salts_plugin_ref_valid(reference->plugin) &&
         reference->export_id[0] != '\0';
}

static inline int turbo_flow_deployment_resource_resolver_valid(
    const turbo_flow_deployment_resource_resolver_v1_t *resolver) {
  return resolver != NULL && resolver->size == sizeof(*resolver) &&
         resolver->resolve != NULL;
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DEPLOYMENT_RESOURCE_H */
