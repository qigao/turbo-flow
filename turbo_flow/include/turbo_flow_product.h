#ifndef TURBO_FLOW_PRODUCT_H
#define TURBO_FLOW_PRODUCT_H

#include "turbo_flow.h"
#include "turbo_flow_resolved_config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*turbo_flow_product_adapter_register_fn)(void *ctx, turbo_flow_t *flow,
                                                      const turbo_flow_resolved_config_t *resolved,
                                                      const char *adapter_name,
                                                      turbo_flow_config_error_t *error);
typedef int (*turbo_flow_product_resource_register_fn)(void *ctx, turbo_flow_t *flow,
                                                       const turbo_flow_resolved_config_t *resolved,
                                                       const char *resource_name,
                                                       turbo_flow_config_error_t *error);

typedef struct turbo_flow_product_adapter_provider_s {
  size_t size;
  const char *kind;
  turbo_flow_product_adapter_register_fn register_adapter;
  void *ctx;
} turbo_flow_product_adapter_provider_t;

#define TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT                                                   \
  {sizeof(turbo_flow_product_adapter_provider_t), NULL, NULL, NULL}

typedef struct turbo_flow_product_resource_provider_s {
  size_t size;
  const char *kind;
  turbo_flow_product_resource_register_fn register_resource;
  void *ctx;
} turbo_flow_product_resource_provider_t;

#define TURBO_FLOW_PRODUCT_RESOURCE_PROVIDER_INIT                                                  \
  {sizeof(turbo_flow_product_resource_provider_t), NULL, NULL, NULL}

typedef struct turbo_flow_product_provider_registry_s {
  size_t size;
  const turbo_flow_product_adapter_provider_t *adapter_providers;
  size_t adapter_provider_count;
  const turbo_flow_product_resource_provider_t *resource_providers;
  size_t resource_provider_count;
} turbo_flow_product_provider_registry_t;

#define TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT                                                  \
  {sizeof(turbo_flow_product_provider_registry_t), NULL, 0u, NULL, 0u}

/**
 * Copy the resolver-expanded bounded Graph ingress configuration.
 * @return TURBO_OK, TURBO_EINVAL for invalid arguments, or TURBO_EPROTO for a corrupt snapshot.
 */
TURBO_FLOW_C_API int
turbo_flow_resolved_config_runtime_ingress(const turbo_flow_resolved_config_t *config,
                                           turbo_flow_async_ingress_config_t *ingress);

/**
 * Validate provider availability without mutating a Graph.
 * @return TURBO_OK or a structured registry/configuration error.
 */
TURBO_FLOW_C_API int turbo_flow_product_preflight(const turbo_flow_resolved_config_t *config,
                                           const turbo_flow_product_provider_registry_t *registry,
                                           turbo_flow_config_error_t *error);

/**
 * Register each resource and adapter referenced by a parsed Graph exactly once.
 * On callback failure, the caller discards that Graph generation and destroys
 * provider-owned resources; this function does not attempt partial rollback.
 * @return TURBO_OK, the exact provider error, or a structured binding error.
 */
TURBO_FLOW_C_API int
turbo_flow_product_assemble_graph(turbo_flow_t *flow, const turbo_flow_resolved_config_t *config,
                                  const turbo_flow_product_provider_registry_t *registry,
                                  turbo_flow_config_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PRODUCT_H */
