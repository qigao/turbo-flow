#ifndef FLOWIE_CLUSTER_GENERATION_INTERNAL_H
#define FLOWIE_CLUSTER_GENERATION_INTERNAL_H

#include "flowie_cluster_endpoint_binding_internal.h"
#include "flowie_cluster_owner_directory_runtime_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_GENERATION_ABI_V1 1u

typedef struct flowie_cluster_generation_s flowie_cluster_generation_t;

typedef struct flowie_cluster_generation_config_s {
  size_t size;
  uint32_t abi_version;
  /** Root-owned execution placement. PRIVATE and OWNED_CONTEXT are exposed to the endpoint as a
   * borrowed context so both sides execute on the same owner lane. */
  turbo_flow_coronet_execution_binding_t execution;
  flowie_cluster_owner_directory_runtime_config_t owners;
  flowie_cluster_node_config_t node;
  /** Template: execution, node, and owners must be unset and are injected by this root. */
  flowie_cluster_endpoint_binding_config_t endpoint;
  uint64_t startup_timeout_ns;
  uint64_t shutdown_timeout_ns;
} flowie_cluster_generation_config_t;

#define FLOWIE_CLUSTER_GENERATION_CONFIG_INIT                                                     \
  {sizeof(flowie_cluster_generation_config_t),                                                    \
   FLOWIE_CLUSTER_GENERATION_ABI_V1,                                                              \
   {sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE, NULL,  \
    NULL, 0u, 0u},                                                                                \
   FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_CONFIG_INIT,                                            \
   FLOWIE_CLUSTER_NODE_CONFIG_INIT,                                                               \
   FLOWIE_CLUSTER_ENDPOINT_BINDING_CONFIG_INIT,                                                   \
   0u,                                                                                            \
   0u}

/** Internal dependency boundary used only for composition-order and rollback tests. */
typedef struct flowie_cluster_generation_api_s {
  size_t size;
  uint32_t abi_version;
  int (*owners_create)(const flowie_cluster_owner_directory_runtime_config_t *config,
                       flowie_cluster_owner_directory_runtime_t **out);
  int (*owners_start)(flowie_cluster_owner_directory_runtime_t *runtime);
  int (*owners_wait_ready)(flowie_cluster_owner_directory_runtime_t *runtime,
                           uint64_t timeout_ns);
  flowie_cluster_owner_directory_t *(*owners_directory)(
      flowie_cluster_owner_directory_runtime_t *runtime);
  int (*owners_close)(flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns);
  void (*owners_destroy)(flowie_cluster_owner_directory_runtime_t *runtime);
  int (*node_create)(const flowie_cluster_node_config_t *config, flowie_cluster_node_t **out);
  int (*node_start)(flowie_cluster_node_t *node, uint64_t timeout_ns);
  int (*node_close)(flowie_cluster_node_t *node, uint64_t timeout_ns);
  int (*node_destroy)(flowie_cluster_node_t *node);
  int (*endpoint_create)(const flowie_cluster_endpoint_binding_config_t *config,
                         flowie_cluster_endpoint_binding_t **out);
  const flowie_endpoint_cluster_binding_t *(*endpoint_port)(
      flowie_cluster_endpoint_binding_t *binding);
  int (*endpoint_close)(flowie_cluster_endpoint_binding_t *binding, uint64_t timeout_ns);
  int (*endpoint_destroy)(flowie_cluster_endpoint_binding_t *binding);
} flowie_cluster_generation_api_t;

/**
 * Create one generation and its shared CoroNet lane. Nested PostgreSQL and topology views remain
 * borrowed through destroy. Peer admission and owner refresh do not start until start().
 */
int flowie_cluster_generation_create(const flowie_cluster_generation_config_t *config,
                                     flowie_cluster_generation_t **out);
int flowie_cluster_generation_create_with_api(
    const flowie_cluster_generation_config_t *config,
    const flowie_cluster_generation_api_t *api, flowie_cluster_generation_t **out);

/** Start PostgreSQL owner refresh, wait for its first snapshot, then admit cluster peers. */
int flowie_cluster_generation_start(flowie_cluster_generation_t *generation);

/** Borrowed endpoint placement and cluster port; both remain valid through generation destroy. */
const turbo_flow_coronet_execution_binding_t *flowie_cluster_generation_endpoint_execution(
    flowie_cluster_generation_t *generation);
const flowie_endpoint_cluster_binding_t *flowie_cluster_generation_endpoint_port(
    flowie_cluster_generation_t *generation);

/**
 * Irreversibly stop edge routing, peer routing, and owner refresh in dependency order. A timeout
 * or busy result leaves the generation valid for another close attempt.
 */
int flowie_cluster_generation_close(flowie_cluster_generation_t *generation);

/** Close without a deadline if necessary, release components in reverse order, and stop the lane. */
int flowie_cluster_generation_destroy(flowie_cluster_generation_t *generation);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_GENERATION_INTERNAL_H */
