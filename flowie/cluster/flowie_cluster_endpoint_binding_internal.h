#ifndef FLOWIE_CLUSTER_ENDPOINT_BINDING_INTERNAL_H
#define FLOWIE_CLUSTER_ENDPOINT_BINDING_INTERNAL_H

#include "flowie.h"
#include "flow_coronet_execution.h"
#include "flowie_cluster_node_internal.h"
#include "flowie_cluster_owner_directory_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_ENDPOINT_BINDING_ABI_V1 1u

typedef struct flowie_cluster_endpoint_binding_config_s {
  size_t size;
  uint32_t abi_version;
  tf_coronet_execution_t *execution;
  flowie_cluster_node_t *node;
  flowie_cluster_owner_directory_t *owners;
  size_t max_payload_size;
  size_t max_connections;
  size_t max_connection_bytes;
  size_t max_pending_bytes;
  size_t max_inbound_actions;
  size_t max_inbound_bytes;
  uint64_t request_timeout_ms;
  tstr_v cluster_id;
  tstr_v listener_id;
  tstr_v local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
} flowie_cluster_endpoint_binding_config_t;

#define FLOWIE_CLUSTER_ENDPOINT_BINDING_CONFIG_INIT                                               \
  {sizeof(flowie_cluster_endpoint_binding_config_t), FLOWIE_CLUSTER_ENDPOINT_BINDING_ABI_V1,     \
   NULL, NULL, NULL, 0u, 0u, 0u, 0u, 0u, 0u,                                                    \
   FLOWIE_ENDPOINT_CLUSTER_DEFAULT_REQUEST_TIMEOUT_MS, {NULL, 0u}, {NULL, 0u}, {NULL, 0u}, {0}}

typedef struct flowie_cluster_endpoint_binding_s flowie_cluster_endpoint_binding_t;

/**
 * Compose one endpoint socket boundary with the node router and PostgreSQL-derived
 * owner directory. The execution must be the same owner lane used by the endpoint.
 */
int flowie_cluster_endpoint_binding_create(
    const flowie_cluster_endpoint_binding_config_t *config,
    flowie_cluster_endpoint_binding_t **out);

/** Borrowed public binding, valid until destroy. */
const flowie_endpoint_cluster_binding_t *flowie_cluster_endpoint_binding_port(
    flowie_cluster_endpoint_binding_t *binding);

/** Requires all endpoint connections to have detached. */
int flowie_cluster_endpoint_binding_close(flowie_cluster_endpoint_binding_t *binding,
                                          uint64_t timeout_ns);
int flowie_cluster_endpoint_binding_destroy(flowie_cluster_endpoint_binding_t *binding);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_ENDPOINT_BINDING_INTERNAL_H */
