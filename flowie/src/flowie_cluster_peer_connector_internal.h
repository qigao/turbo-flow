#ifndef FLOWIE_CLUSTER_PEER_CONNECTOR_INTERNAL_H
#define FLOWIE_CLUSTER_PEER_CONNECTOR_INTERNAL_H

#include "flowie_cluster_node_router_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1 1u

typedef struct flowie_cluster_peer_connector_config_s {
  size_t size;
  uint32_t abi_version;
  struct tf_coronet_execution_s *execution;
  flowie_cluster_node_router_t *router;
  size_t max_payload_size;
  size_t queue_entries;
  size_t queue_bytes;
  uint32_t socket_timeout_ms;
  uint32_t retry_delay_ms;
  uint16_t remote_port;
  tstr_v remote_host;
  tstr_v cluster_id;
  tstr_v local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_v remote_node_id;
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_v ca_file;
  tstr_v cert_file;
  tstr_v key_file;
  tstr_v key_password;
  flowie_cluster_peer_authorize_fn authorize;
  void *authorize_ctx;
} flowie_cluster_peer_connector_config_t;

#define FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT                                                  \
  {sizeof(flowie_cluster_peer_connector_config_t),                                                 \
   FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1,                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {0},                                                                                            \
   {NULL, 0u},                                                                                     \
   {0},                                                                                            \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   NULL,                                                                                           \
   NULL}

typedef struct flowie_cluster_peer_connector_snapshot_s {
  size_t size;
  uint32_t abi_version;
  uint64_t connect_attempts;
  uint64_t activated_links;
  int task_running;
  int connected;
  int closing;
  int terminal_error;
  int last_error;
} flowie_cluster_peer_connector_snapshot_t;

#define FLOWIE_CLUSTER_PEER_CONNECTOR_SNAPSHOT_INIT                                                \
  {sizeof(flowie_cluster_peer_connector_snapshot_t),                                               \
   FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1,                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0,                                                                                              \
   0,                                                                                              \
   0,                                                                                              \
   0,                                                                                              \
   TURBO_OK}

typedef struct flowie_cluster_peer_connector_s flowie_cluster_peer_connector_t;

/**
 * Create one bounded reconnecting link to an exact node/boot target. To avoid
 * duplicate cross-links, only the lexically smaller node ID may initiate.
 */
int flowie_cluster_peer_connector_create(const flowie_cluster_peer_connector_config_t *config,
                                         flowie_cluster_peer_connector_t **out);
int flowie_cluster_peer_connector_start(flowie_cluster_peer_connector_t *connector,
                                        uint64_t timeout_ns);
int flowie_cluster_peer_connector_close(flowie_cluster_peer_connector_t *connector,
                                        uint64_t timeout_ns);
/** Returns EBUSY on the borrowed execution lane to prevent a self-wait. */
int flowie_cluster_peer_connector_drain(flowie_cluster_peer_connector_t *connector,
                                        uint64_t timeout_ns);
int flowie_cluster_peer_connector_snapshot(flowie_cluster_peer_connector_t *connector,
                                           flowie_cluster_peer_connector_snapshot_t *out);
int flowie_cluster_peer_connector_destroy(flowie_cluster_peer_connector_t *connector);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_PEER_CONNECTOR_INTERNAL_H */
