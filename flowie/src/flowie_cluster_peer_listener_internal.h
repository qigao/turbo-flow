#ifndef FLOWIE_CLUSTER_PEER_LISTENER_INTERNAL_H
#define FLOWIE_CLUSTER_PEER_LISTENER_INTERNAL_H

#include "flowie_cluster_node_router_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_PEER_LISTENER_ABI_V1 1u

typedef struct flowie_cluster_peer_listener_config_s {
  size_t size;
  uint32_t abi_version;
  struct tf_coronet_execution_s *execution;
  flowie_cluster_node_router_t *router;
  size_t max_connections;
  size_t max_payload_size;
  size_t queue_entries;
  size_t queue_bytes;
  uint32_t socket_timeout_ms;
  uint16_t bind_port;
  tstr_v bind_host;
  tstr_v cluster_id;
  tstr_v local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_v ca_file;
  tstr_v cert_file;
  tstr_v key_file;
  tstr_v key_password;
  flowie_cluster_peer_authorize_fn authorize;
  void *authorize_ctx;
} flowie_cluster_peer_listener_config_t;

#define FLOWIE_CLUSTER_PEER_LISTENER_CONFIG_INIT                                                   \
  {sizeof(flowie_cluster_peer_listener_config_t),                                                  \
   FLOWIE_CLUSTER_PEER_LISTENER_ABI_V1,                                                            \
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
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   NULL,                                                                                           \
   NULL}

typedef struct flowie_cluster_peer_listener_snapshot_s {
  size_t size;
  uint32_t abi_version;
  size_t active_handlers;
  size_t registered_links;
  uint64_t accepted_connections;
  uint64_t rejected_connections;
  uint64_t activated_links;
  uint64_t failed_links;
  int running;
  int closing;
  int last_error;
} flowie_cluster_peer_listener_snapshot_t;

#define FLOWIE_CLUSTER_PEER_LISTENER_SNAPSHOT_INIT                                                 \
  {sizeof(flowie_cluster_peer_listener_snapshot_t),                                                \
   FLOWIE_CLUSTER_PEER_LISTENER_ABI_V1,                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0,                                                                                              \
   0,                                                                                              \
   TURBO_OK}

typedef struct flowie_cluster_peer_listener_s flowie_cluster_peer_listener_t;

/**
 * Create a bounded mTLS peer listener. Configuration strings are copied and
 * the execution lane and node router remain borrowed through destroy(). Only
 * lexically smaller node IDs are accepted as initiators, matching connector.
 * max_connections bounds CoroNet tasks from raw accept through TLS admission,
 * link execution, and accepted-socket close completion.
 */
int flowie_cluster_peer_listener_create(const flowie_cluster_peer_listener_config_t *config,
                                        flowie_cluster_peer_listener_t **out);
int flowie_cluster_peer_listener_start(flowie_cluster_peer_listener_t *listener,
                                       uint64_t timeout_ns);
/** Stop socket admission and cancel every admitted peer task on the owner lane. */
int flowie_cluster_peer_listener_close(flowie_cluster_peer_listener_t *listener,
                                       uint64_t timeout_ns);
/**
 * Wait for CoroNet socket destruction and exact router link unregistration.
 * Returns EBUSY on the borrowed execution lane to prevent a self-wait.
 */
int flowie_cluster_peer_listener_drain(flowie_cluster_peer_listener_t *listener,
                                       uint64_t timeout_ns);
int flowie_cluster_peer_listener_snapshot(flowie_cluster_peer_listener_t *listener,
                                          flowie_cluster_peer_listener_snapshot_t *out);
/** Requires close() and a successful drain(). */
int flowie_cluster_peer_listener_destroy(flowie_cluster_peer_listener_t *listener);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_PEER_LISTENER_INTERNAL_H */
