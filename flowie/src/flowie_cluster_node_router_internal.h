#ifndef FLOWIE_CLUSTER_NODE_ROUTER_INTERNAL_H
#define FLOWIE_CLUSTER_NODE_ROUTER_INTERNAL_H

#include "flowie_cluster_shard_runtime_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_NODE_ROUTER_ABI_V1 1u

typedef int (*flowie_cluster_node_shard_receive_fn)(void *ctx,
                                                    const flowie_cluster_peer_frame_t *frame);
typedef int (*flowie_cluster_node_edge_receive_fn)(void *ctx,
                                                   const flowie_cluster_peer_frame_t *frame);

typedef struct flowie_cluster_node_router_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_count;
  size_t max_links;
  size_t max_inflight_sends;
  tstr_v cluster_id;
  tstr_v listener_id;
  tstr_v local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
} flowie_cluster_node_router_config_t;

#define FLOWIE_CLUSTER_NODE_ROUTER_CONFIG_INIT                                                     \
  {sizeof(flowie_cluster_node_router_config_t),                                                    \
   FLOWIE_CLUSTER_NODE_ROUTER_ABI_V1,                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {0}}

typedef struct flowie_cluster_node_router_snapshot_s {
  size_t size;
  uint32_t abi_version;
  size_t registered_shards;
  size_t registered_edges;
  size_t draining_shards;
  size_t inflight_routes;
  int closing;
  flowie_cluster_peer_registry_snapshot_t peers;
} flowie_cluster_node_router_snapshot_t;

#define FLOWIE_CLUSTER_NODE_ROUTER_SNAPSHOT_INIT                                                   \
  {                                                                                                \
      sizeof(flowie_cluster_node_router_snapshot_t),                                               \
      FLOWIE_CLUSTER_NODE_ROUTER_ABI_V1,                                                           \
      0u,                                                                                          \
      0u,                                                                                          \
      0u,                                                                                          \
      0u,                                                                                          \
      0,                                                                                           \
      FLOWIE_CLUSTER_PEER_REGISTRY_SNAPSHOT_INIT}

typedef struct flowie_cluster_node_router_s flowie_cluster_node_router_t;

/**
 * Internal node-local routing composition. PostgreSQL ownership remains the
 * authority checked by each shard runtime; this table only selects a borrowed
 * local runtime and an authenticated borrowed peer link.
 */
int flowie_cluster_node_router_create(const flowie_cluster_node_router_config_t *config,
                                      flowie_cluster_node_router_t **out);
int flowie_cluster_node_router_register_shard(flowie_cluster_node_router_t *router,
                                              uint32_t shard_id,
                                              flowie_cluster_node_shard_receive_fn receive,
                                              void *receive_ctx);
/** EBUSY stops new routes; retry after accepted receive calls return. */
int flowie_cluster_node_router_unregister_shard(flowie_cluster_node_router_t *router,
                                                uint32_t shard_id,
                                                flowie_cluster_node_shard_receive_fn receive,
                                                void *receive_ctx);
int flowie_cluster_node_router_register_runtime(flowie_cluster_node_router_t *router,
                                                uint32_t shard_id,
                                                flowie_cluster_shard_runtime_t *runtime);
int flowie_cluster_node_router_unregister_runtime(flowie_cluster_node_router_t *router,
                                                  uint32_t shard_id,
                                                  flowie_cluster_shard_runtime_t *runtime);

/** Register the one socket-owning edge adapter for this cluster/listener node. */
int flowie_cluster_node_router_register_edge(flowie_cluster_node_router_t *router,
                                             flowie_cluster_node_edge_receive_fn receive,
                                             void *receive_ctx);
/** EBUSY stops new edge routes; retry after accepted receive calls return. */
int flowie_cluster_node_router_unregister_edge(flowie_cluster_node_router_t *router,
                                               flowie_cluster_node_edge_receive_fn receive,
                                               void *receive_ctx);

int flowie_cluster_node_router_register_link(
    flowie_cluster_node_router_t *router, tstr_v remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_peer_link_t *link);
int flowie_cluster_node_router_unregister_link(
    flowie_cluster_node_router_t *router, tstr_v remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_peer_link_t *link);

/** Peer receive callback: validates the exact local identity then takes a bounded shard lease. */
int flowie_cluster_node_router_receive(void *ctx, const flowie_cluster_peer_frame_t *frame);
/** Dispatcher-compatible send callback with exact local-node loopback. */
int flowie_cluster_node_router_send(void *ctx, const flowie_cluster_peer_frame_t *frame,
                                    flowie_cluster_peer_send_complete_fn complete,
                                    void *complete_ctx);

int flowie_cluster_node_router_snapshot(flowie_cluster_node_router_t *router,
                                        flowie_cluster_node_router_snapshot_t *out);
int flowie_cluster_node_router_close(flowie_cluster_node_router_t *router);
int flowie_cluster_node_router_drain(flowie_cluster_node_router_t *router, uint64_t timeout_ns);
/** Requires every borrowed shard runtime and peer link to be unregistered. */
int flowie_cluster_node_router_destroy(flowie_cluster_node_router_t *router);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_NODE_ROUTER_INTERNAL_H */
