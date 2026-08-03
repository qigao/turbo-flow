#ifndef FLOWIE_CLUSTER_NODE_INTERNAL_H
#define FLOWIE_CLUSTER_NODE_INTERNAL_H

#include "flowie_cluster_membership_runtime_internal.h"
#include "flowie_cluster_peer_authority_internal.h"
#include "flowie_cluster_peer_connector_internal.h"
#include "flowie_cluster_peer_listener_internal.h"
#include "flowie_cluster_redis_bus_internal.h"
#include "flowie_cluster_topology_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_NODE_ABI_V1 1u

typedef struct flowie_cluster_node_router_api_s {
  int (*create)(const flowie_cluster_node_router_config_t *config,
                flowie_cluster_node_router_t **out);
  int (*register_runtime)(flowie_cluster_node_router_t *router, uint32_t shard_id,
                          flowie_cluster_shard_runtime_t *runtime);
  int (*unregister_runtime)(flowie_cluster_node_router_t *router, uint32_t shard_id,
                            flowie_cluster_shard_runtime_t *runtime);
  int (*register_edge)(flowie_cluster_node_router_t *router,
                       flowie_cluster_node_edge_receive_fn receive, void *receive_ctx);
  int (*unregister_edge)(flowie_cluster_node_router_t *router,
                         flowie_cluster_node_edge_receive_fn receive, void *receive_ctx);
  int (*send)(void *router, const flowie_cluster_peer_frame_t *frame,
              flowie_cluster_peer_send_complete_fn complete, void *complete_ctx);
  int (*close)(flowie_cluster_node_router_t *router);
  int (*drain)(flowie_cluster_node_router_t *router, uint64_t timeout_ns);
  int (*destroy)(flowie_cluster_node_router_t *router);
} flowie_cluster_node_router_api_t;

typedef struct flowie_cluster_node_redis_api_s {
  int (*bus_create)(const flowie_cluster_redis_bus_config_t *config,
                    flowie_cluster_redis_bus_t **out);
  int (*target_create)(flowie_cluster_redis_bus_t *bus, uint32_t shard_id,
                       flowie_cluster_redis_target_t **out);
  int (*publish)(void *bus, const void *payload, size_t payload_size);
  int (*target_claim)(void *target, flowie_cluster_broadcast_target_claim_t *out);
  int (*target_ack)(void *target, uint64_t token);
  int (*target_requeue)(void *target, uint64_t token);
  void (*target_destroy)(flowie_cluster_redis_target_t *target);
  int (*bus_destroy)(flowie_cluster_redis_bus_t *bus);
  flowie_cluster_route_store_t *(*route_store)(flowie_cluster_redis_bus_t *bus);
} flowie_cluster_node_redis_api_t;

typedef struct flowie_cluster_node_shard_api_s {
  int (*create)(const flowie_cluster_shard_runtime_config_t *config,
                flowie_cluster_shard_runtime_t **out);
  int (*close)(flowie_cluster_shard_runtime_t *runtime, uint64_t timeout_ns);
  int (*destroy)(flowie_cluster_shard_runtime_t *runtime);
} flowie_cluster_node_shard_api_t;

typedef struct flowie_cluster_node_listener_api_s {
  int (*create)(const flowie_cluster_peer_listener_config_t *config,
                flowie_cluster_peer_listener_t **out);
  int (*start)(flowie_cluster_peer_listener_t *listener, uint64_t timeout_ns);
  int (*close)(flowie_cluster_peer_listener_t *listener, uint64_t timeout_ns);
  int (*drain)(flowie_cluster_peer_listener_t *listener, uint64_t timeout_ns);
  int (*destroy)(flowie_cluster_peer_listener_t *listener);
} flowie_cluster_node_listener_api_t;

typedef struct flowie_cluster_node_connector_api_s {
  int (*create)(const flowie_cluster_peer_connector_config_t *config,
                flowie_cluster_peer_connector_t **out);
  int (*start)(flowie_cluster_peer_connector_t *connector, uint64_t timeout_ns);
  int (*close)(flowie_cluster_peer_connector_t *connector, uint64_t timeout_ns);
  int (*drain)(flowie_cluster_peer_connector_t *connector, uint64_t timeout_ns);
  int (*destroy)(flowie_cluster_peer_connector_t *connector);
} flowie_cluster_node_connector_api_t;

typedef struct flowie_cluster_node_membership_api_s {
  int (*create)(const flowie_cluster_membership_runtime_config_t *config,
                flowie_cluster_membership_runtime_t **out);
  int (*start)(flowie_cluster_membership_runtime_t *runtime);
  int (*close)(flowie_cluster_membership_runtime_t *runtime, uint64_t timeout_ns);
  void (*destroy)(flowie_cluster_membership_runtime_t *runtime);
} flowie_cluster_node_membership_api_t;

/** Resolve one PostgreSQL-owned topology peer into a complete connector config. */
typedef int (*flowie_cluster_node_connector_configure_fn)(
    void *ctx, const flowie_cluster_topology_peer_t *peer,
    flowie_cluster_peer_connector_config_t *out);

/** Internal dependency boundary used only to test composition and failure rollback. */
typedef struct flowie_cluster_node_api_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_node_router_api_t router;
  flowie_cluster_node_redis_api_t redis;
  flowie_cluster_node_shard_api_t shard;
  flowie_cluster_node_listener_api_t listener;
  flowie_cluster_node_connector_api_t connector;
  flowie_cluster_node_membership_api_t membership;
} flowie_cluster_node_api_t;

typedef struct flowie_cluster_node_config_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_node_router_config_t router;
  flowie_cluster_redis_bus_config_t redis;
  flowie_cluster_peer_listener_config_t listener;
  const flowie_cluster_shard_runtime_config_t *local_shards;
  size_t local_shard_count;
  /** Initial connector configs and PostgreSQL-derived identities are index-aligned. */
  const flowie_cluster_peer_connector_config_t *connectors;
  const flowie_cluster_topology_peer_t *connector_peers;
  size_t connector_count;
  uint64_t topology_revision;
  flowie_cluster_node_connector_configure_fn connector_configure;
  void *connector_configure_ctx;
  /** Optional root-owned authorization map; callbacks in peer configs must then be unset. */
  const flowie_cluster_peer_authority_config_t *peer_authority;
  /** Optional scheduler; topology callbacks are supplied by this composition root. */
  const flowie_cluster_membership_runtime_config_t *membership;
} flowie_cluster_node_config_t;

#define FLOWIE_CLUSTER_NODE_CONFIG_INIT                                                            \
  {sizeof(flowie_cluster_node_config_t),                                                           \
   FLOWIE_CLUSTER_NODE_ABI_V1,                                                                     \
   FLOWIE_CLUSTER_NODE_ROUTER_CONFIG_INIT,                                                         \
   FLOWIE_CLUSTER_REDIS_BUS_CONFIG_INIT,                                                           \
   FLOWIE_CLUSTER_PEER_LISTENER_CONFIG_INIT,                                                       \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef struct flowie_cluster_node_s flowie_cluster_node_t;

/**
 * Create one internally composed cluster generation. The node owns every
 * created component, but the CoroNet execution and nested PostgreSQL/session
 * configuration views remain borrowed through destroy(). Transport callbacks
 * in local_shards and router pointers in peer configs must be unset because
 * this composition supplies the single node router and Redis bus.
 */
int flowie_cluster_node_create(const flowie_cluster_node_config_t *config,
                               flowie_cluster_node_t **out);

/** Internal test seam; api is borrowed and must outlive the node. */
int flowie_cluster_node_create_with_api(const flowie_cluster_node_config_t *config,
                                        const flowie_cluster_node_api_t *api,
                                        flowie_cluster_node_t **out);

/** Start peer admission after every local shard is registered and authoritative. */
int flowie_cluster_node_start(flowie_cluster_node_t *node, uint64_t timeout_ns);

/** Borrow one socket-owning edge receive port until it is explicitly unregistered. */
int flowie_cluster_node_register_edge(flowie_cluster_node_t *node,
                                      flowie_cluster_node_edge_receive_fn receive,
                                      void *receive_ctx);
int flowie_cluster_node_unregister_edge(flowie_cluster_node_t *node,
                                        flowie_cluster_node_edge_receive_fn receive,
                                        void *receive_ctx);

/** Edge-bind-compatible command submission through local loopback or an exact peer link. */
int flowie_cluster_node_submit(void *ctx, const flowie_cluster_peer_frame_t *frame);

/** Copy the current strictly node-id ordered connector topology as borrowed views. */
int flowie_cluster_node_topology_snapshot(const flowie_cluster_node_t *node,
                                          flowie_cluster_topology_peer_t *storage, size_t capacity,
                                          size_t *out_count, uint64_t *out_revision);

/**
 * Apply one REMOVE-before-ADD topology plan on the single topology owner.
 * When membership scheduling is configured, its worker is that owner and must
 * be joined before any other thread calls close. Revision advances only after
 * every operation succeeds; a partial plan is retryable because exact missing
 * REMOVE and exact existing ADD are idempotent.
 */
int flowie_cluster_node_apply_topology(flowie_cluster_node_t *node,
                                       const flowie_cluster_topology_plan_t *plan,
                                       uint64_t timeout_ns);

/**
 * Irreversibly stop peer admission, drain routing, unregister and fence local
 * shard runtimes. A timeout leaves the node valid for another close attempt.
 */
int flowie_cluster_node_close(flowie_cluster_node_t *node, uint64_t timeout_ns);

/** Close with an unbounded drain if needed and release the complete generation. */
int flowie_cluster_node_destroy(flowie_cluster_node_t *node);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_NODE_INTERNAL_H */
