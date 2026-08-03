#ifndef FLOWIE_CLUSTER_REDIS_BUS_INTERNAL_H
#define FLOWIE_CLUSTER_REDIS_BUS_INTERNAL_H

#include "flowie_cluster_broadcast_target_dispatch_internal.h"
#include "flowie_cluster_route_store_internal.h"
#include "turbo_flow_redis.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_REDIS_BUS_ABI_V1 1u

typedef struct flowie_cluster_redis_api_s {
  size_t size;
  uint32_t abi_version;
  int (*publisher_create)(const turbo_flow_redis_stream_publisher_config_t *config,
                          turbo_flow_redis_stream_publisher_t **out);
  void (*publisher_destroy)(turbo_flow_redis_stream_publisher_t *publisher);
  int (*publisher_publish)(void *publisher, const void *payload, size_t payload_size);
  int (*owner_create_ex)(const turbo_flow_redis_stream_config_t *config,
                         const turbo_flow_redis_stream_claim_owner_config_t *claim_config,
                         turbo_flow_redis_stream_owner_t **out);
  void (*owner_destroy)(turbo_flow_redis_stream_owner_t *owner);
  int (*owner_claim)(turbo_flow_redis_stream_owner_t *owner,
                     turbo_flow_redis_stream_claim_t *claim);
  int (*owner_ack)(turbo_flow_redis_stream_owner_t *owner, uint64_t token);
  int (*owner_requeue)(turbo_flow_redis_stream_owner_t *owner, uint64_t token);
} flowie_cluster_redis_api_t;

#define FLOWIE_CLUSTER_REDIS_API_INIT                                                             \
  {sizeof(flowie_cluster_redis_api_t), FLOWIE_CLUSTER_REDIS_BUS_ABI_V1, NULL, NULL, NULL, NULL,   \
   NULL, NULL, NULL, NULL}

typedef struct flowie_cluster_redis_bus_config_s {
  size_t size;
  uint32_t abi_version;
  const char *cluster_id;
  const char *listener_id;
  uint32_t shard_count;
  turbo_flow_redis_stream_publisher_config_t stream;
  uint32_t target_block_ms;
  int create_groups;
  int route_enabled;
  turbo_flow_redis_record_store_config_t route_state;
  turbo_flow_store_limits_t route_limits;
  size_t route_max_client_id_size;
  size_t route_max_endpoint_size;
  size_t route_max_cas_attempts;
} flowie_cluster_redis_bus_config_t;

#define FLOWIE_CLUSTER_REDIS_BUS_CONFIG_INIT                                                      \
  {sizeof(flowie_cluster_redis_bus_config_t), FLOWIE_CLUSTER_REDIS_BUS_ABI_V1, NULL, NULL, 0u,   \
   TURBO_FLOW_REDIS_STREAM_PUBLISHER_CONFIG_INIT, 0u, 0, 0, {0}, TURBO_FLOW_STORE_LIMITS_INIT,   \
   0u, 0u, 0u}

typedef struct flowie_cluster_redis_bus_s flowie_cluster_redis_bus_t;
typedef struct flowie_cluster_redis_target_s flowie_cluster_redis_target_t;

/**
 * Create one shared Redis publisher. Configuration strings are copied.
 * Lifecycle calls are application-owner-thread only; publish may be called by
 * multiple source dispatchers because the Redis publisher serializes XADD.
 */
int flowie_cluster_redis_bus_create(const flowie_cluster_redis_bus_config_t *config,
                                    flowie_cluster_redis_bus_t **out);

/** Internal test seam; api is borrowed and must outlive the bus. */
int flowie_cluster_redis_bus_create_with_api(const flowie_cluster_redis_bus_config_t *config,
                                             const flowie_cluster_redis_api_t *api,
                                             flowie_cluster_redis_bus_t **out);

/** Type-erased source dispatcher port. */
int flowie_cluster_redis_bus_publish(void *ctx, const void *payload, size_t payload_size);

/** Borrowed route projection store, or NULL when route_enabled is false. */
flowie_cluster_route_store_t *flowie_cluster_redis_bus_route_store(
    flowie_cluster_redis_bus_t *bus);

/**
 * Create one stable consumer-group owner for a locally owned shard. A bus may
 * have at most one live target per shard. The group starts at Redis ID 0 so a
 * late-created shard group can observe retained TFBE copies.
 */
int flowie_cluster_redis_target_create(flowie_cluster_redis_bus_t *bus, uint32_t shard_id,
                                       flowie_cluster_redis_target_t **out);

/** Target dispatcher ports; claim payload is borrowed until ACK or requeue. */
int flowie_cluster_redis_target_claim(void *ctx,
                                      flowie_cluster_broadcast_target_claim_t *out);
int flowie_cluster_redis_target_ack(void *ctx, uint64_t token);
int flowie_cluster_redis_target_requeue(void *ctx, uint64_t token);

/** Borrowed diagnostic identity, invalidated by target destruction. */
int flowie_cluster_redis_target_identity(const flowie_cluster_redis_target_t *target,
                                         tstr_v *group, tstr_v *consumer);

/**
 * The target dispatcher must be closed and drained before destruction. Any
 * unacked Redis entry remains in the stable consumer's PEL for owner recovery.
 */
void flowie_cluster_redis_target_destroy(flowie_cluster_redis_target_t *target);

/** Returns EBUSY while any target exists; source dispatchers must also be quiescent. */
int flowie_cluster_redis_bus_destroy(flowie_cluster_redis_bus_t *bus);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_REDIS_BUS_INTERNAL_H */
