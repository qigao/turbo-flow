#ifndef FLOWIE_CLUSTER_ROUTE_DISPATCH_INTERNAL_H
#define FLOWIE_CLUSTER_ROUTE_DISPATCH_INTERNAL_H

#include "flowie_cluster_route_projection_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_ROUTE_DISPATCH_ABI_V1 1u

typedef int (*flowie_cluster_route_owner_resolve_fn)(void *ctx, uint32_t shard_id,
                                                     flowie_cluster_owner_token_t *out);

typedef int (*flowie_cluster_route_source_fetch_fn)(
    void *ctx, const flowie_cluster_owner_token_t *owner, uint64_t event_type,
    flowie_cluster_pgsql_outbox_event_t *out);
typedef int (*flowie_cluster_route_source_settle_fn)(
    void *ctx, const flowie_cluster_owner_token_t *owner,
    const flowie_cluster_pgsql_outbox_event_t *event);
typedef int (*flowie_cluster_route_source_recover_fn)(void *ctx);

typedef enum flowie_cluster_route_dispatcher_state_e {
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_CREATED = 1,
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_FETCHING,
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_PROJECTING,
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_SETTLING,
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_RETRY_WAIT,
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_CLOSING,
  FLOWIE_CLUSTER_ROUTE_DISPATCHER_CLOSED
} flowie_cluster_route_dispatcher_state_t;

typedef struct flowie_cluster_route_dispatcher_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  flowie_cluster_route_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_route_source_fetch_fn fetch;
  flowie_cluster_route_source_settle_fn settle;
  flowie_cluster_route_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_outbox_before_settle_fn project;
  void *project_ctx;
} flowie_cluster_route_dispatcher_config_t;

#define FLOWIE_CLUSTER_ROUTE_DISPATCHER_CONFIG_INIT                                                \
  {sizeof(flowie_cluster_route_dispatcher_config_t), FLOWIE_CLUSTER_ROUTE_DISPATCH_ABI_V1,         \
   0u, 0u, 0u, 0u, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}

typedef struct flowie_cluster_route_dispatcher_s flowie_cluster_route_dispatcher_t;

typedef struct flowie_cluster_route_dispatcher_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_route_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t projected_events;
  uint64_t settled_events;
} flowie_cluster_route_dispatcher_snapshot_t;

#define FLOWIE_CLUSTER_ROUTE_DISPATCHER_SNAPSHOT_INIT                                               \
  {sizeof(flowie_cluster_route_dispatcher_snapshot_t), FLOWIE_CLUSTER_ROUTE_DISPATCH_ABI_V1,       \
   FLOWIE_CLUSTER_ROUTE_DISPATCHER_CREATED, TURBO_OK, 0u, 0u, 0u}

int flowie_cluster_route_dispatcher_create(
    const flowie_cluster_route_dispatcher_config_t *config,
    flowie_cluster_route_dispatcher_t **out);
int flowie_cluster_route_dispatcher_snapshot(flowie_cluster_route_dispatcher_t *dispatcher,
                                             flowie_cluster_route_dispatcher_snapshot_t *out);
int flowie_cluster_route_dispatcher_close(flowie_cluster_route_dispatcher_t *dispatcher);
int flowie_cluster_route_dispatcher_drain(flowie_cluster_route_dispatcher_t *dispatcher,
                                          uint64_t timeout_ns);
int flowie_cluster_route_dispatcher_destroy(flowie_cluster_route_dispatcher_t *dispatcher);

typedef struct flowie_cluster_route_pgsql_source_s flowie_cluster_route_pgsql_source_t;
int flowie_cluster_route_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                             flowie_cluster_route_pgsql_source_t **out);
int flowie_cluster_route_pgsql_source_fetch(void *ctx, const flowie_cluster_owner_token_t *owner,
                                            uint64_t event_type,
                                            flowie_cluster_pgsql_outbox_event_t *out);
int flowie_cluster_route_pgsql_source_settle(void *ctx,
                                             const flowie_cluster_owner_token_t *owner,
                                             const flowie_cluster_pgsql_outbox_event_t *event);
int flowie_cluster_route_pgsql_source_recover(void *ctx);
void flowie_cluster_route_pgsql_source_destroy(flowie_cluster_route_pgsql_source_t *source);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_ROUTE_DISPATCH_INTERNAL_H */
