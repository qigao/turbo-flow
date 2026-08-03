#ifndef FLOWIE_CLUSTER_BROADCAST_DISPATCH_INTERNAL_H
#define FLOWIE_CLUSTER_BROADCAST_DISPATCH_INTERNAL_H

#include "flowie_cluster_broadcast_event_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_BROADCAST_DISPATCH_ABI_V1 1u

typedef int (*flowie_cluster_broadcast_owner_resolve_fn)(void *ctx, uint32_t shard_id,
                                                         flowie_cluster_owner_token_t *out);
typedef int (*flowie_cluster_broadcast_source_fetch_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out);
typedef int (*flowie_cluster_broadcast_source_settle_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event);
typedef int (*flowie_cluster_broadcast_source_ack_count_fn)(
    void *ctx, const flowie_cluster_pgsql_event_dedupe_t *source_event, size_t *out_count);
typedef int (*flowie_cluster_broadcast_source_recover_fn)(void *ctx);
typedef int (*flowie_cluster_broadcast_publish_fn)(void *ctx, const void *payload,
                                                   size_t payload_size);

typedef enum flowie_cluster_broadcast_dispatcher_state_e {
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CREATED = 1,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_FETCHING,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_PUBLISHING,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_WAITING_ACK,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_SETTLING,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_RETRY_WAIT,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CLOSING,
  FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CLOSED
} flowie_cluster_broadcast_dispatcher_state_t;

typedef struct flowie_cluster_broadcast_dispatcher_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  uint32_t shard_count;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t ack_poll_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t republish_interval_ns;
  flowie_cluster_broadcast_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_broadcast_source_fetch_fn fetch;
  flowie_cluster_broadcast_source_settle_fn settle;
  flowie_cluster_broadcast_source_ack_count_fn ack_count;
  flowie_cluster_broadcast_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_broadcast_publish_fn publish;
  void *publish_ctx;
} flowie_cluster_broadcast_dispatcher_config_t;

#define FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CONFIG_INIT                                            \
  {sizeof(flowie_cluster_broadcast_dispatcher_config_t),                                           \
   FLOWIE_CLUSTER_BROADCAST_DISPATCH_ABI_V1,                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef struct flowie_cluster_broadcast_dispatcher_s flowie_cluster_broadcast_dispatcher_t;

typedef struct flowie_cluster_broadcast_dispatcher_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_broadcast_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t publish_attempts;
  uint64_t ack_queries;
  uint64_t settled_events;
  uint64_t source_attempt_count;
  size_t observed_shard_acks;
} flowie_cluster_broadcast_dispatcher_snapshot_t;

#define FLOWIE_CLUSTER_BROADCAST_DISPATCHER_SNAPSHOT_INIT                                          \
  {sizeof(flowie_cluster_broadcast_dispatcher_snapshot_t),                                         \
   FLOWIE_CLUSTER_BROADCAST_DISPATCH_ABI_V1,                                                       \
   FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CREATED,                                                    \
   TURBO_OK,                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

int flowie_cluster_broadcast_dispatcher_config_validate(
    const flowie_cluster_broadcast_dispatcher_config_t *config);
int flowie_cluster_broadcast_dispatcher_create(
    const flowie_cluster_broadcast_dispatcher_config_t *config,
    flowie_cluster_broadcast_dispatcher_t **out);
int flowie_cluster_broadcast_dispatcher_snapshot(
    flowie_cluster_broadcast_dispatcher_t *dispatcher,
    flowie_cluster_broadcast_dispatcher_snapshot_t *out);
int flowie_cluster_broadcast_dispatcher_close(flowie_cluster_broadcast_dispatcher_t *dispatcher);
int flowie_cluster_broadcast_dispatcher_drain(flowie_cluster_broadcast_dispatcher_t *dispatcher,
                                              uint64_t timeout_ns);
int flowie_cluster_broadcast_dispatcher_destroy(flowie_cluster_broadcast_dispatcher_t *dispatcher);

typedef struct flowie_cluster_broadcast_pgsql_source_s flowie_cluster_broadcast_pgsql_source_t;
int flowie_cluster_broadcast_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                 flowie_cluster_broadcast_pgsql_source_t **out);
int flowie_cluster_broadcast_pgsql_source_fetch(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out);
int flowie_cluster_broadcast_pgsql_source_settle(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event);
int flowie_cluster_broadcast_pgsql_source_ack_count(
    void *ctx, const flowie_cluster_pgsql_event_dedupe_t *source_event, size_t *out_count);
int flowie_cluster_broadcast_pgsql_source_recover(void *ctx);
void flowie_cluster_broadcast_pgsql_source_destroy(flowie_cluster_broadcast_pgsql_source_t *source);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_BROADCAST_DISPATCH_INTERNAL_H */
