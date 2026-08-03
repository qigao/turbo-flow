#ifndef FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_INTERNAL_H
#define FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_INTERNAL_H

#include "flowie_cluster_pgsql_internal.h"
#include "flowie_cluster_session_bind_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1 1u

typedef int (*flowie_cluster_lifecycle_owner_resolve_fn)(void *ctx, uint32_t shard_id,
                                                         flowie_cluster_owner_token_t *out);
typedef int (*flowie_cluster_lifecycle_source_fetch_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out);
typedef int (*flowie_cluster_lifecycle_source_settle_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event);
typedef int (*flowie_cluster_lifecycle_source_recover_fn)(void *ctx);
typedef void (*flowie_cluster_lifecycle_apply_complete_fn)(void *ctx, int status);

typedef struct flowie_cluster_lifecycle_event_view_s {
  size_t size;
  uint32_t abi_version;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  uint32_t shard_id;
  uint64_t event_owner_epoch;
  uint64_t expected_fact_revision;
  uint64_t created_at_epoch_seconds;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_id;
  uint64_t session_generation;
  flowie_mqtt_span_t client_id;
  flowie_mqtt_span_t edge_node_id;
  const uint8_t *edge_boot_id;
} flowie_cluster_lifecycle_event_view_t;

#define FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT                                                   \
  {sizeof(flowie_cluster_lifecycle_event_view_t),                                                  \
   FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1,                                                       \
   {0},                                                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   NULL}

typedef enum flowie_cluster_lifecycle_action_e {
  FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE = 1,
  FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT,
  FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL,
  FLOWIE_CLUSTER_LIFECYCLE_ACTION_EXPIRE_SESSION
} flowie_cluster_lifecycle_action_t;

typedef struct flowie_cluster_lifecycle_decision_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_lifecycle_action_t action;
  uint64_t deadline_epoch_seconds;
} flowie_cluster_lifecycle_decision_t;

#define FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT                                                    \
  {sizeof(flowie_cluster_lifecycle_decision_t), FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1,         \
   FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE, 0u}

/** Decode one durable TFLE row. Every variable-length view borrows event storage. */
int flowie_cluster_lifecycle_event_decode(const flowie_cluster_pgsql_outbox_event_t *event,
                                          size_t max_payload_size,
                                          flowie_cluster_lifecycle_event_view_t *out);

/** Pure authoritative-time decision used by the owner-lane lifecycle executor. */
int flowie_cluster_lifecycle_decide(const flowie_cluster_lifecycle_event_view_t *event,
                                    const flowie_session_snapshot_t *snapshot,
                                    uint64_t now_epoch_seconds,
                                    flowie_cluster_lifecycle_decision_t *out);

/**
 * The event and owner views remain valid until complete is called. A successful
 * admission must invoke complete exactly once; a failed admission must not call it.
 * TURBO_OK completion authorizes outbox settlement only after the lifecycle action
 * is durably materialized or the exact fact revision is proven obsolete.
 */
typedef int (*flowie_cluster_lifecycle_apply_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_lifecycle_event_view_t *event,
    flowie_cluster_lifecycle_apply_complete_fn complete, void *complete_ctx);

typedef enum flowie_cluster_lifecycle_dispatcher_state_e {
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CREATED = 1,
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_FETCHING,
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_APPLYING,
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_SETTLING,
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_RETRY_WAIT,
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSING,
  FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CLOSED
} flowie_cluster_lifecycle_dispatcher_state_t;

typedef struct flowie_cluster_lifecycle_dispatcher_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  flowie_cluster_lifecycle_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_lifecycle_source_fetch_fn fetch;
  flowie_cluster_lifecycle_source_settle_fn settle;
  flowie_cluster_outbox_before_settle_fn before_settle;
  void *before_settle_ctx;
  flowie_cluster_lifecycle_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_lifecycle_apply_fn apply;
  void *apply_ctx;
} flowie_cluster_lifecycle_dispatcher_config_t;

#define FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CONFIG_INIT                                            \
  {sizeof(flowie_cluster_lifecycle_dispatcher_config_t),                                           \
   FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1,                                                       \
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
   NULL,                                                                                           \
   NULL}

typedef struct flowie_cluster_lifecycle_dispatcher_s flowie_cluster_lifecycle_dispatcher_t;

typedef struct flowie_cluster_lifecycle_dispatcher_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_lifecycle_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t apply_attempts;
  uint64_t settled_events;
  uint64_t source_attempt_count;
} flowie_cluster_lifecycle_dispatcher_snapshot_t;

#define FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_SNAPSHOT_INIT                                          \
  {sizeof(flowie_cluster_lifecycle_dispatcher_snapshot_t),                                         \
   FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_ABI_V1,                                                       \
   FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CREATED,                                                    \
   TURBO_OK,                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

int flowie_cluster_lifecycle_dispatcher_config_validate(
    const flowie_cluster_lifecycle_dispatcher_config_t *config);
int flowie_cluster_lifecycle_dispatcher_create(
    const flowie_cluster_lifecycle_dispatcher_config_t *config,
    flowie_cluster_lifecycle_dispatcher_t **out);
int flowie_cluster_lifecycle_dispatcher_snapshot(
    flowie_cluster_lifecycle_dispatcher_t *dispatcher,
    flowie_cluster_lifecycle_dispatcher_snapshot_t *out);
int flowie_cluster_lifecycle_dispatcher_close(flowie_cluster_lifecycle_dispatcher_t *dispatcher);
/** Wait for worker exit and every accepted apply completion. */
int flowie_cluster_lifecycle_dispatcher_drain(flowie_cluster_lifecycle_dispatcher_t *dispatcher,
                                              uint64_t timeout_ns);
/** Requires close()+drain(); otherwise returns EBUSY. */
int flowie_cluster_lifecycle_dispatcher_destroy(flowie_cluster_lifecycle_dispatcher_t *dispatcher);

typedef struct flowie_cluster_lifecycle_pgsql_source_s flowie_cluster_lifecycle_pgsql_source_t;
int flowie_cluster_lifecycle_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                 flowie_cluster_lifecycle_pgsql_source_t **out);
int flowie_cluster_lifecycle_pgsql_source_fetch(void *ctx,
                                                const flowie_cluster_owner_token_t *current_owner,
                                                flowie_cluster_pgsql_outbox_event_t *out);
int flowie_cluster_lifecycle_pgsql_source_settle(void *ctx,
                                                 const flowie_cluster_owner_token_t *current_owner,
                                                 const flowie_cluster_pgsql_outbox_event_t *event);
int flowie_cluster_lifecycle_pgsql_source_recover(void *ctx);
void flowie_cluster_lifecycle_pgsql_source_destroy(flowie_cluster_lifecycle_pgsql_source_t *source);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_LIFECYCLE_DISPATCH_INTERNAL_H */
