#ifndef FLOWIE_CLUSTER_TAKEOVER_DISPATCH_INTERNAL_H
#define FLOWIE_CLUSTER_TAKEOVER_DISPATCH_INTERNAL_H

#include "flowie_cluster_pgsql_internal.h"
#include "flowie_cluster_session_bind_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1 1u

typedef int (*flowie_cluster_takeover_owner_resolve_fn)(void *ctx, uint32_t shard_id,
                                                        flowie_cluster_owner_token_t *out);
typedef int (*flowie_cluster_takeover_source_fetch_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out);
typedef int (*flowie_cluster_takeover_source_settle_fn)(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event);
typedef int (*flowie_cluster_takeover_source_recover_fn)(void *ctx);
typedef int (*flowie_cluster_takeover_send_fn)(void *ctx, const flowie_cluster_peer_frame_t *frame,
                                               flowie_cluster_peer_send_complete_fn complete,
                                               void *complete_ctx);
/*
 * A successful send must own/copy every frame view before returning and invoke
 * complete exactly once. A failed send must not invoke complete.
 */

/** Prepared frame owns every variable-length frame view until cleanup. */
typedef struct flowie_cluster_takeover_command_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_peer_frame_t frame;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t source_node_id;
  tstr_t target_node_id;
  tstr_t payload;
} flowie_cluster_takeover_command_t;

#define FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT                                                       \
  {sizeof(flowie_cluster_takeover_command_t),                                                      \
   FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1,                                                        \
   FLOWIE_CLUSTER_PEER_FRAME_INIT,                                                                 \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * Convert a durable TFTE row into an exact-generation TFTC peer command.
 * current_owner must be the caller's live PostgreSQL-derived shard token; an
 * event from an older epoch is intentionally replayable by that current owner.
 */
int flowie_cluster_takeover_dispatch_prepare(const flowie_cluster_pgsql_outbox_event_t *event,
                                             const flowie_cluster_owner_token_t *current_owner,
                                             tstr_v cluster_id, tstr_v listener_id,
                                             size_t max_payload_size,
                                             flowie_cluster_takeover_command_t *out);

/**
 * Validate the edge's exact reverse-route TFRP acknowledgement. A valid
 * nonzero reply status is returned unchanged and must leave the outbox row
 * pending; only TURBO_OK authorizes settlement.
 */
int flowie_cluster_takeover_dispatch_reply_validate(
    const flowie_cluster_takeover_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size);
/** Structural validation separated from the peer's operational status. */
int flowie_cluster_takeover_dispatch_reply_inspect(const flowie_cluster_takeover_command_t *command,
                                                   const flowie_cluster_peer_frame_t *reply,
                                                   size_t max_payload_size, int *reply_status);

typedef int (*flowie_cluster_peer_outbox_prepare_fn)(
    const flowie_cluster_pgsql_outbox_event_t *event,
    const flowie_cluster_owner_token_t *current_owner, tstr_v cluster_id, tstr_v listener_id,
    size_t max_payload_size, flowie_cluster_takeover_command_t *out);
typedef int (*flowie_cluster_peer_outbox_reply_inspect_fn)(
    const flowie_cluster_takeover_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size, int *reply_status);

void flowie_cluster_takeover_command_cleanup(flowie_cluster_takeover_command_t *command);

typedef enum flowie_cluster_takeover_dispatcher_state_e {
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CREATED = 1,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_FETCHING,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SENDING,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_WAITING_REPLY,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SETTLING,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_RETRY_WAIT,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSING,
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CLOSED
} flowie_cluster_takeover_dispatcher_state_t;

/**
 * One dispatcher owns one blocking worker, one source connection and at most
 * one fetched event/peer send. The source remains the durable queue; all
 * retry waits are bounded and keep the event unpublished. Callback contexts
 * must outlive close() followed by a successful drain().
 */
typedef struct flowie_cluster_takeover_dispatcher_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t reply_timeout_ns;
  tstr_v cluster_id;
  tstr_v listener_id;
  flowie_cluster_takeover_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_takeover_source_fetch_fn fetch;
  flowie_cluster_takeover_source_settle_fn settle;
  flowie_cluster_outbox_before_settle_fn before_settle;
  void *before_settle_ctx;
  flowie_cluster_takeover_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_takeover_send_fn send;
  void *send_ctx;
} flowie_cluster_takeover_dispatcher_config_t;

#define FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CONFIG_INIT                                             \
  {sizeof(flowie_cluster_takeover_dispatcher_config_t),                                            \
   FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1,                                                        \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
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

typedef struct flowie_cluster_takeover_dispatcher_s flowie_cluster_takeover_dispatcher_t;

typedef struct flowie_cluster_takeover_dispatcher_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_takeover_dispatcher_state_t state;
  int last_status;
  uint64_t fetched_events;
  uint64_t send_attempts;
  uint64_t reply_timeouts;
  uint64_t settled_events;
  uint64_t source_attempt_count;
} flowie_cluster_takeover_dispatcher_snapshot_t;

#define FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SNAPSHOT_INIT                                           \
  {sizeof(flowie_cluster_takeover_dispatcher_snapshot_t),                                          \
   FLOWIE_CLUSTER_TAKEOVER_DISPATCH_ABI_V1,                                                        \
   FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CREATED,                                                     \
   TURBO_OK,                                                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

int flowie_cluster_takeover_dispatcher_config_validate(
    const flowie_cluster_takeover_dispatcher_config_t *config);
int flowie_cluster_takeover_dispatcher_create(
    const flowie_cluster_takeover_dispatcher_config_t *config,
    flowie_cluster_takeover_dispatcher_t **out);
/**
 * Reuse the bounded one-row peer dispatcher with another immutable outbox
 * codec. Both strategies run on the dispatch worker and must not retain views.
 */
int flowie_cluster_takeover_dispatcher_create_strategy(
    const flowie_cluster_takeover_dispatcher_config_t *config,
    flowie_cluster_peer_outbox_prepare_fn prepare,
    flowie_cluster_peer_outbox_reply_inspect_fn reply_inspect,
    flowie_cluster_takeover_dispatcher_t **out);
/** Peer receive callback; exact duplicate replies are consumed idempotently. */
int flowie_cluster_takeover_dispatcher_reply(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                             const flowie_cluster_peer_frame_t *reply);
int flowie_cluster_takeover_dispatcher_snapshot(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                                flowie_cluster_takeover_dispatcher_snapshot_t *out);
int flowie_cluster_takeover_dispatcher_close(flowie_cluster_takeover_dispatcher_t *dispatcher);
/** Wait for worker exit and every accepted peer-send completion. */
int flowie_cluster_takeover_dispatcher_drain(flowie_cluster_takeover_dispatcher_t *dispatcher,
                                             uint64_t timeout_ns);
/** Requires close()+drain(); otherwise returns EBUSY. */
int flowie_cluster_takeover_dispatcher_destroy(flowie_cluster_takeover_dispatcher_t *dispatcher);

/** Blocking PostgreSQL source adapter; use only from the dispatcher worker. */
typedef struct flowie_cluster_takeover_pgsql_source_s flowie_cluster_takeover_pgsql_source_t;
int flowie_cluster_takeover_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                flowie_cluster_takeover_pgsql_source_t **out);
int flowie_cluster_takeover_pgsql_source_fetch(void *ctx,
                                               const flowie_cluster_owner_token_t *current_owner,
                                               flowie_cluster_pgsql_outbox_event_t *out);
int flowie_cluster_takeover_pgsql_source_settle(void *ctx,
                                                const flowie_cluster_owner_token_t *current_owner,
                                                const flowie_cluster_pgsql_outbox_event_t *event);
int flowie_cluster_takeover_pgsql_source_recover(void *ctx);
void flowie_cluster_takeover_pgsql_source_destroy(flowie_cluster_takeover_pgsql_source_t *source);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_TAKEOVER_DISPATCH_INTERNAL_H */
