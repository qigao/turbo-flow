#ifndef FLOWIE_CLUSTER_DELIVERY_DISPATCH_INTERNAL_H
#define FLOWIE_CLUSTER_DELIVERY_DISPATCH_INTERNAL_H

#include "flowie_cluster_delivery_action_internal.h"
#include "flowie_cluster_takeover_dispatch_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_DELIVERY_DISPATCH_ABI_V1 1u

/** Prepared EDGE_ACTION frame owns all variable-length views until cleanup. */
typedef flowie_cluster_takeover_command_t flowie_cluster_delivery_command_t;
#define FLOWIE_CLUSTER_DELIVERY_COMMAND_INIT FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT

int flowie_cluster_delivery_dispatch_prepare(const flowie_cluster_pgsql_outbox_event_t *event,
                                             const flowie_cluster_owner_token_t *current_owner,
                                             tstr_v cluster_id, tstr_v listener_id,
                                             size_t max_payload_size,
                                             flowie_cluster_delivery_command_t *out);
int flowie_cluster_delivery_dispatch_reply_inspect(
    const flowie_cluster_delivery_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size, int *reply_status);
int flowie_cluster_delivery_dispatch_reply_validate(
    const flowie_cluster_delivery_command_t *command, const flowie_cluster_peer_frame_t *reply,
    size_t max_payload_size);
void flowie_cluster_delivery_command_cleanup(flowie_cluster_delivery_command_t *command);

typedef flowie_cluster_takeover_owner_resolve_fn flowie_cluster_delivery_owner_resolve_fn;
typedef flowie_cluster_takeover_source_fetch_fn flowie_cluster_delivery_source_fetch_fn;
typedef flowie_cluster_takeover_source_settle_fn flowie_cluster_delivery_source_settle_fn;
typedef flowie_cluster_takeover_source_recover_fn flowie_cluster_delivery_source_recover_fn;
typedef flowie_cluster_takeover_send_fn flowie_cluster_delivery_send_fn;

typedef struct flowie_cluster_delivery_dispatcher_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t reply_timeout_ns;
  tstr_v cluster_id;
  tstr_v listener_id;
  flowie_cluster_delivery_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_delivery_source_fetch_fn fetch;
  flowie_cluster_delivery_source_settle_fn settle;
  flowie_cluster_delivery_source_recover_fn recover;
  void *source_ctx;
  flowie_cluster_delivery_send_fn send;
  void *send_ctx;
} flowie_cluster_delivery_dispatcher_config_t;

#define FLOWIE_CLUSTER_DELIVERY_DISPATCHER_CONFIG_INIT                                            \
  {sizeof(flowie_cluster_delivery_dispatcher_config_t),                                           \
   FLOWIE_CLUSTER_DELIVERY_DISPATCH_ABI_V1,                                                       \
   0u,                                                                                            \
   0u,                                                                                            \
   0u,                                                                                            \
   0u,                                                                                            \
   0u,                                                                                            \
   {NULL, 0u},                                                                                    \
   {NULL, 0u},                                                                                    \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL,                                                                                          \
   NULL}

typedef flowie_cluster_takeover_dispatcher_t flowie_cluster_delivery_dispatcher_t;
typedef flowie_cluster_takeover_dispatcher_state_t flowie_cluster_delivery_dispatcher_state_t;
typedef flowie_cluster_takeover_dispatcher_snapshot_t flowie_cluster_delivery_dispatcher_snapshot_t;
#define FLOWIE_CLUSTER_DELIVERY_DISPATCHER_SNAPSHOT_INIT                                          \
  FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SNAPSHOT_INIT

int flowie_cluster_delivery_dispatcher_config_validate(
    const flowie_cluster_delivery_dispatcher_config_t *config);
int flowie_cluster_delivery_dispatcher_create(
    const flowie_cluster_delivery_dispatcher_config_t *config,
    flowie_cluster_delivery_dispatcher_t **out);
int flowie_cluster_delivery_dispatcher_reply(flowie_cluster_delivery_dispatcher_t *dispatcher,
                                             const flowie_cluster_peer_frame_t *reply);
int flowie_cluster_delivery_dispatcher_snapshot(
    flowie_cluster_delivery_dispatcher_t *dispatcher,
    flowie_cluster_delivery_dispatcher_snapshot_t *out);
int flowie_cluster_delivery_dispatcher_close(flowie_cluster_delivery_dispatcher_t *dispatcher);
int flowie_cluster_delivery_dispatcher_drain(flowie_cluster_delivery_dispatcher_t *dispatcher,
                                             uint64_t timeout_ns);
int flowie_cluster_delivery_dispatcher_destroy(flowie_cluster_delivery_dispatcher_t *dispatcher);

typedef struct flowie_cluster_delivery_pgsql_source_s flowie_cluster_delivery_pgsql_source_t;
int flowie_cluster_delivery_pgsql_source_create(const flowie_cluster_pgsql_fact_config_t *config,
                                                flowie_cluster_delivery_pgsql_source_t **out);
int flowie_cluster_delivery_pgsql_source_fetch(void *ctx,
                                               const flowie_cluster_owner_token_t *current_owner,
                                               flowie_cluster_pgsql_outbox_event_t *out);
int flowie_cluster_delivery_pgsql_source_settle(void *ctx,
                                                const flowie_cluster_owner_token_t *current_owner,
                                                const flowie_cluster_pgsql_outbox_event_t *event);
int flowie_cluster_delivery_pgsql_source_recover(void *ctx);
void flowie_cluster_delivery_pgsql_source_destroy(flowie_cluster_delivery_pgsql_source_t *source);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_DELIVERY_DISPATCH_INTERNAL_H */
