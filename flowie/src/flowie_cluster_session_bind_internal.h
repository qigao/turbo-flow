#ifndef FLOWIE_CLUSTER_SESSION_BIND_INTERNAL_H
#define FLOWIE_CLUSTER_SESSION_BIND_INTERNAL_H

#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_pgsql_internal.h"
#include "flowie_cluster_broadcast_event_internal.h"
#include "flowie_cluster_delivery_action_internal.h"
#include "flowie_cluster_publish_event_internal.h"
#include "flowie_session_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_SESSION_BIND_ABI_V1 1u
#define FLOWIE_CLUSTER_SESSION_FACT_VERSION_V1 1u
#define FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1 24u
#define FLOWIE_CLUSTER_SESSION_FACT_VERSION 2u
#define FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE 72u
#define FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION 1u
#define FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE 68u
#define FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION 1u
#define FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE 40u
#define FLOWIE_CLUSTER_SESSION_EVENT_BOUND 1u
#define FLOWIE_CLUSTER_SESSION_EVENT_UPDATED 2u
#define FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST 3u
#define FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER 4u
#define FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE

typedef struct flowie_cluster_session_bind_reply_view_s {
  size_t size;
  uint32_t abi_version;
  uint8_t accepted;
  uint8_t close_after_reply;
  uint8_t session_present;
  turbo_flow_protocol_route_t route;
  flowie_mqtt_span_t packet;
} flowie_cluster_session_bind_reply_view_t;

#define FLOWIE_CLUSTER_SESSION_BIND_REPLY_VIEW_INIT                                                \
  {sizeof(flowie_cluster_session_bind_reply_view_t),                                               \
   FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION,                                                      \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_PROTOCOL_ROUTE_INIT,                                                                 \
   {NULL, 0u}}

typedef int (*flowie_cluster_session_fact_submit_fn)(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx);
/* submit() deep-copies command and every borrowed view before returning TURBO_OK. */
typedef uint64_t (*flowie_cluster_session_now_fn)(void *ctx);
/** May run on the PostgreSQL worker thread; it must be thread-safe and nonblocking. */
typedef void (*flowie_cluster_session_self_fence_fn)(void *ctx, int reason);

/**
 * @internal @incomplete
 * Session owner executor for CONNECT_BIND, state-only post-CONNECT packet
 * commands and abnormal CONNECTION_LOST lifecycle commands. Inbound PUBLISH
 * writes a versioned broadcast intent; QoS 0 uses an event-only transaction,
 * while QoS 1/2 atomically persist sender inflight state with that intent. A successful
 * cross-edge CONNECT takeover emits the previous binding as a durable TFTE
 * intent. Graph completion returns through a generation-fenced TFPS command
 * before sender inflight state advances. Retained replay and production rollout
 * remain gated, so this type stays NO_INSTALL and cannot independently enable clustering.
 */
typedef struct flowie_cluster_session_bind_config_s {
  size_t size;
  uint32_t abi_version;
  size_t max_sessions;
  size_t max_bind_payload_size;
  size_t max_fact_value_size;
  size_t max_event_payload_size;
  flowie_session_config_t session;
  uint64_t first_session_id;
  uint8_t security_enabled;
  flowie_cluster_session_fact_submit_fn submit;
  void *submit_ctx;
  flowie_cluster_session_now_fn now;
  void *now_ctx;
  flowie_cluster_session_self_fence_fn self_fence;
  void *self_fence_ctx;
} flowie_cluster_session_bind_config_t;

#define FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT                                                    \
  {sizeof(flowie_cluster_session_bind_config_t),                                                   \
   FLOWIE_CLUSTER_SESSION_BIND_ABI_V1,                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   FLOWIE_SESSION_CONFIG_INIT,                                                                     \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef struct flowie_cluster_session_bind_s flowie_cluster_session_bind_t;
typedef struct flowie_cluster_session_recovery_s flowie_cluster_session_recovery_t;
typedef struct flowie_cluster_session_delivery_plan_s flowie_cluster_session_delivery_plan_t;
typedef struct flowie_cluster_session_lifecycle_plan_s flowie_cluster_session_lifecycle_plan_t;
struct flowie_cluster_lifecycle_event_view_s;

/** Borrowed route fields decoded from one exact TFSE v2 fact record. */
typedef struct flowie_cluster_session_fact_route_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_span_t client_id;
  tstr_v edge_node_id;
  const uint8_t *edge_boot_id;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_id;
  uint64_t session_generation;
} flowie_cluster_session_fact_route_view_t;

#define FLOWIE_CLUSTER_SESSION_FACT_ROUTE_VIEW_INIT                                                \
  {sizeof(flowie_cluster_session_fact_route_view_t), FLOWIE_CLUSTER_SESSION_BIND_ABI_V1,          \
   {NULL, 0u}, {NULL, 0u}, NULL, 0u, 0u, 0u, 0u}

int flowie_cluster_session_fact_route_decode(
    const flowie_cluster_session_bind_config_t *config,
    const flowie_cluster_pgsql_fact_record_t *record,
    flowie_cluster_session_fact_route_view_t *out);

/** Borrowed target selected from the shard's rebuildable subscription index. */
typedef struct flowie_cluster_session_publish_target_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_span_t client_id;
  flowie_mqtt_version_t mqtt_version;
  uint64_t session_id;
  uint64_t session_generation;
  uint64_t resource_generation;
  uint32_t session_expiry_interval;
  uint8_t active;
  uint8_t qos;
  uint8_t retain_as_published;
  tstr_v edge_node_id;
  const uint8_t *edge_boot_id;
  uint64_t connection_id;
  uint64_t connection_generation;
  const uint32_t *subscription_identifiers;
  size_t subscription_identifier_count;
  /** Full $share/<group>/<filter> key; empty for ordinary subscriptions. */
  flowie_mqtt_span_t shared_filter;
} flowie_cluster_session_publish_target_t;

#define FLOWIE_CLUSTER_SESSION_PUBLISH_TARGET_INIT                                                \
  {sizeof(flowie_cluster_session_publish_target_t), FLOWIE_CLUSTER_SESSION_BIND_ABI_V1}

typedef int (*flowie_cluster_session_publish_target_visit_fn)(
    void *ctx, const flowie_cluster_session_publish_target_t *target);

int flowie_cluster_session_bind_config_validate(const flowie_cluster_session_bind_config_t *config);
int flowie_cluster_session_bind_create(const flowie_cluster_session_bind_config_t *config,
                                       flowie_cluster_session_bind_t **out);
/** Requires no outstanding execute_async completion. */
int flowie_cluster_session_bind_destroy(flowie_cluster_session_bind_t *bind);

/**
 * Builds a bounded recovery registry without changing the live owner registry.
 * The bind must remain alive, empty, and owner-lane quiescent until publish or
 * destroy. visit() is signature-compatible with turbo_flow_record_visit_fn.
 */
int flowie_cluster_session_recovery_create(flowie_cluster_session_bind_t *bind,
                                           flowie_cluster_session_recovery_t **out);
int flowie_cluster_session_recovery_visit(void *ctx, const turbo_flow_record_view_t *record);
/** Atomically replaces the still-empty live registry with the validated staging registry. */
int flowie_cluster_session_recovery_publish(flowie_cluster_session_recovery_t *recovery);
void flowie_cluster_session_recovery_destroy(flowie_cluster_session_recovery_t *recovery);

/**
 * Blocking startup composition: scan PostgreSQL into a temporary registry,
 * publish it, then activate the matching lease worker. Do not call this on a
 * CoroNet owner lane. On failure the caller must destroy the still-inactive
 * bind and lease worker rather than reuse either object.
 */
int flowie_cluster_session_bind_recover_pgsql(flowie_cluster_session_bind_t *bind,
                                              flowie_cluster_pgsql_fact_store_t *store,
                                              flowie_cluster_pgsql_lease_worker_t *lease_worker);

/** Signature-compatible with flowie_cluster_peer_owner_execute_async_fn. */
int flowie_cluster_session_bind_execute_async(void *user_data,
                                              const flowie_cluster_peer_frame_t *command,
                                              flowie_cluster_peer_owner_complete_fn complete,
                                              void *completion_ctx);

/** Owner-lane read-only view used by packet executors and recovery tests. */
int flowie_cluster_session_bind_snapshot(const flowie_cluster_session_bind_t *bind,
                                         flowie_mqtt_span_t client_id,
                                         flowie_session_snapshot_t *snapshot,
                                         turbo_flow_security_principal_t *principal);

/**
 * Match one validated durable TFPE event against this ACTIVE shard's derived
 * subscription index. Ordinary overlapping filters merge per session. Each
 * matching shared filter emits one deterministic local candidate carrying the
 * full shared key; PostgreSQL arbitrates candidates across shards in the same
 * fenced transaction as target dedupe/fact/outbox. Borrowed target fields
 * remain valid only for the duration of the callback. This owner-lane operation
 * never changes authoritative session facts.
 */
int flowie_cluster_session_bind_match_publish(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_publish_event_view_t *event,
    flowie_cluster_session_publish_target_visit_fn visit, void *visit_ctx, size_t *target_count);

/**
 * Stage one matched target delivery on the shard owner lane. The returned
 * command borrows plan-owned storage and may be submitted exactly once. A
 * Durable OK publishes staged state. EALREADY confirms a prior target replay
 * and discards the newly staged copy; other statuses abort. The plan serializes
 * against ordinary session commands.
 */
int flowie_cluster_session_delivery_plan_create(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_broadcast_event_view_t *source,
    const uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE],
    const flowie_cluster_session_publish_target_t *target,
    flowie_cluster_session_delivery_plan_t **out);
const flowie_cluster_pgsql_fact_command_t *flowie_cluster_session_delivery_plan_command(
    const flowie_cluster_session_delivery_plan_t *plan);
/** Consumes *plan and publishes staged in-memory state only after durable success. */
int flowie_cluster_session_delivery_plan_finalize(flowie_cluster_session_delivery_plan_t **plan,
                                                  int durable_status);
/** Abort an unsubmitted or failed plan and release its owner-lane reservation. */
void flowie_cluster_session_delivery_plan_destroy(flowie_cluster_session_delivery_plan_t **plan);

/**
 * Stage one decoded TFLE on the shard owner lane. A NULL-command plan is still
 * finalized: TURBO_OK settles an obsolete lifecycle event, while TURBO_EBUSY
 * keeps a not-yet-due event pending. PUBLISH_WILL atomically clears the Will
 * fact and emits TFPE; its successful finalize returns TURBO_EBUSY so the
 * original TFLE can drive later expiry. EXPIRE_SESSION deletes the fact and
 * returns TURBO_OK only after the in-memory registry follows the durable delete.
 */
int flowie_cluster_session_lifecycle_plan_create(
    flowie_cluster_session_bind_t *bind, const flowie_cluster_owner_token_t *current_owner,
    const struct flowie_cluster_lifecycle_event_view_s *event, uint64_t now_epoch_seconds,
    flowie_cluster_session_lifecycle_plan_t **out);
const flowie_cluster_pgsql_fact_command_t *flowie_cluster_session_lifecycle_plan_command(
    const flowie_cluster_session_lifecycle_plan_t *plan);
/** Consumes *plan. EALREADY self-fences because local state must be recovered. */
int flowie_cluster_session_lifecycle_plan_finalize(
    flowie_cluster_session_lifecycle_plan_t **plan, int durable_status);
void flowie_cluster_session_lifecycle_plan_destroy(
    flowie_cluster_session_lifecycle_plan_t **plan);

/** Decode the owner route and raw CONNACK action returned to an edge. */
int flowie_cluster_session_bind_reply_decode(const void *data, size_t data_size,
                                             size_t max_payload_size,
                                             flowie_cluster_session_bind_reply_view_t *out);

/** Adapter for a real bounded PostgreSQL fact worker. */
int flowie_cluster_session_bind_pgsql_submit(void *ctx,
                                             const flowie_cluster_pgsql_fact_command_t *command,
                                             flowie_cluster_pgsql_fact_completion_fn completion,
                                             void *completion_ctx);

#ifdef __cplusplus
}
#endif

#endif
