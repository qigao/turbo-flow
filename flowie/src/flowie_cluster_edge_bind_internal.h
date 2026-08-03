#ifndef FLOWIE_CLUSTER_EDGE_BIND_INTERNAL_H
#define FLOWIE_CLUSTER_EDGE_BIND_INTERNAL_H

#include "flowie_cluster_session_bind_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_EDGE_BIND_ABI_V1 1u

typedef struct flowie_cluster_edge_bind_s flowie_cluster_edge_bind_t;

/** Resolve the current PostgreSQL-derived owner token for one canonical Client ID. */
typedef int (*flowie_cluster_edge_bind_resolve_fn)(void *ctx, flowie_mqtt_span_t client_id,
                                                   flowie_cluster_owner_token_t *out);

/**
 * Route one locally produced command or reply to either a local adapter or a
 * peer link. Success means the implementation copied every frame view before
 * returning.
 */
typedef int (*flowie_cluster_edge_bind_submit_fn)(void *ctx,
                                                  const flowie_cluster_peer_frame_t *command);

/**
 * Runs on the edge CoroNet owner lane. reply/frame views are borrowed for this
 * callback only and are NULL when status is not a validated owner reply.
 */
typedef void (*flowie_cluster_edge_bind_complete_fn)(
    void *ctx, int status, const flowie_cluster_peer_frame_t *frame,
    const flowie_cluster_session_bind_reply_view_t *reply);

/** Runs on the edge owner lane with a validated post-CONNECT socket action. */
typedef void (*flowie_cluster_edge_command_complete_fn)(
    void *ctx, int status, const flowie_cluster_peer_frame_t *frame,
    const flowie_cluster_peer_mqtt_reply_action_t *action);

/**
 * Close the endpoint-owned old socket with MQTT Session Taken Over semantics.
 * The callback runs on the edge owner lane and must be idempotent. TURBO_EALREADY
 * means this exact socket generation is already closing and is acknowledged as success.
 */
typedef int (*flowie_cluster_edge_takeover_close_fn)(void *ctx);

/**
 * Atomically admit one owner-issued action into the endpoint-owned ordered
 * socket output path. The callback runs on the edge owner lane, must copy every
 * borrowed packet byte before returning, and must leave no observable partial
 * effect when it returns an error.
 */
typedef int (*flowie_cluster_edge_action_apply_fn)(
    void *ctx, const flowie_cluster_peer_mqtt_reply_action_t *action);

/**
 * @internal @incomplete
 * Bounded connection-edge CONNECT_BIND port. It owns only correlation and
 * completion state; the endpoint remains the sole socket owner and resolve()
 * remains a derived routing view rather than an ownership authority.
 */
typedef struct flowie_cluster_edge_bind_config_s {
  size_t size;
  uint32_t abi_version;
  struct tf_coronet_execution_s *execution;
  size_t max_payload_size;
  size_t max_pending_entries;
  size_t max_pending_bytes;
  tstr_v cluster_id;
  tstr_v listener_id;
  tstr_v local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_edge_bind_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_edge_bind_submit_fn submit;
  void *submit_ctx;
} flowie_cluster_edge_bind_config_t;

#define FLOWIE_CLUSTER_EDGE_BIND_CONFIG_INIT                                                       \
  {sizeof(flowie_cluster_edge_bind_config_t),                                                      \
   FLOWIE_CLUSTER_EDGE_BIND_ABI_V1,                                                                \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {NULL, 0u},                                                                                     \
   {0},                                                                                            \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

int flowie_cluster_edge_bind_config_validate(const flowie_cluster_edge_bind_config_t *config);
int flowie_cluster_edge_bind_create(const flowie_cluster_edge_bind_config_t *config,
                                    flowie_cluster_edge_bind_t **out);

/**
 * Encode, correlate and submit one authenticated CONNECT. This must run on the
 * borrowed edge execution's owner lane. connection_id and connection_generation
 * identify this edge-owned socket attempt and must be non-zero.
 */
int flowie_cluster_edge_bind_connect(flowie_cluster_edge_bind_t *edge, uint64_t connection_id,
                                     uint64_t connection_generation,
                                     const flowie_mqtt_connect_view_t *connect,
                                     const turbo_flow_security_principal_t *principal,
                                     const flowie_cluster_peer_ingress_metadata_t *metadata,
                                     flowie_cluster_edge_bind_complete_fn complete,
                                     void *complete_ctx);

/**
 * Submit one post-CONNECT packet against the exact owner token installed by
 * CONNECT_BIND. resolve() is consulted again and stale ownership fails before
 * admission; the edge never reroutes a live socket command implicitly.
 */
int flowie_cluster_edge_bind_command(
    flowie_cluster_edge_bind_t *edge, uint64_t connection_id, uint64_t connection_generation,
    const flowie_cluster_owner_token_t *expected_owner, flowie_cluster_peer_operation_t operation,
    flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
    flowie_cluster_edge_command_complete_fn complete, void *complete_ctx);

/**
 * Submit one graph settlement against the CONNECT-installed owner. The edge
 * validates the current derived route, while the owner validates the durable
 * session generation carried by settlement.
 */
int flowie_cluster_edge_bind_publish_settle(
    flowie_cluster_edge_bind_t *edge, uint64_t connection_id, uint64_t connection_generation,
    const flowie_cluster_owner_token_t *expected_owner, flowie_mqtt_span_t client_id,
    const turbo_flow_protocol_settlement_request_t *settlement,
    flowie_cluster_edge_command_complete_fn complete, void *complete_ctx);

/**
 * Submit an abnormal socket-loss notification for the exact CONNECT-installed
 * owner token and edge connection generation. A successful completion carries
 * an explicit no-op action because the edge socket is already gone.
 */
int flowie_cluster_edge_bind_connection_lost(flowie_cluster_edge_bind_t *edge,
                                             uint64_t connection_id, uint64_t connection_generation,
                                             const flowie_cluster_owner_token_t *expected_owner,
                                             flowie_mqtt_version_t mqtt_version,
                                             flowie_mqtt_span_t client_id,
                                             flowie_cluster_edge_command_complete_fn complete,
                                             void *complete_ctx);

/**
 * Validate and apply an owner-issued TAKEOVER_CLOSE to one exact endpoint-owned
 * socket generation. Operational rejection is returned to the owner as an
 * MQTT_REPLY status; a trusted command envelope therefore returns the reply
 * submission status rather than the embedded operation status.
 */
int flowie_cluster_edge_bind_takeover_close(
    flowie_cluster_edge_bind_t *edge, const flowie_cluster_peer_frame_t *command,
    uint64_t expected_connection_id, uint64_t expected_connection_generation,
    flowie_mqtt_version_t expected_mqtt_version, flowie_mqtt_span_t expected_client_id,
    flowie_cluster_edge_takeover_close_fn close_socket, void *close_ctx);

/**
 * Apply one independently routed, connection-ordered socket action. The caller
 * owns last_applied_sequence as part of the exact connection generation and
 * may access it only on the same edge owner lane. The next sequence is applied
 * once, an older/equal sequence is acknowledged without reapplying, and a gap
 * is rejected with TURBO_EBUSY. A trusted command always produces one
 * EDGE_ACTION_ACK reply; this function then returns the reply submission status.
 */
int flowie_cluster_edge_bind_apply_action(
    flowie_cluster_edge_bind_t *edge, const flowie_cluster_peer_frame_t *command,
    const flowie_cluster_owner_token_t *expected_owner, uint64_t expected_connection_id,
    uint64_t expected_connection_generation, flowie_mqtt_version_t expected_mqtt_version,
    uint64_t *last_applied_sequence, flowie_cluster_edge_action_apply_fn apply, void *apply_ctx);

/** Signature-compatible with flowie_cluster_peer_owner_reply_fn and peer receive adapters. */
int flowie_cluster_edge_bind_reply(void *ctx, const flowie_cluster_peer_frame_t *reply);

int flowie_cluster_edge_bind_pending(flowie_cluster_edge_bind_t *edge, size_t *entries,
                                     size_t *bytes);

/**
 * Stop admission and enqueue cancellation of all unresolved requests on the
 * edge owner lane. The borrowed execution must remain running through drain.
 */
int flowie_cluster_edge_bind_close(flowie_cluster_edge_bind_t *edge);
/** Wait from outside the borrowed edge owner lane; waiting on that lane would block completion. */
int flowie_cluster_edge_bind_drain(flowie_cluster_edge_bind_t *edge, uint64_t timeout_ns);

/** Destroy only after close and drain; otherwise returns TURBO_EBUSY. */
int flowie_cluster_edge_bind_destroy(flowie_cluster_edge_bind_t *edge);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_EDGE_BIND_INTERNAL_H */
