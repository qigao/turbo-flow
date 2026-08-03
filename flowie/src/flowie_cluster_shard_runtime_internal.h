#ifndef FLOWIE_CLUSTER_SHARD_RUNTIME_INTERNAL_H
#define FLOWIE_CLUSTER_SHARD_RUNTIME_INTERNAL_H

#include "flowie_cluster_broadcast_dispatch_internal.h"
#include "flowie_cluster_broadcast_target_owner_internal.h"
#include "flowie_cluster_delivery_dispatch_internal.h"
#include "flowie_cluster_lifecycle_dispatch_internal.h"
#include "flowie_cluster_lifecycle_owner_internal.h"
#include "flowie_cluster_route_projection_internal.h"
#include "flowie_cluster_route_dispatch_internal.h"
#include "flowie_cluster_session_bind_internal.h"
#include "flowie_cluster_takeover_dispatch_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_SHARD_RUNTIME_ABI_V1 1u

/**
 * @internal @incomplete
 * One PostgreSQL-authoritative MQTT session shard. The runtime borrows the
 * CoroNet execution and all create-time configuration views, but owns every
 * object it creates. The execution must remain running through close/destroy.
 */
typedef struct flowie_cluster_shard_runtime_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  struct tf_coronet_execution_s *execution;
  const flowie_cluster_pgsql_fact_worker_config_t *fact_worker;
  const flowie_cluster_session_bind_config_t *session_bind;
  size_t owner_max_payload_size;
  size_t owner_queue_entries;
  size_t owner_queue_bytes;
  flowie_cluster_peer_owner_reply_fn reply;
  void *reply_ctx;
  flowie_cluster_session_self_fence_fn self_fence;
  void *self_fence_ctx;
  /**
   * Optional PostgreSQL publish-outbox to broadcast-bus port. The callback may
   * append duplicate immutable TFBE payloads after an uncertain result.
   */
  size_t broadcast_max_payload_size;
  flowie_cluster_broadcast_publish_fn broadcast_publish;
  void *broadcast_publish_ctx;
  uint64_t broadcast_poll_interval_ns;
  uint64_t broadcast_ack_poll_interval_ns;
  uint64_t broadcast_retry_interval_ns;
  uint64_t broadcast_republish_interval_ns;
  /**
   * Optional broadcast-bus target consumer. claim payloads are borrowed only
   * until ack/requeue; the runtime copies them into the shard owner lane.
   */
  size_t broadcast_target_max_payload_size;
  flowie_cluster_broadcast_target_claim_fn broadcast_target_claim;
  flowie_cluster_broadcast_target_settle_fn broadcast_target_ack;
  flowie_cluster_broadcast_target_settle_fn broadcast_target_requeue;
  void *broadcast_target_transport_ctx;
  uint64_t broadcast_target_poll_interval_ns;
  uint64_t broadcast_target_retry_interval_ns;
  uint64_t broadcast_target_ack_retry_interval_ns;
  flowie_cluster_takeover_send_fn takeover_send;
  void *takeover_send_ctx;
  uint64_t takeover_poll_interval_ns;
  uint64_t takeover_retry_interval_ns;
  uint64_t takeover_reply_timeout_ns;
  /** Optional TFDA PostgreSQL outbox to peer EDGE_ACTION/TFEK port. */
  flowie_cluster_delivery_send_fn delivery_send;
  void *delivery_send_ctx;
  uint64_t delivery_poll_interval_ns;
  uint64_t delivery_retry_interval_ns;
  uint64_t delivery_reply_timeout_ns;
  /**
   * Optional TFLE consumer. Nonzero poll/retry intervals enable the runtime's
   * built-in owner-lane applicator. lifecycle_apply may override that internal
   * applicator for tests; apply_ctx remains borrowed through destroy().
   */
  flowie_cluster_lifecycle_apply_fn lifecycle_apply;
  void *lifecycle_apply_ctx;
  uint64_t lifecycle_poll_interval_ns;
  uint64_t lifecycle_retry_interval_ns;
  /** Optional Redis route projection; called before takeover/lifecycle settlement. */
  flowie_cluster_outbox_before_settle_fn route_before_settle;
  void *route_before_settle_ctx;
  flowie_cluster_route_store_t *route_store;
  flowie_cluster_route_member_resolve_fn route_member_resolve;
  void *route_member_resolve_ctx;
  uint64_t route_poll_interval_ns;
  uint64_t route_retry_interval_ns;
} flowie_cluster_shard_runtime_config_t;

#define FLOWIE_CLUSTER_SHARD_RUNTIME_CONFIG_INIT                                                   \
  {sizeof(flowie_cluster_shard_runtime_config_t),                                                  \
   FLOWIE_CLUSTER_SHARD_RUNTIME_ABI_V1,                                                            \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u}

typedef struct flowie_cluster_shard_runtime_s flowie_cluster_shard_runtime_t;

int flowie_cluster_shard_runtime_config_validate(
    const flowie_cluster_shard_runtime_config_t *config);

/**
 * Claim, recover, activate, and expose exactly one shard owner. Success means
 * peer admission is ready; failure releases every partially-created resource.
 */
int flowie_cluster_shard_runtime_create(const flowie_cluster_shard_runtime_config_t *config,
                                        flowie_cluster_shard_runtime_t **out);

/** Submit one copied peer command to this shard's owner lane. */
int flowie_cluster_shard_runtime_submit(flowie_cluster_shard_runtime_t *runtime,
                                        const flowie_cluster_peer_frame_t *command);

/** Route a borrowed peer command or reply to the shard's owning component. */
int flowie_cluster_shard_runtime_receive(flowie_cluster_shard_runtime_t *runtime,
                                         const flowie_cluster_peer_frame_t *frame);

/** Thread-safe copied authority state; a locally self-fenced runtime reports FENCED. */
int flowie_cluster_shard_runtime_snapshot(flowie_cluster_shard_runtime_t *runtime,
                                          flowie_cluster_pgsql_lease_snapshot_t *out);

/** Owner-lane diagnostic view. Caller must externally serialize it with command execution. */
int flowie_cluster_shard_runtime_session_snapshot(const flowie_cluster_shard_runtime_t *runtime,
                                                  flowie_mqtt_span_t client_id,
                                                  flowie_session_snapshot_t *snapshot,
                                                  turbo_flow_security_principal_t *principal);

/** Copied TFLE consumer diagnostics; ENOENT means lifecycle intervals were not configured. */
int flowie_cluster_shard_runtime_lifecycle_snapshot(
    flowie_cluster_shard_runtime_t *runtime, flowie_cluster_lifecycle_dispatcher_snapshot_t *out);

/** Copied source-broadcast diagnostics; ENOENT means the publisher port was omitted. */
int flowie_cluster_shard_runtime_broadcast_snapshot(
    flowie_cluster_shard_runtime_t *runtime, flowie_cluster_broadcast_dispatcher_snapshot_t *out);

/** Copied target-consumer transport diagnostics; ENOENT means it was omitted. */
int flowie_cluster_shard_runtime_broadcast_target_snapshot(
    flowie_cluster_shard_runtime_t *runtime,
    flowie_cluster_broadcast_target_dispatcher_snapshot_t *out);

/** Copied target owner-lane diagnostics; ENOENT means target consumption was omitted. */
int flowie_cluster_shard_runtime_broadcast_target_owner_snapshot(
    flowie_cluster_shard_runtime_t *runtime,
    flowie_cluster_broadcast_target_owner_snapshot_t *out);

/** Copied TFDA peer-delivery diagnostics; ENOENT means delivery_send was omitted. */
int flowie_cluster_shard_runtime_delivery_snapshot(
    flowie_cluster_shard_runtime_t *runtime,
    flowie_cluster_delivery_dispatcher_snapshot_t *out);

/**
 * Stop admission, self-fence/release the lease, drain owner completions, then
 * stop the fact worker. A drain timeout leaves the runtime closed to admission
 * but valid for another close attempt.
 */
int flowie_cluster_shard_runtime_close(flowie_cluster_shard_runtime_t *runtime,
                                       uint64_t timeout_ns);

/** Close with an unbounded drain if needed, release storage, and invalidate runtime. */
int flowie_cluster_shard_runtime_destroy(flowie_cluster_shard_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_SHARD_RUNTIME_INTERNAL_H */
