#ifndef FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_INTERNAL_H
#define FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_INTERNAL_H

#include "flowie_cluster_broadcast_event_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1 1u

/**
 * Borrowed transport claim. payload remains valid until the exact token is
 * acknowledged, requeued, or the external transport owner is destroyed.
 */
typedef struct flowie_cluster_broadcast_target_claim_s {
  size_t size;
  uint32_t abi_version;
  uint64_t token;
  tstr_v payload;
} flowie_cluster_broadcast_target_claim_t;

#define FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT                                                \
  {sizeof(flowie_cluster_broadcast_target_claim_t),                                               \
   FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1, 0u, {NULL, 0u}}

typedef int (*flowie_cluster_broadcast_target_claim_fn)(
    void *ctx, flowie_cluster_broadcast_target_claim_t *out);
/** OK or EALREADY releases the exact active claim; uncertain errors retain it. */
typedef int (*flowie_cluster_broadcast_target_settle_fn)(void *ctx, uint64_t token);
typedef void (*flowie_cluster_broadcast_target_apply_complete_fn)(void *ctx, int status);

/**
 * Copy and admit one TFBE payload to its shard owner lane. TURBO_OK transfers
 * responsibility for exactly one completion callback. Failure must not call
 * complete and leaves the caller responsible for the transport claim.
 */
typedef int (*flowie_cluster_broadcast_target_apply_fn)(
    void *ctx, const void *payload, size_t payload_size,
    flowie_cluster_broadcast_target_apply_complete_fn complete, void *completion_ctx);

typedef enum flowie_cluster_broadcast_target_dispatcher_state_e {
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CREATED = 1,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLAIMING,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_APPLYING,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_ACKING,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_REQUEUEING,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_RETRY_WAIT,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSING,
  FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CLOSED
} flowie_cluster_broadcast_target_dispatcher_state_t;

typedef struct flowie_cluster_broadcast_target_dispatcher_config_s {
  size_t size;
  uint32_t abi_version;
  size_t max_payload_size;
  uint64_t poll_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t ack_retry_interval_ns;
  flowie_cluster_broadcast_target_claim_fn claim;
  flowie_cluster_broadcast_target_settle_fn ack;
  flowie_cluster_broadcast_target_settle_fn requeue;
  void *transport_ctx;
  flowie_cluster_broadcast_target_apply_fn apply;
  void *apply_ctx;
} flowie_cluster_broadcast_target_dispatcher_config_t;

#define FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CONFIG_INIT                                    \
  {sizeof(flowie_cluster_broadcast_target_dispatcher_config_t),                                   \
   FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1, 0u, 0u, 0u, 0u, NULL, NULL, NULL, NULL, NULL, \
   NULL}

typedef struct flowie_cluster_broadcast_target_dispatcher_s
    flowie_cluster_broadcast_target_dispatcher_t;

typedef struct flowie_cluster_broadcast_target_dispatcher_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_broadcast_target_dispatcher_state_t state;
  int last_status;
  uint64_t claimed_events;
  uint64_t apply_attempts;
  uint64_t requeued_events;
  uint64_t ack_attempts;
  uint64_t acknowledged_events;
  uint64_t active_token;
} flowie_cluster_broadcast_target_dispatcher_snapshot_t;

#define FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_SNAPSHOT_INIT                                  \
  {sizeof(flowie_cluster_broadcast_target_dispatcher_snapshot_t),                                 \
   FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1,                                               \
   FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCHER_CREATED, TURBO_OK, 0u, 0u, 0u, 0u, 0u, 0u}

int flowie_cluster_broadcast_target_dispatcher_config_validate(
    const flowie_cluster_broadcast_target_dispatcher_config_t *config);
int flowie_cluster_broadcast_target_dispatcher_create(
    const flowie_cluster_broadcast_target_dispatcher_config_t *config,
    flowie_cluster_broadcast_target_dispatcher_t **out);
int flowie_cluster_broadcast_target_dispatcher_snapshot(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher,
    flowie_cluster_broadcast_target_dispatcher_snapshot_t *out);
int flowie_cluster_broadcast_target_dispatcher_close(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher);
int flowie_cluster_broadcast_target_dispatcher_drain(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher, uint64_t timeout_ns);
int flowie_cluster_broadcast_target_dispatcher_destroy(
    flowie_cluster_broadcast_target_dispatcher_t *dispatcher);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_INTERNAL_H */
