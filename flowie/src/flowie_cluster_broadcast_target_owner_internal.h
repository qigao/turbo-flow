#ifndef FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_INTERNAL_H
#define FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_INTERNAL_H

#include "flowie_cluster_broadcast_dispatch_internal.h"
#include "flowie_cluster_broadcast_target_dispatch_internal.h"
#include "flowie_cluster_session_bind_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_ABI_V1 1u

typedef struct flowie_cluster_broadcast_target_owner_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  size_t max_payload_size;
  size_t max_targets;
  struct tf_coronet_execution_s *execution;
  flowie_cluster_session_bind_t *session_bind;
  flowie_cluster_broadcast_owner_resolve_fn resolve;
  void *resolve_ctx;
  flowie_cluster_session_fact_submit_fn submit;
  void *submit_ctx;
  /** Called when a durable completion cannot be returned to the owner lane. */
  flowie_cluster_session_self_fence_fn self_fence;
  void *self_fence_ctx;
} flowie_cluster_broadcast_target_owner_config_t;

#define FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_CONFIG_INIT                                         \
  {sizeof(flowie_cluster_broadcast_target_owner_config_t),                                        \
   FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_ABI_V1, 0u, 0u, 0u, NULL, NULL, NULL, NULL, NULL, NULL,  \
   NULL, NULL}

typedef struct flowie_cluster_broadcast_target_owner_s
    flowie_cluster_broadcast_target_owner_t;

typedef struct flowie_cluster_broadcast_target_owner_snapshot_s {
  size_t size;
  uint32_t abi_version;
  int last_status;
  uint64_t admitted_events;
  uint64_t completed_events;
  uint64_t target_transactions;
  uint64_t shard_ack_transactions;
  size_t current_target_count;
  int accepting;
  int active;
} flowie_cluster_broadcast_target_owner_snapshot_t;

#define FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_SNAPSHOT_INIT                                       \
  {sizeof(flowie_cluster_broadcast_target_owner_snapshot_t),                                      \
   FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_ABI_V1, TURBO_OK, 0u, 0u, 0u, 0u, 0u, 0, 0}

int flowie_cluster_broadcast_target_owner_config_validate(
    const flowie_cluster_broadcast_target_owner_config_t *config);
int flowie_cluster_broadcast_target_owner_create(
    const flowie_cluster_broadcast_target_owner_config_t *config,
    flowie_cluster_broadcast_target_owner_t **out);

/** Signature-compatible with flowie_cluster_broadcast_target_apply_fn. */
int flowie_cluster_broadcast_target_owner_apply(
    void *ctx, const void *payload, size_t payload_size,
    flowie_cluster_broadcast_target_apply_complete_fn complete, void *completion_ctx);

int flowie_cluster_broadcast_target_owner_snapshot(
    flowie_cluster_broadcast_target_owner_t *owner,
    flowie_cluster_broadcast_target_owner_snapshot_t *out);
int flowie_cluster_broadcast_target_owner_close(flowie_cluster_broadcast_target_owner_t *owner);
int flowie_cluster_broadcast_target_owner_drain(flowie_cluster_broadcast_target_owner_t *owner,
                                                uint64_t timeout_ns);
int flowie_cluster_broadcast_target_owner_destroy(flowie_cluster_broadcast_target_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_INTERNAL_H */
