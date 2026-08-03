#ifndef FLOWIE_CLUSTER_OWNER_DIRECTORY_INTERNAL_H
#define FLOWIE_CLUSTER_OWNER_DIRECTORY_INTERNAL_H

#include "flowie_cluster_edge_bind_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_OWNER_DIRECTORY_ABI_V1 1u

typedef struct flowie_cluster_owner_directory_entry_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  /** Zero means currently unassigned; otherwise this is a conservative monotonic deadline. */
  uint64_t local_deadline_ns;
  flowie_cluster_owner_token_t owner;
} flowie_cluster_owner_directory_entry_t;

#define FLOWIE_CLUSTER_OWNER_DIRECTORY_ENTRY_INIT                                                 \
  {sizeof(flowie_cluster_owner_directory_entry_t), FLOWIE_CLUSTER_OWNER_DIRECTORY_ABI_V1, 0u,     \
   0u, FLOWIE_CLUSTER_OWNER_TOKEN_INIT}

typedef struct flowie_cluster_owner_directory_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t hash_version;
  uint32_t shard_count;
  tstr_v cluster_id;
  tstr_v listener_id;
} flowie_cluster_owner_directory_config_t;

#define FLOWIE_CLUSTER_OWNER_DIRECTORY_CONFIG_INIT                                                \
  {sizeof(flowie_cluster_owner_directory_config_t), FLOWIE_CLUSTER_OWNER_DIRECTORY_ABI_V1,        \
   FLOWIE_CLUSTER_HASH_VERSION_1, 0u, {NULL, 0u}, {NULL, 0u}}

typedef struct flowie_cluster_owner_directory_s flowie_cluster_owner_directory_t;

int flowie_cluster_owner_directory_create(const flowie_cluster_owner_directory_config_t *config,
                                          flowie_cluster_owner_directory_t **out);

/**
 * Replace the complete fixed-shard view. A single refresh writer supplies
 * exactly one index-aligned entry per shard. Concurrent resolve calls are
 * allowed. The revision is a local refresh generation, not an MQTT fact-source
 * revision; same-generation changed data is a protocol error.
 */
int flowie_cluster_owner_directory_replace(
    flowie_cluster_owner_directory_t *directory,
    const flowie_cluster_owner_directory_entry_t *entries, size_t entry_count,
    uint64_t revision);

/** Signature-compatible edge resolver; returns EBUSY for absent or expired ownership. */
int flowie_cluster_owner_directory_resolve(void *ctx, flowie_mqtt_span_t client_id,
                                           flowie_cluster_owner_token_t *out);
int flowie_cluster_owner_directory_resolve_shard(flowie_cluster_owner_directory_t *directory,
                                                 uint32_t shard_id,
                                                 flowie_cluster_owner_token_t *out);

int flowie_cluster_owner_directory_revision(flowie_cluster_owner_directory_t *directory,
                                            uint64_t *out_revision);
void flowie_cluster_owner_directory_destroy(flowie_cluster_owner_directory_t *directory);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_OWNER_DIRECTORY_INTERNAL_H */
