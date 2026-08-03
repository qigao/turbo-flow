#ifndef FLOWIE_CLUSTER_INTERNAL_H
#define FLOWIE_CLUSTER_INTERNAL_H

#include "flowie.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_INTERNAL_ABI_V1 1u
#define FLOWIE_CLUSTER_HASH_VERSION_1 1u
#define FLOWIE_CLUSTER_BOOT_ID_SIZE 16u
#define FLOWIE_CLUSTER_NODE_ID_MAX 64u
#define FLOWIE_CLUSTER_ID_MAX 255u
#define FLOWIE_CLUSTER_LISTENER_ID_MAX 255u
#define FLOWIE_CLUSTER_KEY_MAX 65535u
#define FLOWIE_CLUSTER_SHARD_COUNT_MAX 1048576u
#define FLOWIE_CLUSTER_NODE_COUNT_MAX 4096u
#define FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX 1024u

typedef enum flowie_cluster_key_kind_e {
  FLOWIE_CLUSTER_KEY_SESSION = 1,
  FLOWIE_CLUSTER_KEY_RETAINED = 2
} flowie_cluster_key_kind_t;

typedef enum flowie_cluster_node_state_e {
  FLOWIE_CLUSTER_NODE_STARTING = 1,
  FLOWIE_CLUSTER_NODE_SYNCING,
  FLOWIE_CLUSTER_NODE_READY,
  FLOWIE_CLUSTER_NODE_DRAINING,
  FLOWIE_CLUSTER_NODE_OFFLINE,
  FLOWIE_CLUSTER_NODE_EXPIRED
} flowie_cluster_node_state_t;

typedef enum flowie_cluster_shard_state_e {
  FLOWIE_CLUSTER_SHARD_UNASSIGNED = 1,
  FLOWIE_CLUSTER_SHARD_CLAIMING,
  FLOWIE_CLUSTER_SHARD_RECOVERING,
  FLOWIE_CLUSTER_SHARD_ACTIVE,
  FLOWIE_CLUSTER_SHARD_DRAINING,
  FLOWIE_CLUSTER_SHARD_RELEASED,
  FLOWIE_CLUSTER_SHARD_FENCED
} flowie_cluster_shard_state_t;

typedef enum flowie_cluster_connection_state_e {
  FLOWIE_CLUSTER_CONNECTION_ACCEPTED = 1,
  FLOWIE_CLUSTER_CONNECTION_AUTHENTICATING,
  FLOWIE_CLUSTER_CONNECTION_BINDING,
  FLOWIE_CLUSTER_CONNECTION_ACTIVE,
  FLOWIE_CLUSTER_CONNECTION_CLOSING,
  FLOWIE_CLUSTER_CONNECTION_CLOSED,
  FLOWIE_CLUSTER_CONNECTION_FAILED
} flowie_cluster_connection_state_t;

/**
 * Internal cluster limits. All queues are bounded by entries and bytes. The
 * caller owns this value and may discard it after validation.
 */
typedef struct flowie_cluster_config_s {
  size_t size;
  uint32_t abi_version;
  uint32_t hash_version;
  uint32_t shard_count;
  uint32_t max_nodes;
  uint64_t lease_ttl_ms;
  uint64_t renew_interval_ms;
  uint64_t worst_case_db_latency_ms;
  uint64_t safety_margin_ms;
  uint64_t peer_queue_entries;
  uint64_t peer_queue_bytes;
  uint64_t max_command_bytes;
  uint64_t outbox_records;
  uint64_t outbox_bytes;
} flowie_cluster_config_t;

#define FLOWIE_CLUSTER_CONFIG_INIT                                                                 \
  {sizeof(flowie_cluster_config_t),                                                                \
   FLOWIE_CLUSTER_INTERNAL_ABI_V1,                                                                 \
   FLOWIE_CLUSTER_HASH_VERSION_1,                                                                  \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/** Immutable ownership credential copied into commands and fact transactions. */
typedef struct flowie_cluster_owner_token_s {
  size_t size;
  uint32_t abi_version;
  uint32_t shard_id;
  uint64_t owner_epoch;
  size_t node_id_size;
  char node_id[FLOWIE_CLUSTER_NODE_ID_MAX + 1u];
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
} flowie_cluster_owner_token_t;

#define FLOWIE_CLUSTER_OWNER_TOKEN_INIT                                                            \
  {sizeof(flowie_cluster_owner_token_t), FLOWIE_CLUSTER_INTERNAL_ABI_V1, 0u, 0u, 0u, {0}, {0}}

/**
 * Pure single-transaction lease model. A PostgreSQL adapter serializes access
 * to one row before calling these mutations; the structure itself is not
 * thread-safe and performs no I/O.
 */
typedef struct flowie_cluster_shard_lease_s {
  size_t size;
  uint32_t abi_version;
  uint8_t owned;
  uint64_t lease_until_db_ms;
  flowie_cluster_owner_token_t owner;
} flowie_cluster_shard_lease_t;

#define FLOWIE_CLUSTER_SHARD_LEASE_INIT                                                            \
  {sizeof(flowie_cluster_shard_lease_t), FLOWIE_CLUSTER_INTERNAL_ABI_V1, 0u, 0u,                   \
   FLOWIE_CLUSTER_OWNER_TOKEN_INIT}

/** Immutable same-lane cluster placement runtime. */
typedef struct flowie_cluster_runtime_s flowie_cluster_runtime_t;

/** Validate all bounded-capacity and lease timing invariants. */
CXX_C_API int flowie_cluster_config_validate(const flowie_cluster_config_t *config);

/**
 * Stable, allocation-free shard mapping. Identifiers and key are borrowed for
 * the call only. The versioned hash input is independent of host byte order.
 */
CXX_C_API int flowie_cluster_shard_for_key(uint32_t hash_version, flowie_cluster_key_kind_t kind,
                                           const uint8_t *cluster_id, size_t cluster_id_size,
                                           const uint8_t *listener_id, size_t listener_id_size,
                                           const uint8_t *key, size_t key_size,
                                           uint32_t shard_count, uint32_t *out_shard);

/** Validate one state transition. Same-state returns TURBO_EALREADY. */
CXX_C_API int flowie_cluster_node_transition_validate(flowie_cluster_node_state_t from,
                                                      flowie_cluster_node_state_t to);
CXX_C_API int flowie_cluster_shard_transition_validate(flowie_cluster_shard_state_t from,
                                                       flowie_cluster_shard_state_t to);
CXX_C_API int flowie_cluster_connection_transition_validate(flowie_cluster_connection_state_t from,
                                                            flowie_cluster_connection_state_t to);

/**
 * Convert a database-returned lease validity into a conservative monotonic
 * deadline. Overflow returns TURBO_ERANGE and never saturates silently.
 */
CXX_C_API int flowie_cluster_lease_deadline_ns(uint64_t request_start_ns,
                                               uint64_t returned_validity_ms,
                                               uint64_t safety_margin_ms,
                                               uint64_t *out_deadline_ns);

/** Overflow-safe implementation of the ADR queue capacity formula. */
CXX_C_API int flowie_cluster_required_queue_entries(uint64_t peak_commands_per_second,
                                                    uint64_t worst_peer_stall_ms,
                                                    uint64_t max_inflight_batch,
                                                    uint64_t *out_entries);

CXX_C_API int flowie_cluster_owner_token_init(flowie_cluster_owner_token_t *out, uint32_t shard_id,
                                              uint64_t owner_epoch, const char *node_id,
                                              size_t node_id_size,
                                              const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]);

/** Exact token match is required; a well-formed stale token returns TURBO_EBUSY. */
CXX_C_API int flowie_cluster_owner_token_require(const flowie_cluster_owner_token_t *expected,
                                                 const flowie_cluster_owner_token_t *presented);

/** Claim only an unowned or expired row and advance its epoch exactly once. */
CXX_C_API int flowie_cluster_shard_lease_claim(flowie_cluster_shard_lease_t *lease,
                                               uint64_t database_now_ms, uint64_t lease_ttl_ms,
                                               uint32_t shard_id, const char *node_id,
                                               size_t node_id_size,
                                               const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                               flowie_cluster_owner_token_t *out);

/** Renew, authorize or release only an exact, unexpired fencing token. */
CXX_C_API int flowie_cluster_shard_lease_renew(flowie_cluster_shard_lease_t *lease,
                                               uint64_t database_now_ms, uint64_t lease_ttl_ms,
                                               const flowie_cluster_owner_token_t *presented,
                                               uint64_t *out_lease_until_db_ms);
CXX_C_API int flowie_cluster_shard_lease_require(const flowie_cluster_shard_lease_t *lease,
                                                 uint64_t database_now_ms,
                                                 const flowie_cluster_owner_token_t *presented);
CXX_C_API int flowie_cluster_shard_lease_release(flowie_cluster_shard_lease_t *lease,
                                                 uint64_t database_now_ms,
                                                 const flowie_cluster_owner_token_t *presented);

/**
 * Create the behavior-preserving local adapter. It owns no socket or external
 * provider and maps every key to shard zero at epoch one.
 */
CXX_C_API int flowie_cluster_runtime_create_local(uint64_t endpoint_instance_id,
                                                  flowie_cluster_runtime_t **out);
CXX_C_API void flowie_cluster_runtime_destroy(flowie_cluster_runtime_t *runtime);

/** Return a copied owner token; key bytes remain caller-owned. */
CXX_C_API int flowie_cluster_runtime_owner_for_key(const flowie_cluster_runtime_t *runtime,
                                                   flowie_cluster_key_kind_t kind,
                                                   const uint8_t *key, size_t key_size,
                                                   flowie_cluster_owner_token_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_INTERNAL_H */
