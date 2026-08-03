#ifndef FLOWIE_CLUSTER_ROUTE_STORE_INTERNAL_H
#define FLOWIE_CLUSTER_ROUTE_STORE_INTERNAL_H

#include "flowie_cluster_internal.h"
#include "turbo_flow_state_store.h"
#include "turbo_str.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1 1u
#define FLOWIE_CLUSTER_ROUTE_RECORD_VERSION 1u
#define FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE 96u
#define FLOWIE_CLUSTER_ROUTE_CAS_ATTEMPTS_MAX 64u

typedef struct flowie_cluster_route_store_s flowie_cluster_route_store_t;
typedef struct flowie_cluster_route_snapshot_s flowie_cluster_route_snapshot_t;

typedef struct flowie_cluster_route_store_config_s {
  size_t size;
  uint32_t abi_version;
  /** Borrowed for the route store lifetime; the caller owns close/destroy. */
  turbo_flow_state_store_t *state;
  size_t max_client_id_size;
  size_t max_endpoint_size;
  size_t max_cas_attempts;
} flowie_cluster_route_store_config_t;

#define FLOWIE_CLUSTER_ROUTE_STORE_CONFIG_INIT                                                   \
  {sizeof(flowie_cluster_route_store_config_t), FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1, NULL, 0u, 0u, \
   0u}

/** One route projection derived from a committed PostgreSQL session fact. */
typedef struct flowie_cluster_route_projection_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_span_t client_id;
  tstr_v edge_node_id;
  uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_v advertised_endpoint;
  uint32_t session_shard;
  uint64_t owner_epoch;
  uint64_t fact_revision;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_generation;
  /** Authoritative epoch milliseconds. Required for active routes, zero for tombstones. */
  uint64_t lease_deadline_epoch_ms;
  uint8_t active;
} flowie_cluster_route_projection_t;

#define FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT                                                     \
  {sizeof(flowie_cluster_route_projection_t), FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1, {NULL, 0u},     \
   {NULL, 0u}, {0}, {NULL, 0u}, 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u}

typedef enum flowie_cluster_route_project_result_e {
  FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED = 1,
  FLOWIE_CLUSTER_ROUTE_PROJECT_UNCHANGED,
  FLOWIE_CLUSTER_ROUTE_PROJECT_STALE
} flowie_cluster_route_project_result_t;

/** Owned route returned by resolve(); release strings with cleanup(). */
typedef struct flowie_cluster_route_record_s {
  size_t size;
  uint32_t abi_version;
  tstr_t edge_node_id;
  uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t advertised_endpoint;
  uint32_t session_shard;
  uint64_t owner_epoch;
  uint64_t fact_revision;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_generation;
  uint64_t lease_deadline_epoch_ms;
} flowie_cluster_route_record_t;

#define FLOWIE_CLUSTER_ROUTE_RECORD_INIT                                                         \
  {sizeof(flowie_cluster_route_record_t), FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1, NULL, {0}, NULL, 0u, \
   0u, 0u, 0u, 0u, 0u, 0u}

int flowie_cluster_route_store_config_validate(
    const flowie_cluster_route_store_config_t *config);
int flowie_cluster_route_store_create(const flowie_cluster_route_store_config_t *config,
                                      flowie_cluster_route_store_t **out);
void flowie_cluster_route_store_destroy(flowie_cluster_route_store_t *store);

/**
 * Copy one stable, bounded StateStore scan. Returned projection views borrow snapshot storage and
 * remain valid until snapshot_destroy(). Concurrent growth is retried only up to max_cas_attempts.
 */
int flowie_cluster_route_store_snapshot_create(flowie_cluster_route_store_t *store,
                                               flowie_cluster_route_snapshot_t **out);
size_t flowie_cluster_route_snapshot_count(const flowie_cluster_route_snapshot_t *snapshot);
int flowie_cluster_route_snapshot_at(const flowie_cluster_route_snapshot_t *snapshot, size_t index,
                                     flowie_cluster_route_projection_t *out);
void flowie_cluster_route_snapshot_destroy(flowie_cluster_route_snapshot_t *snapshot);

/**
 * CAS one active route or tombstone. Older PostgreSQL versions are successful no-ops;
 * an equal session version may refresh member-derived endpoint/lease fields only when the exact
 * session identity is unchanged and the lease deadline advances. Other divergence is a protocol
 * error.
 */
int flowie_cluster_route_store_project(flowie_cluster_route_store_t *store,
                                       const flowie_cluster_route_projection_t *projection,
                                       flowie_cluster_route_project_result_t *out_result);

/**
 * Resolve an active, unexpired route. TURBO_ENOENT covers absence, tombstones and expiry.
 * now_epoch_ms must come from the same wall-clock domain used for lease deadlines.
 */
int flowie_cluster_route_store_resolve(flowie_cluster_route_store_t *store,
                                       flowie_mqtt_span_t client_id, uint64_t now_epoch_ms,
                                       flowie_cluster_route_record_t *out);
void flowie_cluster_route_record_cleanup(flowie_cluster_route_record_t *record);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_ROUTE_STORE_INTERNAL_H */
