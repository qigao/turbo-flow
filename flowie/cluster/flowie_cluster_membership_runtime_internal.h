#ifndef FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_INTERNAL_H
#define FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_INTERNAL_H

#include "flowie_cluster_member_directory_internal.h"
#include "flowie_cluster_membership_controller_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1 1u

typedef struct flowie_cluster_membership_runtime_s flowie_cluster_membership_runtime_t;

typedef int (*flowie_cluster_membership_current_fn)(void *ctx,
                                                    flowie_cluster_topology_peer_t *storage,
                                                    size_t capacity, size_t *out_count,
                                                    uint64_t *out_revision);
typedef int (*flowie_cluster_membership_apply_fn)(void *ctx,
                                                  const flowie_cluster_topology_plan_t *plan,
                                                  uint64_t timeout_ns);
typedef int (*flowie_cluster_membership_maintenance_fn)(void *ctx, size_t *out_changed);

typedef struct flowie_cluster_membership_runtime_config_s {
  size_t size;
  uint32_t abi_version;
  const flowie_cluster_pgsql_config_t *coordinator;
  flowie_cluster_topology_plan_config_t topology;
  uint64_t refresh_interval_ns;
  uint64_t retry_interval_ns;
  uint64_t apply_timeout_ns;
  flowie_cluster_membership_current_fn current;
  flowie_cluster_membership_apply_fn apply;
  void *topology_ctx;
  /** Borrowed through close; receives only successfully applied PG snapshots. */
  flowie_cluster_member_directory_t *member_directory;
  /** Optional cluster-wide maintenance run once after each successful directory publication. */
  flowie_cluster_membership_maintenance_fn maintenance;
  void *maintenance_ctx;
} flowie_cluster_membership_runtime_config_t;

#define FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_CONFIG_INIT                                              \
  {sizeof(flowie_cluster_membership_runtime_config_t),                                             \
   FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1,                                                       \
   NULL,                                                                                           \
   FLOWIE_CLUSTER_TOPOLOGY_PLAN_CONFIG_INIT,                                                       \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef enum flowie_cluster_membership_runtime_state_e {
  FLOWIE_CLUSTER_MEMBERSHIP_CREATED = 0,
  FLOWIE_CLUSTER_MEMBERSHIP_STARTING,
  FLOWIE_CLUSTER_MEMBERSHIP_RUNNING,
  FLOWIE_CLUSTER_MEMBERSHIP_RETRYING,
  FLOWIE_CLUSTER_MEMBERSHIP_FAULTED,
  FLOWIE_CLUSTER_MEMBERSHIP_CLOSED
} flowie_cluster_membership_runtime_state_t;

typedef struct flowie_cluster_membership_runtime_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_membership_runtime_state_t state;
  uint64_t cycle_count;
  uint64_t applied_revision;
  int last_status;
} flowie_cluster_membership_runtime_snapshot_t;

#define FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_SNAPSHOT_INIT                                            \
  {sizeof(flowie_cluster_membership_runtime_snapshot_t),                                           \
   FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1,                                                       \
   FLOWIE_CLUSTER_MEMBERSHIP_CREATED,                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_EBUSY}

typedef struct flowie_cluster_membership_runtime_api_s {
  size_t size;
  uint32_t abi_version;
  int (*coordinator_open)(const flowie_cluster_pgsql_config_t *config,
                          flowie_cluster_pgsql_coordinator_t **out);
  void (*coordinator_destroy)(flowie_cluster_pgsql_coordinator_t *coordinator);
  int (*refresh_plan)(flowie_cluster_pgsql_coordinator_t *coordinator,
                      const flowie_cluster_topology_plan_config_t *config,
                      const flowie_cluster_topology_peer_t *current_peers,
                      size_t current_peer_count, flowie_cluster_topology_plan_t **out,
                      flowie_cluster_pgsql_membership_snapshot_t *out_snapshot);
  void (*plan_destroy)(flowie_cluster_topology_plan_t *plan);
} flowie_cluster_membership_runtime_api_t;

int flowie_cluster_membership_runtime_config_validate(
    const flowie_cluster_membership_runtime_config_t *config);
int flowie_cluster_membership_runtime_create(
    const flowie_cluster_membership_runtime_config_t *config,
    flowie_cluster_membership_runtime_t **out);
int flowie_cluster_membership_runtime_create_with_api(
    const flowie_cluster_membership_runtime_config_t *config,
    const flowie_cluster_membership_runtime_api_t *api, flowie_cluster_membership_runtime_t **out);
int flowie_cluster_membership_runtime_start(flowie_cluster_membership_runtime_t *runtime);
int flowie_cluster_membership_runtime_snapshot(flowie_cluster_membership_runtime_t *runtime,
                                               flowie_cluster_membership_runtime_snapshot_t *out);
/**
 * Request worker stop and join it within timeout_ns. A timeout leaves the
 * runtime alive, closed to future cycles and safe for a later close retry.
 */
int flowie_cluster_membership_runtime_close(flowie_cluster_membership_runtime_t *runtime,
                                            uint64_t timeout_ns);
void flowie_cluster_membership_runtime_destroy(flowie_cluster_membership_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_INTERNAL_H */
