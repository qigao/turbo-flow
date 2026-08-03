#ifndef FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_INTERNAL_H
#define FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_INTERNAL_H

#include "flowie_cluster_owner_directory_internal.h"
#include "flowie_cluster_pgsql_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1 1u

typedef struct flowie_cluster_owner_directory_runtime_s
    flowie_cluster_owner_directory_runtime_t;

typedef struct flowie_cluster_owner_directory_runtime_config_s {
  size_t size;
  uint32_t abi_version;
  const flowie_cluster_pgsql_config_t *coordinator;
  uint32_t hash_version;
  uint64_t refresh_interval_ns;
  uint64_t retry_interval_ns;
} flowie_cluster_owner_directory_runtime_config_t;

#define FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_CONFIG_INIT                                      \
  {sizeof(flowie_cluster_owner_directory_runtime_config_t),                                     \
   FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1, NULL, FLOWIE_CLUSTER_HASH_VERSION_1, 0u, 0u}

typedef enum flowie_cluster_owner_directory_runtime_state_e {
  FLOWIE_CLUSTER_OWNER_DIRECTORY_CREATED = 0,
  FLOWIE_CLUSTER_OWNER_DIRECTORY_STARTING,
  FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNNING,
  FLOWIE_CLUSTER_OWNER_DIRECTORY_RETRYING,
  FLOWIE_CLUSTER_OWNER_DIRECTORY_FAULTED,
  FLOWIE_CLUSTER_OWNER_DIRECTORY_CLOSED
} flowie_cluster_owner_directory_runtime_state_t;

typedef struct flowie_cluster_owner_directory_runtime_snapshot_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_owner_directory_runtime_state_t state;
  uint64_t cycle_count;
  /** Successful local refresh generation; zero means no owner view was published. */
  uint64_t refresh_generation;
  int last_status;
} flowie_cluster_owner_directory_runtime_snapshot_t;

#define FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_SNAPSHOT_INIT                                    \
  {sizeof(flowie_cluster_owner_directory_runtime_snapshot_t),                                   \
   FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1, FLOWIE_CLUSTER_OWNER_DIRECTORY_CREATED, 0u,   \
   0u, TURBO_EBUSY}

typedef struct flowie_cluster_owner_directory_runtime_api_s {
  size_t size;
  uint32_t abi_version;
  int (*coordinator_open)(const flowie_cluster_pgsql_config_t *config,
                          flowie_cluster_pgsql_coordinator_t **out);
  void (*coordinator_destroy)(flowie_cluster_pgsql_coordinator_t *coordinator);
  int (*owner_snapshot)(flowie_cluster_pgsql_coordinator_t *coordinator,
                        flowie_cluster_pgsql_shard_owner_snapshot_t *out);
  void (*owner_snapshot_cleanup)(flowie_cluster_pgsql_shard_owner_snapshot_t *snapshot);
} flowie_cluster_owner_directory_runtime_api_t;

int flowie_cluster_owner_directory_runtime_config_validate(
    const flowie_cluster_owner_directory_runtime_config_t *config);
int flowie_cluster_owner_directory_runtime_create(
    const flowie_cluster_owner_directory_runtime_config_t *config,
    flowie_cluster_owner_directory_runtime_t **out);
int flowie_cluster_owner_directory_runtime_create_with_api(
    const flowie_cluster_owner_directory_runtime_config_t *config,
    const flowie_cluster_owner_directory_runtime_api_t *api,
    flowie_cluster_owner_directory_runtime_t **out);
int flowie_cluster_owner_directory_runtime_start(
    flowie_cluster_owner_directory_runtime_t *runtime);

/**
 * Wait for the first complete publication. A timeout leaves the runtime alive;
 * a permanent pre-publication failure is returned immediately.
 */
int flowie_cluster_owner_directory_runtime_wait_ready(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns);

/** Borrowed runtime-owned directory; valid until close followed by destroy. */
flowie_cluster_owner_directory_t *flowie_cluster_owner_directory_runtime_directory(
    flowie_cluster_owner_directory_runtime_t *runtime);
int flowie_cluster_owner_directory_runtime_snapshot(
    flowie_cluster_owner_directory_runtime_t *runtime,
    flowie_cluster_owner_directory_runtime_snapshot_t *out);

/**
 * Stop and join within timeout_ns. A timeout keeps the closed-to-new-cycles
 * runtime alive and safe for a later close retry.
 */
int flowie_cluster_owner_directory_runtime_close(
    flowie_cluster_owner_directory_runtime_t *runtime, uint64_t timeout_ns);
void flowie_cluster_owner_directory_runtime_destroy(
    flowie_cluster_owner_directory_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_INTERNAL_H */
