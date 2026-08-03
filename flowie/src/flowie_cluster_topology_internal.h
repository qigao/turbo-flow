#ifndef FLOWIE_CLUSTER_TOPOLOGY_INTERNAL_H
#define FLOWIE_CLUSTER_TOPOLOGY_INTERNAL_H

#include "flowie_cluster_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 1u
/** One PostgreSQL-derived membership row. Views are borrowed for plan_build(). */
typedef struct flowie_cluster_topology_member_s {
  size_t size;
  uint32_t abi_version;
  tstr_v node_id;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_node_state_t state;
  tstr_v advertised_endpoint;
  uint64_t revision;
} flowie_cluster_topology_member_t;

#define FLOWIE_CLUSTER_TOPOLOGY_MEMBER_INIT                                                        \
  {sizeof(flowie_cluster_topology_member_t),                                                       \
   FLOWIE_CLUSTER_TOPOLOGY_ABI_V1,                                                                 \
   {NULL, 0u},                                                                                     \
   {0},                                                                                            \
   FLOWIE_CLUSTER_NODE_STARTING,                                                                   \
   {NULL, 0u},                                                                                     \
   0u}

/** Members must be strictly ordered by node_id and represent one SQL snapshot. */
typedef struct flowie_cluster_topology_membership_s {
  size_t size;
  uint32_t abi_version;
  uint64_t membership_revision;
  const flowie_cluster_topology_member_t *members;
  size_t member_count;
} flowie_cluster_topology_membership_t;

#define FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT                                                    \
  {sizeof(flowie_cluster_topology_membership_t), FLOWIE_CLUSTER_TOPOLOGY_ABI_V1, 0u, NULL, 0u}

/** One currently owned outgoing connector identity, strictly ordered by node_id. */
typedef struct flowie_cluster_topology_peer_s {
  size_t size;
  uint32_t abi_version;
  tstr_v node_id;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_v advertised_endpoint;
} flowie_cluster_topology_peer_t;

#define FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT                                                          \
  {sizeof(flowie_cluster_topology_peer_t),                                                         \
   FLOWIE_CLUSTER_TOPOLOGY_ABI_V1,                                                                 \
   {NULL, 0u},                                                                                     \
   {0},                                                                                            \
   {NULL, 0u}}

typedef struct flowie_cluster_topology_plan_config_s {
  size_t size;
  uint32_t abi_version;
  tstr_v local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  size_t max_nodes;
  size_t max_endpoint_size;
  uint64_t last_applied_revision;
} flowie_cluster_topology_plan_config_t;

#define FLOWIE_CLUSTER_TOPOLOGY_PLAN_CONFIG_INIT                                                   \
  {sizeof(flowie_cluster_topology_plan_config_t),                                                  \
   FLOWIE_CLUSTER_TOPOLOGY_ABI_V1,                                                                 \
   {NULL, 0u},                                                                                     \
   {0},                                                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

typedef enum flowie_cluster_topology_operation_kind_e {
  FLOWIE_CLUSTER_TOPOLOGY_REMOVE = 1,
  FLOWIE_CLUSTER_TOPOLOGY_ADD
} flowie_cluster_topology_operation_kind_t;

/** Borrowed view valid until the owning plan is destroyed. */
typedef struct flowie_cluster_topology_operation_s {
  size_t size;
  uint32_t abi_version;
  flowie_cluster_topology_operation_kind_t kind;
  flowie_cluster_topology_peer_t peer;
} flowie_cluster_topology_operation_t;

#define FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT                                                     \
  {sizeof(flowie_cluster_topology_operation_t), FLOWIE_CLUSTER_TOPOLOGY_ABI_V1,                    \
   FLOWIE_CLUSTER_TOPOLOGY_REMOVE, FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT}

typedef struct flowie_cluster_topology_plan_s flowie_cluster_topology_plan_t;

/** Validate bounded local identity and topology capacity without building a plan. */
int flowie_cluster_topology_plan_config_validate(
    const flowie_cluster_topology_plan_config_t *config);

/**
 * Build an owned O(nodes + peers) side-effect plan without I/O or callbacks.
 * Only SYNCING/READY/DRAINING members participate. Deterministic direction
 * means an outgoing connector is planned only when local_node_id is smaller.
 * Every REMOVE precedes every ADD, including boot/endpoint replacement.
 *
 * TURBO_EBUSY rejects a snapshot older than last_applied_revision. A changed
 * topology at the exact already-applied revision is TURBO_EPROTO. Capacity is
 * bounded by max_nodes and at most 2 * max_nodes operations.
 */
int flowie_cluster_topology_plan_build(const flowie_cluster_topology_plan_config_t *config,
                                       const flowie_cluster_topology_membership_t *membership,
                                       const flowie_cluster_topology_peer_t *current_peers,
                                       size_t current_peer_count,
                                       flowie_cluster_topology_plan_t **out);

uint64_t flowie_cluster_topology_plan_revision(const flowie_cluster_topology_plan_t *plan);
size_t flowie_cluster_topology_plan_operation_count(const flowie_cluster_topology_plan_t *plan);
int flowie_cluster_topology_plan_operation_at(const flowie_cluster_topology_plan_t *plan,
                                              size_t index,
                                              flowie_cluster_topology_operation_t *out);
void flowie_cluster_topology_plan_destroy(flowie_cluster_topology_plan_t *plan);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_TOPOLOGY_INTERNAL_H */
