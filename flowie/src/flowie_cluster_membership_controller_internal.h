#ifndef FLOWIE_CLUSTER_MEMBERSHIP_CONTROLLER_INTERNAL_H
#define FLOWIE_CLUSTER_MEMBERSHIP_CONTROLLER_INTERNAL_H

#include "flowie_cluster_pgsql_internal.h"
#include "flowie_cluster_topology_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Build borrowed topology views over one owned PostgreSQL snapshot. Views are
 * invalidated by membership_snapshot_cleanup(); no allocation is performed.
 */
int flowie_cluster_membership_topology_view(
    const flowie_cluster_pgsql_membership_snapshot_t *snapshot,
    flowie_cluster_topology_member_t *member_storage, size_t member_capacity,
    flowie_cluster_topology_membership_t *out);

/**
 * Reconcile expired membership rows, read one bounded snapshot and return an
 * owned deterministic topology plan. This synchronous boundary performs
 * blocking PostgreSQL I/O and must not run on a CoroNet owner lane.
 */
int flowie_cluster_membership_refresh_plan(
    flowie_cluster_pgsql_coordinator_t *coordinator,
    const flowie_cluster_topology_plan_config_t *config,
    const flowie_cluster_topology_peer_t *current_peers, size_t current_peer_count,
    flowie_cluster_topology_plan_t **out,
    flowie_cluster_pgsql_membership_snapshot_t *out_snapshot);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_MEMBERSHIP_CONTROLLER_INTERNAL_H */
