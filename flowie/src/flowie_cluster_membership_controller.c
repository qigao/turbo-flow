#include "flowie_cluster_membership_controller_internal.h"

#include <stdlib.h>
#include <string.h>

int flowie_cluster_membership_topology_view(
    const flowie_cluster_pgsql_membership_snapshot_t *snapshot,
    flowie_cluster_topology_member_t *member_storage, size_t member_capacity,
    flowie_cluster_topology_membership_t *out) {
  if (!snapshot || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 || snapshot->size != sizeof(*snapshot) ||
      snapshot->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      snapshot->member_count > FLOWIE_CLUSTER_NODE_COUNT_MAX ||
      snapshot->member_count > member_capacity ||
      (snapshot->member_count != 0u && (!snapshot->members || !member_storage)))
    return TURBO_EINVAL;
  *out = (flowie_cluster_topology_membership_t)FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
  for (size_t index = 0u; index < snapshot->member_count; ++index) {
    const flowie_cluster_pgsql_member_t *source = &snapshot->members[index];
    flowie_cluster_topology_member_t *target = &member_storage[index];
    if (source->size != sizeof(*source) || source->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
        source->node_id_size == 0u || source->node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
        source->node_id[source->node_id_size] != '\0' ||
        source->advertised_endpoint_size == 0u ||
        source->advertised_endpoint_size > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX ||
        source->advertised_endpoint[source->advertised_endpoint_size] != '\0' ||
        source->state < FLOWIE_CLUSTER_NODE_STARTING ||
        source->state > FLOWIE_CLUSTER_NODE_EXPIRED || source->revision == 0u ||
        source->revision > snapshot->membership_revision ||
        (index > 0u && strcmp(snapshot->members[index - 1u].node_id, source->node_id) >= 0))
      return TURBO_EPROTO;
    *target = (flowie_cluster_topology_member_t)FLOWIE_CLUSTER_TOPOLOGY_MEMBER_INIT;
    target->node_id = tstr_v_from_buf(source->node_id, source->node_id_size);
    memcpy(target->boot_id, source->boot_id, sizeof(target->boot_id));
    target->state = source->state;
    target->advertised_endpoint =
        tstr_v_from_buf(source->advertised_endpoint, source->advertised_endpoint_size);
    target->revision = source->revision;
  }
  out->membership_revision = snapshot->membership_revision;
  out->members = member_storage;
  out->member_count = snapshot->member_count;
  return TURBO_OK;
}

int flowie_cluster_membership_refresh_plan(
    flowie_cluster_pgsql_coordinator_t *coordinator,
    const flowie_cluster_topology_plan_config_t *config,
    const flowie_cluster_topology_peer_t *current_peers, size_t current_peer_count,
    flowie_cluster_topology_plan_t **out,
    flowie_cluster_pgsql_membership_snapshot_t *out_snapshot) {
  flowie_cluster_pgsql_membership_snapshot_t snapshot =
      FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
  flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
  flowie_cluster_topology_member_t *members = NULL;
  uint64_t reconciled_revision = 0u;
  int rc;
  if (out) *out = NULL;
  if (out_snapshot)
    *out_snapshot = (flowie_cluster_pgsql_membership_snapshot_t)
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
  if (!coordinator || !config || !out || !out_snapshot || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 || config->max_nodes == 0u ||
      config->max_nodes > FLOWIE_CLUSTER_NODE_COUNT_MAX)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_membership_expire(coordinator, &reconciled_revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_membership_snapshot(coordinator, config->max_nodes, &snapshot);
  if (rc == TURBO_OK && snapshot.membership_revision < reconciled_revision) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && snapshot.member_count != 0u) {
    members = (flowie_cluster_topology_member_t *)calloc(snapshot.member_count, sizeof(*members));
    if (!members) rc = TURBO_ENOMEM;
  }
  if (rc == TURBO_OK)
    rc = flowie_cluster_membership_topology_view(&snapshot, members, snapshot.member_count,
                                                 &membership);
  if (rc == TURBO_OK)
    rc = flowie_cluster_topology_plan_build(config, &membership, current_peers, current_peer_count,
                                            out);
  free(members);
  if (rc == TURBO_OK) {
    *out_snapshot = snapshot;
  } else {
    flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
  }
  return rc;
}
