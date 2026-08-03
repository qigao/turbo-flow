#include "flowie_cluster_membership_controller_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

static void flowie_cluster_membership_test_member(flowie_cluster_pgsql_member_t *member,
                                                  const char *node_id, uint8_t boot_seed,
                                                  const char *endpoint, uint64_t revision) {
  size_t node_size = strlen(node_id);
  size_t endpoint_size = strlen(endpoint);
  *member = (flowie_cluster_pgsql_member_t)FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
  memcpy(member->node_id, node_id, node_size + 1u);
  member->node_id_size = node_size;
  member->boot_id[0] = boot_seed;
  member->state = FLOWIE_CLUSTER_NODE_READY;
  memcpy(member->advertised_endpoint, endpoint, endpoint_size + 1u);
  member->advertised_endpoint_size = endpoint_size;
  member->revision = revision;
}

spec("flowie cluster membership controller") {
  it("adapts one owned PostgreSQL snapshot into a deterministic topology plan") {
    flowie_cluster_pgsql_membership_snapshot_t snapshot =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_topology_member_t views[2];
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_topology_plan_config_t config = FLOWIE_CLUSTER_TOPOLOGY_PLAN_CONFIG_INIT;
    flowie_cluster_topology_plan_t *plan = NULL;
    flowie_cluster_topology_operation_t operation = FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT;
    snapshot.members =
        (flowie_cluster_pgsql_member_t *)calloc(2u, sizeof(*snapshot.members));
    check_not_null(snapshot.members);
    snapshot.membership_revision = 2u;
    snapshot.member_count = 2u;
    flowie_cluster_membership_test_member(&snapshot.members[0], "node-a", 1u,
                                          "127.0.0.1:7101", 1u);
    flowie_cluster_membership_test_member(&snapshot.members[1], "node-b", 2u,
                                          "127.0.0.1:7102", 2u);
    check_int_eq(flowie_cluster_membership_topology_view(&snapshot, views, 2u, &membership),
                 TURBO_OK);
    check_uint_eq(membership.membership_revision, 2u);
    check_size_eq(membership.member_count, 2u);
    config.local_node_id = tstr_v_from_cstr("node-a");
    config.local_boot_id[0] = 1u;
    config.max_nodes = 2u;
    config.max_endpoint_size = FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX;
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_OK);
    check_size_eq(flowie_cluster_topology_plan_operation_count(plan), 1u);
    check_int_eq(flowie_cluster_topology_plan_operation_at(plan, 0u, &operation), TURBO_OK);
    check_int_eq(operation.kind, FLOWIE_CLUSTER_TOPOLOGY_ADD);
    check_size_eq(operation.peer.node_id.len, strlen("node-b"));
    check_mem_eq(operation.peer.node_id.data, "node-b", operation.peer.node_id.len);
    flowie_cluster_topology_plan_destroy(plan);
    flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
  }

  it("rejects an unsorted or under-capacity snapshot view") {
    flowie_cluster_pgsql_membership_snapshot_t snapshot =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_topology_member_t views[2];
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_pgsql_member_t members[2];
    snapshot.membership_revision = 2u;
    snapshot.members = members;
    snapshot.member_count = 2u;
    flowie_cluster_membership_test_member(&members[0], "node-b", 2u, "127.0.0.1:7102", 1u);
    flowie_cluster_membership_test_member(&members[1], "node-a", 1u, "127.0.0.1:7101", 2u);
    check_int_eq(flowie_cluster_membership_topology_view(&snapshot, views, 2u, &membership),
                 TURBO_EPROTO);
    check_int_eq(flowie_cluster_membership_topology_view(&snapshot, views, 1u, &membership),
                 TURBO_EINVAL);
  }
}
