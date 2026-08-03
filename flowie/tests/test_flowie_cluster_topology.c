#include "flowie_cluster_topology_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static void flowie_cluster_topology_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                              uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(seed + index);
}

static flowie_cluster_topology_member_t
flowie_cluster_topology_test_member(const char *node_id, uint8_t boot_seed,
                                    flowie_cluster_node_state_t state,
                                    const char *advertised_endpoint, uint64_t revision) {
  flowie_cluster_topology_member_t member = FLOWIE_CLUSTER_TOPOLOGY_MEMBER_INIT;
  member.node_id = tstr_v_from_cstr(node_id);
  flowie_cluster_topology_test_boot(member.boot_id, boot_seed);
  member.state = state;
  member.advertised_endpoint = tstr_v_from_cstr(advertised_endpoint);
  member.revision = revision;
  return member;
}

static flowie_cluster_topology_peer_t
flowie_cluster_topology_test_peer(const char *node_id, uint8_t boot_seed,
                                  const char *advertised_endpoint) {
  flowie_cluster_topology_peer_t peer = FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT;
  peer.node_id = tstr_v_from_cstr(node_id);
  flowie_cluster_topology_test_boot(peer.boot_id, boot_seed);
  peer.advertised_endpoint = tstr_v_from_cstr(advertised_endpoint);
  return peer;
}

static flowie_cluster_topology_plan_config_t flowie_cluster_topology_test_config(void) {
  flowie_cluster_topology_plan_config_t config = FLOWIE_CLUSTER_TOPOLOGY_PLAN_CONFIG_INIT;
  config.local_node_id = tstr_v_from_cstr("node-b");
  flowie_cluster_topology_test_boot(config.local_boot_id, 2u);
  config.max_nodes = 8u;
  config.max_endpoint_size = 128u;
  config.last_applied_revision = 9u;
  return config;
}

static void flowie_cluster_topology_test_check_operation(
    const flowie_cluster_topology_plan_t *plan, size_t index,
    flowie_cluster_topology_operation_kind_t kind, const char *node_id, uint8_t boot_seed,
    const char *advertised_endpoint) {
  flowie_cluster_topology_operation_t operation = FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT;
  uint8_t expected_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_topology_test_boot(expected_boot, boot_seed);
  check_int_eq(flowie_cluster_topology_plan_operation_at(plan, index, &operation), TURBO_OK);
  check_int_eq(operation.kind, kind);
  check_size_eq(operation.peer.node_id.len, strlen(node_id));
  check_mem_eq(operation.peer.node_id.data, node_id, operation.peer.node_id.len);
  check_mem_eq(operation.peer.boot_id, expected_boot, sizeof(expected_boot));
  check_size_eq(operation.peer.advertised_endpoint.len, strlen(advertised_endpoint));
  check_mem_eq(operation.peer.advertised_endpoint.data, advertised_endpoint,
               operation.peer.advertised_endpoint.len);
}

spec("flowie cluster membership topology") {
  it("plans deterministic outgoing connectors for connectable members") {
    flowie_cluster_topology_plan_config_t config = flowie_cluster_topology_test_config();
    flowie_cluster_topology_member_t members[] = {
        flowie_cluster_topology_test_member("node-a", 1u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7101", 10u),
        flowie_cluster_topology_test_member("node-b", 2u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7102", 10u),
        flowie_cluster_topology_test_member("node-c", 3u, FLOWIE_CLUSTER_NODE_STARTING,
                                            "127.0.0.1:7103", 10u),
        flowie_cluster_topology_test_member("node-d", 4u, FLOWIE_CLUSTER_NODE_SYNCING,
                                            "127.0.0.1:7104", 10u),
        flowie_cluster_topology_test_member("node-e", 5u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7105", 10u),
        flowie_cluster_topology_test_member("node-f", 6u, FLOWIE_CLUSTER_NODE_DRAINING,
                                            "127.0.0.1:7106", 10u),
        flowie_cluster_topology_test_member("node-g", 7u, FLOWIE_CLUSTER_NODE_OFFLINE,
                                            "127.0.0.1:7107", 10u)};
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_topology_plan_t *plan = NULL;
    membership.membership_revision = 10u;
    membership.members = members;
    membership.member_count = sizeof(members) / sizeof(members[0]);

    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_OK);
    check_not_null(plan);
    check_uint_eq(flowie_cluster_topology_plan_revision(plan), 10u);
    check_size_eq(flowie_cluster_topology_plan_operation_count(plan), 3u);
    flowie_cluster_topology_test_check_operation(plan, 0u, FLOWIE_CLUSTER_TOPOLOGY_ADD, "node-d",
                                                 4u, "127.0.0.1:7104");
    flowie_cluster_topology_test_check_operation(plan, 1u, FLOWIE_CLUSTER_TOPOLOGY_ADD, "node-e",
                                                 5u, "127.0.0.1:7105");
    flowie_cluster_topology_test_check_operation(plan, 2u, FLOWIE_CLUSTER_TOPOLOGY_ADD, "node-f",
                                                 6u, "127.0.0.1:7106");
    flowie_cluster_topology_plan_destroy(plan);
  }

  it("orders replacement removals first and owns copied operation identities") {
    char node_c[] = "node-c";
    char desired_c[] = "127.0.0.1:7203";
    char current_c[] = "127.0.0.1:7103";
    flowie_cluster_topology_plan_config_t config = flowie_cluster_topology_test_config();
    flowie_cluster_topology_member_t members[] = {
        flowie_cluster_topology_test_member("node-b", 2u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7102", 10u),
        flowie_cluster_topology_test_member(node_c, 30u, FLOWIE_CLUSTER_NODE_READY, desired_c, 10u),
        flowie_cluster_topology_test_member("node-d", 40u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7104", 10u),
        flowie_cluster_topology_test_member("node-e", 50u, FLOWIE_CLUSTER_NODE_OFFLINE,
                                            "127.0.0.1:7105", 10u)};
    flowie_cluster_topology_peer_t current[] = {
        flowie_cluster_topology_test_peer(node_c, 20u, current_c),
        flowie_cluster_topology_test_peer("node-e", 50u, "127.0.0.1:7105")};
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_topology_plan_t *plan = NULL;
    membership.membership_revision = 10u;
    membership.members = members;
    membership.member_count = sizeof(members) / sizeof(members[0]);

    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, current,
                                                    sizeof(current) / sizeof(current[0]), &plan),
                 TURBO_OK);
    node_c[0] = 'x';
    desired_c[0] = 'x';
    current_c[0] = 'x';
    check_size_eq(flowie_cluster_topology_plan_operation_count(plan), 4u);
    flowie_cluster_topology_test_check_operation(plan, 0u, FLOWIE_CLUSTER_TOPOLOGY_REMOVE, "node-c",
                                                 20u, "127.0.0.1:7103");
    flowie_cluster_topology_test_check_operation(plan, 1u, FLOWIE_CLUSTER_TOPOLOGY_REMOVE, "node-e",
                                                 50u, "127.0.0.1:7105");
    flowie_cluster_topology_test_check_operation(plan, 2u, FLOWIE_CLUSTER_TOPOLOGY_ADD, "node-c",
                                                 30u, "127.0.0.1:7203");
    flowie_cluster_topology_test_check_operation(plan, 3u, FLOWIE_CLUSTER_TOPOLOGY_ADD, "node-d",
                                                 40u, "127.0.0.1:7104");
    flowie_cluster_topology_plan_destroy(plan);
  }

  it("accepts an identical applied revision and rejects divergent or stale revisions") {
    flowie_cluster_topology_plan_config_t config = flowie_cluster_topology_test_config();
    flowie_cluster_topology_member_t members[] = {
        flowie_cluster_topology_test_member("node-b", 2u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7102", 12u),
        flowie_cluster_topology_test_member("node-c", 3u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7103", 12u)};
    flowie_cluster_topology_peer_t current =
        flowie_cluster_topology_test_peer("node-c", 3u, "127.0.0.1:7103");
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_topology_plan_t *plan = NULL;
    config.last_applied_revision = 12u;
    membership.membership_revision = 12u;
    membership.members = members;
    membership.member_count = sizeof(members) / sizeof(members[0]);

    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, &current, 1u, &plan),
                 TURBO_OK);
    check_size_eq(flowie_cluster_topology_plan_operation_count(plan), 0u);
    flowie_cluster_topology_plan_destroy(plan);

    plan = NULL;
    current.advertised_endpoint = tstr_v_from_cstr("127.0.0.1:7999");
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, &current, 1u, &plan),
                 TURBO_EPROTO);
    check_null(plan);

    membership.membership_revision = 11u;
    members[0].revision = 11u;
    members[1].revision = 11u;
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, &current, 1u, &plan),
                 TURBO_EBUSY);
    check_null(plan);
  }

  it("fails fast for malformed membership and peer snapshots") {
    char long_endpoint[130];
    flowie_cluster_topology_plan_config_t config = flowie_cluster_topology_test_config();
    flowie_cluster_topology_member_t members[] = {
        flowie_cluster_topology_test_member("node-b", 2u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7102", 10u),
        flowie_cluster_topology_test_member("node-c", 3u, FLOWIE_CLUSTER_NODE_READY,
                                            "127.0.0.1:7103", 10u)};
    flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
    flowie_cluster_topology_peer_t peer =
        flowie_cluster_topology_test_peer("node-a", 1u, "127.0.0.1:7101");
    flowie_cluster_topology_plan_t *plan = NULL;
    membership.membership_revision = 10u;
    membership.members = members;
    membership.member_count = sizeof(members) / sizeof(members[0]);

    members[0].node_id = tstr_v_from_cstr("node-c");
    members[1].node_id = tstr_v_from_cstr("node-b");
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_EPROTO);
    members[0].node_id = tstr_v_from_cstr("node-b");
    members[1].node_id = tstr_v_from_cstr("node-c");

    flowie_cluster_topology_test_boot(members[0].boot_id, 99u);
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_EBUSY);
    flowie_cluster_topology_test_boot(members[0].boot_id, 2u);

    membership.members = &members[1];
    membership.member_count = 1u;
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_EBUSY);
    membership.members = members;
    membership.member_count = 2u;

    config.max_nodes = 1u;
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_EINVAL);
    config.max_nodes = 8u;

    memset(long_endpoint, 'x', sizeof(long_endpoint));
    members[1].advertised_endpoint.data = long_endpoint;
    members[1].advertised_endpoint.len = 129u;
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_EPROTO);
    members[1].advertised_endpoint = tstr_v_from_cstr("127.0.0.1:7103");

    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, &peer, 1u, &plan),
                 TURBO_EPROTO);
    config.max_endpoint_size = FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX + 1u;
    check_int_eq(flowie_cluster_topology_plan_build(&config, &membership, NULL, 0u, &plan),
                 TURBO_EINVAL);
    check_null(plan);
  }
}
