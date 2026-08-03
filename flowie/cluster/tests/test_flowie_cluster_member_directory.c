#include "flowie_cluster_member_directory_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static void flowie_cluster_member_directory_test_member(
    flowie_cluster_pgsql_member_t *member, const char *node_id, uint8_t boot_seed,
    const char *endpoint, uint64_t revision) {
  size_t node_id_size = strlen(node_id);
  size_t endpoint_size = strlen(endpoint);
  *member = (flowie_cluster_pgsql_member_t)FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
  memcpy(member->node_id, node_id, node_id_size + 1u);
  member->node_id_size = node_id_size;
  for (size_t index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    member->boot_id[index] = (uint8_t)(boot_seed + index);
  member->state = FLOWIE_CLUSTER_NODE_READY;
  memcpy(member->advertised_endpoint, endpoint, endpoint_size + 1u);
  member->advertised_endpoint_size = endpoint_size;
  member->lease_deadline_epoch_ms = 10000u + revision;
  member->revision = revision;
}

spec("flowie cluster member directory") {
  it("accepts the empty revision-zero snapshot of a new PostgreSQL cluster") {
    flowie_cluster_pgsql_membership_snapshot_t source =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_member_directory_t *directory = NULL;
    flowie_cluster_member_directory_snapshot_t *candidate = NULL;
    flowie_cluster_pgsql_member_t resolved = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};
    check_int_eq(flowie_cluster_member_directory_create(1u, &directory), TURBO_OK);
    check_int_eq(flowie_cluster_member_directory_prepare(directory, &source, &candidate),
                 TURBO_OK);
    check_int_eq(flowie_cluster_member_directory_publish(directory, candidate), TURBO_OK);
    candidate = NULL;
    check_int_eq(flowie_cluster_member_directory_resolve(
                     directory, tstr_v_from_cstr("node-a"), boot_id, &resolved),
                 TURBO_ENOENT);
    flowie_cluster_member_directory_snapshot_destroy(candidate);
    flowie_cluster_member_directory_destroy(directory);
  }

  it("publishes one immutable snapshot and resolves exact node boot identity") {
    flowie_cluster_pgsql_member_t members[2];
    flowie_cluster_pgsql_membership_snapshot_t source =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_member_directory_t *directory = NULL;
    flowie_cluster_member_directory_snapshot_t *candidate = NULL;
    flowie_cluster_pgsql_member_t resolved = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    uint8_t wrong_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    flowie_cluster_member_directory_test_member(&members[0], "node-a", 1u,
                                                "127.0.0.1:7101", 1u);
    flowie_cluster_member_directory_test_member(&members[1], "node-b", 2u,
                                                "127.0.0.1:7102", 2u);
    source.membership_revision = 2u;
    source.members = members;
    source.member_count = 2u;

    check_int_eq(flowie_cluster_member_directory_create(4u, &directory), TURBO_OK);
    check_int_eq(flowie_cluster_member_directory_resolve(
                     directory, tstr_v_from_cstr("node-a"), members[0].boot_id, &resolved),
                 TURBO_ENOENT);
    check_int_eq(flowie_cluster_member_directory_prepare(directory, &source, &candidate),
                 TURBO_OK);
    members[0].advertised_endpoint[0] = 'x';
    check_int_eq(flowie_cluster_member_directory_publish(directory, candidate), TURBO_OK);
    candidate = NULL;
    check_int_eq(flowie_cluster_member_directory_resolve(
                     directory, tstr_v_from_cstr("node-a"), members[0].boot_id, &resolved),
                 TURBO_OK);
    check_str_eq(resolved.advertised_endpoint, "127.0.0.1:7101");
    check_int_eq(flowie_cluster_member_directory_resolve(
                     directory, tstr_v_from_cstr("node-a"), wrong_boot, &resolved),
                 TURBO_ENOENT);
    flowie_cluster_member_directory_snapshot_destroy(candidate);
    flowie_cluster_member_directory_destroy(directory);
  }

  it("rejects unordered and over-capacity snapshots before publication") {
    flowie_cluster_pgsql_member_t members[2];
    flowie_cluster_pgsql_membership_snapshot_t source =
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
    flowie_cluster_member_directory_t *directory = NULL;
    flowie_cluster_member_directory_snapshot_t *candidate = NULL;
    flowie_cluster_member_directory_test_member(&members[0], "node-b", 2u,
                                                "127.0.0.1:7102", 1u);
    flowie_cluster_member_directory_test_member(&members[1], "node-a", 1u,
                                                "127.0.0.1:7101", 2u);
    source.membership_revision = 2u;
    source.members = members;
    source.member_count = 2u;

    check_int_eq(flowie_cluster_member_directory_create(1u, &directory), TURBO_OK);
    check_int_eq(flowie_cluster_member_directory_prepare(directory, &source, &candidate),
                 TURBO_EINVAL);
    flowie_cluster_member_directory_destroy(directory);
    check_int_eq(flowie_cluster_member_directory_create(2u, &directory), TURBO_OK);
    check_int_eq(flowie_cluster_member_directory_prepare(directory, &source, &candidate),
                 TURBO_EPROTO);
    check_null(candidate);
    flowie_cluster_member_directory_destroy(directory);
  }
}
