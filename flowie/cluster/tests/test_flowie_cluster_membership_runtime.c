#include "flowie_cluster_membership_runtime_internal.h"

#include "tinytest.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_membership_test_s {
  int open_results[3];
  size_t open_result_count;
  size_t open_count;
  int refresh_result;
  int apply_results[3];
  size_t apply_result_count;
  size_t apply_count;
  size_t destroy_count;
  uint64_t current_revision;
  uint64_t observed_last_applied_revision;
  uint64_t observed_apply_timeout_ns;
  atomic_int refresh_entered;
  uint32_t refresh_delay_ms;
  flowie_cluster_member_directory_t *maintenance_directory;
  size_t maintenance_count;
} flowie_cluster_membership_test_t;

static flowie_cluster_membership_test_t *flowie_cluster_membership_active_test;

static int flowie_cluster_membership_test_open(const flowie_cluster_pgsql_config_t *config,
                                               flowie_cluster_pgsql_coordinator_t **out) {
  flowie_cluster_membership_test_t *test = flowie_cluster_membership_active_test;
  int rc = TURBO_OK;
  (void)config;
  if (out) *out = NULL;
  if (!test || !out) return TURBO_EINVAL;
  if (test->open_count < test->open_result_count) rc = test->open_results[test->open_count];
  test->open_count++;
  if (rc == TURBO_OK) *out = (flowie_cluster_pgsql_coordinator_t *)test;
  return rc;
}

static void flowie_cluster_membership_test_coordinator_destroy(
    flowie_cluster_pgsql_coordinator_t *coordinator) {
  flowie_cluster_membership_test_t *test = (flowie_cluster_membership_test_t *)coordinator;
  if (test) test->destroy_count++;
}

static int
flowie_cluster_membership_test_refresh(flowie_cluster_pgsql_coordinator_t *coordinator,
                                       const flowie_cluster_topology_plan_config_t *config,
                                       const flowie_cluster_topology_peer_t *current_peers,
                                       size_t current_peer_count,
                                       flowie_cluster_topology_plan_t **out,
                                       flowie_cluster_pgsql_membership_snapshot_t *out_snapshot) {
  flowie_cluster_membership_test_t *test = (flowie_cluster_membership_test_t *)coordinator;
  flowie_cluster_topology_member_t members[2] = {FLOWIE_CLUSTER_TOPOLOGY_MEMBER_INIT,
                                                 FLOWIE_CLUSTER_TOPOLOGY_MEMBER_INIT};
  flowie_cluster_topology_membership_t membership = FLOWIE_CLUSTER_TOPOLOGY_MEMBERSHIP_INIT;
  if (out) *out = NULL;
  if (out_snapshot)
    *out_snapshot = (flowie_cluster_pgsql_membership_snapshot_t)
        FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
  if (!test || !config || !out || !out_snapshot) return TURBO_EINVAL;
  atomic_store_explicit(&test->refresh_entered, 1, memory_order_release);
  if (test->refresh_delay_ms != 0u) turbo_sleep_ms(test->refresh_delay_ms);
  test->observed_last_applied_revision = config->last_applied_revision;
  if (test->refresh_result != TURBO_OK) return test->refresh_result;
  members[0].node_id = tstr_v_from_cstr("node-a");
  members[0].boot_id[0] = 1u;
  members[0].state = FLOWIE_CLUSTER_NODE_READY;
  members[0].advertised_endpoint = tstr_v_from_cstr("127.0.0.1:7101");
  members[0].revision = 1u;
  members[1].node_id = tstr_v_from_cstr("node-b");
  members[1].boot_id[0] = 2u;
  members[1].state = FLOWIE_CLUSTER_NODE_READY;
  members[1].advertised_endpoint = tstr_v_from_cstr("127.0.0.1:7102");
  members[1].revision = 2u;
  membership.membership_revision = 2u;
  membership.members = members;
  membership.member_count = 2u;
  out_snapshot->members =
      (flowie_cluster_pgsql_member_t *)calloc(2u, sizeof(*out_snapshot->members));
  if (!out_snapshot->members) return TURBO_ENOMEM;
  out_snapshot->membership_revision = 2u;
  out_snapshot->member_count = 2u;
  for (size_t index = 0u; index < 2u; ++index) {
    flowie_cluster_pgsql_member_t *member = &out_snapshot->members[index];
    const char *node_id = index == 0u ? "node-a" : "node-b";
    const char *endpoint = index == 0u ? "127.0.0.1:7101" : "127.0.0.1:7102";
    *member = (flowie_cluster_pgsql_member_t)FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    member->node_id_size = strlen(node_id);
    memcpy(member->node_id, node_id, member->node_id_size + 1u);
    member->boot_id[0] = (uint8_t)(index + 1u);
    member->state = FLOWIE_CLUSTER_NODE_READY;
    member->advertised_endpoint_size = strlen(endpoint);
    memcpy(member->advertised_endpoint, endpoint, member->advertised_endpoint_size + 1u);
    member->lease_deadline_epoch_ms = 10000u;
    member->revision = index + 1u;
  }
  return flowie_cluster_topology_plan_build(config, &membership, current_peers,
                                            current_peer_count, out);
}

static int flowie_cluster_membership_test_current(void *ctx,
                                                  flowie_cluster_topology_peer_t *storage,
                                                  size_t capacity, size_t *out_count,
                                                  uint64_t *out_revision) {
  flowie_cluster_membership_test_t *test = (flowie_cluster_membership_test_t *)ctx;
  (void)storage;
  (void)capacity;
  if (!test || !out_count || !out_revision) return TURBO_EINVAL;
  *out_count = 0u;
  *out_revision = test->current_revision;
  return TURBO_OK;
}

static int flowie_cluster_membership_test_apply(void *ctx,
                                                const flowie_cluster_topology_plan_t *plan,
                                                uint64_t timeout_ns) {
  flowie_cluster_membership_test_t *test = (flowie_cluster_membership_test_t *)ctx;
  int rc = TURBO_OK;
  if (!test || !plan) return TURBO_EINVAL;
  test->observed_apply_timeout_ns = timeout_ns;
  if (test->apply_count < test->apply_result_count) rc = test->apply_results[test->apply_count];
  test->apply_count++;
  if (rc == TURBO_OK) test->current_revision = flowie_cluster_topology_plan_revision(plan);
  return rc;
}

static int flowie_cluster_membership_test_maintenance(void *ctx, size_t *out_changed) {
  flowie_cluster_membership_test_t *test = (flowie_cluster_membership_test_t *)ctx;
  flowie_cluster_pgsql_member_t member = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {2u};
  if (out_changed) *out_changed = 0u;
  if (!test || !test->maintenance_directory || !out_changed) return TURBO_EINVAL;
  ++test->maintenance_count;
  return flowie_cluster_member_directory_resolve(
      test->maintenance_directory, tstr_v_from_cstr("node-b"), boot_id, &member);
}

static flowie_cluster_membership_runtime_config_t
flowie_cluster_membership_test_config(flowie_cluster_membership_test_t *test,
                                      flowie_cluster_pgsql_config_t *coordinator) {
  flowie_cluster_membership_runtime_config_t config = FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_CONFIG_INIT;
  atomic_init(&test->refresh_entered, 0);
  *coordinator = (flowie_cluster_pgsql_config_t)FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  coordinator->conninfo = "host=127.0.0.1 port=1 connect_timeout=1";
  coordinator->schema_name = "flowie_cluster";
  coordinator->cluster_id = "cluster-a";
  coordinator->listener_id = "mqtt";
  coordinator->node_id = "node-a";
  coordinator->advertised_endpoint = "127.0.0.1:7101";
  coordinator->boot_id[0] = 1u;
  coordinator->shard_count = 2u;
  coordinator->lease_ttl_ms = 10000u;
  coordinator->renew_interval_ms = 1000u;
  coordinator->retry_interval_ms = 10u;
  coordinator->worst_case_db_latency_ms = 100u;
  coordinator->safety_margin_ms = 100u;
  config.coordinator = coordinator;
  config.topology.local_node_id = tstr_v_from_cstr("node-a");
  config.topology.local_boot_id[0] = 1u;
  config.topology.max_nodes = 4u;
  config.topology.max_endpoint_size = FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX;
  config.refresh_interval_ns = UINT64_C(60000000000);
  config.retry_interval_ns = UINT64_C(1000000);
  config.apply_timeout_ns = UINT64_C(5000000);
  config.current = flowie_cluster_membership_test_current;
  config.apply = flowie_cluster_membership_test_apply;
  config.topology_ctx = test;
  return config;
}

static int
flowie_cluster_membership_test_wait_refresh(flowie_cluster_membership_test_t *test) {
  unsigned attempt;
  for (attempt = 0u; attempt < 1000u; ++attempt) {
    if (atomic_load_explicit(&test->refresh_entered, memory_order_acquire)) return 1;
    turbo_sleep_ms(1u);
  }
  return 0;
}

static int flowie_cluster_membership_test_wait(flowie_cluster_membership_runtime_t *runtime,
                                               flowie_cluster_membership_runtime_state_t state,
                                               uint64_t minimum_cycles) {
  unsigned attempt;
  for (attempt = 0u; attempt < 1000u; ++attempt) {
    flowie_cluster_membership_runtime_snapshot_t snapshot =
        FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_SNAPSHOT_INIT;
    if (flowie_cluster_membership_runtime_snapshot(runtime, &snapshot) != TURBO_OK) return 0;
    if (snapshot.state == state && snapshot.cycle_count >= minimum_cycles) return 1;
    turbo_sleep_ms(1u);
  }
  return 0;
}

static const flowie_cluster_membership_runtime_api_t FLOWIE_CLUSTER_MEMBERSHIP_TEST_API = {
    sizeof(flowie_cluster_membership_runtime_api_t),
    FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_ABI_V1,
    flowie_cluster_membership_test_open,
    flowie_cluster_membership_test_coordinator_destroy,
    flowie_cluster_membership_test_refresh,
    flowie_cluster_topology_plan_destroy};

spec("flowie cluster membership runtime") {
  it("refreshes and applies topology on its dedicated worker") {
    flowie_cluster_membership_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_membership_runtime_config_t config =
        flowie_cluster_membership_test_config(&test, &coordinator);
    flowie_cluster_membership_runtime_t *runtime = NULL;
    flowie_cluster_member_directory_t *directory = NULL;
    flowie_cluster_pgsql_member_t member = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {2u};
    flowie_cluster_membership_runtime_snapshot_t snapshot =
        FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_SNAPSHOT_INIT;
    flowie_cluster_membership_active_test = &test;
    check_int_eq(flowie_cluster_member_directory_create(4u, &directory), TURBO_OK);
    config.member_directory = directory;
    config.maintenance = flowie_cluster_membership_test_maintenance;
    config.maintenance_ctx = &test;
    test.maintenance_directory = directory;
    check_int_eq(flowie_cluster_membership_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_MEMBERSHIP_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_membership_runtime_start(runtime), TURBO_OK);
    check_true(flowie_cluster_membership_test_wait(runtime, FLOWIE_CLUSTER_MEMBERSHIP_RUNNING, 1u));
    check_int_eq(flowie_cluster_membership_runtime_snapshot(runtime, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.applied_revision, 2u);
    check_uint_eq(test.observed_last_applied_revision, 0u);
    check_uint_eq(test.observed_apply_timeout_ns, config.apply_timeout_ns);
    check_int_eq(flowie_cluster_member_directory_resolve(
                     directory, tstr_v_from_cstr("node-b"), boot_id, &member),
                 TURBO_OK);
    check_str_eq(member.advertised_endpoint, "127.0.0.1:7102");
    check_int_eq(flowie_cluster_membership_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    flowie_cluster_membership_runtime_destroy(runtime);
    check_size_eq(test.open_count, 1u);
    check_size_eq(test.apply_count, 1u);
    check_size_eq(test.destroy_count, 1u);
    check_size_eq(test.maintenance_count, 1u);
    flowie_cluster_member_directory_destroy(directory);
    flowie_cluster_membership_active_test = NULL;
  }

  it("does not publish a member snapshot when topology apply fails") {
    flowie_cluster_membership_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_membership_runtime_config_t config =
        flowie_cluster_membership_test_config(&test, &coordinator);
    flowie_cluster_membership_runtime_t *runtime = NULL;
    flowie_cluster_member_directory_t *directory = NULL;
    flowie_cluster_pgsql_member_t member = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {2u};
    test.apply_results[0] = TURBO_EPROTO;
    test.apply_result_count = 1u;
    flowie_cluster_membership_active_test = &test;
    check_int_eq(flowie_cluster_member_directory_create(4u, &directory), TURBO_OK);
    config.member_directory = directory;
    check_int_eq(flowie_cluster_membership_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_MEMBERSHIP_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_membership_runtime_start(runtime), TURBO_OK);
    check_true(flowie_cluster_membership_test_wait(runtime, FLOWIE_CLUSTER_MEMBERSHIP_FAULTED, 1u));
    check_int_eq(flowie_cluster_member_directory_resolve(
                     directory, tstr_v_from_cstr("node-b"), boot_id, &member),
                 TURBO_ENOENT);
    check_int_eq(flowie_cluster_membership_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    flowie_cluster_membership_runtime_destroy(runtime);
    flowie_cluster_member_directory_destroy(directory);
    flowie_cluster_membership_active_test = NULL;
  }

  it("reopens after transient PostgreSQL failure and retries partial apply") {
    flowie_cluster_membership_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_membership_runtime_config_t config =
        flowie_cluster_membership_test_config(&test, &coordinator);
    flowie_cluster_membership_runtime_t *runtime = NULL;
    test.open_results[0] = TURBO_EIO;
    test.open_results[1] = TURBO_OK;
    test.open_result_count = 2u;
    test.apply_results[0] = TURBO_EBUSY;
    test.apply_results[1] = TURBO_OK;
    test.apply_result_count = 2u;
    flowie_cluster_membership_active_test = &test;
    check_int_eq(flowie_cluster_membership_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_MEMBERSHIP_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_membership_runtime_start(runtime), TURBO_OK);
    check_true(flowie_cluster_membership_test_wait(runtime, FLOWIE_CLUSTER_MEMBERSHIP_RUNNING, 3u));
    check_int_eq(flowie_cluster_membership_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    flowie_cluster_membership_runtime_destroy(runtime);
    check_size_eq(test.open_count, 2u);
    check_size_eq(test.apply_count, 2u);
    check_uint_eq(test.current_revision, 2u);
    flowie_cluster_membership_active_test = NULL;
  }

  it("stops retrying permanent protocol errors and remains joinable") {
    flowie_cluster_membership_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_membership_runtime_config_t config =
        flowie_cluster_membership_test_config(&test, &coordinator);
    flowie_cluster_membership_runtime_t *runtime = NULL;
    flowie_cluster_membership_runtime_snapshot_t snapshot =
        FLOWIE_CLUSTER_MEMBERSHIP_RUNTIME_SNAPSHOT_INIT;
    test.refresh_result = TURBO_EPROTO;
    flowie_cluster_membership_active_test = &test;
    check_int_eq(flowie_cluster_membership_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_MEMBERSHIP_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_membership_runtime_start(runtime), TURBO_OK);
    check_true(flowie_cluster_membership_test_wait(runtime, FLOWIE_CLUSTER_MEMBERSHIP_FAULTED, 1u));
    turbo_sleep_ms(2u);
    check_int_eq(flowie_cluster_membership_runtime_snapshot(runtime, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.cycle_count, 1u);
    check_int_eq(snapshot.last_status, TURBO_EPROTO);
    check_int_eq(flowie_cluster_membership_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    flowie_cluster_membership_runtime_destroy(runtime);
    check_size_eq(test.destroy_count, 1u);
    flowie_cluster_membership_active_test = NULL;
  }

  it("keeps the runtime alive for a close retry when a PostgreSQL cycle exceeds the deadline") {
    flowie_cluster_membership_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_membership_runtime_config_t config =
        flowie_cluster_membership_test_config(&test, &coordinator);
    flowie_cluster_membership_runtime_t *runtime = NULL;
    test.refresh_delay_ms = 50u;
    flowie_cluster_membership_active_test = &test;
    check_int_eq(flowie_cluster_membership_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_MEMBERSHIP_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_membership_runtime_start(runtime), TURBO_OK);
    check_true(flowie_cluster_membership_test_wait_refresh(&test));
    check_int_eq(flowie_cluster_membership_runtime_close(runtime, UINT64_C(1000000)),
                 TURBO_ETIMEDOUT);
    check_int_eq(flowie_cluster_membership_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    flowie_cluster_membership_runtime_destroy(runtime);
    check_size_eq(test.apply_count, 1u);
    check_size_eq(test.destroy_count, 1u);
    flowie_cluster_membership_active_test = NULL;
  }
}
