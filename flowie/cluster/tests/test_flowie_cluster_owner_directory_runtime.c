#include "flowie_cluster_owner_directory_runtime_internal.h"

#include "tinytest.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_owner_runtime_test_s {
  int open_results[4];
  size_t open_result_count;
  size_t open_count;
  int snapshot_results[4];
  size_t snapshot_result_count;
  size_t snapshot_count;
  size_t destroy_count;
  size_t cleanup_count;
  uint32_t snapshot_delay_ms;
  atomic_int snapshot_entered;
} flowie_cluster_owner_runtime_test_t;

static flowie_cluster_owner_runtime_test_t *flowie_cluster_owner_runtime_active_test;

static int flowie_cluster_owner_runtime_test_open(
    const flowie_cluster_pgsql_config_t *config,
    flowie_cluster_pgsql_coordinator_t **out) {
  flowie_cluster_owner_runtime_test_t *test = flowie_cluster_owner_runtime_active_test;
  int rc = TURBO_OK;
  (void)config;
  if (out) *out = NULL;
  if (!test || !out) return TURBO_EINVAL;
  if (test->open_count < test->open_result_count) rc = test->open_results[test->open_count];
  test->open_count++;
  if (rc == TURBO_OK) *out = (flowie_cluster_pgsql_coordinator_t *)test;
  return rc;
}

static void flowie_cluster_owner_runtime_test_destroy(
    flowie_cluster_pgsql_coordinator_t *coordinator) {
  flowie_cluster_owner_runtime_test_t *test =
      (flowie_cluster_owner_runtime_test_t *)coordinator;
  if (test) test->destroy_count++;
}

static int flowie_cluster_owner_runtime_test_snapshot(
    flowie_cluster_pgsql_coordinator_t *coordinator,
    flowie_cluster_pgsql_shard_owner_snapshot_t *out) {
  flowie_cluster_owner_runtime_test_t *test =
      (flowie_cluster_owner_runtime_test_t *)coordinator;
  int rc = TURBO_OK;
  if (out) *out = (flowie_cluster_pgsql_shard_owner_snapshot_t)
      FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT;
  if (!test || !out) return TURBO_EINVAL;
  atomic_store_explicit(&test->snapshot_entered, 1, memory_order_release);
  if (test->snapshot_delay_ms != 0u) turbo_sleep_ms(test->snapshot_delay_ms);
  if (test->snapshot_count < test->snapshot_result_count)
    rc = test->snapshot_results[test->snapshot_count];
  test->snapshot_count++;
  if (rc != TURBO_OK) return rc;
  out->owners = (flowie_cluster_pgsql_shard_owner_t *)calloc(2u, sizeof(*out->owners));
  if (!out->owners) return TURBO_ENOMEM;
  out->owner_count = 2u;
  for (size_t index = 0u; index < out->owner_count; ++index) {
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0};
    const char *node_id = index == 0u ? "node-a" : "node-b";
    flowie_cluster_pgsql_shard_owner_t *owner = &out->owners[index];
    *owner = (flowie_cluster_pgsql_shard_owner_t)FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_INIT;
    owner->shard_id = (uint32_t)index;
    owner->local_deadline_ns = turbo_hrtime() + UINT64_C(1000000000);
    boot_id[0] = (uint8_t)(index + 1u);
    rc = flowie_cluster_owner_token_init(&owner->owner, (uint32_t)index,
                                         (uint64_t)index + 10u, node_id, strlen(node_id),
                                         boot_id);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static void flowie_cluster_owner_runtime_test_snapshot_cleanup(
    flowie_cluster_pgsql_shard_owner_snapshot_t *snapshot) {
  flowie_cluster_owner_runtime_test_t *test = flowie_cluster_owner_runtime_active_test;
  if (test) test->cleanup_count++;
  flowie_cluster_pgsql_shard_owner_snapshot_cleanup(snapshot);
}

static flowie_cluster_owner_directory_runtime_config_t
flowie_cluster_owner_runtime_test_config(flowie_cluster_owner_runtime_test_t *test,
                                         flowie_cluster_pgsql_config_t *coordinator) {
  flowie_cluster_owner_directory_runtime_config_t config =
      FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_CONFIG_INIT;
  atomic_init(&test->snapshot_entered, 0);
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
  config.refresh_interval_ns = UINT64_C(60000000000);
  config.retry_interval_ns = UINT64_C(1000000);
  return config;
}

static int flowie_cluster_owner_runtime_test_wait_state(
    flowie_cluster_owner_directory_runtime_t *runtime,
    flowie_cluster_owner_directory_runtime_state_t state, uint64_t minimum_cycles) {
  for (unsigned attempt = 0u; attempt < 1000u; ++attempt) {
    flowie_cluster_owner_directory_runtime_snapshot_t snapshot =
        FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_SNAPSHOT_INIT;
    if (flowie_cluster_owner_directory_runtime_snapshot(runtime, &snapshot) != TURBO_OK) return 0;
    if (snapshot.state == state && snapshot.cycle_count >= minimum_cycles) return 1;
    turbo_sleep_ms(1u);
  }
  return 0;
}

static int flowie_cluster_owner_runtime_test_wait_snapshot(
    flowie_cluster_owner_runtime_test_t *test) {
  for (unsigned attempt = 0u; attempt < 1000u; ++attempt) {
    if (atomic_load_explicit(&test->snapshot_entered, memory_order_acquire)) return 1;
    turbo_sleep_ms(1u);
  }
  return 0;
}

static const flowie_cluster_owner_directory_runtime_api_t
    FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_TEST_API = {
        sizeof(flowie_cluster_owner_directory_runtime_api_t),
        FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_ABI_V1,
        flowie_cluster_owner_runtime_test_open,
        flowie_cluster_owner_runtime_test_destroy,
        flowie_cluster_owner_runtime_test_snapshot,
        flowie_cluster_owner_runtime_test_snapshot_cleanup};

spec("flowie cluster owner directory runtime") {
  it("publishes the first complete snapshot before edge resolution becomes ready") {
    flowie_cluster_owner_runtime_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_owner_directory_runtime_config_t config =
        flowie_cluster_owner_runtime_test_config(&test, &coordinator);
    flowie_cluster_owner_directory_runtime_t *runtime = NULL;
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_owner_directory_runtime_snapshot_t snapshot =
        FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_SNAPSHOT_INIT;
    flowie_cluster_owner_runtime_active_test = &test;
    check_int_eq(flowie_cluster_owner_directory_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_resolve_shard(
                     flowie_cluster_owner_directory_runtime_directory(runtime), 0u, &owner),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_owner_directory_runtime_start(runtime), TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_runtime_wait_ready(
                     runtime, UINT64_C(1000000000)),
                 TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_resolve_shard(
                     flowie_cluster_owner_directory_runtime_directory(runtime), 0u, &owner),
                 TURBO_OK);
    check_uint_eq(owner.owner_epoch, 10u);
    check_int_eq(flowie_cluster_owner_directory_runtime_snapshot(runtime, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.refresh_generation, 1u);
    flowie_cluster_owner_directory_runtime_destroy(runtime);
    check_size_eq(test.open_count, 1u);
    check_size_eq(test.snapshot_count, 1u);
    check_size_eq(test.cleanup_count, 1u);
    check_size_eq(test.destroy_count, 1u);
    flowie_cluster_owner_runtime_active_test = NULL;
  }

  it("reopens its dedicated PostgreSQL connection after transient failures") {
    flowie_cluster_owner_runtime_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_owner_directory_runtime_config_t config =
        flowie_cluster_owner_runtime_test_config(&test, &coordinator);
    flowie_cluster_owner_directory_runtime_t *runtime = NULL;
    test.open_results[0] = TURBO_EIO;
    test.open_results[1] = TURBO_OK;
    test.open_results[2] = TURBO_OK;
    test.open_result_count = 3u;
    test.snapshot_results[0] = TURBO_EIO;
    test.snapshot_results[1] = TURBO_OK;
    test.snapshot_result_count = 2u;
    flowie_cluster_owner_runtime_active_test = &test;
    check_int_eq(flowie_cluster_owner_directory_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_runtime_start(runtime), TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_runtime_wait_ready(
                     runtime, UINT64_C(1000000000)),
                 TURBO_OK);
    check_true(flowie_cluster_owner_runtime_test_wait_state(
        runtime, FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNNING, 3u));
    flowie_cluster_owner_directory_runtime_destroy(runtime);
    check_size_eq(test.open_count, 3u);
    check_size_eq(test.snapshot_count, 2u);
    check_size_eq(test.destroy_count, 2u);
    flowie_cluster_owner_runtime_active_test = NULL;
  }

  it("fails startup immediately on a permanent snapshot protocol error") {
    flowie_cluster_owner_runtime_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_owner_directory_runtime_config_t config =
        flowie_cluster_owner_runtime_test_config(&test, &coordinator);
    flowie_cluster_owner_directory_runtime_t *runtime = NULL;
    test.snapshot_results[0] = TURBO_EPROTO;
    test.snapshot_result_count = 1u;
    flowie_cluster_owner_runtime_active_test = &test;
    check_int_eq(flowie_cluster_owner_directory_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_runtime_start(runtime), TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_runtime_wait_ready(
                     runtime, UINT64_C(1000000000)),
                 TURBO_EPROTO);
    check_true(flowie_cluster_owner_runtime_test_wait_state(
        runtime, FLOWIE_CLUSTER_OWNER_DIRECTORY_FAULTED, 1u));
    flowie_cluster_owner_directory_runtime_destroy(runtime);
    check_size_eq(test.snapshot_count, 1u);
    flowie_cluster_owner_runtime_active_test = NULL;
  }

  it("keeps a timed-out close joinable while a database snapshot is in flight") {
    flowie_cluster_owner_runtime_test_t test = {0};
    flowie_cluster_pgsql_config_t coordinator;
    flowie_cluster_owner_directory_runtime_config_t config =
        flowie_cluster_owner_runtime_test_config(&test, &coordinator);
    flowie_cluster_owner_directory_runtime_t *runtime = NULL;
    test.snapshot_delay_ms = 50u;
    flowie_cluster_owner_runtime_active_test = &test;
    check_int_eq(flowie_cluster_owner_directory_runtime_create_with_api(
                     &config, &FLOWIE_CLUSTER_OWNER_DIRECTORY_RUNTIME_TEST_API, &runtime),
                 TURBO_OK);
    check_int_eq(flowie_cluster_owner_directory_runtime_start(runtime), TURBO_OK);
    check_true(flowie_cluster_owner_runtime_test_wait_snapshot(&test));
    check_int_eq(flowie_cluster_owner_directory_runtime_close(runtime, UINT64_C(1000000)),
                 TURBO_ETIMEDOUT);
    check_int_eq(flowie_cluster_owner_directory_runtime_close(runtime, UINT64_MAX), TURBO_OK);
    flowie_cluster_owner_directory_runtime_destroy(runtime);
    check_size_eq(test.snapshot_count, 1u);
    check_size_eq(test.destroy_count, 1u);
    flowie_cluster_owner_runtime_active_test = NULL;
  }
}
