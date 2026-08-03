#include "flowie_cluster_route_dispatch_internal.h"

#include "tinytest.h"
#include "turbo_thread.h"

#include <string.h>

enum {
  FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_SHARD = 7u,
  FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_POLL_NS = 1000000u,
  FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_RETRY_NS = 1000000u,
  FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_WAIT_NS = 2000000000u
};

typedef struct flowie_cluster_route_dispatch_test_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  flowie_cluster_owner_token_t owner;
  flowie_cluster_route_dispatcher_t *dispatcher;
  int projection_failures;
  int settle_failures;
  int pending;
  int fetch_count;
  int project_count;
  int settle_count;
  int recover_count;
} flowie_cluster_route_dispatch_test_t;

static void flowie_cluster_route_dispatch_test_init(
    flowie_cluster_route_dispatch_test_t *test) {
  uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  size_t index;
  memset(test, 0, sizeof(*test));
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
  for (index = 0u; index < sizeof(boot); ++index) boot[index] = (uint8_t)(index + 1u);
  check_int_eq(flowie_cluster_owner_token_init(&test->owner,
                                               FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_SHARD, 12u,
                                               "owner-a", 7u, boot),
               TURBO_OK);
  test->pending = 1;
}

static void flowie_cluster_route_dispatch_test_cleanup(
    flowie_cluster_route_dispatch_test_t *test) {
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

static int flowie_cluster_route_dispatch_test_resolve(
    void *ctx, uint32_t shard_id, flowie_cluster_owner_token_t *out) {
  flowie_cluster_route_dispatch_test_t *test = (flowie_cluster_route_dispatch_test_t *)ctx;
  if (!test || !out || shard_id != test->owner.shard_id) return TURBO_EINVAL;
  *out = test->owner;
  return TURBO_OK;
}

static int flowie_cluster_route_dispatch_test_fetch(
    void *ctx, const flowie_cluster_owner_token_t *owner, uint64_t event_type,
    flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_route_dispatch_test_t *test = (flowie_cluster_route_dispatch_test_t *)ctx;
  static const char record_key[] = "device-a";
  static const char payload[] = "route-event";
  if (!test || !owner || !out) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->fetch_count;
  if (event_type != FLOWIE_CLUSTER_SESSION_EVENT_BOUND || !test->pending) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOENT;
  }
  turbo_mutex_unlock(&test->mutex);
  *out = (flowie_cluster_pgsql_outbox_event_t)FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  out->shard_id = owner->shard_id;
  out->event_owner_epoch = owner->owner_epoch;
  out->fact_revision = 1u;
  out->event_type = FLOWIE_CLUSTER_SESSION_EVENT_BOUND;
  out->record_kind = FLOWIE_CLUSTER_KEY_SESSION;
  out->record_key = tstr_new_len(record_key, sizeof(record_key) - 1u);
  out->payload = tstr_new_len(payload, sizeof(payload) - 1u);
  if (!out->record_key || !out->payload) {
    flowie_cluster_pgsql_outbox_event_cleanup(out);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flowie_cluster_route_dispatch_test_project(
    void *ctx, const flowie_cluster_owner_token_t *owner,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_route_dispatch_test_t *test = (flowie_cluster_route_dispatch_test_t *)ctx;
  int rc = TURBO_OK;
  if (!test || !owner || !event) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->project_count;
  if (test->project_count <= test->projection_failures) rc = TURBO_EIO;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_route_dispatch_test_settle(
    void *ctx, const flowie_cluster_owner_token_t *owner,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_route_dispatch_test_t *test = (flowie_cluster_route_dispatch_test_t *)ctx;
  int rc = TURBO_OK;
  if (!test || !owner || !event) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->settle_count;
  if (test->settle_count <= test->settle_failures)
    rc = TURBO_EIO;
  else
    test->pending = 0;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_route_dispatch_test_recover(void *ctx) {
  flowie_cluster_route_dispatch_test_t *test = (flowie_cluster_route_dispatch_test_t *)ctx;
  if (!test) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->recover_count;
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static flowie_cluster_route_dispatcher_config_t
flowie_cluster_route_dispatch_test_config(flowie_cluster_route_dispatch_test_t *test) {
  flowie_cluster_route_dispatcher_config_t config =
      FLOWIE_CLUSTER_ROUTE_DISPATCHER_CONFIG_INIT;
  config.shard_id = FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_SHARD;
  config.max_payload_size = 1024u;
  config.poll_interval_ns = FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_POLL_NS;
  config.retry_interval_ns = FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_RETRY_NS;
  config.resolve = flowie_cluster_route_dispatch_test_resolve;
  config.resolve_ctx = test;
  config.fetch = flowie_cluster_route_dispatch_test_fetch;
  config.settle = flowie_cluster_route_dispatch_test_settle;
  config.recover = flowie_cluster_route_dispatch_test_recover;
  config.source_ctx = test;
  config.project = flowie_cluster_route_dispatch_test_project;
  config.project_ctx = test;
  return config;
}

static int flowie_cluster_route_dispatch_test_wait(flowie_cluster_route_dispatch_test_t *test,
                                                   int expected_settles) {
  uint64_t start = turbo_hrtime();
  uint64_t deadline = start + FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_WAIT_NS;
  int ready;
  turbo_mutex_lock(&test->mutex);
  while (test->settle_count < expected_settles) {
    uint64_t now = turbo_hrtime();
    if (now >= deadline) break;
    (void)turbo_cond_timedwait(&test->changed, &test->mutex, deadline - now);
  }
  ready = test->settle_count >= expected_settles;
  turbo_mutex_unlock(&test->mutex);
  return ready ? TURBO_OK : TURBO_ETIMEDOUT;
}

static void flowie_cluster_route_dispatch_test_stop(
    flowie_cluster_route_dispatch_test_t *test) {
  check_int_eq(flowie_cluster_route_dispatcher_close(test->dispatcher), TURBO_OK);
  check_int_eq(flowie_cluster_route_dispatcher_drain(
                   test->dispatcher, FLOWIE_CLUSTER_ROUTE_DISPATCH_TEST_WAIT_NS),
               TURBO_OK);
  check_int_eq(flowie_cluster_route_dispatcher_destroy(test->dispatcher), TURBO_OK);
  test->dispatcher = NULL;
}

spec("flowie cluster route outbox dispatch") {
  it("does not settle until projection succeeds") {
    flowie_cluster_route_dispatch_test_t test;
    flowie_cluster_route_dispatcher_config_t config;
    flowie_cluster_route_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_ROUTE_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_route_dispatch_test_init(&test);
    test.projection_failures = 1;
    config = flowie_cluster_route_dispatch_test_config(&test);
    check_int_eq(flowie_cluster_route_dispatcher_create(&config, &test.dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_route_dispatch_test_wait(&test, 1), TURBO_OK);
    check_int_eq(flowie_cluster_route_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_int_eq(test.settle_count, 1);
    check_int_eq(test.project_count, 2);
    check_int_eq(test.recover_count, 0);
    check_int_eq(snapshot.projected_events, 1u);
    check_int_eq(snapshot.settled_events, 1u);
    flowie_cluster_route_dispatch_test_stop(&test);
    flowie_cluster_route_dispatch_test_cleanup(&test);
  }

  it("recovers the PG source after settlement I/O failure") {
    flowie_cluster_route_dispatch_test_t test;
    flowie_cluster_route_dispatcher_config_t config;
    flowie_cluster_route_dispatch_test_init(&test);
    test.settle_failures = 1;
    config = flowie_cluster_route_dispatch_test_config(&test);
    check_int_eq(flowie_cluster_route_dispatcher_create(&config, &test.dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_route_dispatch_test_wait(&test, 2), TURBO_OK);
    check_int_eq(test.project_count, 2);
    check_int_eq(test.recover_count, 1);
    flowie_cluster_route_dispatch_test_stop(&test);
    flowie_cluster_route_dispatch_test_cleanup(&test);
  }
}
