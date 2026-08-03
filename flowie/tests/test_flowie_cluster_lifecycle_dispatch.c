#include "flowie_cluster_lifecycle_dispatch_internal.h"

#include "flowie_cluster_peer_wire_internal.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <string.h>

static const uint64_t FLOWIE_CLUSTER_LIFECYCLE_TEST_WAIT_NS = 2000000000u;
static const uint64_t FLOWIE_CLUSTER_LIFECYCLE_TEST_INTERVAL_NS = 1000000u;

static void flowie_cluster_lifecycle_test_boot(uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                               uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot[index] = (uint8_t)(seed + index);
}

static tstr_t
flowie_cluster_lifecycle_test_payload(const uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  static const char edge_node[] = "edge-a";
  size_t edge_node_size = sizeof(edge_node) - 1u;
  size_t total = FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE + edge_node_size;
  tstr_t payload = tstr_new_len(NULL, total);
  uint8_t *bytes = (uint8_t *)payload;
  if (!payload) return NULL;
  memcpy(bytes, "TFLE", 4u);
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  memset(bytes + 12u, 0, 4u);
  flowie_cluster_peer_wire_write_u64(bytes + 16u, 71u);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, 9u);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, 101u);
  flowie_cluster_peer_wire_write_u64(bytes + 40u, 3u);
  memcpy(bytes + 48u, edge_boot, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  flowie_cluster_peer_wire_write_u16(bytes + 64u, (uint16_t)edge_node_size);
  flowie_cluster_peer_wire_write_u16(bytes + 66u, 0u);
  memcpy(bytes + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, edge_node, edge_node_size);
  return payload;
}

static flowie_cluster_pgsql_outbox_event_t
flowie_cluster_lifecycle_test_event(const uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  size_t index;
  for (index = 0u; index < sizeof(event.command_id); ++index)
    event.command_id[index] = (uint8_t)(0x40u + index);
  event.shard_id = 7u;
  event.event_owner_epoch = 4u;
  event.fact_revision = 8u;
  event.event_type = FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST;
  event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
  event.record_key = tstr_dup("device-a");
  event.payload = flowie_cluster_lifecycle_test_payload(edge_boot);
  event.created_at_epoch_seconds = 1000u;
  event.attempt_count = 1u;
  return event;
}

typedef struct flowie_cluster_lifecycle_dispatch_test_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  flowie_cluster_owner_token_t owner;
  uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_lifecycle_dispatcher_t *dispatcher;
  flowie_cluster_lifecycle_apply_complete_fn delayed_complete;
  void *delayed_complete_ctx;
  int event_delivered;
  int fetch_count;
  int apply_count;
  int settle_count;
  int recover_count;
  int apply_failures;
  int settle_failures;
  int delay_completion;
} flowie_cluster_lifecycle_dispatch_test_t;

static void
flowie_cluster_lifecycle_dispatch_test_init(flowie_cluster_lifecycle_dispatch_test_t *test) {
  uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  memset(test, 0, sizeof(*test));
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
  flowie_cluster_lifecycle_test_boot(owner_boot, 1u);
  flowie_cluster_lifecycle_test_boot(test->edge_boot, 33u);
  check_int_eq(flowie_cluster_owner_token_init(&test->owner, 7u, 12u, "owner-a", 7u, owner_boot),
               TURBO_OK);
}

static void
flowie_cluster_lifecycle_dispatch_test_cleanup(flowie_cluster_lifecycle_dispatch_test_t *test) {
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

static int flowie_cluster_lifecycle_dispatch_test_owner(void *ctx, uint32_t shard_id,
                                                        flowie_cluster_owner_token_t *out) {
  flowie_cluster_lifecycle_dispatch_test_t *test = (flowie_cluster_lifecycle_dispatch_test_t *)ctx;
  if (!out || shard_id != test->owner.shard_id) return TURBO_EBUSY;
  *out = test->owner;
  return TURBO_OK;
}

static int
flowie_cluster_lifecycle_dispatch_test_fetch(void *ctx,
                                             const flowie_cluster_owner_token_t *current_owner,
                                             flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_lifecycle_dispatch_test_t *test = (flowie_cluster_lifecycle_dispatch_test_t *)ctx;
  if (!current_owner || !out) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->fetch_count;
  if (test->event_delivered) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOENT;
  }
  test->event_delivered = 1;
  turbo_mutex_unlock(&test->mutex);
  *out = flowie_cluster_lifecycle_test_event(test->edge_boot);
  if (!out->record_key || !out->payload) {
    flowie_cluster_pgsql_outbox_event_cleanup(out);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int
flowie_cluster_lifecycle_dispatch_test_settle(void *ctx,
                                              const flowie_cluster_owner_token_t *current_owner,
                                              const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_lifecycle_dispatch_test_t *test = (flowie_cluster_lifecycle_dispatch_test_t *)ctx;
  int status = TURBO_OK;
  if (!current_owner || !event) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->settle_count;
  if (test->settle_count <= test->settle_failures) status = TURBO_EIO;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return status;
}

static int flowie_cluster_lifecycle_dispatch_test_recover(void *ctx) {
  flowie_cluster_lifecycle_dispatch_test_t *test = (flowie_cluster_lifecycle_dispatch_test_t *)ctx;
  turbo_mutex_lock(&test->mutex);
  ++test->recover_count;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static int flowie_cluster_lifecycle_dispatch_test_apply(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_lifecycle_event_view_t *event,
    flowie_cluster_lifecycle_apply_complete_fn complete, void *complete_ctx) {
  flowie_cluster_lifecycle_dispatch_test_t *test = (flowie_cluster_lifecycle_dispatch_test_t *)ctx;
  int attempt;
  if (!current_owner || !event || !complete || event->expected_fact_revision != 8u ||
      event->client_id.size != sizeof("device-a") - 1u ||
      memcmp(event->client_id.data, "device-a", event->client_id.size) != 0)
    return TURBO_EPROTO;
  turbo_mutex_lock(&test->mutex);
  attempt = ++test->apply_count;
  if (test->delay_completion) {
    test->delayed_complete = complete;
    test->delayed_complete_ctx = complete_ctx;
    turbo_cond_broadcast(&test->changed);
    turbo_mutex_unlock(&test->mutex);
    return TURBO_OK;
  }
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  if (attempt <= test->apply_failures) return TURBO_ENOSPC;
  complete(complete_ctx, TURBO_OK);
  return TURBO_OK;
}

static flowie_cluster_lifecycle_dispatcher_config_t
flowie_cluster_lifecycle_dispatch_test_config(flowie_cluster_lifecycle_dispatch_test_t *test) {
  flowie_cluster_lifecycle_dispatcher_config_t config =
      FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_CONFIG_INIT;
  config.shard_id = 7u;
  config.max_payload_size = 1024u;
  config.poll_interval_ns = FLOWIE_CLUSTER_LIFECYCLE_TEST_INTERVAL_NS;
  config.retry_interval_ns = FLOWIE_CLUSTER_LIFECYCLE_TEST_INTERVAL_NS;
  config.resolve = flowie_cluster_lifecycle_dispatch_test_owner;
  config.resolve_ctx = test;
  config.fetch = flowie_cluster_lifecycle_dispatch_test_fetch;
  config.settle = flowie_cluster_lifecycle_dispatch_test_settle;
  config.recover = flowie_cluster_lifecycle_dispatch_test_recover;
  config.source_ctx = test;
  config.apply = flowie_cluster_lifecycle_dispatch_test_apply;
  config.apply_ctx = test;
  return config;
}

static int
flowie_cluster_lifecycle_dispatch_test_wait(flowie_cluster_lifecycle_dispatch_test_t *test,
                                            int expected_applies, int expected_settles) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = start_ns + FLOWIE_CLUSTER_LIFECYCLE_TEST_WAIT_NS;
  int ready;
  turbo_mutex_lock(&test->mutex);
  while (test->apply_count < expected_applies || test->settle_count < expected_settles) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&test->changed, &test->mutex, deadline_ns - now_ns);
  }
  ready = test->apply_count >= expected_applies && test->settle_count >= expected_settles;
  turbo_mutex_unlock(&test->mutex);
  return ready ? TURBO_OK : TURBO_ETIMEDOUT;
}

static void
flowie_cluster_lifecycle_dispatch_test_start(flowie_cluster_lifecycle_dispatch_test_t *test) {
  flowie_cluster_lifecycle_dispatcher_config_t config =
      flowie_cluster_lifecycle_dispatch_test_config(test);
  check_int_eq(flowie_cluster_lifecycle_dispatcher_create(&config, &test->dispatcher), TURBO_OK);
  check_not_null(test->dispatcher);
}

static void
flowie_cluster_lifecycle_dispatch_test_stop(flowie_cluster_lifecycle_dispatch_test_t *test) {
  check_int_eq(flowie_cluster_lifecycle_dispatcher_close(test->dispatcher), TURBO_OK);
  check_int_eq(flowie_cluster_lifecycle_dispatcher_drain(test->dispatcher,
                                                         FLOWIE_CLUSTER_LIFECYCLE_TEST_WAIT_NS),
               TURBO_OK);
  check_int_eq(flowie_cluster_lifecycle_dispatcher_destroy(test->dispatcher), TURBO_OK);
  test->dispatcher = NULL;
}

spec("flowie cluster lifecycle outbox dispatch") {
  it("orders a delayed Will before durable session expiry") {
    flowie_cluster_lifecycle_event_view_t event = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_cluster_lifecycle_decision_t decision = FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    event.expected_fact_revision = 8u;
    event.created_at_epoch_seconds = 1000u;
    event.session_id = 101u;
    event.session_generation = 3u;
    snapshot.resource_generation = 9u;
    snapshot.session_id = 101u;
    snapshot.session_generation = 3u;
    snapshot.session_expiry_interval = 60u;
    snapshot.has_will = 1u;
    snapshot.will_pending = 1u;
    snapshot.will_delay_interval = 10u;

    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 1005u, &decision), TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT);
    check_hex64_eq(decision.deadline_epoch_seconds, 1010u);
    decision = (flowie_cluster_lifecycle_decision_t)FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 1010u, &decision), TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL);

    snapshot.has_will = 0u;
    snapshot.will_pending = 0u;
    decision = (flowie_cluster_lifecycle_decision_t)FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 1010u, &decision), TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT);
    check_hex64_eq(decision.deadline_epoch_seconds, 1060u);
    decision = (flowie_cluster_lifecycle_decision_t)FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 1060u, &decision), TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_EXPIRE_SESSION);
  }

  it("caps Will delay at session expiry and treats reconnect as terminal") {
    flowie_cluster_lifecycle_event_view_t event = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_cluster_lifecycle_decision_t decision = FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    event.expected_fact_revision = 5u;
    event.created_at_epoch_seconds = 2000u;
    event.session_id = 51u;
    event.session_generation = 7u;
    snapshot.resource_generation = 5u;
    snapshot.session_id = 51u;
    snapshot.session_generation = 7u;
    snapshot.session_expiry_interval = 5u;
    snapshot.has_will = 1u;
    snapshot.will_pending = 1u;
    snapshot.will_delay_interval = 30u;

    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 2005u, &decision), TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL);
    check_hex64_eq(decision.deadline_epoch_seconds, 2005u);
    snapshot.active = 1u;
    snapshot.session_generation = 8u;
    decision = (flowie_cluster_lifecycle_decision_t)FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 2005u, &decision), TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE);
  }

  it("rejects a session view older than the durable lifecycle fact") {
    flowie_cluster_lifecycle_event_view_t event = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_cluster_lifecycle_decision_t decision = FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    event.expected_fact_revision = 9u;
    event.created_at_epoch_seconds = 3000u;
    event.session_id = 61u;
    event.session_generation = 2u;
    snapshot.resource_generation = 8u;
    snapshot.session_id = 61u;
    snapshot.session_generation = 2u;

    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, 3000u, &decision),
                 TURBO_EPROTO);
  }

  it("saturates deadline arithmetic and completes an infinite idle session") {
    flowie_cluster_lifecycle_event_view_t event = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_cluster_lifecycle_decision_t decision = FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    event.expected_fact_revision = 4u;
    event.created_at_epoch_seconds = UINT64_MAX - 5u;
    event.session_id = 71u;
    event.session_generation = 6u;
    snapshot.resource_generation = 4u;
    snapshot.session_id = 71u;
    snapshot.session_generation = 6u;
    snapshot.session_expiry_interval = UINT32_MAX;

    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, UINT64_MAX - 1u, &decision),
                 TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_COMPLETE);
    snapshot.has_will = 1u;
    snapshot.will_pending = 1u;
    snapshot.will_delay_interval = 10u;
    decision = (flowie_cluster_lifecycle_decision_t)FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, UINT64_MAX - 1u, &decision),
                 TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_WAIT);
    check_hex64_eq(decision.deadline_epoch_seconds, UINT64_MAX);
    decision = (flowie_cluster_lifecycle_decision_t)FLOWIE_CLUSTER_LIFECYCLE_DECISION_INIT;
    check_int_eq(flowie_cluster_lifecycle_decide(&event, &snapshot, UINT64_MAX, &decision),
                 TURBO_OK);
    check_int_eq(decision.action, FLOWIE_CLUSTER_LIFECYCLE_ACTION_PUBLISH_WILL);
  }

  it("decodes only an exact durable TFLE row") {
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_pgsql_outbox_event_t event;
    flowie_cluster_lifecycle_event_view_t decoded = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_cluster_lifecycle_test_boot(edge_boot, 33u);
    event = flowie_cluster_lifecycle_test_event(edge_boot);
    check_not_null(event.record_key);
    check_not_null(event.payload);
    check_int_eq(flowie_cluster_lifecycle_event_decode(&event, 1024u, &decoded), TURBO_OK);
    check_uint_eq(decoded.shard_id, 7u);
    check_uint_eq(decoded.expected_fact_revision, 8u);
    check_uint_eq(decoded.created_at_epoch_seconds, 1000u);
    check_uint_eq(decoded.connection_id, 71u);
    check_uint_eq(decoded.session_generation, 3u);
    check_mem_eq(decoded.edge_node_id.data, "edge-a", decoded.edge_node_id.size);
    memcpy(event.payload, "TFTE", 4u);
    check_int_eq(flowie_cluster_lifecycle_event_decode(&event, 1024u, &decoded), TURBO_EPROTO);
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }

  it("retries bounded apply admission before settling the TFLE row") {
    flowie_cluster_lifecycle_dispatch_test_t test;
    flowie_cluster_lifecycle_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_LIFECYCLE_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_lifecycle_dispatch_test_init(&test);
    test.apply_failures = 1;
    flowie_cluster_lifecycle_dispatch_test_start(&test);
    check_int_eq(flowie_cluster_lifecycle_dispatch_test_wait(&test, 2, 1), TURBO_OK);
    check_int_eq(flowie_cluster_lifecycle_dispatcher_snapshot(test.dispatcher, &snapshot),
                 TURBO_OK);
    check_uint_eq(snapshot.fetched_events, 1u);
    check_uint_eq(snapshot.apply_attempts, 2u);
    check_uint_eq(snapshot.settled_events, 1u);
    flowie_cluster_lifecycle_dispatch_test_stop(&test);
    flowie_cluster_lifecycle_dispatch_test_cleanup(&test);
  }

  it("recovers uncertain settlement without applying the lifecycle action twice") {
    flowie_cluster_lifecycle_dispatch_test_t test;
    flowie_cluster_lifecycle_dispatch_test_init(&test);
    test.settle_failures = 1;
    flowie_cluster_lifecycle_dispatch_test_start(&test);
    check_int_eq(flowie_cluster_lifecycle_dispatch_test_wait(&test, 1, 2), TURBO_OK);
    check_int_eq(test.apply_count, 1);
    check_int_eq(test.recover_count, 1);
    flowie_cluster_lifecycle_dispatch_test_stop(&test);
    flowie_cluster_lifecycle_dispatch_test_cleanup(&test);
  }

  it("keeps accepted apply storage alive until its late completion drains") {
    flowie_cluster_lifecycle_dispatch_test_t test;
    flowie_cluster_lifecycle_apply_complete_fn complete;
    void *complete_ctx;
    flowie_cluster_lifecycle_dispatch_test_init(&test);
    test.delay_completion = 1;
    flowie_cluster_lifecycle_dispatch_test_start(&test);
    check_int_eq(flowie_cluster_lifecycle_dispatch_test_wait(&test, 1, 0), TURBO_OK);
    check_int_eq(flowie_cluster_lifecycle_dispatcher_close(test.dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_lifecycle_dispatcher_drain(test.dispatcher, 0u), TURBO_EBUSY);
    check_int_eq(flowie_cluster_lifecycle_dispatcher_destroy(test.dispatcher), TURBO_EBUSY);
    turbo_mutex_lock(&test.mutex);
    complete = test.delayed_complete;
    complete_ctx = test.delayed_complete_ctx;
    turbo_mutex_unlock(&test.mutex);
    check_not_null(complete);
    complete(complete_ctx, TURBO_ECANCELED);
    check_int_eq(flowie_cluster_lifecycle_dispatcher_drain(test.dispatcher,
                                                           FLOWIE_CLUSTER_LIFECYCLE_TEST_WAIT_NS),
                 TURBO_OK);
    check_int_eq(flowie_cluster_lifecycle_dispatcher_destroy(test.dispatcher), TURBO_OK);
    test.dispatcher = NULL;
    flowie_cluster_lifecycle_dispatch_test_cleanup(&test);
  }
}
