#include "flowie_cluster_broadcast_dispatch_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <string.h>

static const uint64_t FLOWIE_CLUSTER_BROADCAST_TEST_WAIT_NS = 2000000000u;
static const uint64_t FLOWIE_CLUSTER_BROADCAST_TEST_INTERVAL_NS = 1000000u;

typedef struct flowie_cluster_broadcast_dispatch_test_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  flowie_cluster_owner_token_t owner;
  flowie_cluster_broadcast_dispatcher_t *dispatcher;
  tstr_t first_publish;
  int event_delivered;
  int fetch_count;
  int publish_count;
  int ack_count_calls;
  int settle_count;
  int recover_count;
  int publish_failures;
  int ack_failures;
  int complete_after_publish_count;
} flowie_cluster_broadcast_dispatch_test_t;

static int flowie_cluster_broadcast_test_publish_payload(tstr_t *out) {
  static const uint8_t client_id[] = "publisher-a";
  static const uint8_t publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                    0x00u, 0x07u, 0x00u, 'o',   'k'};
  uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
  edge_boot[0] = 0x11u;
  return flowie_cluster_publish_event_encode(
      FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 3u, 4u, 5u, 6u, 1000u,
      tstr_v_from_cstr("edge-a"), edge_boot,
      (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
      (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, out);
}

static flowie_cluster_pgsql_outbox_event_t flowie_cluster_broadcast_test_event(void) {
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  event.command_id[0] = 0x51u;
  event.event_index = 2u;
  event.shard_id = 1u;
  event.event_owner_epoch = 11u;
  event.fact_revision = 0u;
  event.event_type = FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE;
  event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
  event.record_key = tstr_dup("publisher-a");
  event.attempt_count = 1u;
  if (flowie_cluster_broadcast_test_publish_payload(&event.payload) != TURBO_OK) {
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }
  return event;
}

static void flowie_cluster_broadcast_test_init(flowie_cluster_broadcast_dispatch_test_t *test) {
  uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
  memset(test, 0, sizeof(*test));
  boot[0] = 1u;
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
  check_int_eq(flowie_cluster_owner_token_init(&test->owner, 1u, 20u, "owner-a", 7u, boot),
               TURBO_OK);
}

static void flowie_cluster_broadcast_test_cleanup(
    flowie_cluster_broadcast_dispatch_test_t *test) {
  tstr_free(test->first_publish);
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

static int flowie_cluster_broadcast_test_owner(void *ctx, uint32_t shard_id,
                                               flowie_cluster_owner_token_t *out) {
  flowie_cluster_broadcast_dispatch_test_t *test =
      (flowie_cluster_broadcast_dispatch_test_t *)ctx;
  if (!out || shard_id != test->owner.shard_id) return TURBO_EBUSY;
  *out = test->owner;
  return TURBO_OK;
}

static int flowie_cluster_broadcast_test_fetch(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_broadcast_dispatch_test_t *test =
      (flowie_cluster_broadcast_dispatch_test_t *)ctx;
  if (!current_owner || !out) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->fetch_count;
  if (test->event_delivered) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOENT;
  }
  test->event_delivered = 1;
  turbo_mutex_unlock(&test->mutex);
  *out = flowie_cluster_broadcast_test_event();
  return out->record_key && out->payload ? TURBO_OK : TURBO_ENOMEM;
}

static int flowie_cluster_broadcast_test_publish(void *ctx, const void *payload,
                                                 size_t payload_size) {
  flowie_cluster_broadcast_dispatch_test_t *test =
      (flowie_cluster_broadcast_dispatch_test_t *)ctx;
  int rc = TURBO_OK;
  turbo_mutex_lock(&test->mutex);
  ++test->publish_count;
  if (!test->first_publish) {
    test->first_publish = tstr_new_len(payload, payload_size);
    if (!test->first_publish) rc = TURBO_ENOMEM;
  } else if (tstr_len(test->first_publish) != payload_size ||
             memcmp(test->first_publish, payload, payload_size) != 0) {
    rc = TURBO_EPROTO;
  }
  if (rc == TURBO_OK && test->publish_count <= test->publish_failures) rc = TURBO_EIO;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_broadcast_test_ack_count(
    void *ctx, const flowie_cluster_pgsql_event_dedupe_t *source, size_t *out_count) {
  flowie_cluster_broadcast_dispatch_test_t *test =
      (flowie_cluster_broadcast_dispatch_test_t *)ctx;
  uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
  int rc;
  if (!source || !out_count || source->target_session_id != 0u ||
      source->source_command_id[0] != 0x51u || source->event_index != 2u ||
      source->source_shard_id != 1u || source->source_owner_epoch != 11u)
    return TURBO_EPROTO;
  turbo_mutex_lock(&test->mutex);
  ++test->ack_count_calls;
  if (!test->first_publish) rc = TURBO_EPROTO;
  else
    rc = flowie_cluster_broadcast_event_digest(test->first_publish,
                                               tstr_len(test->first_publish), 2048u, digest);
  if (rc == TURBO_OK && memcmp(digest, source->event_digest, sizeof(digest)) != 0)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK && test->ack_count_calls <= test->ack_failures) rc = TURBO_EIO;
  if (rc == TURBO_OK)
    *out_count = test->publish_count >= test->complete_after_publish_count ? 3u : 0u;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_broadcast_test_settle(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_broadcast_dispatch_test_t *test =
      (flowie_cluster_broadcast_dispatch_test_t *)ctx;
  if (!current_owner || !event || event->command_id[0] != 0x51u) return TURBO_EPROTO;
  turbo_mutex_lock(&test->mutex);
  ++test->settle_count;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static int flowie_cluster_broadcast_test_recover(void *ctx) {
  flowie_cluster_broadcast_dispatch_test_t *test =
      (flowie_cluster_broadcast_dispatch_test_t *)ctx;
  turbo_mutex_lock(&test->mutex);
  ++test->recover_count;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static flowie_cluster_broadcast_dispatcher_config_t flowie_cluster_broadcast_test_config(
    flowie_cluster_broadcast_dispatch_test_t *test) {
  flowie_cluster_broadcast_dispatcher_config_t config =
      FLOWIE_CLUSTER_BROADCAST_DISPATCHER_CONFIG_INIT;
  config.shard_id = 1u;
  config.shard_count = 3u;
  config.max_payload_size = 2048u;
  config.poll_interval_ns = FLOWIE_CLUSTER_BROADCAST_TEST_INTERVAL_NS;
  config.ack_poll_interval_ns = FLOWIE_CLUSTER_BROADCAST_TEST_INTERVAL_NS;
  config.retry_interval_ns = FLOWIE_CLUSTER_BROADCAST_TEST_INTERVAL_NS;
  config.republish_interval_ns = FLOWIE_CLUSTER_BROADCAST_TEST_INTERVAL_NS;
  config.resolve = flowie_cluster_broadcast_test_owner;
  config.resolve_ctx = test;
  config.fetch = flowie_cluster_broadcast_test_fetch;
  config.settle = flowie_cluster_broadcast_test_settle;
  config.ack_count = flowie_cluster_broadcast_test_ack_count;
  config.recover = flowie_cluster_broadcast_test_recover;
  config.source_ctx = test;
  config.publish = flowie_cluster_broadcast_test_publish;
  config.publish_ctx = test;
  return config;
}

static int flowie_cluster_broadcast_test_wait(flowie_cluster_broadcast_dispatch_test_t *test,
                                              int publish_count, int settle_count) {
  uint64_t deadline_ns = turbo_hrtime() + FLOWIE_CLUSTER_BROADCAST_TEST_WAIT_NS;
  int ready;
  turbo_mutex_lock(&test->mutex);
  while (test->publish_count < publish_count || test->settle_count < settle_count) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&test->changed, &test->mutex, deadline_ns - now_ns);
  }
  ready = test->publish_count >= publish_count && test->settle_count >= settle_count;
  turbo_mutex_unlock(&test->mutex);
  return ready ? TURBO_OK : TURBO_ETIMEDOUT;
}

static void flowie_cluster_broadcast_test_start(flowie_cluster_broadcast_dispatch_test_t *test) {
  flowie_cluster_broadcast_dispatcher_config_t config =
      flowie_cluster_broadcast_test_config(test);
  check_int_eq(flowie_cluster_broadcast_dispatcher_create(&config, &test->dispatcher), TURBO_OK);
  check_not_null(test->dispatcher);
}

static void flowie_cluster_broadcast_test_stop(flowie_cluster_broadcast_dispatch_test_t *test) {
  check_int_eq(flowie_cluster_broadcast_dispatcher_close(test->dispatcher), TURBO_OK);
  check_int_eq(flowie_cluster_broadcast_dispatcher_drain(
                   test->dispatcher, FLOWIE_CLUSTER_BROADCAST_TEST_WAIT_NS),
               TURBO_OK);
  check_int_eq(flowie_cluster_broadcast_dispatcher_destroy(test->dispatcher), TURBO_OK);
  test->dispatcher = NULL;
}

spec("flowie cluster PostgreSQL to broadcast dispatch") {
  it("settles an uncertain XADD only after all PostgreSQL shard ACKs exist") {
    flowie_cluster_broadcast_dispatch_test_t test;
    flowie_cluster_broadcast_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_broadcast_test_init(&test);
    test.publish_failures = 1;
    test.complete_after_publish_count = 1;
    flowie_cluster_broadcast_test_start(&test);
    check_int_eq(flowie_cluster_broadcast_test_wait(&test, 1, 1), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_dispatcher_snapshot(test.dispatcher, &snapshot),
                 TURBO_OK);
    check_uint_eq(snapshot.fetched_events, 1u);
    check_uint_eq(snapshot.publish_attempts, 1u);
    check_uint_eq(snapshot.observed_shard_acks, 3u);
    check_uint_eq(snapshot.settled_events, 1u);
    flowie_cluster_broadcast_test_stop(&test);
    flowie_cluster_broadcast_test_cleanup(&test);
  }

  it("re-XADDs the exact immutable TFBE until target shards finish") {
    flowie_cluster_broadcast_dispatch_test_t test;
    flowie_cluster_broadcast_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_broadcast_test_init(&test);
    test.complete_after_publish_count = 2;
    flowie_cluster_broadcast_test_start(&test);
    check_int_eq(flowie_cluster_broadcast_test_wait(&test, 2, 1), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_dispatcher_snapshot(test.dispatcher, &snapshot),
                 TURBO_OK);
    check_uint_eq(snapshot.publish_attempts, 2u);
    check(snapshot.ack_queries >= 2u);
    check_uint_eq(snapshot.settled_events, 1u);
    flowie_cluster_broadcast_test_stop(&test);
    flowie_cluster_broadcast_test_cleanup(&test);
  }

  it("reopens PostgreSQL after an ACK query I/O failure") {
    flowie_cluster_broadcast_dispatch_test_t test;
    flowie_cluster_broadcast_test_init(&test);
    test.ack_failures = 1;
    test.complete_after_publish_count = 1;
    flowie_cluster_broadcast_test_start(&test);
    check_int_eq(flowie_cluster_broadcast_test_wait(&test, 1, 1), TURBO_OK);
    check_int_eq(test.recover_count, 1);
    check(test.ack_count_calls >= 2);
    flowie_cluster_broadcast_test_stop(&test);
    flowie_cluster_broadcast_test_cleanup(&test);
  }
}
