#include "flowie_cluster_delivery_dispatch_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <string.h>

static const uint64_t FLOWIE_CLUSTER_DELIVERY_TEST_WAIT_NS = UINT64_C(2000000000);
static const uint64_t FLOWIE_CLUSTER_DELIVERY_TEST_POLL_NS = UINT64_C(1000000);
static const uint64_t FLOWIE_CLUSTER_DELIVERY_TEST_RETRY_NS = UINT64_C(1000000);
static const uint64_t FLOWIE_CLUSTER_DELIVERY_TEST_REPLY_NS = UINT64_C(5000000);

static void flowie_cluster_delivery_test_boot(uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                              uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot[index] = (uint8_t)(seed + index);
}

static flowie_cluster_pgsql_outbox_event_t
flowie_cluster_delivery_test_row(const uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  static const uint8_t packet[] = {0x30u, 0x06u, 0x00u, 0x01u,
                                   'a',   0x00u, 'o',   'k'};
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  event.command_id[0] = 0x42u;
  event.shard_id = 7u;
  event.event_owner_epoch = 11u;
  event.fact_revision = 13u;
  event.event_type = FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE;
  event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
  event.record_key = tstr_new_len("device-a", sizeof("device-a") - 1u);
  if (flowie_cluster_delivery_action_encode(
          tstr_v_from_cstr("edge-a"), edge_boot, 71u, 9u, 17u, 5u, 3u,
          FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){packet, sizeof(packet)}, 1024u,
          &event.payload) != TURBO_OK)
    tstr_freep(&event.record_key);
  return event;
}

static flowie_cluster_peer_frame_t
flowie_cluster_delivery_test_reply(const flowie_cluster_peer_frame_t *command, int status,
                                   tstr_t payload) {
  flowie_cluster_peer_frame_t reply = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  reply.kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply.operation = FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK;
  reply.shard_id = command->shard_id;
  reply.status = status;
  reply.owner_epoch = command->owner_epoch;
  reply.connection_id = command->connection_id;
  reply.connection_generation = command->connection_generation;
  reply.cluster_id = command->cluster_id;
  reply.listener_id = command->listener_id;
  reply.source_node_id = command->target_node_id;
  reply.target_node_id = command->source_node_id;
  reply.payload = payload ? tstr_to_v(payload) : tstr_v_from_buf(NULL, 0u);
  memcpy(reply.source_boot_id, command->target_boot_id, sizeof(reply.source_boot_id));
  memcpy(reply.target_boot_id, command->source_boot_id, sizeof(reply.target_boot_id));
  memcpy(reply.correlation_id, command->correlation_id, sizeof(reply.correlation_id));
  return reply;
}

typedef struct flowie_cluster_delivery_dispatch_test_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  flowie_cluster_owner_token_t owner;
  uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_delivery_dispatcher_t *dispatcher;
  int ready;
  int event_delivered;
  int fetch_count;
  int send_count;
  int settle_count;
  int recover_count;
  int reply_failures;
  int reply_suppressions;
  int settle_failures;
} flowie_cluster_delivery_dispatch_test_t;

static void flowie_cluster_delivery_dispatch_test_init(
    flowie_cluster_delivery_dispatch_test_t *test) {
  uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  memset(test, 0, sizeof(*test));
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
  flowie_cluster_delivery_test_boot(owner_boot, 1u);
  flowie_cluster_delivery_test_boot(test->edge_boot, 33u);
  (void)flowie_cluster_owner_token_init(&test->owner, 7u, 12u, "owner-a", 7u, owner_boot);
}

static void flowie_cluster_delivery_dispatch_test_cleanup(
    flowie_cluster_delivery_dispatch_test_t *test) {
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

static int flowie_cluster_delivery_dispatch_test_owner(void *ctx, uint32_t shard_id,
                                                       flowie_cluster_owner_token_t *out) {
  flowie_cluster_delivery_dispatch_test_t *test =
      (flowie_cluster_delivery_dispatch_test_t *)ctx;
  if (!out || shard_id != test->owner.shard_id) return TURBO_EBUSY;
  *out = test->owner;
  return TURBO_OK;
}

static int flowie_cluster_delivery_dispatch_test_fetch(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_delivery_dispatch_test_t *test =
      (flowie_cluster_delivery_dispatch_test_t *)ctx;
  if (!current_owner || !out) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  while (!test->ready)
    turbo_cond_wait(&test->changed, &test->mutex);
  ++test->fetch_count;
  if (test->event_delivered) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOENT;
  }
  test->event_delivered = 1;
  turbo_mutex_unlock(&test->mutex);
  *out = flowie_cluster_delivery_test_row(test->edge_boot);
  return out->record_key && out->payload ? TURBO_OK : TURBO_ENOMEM;
}

static int flowie_cluster_delivery_dispatch_test_settle(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_delivery_dispatch_test_t *test =
      (flowie_cluster_delivery_dispatch_test_t *)ctx;
  int rc = TURBO_OK;
  if (!current_owner || !event) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->settle_count;
  if (test->settle_count <= test->settle_failures) rc = TURBO_EIO;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_delivery_dispatch_test_recover(void *ctx) {
  flowie_cluster_delivery_dispatch_test_t *test =
      (flowie_cluster_delivery_dispatch_test_t *)ctx;
  turbo_mutex_lock(&test->mutex);
  ++test->recover_count;
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static int flowie_cluster_delivery_dispatch_test_send(
    void *ctx, const flowie_cluster_peer_frame_t *frame,
    flowie_cluster_peer_send_complete_fn complete, void *complete_ctx) {
  flowie_cluster_delivery_dispatch_test_t *test =
      (flowie_cluster_delivery_dispatch_test_t *)ctx;
  flowie_cluster_peer_edge_action_t action = FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT;
  flowie_cluster_peer_frame_t reply;
  tstr_t payload = NULL;
  int attempt;
  int status = TURBO_OK;
  int suppress = 0;
  turbo_mutex_lock(&test->mutex);
  attempt = ++test->send_count;
  if (attempt <= test->reply_failures) status = TURBO_EBUSY;
  if (attempt <= test->reply_suppressions) suppress = 1;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  complete(complete_ctx, TURBO_OK);
  if (suppress) return TURBO_OK;
  if (status == TURBO_OK) {
    if (flowie_cluster_peer_edge_action_decode(frame->payload.data, frame->payload.len, 1024u,
                                               &action) != TURBO_OK)
      return TURBO_EPROTO;
    if (flowie_cluster_peer_edge_action_ack_encode(action.action_sequence, &payload) != TURBO_OK)
      return TURBO_ENOMEM;
  }
  reply = flowie_cluster_delivery_test_reply(frame, status, payload);
  (void)flowie_cluster_delivery_dispatcher_reply(test->dispatcher, &reply);
  tstr_free(payload);
  return TURBO_OK;
}

static flowie_cluster_delivery_dispatcher_config_t
flowie_cluster_delivery_dispatch_test_config(flowie_cluster_delivery_dispatch_test_t *test) {
  flowie_cluster_delivery_dispatcher_config_t config =
      FLOWIE_CLUSTER_DELIVERY_DISPATCHER_CONFIG_INIT;
  config.shard_id = 7u;
  config.max_payload_size = 1024u;
  config.poll_interval_ns = FLOWIE_CLUSTER_DELIVERY_TEST_POLL_NS;
  config.retry_interval_ns = FLOWIE_CLUSTER_DELIVERY_TEST_RETRY_NS;
  config.reply_timeout_ns = FLOWIE_CLUSTER_DELIVERY_TEST_REPLY_NS;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.listener_id = tstr_v_from_cstr("listener-a");
  config.resolve = flowie_cluster_delivery_dispatch_test_owner;
  config.resolve_ctx = test;
  config.fetch = flowie_cluster_delivery_dispatch_test_fetch;
  config.settle = flowie_cluster_delivery_dispatch_test_settle;
  config.recover = flowie_cluster_delivery_dispatch_test_recover;
  config.source_ctx = test;
  config.send = flowie_cluster_delivery_dispatch_test_send;
  config.send_ctx = test;
  return config;
}

static int flowie_cluster_delivery_dispatch_test_wait(
    flowie_cluster_delivery_dispatch_test_t *test, int expected_settles, int expected_sends) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = start_ns + FLOWIE_CLUSTER_DELIVERY_TEST_WAIT_NS;
  int ready;
  turbo_mutex_lock(&test->mutex);
  while (test->settle_count < expected_settles || test->send_count < expected_sends) {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) break;
    (void)turbo_cond_timedwait(&test->changed, &test->mutex, deadline_ns - now_ns);
  }
  ready = test->settle_count >= expected_settles && test->send_count >= expected_sends;
  turbo_mutex_unlock(&test->mutex);
  return ready ? TURBO_OK : TURBO_ETIMEDOUT;
}

static void flowie_cluster_delivery_dispatch_test_start(
    flowie_cluster_delivery_dispatch_test_t *test,
    flowie_cluster_delivery_dispatcher_config_t *config) {
  check_int_eq(flowie_cluster_delivery_dispatcher_create(config, &test->dispatcher), TURBO_OK);
  check_not_null(test->dispatcher);
  turbo_mutex_lock(&test->mutex);
  test->ready = 1;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
}

static void flowie_cluster_delivery_dispatch_test_stop(
    flowie_cluster_delivery_dispatch_test_t *test) {
  check_int_eq(flowie_cluster_delivery_dispatcher_close(test->dispatcher), TURBO_OK);
  check_int_eq(flowie_cluster_delivery_dispatcher_drain(test->dispatcher,
                                                        FLOWIE_CLUSTER_DELIVERY_TEST_WAIT_NS),
               TURBO_OK);
  check_int_eq(flowie_cluster_delivery_dispatcher_destroy(test->dispatcher), TURBO_OK);
  test->dispatcher = NULL;
}

spec("flowie cluster delivery outbox dispatch") {
  it("maps one durable TFDA row to an exact copied EDGE_ACTION command") {
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_outbox_event_t event;
    flowie_cluster_delivery_command_t command = FLOWIE_CLUSTER_DELIVERY_COMMAND_INIT;
    flowie_cluster_peer_edge_action_t action = FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT;
    flowie_cluster_delivery_test_boot(owner_boot, 1u);
    flowie_cluster_delivery_test_boot(edge_boot, 33u);
    event = flowie_cluster_delivery_test_row(edge_boot);
    check_int_eq(flowie_cluster_owner_token_init(&owner, 7u, 12u, "owner-a", 7u, owner_boot),
                 TURBO_OK);
    check_int_eq(flowie_cluster_delivery_dispatch_prepare(
                     &event, &owner, tstr_v_from_cstr("cluster-a"),
                     tstr_v_from_cstr("listener-a"), 1024u, &command),
                 TURBO_OK);
    check_int_eq(command.frame.kind, FLOWIE_CLUSTER_PEER_FRAME_COMMAND);
    check_int_eq(command.frame.operation, FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION);
    check_uint_eq(command.frame.owner_epoch, 12u);
    check_uint_eq(command.frame.connection_id, 71u);
    check_uint_eq(command.frame.connection_generation, 9u);
    check_mem_eq(command.frame.target_boot_id, edge_boot, sizeof(edge_boot));
    check_mem_eq(command.frame.correlation_id, event.command_id, sizeof(event.command_id));
    event.payload[FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE] = 'X';
    check_mem_eq(command.frame.target_node_id.data, "edge-a", command.frame.target_node_id.len);
    check_int_eq(flowie_cluster_peer_edge_action_decode(
                     command.frame.payload.data, command.frame.payload.len, 1024u, &action),
                 TURBO_OK);
    check_uint_eq(action.action_sequence, 3u);
    flowie_cluster_delivery_command_cleanup(&command);
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }

  it("settles only the exact reverse-route TFEK sequence") {
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_outbox_event_t event;
    flowie_cluster_delivery_command_t command = FLOWIE_CLUSTER_DELIVERY_COMMAND_INIT;
    flowie_cluster_peer_frame_t reply;
    tstr_t ack = NULL;
    flowie_cluster_delivery_test_boot(owner_boot, 1u);
    flowie_cluster_delivery_test_boot(edge_boot, 33u);
    event = flowie_cluster_delivery_test_row(edge_boot);
    check_int_eq(flowie_cluster_owner_token_init(&owner, 7u, 12u, "owner-a", 7u, owner_boot),
                 TURBO_OK);
    check_int_eq(flowie_cluster_delivery_dispatch_prepare(
                     &event, &owner, tstr_v_from_cstr("cluster-a"),
                     tstr_v_from_cstr("listener-a"), 1024u, &command),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_edge_action_ack_encode(3u, &ack), TURBO_OK);
    reply = flowie_cluster_delivery_test_reply(&command.frame, TURBO_OK, ack);
    check_int_eq(flowie_cluster_delivery_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_OK);
    tstr_freep(&ack);
    check_int_eq(flowie_cluster_peer_edge_action_ack_encode(4u, &ack), TURBO_OK);
    reply.payload = tstr_to_v(ack);
    check_int_eq(flowie_cluster_delivery_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_EPROTO);
    reply.payload = tstr_v_from_buf(NULL, 0u);
    reply.status = TURBO_EBUSY;
    check_int_eq(flowie_cluster_delivery_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_EBUSY);
    reply.status = TURBO_OK;
    reply.payload = tstr_to_v(ack);
    ++reply.connection_generation;
    check_int_eq(flowie_cluster_delivery_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_EPROTO);
    tstr_free(ack);
    flowie_cluster_delivery_command_cleanup(&command);
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }

  it("replays one immutable action after a missing or rejected acknowledgement") {
    flowie_cluster_delivery_dispatch_test_t test;
    flowie_cluster_delivery_dispatcher_config_t config;
    flowie_cluster_delivery_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_DELIVERY_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_delivery_dispatch_test_init(&test);
    test.reply_suppressions = 1;
    test.reply_failures = 2;
    config = flowie_cluster_delivery_dispatch_test_config(&test);
    flowie_cluster_delivery_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_delivery_dispatch_test_wait(&test, 1, 3), TURBO_OK);
    check_int_eq(flowie_cluster_delivery_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.send_attempts, 3u);
    check_uint_eq(snapshot.reply_timeouts, 1u);
    check_uint_eq(snapshot.settled_events, 1u);
    flowie_cluster_delivery_dispatch_test_stop(&test);
    flowie_cluster_delivery_dispatch_test_cleanup(&test);
  }

  it("retries uncertain PostgreSQL settlement without resending the acknowledged action") {
    flowie_cluster_delivery_dispatch_test_t test;
    flowie_cluster_delivery_dispatcher_config_t config;
    flowie_cluster_delivery_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_DELIVERY_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_delivery_dispatch_test_init(&test);
    test.settle_failures = 1;
    config = flowie_cluster_delivery_dispatch_test_config(&test);
    flowie_cluster_delivery_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_delivery_dispatch_test_wait(&test, 2, 1), TURBO_OK);
    check_int_eq(flowie_cluster_delivery_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.send_attempts, 1u);
    check_uint_eq(snapshot.settled_events, 1u);
    check_int_eq(test.recover_count, 1);
    flowie_cluster_delivery_dispatch_test_stop(&test);
    flowie_cluster_delivery_dispatch_test_cleanup(&test);
  }
}
