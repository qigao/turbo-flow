#include "flowie_cluster_takeover_dispatch_internal.h"

#include "flowie_cluster_peer_wire_internal.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <string.h>

static const uint64_t FLOWIE_CLUSTER_TAKEOVER_TEST_WAIT_NS = 2000000000u;
static const uint64_t FLOWIE_CLUSTER_TAKEOVER_TEST_POLL_NS = 1000000u;
static const uint64_t FLOWIE_CLUSTER_TAKEOVER_TEST_RETRY_NS = 1000000u;
static const uint64_t FLOWIE_CLUSTER_TAKEOVER_TEST_REPLY_NS = 5000000u;

static void flowie_cluster_takeover_test_boot(uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                              uint8_t seed) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot[index] = (uint8_t)(seed + index);
}

static tstr_t
flowie_cluster_takeover_test_event(const uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                   tstr_v edge_node) {
  size_t total = FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE + edge_node.len;
  tstr_t payload = tstr_new_len(NULL, total);
  uint8_t *bytes = (uint8_t *)payload;
  if (!payload) return NULL;
  memcpy(bytes, "TFTE", 4u);
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  bytes[12] = FLOWIE_MQTT_VERSION_5;
  memset(bytes + 13u, 0, 3u);
  flowie_cluster_peer_wire_write_u64(bytes + 16u, 71u);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, 9u);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, 101u);
  flowie_cluster_peer_wire_write_u64(bytes + 40u, 3u);
  memcpy(bytes + 48u, edge_boot, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  flowie_cluster_peer_wire_write_u16(bytes + 64u, (uint16_t)edge_node.len);
  flowie_cluster_peer_wire_write_u16(bytes + 66u, 0u);
  memcpy(bytes + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, edge_node.data, edge_node.len);
  return payload;
}

static flowie_cluster_pgsql_outbox_event_t flowie_cluster_takeover_test_row(tstr_t payload) {
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  size_t index;
  for (index = 0u; index < sizeof(event.command_id); ++index)
    event.command_id[index] = (uint8_t)(0x30u + index);
  event.shard_id = 7u;
  event.event_owner_epoch = 4u;
  event.fact_revision = 8u;
  event.event_type = FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER;
  event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
  event.record_key = tstr_dup("device-a");
  event.payload = payload;
  event.attempt_count = 1u;
  return event;
}

static flowie_cluster_peer_frame_t
flowie_cluster_takeover_test_reply(const flowie_cluster_takeover_command_t *command, int status,
                                   tstr_t payload) {
  flowie_cluster_peer_frame_t reply = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  reply.kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
  reply.shard_id = command->frame.shard_id;
  reply.status = status;
  reply.owner_epoch = command->frame.owner_epoch;
  reply.connection_id = command->frame.connection_id;
  reply.connection_generation = command->frame.connection_generation;
  reply.cluster_id = command->frame.cluster_id;
  reply.listener_id = command->frame.listener_id;
  reply.source_node_id = command->frame.target_node_id;
  reply.target_node_id = command->frame.source_node_id;
  reply.payload = payload ? tstr_to_v(payload) : tstr_v_from_buf(NULL, 0u);
  memcpy(reply.source_boot_id, command->frame.target_boot_id, sizeof(reply.source_boot_id));
  memcpy(reply.target_boot_id, command->frame.source_boot_id, sizeof(reply.target_boot_id));
  memcpy(reply.correlation_id, command->frame.correlation_id, sizeof(reply.correlation_id));
  return reply;
}

typedef struct flowie_cluster_takeover_dispatch_test_s {
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  flowie_cluster_owner_token_t owner;
  uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_takeover_dispatcher_t *dispatcher;
  flowie_cluster_peer_send_complete_fn delayed_complete;
  void *delayed_complete_ctx;
  int ready;
  int event_delivered;
  int fetch_count;
  int settle_count;
  int recover_count;
  int send_count;
  int send_failures;
  int reply_failures;
  int reply_suppressions;
  int settle_failures;
  int delay_completion;
} flowie_cluster_takeover_dispatch_test_t;

static void
flowie_cluster_takeover_dispatch_test_init(flowie_cluster_takeover_dispatch_test_t *test) {
  uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  memset(test, 0, sizeof(*test));
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
  flowie_cluster_takeover_test_boot(owner_boot, 1u);
  flowie_cluster_takeover_test_boot(test->edge_boot, 33u);
  (void)flowie_cluster_owner_token_init(&test->owner, 7u, 12u, "owner-new", 9u, owner_boot);
}

static void
flowie_cluster_takeover_dispatch_test_cleanup(flowie_cluster_takeover_dispatch_test_t *test) {
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

static int flowie_cluster_takeover_dispatch_test_owner(void *ctx, uint32_t shard_id,
                                                       flowie_cluster_owner_token_t *out) {
  flowie_cluster_takeover_dispatch_test_t *test = (flowie_cluster_takeover_dispatch_test_t *)ctx;
  if (!out || shard_id != test->owner.shard_id) return TURBO_EBUSY;
  *out = test->owner;
  return TURBO_OK;
}

static int
flowie_cluster_takeover_dispatch_test_fetch(void *ctx,
                                            const flowie_cluster_owner_token_t *current_owner,
                                            flowie_cluster_pgsql_outbox_event_t *out) {
  flowie_cluster_takeover_dispatch_test_t *test = (flowie_cluster_takeover_dispatch_test_t *)ctx;
  tstr_t payload;
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
  payload = flowie_cluster_takeover_test_event(test->edge_boot, tstr_v_from_cstr("edge-old"));
  if (!payload) return TURBO_ENOMEM;
  *out = flowie_cluster_takeover_test_row(payload);
  if (!out->record_key) {
    flowie_cluster_pgsql_outbox_event_cleanup(out);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int
flowie_cluster_takeover_dispatch_test_settle(void *ctx,
                                             const flowie_cluster_owner_token_t *current_owner,
                                             const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_takeover_dispatch_test_t *test = (flowie_cluster_takeover_dispatch_test_t *)ctx;
  if (!current_owner || !event) return TURBO_EINVAL;
  turbo_mutex_lock(&test->mutex);
  ++test->settle_count;
  turbo_cond_broadcast(&test->changed);
  if (test->settle_count <= test->settle_failures) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_EIO;
  }
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static int flowie_cluster_takeover_dispatch_test_recover(void *ctx) {
  flowie_cluster_takeover_dispatch_test_t *test = (flowie_cluster_takeover_dispatch_test_t *)ctx;
  turbo_mutex_lock(&test->mutex);
  ++test->recover_count;
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static flowie_cluster_peer_frame_t
flowie_cluster_takeover_dispatch_test_reply(const flowie_cluster_peer_frame_t *command, int status,
                                            tstr_t payload) {
  flowie_cluster_peer_frame_t reply = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  reply.kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
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

static int flowie_cluster_takeover_dispatch_test_send(void *ctx,
                                                      const flowie_cluster_peer_frame_t *frame,
                                                      flowie_cluster_peer_send_complete_fn complete,
                                                      void *complete_ctx) {
  flowie_cluster_takeover_dispatch_test_t *test = (flowie_cluster_takeover_dispatch_test_t *)ctx;
  flowie_cluster_peer_frame_t reply;
  tstr_t payload = NULL;
  int attempt;
  int reply_status = TURBO_OK;
  int suppress_reply = 0;
  turbo_mutex_lock(&test->mutex);
  attempt = ++test->send_count;
  if (test->delay_completion) {
    test->delayed_complete = complete;
    test->delayed_complete_ctx = complete_ctx;
    turbo_cond_broadcast(&test->changed);
    turbo_mutex_unlock(&test->mutex);
    return TURBO_OK;
  }
  if (attempt <= test->send_failures) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOSPC;
  }
  if (attempt - test->send_failures <= test->reply_failures) reply_status = TURBO_EBUSY;
  if (attempt - test->send_failures <= test->reply_suppressions) suppress_reply = 1;
  turbo_mutex_unlock(&test->mutex);
  complete(complete_ctx, TURBO_OK);
  if (suppress_reply) return TURBO_OK;
  if (reply_status == TURBO_OK &&
      flowie_cluster_peer_mqtt_reply_encode(FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){NULL, 0u},
                                            0, (turbo_flow_protocol_settlement_point_t)0, 1024u,
                                            &payload) != TURBO_OK)
    return TURBO_ENOMEM;
  reply = flowie_cluster_takeover_dispatch_test_reply(frame, reply_status, payload);
  (void)flowie_cluster_takeover_dispatcher_reply(test->dispatcher, &reply);
  tstr_free(payload);
  return TURBO_OK;
}

static flowie_cluster_takeover_dispatcher_config_t
flowie_cluster_takeover_dispatch_test_config(flowie_cluster_takeover_dispatch_test_t *test) {
  flowie_cluster_takeover_dispatcher_config_t config =
      FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_CONFIG_INIT;
  config.shard_id = 7u;
  config.max_payload_size = 1024u;
  config.poll_interval_ns = FLOWIE_CLUSTER_TAKEOVER_TEST_POLL_NS;
  config.retry_interval_ns = FLOWIE_CLUSTER_TAKEOVER_TEST_RETRY_NS;
  config.reply_timeout_ns = FLOWIE_CLUSTER_TAKEOVER_TEST_REPLY_NS;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.listener_id = tstr_v_from_cstr("listener-a");
  config.resolve = flowie_cluster_takeover_dispatch_test_owner;
  config.resolve_ctx = test;
  config.fetch = flowie_cluster_takeover_dispatch_test_fetch;
  config.settle = flowie_cluster_takeover_dispatch_test_settle;
  config.recover = flowie_cluster_takeover_dispatch_test_recover;
  config.source_ctx = test;
  config.send = flowie_cluster_takeover_dispatch_test_send;
  config.send_ctx = test;
  return config;
}

static int flowie_cluster_takeover_dispatch_test_wait(flowie_cluster_takeover_dispatch_test_t *test,
                                                      int expected_settles, int expected_sends) {
  uint64_t start_ns = turbo_hrtime();
  uint64_t deadline_ns = start_ns + FLOWIE_CLUSTER_TAKEOVER_TEST_WAIT_NS;
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

static void
flowie_cluster_takeover_dispatch_test_start(flowie_cluster_takeover_dispatch_test_t *test,
                                            flowie_cluster_takeover_dispatcher_config_t *config) {
  check_int_eq(flowie_cluster_takeover_dispatcher_create(config, &test->dispatcher), TURBO_OK);
  check_not_null(test->dispatcher);
  turbo_mutex_lock(&test->mutex);
  test->ready = 1;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
}

static void
flowie_cluster_takeover_dispatch_test_stop(flowie_cluster_takeover_dispatch_test_t *test) {
  check_int_eq(flowie_cluster_takeover_dispatcher_close(test->dispatcher), TURBO_OK);
  check_int_eq(flowie_cluster_takeover_dispatcher_drain(test->dispatcher,
                                                        FLOWIE_CLUSTER_TAKEOVER_TEST_WAIT_NS),
               TURBO_OK);
  check_int_eq(flowie_cluster_takeover_dispatcher_destroy(test->dispatcher), TURBO_OK);
  test->dispatcher = NULL;
}

spec("flowie cluster takeover outbox dispatch") {
  it("maps one durable TFTE row to an exact generation TFTC command") {
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_outbox_event_t event;
    flowie_cluster_takeover_command_t command = FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
    flowie_cluster_peer_takeover_close_view_t decoded =
        FLOWIE_CLUSTER_PEER_TAKEOVER_CLOSE_VIEW_INIT;
    tstr_t event_payload;
    flowie_cluster_takeover_test_boot(owner_boot, 1u);
    flowie_cluster_takeover_test_boot(edge_boot, 33u);
    event_payload = flowie_cluster_takeover_test_event(edge_boot, tstr_v_from_cstr("edge-old"));
    check_not_null(event_payload);
    event = flowie_cluster_takeover_test_row(event_payload);
    check_int_eq(flowie_cluster_owner_token_init(&owner, 7u, 12u, "owner-new", 9u, owner_boot),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_takeover_dispatch_prepare(&event, &owner, tstr_v_from_cstr("cluster-a"),
                                                 tstr_v_from_cstr("listener-a"), 1024u, &command),
        TURBO_OK);
    check_int_eq(command.frame.kind, FLOWIE_CLUSTER_PEER_FRAME_COMMAND);
    check_int_eq(command.frame.operation, FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE);
    check_int_eq(command.frame.shard_id, 7u);
    check_int_eq(command.frame.owner_epoch, 12u);
    check_int_eq(command.frame.connection_id, 71u);
    check_int_eq(command.frame.connection_generation, 9u);
    check_mem_eq(command.frame.correlation_id, event.command_id, sizeof(event.command_id));
    check_mem_eq(command.frame.target_boot_id, edge_boot, sizeof(edge_boot));
    check_int_eq(flowie_cluster_peer_takeover_close_decode(
                     command.frame.payload.data, command.frame.payload.len, 1024u, &decoded),
                 TURBO_OK);
    check_int_eq(decoded.mqtt_version, FLOWIE_MQTT_VERSION_5);
    check_mem_eq(decoded.client_id.data, "device-a", decoded.client_id.size);
    event_payload[FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE] = 'X';
    check_mem_eq(command.frame.target_node_id.data, "edge-old", command.frame.target_node_id.len);
    flowie_cluster_takeover_command_cleanup(&command);
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }

  it("settles only an exact successful no-op acknowledgement") {
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_outbox_event_t event;
    flowie_cluster_takeover_command_t command = FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
    flowie_cluster_peer_frame_t reply;
    tstr_t event_payload;
    tstr_t reply_payload = NULL;
    flowie_cluster_takeover_test_boot(owner_boot, 1u);
    flowie_cluster_takeover_test_boot(edge_boot, 33u);
    event_payload = flowie_cluster_takeover_test_event(edge_boot, tstr_v_from_cstr("edge-old"));
    check_not_null(event_payload);
    event = flowie_cluster_takeover_test_row(event_payload);
    check_int_eq(flowie_cluster_owner_token_init(&owner, 7u, 12u, "owner-new", 9u, owner_boot),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_takeover_dispatch_prepare(&event, &owner, tstr_v_from_cstr("cluster-a"),
                                                 tstr_v_from_cstr("listener-a"), 1024u, &command),
        TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_reply_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){NULL, 0u}, 0,
                     (turbo_flow_protocol_settlement_point_t)0, 1024u, &reply_payload),
                 TURBO_OK);
    reply = flowie_cluster_takeover_test_reply(&command, TURBO_OK, reply_payload);
    check_int_eq(flowie_cluster_takeover_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_OK);
    reply.status = TURBO_EBUSY;
    reply.payload = tstr_v_from_buf(NULL, 0u);
    check_int_eq(flowie_cluster_takeover_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_EBUSY);
    reply.status = TURBO_OK;
    reply.payload = tstr_to_v(reply_payload);
    ++reply.connection_generation;
    check_int_eq(flowie_cluster_takeover_dispatch_reply_validate(&command, &reply, 1024u),
                 TURBO_EPROTO);
    tstr_free(reply_payload);
    flowie_cluster_takeover_command_cleanup(&command);
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }

  it("rejects unsupported multi-event correlation and malformed TFTE payloads") {
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_pgsql_outbox_event_t event;
    flowie_cluster_takeover_command_t command = FLOWIE_CLUSTER_TAKEOVER_COMMAND_INIT;
    tstr_t event_payload;
    flowie_cluster_takeover_test_boot(owner_boot, 1u);
    flowie_cluster_takeover_test_boot(edge_boot, 33u);
    event_payload = flowie_cluster_takeover_test_event(edge_boot, tstr_v_from_cstr("edge-old"));
    check_not_null(event_payload);
    event = flowie_cluster_takeover_test_row(event_payload);
    check_int_eq(flowie_cluster_owner_token_init(&owner, 7u, 12u, "owner-new", 9u, owner_boot),
                 TURBO_OK);
    event.event_index = 1u;
    check_int_eq(
        flowie_cluster_takeover_dispatch_prepare(&event, &owner, tstr_v_from_cstr("cluster-a"),
                                                 tstr_v_from_cstr("listener-a"), 1024u, &command),
        TURBO_EINVAL);
    event.event_index = 0u;
    event_payload[66] = 1;
    check_int_eq(
        flowie_cluster_takeover_dispatch_prepare(&event, &owner, tstr_v_from_cstr("cluster-a"),
                                                 tstr_v_from_cstr("listener-a"), 1024u, &command),
        TURBO_EPROTO);
    flowie_cluster_pgsql_outbox_event_cleanup(&event);
  }

  it("retries bounded transport admission before settling the durable event") {
    flowie_cluster_takeover_dispatch_test_t test;
    flowie_cluster_takeover_dispatcher_config_t config;
    flowie_cluster_takeover_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_takeover_dispatch_test_init(&test);
    test.send_failures = 1;
    config = flowie_cluster_takeover_dispatch_test_config(&test);
    flowie_cluster_takeover_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_takeover_dispatch_test_wait(&test, 1, 2), TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_int_eq(snapshot.fetched_events, 1u);
    check_int_eq(snapshot.send_attempts, 2u);
    check_int_eq(snapshot.reply_timeouts, 0u);
    check_int_eq(snapshot.settled_events, 1u);
    check_int_eq(snapshot.source_attempt_count, 1u);
    flowie_cluster_takeover_dispatch_test_stop(&test);
    flowie_cluster_takeover_dispatch_test_cleanup(&test);
  }

  it("keeps the outbox event pending after an exact operational error reply") {
    flowie_cluster_takeover_dispatch_test_t test;
    flowie_cluster_takeover_dispatcher_config_t config;
    flowie_cluster_takeover_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_takeover_dispatch_test_init(&test);
    test.reply_failures = 1;
    config = flowie_cluster_takeover_dispatch_test_config(&test);
    flowie_cluster_takeover_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_takeover_dispatch_test_wait(&test, 1, 2), TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_int_eq(snapshot.send_attempts, 2u);
    check_int_eq(snapshot.settled_events, 1u);
    flowie_cluster_takeover_dispatch_test_stop(&test);
    flowie_cluster_takeover_dispatch_test_cleanup(&test);
  }

  it("retries an acknowledged send when its exact reply times out") {
    flowie_cluster_takeover_dispatch_test_t test;
    flowie_cluster_takeover_dispatcher_config_t config;
    flowie_cluster_takeover_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_takeover_dispatch_test_init(&test);
    test.reply_suppressions = 1;
    config = flowie_cluster_takeover_dispatch_test_config(&test);
    flowie_cluster_takeover_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_takeover_dispatch_test_wait(&test, 1, 2), TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_int_eq(snapshot.send_attempts, 2u);
    check_int_eq(snapshot.reply_timeouts, 1u);
    check_int_eq(snapshot.settled_events, 1u);
    flowie_cluster_takeover_dispatch_test_stop(&test);
    flowie_cluster_takeover_dispatch_test_cleanup(&test);
  }

  it("recovers an uncertain settle without resending the socket close") {
    flowie_cluster_takeover_dispatch_test_t test;
    flowie_cluster_takeover_dispatcher_config_t config;
    flowie_cluster_takeover_dispatcher_snapshot_t snapshot =
        FLOWIE_CLUSTER_TAKEOVER_DISPATCHER_SNAPSHOT_INIT;
    flowie_cluster_takeover_dispatch_test_init(&test);
    test.settle_failures = 1;
    config = flowie_cluster_takeover_dispatch_test_config(&test);
    flowie_cluster_takeover_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_takeover_dispatch_test_wait(&test, 2, 1), TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_snapshot(test.dispatcher, &snapshot), TURBO_OK);
    check_int_eq(snapshot.send_attempts, 1u);
    check_int_eq(snapshot.settled_events, 1u);
    check_int_eq(test.recover_count, 1);
    flowie_cluster_takeover_dispatch_test_stop(&test);
    flowie_cluster_takeover_dispatch_test_cleanup(&test);
  }

  it("drains a late accepted-send completion before destruction") {
    flowie_cluster_takeover_dispatch_test_t test;
    flowie_cluster_takeover_dispatcher_config_t config;
    flowie_cluster_peer_send_complete_fn complete;
    void *complete_ctx;
    flowie_cluster_takeover_dispatch_test_init(&test);
    test.delay_completion = 1;
    config = flowie_cluster_takeover_dispatch_test_config(&test);
    flowie_cluster_takeover_dispatch_test_start(&test, &config);
    check_int_eq(flowie_cluster_takeover_dispatch_test_wait(&test, 0, 1), TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_close(test.dispatcher), TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_drain(test.dispatcher, 0u), TURBO_EBUSY);
    check_int_eq(flowie_cluster_takeover_dispatcher_destroy(test.dispatcher), TURBO_EBUSY);
    turbo_mutex_lock(&test.mutex);
    complete = test.delayed_complete;
    complete_ctx = test.delayed_complete_ctx;
    turbo_mutex_unlock(&test.mutex);
    check_not_null(complete);
    complete(complete_ctx, TURBO_ECANCELED);
    check_int_eq(flowie_cluster_takeover_dispatcher_drain(test.dispatcher,
                                                          FLOWIE_CLUSTER_TAKEOVER_TEST_WAIT_NS),
                 TURBO_OK);
    check_int_eq(flowie_cluster_takeover_dispatcher_destroy(test.dispatcher), TURBO_OK);
    test.dispatcher = NULL;
    flowie_cluster_takeover_dispatch_test_cleanup(&test);
  }
}
