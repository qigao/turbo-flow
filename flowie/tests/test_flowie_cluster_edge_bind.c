#include "flowie_cluster_edge_bind_internal.h"

#include "flow_coronet_execution.h"
#include "flowie_cluster_peer_wire_internal.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <string.h>

#define FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD 2048u
#define FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS UINT64_C(5000000000)

typedef struct flowie_cluster_edge_bind_test_s {
  flowie_cluster_owner_token_t owner;
  flowie_cluster_peer_frame_t command;
  int submit_count;
  int submit_status;
  int completion_count;
  int completion_status;
  uint64_t reply_session_id;
  uint8_t reply_packet_type;
  uint8_t reply_close;
  turbo_flow_protocol_settlement_point_t reply_settlement;
  int takeover_close_count;
  int takeover_close_status;
  int action_apply_count;
  int action_apply_status;
  uint8_t action_packet_type;
  turbo_flow_protocol_settlement_point_t action_settlement;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
} flowie_cluster_edge_bind_test_t;

typedef struct flowie_cluster_edge_bind_local_s {
  flowie_cluster_edge_bind_test_t *test;
  flowie_cluster_session_bind_t *session;
  flowie_cluster_peer_owner_t *owner;
  flowie_cluster_edge_bind_t *edge;
  int self_fence_count;
} flowie_cluster_edge_bind_local_t;

typedef struct flowie_cluster_edge_bind_connect_call_s {
  flowie_cluster_edge_bind_t *edge;
  const flowie_mqtt_connect_view_t *connect;
  flowie_cluster_edge_bind_test_t *test;
  uint64_t connection_id;
} flowie_cluster_edge_bind_connect_call_t;

typedef struct flowie_cluster_edge_bind_command_call_s {
  flowie_cluster_edge_bind_t *edge;
  flowie_cluster_edge_bind_test_t *test;
  flowie_cluster_owner_token_t expected_owner;
  uint64_t connection_id;
  flowie_cluster_peer_operation_t operation;
  flowie_mqtt_version_t version;
  flowie_mqtt_span_t client_id;
  flowie_mqtt_span_t packet;
} flowie_cluster_edge_bind_command_call_t;

typedef struct flowie_cluster_edge_bind_takeover_call_s {
  flowie_cluster_edge_bind_t *edge;
  flowie_cluster_edge_bind_test_t *test;
  const flowie_cluster_peer_frame_t *command;
  uint64_t expected_connection_id;
  uint64_t expected_connection_generation;
  flowie_mqtt_version_t version;
  flowie_mqtt_span_t client_id;
} flowie_cluster_edge_bind_takeover_call_t;

typedef struct flowie_cluster_edge_bind_action_call_s {
  flowie_cluster_edge_bind_t *edge;
  flowie_cluster_edge_bind_test_t *test;
  const flowie_cluster_peer_frame_t *command;
  flowie_cluster_owner_token_t expected_owner;
  uint64_t expected_connection_id;
  uint64_t expected_connection_generation;
  flowie_mqtt_version_t version;
  uint64_t *last_applied_sequence;
} flowie_cluster_edge_bind_action_call_t;

static int flowie_cluster_edge_bind_test_resolve(void *ctx, flowie_mqtt_span_t client_id,
                                                 flowie_cluster_owner_token_t *out) {
  flowie_cluster_edge_bind_test_t *test = (flowie_cluster_edge_bind_test_t *)ctx;
  if (!test || !out || !client_id.data || client_id.size == 0u) return TURBO_EINVAL;
  *out = test->owner;
  return TURBO_OK;
}

static int flowie_cluster_edge_bind_test_submit(void *ctx,
                                                const flowie_cluster_peer_frame_t *command) {
  flowie_cluster_edge_bind_test_t *test = (flowie_cluster_edge_bind_test_t *)ctx;
  tstr_t encoded = NULL;
  size_t consumed = 0u;
  int rc;
  if (!test || !command) return TURBO_EINVAL;
  ++test->submit_count;
  if (test->submit_status != TURBO_OK) return test->submit_status;
  flowie_cluster_peer_frame_cleanup(&test->command);
  rc = flowie_cluster_peer_frame_encode(command, FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD,
                                        &encoded);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_frame_decode(encoded, tstr_len(encoded),
                                          FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &test->command,
                                          &consumed);
  if (rc == TURBO_OK && consumed != tstr_len(encoded)) rc = TURBO_EPROTO;
  tstr_free(encoded);
  return rc;
}

static void
flowie_cluster_edge_bind_test_complete(void *ctx, int status,
                                       const flowie_cluster_peer_frame_t *frame,
                                       const flowie_cluster_session_bind_reply_view_t *reply) {
  flowie_cluster_edge_bind_test_t *test = (flowie_cluster_edge_bind_test_t *)ctx;
  if (!test) return;
  turbo_mutex_lock(&test->mutex);
  ++test->completion_count;
  test->completion_status = status;
  if (status == TURBO_OK && frame && reply) {
    test->reply_session_id = reply->route.session_id;
    test->reply_packet_type = reply->packet.data ? reply->packet.data[0] : 0u;
  }
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
}

static void flowie_cluster_edge_bind_test_command_complete(
    void *ctx, int status, const flowie_cluster_peer_frame_t *frame,
    const flowie_cluster_peer_mqtt_reply_action_t *action) {
  flowie_cluster_edge_bind_test_t *test = (flowie_cluster_edge_bind_test_t *)ctx;
  if (!test) return;
  turbo_mutex_lock(&test->mutex);
  ++test->completion_count;
  test->completion_status = status;
  if (status == TURBO_OK && frame && action) {
    test->reply_packet_type = (uint8_t)action->packet.type;
    test->reply_close = action->close_after_send;
    test->reply_settlement = action->settlement_point;
  }
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
}

static int flowie_cluster_edge_bind_test_wait(flowie_cluster_edge_bind_test_t *test,
                                              int completion_count) {
  uint64_t deadline = turbo_hrtime() + FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS;
  int rc = TURBO_OK;
  turbo_mutex_lock(&test->mutex);
  while (test->completion_count < completion_count) {
    uint64_t now = turbo_hrtime();
    if (now >= deadline ||
        turbo_cond_timedwait(&test->changed, &test->mutex, deadline - now) != TURBO_OK) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
  }
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_edge_bind_test_connect_call(void *arg) {
  flowie_cluster_edge_bind_connect_call_t *call = (flowie_cluster_edge_bind_connect_call_t *)arg;
  if (!call) return TURBO_EINVAL;
  return flowie_cluster_edge_bind_connect(call->edge, call->connection_id, 1u, call->connect, NULL,
                                          NULL, flowie_cluster_edge_bind_test_complete, call->test);
}

static int flowie_cluster_edge_bind_test_connect_submit(tf_coronet_execution_t *execution,
                                                        flowie_cluster_edge_bind_t *edge,
                                                        uint64_t connection_id,
                                                        const flowie_mqtt_connect_view_t *connect,
                                                        flowie_cluster_edge_bind_test_t *test) {
  flowie_cluster_edge_bind_connect_call_t call;
  memset(&call, 0, sizeof(call));
  call.edge = edge;
  call.connect = connect;
  call.test = test;
  call.connection_id = connection_id;
  return tf_coronet_execution_call(execution, flowie_cluster_edge_bind_test_connect_call, &call,
                                   FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS);
}

static int flowie_cluster_edge_bind_test_command_call(void *arg) {
  flowie_cluster_edge_bind_command_call_t *call = (flowie_cluster_edge_bind_command_call_t *)arg;
  if (!call) return TURBO_EINVAL;
  if (call->operation == FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST)
    return flowie_cluster_edge_bind_connection_lost(
        call->edge, call->connection_id, 1u, &call->expected_owner, call->version, call->client_id,
        flowie_cluster_edge_bind_test_command_complete, call->test);
  return flowie_cluster_edge_bind_command(
      call->edge, call->connection_id, 1u, &call->expected_owner, call->operation, call->version,
      call->client_id, call->packet, flowie_cluster_edge_bind_test_command_complete, call->test);
}

static int flowie_cluster_edge_bind_test_command_submit(
    tf_coronet_execution_t *execution, flowie_cluster_edge_bind_t *edge,
    flowie_cluster_edge_bind_test_t *test, const flowie_cluster_owner_token_t *expected_owner,
    flowie_cluster_peer_operation_t operation, flowie_mqtt_version_t version,
    flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet) {
  flowie_cluster_edge_bind_command_call_t call;
  memset(&call, 0, sizeof(call));
  call.edge = edge;
  call.test = test;
  call.expected_owner = *expected_owner;
  call.connection_id = 5u;
  call.operation = operation;
  call.version = version;
  call.client_id = client_id;
  call.packet = packet;
  return tf_coronet_execution_call(execution, flowie_cluster_edge_bind_test_command_call, &call,
                                   FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS);
}

static int flowie_cluster_edge_bind_test_takeover_close(void *ctx) {
  flowie_cluster_edge_bind_test_t *test = (flowie_cluster_edge_bind_test_t *)ctx;
  if (!test) return TURBO_EINVAL;
  ++test->takeover_close_count;
  return test->takeover_close_status;
}

static int flowie_cluster_edge_bind_test_takeover_call(void *arg) {
  flowie_cluster_edge_bind_takeover_call_t *call = (flowie_cluster_edge_bind_takeover_call_t *)arg;
  if (!call) return TURBO_EINVAL;
  return flowie_cluster_edge_bind_takeover_close(
      call->edge, call->command, call->expected_connection_id, call->expected_connection_generation,
      call->version, call->client_id, flowie_cluster_edge_bind_test_takeover_close, call->test);
}

static int flowie_cluster_edge_bind_test_takeover_submit(
    tf_coronet_execution_t *execution, flowie_cluster_edge_bind_t *edge,
    flowie_cluster_edge_bind_test_t *test, const flowie_cluster_peer_frame_t *command,
    uint64_t expected_connection_id, uint64_t expected_connection_generation,
    flowie_mqtt_version_t version, flowie_mqtt_span_t client_id) {
  flowie_cluster_edge_bind_takeover_call_t call;
  memset(&call, 0, sizeof(call));
  call.edge = edge;
  call.test = test;
  call.command = command;
  call.expected_connection_id = expected_connection_id;
  call.expected_connection_generation = expected_connection_generation;
  call.version = version;
  call.client_id = client_id;
  return tf_coronet_execution_call(execution, flowie_cluster_edge_bind_test_takeover_call, &call,
                                   FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS);
}

static int flowie_cluster_edge_bind_test_action_apply(
    void *ctx, const flowie_cluster_peer_mqtt_reply_action_t *action) {
  flowie_cluster_edge_bind_test_t *test = (flowie_cluster_edge_bind_test_t *)ctx;
  if (!test || !action) return TURBO_EINVAL;
  ++test->action_apply_count;
  test->action_packet_type = (uint8_t)action->packet.type;
  test->action_settlement = action->settlement_point;
  return test->action_apply_status;
}

static int flowie_cluster_edge_bind_test_action_call(void *arg) {
  flowie_cluster_edge_bind_action_call_t *call = (flowie_cluster_edge_bind_action_call_t *)arg;
  if (!call) return TURBO_EINVAL;
  return flowie_cluster_edge_bind_apply_action(
      call->edge, call->command, &call->expected_owner, call->expected_connection_id,
      call->expected_connection_generation, call->version, call->last_applied_sequence,
      flowie_cluster_edge_bind_test_action_apply, call->test);
}

static int flowie_cluster_edge_bind_test_action_submit(
    tf_coronet_execution_t *execution, flowie_cluster_edge_bind_t *edge,
    flowie_cluster_edge_bind_test_t *test, const flowie_cluster_peer_frame_t *command,
    const flowie_cluster_owner_token_t *expected_owner, uint64_t expected_connection_id,
    uint64_t expected_connection_generation, flowie_mqtt_version_t version,
    uint64_t *last_applied_sequence) {
  flowie_cluster_edge_bind_action_call_t call;
  memset(&call, 0, sizeof(call));
  call.edge = edge;
  call.test = test;
  call.command = command;
  call.expected_owner = *expected_owner;
  call.expected_connection_id = expected_connection_id;
  call.expected_connection_generation = expected_connection_generation;
  call.version = version;
  call.last_applied_sequence = last_applied_sequence;
  return tf_coronet_execution_call(execution, flowie_cluster_edge_bind_test_action_call, &call,
                                   FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS);
}

static int flowie_cluster_edge_bind_test_connect(flowie_mqtt_packet_view_t *packet,
                                                 flowie_mqtt_connect_view_t *connect, uint8_t *wire,
                                                 size_t capacity) {
  static const uint8_t client_id[] = "client-a";
  static const uint8_t username[] = "edge-user";
  static const uint8_t password[] = "edge-secret";
  flowie_mqtt_connect_packet_t description = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  size_t written = 0u;
  size_t consumed = 0u;
  int rc;
  description.version = FLOWIE_MQTT_VERSION_3_1_1;
  description.clean_start = 1u;
  description.has_username = 1u;
  description.has_password = 1u;
  description.keep_alive = 30u;
  description.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  description.username = (flowie_mqtt_span_t){username, sizeof(username) - 1u};
  description.password = (flowie_mqtt_span_t){password, sizeof(password) - 1u};
  rc = flowie_mqtt_connect_packet_encode(&description, wire, capacity, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  options.max_packet_size = capacity;
  rc = flowie_mqtt_packet_parse(wire, written, &options, packet, &consumed, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK || consumed != written) return TURBO_EPROTO;
  rc = flowie_mqtt_connect_parse(packet, connect);
  return rc == FLOWIE_MQTT_PARSE_OK ? TURBO_OK : TURBO_EPROTO;
}

static tstr_t flowie_cluster_edge_bind_test_reply_payload(void) {
  static const uint8_t connack[] = {0x20u, 0x02u, 0x00u, 0x00u};
  const size_t total = FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE + sizeof(connack);
  tstr_t payload = tstr_new_len(NULL, total);
  uint8_t *bytes = (uint8_t *)payload;
  if (!payload) return NULL;
  memcpy(bytes, "TFBR", 4u);
  flowie_cluster_peer_wire_write_u16(bytes + 4u, FLOWIE_CLUSTER_SESSION_BIND_REPLY_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + 6u, FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + 8u, (uint32_t)total);
  flowie_cluster_peer_wire_write_u32(bytes + 12u, 1u | ((uint32_t)FLOWIE_MQTT_VERSION_3_1_1 << 8u));
  flowie_cluster_peer_wire_write_u64(bytes + 16u, 9u);
  flowie_cluster_peer_wire_write_u64(bytes + 24u, 17u);
  flowie_cluster_peer_wire_write_u64(bytes + 32u, 3u);
  memcpy(bytes + FLOWIE_CLUSTER_SESSION_BIND_REPLY_HEADER_SIZE, connack, sizeof(connack));
  return payload;
}

static flowie_cluster_peer_frame_t
flowie_cluster_edge_bind_test_reply(flowie_cluster_edge_bind_test_t *test, tstr_t payload) {
  flowie_cluster_peer_frame_t reply = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  reply.kind = FLOWIE_CLUSTER_PEER_FRAME_REPLY;
  reply.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY;
  reply.shard_id = test->command.shard_id;
  reply.status = TURBO_OK;
  reply.owner_epoch = test->command.owner_epoch;
  reply.connection_id = test->command.connection_id;
  reply.connection_generation = test->command.connection_generation;
  reply.cluster_id = test->command.cluster_id;
  reply.listener_id = test->command.listener_id;
  reply.source_node_id = test->command.target_node_id;
  reply.target_node_id = test->command.source_node_id;
  reply.payload = tstr_to_v(payload);
  memcpy(reply.source_boot_id, test->command.target_boot_id, sizeof(reply.source_boot_id));
  memcpy(reply.target_boot_id, test->command.source_boot_id, sizeof(reply.target_boot_id));
  memcpy(reply.correlation_id, test->command.correlation_id, sizeof(reply.correlation_id));
  return reply;
}

static flowie_cluster_edge_bind_config_t
flowie_cluster_edge_bind_test_config(tf_coronet_execution_t *execution,
                                     flowie_cluster_edge_bind_test_t *test,
                                     size_t max_pending_entries) {
  static const char cluster_id[] = "cluster-a";
  static const char listener_id[] = "mqtt-main";
  static const char node_id[] = "edge-a";
  flowie_cluster_edge_bind_config_t config = FLOWIE_CLUSTER_EDGE_BIND_CONFIG_INIT;
  config.execution = execution;
  config.max_payload_size = FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD;
  config.max_pending_entries = max_pending_entries;
  config.max_pending_bytes = 1024u * 1024u;
  config.cluster_id = tstr_v_from_buf(cluster_id, sizeof(cluster_id) - 1u);
  config.listener_id = tstr_v_from_buf(listener_id, sizeof(listener_id) - 1u);
  config.local_node_id = tstr_v_from_buf(node_id, sizeof(node_id) - 1u);
  config.local_boot_id[0] = 0x11u;
  config.resolve = flowie_cluster_edge_bind_test_resolve;
  config.resolve_ctx = test;
  config.submit = flowie_cluster_edge_bind_test_submit;
  config.submit_ctx = test;
  return config;
}

static int flowie_cluster_edge_bind_test_setup(flowie_cluster_edge_bind_test_t *test) {
  uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
  memset(test, 0, sizeof(*test));
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
  test->command = (flowie_cluster_peer_frame_t)FLOWIE_CLUSTER_PEER_FRAME_INIT;
  owner_boot[0] = 0x22u;
  return flowie_cluster_owner_token_init(&test->owner, 7u, 41u, "owner-a", 7u, owner_boot);
}

static void
flowie_cluster_edge_bind_test_execution_init(tf_coronet_execution_t *execution,
                                             turbo_flow_coronet_execution_binding_t *binding) {
  *binding = (turbo_flow_coronet_execution_binding_t){0};
  binding->size = sizeof(*binding);
  binding->kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  check_int_eq(tf_coronet_execution_init(execution, binding), TURBO_OK);
  check_int_eq(tf_coronet_execution_start(execution), TURBO_OK);
}

static void flowie_cluster_edge_bind_test_finish(flowie_cluster_edge_bind_t *edge,
                                                 tf_coronet_execution_t *execution,
                                                 flowie_cluster_edge_bind_test_t *test) {
  check_int_eq(flowie_cluster_edge_bind_close(edge), TURBO_OK);
  check_int_eq(flowie_cluster_edge_bind_drain(edge, FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS),
               TURBO_OK);
  check_int_eq(flowie_cluster_edge_bind_destroy(edge), TURBO_OK);
  tf_coronet_execution_stop(execution);
  tf_coronet_execution_destroy(execution);
  flowie_cluster_peer_frame_cleanup(&test->command);
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

static uint64_t flowie_cluster_edge_bind_local_now(void *ctx) {
  (void)ctx;
  return UINT64_C(1000);
}

static void flowie_cluster_edge_bind_local_fence(void *ctx, int reason) {
  flowie_cluster_edge_bind_local_t *local = (flowie_cluster_edge_bind_local_t *)ctx;
  (void)reason;
  if (local) ++local->self_fence_count;
}

static int flowie_cluster_edge_bind_local_fact_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  (void)ctx;
  if (!command || !completion || !completion_ctx) return TURBO_EINVAL;
  completion(completion_ctx, command->command_id, TURBO_OK);
  return TURBO_OK;
}

static int flowie_cluster_edge_bind_local_owner_resolve(void *ctx, uint32_t shard_id,
                                                        flowie_cluster_owner_token_t *out) {
  flowie_cluster_edge_bind_local_t *local = (flowie_cluster_edge_bind_local_t *)ctx;
  if (!local || !local->test || !out || shard_id != local->test->owner.shard_id)
    return TURBO_EINVAL;
  *out = local->test->owner;
  return TURBO_OK;
}

static int flowie_cluster_edge_bind_local_execute(void *ctx,
                                                  const flowie_cluster_peer_frame_t *command,
                                                  flowie_cluster_peer_owner_complete_fn complete,
                                                  void *completion_ctx) {
  flowie_cluster_edge_bind_local_t *local = (flowie_cluster_edge_bind_local_t *)ctx;
  return !local || !local->session ? TURBO_EINVAL
                                   : flowie_cluster_session_bind_execute_async(
                                         local->session, command, complete, completion_ctx);
}

static int flowie_cluster_edge_bind_local_reply(void *ctx,
                                                const flowie_cluster_peer_frame_t *reply) {
  flowie_cluster_edge_bind_local_t *local = (flowie_cluster_edge_bind_local_t *)ctx;
  return !local || !local->edge ? TURBO_EINVAL : flowie_cluster_edge_bind_reply(local->edge, reply);
}

static int flowie_cluster_edge_bind_local_submit(void *ctx,
                                                 const flowie_cluster_peer_frame_t *command) {
  flowie_cluster_edge_bind_local_t *local = (flowie_cluster_edge_bind_local_t *)ctx;
  return !local || !local->owner ? TURBO_EINVAL
                                 : flowie_cluster_peer_owner_submit(local->owner, command);
}

spec("flowie cluster connection-edge CONNECT_BIND port") {
  it("correlates a post-CONNECT command to a typed socket action and fences stale ownership") {
    static const uint8_t client_id[] = "client-a";
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x07u, 0x00u, 0x00u,
                                        0x03u, 'a',   '/',   '#',   0x01u};
    static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x07u, 0x00u, 0x01u};
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_config_t config;
    flowie_cluster_edge_bind_t *edge = NULL;
    flowie_cluster_owner_token_t stale;
    flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
    flowie_cluster_peer_frame_t reply;
    tstr_t reply_payload = NULL;
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);
    config = flowie_cluster_edge_bind_test_config(&execution, &test, 2u);
    check_int_eq(flowie_cluster_edge_bind_create(&config, &edge), TURBO_OK);
    stale = test.owner;
    ++stale.owner_epoch;
    check_int_eq(flowie_cluster_edge_bind_test_command_submit(
                     &execution, edge, &test, &stale, FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE,
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){subscribe, sizeof(subscribe)}),
                 TURBO_EBUSY);
    check_int_eq(test.submit_count, 0);
    check_int_eq(flowie_cluster_edge_bind_test_command_submit(
                     &execution, edge, &test, &stale, FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST,
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){NULL, 0u}),
                 TURBO_EBUSY);
    check_int_eq(test.submit_count, 0);
    check_int_eq(flowie_cluster_edge_bind_test_command_submit(
                     &execution, edge, &test, &test.owner,
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){subscribe, sizeof(subscribe)}),
                 TURBO_OK);
    check_int_eq(test.command.operation, FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE);
    check_int_eq(flowie_cluster_peer_mqtt_command_decode(
                     test.command.operation, test.command.payload.data, test.command.payload.len,
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &decoded),
                 TURBO_OK);
    check_int_eq(decoded.packet.type, FLOWIE_MQTT_PACKET_SUBSCRIBE);
    check_int_eq(flowie_cluster_peer_mqtt_reply_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){suback, sizeof(suback)}, 0,
                     TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED,
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &reply_payload),
                 TURBO_OK);
    reply = flowie_cluster_edge_bind_test_reply(&test, reply_payload);
    check_int_eq(flowie_cluster_edge_bind_reply(edge, &reply), TURBO_OK);
    tstr_free(reply_payload);
    check_int_eq(flowie_cluster_edge_bind_test_wait(&test, 1), TURBO_OK);
    check_int_eq(test.completion_status, TURBO_OK);
    check_int_eq(test.reply_packet_type, FLOWIE_MQTT_PACKET_SUBACK);
    check_int_eq(test.reply_close, 0);
    check_int_eq(test.reply_settlement, TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED);
    flowie_cluster_edge_bind_test_finish(edge, &execution, &test);
  }

  it("closes only the exact old generation for an authoritative takeover command") {
    static const uint8_t client_id[] = "client-a";
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_config_t config;
    flowie_cluster_edge_bind_t *edge = NULL;
    flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    tstr_t payload = NULL;
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);
    config = flowie_cluster_edge_bind_test_config(&execution, &test, 2u);
    check_int_eq(flowie_cluster_edge_bind_create(&config, &edge), TURBO_OK);
    check_int_eq(flowie_cluster_peer_takeover_close_encode(
                     FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &payload),
                 TURBO_OK);
    edge_boot[0] = 0x11u;
    command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
    command.operation = FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE;
    command.shard_id = test.owner.shard_id;
    command.owner_epoch = test.owner.owner_epoch;
    command.connection_id = 5u;
    command.connection_generation = 1u;
    command.cluster_id = tstr_v_from_cstr("cluster-a");
    command.listener_id = tstr_v_from_cstr("mqtt-main");
    command.source_node_id = tstr_v_from_buf(test.owner.node_id, test.owner.node_id_size);
    command.target_node_id = tstr_v_from_cstr("edge-a");
    command.payload = tstr_to_v(payload);
    memcpy(command.source_boot_id, test.owner.boot_id, sizeof(command.source_boot_id));
    memcpy(command.target_boot_id, edge_boot, sizeof(command.target_boot_id));
    command.correlation_id[0] = 1u;

    check_int_eq(flowie_cluster_edge_bind_test_takeover_submit(
                     &execution, edge, &test, &command, 5u, 1u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}),
                 TURBO_OK);
    check_int_eq(test.takeover_close_count, 1);
    check_int_eq(test.command.kind, FLOWIE_CLUSTER_PEER_FRAME_REPLY);
    check_int_eq(test.command.operation, FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY);
    check_int_eq(test.command.status, TURBO_OK);
    check_int_eq(
        flowie_cluster_peer_mqtt_reply_decode(test.command.payload.data, test.command.payload.len,
                                              FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &action),
        TURBO_OK);
    check_int_eq(action.packet.type, 0);
    check_false(action.close_after_send);

    command.correlation_id[0] = 2u;
    check_int_eq(flowie_cluster_edge_bind_test_takeover_submit(
                     &execution, edge, &test, &command, 5u, 2u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}),
                 TURBO_OK);
    check_int_eq(test.takeover_close_count, 1);
    check_int_eq(test.command.status, TURBO_EBUSY);
    check_size_eq(test.command.payload.len, 0u);

    test.takeover_close_status = TURBO_EALREADY;
    command.correlation_id[0] = 3u;
    check_int_eq(flowie_cluster_edge_bind_test_takeover_submit(
                     &execution, edge, &test, &command, 5u, 1u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}),
                 TURBO_OK);
    check_int_eq(test.takeover_close_count, 2);
    check_int_eq(test.command.status, TURBO_OK);

    ++test.owner.owner_epoch;
    command.correlation_id[0] = 4u;
    check_int_eq(flowie_cluster_edge_bind_test_takeover_submit(
                     &execution, edge, &test, &command, 5u, 1u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}),
                 TURBO_OK);
    check_int_eq(test.takeover_close_count, 2);
    check_int_eq(test.command.status, TURBO_EBUSY);
    tstr_free(payload);
    flowie_cluster_edge_bind_test_finish(edge, &execution, &test);
  }

  it("applies independent socket actions once and rejects sequence or fence gaps") {
    static const uint8_t publish[] = {0x30u, 0x06u, 0x00u, 0x01u,
                                      'a',   0x00u, 'o',   'k'};
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_config_t config;
    flowie_cluster_edge_bind_t *edge = NULL;
    flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    tstr_t payload = NULL;
    uint64_t last_applied_sequence = 0u;
    uint64_t acknowledged_sequence = 0u;
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);
    config = flowie_cluster_edge_bind_test_config(&execution, &test, 2u);
    check_int_eq(flowie_cluster_edge_bind_create(&config, &edge), TURBO_OK);
    check_int_eq(flowie_cluster_peer_edge_action_encode(
                     1u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 0,
                     TURBO_FLOW_PROTOCOL_SETTLE_DURABLE,
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &payload),
                 TURBO_OK);
    edge_boot[0] = 0x11u;
    command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
    command.operation = FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION;
    command.shard_id = test.owner.shard_id;
    command.owner_epoch = test.owner.owner_epoch;
    command.connection_id = 5u;
    command.connection_generation = 1u;
    command.cluster_id = tstr_v_from_cstr("cluster-a");
    command.listener_id = tstr_v_from_cstr("mqtt-main");
    command.source_node_id = tstr_v_from_buf(test.owner.node_id, test.owner.node_id_size);
    command.target_node_id = tstr_v_from_cstr("edge-a");
    command.payload = tstr_to_v(payload);
    memcpy(command.source_boot_id, test.owner.boot_id, sizeof(command.source_boot_id));
    memcpy(command.target_boot_id, edge_boot, sizeof(command.target_boot_id));
    command.correlation_id[0] = 1u;

    check_int_eq(flowie_cluster_edge_bind_test_action_submit(
                     &execution, edge, &test, &command, &test.owner, 5u, 1u,
                     FLOWIE_MQTT_VERSION_5, &last_applied_sequence),
                 TURBO_OK);
    check_int_eq(test.action_apply_count, 1);
    check_int_eq(test.action_packet_type, FLOWIE_MQTT_PACKET_PUBLISH);
    check_int_eq(test.action_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_uint_eq(last_applied_sequence, 1u);
    check_int_eq(test.command.operation, FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK);
    check_int_eq(test.command.status, TURBO_OK);
    check_int_eq(flowie_cluster_peer_edge_action_ack_decode(
                     test.command.payload.data, test.command.payload.len, &acknowledged_sequence),
                 TURBO_OK);
    check_uint_eq(acknowledged_sequence, 1u);

    command.correlation_id[0] = 2u;
    check_int_eq(flowie_cluster_edge_bind_test_action_submit(
                     &execution, edge, &test, &command, &test.owner, 5u, 1u,
                     FLOWIE_MQTT_VERSION_5, &last_applied_sequence),
                 TURBO_OK);
    check_int_eq(test.action_apply_count, 1);
    check_int_eq(test.command.status, TURBO_OK);

    tstr_free(payload);
    payload = NULL;
    check_int_eq(flowie_cluster_peer_edge_action_encode(
                     3u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 0,
                     TURBO_FLOW_PROTOCOL_SETTLE_DURABLE,
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &payload),
                 TURBO_OK);
    command.payload = tstr_to_v(payload);
    command.correlation_id[0] = 3u;
    check_int_eq(flowie_cluster_edge_bind_test_action_submit(
                     &execution, edge, &test, &command, &test.owner, 5u, 1u,
                     FLOWIE_MQTT_VERSION_5, &last_applied_sequence),
                 TURBO_OK);
    check_int_eq(test.action_apply_count, 1);
    check_uint_eq(last_applied_sequence, 1u);
    check_int_eq(test.command.status, TURBO_EBUSY);
    check_size_eq(test.command.payload.len, 0u);

    tstr_free(payload);
    payload = NULL;
    check_int_eq(flowie_cluster_peer_edge_action_encode(
                     2u, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, 0,
                     TURBO_FLOW_PROTOCOL_SETTLE_DURABLE,
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &payload),
                 TURBO_OK);
    command.payload = tstr_to_v(payload);
    command.correlation_id[0] = 4u;
    check_int_eq(flowie_cluster_edge_bind_test_action_submit(
                     &execution, edge, &test, &command, &test.owner, 5u, 2u,
                     FLOWIE_MQTT_VERSION_5, &last_applied_sequence),
                 TURBO_OK);
    check_int_eq(test.action_apply_count, 1);
    check_int_eq(test.command.status, TURBO_EBUSY);

    ++test.owner.owner_epoch;
    command.correlation_id[0] = 5u;
    check_int_eq(flowie_cluster_edge_bind_test_action_submit(
                     &execution, edge, &test, &command, &test.owner, 5u, 1u,
                     FLOWIE_MQTT_VERSION_5, &last_applied_sequence),
                 TURBO_OK);
    check_int_eq(test.action_apply_count, 1);
    check_int_eq(test.command.status, TURBO_EBUSY);
    tstr_free(payload);
    flowie_cluster_edge_bind_test_finish(edge, &execution, &test);
  }

  it("correlates a validated owner reply back onto the edge execution") {
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_config_t config;
    flowie_cluster_edge_bind_t *edge = NULL;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    flowie_cluster_peer_frame_t reply;
    tstr_t reply_payload;
    uint8_t wire[256];
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);
    config = flowie_cluster_edge_bind_test_config(&execution, &test, 2u);
    check_int_eq(flowie_cluster_edge_bind_create(&config, &edge), TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_test_connect(&packet, &connect, wire, sizeof(wire)),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_edge_bind_test_connect_submit(&execution, edge, 5u, &connect, &test),
        TURBO_OK);
    check_int_eq(test.submit_count, 1);
    check_int_eq(test.command.operation, FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND);
    check_int_eq(flowie_cluster_peer_connect_bind_decode(
                     test.command.payload.data, test.command.payload.len,
                     FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD, &decoded),
                 TURBO_OK);
    check_size_eq(decoded.connect.username.size, 0u);
    check_size_eq(decoded.connect.password.size, 0u);
    reply_payload = flowie_cluster_edge_bind_test_reply_payload();
    check_not_null(reply_payload);
    reply = flowie_cluster_edge_bind_test_reply(&test, reply_payload);
    check_int_eq(flowie_cluster_edge_bind_reply(edge, &reply), TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_reply(edge, &reply), TURBO_EALREADY);
    tstr_free(reply_payload);
    flowie_cluster_edge_bind_test_finish(edge, &execution, &test);
    check_int_eq(test.completion_count, 1);
    check_int_eq(test.completion_status, TURBO_OK);
    check_uint_eq(test.reply_session_id, 17u);
    check_uint_eq(test.reply_packet_type, 0x20u);
  }

  it("applies bounded admission and cancels an unresolved request during close") {
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_config_t config;
    flowie_cluster_edge_bind_t *edge = NULL;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    uint8_t wire[256];
    size_t pending_entries = 0u;
    size_t pending_bytes = 0u;
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);
    config = flowie_cluster_edge_bind_test_config(&execution, &test, 1u);
    check_int_eq(flowie_cluster_edge_bind_create(&config, &edge), TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_test_connect(&packet, &connect, wire, sizeof(wire)),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_edge_bind_test_connect_submit(&execution, edge, 5u, &connect, &test),
        TURBO_OK);
    check_int_eq(
        flowie_cluster_edge_bind_test_connect_submit(&execution, edge, 6u, &connect, &test),
        TURBO_ENOSPC);
    check_int_eq(flowie_cluster_edge_bind_pending(edge, &pending_entries, &pending_bytes),
                 TURBO_OK);
    check_size_eq(pending_entries, 1u);
    check_true(pending_bytes != 0u);
    flowie_cluster_edge_bind_test_finish(edge, &execution, &test);
    check_int_eq(test.submit_count, 1);
    check_int_eq(test.completion_count, 1);
    check_int_eq(test.completion_status, TURBO_ESHUTDOWN);
  }

  it("fails a correlated reply from the wrong owner without installing its route") {
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_config_t config;
    flowie_cluster_edge_bind_t *edge = NULL;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_cluster_peer_frame_t reply;
    tstr_t reply_payload;
    uint8_t wire[256];
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);
    config = flowie_cluster_edge_bind_test_config(&execution, &test, 1u);
    check_int_eq(flowie_cluster_edge_bind_create(&config, &edge), TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_test_connect(&packet, &connect, wire, sizeof(wire)),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_edge_bind_test_connect_submit(&execution, edge, 5u, &connect, &test),
        TURBO_OK);
    reply_payload = flowie_cluster_edge_bind_test_reply_payload();
    check_not_null(reply_payload);
    reply = flowie_cluster_edge_bind_test_reply(&test, reply_payload);
    reply.source_boot_id[0] ^= 1u;
    check_int_eq(flowie_cluster_edge_bind_reply(edge, &reply), TURBO_OK);
    tstr_free(reply_payload);
    flowie_cluster_edge_bind_test_finish(edge, &execution, &test);
    check_int_eq(test.completion_count, 1);
    check_int_eq(test.completion_status, TURBO_EPROTO);
    check_uint_eq(test.reply_session_id, 0u);
  }

  it("composes the local edge and session owner through the same bounded command port") {
    static const char cluster_id[] = "cluster-a";
    static const char listener_id[] = "mqtt-main";
    static const char owner_node_id[] = "owner-a";
    static const uint8_t client_id[] = "client-a";
    static const uint8_t subscribe[] = {0x82u, 0x08u, 0x00u, 0x07u, 0x00u,
                                        0x03u, 'a',   '/',   '#',   0x01u};
    turbo_flow_coronet_execution_binding_t binding;
    tf_coronet_execution_t execution;
    flowie_cluster_edge_bind_test_t test;
    flowie_cluster_edge_bind_local_t local;
    flowie_cluster_session_bind_config_t session_config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
    flowie_cluster_peer_owner_config_t owner_config = FLOWIE_CLUSTER_PEER_OWNER_CONFIG_INIT;
    flowie_cluster_edge_bind_config_t edge_config;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    uint8_t wire[256];
    memset(&local, 0, sizeof(local));
    check_int_eq(flowie_cluster_edge_bind_test_setup(&test), TURBO_OK);
    local.test = &test;
    flowie_cluster_edge_bind_test_execution_init(&execution, &binding);

    session_config.max_sessions = 8u;
    session_config.max_bind_payload_size = FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD;
    session_config.max_fact_value_size = 4096u;
    session_config.max_event_payload_size = 4096u;
    session_config.session.owner_instance_id = 9u;
    session_config.session.max_subscriptions = 8u;
    session_config.session.max_inflight = 8u;
    session_config.first_session_id = 10u;
    session_config.submit = flowie_cluster_edge_bind_local_fact_submit;
    session_config.now = flowie_cluster_edge_bind_local_now;
    session_config.self_fence = flowie_cluster_edge_bind_local_fence;
    session_config.self_fence_ctx = &local;
    check_int_eq(flowie_cluster_session_bind_create(&session_config, &local.session), TURBO_OK);

    edge_config = flowie_cluster_edge_bind_test_config(&execution, &test, 2u);
    edge_config.submit = flowie_cluster_edge_bind_local_submit;
    edge_config.submit_ctx = &local;
    check_int_eq(flowie_cluster_edge_bind_create(&edge_config, &local.edge), TURBO_OK);

    owner_config.execution = &execution;
    owner_config.max_payload_size = FLOWIE_CLUSTER_EDGE_BIND_TEST_MAX_PAYLOAD;
    owner_config.queue_entries = 4u;
    owner_config.queue_bytes = 1024u * 1024u;
    owner_config.cluster_id = tstr_v_from_buf(cluster_id, sizeof(cluster_id) - 1u);
    owner_config.listener_id = tstr_v_from_buf(listener_id, sizeof(listener_id) - 1u);
    owner_config.local_node_id = tstr_v_from_buf(owner_node_id, sizeof(owner_node_id) - 1u);
    memcpy(owner_config.local_boot_id, test.owner.boot_id, sizeof(owner_config.local_boot_id));
    owner_config.resolve = flowie_cluster_edge_bind_local_owner_resolve;
    owner_config.execute_async = flowie_cluster_edge_bind_local_execute;
    owner_config.reply = flowie_cluster_edge_bind_local_reply;
    owner_config.user_data = &local;
    check_int_eq(flowie_cluster_peer_owner_create(&owner_config, &local.owner), TURBO_OK);

    check_int_eq(flowie_cluster_edge_bind_test_connect(&packet, &connect, wire, sizeof(wire)),
                 TURBO_OK);
    check_int_eq(
        flowie_cluster_edge_bind_test_connect_submit(&execution, local.edge, 5u, &connect, &test),
        TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_test_wait(&test, 1), TURBO_OK);
    check_int_eq(test.completion_status, TURBO_OK);
    check_uint_eq(test.reply_session_id, 11u);
    check_uint_eq(test.reply_packet_type, 0x20u);
    check_int_eq(local.self_fence_count, 0);

    check_int_eq(flowie_cluster_edge_bind_test_command_submit(
                     &execution, local.edge, &test, &test.owner,
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, FLOWIE_MQTT_VERSION_3_1_1,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){subscribe, sizeof(subscribe)}),
                 TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_test_wait(&test, 2), TURBO_OK);
    check_int_eq(test.completion_status, TURBO_OK);
    check_int_eq(test.reply_packet_type, FLOWIE_MQTT_PACKET_SUBACK);
    check_int_eq(test.reply_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     local.session, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     &snapshot, NULL),
                 TURBO_OK);
    check_size_eq(snapshot.subscription_count, 1u);
    check_true(snapshot.active);

    check_int_eq(flowie_cluster_edge_bind_test_command_submit(
                     &execution, local.edge, &test, &test.owner,
                     FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST, FLOWIE_MQTT_VERSION_3_1_1,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     (flowie_mqtt_span_t){NULL, 0u}),
                 TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_test_wait(&test, 3), TURBO_OK);
    check_int_eq(test.completion_status, TURBO_OK);
    check_int_eq(test.reply_packet_type, 0);
    check_int_eq(test.reply_settlement, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     local.session, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     &snapshot, NULL),
                 TURBO_OK);
    check_false(snapshot.active);

    check_int_eq(flowie_cluster_edge_bind_close(local.edge), TURBO_OK);
    check_int_eq(
        flowie_cluster_edge_bind_drain(local.edge, FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_close(local.owner), TURBO_OK);
    check_int_eq(
        flowie_cluster_peer_owner_drain(local.owner, FLOWIE_CLUSTER_EDGE_BIND_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_destroy(local.owner), TURBO_OK);
    check_int_eq(flowie_cluster_edge_bind_destroy(local.edge), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_destroy(local.session), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    flowie_cluster_peer_frame_cleanup(&test.command);
    turbo_cond_destroy(&test.changed);
    turbo_mutex_destroy(&test.mutex);
  }
}
