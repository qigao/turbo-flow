#include "flowie_cluster_session_bind_internal.h"

#include "flowie_cluster_lifecycle_dispatch_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

#define FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD 2048u
#define FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_VALUE 8192u

static const uint8_t FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT[] = "device-a";
static const uint8_t FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B[] = "device-b";

typedef struct flowie_cluster_session_bind_test_s {
  flowie_cluster_pgsql_fact_completion_fn fact_complete;
  void *fact_ctx;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  flowie_cluster_peer_owner_finalize_fn finalize;
  void *finalize_ctx;
  int complete_status;
  int complete_rc;
  int submit_rc;
  int submit_calls;
  int complete_calls;
  int fence_calls;
  int fence_reason;
  uint64_t expected_revision;
  uint64_t next_revision;
  tstr_t fact_key;
  tstr_t fact_value;
  tstr_t event_payload;
  const uint8_t *expected_client;
  size_t expected_client_size;
  uint32_t expected_event_type;
  int expected_event_only;
} flowie_cluster_session_bind_test_t;

static turbo_flow_security_principal_t flowie_cluster_session_bind_test_principal(void) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)strcpy(principal.principal_id, "writer");
  (void)strcpy(principal.principal_type, "device");
  (void)strcpy(principal.root_group_id, "root-a");
  (void)strcpy(principal.auth_method, "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  principal.role_count = 1u;
  (void)strcpy(principal.roles[0], "writer");
  principal.group_count = 1u;
  (void)strcpy(principal.groups[0], "root-a");
  principal.policy_version = 7u;
  return principal;
}

static int flowie_cluster_session_bind_test_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  static const uint8_t fact_magic[] = {'T', 'F', 'S', 'E'};
  static const uint8_t bound_event_magic[] = {'T', 'F', 'B', 'E'};
  static const uint8_t updated_event_magic[] = {'T', 'F', 'U', 'E'};
  static const uint8_t connection_lost_event_magic[] = {'T', 'F', 'L', 'E'};
  static const uint8_t taken_over_event_magic[] = {'T', 'F', 'T', 'E'};
  static const uint8_t publish_event_magic[] = {'T', 'F', 'P', 'E'};
  flowie_cluster_session_bind_test_t *test = (flowie_cluster_session_bind_test_t *)ctx;
  const flowie_cluster_pgsql_fact_mutation_t *mutation;
  const uint8_t *expected_client;
  size_t expected_client_size;
  tstr_t fact_key = NULL;
  tstr_t fact_value = NULL;
  tstr_t event_payload = NULL;
  if (!test || !command || !completion || !completion_ctx || command->mutation_count != 1u ||
      !command->mutations)
    return TURBO_EPROTO;
  mutation = &command->mutations[0];
  expected_client =
      test->expected_client ? test->expected_client : FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT;
  expected_client_size = test->expected_client
                             ? test->expected_client_size
                             : sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u;
  if (test->expected_event_type == 0u)
    test->expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_BOUND;
  if (mutation->write_kind != (test->expected_event_only ? FLOWIE_CLUSTER_PGSQL_EVENT_ONLY
                                                         : FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT) ||
      mutation->record.kind != TURBO_FLOW_RECORD_PUT ||
      (!test->expected_event_only &&
       (mutation->record.expected_revision != test->expected_revision ||
        mutation->record.next_revision <= mutation->record.expected_revision ||
        mutation->record.value_size < sizeof(fact_magic) ||
        memcmp(mutation->record.value, fact_magic, sizeof(fact_magic)) != 0)) ||
      (test->expected_event_only &&
       (mutation->record.expected_revision != TURBO_FLOW_RECORD_REVISION_ABSENT ||
        mutation->record.next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT ||
        mutation->record.value != NULL || mutation->record.value_size != 0u)) ||
      mutation->event_type != test->expected_event_type ||
      mutation->event_payload_size < sizeof(bound_event_magic) ||
      memcmp(mutation->event_payload,
             test->expected_event_type == FLOWIE_CLUSTER_SESSION_EVENT_BOUND ? bound_event_magic
             : test->expected_event_type == FLOWIE_CLUSTER_SESSION_EVENT_UPDATED
                 ? updated_event_magic
             : test->expected_event_type == FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST
                 ? connection_lost_event_magic
             : test->expected_event_type == FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER
                 ? taken_over_event_magic
                 : publish_event_magic,
             sizeof(bound_event_magic)) != 0 ||
      mutation->record.key_size != expected_client_size ||
      memcmp(mutation->record.key, expected_client, expected_client_size) != 0)
    return TURBO_EPROTO;
  fact_key = tstr_new_len(mutation->record.key, mutation->record.key_size);
  if (!test->expected_event_only)
    fact_value = tstr_new_len(mutation->record.value, mutation->record.value_size);
  event_payload = tstr_new_len(mutation->event_payload, mutation->event_payload_size);
  if (!fact_key || (!test->expected_event_only && !fact_value) || !event_payload) {
    tstr_free(fact_key);
    tstr_free(fact_value);
    tstr_free(event_payload);
    return TURBO_ENOMEM;
  }
  tstr_freep(&test->fact_key);
  tstr_freep(&test->fact_value);
  tstr_freep(&test->event_payload);
  test->fact_key = fact_key;
  test->fact_value = fact_value;
  test->event_payload = event_payload;
  ++test->submit_calls;
  test->next_revision = test->expected_event_only ? test->expected_revision
                                                  : mutation->record.next_revision;
  test->fact_complete = completion;
  test->fact_ctx = completion_ctx;
  memcpy(test->command_id, command->command_id, sizeof(test->command_id));
  return test->submit_rc;
}

static void flowie_cluster_session_bind_test_destroy(flowie_cluster_session_bind_test_t *test) {
  if (!test) return;
  tstr_freep(&test->fact_key);
  tstr_freep(&test->fact_value);
  tstr_freep(&test->event_payload);
}

static uint64_t flowie_cluster_session_bind_test_now(void *ctx) {
  (void)ctx;
  return UINT64_C(1000);
}

static void flowie_cluster_session_bind_test_fence(void *ctx, int reason) {
  flowie_cluster_session_bind_test_t *test = (flowie_cluster_session_bind_test_t *)ctx;
  ++test->fence_calls;
  test->fence_reason = reason;
}

static int flowie_cluster_session_bind_test_complete(void *ctx, int durable_status,
                                                     flowie_cluster_peer_owner_finalize_fn finalize,
                                                     void *finalize_ctx) {
  flowie_cluster_session_bind_test_t *test = (flowie_cluster_session_bind_test_t *)ctx;
  if (!test || !finalize || !finalize_ctx) return TURBO_EPROTO;
  ++test->complete_calls;
  test->complete_status = durable_status;
  test->finalize = finalize;
  test->finalize_ctx = finalize_ctx;
  return test->complete_rc;
}

static flowie_cluster_session_bind_config_t
flowie_cluster_session_bind_test_config(flowie_cluster_session_bind_test_t *test) {
  flowie_cluster_session_bind_config_t config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  config.max_sessions = 4u;
  config.max_bind_payload_size = FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD;
  config.max_fact_value_size = FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_VALUE;
  config.max_event_payload_size = 512u;
  config.session.owner_instance_id = 9u;
  config.session.max_subscriptions = 8u;
  config.session.max_inflight = 8u;
  config.first_session_id = 10u;
  config.security_enabled = 1u;
  config.submit = flowie_cluster_session_bind_test_submit;
  config.submit_ctx = test;
  config.now = flowie_cluster_session_bind_test_now;
  config.self_fence = flowie_cluster_session_bind_test_fence;
  config.self_fence_ctx = test;
  return config;
}

static int flowie_cluster_session_bind_test_payload_for(const uint8_t *client_id,
                                                        size_t client_id_size, tstr_t *out) {
  static const uint8_t empty_property = 0u;
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  turbo_flow_security_principal_t principal = flowie_cluster_session_bind_test_principal();
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 0u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, client_id_size};
  connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.properties.values.data = &empty_property;
  connect.will_properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.will_properties.values.data = &empty_property;
  return flowie_cluster_peer_connect_bind_encode(&connect, &principal,
                                                 FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, out);
}

static int flowie_cluster_session_bind_test_payload(tstr_t *out) {
  return flowie_cluster_session_bind_test_payload_for(
      FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT, sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u,
      out);
}

static int flowie_cluster_session_bind_test_will_payload(tstr_t *out) {
  static const uint8_t empty_property = 0u;
  static const uint8_t will_topic[] = "status/device-a";
  static const uint8_t will_payload[] = "offline";
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  turbo_flow_security_principal_t principal = flowie_cluster_session_bind_test_principal();
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 0u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                           sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u};
  connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.properties.values.data = &empty_property;
  connect.will_qos = 1u;
  connect.will_topic = (flowie_mqtt_span_t){will_topic, sizeof(will_topic) - 1u};
  connect.will_payload = (flowie_mqtt_span_t){will_payload, sizeof(will_payload) - 1u};
  connect.will_properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.will_properties.values.data = &empty_property;
  return flowie_cluster_peer_connect_bind_encode(&connect, &principal,
                                                 FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, out);
}

static flowie_cluster_peer_frame_t flowie_cluster_session_bind_test_command(tstr_t payload,
                                                                            uint8_t correlation) {
  flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  command.operation = FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND;
  command.shard_id = 3u;
  command.owner_epoch = 17u;
  command.connection_id = 21u;
  command.connection_generation = 2u;
  command.source_node_id = tstr_v_from_cstr("edge-a");
  command.target_node_id = tstr_v_from_cstr("owner-a");
  command.source_boot_id[0] = 1u;
  command.target_boot_id[0] = 2u;
  command.correlation_id[0] = correlation;
  command.payload = tstr_v_from_buf(payload, tstr_len(payload));
  return command;
}

static flowie_cluster_peer_frame_t
flowie_cluster_session_bind_test_packet_command(flowie_cluster_peer_operation_t operation,
                                                tstr_t payload, uint8_t correlation) {
  flowie_cluster_peer_frame_t command =
      flowie_cluster_session_bind_test_command(payload, correlation);
  command.operation = operation;
  return command;
}

static int flowie_cluster_session_bind_test_finalize(flowie_cluster_session_bind_test_t *test,
                                                     int durable_status, tstr_t *reply) {
  test->fact_complete(test->fact_ctx, test->command_id, durable_status);
  if (test->complete_rc != TURBO_OK) return test->complete_rc;
  return test->finalize(test->finalize_ctx, test->complete_status, reply);
}

typedef struct flowie_cluster_session_bind_target_capture_s {
  size_t calls;
  uint64_t session_id;
  uint64_t connection_id;
  uint8_t qos;
  uint8_t active;
  char client_id[32];
  char shared_filter[64];
  flowie_cluster_session_bind_t *bind;
  const flowie_cluster_owner_token_t *owner;
  const flowie_cluster_broadcast_event_view_t *source;
  const uint8_t *event_digest;
  flowie_cluster_session_delivery_plan_t *plan;
} flowie_cluster_session_bind_target_capture_t;

static int flowie_cluster_session_bind_capture_target(
    void *ctx, const flowie_cluster_session_publish_target_t *target) {
  flowie_cluster_session_bind_target_capture_t *capture =
      (flowie_cluster_session_bind_target_capture_t *)ctx;
  if (!capture || !target || target->size != sizeof(*target) ||
      target->abi_version != FLOWIE_CLUSTER_SESSION_BIND_ABI_V1 ||
      target->client_id.size >= sizeof(capture->client_id))
    return TURBO_EPROTO;
  capture->calls += 1u;
  capture->session_id = target->session_id;
  capture->connection_id = target->connection_id;
  capture->qos = target->qos;
  capture->active = target->active;
  memcpy(capture->client_id, target->client_id.data, target->client_id.size);
  capture->client_id[target->client_id.size] = '\0';
  if (target->shared_filter.size >= sizeof(capture->shared_filter)) return TURBO_EPROTO;
  if (target->shared_filter.size != 0u)
    memcpy(capture->shared_filter, target->shared_filter.data, target->shared_filter.size);
  capture->shared_filter[target->shared_filter.size] = '\0';
  if (capture->bind)
    return flowie_cluster_session_delivery_plan_create(
        capture->bind, capture->owner, capture->source, capture->event_digest, target,
        &capture->plan);
  return TURBO_OK;
}

spec("flowie cluster CONNECT_BIND staged executor") {
  it("durably applies a fenced SUBSCRIBE action and keeps TFSE recoverable") {
    static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x07u, 0x00u, 0x00u,
                                        0x03u, 'a',   '/',   '#',   0x01u};
    static const uint8_t unsubscribe[] = {0xa2u, 0x08u, 0x00u, 0x08u, 0x00u,
                                          0x00u, 0x03u, 'a',   '/',   '#'};
    static const uint8_t shared_subscribe[] = {
        0x82u, 0x12u, 0x00u, 0x0au, 0x00u, 0x00u, 0x0cu, '$', 's', 'h', 'a', 'r',
        'e',   '/',   'g',   '/',   'a',   '/',   '#',   0x01u};
    static const uint8_t pubrel[] = {0x62u, 0x04u, 0x00u, 0x09u, 0x00u, 0x00u};
    static const uint8_t disconnect[] = {0xe0u, 0x00u};
    static const uint8_t publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                      0x00u, 0x09u, 0x00u, 'o',   'k'};
    static const uint8_t publisher_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    flowie_cluster_publish_event_view_t publish_event = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    flowie_cluster_broadcast_event_view_t broadcast = FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
    flowie_cluster_session_bind_target_capture_t capture = {0};
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {2u};
    uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE] = {3u};
    size_t target_count = 0u;
    tstr_t connect_payload = NULL;
    tstr_t command_payload = NULL;
    tstr_t publish_event_payload = NULL;
    tstr_t durable_delivery_fact = NULL;
    uint64_t durable_delivery_revision = 0u;
    tstr_t reply = NULL;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&connect_payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(connect_payload, 1u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &before, NULL),
                 TURBO_OK);
    test.expected_revision = before.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_UPDATED;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){subscribe, sizeof(subscribe)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, command_payload, 2u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_uint_eq(after.resource_generation, before.resource_generation);
    check_size_eq(after.subscription_count, 0u);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, FLOWIE_MQTT_PACKET_SUBACK);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_size_eq(after.subscription_count, 1u);
    check_uint_eq(after.resource_generation, test.next_revision);
    check_int_eq(flowie_cluster_publish_event_encode(
                     FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 91u, 3u,
                     after.session_id, after.session_generation, 1000u,
                     tstr_v_from_cstr("edge-publisher"), publisher_boot,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){publish, sizeof(publish)}, config.max_event_payload_size,
                     &publish_event_payload),
                 TURBO_OK);
    check_int_eq(flowie_cluster_publish_event_decode(
                     publish_event_payload, tstr_len(publish_event_payload),
                     config.max_event_payload_size, &publish_event),
                 TURBO_OK);
    broadcast.command_id[0] = 4u;
    broadcast.source_shard_id = 2u;
    broadcast.source_owner_epoch = 7u;
    broadcast.publish = publish_event;
    check_int_eq(flowie_cluster_owner_token_init(&owner, 1u, 8u, "owner-a", 7u, owner_boot),
                 TURBO_OK);
    capture.bind = bind;
    capture.owner = &owner;
    capture.source = &broadcast;
    capture.event_digest = event_digest;
    check_int_eq(flowie_cluster_session_bind_match_publish(
                     bind, &publish_event, flowie_cluster_session_bind_capture_target, &capture,
                     &target_count),
                 TURBO_OK);
    check_size_eq(target_count, 1u);
    check_size_eq(capture.calls, 1u);
    check_uint_eq(capture.session_id, after.session_id);
    check_uint_eq(capture.connection_id, 21u);
    check_int_eq(capture.qos, 1u);
    check_true(capture.active);
    check_str_eq(capture.client_id, "device-a");
    {
      const flowie_cluster_pgsql_fact_command_t *delivery =
          flowie_cluster_session_delivery_plan_command(capture.plan);
      flowie_cluster_delivery_action_view_t delivery_action =
          FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
      check_not_null(delivery);
      check_size_eq(delivery->mutation_count, 1u);
      check_int_eq(delivery->mutations[0].write_kind, FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT);
      check_int_eq(delivery->mutations[0].event_type,
                   FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE);
      check_not_null(delivery->dedupe);
      check_uint_eq(delivery->dedupe->target_session_id, after.session_id);
      check_mem_eq(delivery->dedupe->event_digest, event_digest, sizeof(event_digest));
      check_int_eq(flowie_cluster_delivery_action_decode(
                       delivery->mutations[0].event_payload,
                       delivery->mutations[0].event_payload_size, config.max_event_payload_size,
                       &delivery_action),
                   TURBO_OK);
      check_uint_eq(delivery_action.edge_action.action_sequence, 1u);
      check_uint_eq(delivery_action.edge_action.action.packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
      check_int_eq(flowie_cluster_session_delivery_plan_finalize(&capture.plan, TURBO_OK),
                   TURBO_OK);
      check_null(capture.plan);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       bind,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &after, NULL),
                   TURBO_OK);
      check_size_eq(after.inflight_count, 1u);
    }

    {
      flowie_session_snapshot_t replay_before = after;
      const flowie_cluster_pgsql_fact_command_t *replay_command;
      flowie_cluster_delivery_action_view_t replay_action =
          FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
      capture.calls = 0u;
      capture.plan = NULL;
      check_int_eq(flowie_cluster_session_bind_match_publish(
                       bind, &publish_event, flowie_cluster_session_bind_capture_target, &capture,
                       &target_count),
                   TURBO_OK);
      replay_command = flowie_cluster_session_delivery_plan_command(capture.plan);
      check_not_null(replay_command);
      check_int_eq(flowie_cluster_delivery_action_decode(
                       replay_command->mutations[0].event_payload,
                       replay_command->mutations[0].event_payload_size,
                       config.max_event_payload_size, &replay_action),
                   TURBO_OK);
      check_uint_eq(replay_action.edge_action.action_sequence, 2u);
      check_int_eq(flowie_cluster_session_delivery_plan_finalize(&capture.plan, TURBO_EALREADY),
                   TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       bind,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &after, NULL),
                   TURBO_OK);
      check_uint_eq(after.resource_generation, replay_before.resource_generation);
      check_size_eq(after.inflight_count, replay_before.inflight_count);
    }
    {
      static const uint8_t qos0_publish[] = {0x30u, 0x08u, 0x00u, 0x03u, 'a',
                                             '/',   'b',   0x00u, 'o',   'k'};
      flowie_cluster_publish_event_view_t qos0_event = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
      flowie_cluster_broadcast_event_view_t qos0_broadcast =
          FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
      flowie_session_snapshot_t qos0_before = after;
      uint8_t qos0_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE] = {5u};
      tstr_t qos0_payload = NULL;
      const flowie_cluster_pgsql_fact_command_t *qos0_command;
      check_int_eq(flowie_cluster_publish_event_encode(
                       FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED, 92u, 4u,
                       after.session_id + 100u, 1u, 1000u,
                       tstr_v_from_cstr("edge-publisher"), publisher_boot,
                       (flowie_mqtt_span_t){(const uint8_t *)"publisher-b", 11u},
                       (flowie_mqtt_span_t){qos0_publish, sizeof(qos0_publish)},
                       config.max_event_payload_size, &qos0_payload),
                   TURBO_OK);
      check_int_eq(flowie_cluster_publish_event_decode(
                       qos0_payload, tstr_len(qos0_payload), config.max_event_payload_size,
                       &qos0_event),
                   TURBO_OK);
      qos0_broadcast.command_id[0] = 6u;
      qos0_broadcast.source_shard_id = 2u;
      qos0_broadcast.source_owner_epoch = 7u;
      qos0_broadcast.publish = qos0_event;
      capture.source = &qos0_broadcast;
      capture.event_digest = qos0_digest;
      capture.plan = NULL;
      check_int_eq(flowie_cluster_session_bind_match_publish(
                       bind, &qos0_event, flowie_cluster_session_bind_capture_target, &capture,
                       &target_count),
                   TURBO_OK);
      qos0_command = flowie_cluster_session_delivery_plan_command(capture.plan);
      check_not_null(qos0_command);
      check_size_eq(qos0_command->mutation_count, 1u);
      check_int_eq(qos0_command->mutations[0].write_kind, FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT);
      check_uint_eq(qos0_command->mutations[0].record.expected_revision,
                    qos0_before.resource_generation);
      check_true(qos0_command->mutations[0].record.next_revision >
                 qos0_command->mutations[0].record.expected_revision);
      check_uint_eq(flowie_cluster_peer_wire_read_u16(
                        qos0_command->mutations[0].record.value + 4u),
                    FLOWIE_CLUSTER_SESSION_FACT_VERSION);
      check_uint_eq(flowie_cluster_peer_wire_read_u16(
                        qos0_command->mutations[0].record.value + 6u),
                    FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE);
      check_uint_eq(flowie_cluster_peer_wire_read_u64(
                        qos0_command->mutations[0].record.value + 24u),
                    2u);
      durable_delivery_fact =
          tstr_new_len(qos0_command->mutations[0].record.value,
                       qos0_command->mutations[0].record.value_size);
      durable_delivery_revision = qos0_command->mutations[0].record.next_revision;
      check_not_null(durable_delivery_fact);
      check_int_eq(flowie_cluster_session_delivery_plan_finalize(&capture.plan, TURBO_OK),
                   TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       bind,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &after, NULL),
                   TURBO_OK);
      check_true(after.resource_generation > qos0_before.resource_generation);
      check_size_eq(after.inflight_count, qos0_before.inflight_count);
      capture.source = &broadcast;
      capture.event_digest = event_digest;
      tstr_free(qos0_payload);
    }

    capture.bind = NULL;
    capture.owner = NULL;
    capture.source = NULL;
    capture.event_digest = NULL;

    {
      flowie_cluster_session_bind_test_t recovered_test = {0};
      flowie_cluster_session_bind_config_t recovered_config =
          flowie_cluster_session_bind_test_config(&recovered_test);
      flowie_cluster_session_bind_t *recovered = NULL;
      flowie_cluster_session_recovery_t *recovery = NULL;
      turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
      flowie_session_snapshot_t recovered_snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
      record.key = (const uint8_t *)test.fact_key;
      record.key_size = tstr_len(test.fact_key);
      record.revision = durable_delivery_revision;
      record.value = (const uint8_t *)durable_delivery_fact;
      record.value_size = tstr_len(durable_delivery_fact);
      check_int_eq(flowie_cluster_session_bind_create(&recovered_config, &recovered), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_create(recovered, &recovery), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       recovered,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &recovered_snapshot, NULL),
                   TURBO_OK);
      check_false(recovered_snapshot.active);
      check_size_eq(recovered_snapshot.subscription_count, 1u);
      memset(&capture, 0, sizeof(capture));
      capture.bind = recovered;
      capture.owner = &owner;
      capture.source = &broadcast;
      capture.event_digest = event_digest;
      check_int_eq(flowie_cluster_session_bind_match_publish(
                       recovered, &publish_event, flowie_cluster_session_bind_capture_target,
                       &capture, &target_count),
                   TURBO_OK);
      check_size_eq(target_count, 1u);
      check_size_eq(capture.calls, 1u);
      check_false(capture.active);
      check_uint_eq(capture.connection_id, 0u);
      check_not_null(capture.plan);
      check_size_eq(flowie_cluster_session_delivery_plan_command(capture.plan)->mutation_count,
                    0u);
      check_int_eq(flowie_cluster_session_delivery_plan_finalize(&capture.plan, TURBO_OK),
                   TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       recovered,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &recovered_snapshot, NULL),
                   TURBO_OK);
      check_uint_eq(recovered_snapshot.resource_generation, durable_delivery_revision);
      check_size_eq(recovered_snapshot.inflight_count, 1u);
      recovered_test.expected_revision = recovered_snapshot.resource_generation;
      command = flowie_cluster_session_bind_test_command(connect_payload, 9u);
      check_int_eq(flowie_cluster_session_bind_execute_async(
                       recovered, &command, flowie_cluster_session_bind_test_complete,
                       &recovered_test),
                   TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_test_finalize(&recovered_test, TURBO_OK, &reply),
                   TURBO_OK);
      tstr_freep(&reply);
      check_uint_eq(flowie_cluster_peer_wire_read_u64(
                        (const uint8_t *)recovered_test.fact_value + 24u),
                    2u);
      memset(&capture, 0, sizeof(capture));
      capture.bind = recovered;
      capture.owner = &owner;
      capture.source = &broadcast;
      capture.event_digest = event_digest;
      check_int_eq(flowie_cluster_session_bind_match_publish(
                       recovered, &publish_event, flowie_cluster_session_bind_capture_target,
                       &capture, &target_count),
                   TURBO_OK);
      check_not_null(capture.plan);
      {
        flowie_cluster_delivery_action_view_t recovered_action =
            FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
        const flowie_cluster_pgsql_fact_command_t *recovered_delivery =
            flowie_cluster_session_delivery_plan_command(capture.plan);
        check_int_eq(flowie_cluster_delivery_action_decode(
                         recovered_delivery->mutations[0].event_payload,
                         recovered_delivery->mutations[0].event_payload_size,
                         recovered_config.max_event_payload_size, &recovered_action),
                     TURBO_OK);
        check_uint_eq(recovered_action.edge_action.action_sequence, 3u);
      }
      flowie_cluster_session_delivery_plan_destroy(&capture.plan);

      check_int_eq(flowie_cluster_session_bind_snapshot(
                       recovered,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &recovered_snapshot, NULL),
                   TURBO_OK);
      recovered_test.expected_revision = recovered_snapshot.resource_generation;
      recovered_test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER;
      command = flowie_cluster_session_bind_test_command(connect_payload, 10u);
      command.connection_generation += 1u;
      check_int_eq(flowie_cluster_session_bind_execute_async(
                       recovered, &command, flowie_cluster_session_bind_test_complete,
                       &recovered_test),
                   TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_test_finalize(&recovered_test, TURBO_OK, &reply),
                   TURBO_OK);
      tstr_freep(&reply);
      check_uint_eq(flowie_cluster_peer_wire_read_u64(
                        (const uint8_t *)recovered_test.fact_value + 24u),
                    0u);
      memset(&capture, 0, sizeof(capture));
      capture.bind = recovered;
      capture.owner = &owner;
      capture.source = &broadcast;
      capture.event_digest = event_digest;
      check_int_eq(flowie_cluster_session_bind_match_publish(
                       recovered, &publish_event, flowie_cluster_session_bind_capture_target,
                       &capture, &target_count),
                   TURBO_OK);
      check_not_null(capture.plan);
      {
        flowie_cluster_delivery_action_view_t reset_action =
            FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT;
        const flowie_cluster_pgsql_fact_command_t *reset_delivery =
            flowie_cluster_session_delivery_plan_command(capture.plan);
        check_int_eq(flowie_cluster_delivery_action_decode(
                         reset_delivery->mutations[0].event_payload,
                         reset_delivery->mutations[0].event_payload_size,
                         recovered_config.max_event_payload_size, &reset_action),
                     TURBO_OK);
        check_uint_eq(reset_action.edge_action.action_sequence, 1u);
      }
      flowie_cluster_session_delivery_plan_destroy(&capture.plan);
      flowie_cluster_session_recovery_destroy(recovery);
      check_int_eq(flowie_cluster_session_bind_destroy(recovered), TURBO_OK);
      flowie_cluster_session_bind_test_destroy(&recovered_test);
    }

    tstr_freep(&command_payload);
    test.expected_revision = after.resource_generation;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){unsubscribe, sizeof(unsubscribe)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_UNSUBSCRIBE, command_payload, 3u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_EIO, &reply), TURBO_EIO);
    check_null(reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_size_eq(after.subscription_count, 1u);
    check_uint_eq(after.resource_generation, test.expected_revision);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(flowie_cluster_session_bind_match_publish(
                     bind, &publish_event, flowie_cluster_session_bind_capture_target, &capture,
                     &target_count),
                 TURBO_OK);
    check_size_eq(target_count, 1u);
    check_size_eq(capture.calls, 1u);
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    command.correlation_id[0] = 4u;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    action = (flowie_cluster_peer_mqtt_reply_action_t)FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, FLOWIE_MQTT_PACKET_UNSUBACK);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_size_eq(after.subscription_count, 0u);
    memset(&capture, 0, sizeof(capture));
    check_int_eq(flowie_cluster_session_bind_match_publish(
                     bind, &publish_event, flowie_cluster_session_bind_capture_target, &capture,
                     &target_count),
                 TURBO_OK);
    check_size_eq(target_count, 0u);
    check_size_eq(capture.calls, 0u);

    tstr_freep(&command_payload);
    test.expected_revision = after.resource_generation;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){shared_subscribe, sizeof(shared_subscribe)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, command_payload, 40u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    memset(&capture, 0, sizeof(capture));
    capture.bind = bind;
    capture.owner = &owner;
    capture.source = &broadcast;
    capture.event_digest = event_digest;
    check_int_eq(flowie_cluster_session_bind_match_publish(
                     bind, &publish_event, flowie_cluster_session_bind_capture_target, &capture,
                     &target_count),
                 TURBO_OK);
    check_size_eq(target_count, 1u);
    check_size_eq(capture.calls, 1u);
    check_str_eq(capture.shared_filter, "$share/g/a/#");
    check_not_null(capture.plan);
    check_not_null(flowie_cluster_session_delivery_plan_command(capture.plan)->dedupe);
    check_size_eq(
        flowie_cluster_session_delivery_plan_command(capture.plan)->dedupe->shared_filter_size,
        sizeof("$share/g/a/#") - 1u);
    check_mem_eq(flowie_cluster_session_delivery_plan_command(capture.plan)->dedupe->shared_filter,
                 "$share/g/a/#", sizeof("$share/g/a/#") - 1u);
    flowie_cluster_session_delivery_plan_destroy(&capture.plan);

    tstr_freep(&command_payload);
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){pubrel, sizeof(pubrel)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_ACK, command_payload, 5u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_not_null(test.finalize);
    check_int_eq(test.finalize(test.finalize_ctx, test.complete_status, &reply), TURBO_OK);
    action = (flowie_cluster_peer_mqtt_reply_action_t)FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, FLOWIE_MQTT_PACKET_PUBCOMP);
    check_int_eq(action.settlement_point, 0);
    tstr_freep(&reply);

    tstr_freep(&command_payload);
    test.expected_revision = after.resource_generation;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){disconnect, sizeof(disconnect)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_DISCONNECT, command_payload, 6u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    action = (flowie_cluster_peer_mqtt_reply_action_t)FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_true(action.close_after_send);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_false(after.active);

    command.connection_generation += 1u;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tstr_free(publish_event_payload);
    tstr_free(durable_delivery_fact);
    tstr_free(command_payload);
    tstr_free(connect_payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("persists QoS zero events without a fact write and atomically settles received QoS one") {
    static const uint8_t qos0_publish[] = {0x30u, 0x08u, 0x00u, 0x03u, 'a',
                                           '/',   'b',   0x00u, 'o',   'k'};
    static const uint8_t qos1_publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x07u, 0x00u, 'o',   'k'};
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    flowie_cluster_publish_event_view_t event = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    tstr_t connect_payload = NULL;
    tstr_t command_payload = NULL;
    tstr_t reply = NULL;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&connect_payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(connect_payload, 1u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &before, NULL),
                 TURBO_OK);

    test.expected_revision = before.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH;
    test.expected_event_only = 1;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){qos0_publish, sizeof(qos0_publish)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, command_payload, 2u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, 0);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(flowie_cluster_publish_event_decode(
                     test.event_payload, tstr_len(test.event_payload), config.max_event_payload_size,
                     &event),
                 TURBO_OK);
    check_int_eq(event.requested_settlement, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED);
    check_int_eq(event.publish.packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
    tstr_freep(&reply);
    tstr_freep(&command_payload);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_uint_eq(after.resource_generation, before.resource_generation);

    test.expected_revision = after.resource_generation;
    test.expected_event_only = 0;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){qos1_publish, sizeof(qos1_publish)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &command_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, command_payload, 3u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    action = (flowie_cluster_peer_mqtt_reply_action_t)FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, FLOWIE_MQTT_PACKET_PUBACK);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    event = (flowie_cluster_publish_event_view_t)FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    check_int_eq(flowie_cluster_publish_event_decode(
                     test.event_payload, tstr_len(test.event_payload), config.max_event_payload_size,
                     &event),
                 TURBO_OK);
    check_int_eq(event.requested_settlement, TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_true(after.resource_generation > before.resource_generation);
    check_size_eq(after.inflight_count, 0u);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tstr_free(reply);
    tstr_free(command_payload);
    tstr_free(connect_payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("durably advances owner inflight state only after a fenced graph settlement") {
    static const uint8_t qos1_publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                           0x00u, 0x2au, 0x00u, 'o',   'k'};
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    turbo_flow_protocol_settlement_request_t settlement =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
    tstr_t connect_payload = NULL;
    tstr_t publish_payload = NULL;
    tstr_t settle_payload = NULL;
    tstr_t reply = NULL;

    config.session.settlement.qos1 = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&connect_payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(connect_payload, 31u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_OK);

    test.expected_revision = snapshot.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH;
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     (flowie_mqtt_span_t){qos1_publish, sizeof(qos1_publish)},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &publish_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, publish_payload, 32u);
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, 0);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_OK);
    check_size_eq(snapshot.inflight_count, 1u);

    settlement.message.protocol = TURBO_FLOW_PROTOCOL_MQTT;
    settlement.message.protocol_version = FLOWIE_MQTT_VERSION_5;
    settlement.message.kind = TURBO_FLOW_PROTOCOL_MESSAGE_DATA;
    settlement.message.qos = 1u;
    settlement.message.packet_id = 42u;
    settlement.message.session_generation = snapshot.session_generation;
    settlement.point = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
    settlement.status = TURBO_OK;
    settlement.message_id = 91u;
    settlement.attempt = 1u;
    ++settlement.message.session_generation;
    check_int_eq(flowie_cluster_peer_publish_settle_encode(
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &settlement, FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &settle_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE, settle_payload, 33u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_EBUSY);
    tstr_freep(&settle_payload);
    --settlement.message.session_generation;
    check_int_eq(flowie_cluster_peer_publish_settle_encode(
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &settlement, FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &settle_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE, settle_payload, 34u);
    test.expected_revision = snapshot.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_UPDATED;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    action = (flowie_cluster_peer_mqtt_reply_action_t)FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, FLOWIE_MQTT_PACKET_PUBACK);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_OK);
    check_size_eq(snapshot.inflight_count, 0u);

    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tstr_free(reply);
    tstr_free(settle_payload);
    tstr_free(publish_payload);
    tstr_free(connect_payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("durably marks abnormal connection loss and preserves a pending Will") {
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_peer_mqtt_reply_action_t action = FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    tstr_t connect_payload = NULL;
    tstr_t lost_payload = NULL;
    tstr_t reply = NULL;
    int submit_calls;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_will_payload(&connect_payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(connect_payload, 20u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &before, NULL),
                 TURBO_OK);
    check_true(before.active);
    check_true(before.has_will);
    check_false(before.will_pending);

    check_int_eq(flowie_cluster_peer_connection_lost_encode(
                     FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &lost_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST, lost_payload, 21u);
    test.expected_revision = before.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_true(after.active);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_EIO, &reply), TURBO_EIO);
    check_null(reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_true(after.active);
    check_uint_eq(after.resource_generation, before.resource_generation);

    command.correlation_id[0] = 22u;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.packet.type, 0);
    check_false(action.close_after_send);
    check_int_eq(action.settlement_point, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_false(after.active);
    check_true(after.has_will);
    check_true(after.will_pending);
    check_true(after.resource_generation > before.resource_generation);

    submit_calls = test.submit_calls;
    command.correlation_id[0] = 23u;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(test.submit_calls, submit_calls);
    check_not_null(test.finalize);
    check_int_eq(test.finalize(test.finalize_ctx, test.complete_status, &reply), TURBO_OK);
    action = (flowie_cluster_peer_mqtt_reply_action_t)FLOWIE_CLUSTER_PEER_MQTT_REPLY_ACTION_INIT;
    check_int_eq(flowie_cluster_peer_mqtt_reply_decode(
                     reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &action),
                 TURBO_OK);
    check_int_eq(action.settlement_point, 0);
    tstr_freep(&reply);

    command.connection_generation += 1u;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_EBUSY);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tstr_free(lost_payload);
    tstr_free(connect_payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("materializes a pending Will before deleting the expired session fact") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t edge_node_id[] = "edge-a";
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_session_lifecycle_plan_t *plan = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_cluster_owner_token_t owner = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_lifecycle_event_view_t event = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_session_snapshot_t connected = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t disconnected = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after_will = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_publish_event_view_t publish_event = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    const flowie_cluster_pgsql_fact_command_t *durable;
    const flowie_cluster_pgsql_fact_mutation_t *mutation;
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {7u};
    uint8_t event_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};
    tstr_t connect_payload = NULL;
    tstr_t lost_payload = NULL;
    tstr_t reply = NULL;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_will_payload(&connect_payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(connect_payload, 40u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &connected,
                     NULL),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_connection_lost_encode(
                     FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &lost_payload),
                 TURBO_OK);
    command = flowie_cluster_session_bind_test_packet_command(
        FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST, lost_payload, 41u);
    test.expected_revision = connected.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
                     &disconnected, NULL),
                 TURBO_OK);
    check_false(disconnected.active);
    check_true(disconnected.will_pending);

    check_int_eq(flowie_cluster_owner_token_init(&owner, 3u, 18u, "owner-a", 7u, owner_boot),
                 TURBO_OK);
    event.command_id[0] = 9u;
    event.shard_id = 3u;
    event.event_owner_epoch = 17u;
    event.expected_fact_revision = disconnected.resource_generation;
    event.created_at_epoch_seconds = 1000u;
    event.connection_id = 21u;
    event.connection_generation = 2u;
    event.session_id = disconnected.session_id;
    event.session_generation = disconnected.session_generation;
    event.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    event.edge_node_id =
        (flowie_mqtt_span_t){edge_node_id, sizeof(edge_node_id) - 1u};
    event.edge_boot_id = event_boot;
    check_int_eq(flowie_cluster_session_lifecycle_plan_create(bind, &owner, &event, 1000u,
                                                              &plan),
                 TURBO_OK);
    durable = flowie_cluster_session_lifecycle_plan_command(plan);
    check_not_null(durable);
    check_size_eq(durable->mutation_count, 1u);
    mutation = &durable->mutations[0];
    check_int_eq(mutation->write_kind, FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT);
    check_int_eq(mutation->record.kind, TURBO_FLOW_RECORD_PUT);
    check_uint_eq(mutation->record.expected_revision, disconnected.resource_generation);
    check_true(mutation->record.next_revision > mutation->record.expected_revision);
    check_int_eq(mutation->event_type, FLOWIE_CLUSTER_SESSION_EVENT_PUBLISH);
    check_int_eq(flowie_cluster_publish_event_decode(
                     mutation->event_payload, mutation->event_payload_size,
                     config.max_event_payload_size, &publish_event),
                 TURBO_OK);
    check_int_eq(flowie_mqtt_publish_parse(&publish_event.publish.packet, &publish),
                 FLOWIE_MQTT_PARSE_OK);
    check_mem_eq(publish.topic.data, "status/device-a", publish.topic.size);
    check_mem_eq(publish.payload.data, "offline", publish.payload.size);
    check_int_eq(flowie_cluster_session_lifecycle_plan_finalize(&plan, TURBO_EALREADY),
                 TURBO_EBUSY);
    check_null(plan);
    check_int_eq(test.fence_calls, 1);
    check_int_eq(test.fence_reason, TURBO_EALREADY);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &after_will,
                     NULL),
                 TURBO_OK);
    check_true(after_will.will_pending);
    check_uint_eq(after_will.resource_generation, disconnected.resource_generation);
    check_int_eq(flowie_cluster_session_lifecycle_plan_create(bind, &owner, &event, 1000u,
                                                              &plan),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_lifecycle_plan_finalize(&plan, TURBO_OK), TURBO_EBUSY);
    check_null(plan);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &after_will,
                     NULL),
                 TURBO_OK);
    check_false(after_will.has_will);
    check_false(after_will.will_pending);
    check_true(after_will.resource_generation > disconnected.resource_generation);

    check_int_eq(flowie_cluster_session_lifecycle_plan_create(bind, &owner, &event, 1000u,
                                                              &plan),
                 TURBO_OK);
    durable = flowie_cluster_session_lifecycle_plan_command(plan);
    check_not_null(durable);
    check_size_eq(durable->mutation_count, 1u);
    mutation = &durable->mutations[0];
    check_int_eq(mutation->write_kind, FLOWIE_CLUSTER_PGSQL_FACT_ONLY);
    check_int_eq(mutation->record.kind, TURBO_FLOW_RECORD_DELETE);
    check_uint_eq(mutation->record.expected_revision, after_will.resource_generation);
    check_uint_eq(mutation->record.next_revision, TURBO_FLOW_RECORD_REVISION_ABSENT);
    check_int_eq(flowie_cluster_session_lifecycle_plan_finalize(&plan, TURBO_OK), TURBO_OK);
    check_null(plan);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &after_will,
                     NULL),
                 TURBO_ENOENT);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tstr_free(lost_payload);
    tstr_free(connect_payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("publishes the previous edge binding as one durable takeover intent") {
    static const uint8_t old_edge_node[] = "edge-a";
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    tstr_t payload = NULL;
    tstr_t reply = NULL;
    const uint8_t *event;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(payload, 40u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &before, NULL),
                 TURBO_OK);

    test.expected_revision = before.resource_generation;
    test.expected_event_type = FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    command = flowie_cluster_session_bind_test_command(payload, 41u);
    command.source_node_id = tstr_v_from_cstr("edge-b");
    command.source_boot_id[0] = 3u;
    command.connection_id = 31u;
    command.connection_generation = 4u;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_not_null(test.event_payload);
    check_size_eq(tstr_len(test.event_payload),
                  FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE + sizeof(old_edge_node) - 1u);
    event = (const uint8_t *)test.event_payload;
    check_mem_eq(event, "TFTE", 4u);
    check_int_eq(event[12], FLOWIE_MQTT_VERSION_5);
    check_uint_eq(flowie_cluster_peer_wire_read_u64(event + 16u), 21u);
    check_uint_eq(flowie_cluster_peer_wire_read_u64(event + 24u), 2u);
    check_int_eq(event[48], 1u);
    check_uint_eq(flowie_cluster_peer_wire_read_u16(event + 64u), sizeof(old_edge_node) - 1u);
    check_mem_eq(event + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, old_edge_node,
                 sizeof(old_edge_node) - 1u);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &after, NULL),
                 TURBO_OK);
    check_true(after.active);
    check_true(after.session_generation > before.session_generation);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tstr_free(payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("publishes only a durable session revision and preserves it on PostgreSQL failure") {
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_cluster_session_bind_reply_view_t decoded_reply =
        FLOWIE_CLUSTER_SESSION_BIND_REPLY_VIEW_INIT;
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    tstr_t payload = NULL;
    tstr_t reply = NULL;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(payload, 1u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, &principal),
                 TURBO_ENOENT);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    check_not_null(reply);
    check_int_eq(
        flowie_cluster_session_bind_reply_decode(
            reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &decoded_reply),
        TURBO_OK);
    check_true(decoded_reply.accepted);
    check_uint_eq(decoded_reply.route.owner_instance_id, 9u);
    check_uint_eq(decoded_reply.route.session_id, 11u);
    check_uint_eq(decoded_reply.packet.data[0], UINT8_C(0x20));
    ((uint8_t *)reply)[12] = UINT8_C(0x80);
    decoded_reply =
        (flowie_cluster_session_bind_reply_view_t)FLOWIE_CLUSTER_SESSION_BIND_REPLY_VIEW_INIT;
    check_int_eq(
        flowie_cluster_session_bind_reply_decode(
            reply, tstr_len(reply), FLOWIE_CLUSTER_SESSION_BIND_TEST_MAX_PAYLOAD, &decoded_reply),
        TURBO_EPROTO);
    ((uint8_t *)reply)[12] = 0u;
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, &principal),
                 TURBO_OK);
    check_true(snapshot.active);
    check_uint_eq(snapshot.session_id, 11u);
    check_uint_eq(snapshot.resource_generation, test.next_revision);
    check_str_eq(principal.principal_id, "writer");

    {
      flowie_cluster_session_bind_test_t recovered_test = {0};
      flowie_cluster_session_bind_config_t recovered_config =
          flowie_cluster_session_bind_test_config(&recovered_test);
      flowie_cluster_session_bind_t *recovered_bind = NULL;
      flowie_cluster_session_recovery_t *recovery = NULL;
      turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
      flowie_session_snapshot_t recovered_snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
      turbo_flow_security_principal_t recovered_principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
      flowie_session_snapshot_t next_snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
      flowie_cluster_peer_frame_t next_command;
      tstr_t next_payload = NULL;
      tstr_t next_reply = NULL;
      record.key = (const uint8_t *)test.fact_key;
      record.key_size = tstr_len(test.fact_key);
      record.revision = test.next_revision;
      record.value = (const uint8_t *)test.fact_value;
      record.value_size = tstr_len(test.fact_value);
      check_int_eq(flowie_cluster_session_bind_create(&recovered_config, &recovered_bind),
                   TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_create(recovered_bind, &recovery), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       recovered_bind,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &recovered_snapshot, &recovered_principal),
                   TURBO_OK);
      check_false(recovered_snapshot.active);
      check_uint_eq(recovered_snapshot.session_id, snapshot.session_id);
      check_uint_eq(recovered_snapshot.resource_generation, snapshot.resource_generation);
      check_str_eq(recovered_principal.principal_id, principal.principal_id);
      recovered_test.expected_client = FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B;
      recovered_test.expected_client_size = sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B) - 1u;
      check_int_eq(flowie_cluster_session_bind_test_payload_for(recovered_test.expected_client,
                                                                recovered_test.expected_client_size,
                                                                &next_payload),
                   TURBO_OK);
      next_command = flowie_cluster_session_bind_test_command(next_payload, 9u);
      check_int_eq(flowie_cluster_session_bind_execute_async(
                       recovered_bind, &next_command, flowie_cluster_session_bind_test_complete,
                       &recovered_test),
                   TURBO_OK);
      check_int_eq(
          flowie_cluster_session_bind_test_finalize(&recovered_test, TURBO_OK, &next_reply),
          TURBO_OK);
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       recovered_bind,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B) - 1u},
                       &next_snapshot, NULL),
                   TURBO_OK);
      check_uint_eq(next_snapshot.session_id, recovered_snapshot.session_id + 1u);
      tstr_free(next_reply);
      tstr_free(next_payload);
      flowie_cluster_session_recovery_destroy(recovery);
      check_int_eq(flowie_cluster_session_bind_destroy(recovered_bind), TURBO_OK);
      flowie_cluster_session_bind_test_destroy(&recovered_test);
    }

    test.expected_revision = snapshot.resource_generation;
    test.finalize = NULL;
    test.finalize_ctx = NULL;
    command.correlation_id[0] = 2u;
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_EIO, &reply), TURBO_EIO);
    check_null(reply);
    {
      flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       bind,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &after, NULL),
                   TURBO_OK);
      check_uint_eq(after.resource_generation, snapshot.resource_generation);
      check_uint_eq(after.session_generation, snapshot.session_generation);
    }
    check_int_eq(test.fence_calls, 0);
    tstr_free(payload);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("keeps a new session absent when the bounded fact worker rejects admission") {
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    tstr_t payload = NULL;
    test.submit_rc = TURBO_ENOSPC;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(payload, 4u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_ENOSPC);
    check_int_eq(test.submit_calls, 1);
    check_int_eq(test.complete_calls, 0);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_ENOENT);
    check_int_eq(test.fence_calls, 0);
    tstr_free(payload);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("self-fences when durable completion cannot return to the owner lane") {
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_peer_frame_t command;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    tstr_t payload = NULL;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_EIO;
    check_int_eq(flowie_cluster_session_bind_create(&config, &bind), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(payload, 3u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     bind, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, NULL), TURBO_EIO);
    check_int_eq(test.fence_calls, 1);
    check_int_eq(test.fence_reason, TURBO_EIO);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_ENOENT);
    tstr_free(payload);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    flowie_cluster_session_bind_test_destroy(&test);
  }

  it("rejects malformed or duplicate facts without partially publishing recovery") {
    flowie_cluster_session_bind_test_t test = {0};
    flowie_cluster_session_bind_config_t config = flowie_cluster_session_bind_test_config(&test);
    flowie_cluster_session_bind_t *source = NULL;
    flowie_cluster_session_bind_t *target = NULL;
    flowie_cluster_session_bind_t *legacy_target = NULL;
    flowie_cluster_session_recovery_t *recovery = NULL;
    flowie_cluster_peer_frame_t command;
    turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
    flowie_session_snapshot_t snapshot = FLOWIE_SESSION_SNAPSHOT_INIT;
    tstr_t payload = NULL;
    tstr_t reply = NULL;
    tstr_t malformed = NULL;
    tstr_t duplicate_session = NULL;
    tstr_t legacy_fact = NULL;
    test.submit_rc = TURBO_OK;
    test.complete_rc = TURBO_OK;
    check_int_eq(flowie_cluster_session_bind_create(&config, &source), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_payload(&payload), TURBO_OK);
    command = flowie_cluster_session_bind_test_command(payload, 5u);
    check_int_eq(flowie_cluster_session_bind_execute_async(
                     source, &command, flowie_cluster_session_bind_test_complete, &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_test_finalize(&test, TURBO_OK, &reply), TURBO_OK);
    tstr_freep(&reply);
    check_int_eq(flowie_cluster_session_bind_destroy(source), TURBO_OK);
    source = NULL;

    record.key = (const uint8_t *)test.fact_key;
    record.key_size = tstr_len(test.fact_key);
    record.revision = test.next_revision;
    record.value = (const uint8_t *)test.fact_value;
    record.value_size = tstr_len(test.fact_value);

    {
      const uint8_t *v2 = (const uint8_t *)test.fact_value;
      size_t bind_size = flowie_cluster_peer_wire_read_u32(v2 + 12u);
      size_t owner_size = flowie_cluster_peer_wire_read_u32(v2 + 16u);
      size_t legacy_size = FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1 + bind_size + owner_size;
      legacy_fact = tstr_new_len(NULL, legacy_size);
      check_not_null(legacy_fact);
      memcpy(legacy_fact, v2, FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1);
      flowie_cluster_peer_wire_write_u16((uint8_t *)legacy_fact + 4u,
                                         FLOWIE_CLUSTER_SESSION_FACT_VERSION_V1);
      flowie_cluster_peer_wire_write_u16((uint8_t *)legacy_fact + 6u,
                                         FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1);
      flowie_cluster_peer_wire_write_u32((uint8_t *)legacy_fact + 8u, (uint32_t)legacy_size);
      memcpy(legacy_fact + FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE_V1,
             v2 + FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE, bind_size + owner_size);
      record.value = (const uint8_t *)legacy_fact;
      record.value_size = tstr_len(legacy_fact);
      check_int_eq(flowie_cluster_session_bind_create(&config, &legacy_target), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_create(legacy_target, &recovery), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_OK);
      flowie_cluster_session_recovery_destroy(recovery);
      recovery = NULL;
      check_int_eq(flowie_cluster_session_bind_snapshot(
                       legacy_target,
                       (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                            sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                       &snapshot, NULL),
                   TURBO_OK);
      check_false(snapshot.active);
      check_int_eq(flowie_cluster_session_bind_destroy(legacy_target), TURBO_OK);
      legacy_target = NULL;
      record.value = (const uint8_t *)test.fact_value;
      record.value_size = tstr_len(test.fact_value);
    }

    check_int_eq(flowie_cluster_session_bind_create(&config, &target), TURBO_OK);
    malformed = tstr_new_len(test.fact_value, tstr_len(test.fact_value));
    check_not_null(malformed);
    flowie_cluster_peer_wire_write_u16((uint8_t *)malformed + 4u,
                                       FLOWIE_CLUSTER_SESSION_FACT_VERSION + 1u);
    record.value = (const uint8_t *)malformed;
    check_int_eq(flowie_cluster_session_recovery_create(target, &recovery), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_EPROTO);
    flowie_cluster_session_recovery_destroy(recovery);
    recovery = NULL;
    record.value = (const uint8_t *)test.fact_value;
    record.value_size = FLOWIE_CLUSTER_SESSION_FACT_HEADER_SIZE - 1u;
    check_int_eq(flowie_cluster_session_recovery_create(target, &recovery), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_EPROTO);
    flowie_cluster_session_recovery_destroy(recovery);
    recovery = NULL;
    tstr_freep(&malformed);
    record.value_size = tstr_len(test.fact_value);
    check_int_eq(flowie_cluster_session_recovery_create(target, &recovery), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_EPROTO);
    check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_EPROTO);
    flowie_cluster_session_recovery_destroy(recovery);
    recovery = NULL;
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     target,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_ENOENT);

    duplicate_session = tstr_new_len(test.fact_value, tstr_len(test.fact_value));
    check_not_null(duplicate_session);
    {
      turbo_flow_record_view_t duplicate = record;
      size_t replacements = 0u;
      for (size_t offset = 0u; offset + sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u <=
                               tstr_len(duplicate_session);
           ++offset) {
        if (memcmp(duplicate_session + offset, FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                   sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u) == 0) {
          memcpy(duplicate_session + offset, FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B,
                 sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B) - 1u);
          ++replacements;
        }
      }
      check_size_eq(replacements, 1u);
      duplicate.key = FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B;
      duplicate.key_size = sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT_B) - 1u;
      duplicate.value = (const uint8_t *)duplicate_session;
      check_int_eq(flowie_cluster_session_recovery_create(target, &recovery), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_OK);
      check_int_eq(flowie_cluster_session_recovery_visit(recovery, &duplicate), TURBO_EPROTO);
      check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_EPROTO);
      flowie_cluster_session_recovery_destroy(recovery);
      recovery = NULL;
    }

    malformed = tstr_new_len(test.fact_value, tstr_len(test.fact_value));
    check_not_null(malformed);
    ((uint8_t *)malformed)[20] ^= UINT8_C(1);
    record.value = (const uint8_t *)malformed;
    check_int_eq(flowie_cluster_session_recovery_create(target, &recovery), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_EPROTO);
    check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_EPROTO);
    flowie_cluster_session_recovery_destroy(recovery);
    recovery = NULL;
    record.value = (const uint8_t *)test.fact_value;
    check_int_eq(flowie_cluster_session_recovery_create(target, &recovery), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_visit(recovery, &record), TURBO_OK);
    check_int_eq(flowie_cluster_session_recovery_publish(recovery), TURBO_OK);
    {
      flowie_cluster_session_recovery_t *second = NULL;
      check_int_eq(flowie_cluster_session_recovery_create(target, &second), TURBO_EBUSY);
      check_null(second);
    }
    flowie_cluster_session_recovery_destroy(recovery);
    recovery = NULL;
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     target,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT,
                                          sizeof(FLOWIE_CLUSTER_SESSION_BIND_TEST_CLIENT) - 1u},
                     &snapshot, NULL),
                 TURBO_OK);
    check_false(snapshot.active);
    check_int_eq(flowie_cluster_session_bind_destroy(target), TURBO_OK);
    tstr_free(duplicate_session);
    tstr_free(malformed);
    tstr_free(legacy_fact);
    tstr_free(payload);
    flowie_cluster_session_bind_test_destroy(&test);
  }
}
