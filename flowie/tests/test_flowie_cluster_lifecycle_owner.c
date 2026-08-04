#include "flowie_cluster_lifecycle_owner_internal.h"

#include "flow_coronet_execution.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

typedef struct flowie_cluster_lifecycle_owner_test_s {
  int apply_calls;
  int apply_status;
  int submit_calls;
  int fence_calls;
  int fence_reason;
} flowie_cluster_lifecycle_owner_test_t;

static uint64_t flowie_cluster_lifecycle_owner_test_now(void *ctx) {
  (void)ctx;
  return UINT64_C(1000);
}

static void flowie_cluster_lifecycle_owner_test_fence(void *ctx, int reason) {
  flowie_cluster_lifecycle_owner_test_t *test = (flowie_cluster_lifecycle_owner_test_t *)ctx;
  ++test->fence_calls;
  test->fence_reason = reason;
}

static int flowie_cluster_lifecycle_owner_test_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  flowie_cluster_lifecycle_owner_test_t *test = (flowie_cluster_lifecycle_owner_test_t *)ctx;
  if (!test || !command || !completion || command->mutation_count != 1u) return TURBO_EINVAL;
  ++test->submit_calls;
  completion(completion_ctx, command->command_id, TURBO_OK);
  return TURBO_OK;
}

static int
flowie_cluster_lifecycle_owner_test_peer_complete(void *ctx, int durable_status,
                                                  flowie_cluster_peer_owner_finalize_fn finalize,
                                                  void *finalize_ctx) {
  tstr_t reply = NULL;
  int rc;
  (void)ctx;
  if (!finalize || !finalize_ctx) return TURBO_EINVAL;
  rc = finalize(finalize_ctx, durable_status, &reply);
  tstr_free(reply);
  return rc;
}

static void flowie_cluster_lifecycle_owner_test_apply_complete(void *ctx, int status) {
  flowie_cluster_lifecycle_owner_test_t *test = (flowie_cluster_lifecycle_owner_test_t *)ctx;
  ++test->apply_calls;
  test->apply_status = status;
}

static turbo_flow_security_principal_t flowie_cluster_lifecycle_owner_test_principal(void) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  (void)strcpy(principal.principal_id, "device-a");
  (void)strcpy(principal.principal_type, "device");
  (void)strcpy(principal.domain_id, "root-a");
  (void)strcpy(principal.auth_method, "token");
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_DOMAIN;
  principal.role_count = 1u;
  (void)strcpy(principal.roles[0], "writer");
  principal.group_count = 1u;
  (void)strcpy(principal.groups[0], "root-a");
  principal.policy_version = 1u;
  return principal;
}

static int flowie_cluster_lifecycle_owner_test_connect_payload(tstr_t *out) {
  static const uint8_t empty_property = 0u;
  static const uint8_t client_id[] = "device-a";
  static const uint8_t will_topic[] = "status/device-a";
  static const uint8_t will_payload[] = "offline";
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  turbo_flow_security_principal_t principal = flowie_cluster_lifecycle_owner_test_principal();
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.properties.values.data = &empty_property;
  connect.will_qos = 1u;
  connect.will_topic = (flowie_mqtt_span_t){will_topic, sizeof(will_topic) - 1u};
  connect.will_payload = (flowie_mqtt_span_t){will_payload, sizeof(will_payload) - 1u};
  connect.will_properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.will_properties.values.data = &empty_property;
  return flowie_cluster_peer_connect_bind_encode(&connect, &principal, 2048u, out);
}

static flowie_cluster_peer_frame_t
flowie_cluster_lifecycle_owner_test_command(flowie_cluster_peer_operation_t operation,
                                            tstr_t payload, uint8_t correlation) {
  flowie_cluster_peer_frame_t command = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  command.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  command.operation = operation;
  command.shard_id = 3u;
  command.owner_epoch = 17u;
  command.connection_id = 21u;
  command.connection_generation = 2u;
  command.source_node_id = tstr_v_from_cstr("edge-a");
  command.target_node_id = tstr_v_from_cstr("owner-a");
  command.source_boot_id[0] = 1u;
  command.target_boot_id[0] = 2u;
  command.correlation_id[0] = correlation;
  command.payload = tstr_to_v(payload);
  return command;
}

static int flowie_cluster_lifecycle_owner_test_session(flowie_cluster_lifecycle_owner_test_t *test,
                                                       flowie_cluster_session_bind_t **out,
                                                       flowie_session_snapshot_t *snapshot) {
  static const uint8_t client_id[] = "device-a";
  flowie_cluster_session_bind_config_t config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  flowie_cluster_peer_frame_t command;
  tstr_t payload = NULL;
  int rc;
  config.max_sessions = 4u;
  config.max_bind_payload_size = 2048u;
  config.max_fact_value_size = 8192u;
  config.max_event_payload_size = 1024u;
  config.session.owner_instance_id = 9u;
  config.session.max_subscriptions = 8u;
  config.session.max_inflight = 8u;
  config.first_session_id = 10u;
  config.security_enabled = 1u;
  config.submit = flowie_cluster_lifecycle_owner_test_submit;
  config.submit_ctx = test;
  config.now = flowie_cluster_lifecycle_owner_test_now;
  config.self_fence = flowie_cluster_lifecycle_owner_test_fence;
  config.self_fence_ctx = test;
  rc = flowie_cluster_session_bind_create(&config, out);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_lifecycle_owner_test_connect_payload(&payload);
  if (rc == TURBO_OK) {
    command = flowie_cluster_lifecycle_owner_test_command(
        FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND, payload, 1u);
    rc = flowie_cluster_session_bind_execute_async(
        *out, &command, flowie_cluster_lifecycle_owner_test_peer_complete, test);
  }
  tstr_freep(&payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_peer_connection_lost_encode(
      FLOWIE_MQTT_VERSION_5, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, 2048u,
      &payload);
  if (rc == TURBO_OK) {
    command = flowie_cluster_lifecycle_owner_test_command(
        FLOWIE_CLUSTER_PEER_OPERATION_CONNECTION_LOST, payload, 2u);
    rc = flowie_cluster_session_bind_execute_async(
        *out, &command, flowie_cluster_lifecycle_owner_test_peer_complete, test);
  }
  tstr_free(payload);
  if (rc != TURBO_OK) return rc;
  return flowie_cluster_session_bind_snapshot(
      *out, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, snapshot, NULL);
}

static void flowie_cluster_lifecycle_owner_test_run(coro_context_t *context) {
  check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
  check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
}

spec("flowie cluster lifecycle owner-lane applicator") {
  it("commits Will publication before the same TFLE expires the session") {
    static const uint8_t client_id[] = "device-a";
    static const uint8_t edge_node_id[] = "edge-a";
    flowie_cluster_lifecycle_owner_test_t test = {0};
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_lifecycle_owner_t *owner = NULL;
    flowie_cluster_lifecycle_owner_config_t owner_config =
        FLOWIE_CLUSTER_LIFECYCLE_OWNER_CONFIG_INIT;
    flowie_cluster_lifecycle_owner_snapshot_t owner_snapshot =
        FLOWIE_CLUSTER_LIFECYCLE_OWNER_SNAPSHOT_INIT;
    flowie_cluster_owner_token_t token = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
    flowie_cluster_lifecycle_event_view_t event = FLOWIE_CLUSTER_LIFECYCLE_EVENT_VIEW_INIT;
    flowie_session_snapshot_t session = FLOWIE_SESSION_SNAPSHOT_INIT;
    coro_context_t *context = NULL;
    tf_coronet_execution_t execution;
    turbo_flow_coronet_execution_binding_t binding = {0};
    uint8_t owner_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {2u};
    uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};

    check_int_eq(flowie_cluster_lifecycle_owner_test_session(&test, &bind, &session), TURBO_OK);
    check_false(session.active);
    check_true(session.will_pending);
    context = coro_context_create(NULL);
    check_not_null(context);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    binding.context = context;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    owner_config.shard_id = 3u;
    owner_config.execution = &execution;
    owner_config.session_bind = bind;
    owner_config.submit = flowie_cluster_lifecycle_owner_test_submit;
    owner_config.submit_ctx = &test;
    owner_config.now = flowie_cluster_lifecycle_owner_test_now;
    owner_config.self_fence = flowie_cluster_lifecycle_owner_test_fence;
    owner_config.self_fence_ctx = &test;
    check_int_eq(flowie_cluster_lifecycle_owner_create(&owner_config, &owner), TURBO_OK);
    check_int_eq(flowie_cluster_owner_token_init(&token, 3u, 18u, "owner-a", 7u, owner_boot),
                 TURBO_OK);
    event.command_id[0] = 9u;
    event.shard_id = 3u;
    event.event_owner_epoch = 17u;
    event.expected_fact_revision = session.resource_generation;
    event.created_at_epoch_seconds = 1000u;
    event.connection_id = 21u;
    event.connection_generation = 2u;
    event.session_id = session.session_id;
    event.session_generation = session.session_generation;
    event.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
    event.edge_node_id = (flowie_mqtt_span_t){edge_node_id, sizeof(edge_node_id) - 1u};
    event.edge_boot_id = edge_boot;

    check_int_eq(
        flowie_cluster_lifecycle_owner_apply(
            owner, &token, &event, flowie_cluster_lifecycle_owner_test_apply_complete, &test),
        TURBO_OK);
    flowie_cluster_lifecycle_owner_test_run(context);
    check_int_eq(test.apply_calls, 1);
    check_int_eq(test.apply_status, TURBO_EBUSY);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &session, NULL),
                 TURBO_OK);
    check_false(session.has_will);
    check_false(session.will_pending);

    check_int_eq(
        flowie_cluster_lifecycle_owner_apply(
            owner, &token, &event, flowie_cluster_lifecycle_owner_test_apply_complete, &test),
        TURBO_OK);
    flowie_cluster_lifecycle_owner_test_run(context);
    check_int_eq(test.apply_calls, 2);
    check_int_eq(test.apply_status, TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, &session, NULL),
                 TURBO_ENOENT);
    check_int_eq(flowie_cluster_lifecycle_owner_snapshot(owner, &owner_snapshot), TURBO_OK);
    check_uint_eq(owner_snapshot.admitted_events, 2u);
    check_uint_eq(owner_snapshot.completed_events, 1u);
    check_uint_eq(owner_snapshot.durable_transactions, 2u);
    check_int_eq(test.fence_calls, 0);
    check_int_eq(flowie_cluster_lifecycle_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_lifecycle_owner_drain(owner, UINT64_MAX), TURBO_OK);
    check_int_eq(flowie_cluster_lifecycle_owner_destroy(owner), TURBO_OK);
    check_int_eq(flowie_cluster_session_bind_destroy(bind), TURBO_OK);
    tf_coronet_execution_destroy(&execution);
    coro_context_destroy(context);
  }
}
