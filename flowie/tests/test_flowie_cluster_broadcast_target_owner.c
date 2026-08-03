#include "flowie_cluster_broadcast_target_owner_internal.h"

#include "flow_coronet_execution.h"
#include "tinytest.h"

#include <string.h>

enum {
  TARGET_OWNER_TEST_MAX_PAYLOAD = 2048u,
};

typedef struct flowie_cluster_target_owner_test_s {
  flowie_cluster_owner_token_t owner;
  flowie_cluster_pgsql_fact_completion_fn pending_completion;
  void *pending_ctx;
  uint8_t pending_command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  int durable_status;
  int auto_complete;
  size_t submit_calls;
  size_t marker_calls;
  size_t target_calls;
  size_t apply_completions;
  int apply_status;
} flowie_cluster_target_owner_test_t;

static int flowie_cluster_target_owner_test_resolve(
    void *ctx, uint32_t shard_id, flowie_cluster_owner_token_t *out) {
  flowie_cluster_target_owner_test_t *test =
      (flowie_cluster_target_owner_test_t *)ctx;
  if (!test || !out || shard_id != test->owner.shard_id) return TURBO_EBUSY;
  *out = test->owner;
  return TURBO_OK;
}

static int flowie_cluster_target_owner_test_submit(
    void *ctx, const flowie_cluster_pgsql_fact_command_t *command,
    flowie_cluster_pgsql_fact_completion_fn completion, void *completion_ctx) {
  flowie_cluster_target_owner_test_t *test =
      (flowie_cluster_target_owner_test_t *)ctx;
  if (!test || !command || !completion || command->mutation_count > 1u ||
      (command->mutation_count != 0u && !command->mutations) ||
      command->owner.shard_id != test->owner.shard_id ||
      command->owner.owner_epoch != test->owner.owner_epoch)
    return TURBO_EPROTO;
  test->submit_calls += 1u;
  if (command->mutation_count == 0u) {
    if (!command->dedupe || command->dedupe->target_session_id != 0u) return TURBO_EPROTO;
    test->marker_calls += 1u;
  } else if (command->mutations[0].event_type ==
             FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE) {
    if (!command->dedupe || command->dedupe->target_session_id == 0u) return TURBO_EPROTO;
    test->target_calls += 1u;
  }
  if (test->auto_complete) {
    completion(completion_ctx, command->command_id, test->durable_status);
  } else {
    test->pending_completion = completion;
    test->pending_ctx = completion_ctx;
    memcpy(test->pending_command_id, command->command_id, sizeof(test->pending_command_id));
  }
  return TURBO_OK;
}

static uint64_t flowie_cluster_target_owner_test_now(void *ctx) {
  (void)ctx;
  return UINT64_C(1000);
}

static void flowie_cluster_target_owner_test_fence(void *ctx, int reason) {
  (void)ctx;
  (void)reason;
}

static void flowie_cluster_target_owner_test_complete(void *ctx, int status) {
  flowie_cluster_target_owner_test_t *test =
      (flowie_cluster_target_owner_test_t *)ctx;
  test->apply_completions += 1u;
  test->apply_status = status;
}

static int flowie_cluster_target_owner_test_peer_complete(
    void *ctx, int durable_status, flowie_cluster_peer_owner_finalize_fn finalize,
    void *finalize_ctx) {
  flowie_cluster_target_owner_test_t *test =
      (flowie_cluster_target_owner_test_t *)ctx;
  tstr_t reply = NULL;
  int rc;
  if (!test || !finalize || !finalize_ctx) return TURBO_EPROTO;
  rc = finalize(finalize_ctx, durable_status, &reply);
  tstr_free(reply);
  return rc;
}

static int flowie_cluster_target_owner_test_complete_pending(
    flowie_cluster_target_owner_test_t *test, int status) {
  flowie_cluster_pgsql_fact_completion_fn completion;
  void *completion_ctx;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  if (!test || !(completion = test->pending_completion)) return TURBO_EINVAL;
  completion_ctx = test->pending_ctx;
  memcpy(command_id, test->pending_command_id, sizeof(command_id));
  test->pending_completion = NULL;
  test->pending_ctx = NULL;
  completion(completion_ctx, command_id, status);
  return TURBO_OK;
}

static flowie_cluster_session_bind_config_t flowie_cluster_target_owner_test_bind_config(
    flowie_cluster_target_owner_test_t *test) {
  flowie_cluster_session_bind_config_t config = FLOWIE_CLUSTER_SESSION_BIND_CONFIG_INIT;
  config.max_sessions = 4u;
  config.max_bind_payload_size = 1024u;
  config.max_fact_value_size = 4096u;
  config.max_event_payload_size = 1024u;
  config.session.owner_instance_id = 9u;
  config.session.max_subscriptions = 8u;
  config.session.max_inflight = 8u;
  config.first_session_id = 10u;
  config.security_enabled = 1u;
  config.submit = flowie_cluster_target_owner_test_submit;
  config.submit_ctx = test;
  config.now = flowie_cluster_target_owner_test_now;
  config.self_fence = flowie_cluster_target_owner_test_fence;
  config.self_fence_ctx = test;
  return config;
}

static turbo_flow_coronet_execution_binding_t flowie_cluster_target_owner_test_binding(
    coro_context_t *context) {
  turbo_flow_coronet_execution_binding_t binding = {0};
  binding.size = sizeof(binding);
  binding.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
  binding.context = context;
  return binding;
}

static int flowie_cluster_target_owner_test_event(tstr_t *out) {
  static const uint8_t client_id[] = "publisher-a";
  static const uint8_t publish[] = {0x32u, 0x08u, 0x00u, 0x01u, 'a',
                                    0x00u, 0x07u, 0x00u, 'o',   'k'};
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  uint8_t edge_boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {1u};
  int rc;
  event.command_id[0] = 0x31u;
  event.event_index = 2u;
  event.shard_id = 7u;
  event.event_owner_epoch = 11u;
  event.fact_revision = 13u;
  event.event_type = FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE;
  event.record_kind = FLOWIE_CLUSTER_KEY_SESSION;
  event.record_key = tstr_new_len(client_id, sizeof(client_id) - 1u);
  if (!event.record_key) return TURBO_ENOMEM;
  rc = flowie_cluster_publish_event_encode(
      FLOWIE_MQTT_VERSION_5, TURBO_FLOW_PROTOCOL_SETTLE_DURABLE, 3u, 4u, 5u, 6u, 1000u,
      tstr_v_from_cstr("edge-a"), edge_boot,
      (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
      (flowie_mqtt_span_t){publish, sizeof(publish)}, 1024u, &event.payload);
  if (rc == TURBO_OK)
    rc = flowie_cluster_broadcast_event_encode(&event, TARGET_OWNER_TEST_MAX_PAYLOAD, out);
  tstr_free(event.payload);
  tstr_free(event.record_key);
  return rc;
}

static turbo_flow_security_principal_t flowie_cluster_target_owner_test_principal(void) {
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

static int flowie_cluster_target_owner_test_connect_payload(tstr_t *out) {
  static const uint8_t empty_property = 0u;
  static const uint8_t client_id[] = "device-a";
  flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
  turbo_flow_security_principal_t principal = flowie_cluster_target_owner_test_principal();
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 0u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.properties.values.data = &empty_property;
  connect.will_properties =
      (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  connect.will_properties.values.data = &empty_property;
  return flowie_cluster_peer_connect_bind_encode(&connect, &principal, 1024u, out);
}

static flowie_cluster_peer_frame_t flowie_cluster_target_owner_test_command(
    flowie_cluster_peer_operation_t operation, tstr_t payload, uint8_t correlation) {
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

static int flowie_cluster_target_owner_test_bind_subscription(
    flowie_cluster_target_owner_test_t *test, flowie_cluster_session_bind_t *bind,
    flowie_session_snapshot_t *out) {
  static const uint8_t client_id[] = "device-a";
  static const uint8_t subscribe[] = {0x82u, 0x09u, 0x00u, 0x07u, 0x00u, 0x00u,
                                      0x03u, 'a',   '/',   '#',   0x01u};
  flowie_cluster_peer_frame_t command;
  tstr_t payload = NULL;
  int rc = flowie_cluster_target_owner_test_connect_payload(&payload);
  if (rc != TURBO_OK) return rc;
  command = flowie_cluster_target_owner_test_command(
      FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND, payload, 1u);
  rc = flowie_cluster_session_bind_execute_async(
      bind, &command, flowie_cluster_target_owner_test_peer_complete, test);
  if (rc == TURBO_OK) rc = flowie_cluster_target_owner_test_complete_pending(test, TURBO_OK);
  tstr_freep(&payload);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_peer_mqtt_command_encode(
      FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, FLOWIE_MQTT_VERSION_5,
      (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u},
      (flowie_mqtt_span_t){subscribe, sizeof(subscribe)}, 1024u, &payload);
  if (rc != TURBO_OK) return rc;
  command = flowie_cluster_target_owner_test_command(
      FLOWIE_CLUSTER_PEER_OPERATION_MQTT_SUBSCRIBE, payload, 2u);
  rc = flowie_cluster_session_bind_execute_async(
      bind, &command, flowie_cluster_target_owner_test_peer_complete, test);
  if (rc == TURBO_OK) rc = flowie_cluster_target_owner_test_complete_pending(test, TURBO_OK);
  tstr_free(payload);
  if (rc != TURBO_OK) return rc;
  return flowie_cluster_session_bind_snapshot(
      bind, (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u}, out, NULL);
}

static int flowie_cluster_target_owner_test_fixture(
    flowie_cluster_target_owner_test_t *test, coro_context_t **context,
    tf_coronet_execution_t *execution, flowie_cluster_session_bind_t **bind,
    flowie_cluster_broadcast_target_owner_t **owner) {
  flowie_cluster_session_bind_config_t bind_config;
  flowie_cluster_broadcast_target_owner_config_t owner_config =
      FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_CONFIG_INIT;
  turbo_flow_coronet_execution_binding_t binding;
  uint8_t boot[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {2u};
  int rc;
  memset(test, 0, sizeof(*test));
  test->durable_status = TURBO_OK;
  rc = flowie_cluster_owner_token_init(&test->owner, 3u, 17u, "owner-a", 7u, boot);
  if (rc != TURBO_OK) return rc;
  *context = coro_context_create(NULL);
  if (!*context) return TURBO_ENOMEM;
  binding = flowie_cluster_target_owner_test_binding(*context);
  rc = tf_coronet_execution_init(execution, &binding);
  if (rc != TURBO_OK) return rc;
  bind_config = flowie_cluster_target_owner_test_bind_config(test);
  rc = flowie_cluster_session_bind_create(&bind_config, bind);
  if (rc != TURBO_OK) return rc;
  owner_config.shard_id = test->owner.shard_id;
  owner_config.max_payload_size = TARGET_OWNER_TEST_MAX_PAYLOAD;
  owner_config.max_targets = bind_config.max_sessions;
  owner_config.execution = execution;
  owner_config.session_bind = *bind;
  owner_config.resolve = flowie_cluster_target_owner_test_resolve;
  owner_config.resolve_ctx = test;
  owner_config.submit = flowie_cluster_target_owner_test_submit;
  owner_config.submit_ctx = test;
  owner_config.self_fence = flowie_cluster_target_owner_test_fence;
  owner_config.self_fence_ctx = test;
  return flowie_cluster_broadcast_target_owner_create(&owner_config, owner);
}

static void flowie_cluster_target_owner_test_cleanup(
    coro_context_t *context, tf_coronet_execution_t *execution,
    flowie_cluster_session_bind_t *bind, flowie_cluster_broadcast_target_owner_t *owner) {
  if (owner) {
    (void)flowie_cluster_broadcast_target_owner_close(owner);
    (void)flowie_cluster_broadcast_target_owner_drain(owner, UINT64_MAX);
    (void)flowie_cluster_broadcast_target_owner_destroy(owner);
  }
  if (bind) (void)flowie_cluster_session_bind_destroy(bind);
  tf_coronet_execution_destroy(execution);
  if (context) coro_context_destroy(context);
}

spec("flowie cluster broadcast target owner applicator") {
  it("copies TFBE before owner-lane execution and commits a zero-target shard marker") {
    flowie_cluster_target_owner_test_t test;
    coro_context_t *context = NULL;
    tf_coronet_execution_t execution;
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_broadcast_target_owner_t *owner = NULL;
    flowie_cluster_broadcast_target_owner_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_SNAPSHOT_INIT;
    tstr_t event = NULL;

    check_int_eq(flowie_cluster_target_owner_test_fixture(&test, &context, &execution, &bind,
                                                          &owner),
                 TURBO_OK);
    check_int_eq(flowie_cluster_target_owner_test_event(&event), TURBO_OK);
    test.auto_complete = 1;
    check_int_eq(flowie_cluster_broadcast_target_owner_apply(
                     owner, event, tstr_len(event), flowie_cluster_target_owner_test_complete,
                     &test),
                 TURBO_OK);
    event[0] = 'X';
    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(test.apply_completions, 1);
    check_int_eq(test.apply_status, TURBO_OK);
    check_size_eq(test.submit_calls, 1u);
    check_size_eq(test.marker_calls, 1u);
    check_int_eq(flowie_cluster_broadcast_target_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.admitted_events, 1u);
    check_uint_eq(snapshot.completed_events, 1u);
    check_uint_eq(snapshot.target_transactions, 0u);
    check_uint_eq(snapshot.shard_ack_transactions, 1u);
    check_false(snapshot.active);
    tstr_free(event);
    flowie_cluster_target_owner_test_cleanup(context, &execution, bind, owner);
  }

  it("treats a replayed shard marker as durable completion") {
    flowie_cluster_target_owner_test_t test;
    coro_context_t *context = NULL;
    tf_coronet_execution_t execution;
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_broadcast_target_owner_t *owner = NULL;
    flowie_cluster_broadcast_target_owner_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_SNAPSHOT_INIT;
    tstr_t event = NULL;

    check_int_eq(flowie_cluster_target_owner_test_fixture(&test, &context, &execution, &bind,
                                                          &owner),
                 TURBO_OK);
    check_int_eq(flowie_cluster_target_owner_test_event(&event), TURBO_OK);
    test.auto_complete = 1;
    test.durable_status = TURBO_EALREADY;
    check_int_eq(flowie_cluster_broadcast_target_owner_apply(
                     owner, event, tstr_len(event), flowie_cluster_target_owner_test_complete,
                     &test),
                 TURBO_OK);
    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(test.apply_completions, 1);
    check_int_eq(test.apply_status, TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_uint_eq(snapshot.completed_events, 1u);
    check_uint_eq(snapshot.shard_ack_transactions, 1u);
    tstr_free(event);
    flowie_cluster_target_owner_test_cleanup(context, &execution, bind, owner);
  }

  it("admits only one shard event until its PostgreSQL marker completes") {
    flowie_cluster_target_owner_test_t test;
    coro_context_t *context = NULL;
    tf_coronet_execution_t execution;
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_broadcast_target_owner_t *owner = NULL;
    flowie_cluster_broadcast_target_owner_snapshot_t snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_SNAPSHOT_INIT;
    tstr_t event = NULL;

    check_int_eq(flowie_cluster_target_owner_test_fixture(&test, &context, &execution, &bind,
                                                          &owner),
                 TURBO_OK);
    check_int_eq(flowie_cluster_target_owner_test_event(&event), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_owner_apply(
                     owner, event, tstr_len(event), flowie_cluster_target_owner_test_complete,
                     &test),
                 TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_owner_apply(
                     owner, event, tstr_len(event), flowie_cluster_target_owner_test_complete,
                     &test),
                 TURBO_EBUSY);
    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_not_null(test.pending_completion);
    check_int_eq(flowie_cluster_broadcast_target_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_owner_drain(owner, 0u), TURBO_EBUSY);
    test.pending_completion(test.pending_ctx, test.pending_command_id, TURBO_OK);
    test.pending_completion = NULL;
    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(test.apply_completions, 1);
    check_int_eq(test.apply_status, TURBO_OK);
    check_int_eq(flowie_cluster_broadcast_target_owner_snapshot(owner, &snapshot), TURBO_OK);
    check_false(snapshot.active);
    check_uint_eq(snapshot.admitted_events, 1u);
    check_uint_eq(snapshot.completed_events, 1u);
    check_int_eq(flowie_cluster_broadcast_target_owner_drain(owner, 0u), TURBO_OK);
    tstr_free(event);
    check_int_eq(flowie_cluster_broadcast_target_owner_destroy(owner), TURBO_OK);
    owner = NULL;
    flowie_cluster_target_owner_test_cleanup(context, &execution, bind, owner);
  }

  it("serially commits a matched target before the shard-complete marker") {
    flowie_cluster_target_owner_test_t test;
    coro_context_t *context = NULL;
    tf_coronet_execution_t execution;
    flowie_cluster_session_bind_t *bind = NULL;
    flowie_cluster_broadcast_target_owner_t *owner = NULL;
    flowie_cluster_broadcast_target_owner_snapshot_t owner_snapshot =
        FLOWIE_CLUSTER_BROADCAST_TARGET_OWNER_SNAPSHOT_INIT;
    flowie_session_snapshot_t before = FLOWIE_SESSION_SNAPSHOT_INIT;
    flowie_session_snapshot_t after = FLOWIE_SESSION_SNAPSHOT_INIT;
    size_t setup_submits;
    tstr_t event = NULL;

    check_int_eq(flowie_cluster_target_owner_test_fixture(&test, &context, &execution, &bind,
                                                          &owner),
                 TURBO_OK);
    check_int_eq(flowie_cluster_target_owner_test_bind_subscription(&test, bind, &before),
                 TURBO_OK);
    check_size_eq(before.subscription_count, 1u);
    setup_submits = test.submit_calls;
    check_int_eq(flowie_cluster_target_owner_test_event(&event), TURBO_OK);
    test.auto_complete = 1;
    check_int_eq(flowie_cluster_broadcast_target_owner_apply(
                     owner, event, tstr_len(event), flowie_cluster_target_owner_test_complete,
                     &test),
                 TURBO_OK);
    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(test.apply_completions, 1);
    check_int_eq(test.apply_status, TURBO_OK);
    check_size_eq(test.submit_calls, setup_submits + 2u);
    check_size_eq(test.target_calls, 1u);
    check_size_eq(test.marker_calls, 1u);
    check_int_eq(flowie_cluster_broadcast_target_owner_snapshot(owner, &owner_snapshot),
                 TURBO_OK);
    check_uint_eq(owner_snapshot.target_transactions, 1u);
    check_uint_eq(owner_snapshot.shard_ack_transactions, 1u);
    check_int_eq(flowie_cluster_session_bind_snapshot(
                     bind, (flowie_mqtt_span_t){(const uint8_t *)"device-a", 8u}, &after, NULL),
                 TURBO_OK);
    check_true(after.resource_generation > before.resource_generation);
    check_size_eq(after.inflight_count, 1u);
    tstr_free(event);
    flowie_cluster_target_owner_test_cleanup(context, &execution, bind, owner);
  }
}
