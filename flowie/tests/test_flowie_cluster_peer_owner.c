#include "flowie_cluster_peer_internal.h"

#include "platform.h"
#include "flow_coronet_execution.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

#define FLOWIE_CLUSTER_PEER_OWNER_TEST_QUEUE_BYTES 8192u
#define FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS UINT64_C(1000000000)
#define FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PAYLOAD_SIZE 2048u
#define FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE 128u

static const uint8_t FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID[] = {'c', 'l', 'i', 'e',
                                                                   'n', 't', '-', 'a'};
static const uint8_t FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH[] = {0x30u, 0x05u, 0x00u, 0x01u,
                                                                 'a',   0x00u, 'x'};

typedef struct flowie_cluster_peer_owner_test_s {
  coro_context_t *expected_context;
  flowie_cluster_owner_token_t current;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  int resolve_calls;
  int execute_attempts;
  int execute_calls;
  int async_ready;
  int async_reject;
  int durable_status;
  int completion_status;
  flowie_cluster_peer_owner_complete_fn complete;
  void *completion_ctx;
  tstr_t prepared_reply;
  int finalize_calls;
  int published;
  int aborted;
  int reply_count;
  int callback_error;
  int reply_status[3];
  uint8_t reply_correlation[3];
} flowie_cluster_peer_owner_test_t;

typedef struct flowie_cluster_peer_owner_completion_thread_s {
  flowie_cluster_peer_owner_test_t *test;
} flowie_cluster_peer_owner_completion_thread_t;

static void flowie_cluster_peer_owner_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                                uint8_t first) {
  unsigned int index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(first + index);
}

static turbo_flow_security_principal_t flowie_cluster_peer_owner_test_principal(void) {
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
  principal.policy_version = 1u;
  return principal;
}

static int flowie_cluster_peer_owner_test_resolve(void *user_data, uint32_t shard_id,
                                                  flowie_cluster_owner_token_t *out) {
  flowie_cluster_peer_owner_test_t *test = (flowie_cluster_peer_owner_test_t *)user_data;
  if (!test || !out || coro_context_current() != test->expected_context ||
      shard_id != test->current.shard_id) {
    return TURBO_EPROTO;
  }
  turbo_mutex_lock(&test->mutex);
  ++test->resolve_calls;
  *out = test->current;
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static int flowie_cluster_peer_owner_test_execute(void *user_data,
                                                  const flowie_cluster_peer_frame_t *command,
                                                  tstr_t *reply_payload) {
  static const char accepted[] = {'o', 'k'};
  flowie_cluster_peer_owner_test_t *test = (flowie_cluster_peer_owner_test_t *)user_data;
  int rc;
  if (!test || !command || !reply_payload || coro_context_current() != test->expected_context) {
    return TURBO_EPROTO;
  }
  turbo_mutex_lock(&test->mutex);
  ++test->execute_attempts;
  turbo_mutex_unlock(&test->mutex);
  if (command->operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH) {
    flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
    rc = flowie_cluster_peer_mqtt_command_decode(
        command->operation, command->payload.data, command->payload.len,
        FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &decoded);
    if (rc != TURBO_OK) return rc;
    if (decoded.operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH ||
        decoded.mqtt_version != FLOWIE_MQTT_VERSION_5 ||
        decoded.client_id.size != sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID) ||
        memcmp(decoded.client_id.data, FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
               sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)) != 0 ||
        decoded.packet.type != FLOWIE_MQTT_PACKET_PUBLISH ||
        decoded.packet.packet.size != sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH) ||
        memcmp(decoded.packet.packet.data, FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH,
               sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH)) != 0)
      return TURBO_EPROTO;
  } else if (command->operation == FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND) {
    flowie_cluster_peer_connect_bind_view_t decoded = FLOWIE_CLUSTER_PEER_CONNECT_BIND_VIEW_INIT;
    rc = flowie_cluster_peer_connect_bind_decode(command->payload.data, command->payload.len,
                                                 FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PAYLOAD_SIZE,
                                                 &decoded);
    if (rc != TURBO_OK) return rc;
    if (!decoded.security_enabled || strcmp(decoded.principal.principal_id, "writer") != 0 ||
        decoded.connect.client_id.size != sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID) ||
        memcmp(decoded.connect.client_id.data, FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
               sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)) != 0 ||
        decoded.connect.username.size != 0u || decoded.connect.password.size != 0u)
      return TURBO_EPROTO;
  } else {
    return TURBO_ENOTSUP;
  }
  turbo_mutex_lock(&test->mutex);
  ++test->execute_calls;
  turbo_mutex_unlock(&test->mutex);
  *reply_payload = tstr_new_len(accepted, sizeof(accepted));
  return *reply_payload ? TURBO_OK : TURBO_ENOMEM;
}

static int flowie_cluster_peer_owner_test_execute_async(
    void *user_data, const flowie_cluster_peer_frame_t *command,
    flowie_cluster_peer_owner_complete_fn complete, void *completion_ctx) {
  flowie_cluster_peer_owner_test_t *test = (flowie_cluster_peer_owner_test_t *)user_data;
  flowie_cluster_peer_mqtt_command_view_t decoded = FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT;
  int rc;
  if (!test || !command || !complete || !completion_ctx ||
      coro_context_current() != test->expected_context)
    return TURBO_EPROTO;
  turbo_mutex_lock(&test->mutex);
  ++test->execute_attempts;
  turbo_mutex_unlock(&test->mutex);
  rc = flowie_cluster_peer_mqtt_command_decode(
      command->operation, command->payload.data, command->payload.len,
      FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &decoded);
  if (rc != TURBO_OK) return rc;
  if (decoded.client_id.size != sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID) ||
      memcmp(decoded.client_id.data, FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
             sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)) != 0)
    return TURBO_EPROTO;
  turbo_mutex_lock(&test->mutex);
  if (test->async_reject) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOSPC;
  }
  test->prepared_reply = tstr_new_len("ok", 2u);
  if (!test->prepared_reply) {
    turbo_mutex_unlock(&test->mutex);
    return TURBO_ENOMEM;
  }
  test->complete = complete;
  test->completion_ctx = completion_ctx;
  test->async_ready = 1;
  ++test->execute_calls;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return TURBO_OK;
}

static int flowie_cluster_peer_owner_test_finalize(void *finalize_ctx, int durable_status,
                                                   tstr_t *reply_payload) {
  flowie_cluster_peer_owner_test_t *test = (flowie_cluster_peer_owner_test_t *)finalize_ctx;
  if (!test || !reply_payload || *reply_payload || coro_context_current() != test->expected_context)
    return TURBO_EPROTO;
  turbo_mutex_lock(&test->mutex);
  ++test->finalize_calls;
  if (durable_status == TURBO_OK) {
    test->published = 1;
    *reply_payload = test->prepared_reply;
    test->prepared_reply = NULL;
  } else {
    test->aborted = 1;
    tstr_freep(&test->prepared_reply);
  }
  turbo_mutex_unlock(&test->mutex);
  return durable_status;
}

static void flowie_cluster_peer_owner_test_complete_thread(void *ctx) {
  flowie_cluster_peer_owner_completion_thread_t *thread =
      (flowie_cluster_peer_owner_completion_thread_t *)ctx;
  flowie_cluster_peer_owner_complete_fn complete;
  void *completion_ctx;
  int durable_status;
  int rc;
  turbo_mutex_lock(&thread->test->mutex);
  complete = thread->test->complete;
  completion_ctx = thread->test->completion_ctx;
  durable_status = thread->test->durable_status;
  turbo_mutex_unlock(&thread->test->mutex);
  rc = complete(completion_ctx, durable_status, flowie_cluster_peer_owner_test_finalize,
                thread->test);
  turbo_mutex_lock(&thread->test->mutex);
  thread->test->completion_status = rc;
  turbo_cond_broadcast(&thread->test->changed);
  turbo_mutex_unlock(&thread->test->mutex);
}

static int flowie_cluster_peer_owner_test_reply(void *user_data,
                                                const flowie_cluster_peer_frame_t *reply) {
  static const char accepted[] = {'o', 'k'};
  flowie_cluster_peer_owner_test_t *test = (flowie_cluster_peer_owner_test_t *)user_data;
  size_t encoded_size = 0u;
  int callback_error = TURBO_OK;
  if (!test || !reply || coro_context_current() != test->expected_context ||
      reply->kind != FLOWIE_CLUSTER_PEER_FRAME_REPLY ||
      reply->operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY ||
      !tstr_v_eq(reply->source_node_id, tstr_v_from_cstr("owner-a")) ||
      !tstr_v_eq(reply->target_node_id, tstr_v_from_cstr("edge-a"))) {
    callback_error = TURBO_EPROTO;
  }
  if (callback_error == TURBO_OK &&
      flowie_cluster_peer_frame_encoded_size(reply, 128u, &encoded_size) != TURBO_OK)
    callback_error = TURBO_EPROTO;
  if (callback_error == TURBO_OK && reply->status == TURBO_OK &&
      (reply->payload.len != sizeof(accepted) ||
       memcmp(reply->payload.data, accepted, sizeof(accepted)) != 0)) {
    callback_error = TURBO_EPROTO;
  }
  if (callback_error == TURBO_OK && reply->status != TURBO_OK && reply->payload.len != 0u)
    callback_error = TURBO_EPROTO;

  turbo_mutex_lock(&test->mutex);
  if (callback_error != TURBO_OK) test->callback_error = callback_error;
  if (test->reply_count < 3) {
    test->reply_status[test->reply_count] = reply ? reply->status : TURBO_EPROTO;
    test->reply_correlation[test->reply_count] = reply ? reply->correlation_id[0] : 0u;
  }
  ++test->reply_count;
  turbo_cond_broadcast(&test->changed);
  turbo_mutex_unlock(&test->mutex);
  return callback_error;
}

static flowie_cluster_peer_owner_config_t flowie_cluster_peer_owner_test_config(
    tf_coronet_execution_t *execution, flowie_cluster_peer_owner_test_t *test,
    const uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], size_t queue_entries) {
  flowie_cluster_peer_owner_config_t config = FLOWIE_CLUSTER_PEER_OWNER_CONFIG_INIT;
  config.execution = execution;
  config.max_payload_size = FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PAYLOAD_SIZE;
  config.queue_entries = queue_entries;
  config.queue_bytes = FLOWIE_CLUSTER_PEER_OWNER_TEST_QUEUE_BYTES;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.listener_id = tstr_v_from_cstr("mqtt-main");
  config.local_node_id = tstr_v_from_cstr("owner-a");
  memcpy(config.local_boot_id, local_boot_id, sizeof(config.local_boot_id));
  config.resolve = flowie_cluster_peer_owner_test_resolve;
  config.execute = flowie_cluster_peer_owner_test_execute;
  config.reply = flowie_cluster_peer_owner_test_reply;
  config.user_data = test;
  return config;
}

static flowie_cluster_peer_owner_config_t flowie_cluster_peer_owner_test_async_config(
    tf_coronet_execution_t *execution, flowie_cluster_peer_owner_test_t *test,
    const uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], size_t queue_entries) {
  flowie_cluster_peer_owner_config_t config =
      flowie_cluster_peer_owner_test_config(execution, test, local_boot_id, queue_entries);
  config.execute = NULL;
  config.execute_async = flowie_cluster_peer_owner_test_execute_async;
  return config;
}

static flowie_cluster_peer_frame_t
flowie_cluster_peer_owner_test_command(const uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                       const uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                       uint64_t owner_epoch, uint8_t correlation, tstr_v payload) {
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
  frame.shard_id = 7u;
  frame.owner_epoch = owner_epoch;
  frame.connection_id = 11u;
  frame.connection_generation = 3u;
  frame.cluster_id = tstr_v_from_cstr("cluster-a");
  frame.listener_id = tstr_v_from_cstr("mqtt-main");
  frame.source_node_id = tstr_v_from_cstr("edge-a");
  frame.target_node_id = tstr_v_from_cstr("owner-a");
  frame.payload = payload;
  memcpy(frame.source_boot_id, source_boot_id, sizeof(frame.source_boot_id));
  memcpy(frame.target_boot_id, target_boot_id, sizeof(frame.target_boot_id));
  frame.correlation_id[0] = correlation;
  return frame;
}

static int flowie_cluster_peer_owner_test_wait(flowie_cluster_peer_owner_test_t *test,
                                               int replies) {
  uint64_t deadline = turbo_hrtime() + FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS;
  int rc = TURBO_OK;
  turbo_mutex_lock(&test->mutex);
  while (test->reply_count < replies) {
    uint64_t now = turbo_hrtime();
    if (now >= deadline) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&test->changed, &test->mutex, deadline - now);
  }
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static int flowie_cluster_peer_owner_test_wait_async_ready(flowie_cluster_peer_owner_test_t *test) {
  uint64_t deadline = turbo_hrtime() + FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS;
  int rc = TURBO_OK;
  turbo_mutex_lock(&test->mutex);
  while (!test->async_ready) {
    uint64_t now = turbo_hrtime();
    if (now >= deadline) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&test->changed, &test->mutex, deadline - now);
  }
  turbo_mutex_unlock(&test->mutex);
  return rc;
}

static void flowie_cluster_peer_owner_test_sync_init(flowie_cluster_peer_owner_test_t *test) {
  turbo_mutex_init(&test->mutex);
  turbo_cond_init(&test->changed);
}

static void flowie_cluster_peer_owner_test_sync_destroy(flowie_cluster_peer_owner_test_t *test) {
  tstr_freep(&test->prepared_reply);
  turbo_cond_destroy(&test->changed);
  turbo_mutex_destroy(&test->mutex);
}

spec("flowie cluster peer owner lane") {
  it("returns an async durable completion to the owner lane before drain") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_owner_t *owner = NULL;
    flowie_cluster_peer_owner_test_t test;
    flowie_cluster_peer_owner_config_t config;
    flowie_cluster_peer_frame_t command;
    flowie_cluster_peer_owner_completion_thread_t completion_thread;
    turbo_thread_t thread;
    tstr_t payload = NULL;
    uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&test, 0, sizeof(test));
    test.durable_status = TURBO_OK;
    flowie_cluster_peer_owner_test_sync_init(&test);
    flowie_cluster_peer_owner_test_boot(source_boot_id, 0x10u);
    flowie_cluster_peer_owner_test_boot(target_boot_id, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    test.expected_context = execution.context;
    check_int_eq(
        flowie_cluster_owner_token_init(&test.current, 7u, 41u, "owner-a", 7u, target_boot_id),
        TURBO_OK);
    config = flowie_cluster_peer_owner_test_async_config(&execution, &test, target_boot_id, 2u);
    check_int_eq(flowie_cluster_peer_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH)},
                     FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &payload),
                 TURBO_OK);
    command = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 41u, 0x61u,
                                                     tstr_v_from_buf(payload, tstr_len(payload)));
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_test_wait_async_ready(&test), TURBO_OK);
    command.correlation_id[0] = 0x63u;
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_OK);
    tstr_free(payload);
    check_int_eq(flowie_cluster_peer_owner_test_wait(&test, 1), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, 0u), TURBO_EBUSY);
    completion_thread.test = &test;
    check_int_eq(turbo_thread_create(&thread, flowie_cluster_peer_owner_test_complete_thread,
                                     &completion_thread),
                 TURBO_OK);
    check_int_eq(turbo_thread_join(&thread), TURBO_OK);
    turbo_thread_destroy(&thread);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(test.completion_status, TURBO_OK);
    check_int_eq(test.callback_error, TURBO_OK);
    check_int_eq(test.execute_attempts, 1);
    check_int_eq(test.execute_calls, 1);
    check_int_eq(test.finalize_calls, 1);
    check_int_eq(test.published, 1);
    check_int_eq(test.aborted, 0);
    check_int_eq(test.reply_count, 2);
    check_int_eq(test.reply_status[0], TURBO_EBUSY);
    check_int_eq(test.reply_status[1], TURBO_OK);
    check_uint_eq(test.reply_correlation[0], 0x63u);
    check_uint_eq(test.reply_correlation[1], 0x61u);
    check_int_eq(flowie_cluster_peer_owner_destroy(owner), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    flowie_cluster_peer_owner_test_sync_destroy(&test);
  }

  it("aborts staged state on durable failure before replying on the owner lane") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_owner_t *owner = NULL;
    flowie_cluster_peer_owner_test_t test;
    flowie_cluster_peer_owner_config_t config;
    flowie_cluster_peer_frame_t command;
    flowie_cluster_peer_owner_completion_thread_t completion_thread;
    turbo_thread_t thread;
    tstr_t payload = NULL;
    uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&test, 0, sizeof(test));
    test.durable_status = TURBO_EIO;
    flowie_cluster_peer_owner_test_sync_init(&test);
    flowie_cluster_peer_owner_test_boot(source_boot_id, 0x10u);
    flowie_cluster_peer_owner_test_boot(target_boot_id, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    test.expected_context = execution.context;
    check_int_eq(
        flowie_cluster_owner_token_init(&test.current, 7u, 41u, "owner-a", 7u, target_boot_id),
        TURBO_OK);
    config = flowie_cluster_peer_owner_test_async_config(&execution, &test, target_boot_id, 2u);
    check_int_eq(flowie_cluster_peer_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH)},
                     FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &payload),
                 TURBO_OK);
    command = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 41u, 0x64u,
                                                     tstr_v_from_buf(payload, tstr_len(payload)));
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_OK);
    tstr_free(payload);
    check_int_eq(flowie_cluster_peer_owner_test_wait_async_ready(&test), TURBO_OK);
    completion_thread.test = &test;
    check_int_eq(turbo_thread_create(&thread, flowie_cluster_peer_owner_test_complete_thread,
                                     &completion_thread),
                 TURBO_OK);
    check_int_eq(turbo_thread_join(&thread), TURBO_OK);
    turbo_thread_destroy(&thread);
    check_int_eq(flowie_cluster_peer_owner_test_wait(&test, 1), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(test.completion_status, TURBO_OK);
    check_int_eq(test.callback_error, TURBO_OK);
    check_int_eq(test.finalize_calls, 1);
    check_int_eq(test.published, 0);
    check_int_eq(test.aborted, 1);
    check_int_eq(test.reply_count, 1);
    check_int_eq(test.reply_status[0], TURBO_EIO);
    check_uint_eq(test.reply_correlation[0], 0x64u);
    check_int_eq(flowie_cluster_peer_owner_destroy(owner), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    flowie_cluster_peer_owner_test_sync_destroy(&test);
  }

  it("turns async admission failure into one owner-lane reply") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_owner_t *owner = NULL;
    flowie_cluster_peer_owner_test_t test;
    flowie_cluster_peer_owner_config_t config;
    flowie_cluster_peer_frame_t command;
    tstr_t payload = NULL;
    uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&test, 0, sizeof(test));
    test.async_reject = 1;
    flowie_cluster_peer_owner_test_sync_init(&test);
    flowie_cluster_peer_owner_test_boot(source_boot_id, 0x10u);
    flowie_cluster_peer_owner_test_boot(target_boot_id, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    test.expected_context = execution.context;
    check_int_eq(
        flowie_cluster_owner_token_init(&test.current, 7u, 41u, "owner-a", 7u, target_boot_id),
        TURBO_OK);
    config = flowie_cluster_peer_owner_test_async_config(&execution, &test, target_boot_id, 2u);
    check_int_eq(flowie_cluster_peer_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH)},
                     FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &payload),
                 TURBO_OK);
    command = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 41u, 0x62u,
                                                     tstr_v_from_buf(payload, tstr_len(payload)));
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_OK);
    tstr_free(payload);
    check_int_eq(flowie_cluster_peer_owner_test_wait(&test, 1), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(test.callback_error, TURBO_OK);
    check_int_eq(test.execute_attempts, 1);
    check_int_eq(test.execute_calls, 0);
    check_int_eq(test.reply_count, 1);
    check_int_eq(test.reply_status[0], TURBO_ENOSPC);
    check_uint_eq(test.reply_correlation[0], 0x62u);
    check_int_eq(flowie_cluster_peer_owner_destroy(owner), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    flowie_cluster_peer_owner_test_sync_destroy(&test);
  }

  it("rejects a stale epoch before mutation and executes the current epoch on its owner lane") {
    static const char malformed_payload[] = {'x'};
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_owner_t *owner = NULL;
    flowie_cluster_peer_owner_test_t test;
    flowie_cluster_peer_owner_config_t config;
    flowie_cluster_peer_frame_t stale;
    flowie_cluster_peer_frame_t current;
    flowie_cluster_peer_frame_t malformed;
    tstr_t payload = NULL;
    uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&test, 0, sizeof(test));
    flowie_cluster_peer_owner_test_sync_init(&test);
    flowie_cluster_peer_owner_test_boot(source_boot_id, 0x10u);
    flowie_cluster_peer_owner_test_boot(target_boot_id, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    test.expected_context = execution.context;
    check_int_eq(
        flowie_cluster_owner_token_init(&test.current, 7u, 41u, "owner-a", 7u, target_boot_id),
        TURBO_OK);
    config = flowie_cluster_peer_owner_test_config(&execution, &test, target_boot_id, 4u);
    check_int_eq(flowie_cluster_peer_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH)},
                     FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &payload),
                 TURBO_OK);

    stale = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 40u, 0x31u,
                                                   tstr_v_from_buf(payload, tstr_len(payload)));
    current = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 41u, 0x32u,
                                                     tstr_v_from_buf(payload, tstr_len(payload)));
    malformed = flowie_cluster_peer_owner_test_command(
        source_boot_id, target_boot_id, 41u, 0x33u,
        tstr_v_from_buf(malformed_payload, sizeof(malformed_payload)));
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &stale), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &current), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &malformed), TURBO_OK);
    tstr_free(payload);
    check_int_eq(flowie_cluster_peer_owner_test_wait(&test, 3), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(test.callback_error, TURBO_OK);
    check_int_eq(test.resolve_calls, 3);
    check_int_eq(test.execute_attempts, 2);
    check_int_eq(test.execute_calls, 1);
    check_int_eq(test.reply_count, 3);
    check_int_eq(test.reply_status[0], TURBO_EBUSY);
    check_int_eq(test.reply_status[1], TURBO_OK);
    check_int_eq(test.reply_status[2], TURBO_EPROTO);
    check_uint_eq(test.reply_correlation[0], 0x31u);
    check_uint_eq(test.reply_correlation[1], 0x32u);
    check_uint_eq(test.reply_correlation[2], 0x33u);
    check_int_eq(flowie_cluster_peer_owner_destroy(owner), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    flowie_cluster_peer_owner_test_sync_destroy(&test);
  }

  it("fences CONNECT_BIND before decoding credentials-free owner input") {
    static const char malformed_payload[] = {'x'};
    static const uint8_t empty_property = 0u;
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_owner_t *owner = NULL;
    flowie_cluster_peer_owner_test_t test;
    flowie_cluster_peer_owner_config_t config;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    turbo_flow_security_principal_t principal = flowie_cluster_peer_owner_test_principal();
    flowie_cluster_peer_frame_t stale;
    flowie_cluster_peer_frame_t current;
    tstr_t payload = NULL;
    uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&test, 0, sizeof(test));
    flowie_cluster_peer_owner_test_sync_init(&test);
    flowie_cluster_peer_owner_test_boot(source_boot_id, 0x10u);
    flowie_cluster_peer_owner_test_boot(target_boot_id, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    test.expected_context = execution.context;
    check_int_eq(
        flowie_cluster_owner_token_init(&test.current, 7u, 41u, "owner-a", 7u, target_boot_id),
        TURBO_OK);
    config = flowie_cluster_peer_owner_test_config(&execution, &test, target_boot_id, 2u);
    check_int_eq(flowie_cluster_peer_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 30u;
    connect.client_id = (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
                                             sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)};
    connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.properties.values.data = &empty_property;
    connect.will_properties =
        (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.will_properties.values.data = &empty_property;
    check_int_eq(
        flowie_cluster_peer_connect_bind_encode(
            &connect, &principal, FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PAYLOAD_SIZE, &payload),
        TURBO_OK);
    stale = flowie_cluster_peer_owner_test_command(
        source_boot_id, target_boot_id, 40u, 0x51u,
        tstr_v_from_buf(malformed_payload, sizeof(malformed_payload)));
    stale.operation = FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND;
    current = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 41u, 0x52u,
                                                     tstr_v_from_buf(payload, tstr_len(payload)));
    current.operation = FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND;
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &stale), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &current), TURBO_OK);
    tstr_free(payload);
    check_int_eq(flowie_cluster_peer_owner_test_wait(&test, 2), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_close(owner), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, FLOWIE_CLUSTER_PEER_OWNER_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(test.callback_error, TURBO_OK);
    check_int_eq(test.resolve_calls, 2);
    check_int_eq(test.execute_attempts, 1);
    check_int_eq(test.execute_calls, 1);
    check_int_eq(test.reply_count, 2);
    check_int_eq(test.reply_status[0], TURBO_EBUSY);
    check_int_eq(test.reply_status[1], TURBO_OK);
    check_uint_eq(test.reply_correlation[0], 0x51u);
    check_uint_eq(test.reply_correlation[1], 0x52u);
    check_int_eq(flowie_cluster_peer_owner_destroy(owner), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    flowie_cluster_peer_owner_test_sync_destroy(&test);
  }

  it("keeps admission bounded and rejects a command for another local incarnation") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_owner_t *owner = NULL;
    flowie_cluster_peer_owner_test_t test;
    flowie_cluster_peer_owner_config_t config;
    flowie_cluster_peer_frame_t command;
    tstr_t payload = NULL;
    uint8_t source_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&test, 0, sizeof(test));
    flowie_cluster_peer_owner_test_sync_init(&test);
    flowie_cluster_peer_owner_test_boot(source_boot_id, 0x10u);
    flowie_cluster_peer_owner_test_boot(target_boot_id, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
    binding.context = coro_context_create(NULL);
    check_not_null(binding.context);
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    test.expected_context = execution.context;
    check_int_eq(
        flowie_cluster_owner_token_init(&test.current, 7u, 41u, "owner-a", 7u, target_boot_id),
        TURBO_OK);
    config = flowie_cluster_peer_owner_test_config(&execution, &test, target_boot_id, 1u);
    check_int_eq(flowie_cluster_peer_owner_create(&config, &owner), TURBO_OK);
    check_int_eq(flowie_cluster_peer_mqtt_command_encode(
                     FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH, FLOWIE_MQTT_VERSION_5,
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_CLIENT_ID)},
                     (flowie_mqtt_span_t){FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH,
                                          sizeof(FLOWIE_CLUSTER_PEER_OWNER_TEST_PUBLISH)},
                     FLOWIE_CLUSTER_PEER_OWNER_TEST_MAX_PACKET_SIZE, &payload),
                 TURBO_OK);
    command = flowie_cluster_peer_owner_test_command(source_boot_id, target_boot_id, 41u, 0x41u,
                                                     tstr_v_from_buf(payload, tstr_len(payload)));
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_OK);
    command.correlation_id[0] = 0x42u;
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_ENOSPC);
    command.target_boot_id[0] ^= 1u;
    check_int_eq(flowie_cluster_peer_owner_submit(owner, &command), TURBO_EPROTO);
    tstr_free(payload);
    check_int_eq(flowie_cluster_peer_owner_close(owner), TURBO_OK);
    check_int_eq(coro_context_run(execution.context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(flowie_cluster_peer_owner_drain(owner, 0u), TURBO_OK);
    check_int_eq(test.execute_attempts, 1);
    check_int_eq(test.execute_calls, 1);
    check_int_eq(test.reply_count, 1);
    check_int_eq(flowie_cluster_peer_owner_destroy(owner), TURBO_OK);
    tf_coronet_execution_destroy(&execution);
    coro_context_destroy(binding.context);
    flowie_cluster_peer_owner_test_sync_destroy(&test);
  }
}
