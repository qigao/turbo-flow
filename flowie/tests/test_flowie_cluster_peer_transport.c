#include "flowie_cluster_peer_internal.h"

#include "CoroNet.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <string.h>

typedef struct flowie_cluster_peer_transport_test_s flowie_cluster_peer_transport_test_t;

typedef struct flowie_cluster_peer_transport_callbacks_s {
  flowie_cluster_peer_transport_test_t *test;
  flowie_cluster_peer_link_t **link;
  const char *expected_node_id;
  uint8_t expected_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  int accept_commands;
} flowie_cluster_peer_transport_callbacks_t;

struct flowie_cluster_peer_transport_test_s {
  coro_context_t *context;
  coro_socket_t *server;
  flowie_cluster_peer_link_t *client_link;
  flowie_cluster_peer_registry_t *registry;
  unsigned short port;
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  int client_result;
  int server_result;
  int send_enqueue_result[3];
  int send_complete_result;
  int send_complete_count;
  int sender_started;
  int sender_thread_created;
  int sender_close_result;
  int registry_register_result;
  int registry_unregister_result;
  int registry_close_result;
  int registry_drain_result;
  int registry_destroy_result;
  turbo_thread_t sender_thread;
  turbo_mutex_t sender_mutex;
  int server_received;
  int active_count;
  int active_state_error;
};

static void flowie_cluster_peer_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                          uint8_t first) {
  unsigned int index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(first + index);
}

static int flowie_cluster_peer_transport_authorize(void *user_data, tstr_v peer_node_id,
                                                   const uint8_t *peer_boot_id,
                                                   const char *certificate_sha256) {
  flowie_cluster_peer_transport_callbacks_t *callbacks =
      (flowie_cluster_peer_transport_callbacks_t *)user_data;
  tstr_v expected = tstr_v_from_cstr(callbacks->expected_node_id);
  if (!tstr_v_eq(peer_node_id, expected) ||
      memcmp(peer_boot_id, callbacks->expected_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0 ||
      !certificate_sha256 || strncmp(certificate_sha256, "sha256:", 7u) != 0) {
    return TURBO_EPERM;
  }
  return TURBO_OK;
}

static int flowie_cluster_peer_transport_receive(void *user_data,
                                                 const flowie_cluster_peer_frame_t *frame) {
  static const char expected_payload[] = {'m', 'q', 't', 't', '\0', 'v', '1'};
  flowie_cluster_peer_transport_callbacks_t *callbacks =
      (flowie_cluster_peer_transport_callbacks_t *)user_data;
  if (!callbacks->accept_commands || frame->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH ||
      frame->payload.len != sizeof(expected_payload) ||
      memcmp(frame->payload.data, expected_payload, sizeof(expected_payload)) != 0) {
    return TURBO_EPROTO;
  }
  ++callbacks->test->server_received;
  return callbacks->test->server_received == 2 ? flowie_cluster_peer_link_close(*callbacks->link)
                                               : TURBO_OK;
}

static int flowie_cluster_peer_transport_active(void *user_data, flowie_cluster_peer_link_t *link,
                                                tstr_v peer_node_id, const uint8_t *peer_boot_id) {
  flowie_cluster_peer_transport_callbacks_t *callbacks =
      (flowie_cluster_peer_transport_callbacks_t *)user_data;
  if (!tstr_v_eq(peer_node_id, tstr_v_from_cstr(callbacks->expected_node_id)) ||
      memcmp(peer_boot_id, callbacks->expected_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
    return TURBO_EPERM;
  if (flowie_cluster_peer_link_state(link) != FLOWIE_CLUSTER_PEER_LINK_ACTIVE) {
    callbacks->test->active_state_error = TURBO_EPROTO;
    return TURBO_EPROTO;
  }
  ++callbacks->test->active_count;
  return TURBO_OK;
}

static flowie_cluster_peer_link_config_t
flowie_cluster_peer_transport_config(flowie_cluster_peer_role_t role, const char *local_node_id,
                                     const uint8_t *local_boot_id, const char *remote_node_id,
                                     const uint8_t *remote_boot_id,
                                     flowie_cluster_peer_transport_callbacks_t *callbacks) {
  flowie_cluster_peer_link_config_t config = FLOWIE_CLUSTER_PEER_LINK_CONFIG_INIT;
  config.role = role;
  config.max_payload_size = 256u;
  config.queue_entries = 2u;
  config.queue_bytes = 4096u;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.local_node_id = tstr_v_from_cstr(local_node_id);
  memcpy(config.local_boot_id, local_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  if (remote_node_id) config.remote_node_id = tstr_v_from_cstr(remote_node_id);
  if (remote_boot_id) memcpy(config.remote_boot_id, remote_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  config.authorize = flowie_cluster_peer_transport_authorize;
  config.active = flowie_cluster_peer_transport_active;
  config.receive = flowie_cluster_peer_transport_receive;
  config.user_data = callbacks;
  return config;
}

static void flowie_cluster_peer_transport_server(coro_socket_t *socket, void *user_data) {
  flowie_cluster_peer_transport_test_t *test = (flowie_cluster_peer_transport_test_t *)user_data;
  flowie_cluster_peer_transport_callbacks_t callbacks;
  flowie_cluster_peer_link_config_t config;
  flowie_cluster_peer_link_t *link = NULL;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  memset(&callbacks, 0, sizeof(callbacks));
  flowie_cluster_peer_test_boot(local_boot_id, 0x20u);
  flowie_cluster_peer_test_boot(remote_boot_id, 0x10u);
  callbacks.test = test;
  callbacks.link = &link;
  callbacks.expected_node_id = "node-a";
  memcpy(callbacks.expected_boot_id, remote_boot_id, sizeof(remote_boot_id));
  callbacks.accept_commands = 1;
  config = flowie_cluster_peer_transport_config(FLOWIE_CLUSTER_PEER_ROLE_RESPONDER, "node-b",
                                                local_boot_id, NULL, NULL, &callbacks);
  test->server_result = flowie_cluster_peer_link_create(&config, &link);
  if (test->server_result == TURBO_OK)
    test->server_result = flowie_cluster_peer_link_run(link, socket);
  if (link) {
    int destroy_rc = flowie_cluster_peer_link_destroy(link);
    if (test->server_result == TURBO_OK) test->server_result = destroy_rc;
  }
}

static void flowie_cluster_peer_transport_send_complete(void *user_data, int status) {
  flowie_cluster_peer_transport_test_t *test = (flowie_cluster_peer_transport_test_t *)user_data;
  turbo_mutex_lock(&test->sender_mutex);
  test->send_complete_result = status;
  ++test->send_complete_count;
  turbo_mutex_unlock(&test->sender_mutex);
}

static void flowie_cluster_peer_transport_sender(void *user_data) {
  static const char payload[] = {'m', 'q', 't', 't', '\0', 'v', '1'};
  flowie_cluster_peer_transport_test_t *test = (flowie_cluster_peer_transport_test_t *)user_data;
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  uint64_t deadline = turbo_monotonic_ms() + 3000u;
  int enqueue_result[3] = {TURBO_EBUSY, TURBO_EBUSY, TURBO_EBUSY};
  int close_result = TURBO_EBUSY;
  unsigned int index;
  flowie_cluster_peer_test_boot(remote_boot_id, 0x20u);
  turbo_mutex_lock(&test->sender_mutex);
  test->sender_started = 1;
  turbo_mutex_unlock(&test->sender_mutex);
  while (flowie_cluster_peer_link_state(test->client_link) != FLOWIE_CLUSTER_PEER_LINK_ACTIVE &&
         turbo_monotonic_ms() < deadline) {
    turbo_sleep_ms(1u);
  }
  if (flowie_cluster_peer_link_state(test->client_link) != FLOWIE_CLUSTER_PEER_LINK_ACTIVE) {
    enqueue_result[0] = TURBO_ETIMEDOUT;
    enqueue_result[1] = TURBO_ETIMEDOUT;
    enqueue_result[2] = TURBO_ETIMEDOUT;
    close_result = flowie_cluster_peer_link_close(test->client_link);
    turbo_mutex_lock(&test->sender_mutex);
    memcpy(test->send_enqueue_result, enqueue_result, sizeof(enqueue_result));
    test->sender_close_result = close_result;
    turbo_mutex_unlock(&test->sender_mutex);
    return;
  }
  test->registry_register_result = flowie_cluster_peer_registry_register(
      test->registry, tstr_v_from_cstr("node-b"), remote_boot_id, test->client_link);
  if (test->registry_register_result != TURBO_OK) {
    close_result = flowie_cluster_peer_link_close(test->client_link);
    turbo_mutex_lock(&test->sender_mutex);
    test->sender_close_result = close_result;
    turbo_mutex_unlock(&test->sender_mutex);
    return;
  }
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
  frame.shard_id = 7u;
  frame.owner_epoch = 9u;
  frame.connection_id = 11u;
  frame.connection_generation = 2u;
  frame.cluster_id = tstr_v_from_cstr("cluster-a");
  frame.listener_id = tstr_v_from_cstr("mqtt-main");
  frame.source_node_id = tstr_v_from_cstr("node-a");
  frame.target_node_id = tstr_v_from_cstr("node-b");
  frame.payload = tstr_v_from_buf(payload, sizeof(payload));
  flowie_cluster_peer_test_boot(frame.source_boot_id, 0x10u);
  flowie_cluster_peer_test_boot(frame.target_boot_id, 0x20u);
  for (index = 0u; index < FLOWIE_CLUSTER_PEER_CORRELATION_ID_SIZE; ++index)
    frame.correlation_id[index] = (uint8_t)(0x40u + index);
  enqueue_result[0] = flowie_cluster_peer_registry_send(
      test->registry, &frame, flowie_cluster_peer_transport_send_complete, test);
  frame.correlation_id[0]++;
  enqueue_result[1] = flowie_cluster_peer_registry_send(
      test->registry, &frame, flowie_cluster_peer_transport_send_complete, test);
  frame.correlation_id[0]++;
  enqueue_result[2] = flowie_cluster_peer_registry_send(test->registry, &frame, NULL, NULL);
  test->registry_unregister_result = flowie_cluster_peer_registry_unregister(
      test->registry, tstr_v_from_cstr("node-b"), remote_boot_id, test->client_link);
  close_result = flowie_cluster_peer_link_close(test->client_link);
  while (test->registry_unregister_result == TURBO_EBUSY && turbo_monotonic_ms() < deadline) {
    int completed;
    turbo_mutex_lock(&test->sender_mutex);
    completed = test->send_complete_count == 2;
    turbo_mutex_unlock(&test->sender_mutex);
    if (completed) {
      test->registry_unregister_result = flowie_cluster_peer_registry_unregister(
          test->registry, tstr_v_from_cstr("node-b"), remote_boot_id, test->client_link);
    }
    if (test->registry_unregister_result == TURBO_EBUSY) turbo_sleep_ms(1u);
  }
  test->registry_close_result = flowie_cluster_peer_registry_close(test->registry);
  test->registry_drain_result = flowie_cluster_peer_registry_drain(test->registry, 1000000000u);
  test->registry_destroy_result = flowie_cluster_peer_registry_destroy(test->registry);
  if (test->registry_destroy_result == TURBO_OK) test->registry = NULL;
  turbo_mutex_lock(&test->sender_mutex);
  memcpy(test->send_enqueue_result, enqueue_result, sizeof(enqueue_result));
  test->sender_close_result = close_result;
  turbo_mutex_unlock(&test->sender_mutex);
}

static void flowie_cluster_peer_transport_client(coro_t *coroutine, void *user_data) {
  flowie_cluster_peer_transport_test_t *test = (flowie_cluster_peer_transport_test_t *)user_data;
  flowie_cluster_peer_transport_callbacks_t callbacks;
  flowie_cluster_peer_link_config_t config;
  coro_socket_t *socket = NULL;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  (void)coroutine;
  memset(&callbacks, 0, sizeof(callbacks));
  flowie_cluster_peer_test_boot(local_boot_id, 0x10u);
  flowie_cluster_peer_test_boot(remote_boot_id, 0x20u);
  callbacks.test = test;
  callbacks.link = &test->client_link;
  callbacks.expected_node_id = "node-b";
  memcpy(callbacks.expected_boot_id, remote_boot_id, sizeof(remote_boot_id));
  config =
      flowie_cluster_peer_transport_config(FLOWIE_CLUSTER_PEER_ROLE_INITIATOR, "node-a",
                                           local_boot_id, "node-b", remote_boot_id, &callbacks);
  test->client_result = flowie_cluster_peer_link_create(&config, &test->client_link);
  if (test->client_result != TURBO_OK) return;
  test->client_result =
      turbo_thread_create(&test->sender_thread, flowie_cluster_peer_transport_sender, test);
  if (test->client_result != TURBO_OK) goto cleanup;
  test->sender_thread_created = 1;
  socket = coro_socket_create(test->context, CORO_SOCKET_TLS);
  if (!socket) {
    test->client_result = TURBO_ENOMEM;
    goto cleanup;
  }
  coro_socket_set_timeout(socket, 5000u);
  test->client_result = flowie_cluster_peer_tls_client_configure(
      socket, test->ca_file, test->cert_file, test->key_file, NULL);
  if (test->client_result == TURBO_OK)
    test->client_result = coro_socket_connect(socket, "localhost", test->port);
  if (test->client_result == TURBO_OK)
    test->client_result = flowie_cluster_peer_link_run(test->client_link, socket);

cleanup:
  if (test->sender_thread_created) {
    (void)turbo_thread_join(&test->sender_thread);
    turbo_thread_destroy(&test->sender_thread);
    test->sender_thread_created = 0;
  }
  if (socket) coro_socket_destroy(socket);
  {
    int destroy_rc = flowie_cluster_peer_link_destroy(test->client_link);
    if (test->client_result == TURBO_OK) test->client_result = destroy_rc;
  }
  test->client_link = NULL;
}

static int flowie_cluster_peer_transport_done(flowie_cluster_peer_transport_test_t *test) {
  int done;
  turbo_mutex_lock(&test->sender_mutex);
  done = test->client_result != TURBO_EBUSY && test->server_result != TURBO_EBUSY &&
         test->send_enqueue_result[0] != TURBO_EBUSY &&
         test->send_enqueue_result[1] != TURBO_EBUSY &&
         test->send_enqueue_result[2] != TURBO_EBUSY && test->send_complete_result != TURBO_EBUSY &&
         test->sender_close_result != TURBO_EBUSY && test->send_complete_count == 2 &&
         test->server_received == 2 && test->registry_destroy_result != TURBO_EBUSY;
  turbo_mutex_unlock(&test->sender_mutex);
  return done;
}

spec("flowie cluster peer mTLS transport") {
  it("binds peer identity to mTLS and enforces bounded command admission") {
    flowie_cluster_peer_transport_test_t test;
    test_socket_t probe = TEST_INVALID_SOCKET;
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    uint64_t deadline;
    flowie_cluster_peer_registry_config_t registry_config =
        FLOWIE_CLUSTER_PEER_REGISTRY_CONFIG_INIT;
    memset(&test, 0, sizeof(test));
    test.client_result = TURBO_EBUSY;
    test.server_result = TURBO_EBUSY;
    test.send_enqueue_result[0] = TURBO_EBUSY;
    test.send_enqueue_result[1] = TURBO_EBUSY;
    test.send_enqueue_result[2] = TURBO_EBUSY;
    test.send_complete_result = TURBO_EBUSY;
    test.sender_close_result = TURBO_EBUSY;
    test.registry_register_result = TURBO_EBUSY;
    test.registry_unregister_result = TURBO_EBUSY;
    test.registry_close_result = TURBO_EBUSY;
    test.registry_drain_result = TURBO_EBUSY;
    test.registry_destroy_result = TURBO_EBUSY;
    turbo_mutex_init(&test.sender_mutex);
    registry_config.max_links = 1u;
    registry_config.max_inflight_sends = 2u;
    check_int_eq(flowie_cluster_peer_registry_create(&registry_config, &test.registry), TURBO_OK);
    check_int_eq(tls_test_init_socket_runtime(), TURBO_OK);
    check_int_eq(tls_test_prepare_listener(&probe, &test.port), TURBO_OK);
    test_close_socket(probe);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), TURBO_OK);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)),
        TURBO_OK);
    test.ca_file = ca_file;
    test.cert_file = cert_file;
    test.key_file = key_file;
    test.context = coro_context_create(NULL);
    check_not_null(test.context);
    test.server = coro_socket_create(test.context, CORO_SOCKET_TLS);
    check_not_null(test.server);
    coro_socket_set_timeout(test.server, 5000u);
    check_int_eq(
        flowie_cluster_peer_tls_server_configure(test.server, ca_file, cert_file, key_file, NULL),
        TURBO_OK);
    check_int_eq(coro_socket_listen_on(test.server, "127.0.0.1", test.port,
                                       flowie_cluster_peer_transport_server, &test),
                 TURBO_OK);
    check_int_eq(coro_context_spawn(test.context, flowie_cluster_peer_transport_client, &test),
                 TURBO_OK);
    deadline = turbo_monotonic_ms() + 7000u;
    while (!flowie_cluster_peer_transport_done(&test) && turbo_monotonic_ms() < deadline)
      coro_context_run(test.context, TURBO_RUN_ONCE);
    check_int_eq(test.client_result, TURBO_OK);
    check_int_eq(test.server_result, TURBO_OK);
    check_int_eq(test.active_count, 2);
    check_int_eq(test.active_state_error, TURBO_OK);
    check_int_eq(test.sender_started, 1);
    check_int_eq(test.send_enqueue_result[0], TURBO_OK);
    check_int_eq(test.send_enqueue_result[1], TURBO_OK);
    check_int_eq(test.send_enqueue_result[2], TURBO_ENOSPC);
    check_int_eq(test.sender_close_result, TURBO_OK);
    check_int_eq(test.send_complete_result, TURBO_OK);
    check_int_eq(test.send_complete_count, 2);
    check_int_eq(test.server_received, 2);
    check_int_eq(test.registry_register_result, TURBO_OK);
    check_int_eq(test.registry_unregister_result, TURBO_OK);
    check_int_eq(test.registry_close_result, TURBO_OK);
    check_int_eq(test.registry_drain_result, TURBO_OK);
    check_int_eq(test.registry_destroy_result, TURBO_OK);
    coro_socket_destroy(test.server);
    deadline = turbo_monotonic_ms() + 1000u;
    while (coro_context_alive(test.context) && turbo_monotonic_ms() < deadline)
      coro_context_run(test.context, TURBO_RUN_NOWAIT);
    coro_context_destroy(test.context);
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
    turbo_mutex_destroy(&test.sender_mutex);
  }

  it("rejects links whose queue cannot hold one maximum frame") {
    flowie_cluster_peer_transport_callbacks_t callbacks;
    flowie_cluster_peer_link_config_t config;
    flowie_cluster_peer_link_t *link = NULL;
    uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&callbacks, 0, sizeof(callbacks));
    flowie_cluster_peer_test_boot(local_boot_id, 1u);
    flowie_cluster_peer_test_boot(remote_boot_id, 17u);
    callbacks.expected_node_id = "node-b";
    memcpy(callbacks.expected_boot_id, remote_boot_id, sizeof(remote_boot_id));
    config =
        flowie_cluster_peer_transport_config(FLOWIE_CLUSTER_PEER_ROLE_INITIATOR, "node-a",
                                             local_boot_id, "node-b", remote_boot_id, &callbacks);
    config.queue_bytes = 255u;
    check_int_eq(flowie_cluster_peer_link_create(&config, &link), TURBO_EINVAL);
    check_null(link);
  }
}
