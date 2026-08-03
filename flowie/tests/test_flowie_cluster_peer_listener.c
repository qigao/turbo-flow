#include "flow_coronet_execution.h"
#include "flowie_cluster_peer_listener_internal.h"

#include "CoroNet.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <string.h>

#define FLOWIE_CLUSTER_PEER_LISTENER_TEST_TIMEOUT_NS UINT64_C(5000000000)

typedef struct flowie_cluster_peer_listener_auth_s {
  const char *expected_node_id;
  uint8_t expected_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
} flowie_cluster_peer_listener_auth_t;

typedef struct flowie_cluster_peer_listener_route_s {
  atomic_int frames;
} flowie_cluster_peer_listener_route_t;

typedef struct flowie_cluster_peer_listener_client_s {
  coro_context_t *context;
  unsigned short port;
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  flowie_cluster_peer_listener_auth_t auth;
  flowie_cluster_peer_link_t *link;
  int result;
  int active_count;
  int done;
} flowie_cluster_peer_listener_client_t;

static void flowie_cluster_peer_listener_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                                   uint8_t first) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(first + index);
}

static int flowie_cluster_peer_listener_test_authorize(void *ctx, tstr_v peer_node_id,
                                                       const uint8_t *peer_boot_id,
                                                       const char *certificate_sha256) {
  flowie_cluster_peer_listener_auth_t *auth = (flowie_cluster_peer_listener_auth_t *)ctx;
  return tstr_v_eq(peer_node_id, tstr_v_from_cstr(auth->expected_node_id)) &&
                 memcmp(peer_boot_id, auth->expected_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0 &&
                 certificate_sha256 && strncmp(certificate_sha256, "sha256:", 7u) == 0
             ? TURBO_OK
             : TURBO_EPERM;
}

static int flowie_cluster_peer_listener_test_receive(void *ctx,
                                                     const flowie_cluster_peer_frame_t *frame) {
  (void)ctx;
  (void)frame;
  return TURBO_EPROTO;
}

static int flowie_cluster_peer_listener_shard_receive(void *ctx,
                                                      const flowie_cluster_peer_frame_t *frame) {
  static const char payload[] = {'p', 'u', 'b'};
  flowie_cluster_peer_listener_route_t *route = (flowie_cluster_peer_listener_route_t *)ctx;
  if (frame->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH || frame->shard_id != 0u ||
      frame->payload.len != sizeof(payload) ||
      memcmp(frame->payload.data, payload, sizeof(payload)) != 0)
    return TURBO_EPROTO;
  atomic_fetch_add_explicit(&route->frames, 1, memory_order_release);
  return TURBO_OK;
}

static int flowie_cluster_peer_listener_client_active(void *ctx, flowie_cluster_peer_link_t *link,
                                                      tstr_v peer_node_id,
                                                      const uint8_t *peer_boot_id) {
  static const char payload[] = {'p', 'u', 'b'};
  flowie_cluster_peer_listener_client_t *client = (flowie_cluster_peer_listener_client_t *)ctx;
  flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  size_t index;
  if (flowie_cluster_peer_listener_test_authorize(&client->auth, peer_node_id, peer_boot_id,
                                                  "sha256:authenticated") != TURBO_OK ||
      flowie_cluster_peer_link_state(link) != FLOWIE_CLUSTER_PEER_LINK_ACTIVE)
    return TURBO_EPROTO;
  ++client->active_count;
  flowie_cluster_peer_listener_test_boot(local_boot_id, 0x10u);
  frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
  frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
  frame.shard_id = 0u;
  frame.owner_epoch = 1u;
  frame.connection_id = 1u;
  frame.connection_generation = 1u;
  frame.cluster_id = tstr_v_from_cstr("cluster-a");
  frame.listener_id = tstr_v_from_cstr("mqtt-main");
  frame.source_node_id = tstr_v_from_cstr("node-a");
  frame.target_node_id = peer_node_id;
  frame.payload = tstr_v_from_buf(payload, sizeof(payload));
  memcpy(frame.source_boot_id, local_boot_id, sizeof(frame.source_boot_id));
  memcpy(frame.target_boot_id, peer_boot_id, sizeof(frame.target_boot_id));
  for (index = 0u; index < sizeof(frame.correlation_id); ++index)
    frame.correlation_id[index] = (uint8_t)(index + 1u);
  return flowie_cluster_peer_link_send(link, &frame, NULL, NULL);
}

static int flowie_cluster_peer_listener_client_authorize(void *ctx, tstr_v peer_node_id,
                                                         const uint8_t *peer_boot_id,
                                                         const char *certificate_sha256) {
  flowie_cluster_peer_listener_client_t *client = (flowie_cluster_peer_listener_client_t *)ctx;
  return flowie_cluster_peer_listener_test_authorize(&client->auth, peer_node_id, peer_boot_id,
                                                     certificate_sha256);
}

static void flowie_cluster_peer_listener_client_run(coro_t *coroutine, void *ctx) {
  flowie_cluster_peer_listener_client_t *client = (flowie_cluster_peer_listener_client_t *)ctx;
  flowie_cluster_peer_link_config_t config = FLOWIE_CLUSTER_PEER_LINK_CONFIG_INIT;
  coro_socket_t *socket = NULL;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  (void)coroutine;
  flowie_cluster_peer_listener_test_boot(local_boot_id, 0x10u);
  config.role = FLOWIE_CLUSTER_PEER_ROLE_INITIATOR;
  config.max_payload_size = 256u;
  config.queue_entries = 2u;
  config.queue_bytes = 4096u;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.local_node_id = tstr_v_from_cstr("node-a");
  memcpy(config.local_boot_id, local_boot_id, sizeof(config.local_boot_id));
  config.remote_node_id = tstr_v_from_cstr("node-b");
  memcpy(config.remote_boot_id, client->auth.expected_boot_id, sizeof(config.remote_boot_id));
  config.authorize = flowie_cluster_peer_listener_client_authorize;
  config.active = flowie_cluster_peer_listener_client_active;
  config.receive = flowie_cluster_peer_listener_test_receive;
  config.user_data = client;
  client->result = flowie_cluster_peer_link_create(&config, &client->link);
  if (client->result != TURBO_OK) {
    client->done = 1;
    return;
  }
  socket = coro_socket_create(client->context, CORO_SOCKET_TLS);
  if (!socket) {
    client->result = TURBO_ENOMEM;
    goto cleanup;
  }
  coro_socket_set_timeout(socket, 5000u);
  client->result = flowie_cluster_peer_tls_client_configure(
      socket, client->ca_file, client->cert_file, client->key_file, NULL);
  if (client->result == TURBO_OK)
    client->result = coro_socket_connect(socket, "localhost", client->port);
  if (client->result == TURBO_OK)
    client->result = flowie_cluster_peer_link_run(client->link, socket);

cleanup:
  if (socket) coro_socket_destroy(socket);
  {
    int destroy_rc = flowie_cluster_peer_link_destroy(client->link);
    if (client->result == TURBO_OK) client->result = destroy_rc;
  }
  client->link = NULL;
  client->done = 1;
}

spec("flowie cluster peer listener") {
  it("registers an authenticated responder link and drains CoroNet ownership") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_node_router_config_t router_config = FLOWIE_CLUSTER_NODE_ROUTER_CONFIG_INIT;
    flowie_cluster_peer_listener_config_t listener_config =
        FLOWIE_CLUSTER_PEER_LISTENER_CONFIG_INIT;
    flowie_cluster_peer_listener_snapshot_t listener_snapshot =
        FLOWIE_CLUSTER_PEER_LISTENER_SNAPSHOT_INIT;
    flowie_cluster_node_router_snapshot_t router_snapshot =
        FLOWIE_CLUSTER_NODE_ROUTER_SNAPSHOT_INIT;
    flowie_cluster_peer_listener_auth_t listener_auth;
    flowie_cluster_peer_listener_client_t client;
    flowie_cluster_node_router_t *router = NULL;
    flowie_cluster_peer_listener_t *listener = NULL;
    coro_context_t *client_context;
    test_socket_t probe = TEST_INVALID_SOCKET;
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    flowie_cluster_peer_listener_route_t route;
    uint64_t deadline;
    memset(&execution, 0, sizeof(execution));
    memset(&listener_auth, 0, sizeof(listener_auth));
    memset(&client, 0, sizeof(client));
    client.result = TURBO_EBUSY;
    atomic_init(&route.frames, 0);
    flowie_cluster_peer_listener_test_boot(local_boot_id, 0x20u);
    listener_auth.expected_node_id = "node-a";
    flowie_cluster_peer_listener_test_boot(listener_auth.expected_boot_id, 0x10u);
    client.auth.expected_node_id = "node-b";
    memcpy(client.auth.expected_boot_id, local_boot_id, sizeof(local_boot_id));

    check_int_eq(tls_test_init_socket_runtime(), TURBO_OK);
    check_int_eq(tls_test_prepare_listener(&probe, &client.port), TURBO_OK);
    test_close_socket(probe);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), TURBO_OK);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)),
        TURBO_OK);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);

    router_config.shard_count = 1u;
    router_config.max_links = 1u;
    router_config.max_inflight_sends = 2u;
    router_config.cluster_id = tstr_v_from_cstr("cluster-a");
    router_config.listener_id = tstr_v_from_cstr("mqtt-main");
    router_config.local_node_id = tstr_v_from_cstr("node-b");
    memcpy(router_config.local_boot_id, local_boot_id, sizeof(local_boot_id));
    check_int_eq(flowie_cluster_node_router_create(&router_config, &router), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_register_shard(
                     router, 0u, flowie_cluster_peer_listener_shard_receive, &route),
                 TURBO_OK);

    listener_config.execution = &execution;
    listener_config.router = router;
    listener_config.max_connections = 1u;
    listener_config.max_payload_size = 256u;
    listener_config.queue_entries = 2u;
    listener_config.queue_bytes = 4096u;
    listener_config.socket_timeout_ms = 5000u;
    listener_config.bind_port = client.port;
    listener_config.bind_host = tstr_v_from_cstr("127.0.0.1");
    listener_config.cluster_id = tstr_v_from_cstr("cluster-a");
    listener_config.local_node_id = tstr_v_from_cstr("node-b");
    memcpy(listener_config.local_boot_id, local_boot_id, sizeof(local_boot_id));
    listener_config.ca_file = tstr_v_from_cstr(ca_file);
    listener_config.cert_file = tstr_v_from_cstr(cert_file);
    listener_config.key_file = tstr_v_from_cstr(key_file);
    listener_config.authorize = flowie_cluster_peer_listener_test_authorize;
    listener_config.authorize_ctx = &listener_auth;
    check_int_eq(flowie_cluster_peer_listener_create(&listener_config, &listener), TURBO_OK);
    check_int_eq(
        flowie_cluster_peer_listener_start(listener, FLOWIE_CLUSTER_PEER_LISTENER_TEST_TIMEOUT_NS),
        TURBO_OK);

    client.context = coro_context_create(NULL);
    check_not_null(client.context);
    client_context = client.context;
    client.ca_file = ca_file;
    client.cert_file = cert_file;
    client.key_file = key_file;
    check_int_eq(
        coro_context_spawn(client.context, flowie_cluster_peer_listener_client_run, &client),
        TURBO_OK);
    deadline = turbo_monotonic_ms() + 7000u;
    while (client.active_count == 0 && turbo_monotonic_ms() < deadline)
      (void)coro_context_run(client.context, TURBO_RUN_ONCE);
    check_int_eq(client.active_count, 1);
    while (atomic_load_explicit(&route.frames, memory_order_acquire) == 0 &&
           turbo_monotonic_ms() < deadline)
      (void)coro_context_run(client.context, TURBO_RUN_ONCE);
    check_int_eq(atomic_load_explicit(&route.frames, memory_order_acquire), 1);
    check_int_eq(flowie_cluster_peer_listener_snapshot(listener, &listener_snapshot), TURBO_OK);
    check_int_eq(listener_snapshot.accepted_connections, 1);
    check_int_eq(listener_snapshot.rejected_connections, 0);
    check_int_eq(listener_snapshot.activated_links, 1);
    check_int_eq(listener_snapshot.active_handlers, 1);
    check_int_eq(listener_snapshot.registered_links, 1);
    check_int_eq(flowie_cluster_node_router_snapshot(router, &router_snapshot), TURBO_OK);
    check_int_eq(router_snapshot.peers.registered_links, 1);

    check_int_eq(
        flowie_cluster_peer_listener_close(listener, FLOWIE_CLUSTER_PEER_LISTENER_TEST_TIMEOUT_NS),
        TURBO_OK);
    while (!client.done && turbo_monotonic_ms() < deadline)
      (void)coro_context_run(client.context, TURBO_RUN_ONCE);
    check_int_eq(client.done, 1);
    check_true(client.result == TURBO_EOF || client.result == TURBO_ECANCELED ||
               client.result == TURBO_ECONNABORTED);
    check_int_eq(
        flowie_cluster_peer_listener_drain(listener, FLOWIE_CLUSTER_PEER_LISTENER_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_peer_listener_snapshot(listener, &listener_snapshot), TURBO_OK);
    check_int_eq(listener_snapshot.active_handlers, 0);
    check_int_eq(listener_snapshot.registered_links, 0);
    check_int_eq(flowie_cluster_node_router_snapshot(router, &router_snapshot), TURBO_OK);
    check_int_eq(router_snapshot.peers.registered_links, 0);
    check_int_eq(flowie_cluster_peer_listener_destroy(listener), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_unregister_shard(
                     router, 0u, flowie_cluster_peer_listener_shard_receive, &route),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_close(router), TURBO_OK);
    check_int_eq(
        flowie_cluster_node_router_drain(router, FLOWIE_CLUSTER_PEER_LISTENER_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    while (coro_context_alive(client_context))
      (void)coro_context_run(client_context, TURBO_RUN_NOWAIT);
    coro_context_destroy(client_context);
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }
}
