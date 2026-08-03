#include "flow_coronet_execution.h"
#include "flowie_cluster_peer_connector_internal.h"
#include "flowie_cluster_peer_listener_internal.h"

#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <string.h>

#define FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS UINT64_C(5000000000)

typedef struct flowie_cluster_peer_connector_auth_s {
  const char *expected_node_id;
  uint8_t expected_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
} flowie_cluster_peer_connector_auth_t;

typedef struct flowie_cluster_peer_connector_route_s {
  atomic_int frames;
} flowie_cluster_peer_connector_route_t;

static void flowie_cluster_peer_connector_test_boot(uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                                    uint8_t first) {
  size_t index;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    boot_id[index] = (uint8_t)(first + index);
}

static int flowie_cluster_peer_connector_test_authorize(void *ctx, tstr_v peer_node_id,
                                                        const uint8_t *peer_boot_id,
                                                        const char *certificate_sha256) {
  flowie_cluster_peer_connector_auth_t *auth = (flowie_cluster_peer_connector_auth_t *)ctx;
  return tstr_v_eq(peer_node_id, tstr_v_from_cstr(auth->expected_node_id)) &&
                 memcmp(peer_boot_id, auth->expected_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0 &&
                 certificate_sha256 && strncmp(certificate_sha256, "sha256:", 7u) == 0
             ? TURBO_OK
             : TURBO_EPERM;
}

static int flowie_cluster_peer_connector_route_receive(void *ctx,
                                                       const flowie_cluster_peer_frame_t *frame) {
  static const char payload[] = {'c', 'm', 'd'};
  flowie_cluster_peer_connector_route_t *route = (flowie_cluster_peer_connector_route_t *)ctx;
  if (frame->kind != FLOWIE_CLUSTER_PEER_FRAME_COMMAND ||
      frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH || frame->shard_id != 0u ||
      frame->payload.len != sizeof(payload) ||
      memcmp(frame->payload.data, payload, sizeof(payload)) != 0)
    return TURBO_EPROTO;
  atomic_fetch_add_explicit(&route->frames, 1, memory_order_release);
  return TURBO_OK;
}

static flowie_cluster_node_router_t *
flowie_cluster_peer_connector_test_router(const char *node_id,
                                          const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  flowie_cluster_node_router_config_t config = FLOWIE_CLUSTER_NODE_ROUTER_CONFIG_INIT;
  flowie_cluster_node_router_t *router = NULL;
  config.shard_count = 1u;
  config.max_links = 1u;
  config.max_inflight_sends = 2u;
  config.cluster_id = tstr_v_from_cstr("cluster-a");
  config.listener_id = tstr_v_from_cstr("mqtt-main");
  config.local_node_id = tstr_v_from_cstr(node_id);
  memcpy(config.local_boot_id, boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  check_int_eq(flowie_cluster_node_router_create(&config, &router), TURBO_OK);
  return router;
}

spec("flowie cluster peer connector") {
  it("reconnects one deterministic mTLS link and routes through both node routers") {
    turbo_flow_coronet_execution_binding_t binding_a = {0};
    turbo_flow_coronet_execution_binding_t binding_b = {0};
    tf_coronet_execution_t execution_a;
    tf_coronet_execution_t execution_b;
    flowie_cluster_peer_listener_config_t listener_config =
        FLOWIE_CLUSTER_PEER_LISTENER_CONFIG_INIT;
    flowie_cluster_peer_connector_config_t connector_config =
        FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT;
    flowie_cluster_peer_listener_snapshot_t listener_snapshot =
        FLOWIE_CLUSTER_PEER_LISTENER_SNAPSHOT_INIT;
    flowie_cluster_peer_connector_snapshot_t connector_snapshot =
        FLOWIE_CLUSTER_PEER_CONNECTOR_SNAPSHOT_INIT;
    flowie_cluster_peer_connector_auth_t auth_a;
    flowie_cluster_peer_connector_auth_t auth_b;
    flowie_cluster_peer_connector_route_t route_b;
    flowie_cluster_node_router_t *router_a;
    flowie_cluster_node_router_t *router_b;
    flowie_cluster_peer_listener_t *listener = NULL;
    flowie_cluster_peer_connector_t *connector = NULL;
    flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    test_socket_t probe = TEST_INVALID_SOCKET;
    char ca_file[512] = {0};
    char cert_file[512] = {0};
    char key_file[512] = {0};
    uint8_t boot_a[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t boot_b[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint64_t deadline;
    size_t index;
    memset(&execution_a, 0, sizeof(execution_a));
    memset(&execution_b, 0, sizeof(execution_b));
    memset(&auth_a, 0, sizeof(auth_a));
    memset(&auth_b, 0, sizeof(auth_b));
    atomic_init(&route_b.frames, 0);
    flowie_cluster_peer_connector_test_boot(boot_a, 0x10u);
    flowie_cluster_peer_connector_test_boot(boot_b, 0x20u);
    auth_a.expected_node_id = "node-b";
    memcpy(auth_a.expected_boot_id, boot_b, sizeof(boot_b));
    auth_b.expected_node_id = "node-a";
    memcpy(auth_b.expected_boot_id, boot_a, sizeof(boot_a));

    check_int_eq(tls_test_init_socket_runtime(), TURBO_OK);
    check_int_eq(tls_test_prepare_listener(&probe, &listener_config.bind_port), TURBO_OK);
    test_close_socket(probe);
    check_int_eq(tls_test_write_ca_file(ca_file, sizeof(ca_file)), TURBO_OK);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)),
        TURBO_OK);
    binding_a.size = sizeof(binding_a);
    binding_a.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    binding_b.size = sizeof(binding_b);
    binding_b.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution_a, &binding_a), TURBO_OK);
    check_int_eq(tf_coronet_execution_init(&execution_b, &binding_b), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution_a), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution_b), TURBO_OK);
    router_a = flowie_cluster_peer_connector_test_router("node-a", boot_a);
    router_b = flowie_cluster_peer_connector_test_router("node-b", boot_b);
    check_int_eq(flowie_cluster_node_router_register_shard(
                     router_b, 0u, flowie_cluster_peer_connector_route_receive, &route_b),
                 TURBO_OK);

    listener_config.execution = &execution_b;
    listener_config.router = router_b;
    listener_config.max_connections = 1u;
    listener_config.max_payload_size = 256u;
    listener_config.queue_entries = 2u;
    listener_config.queue_bytes = 4096u;
    listener_config.socket_timeout_ms = 5000u;
    listener_config.bind_host = tstr_v_from_cstr("127.0.0.1");
    listener_config.cluster_id = tstr_v_from_cstr("cluster-a");
    listener_config.local_node_id = tstr_v_from_cstr("node-b");
    memcpy(listener_config.local_boot_id, boot_b, sizeof(boot_b));
    listener_config.ca_file = tstr_v_from_cstr(ca_file);
    listener_config.cert_file = tstr_v_from_cstr(cert_file);
    listener_config.key_file = tstr_v_from_cstr(key_file);
    listener_config.authorize = flowie_cluster_peer_connector_test_authorize;
    listener_config.authorize_ctx = &auth_b;
    check_int_eq(flowie_cluster_peer_listener_create(&listener_config, &listener), TURBO_OK);

    connector_config.execution = &execution_a;
    connector_config.router = router_a;
    connector_config.max_payload_size = 256u;
    connector_config.queue_entries = 2u;
    connector_config.queue_bytes = 4096u;
    connector_config.socket_timeout_ms = 250u;
    connector_config.retry_delay_ms = 20u;
    connector_config.remote_port = listener_config.bind_port;
    connector_config.remote_host = tstr_v_from_cstr("localhost");
    connector_config.cluster_id = tstr_v_from_cstr("cluster-a");
    connector_config.local_node_id = tstr_v_from_cstr("node-a");
    connector_config.remote_node_id = tstr_v_from_cstr("node-b");
    memcpy(connector_config.local_boot_id, boot_a, sizeof(boot_a));
    memcpy(connector_config.remote_boot_id, boot_b, sizeof(boot_b));
    connector_config.ca_file = tstr_v_from_cstr(ca_file);
    connector_config.cert_file = tstr_v_from_cstr(cert_file);
    connector_config.key_file = tstr_v_from_cstr(key_file);
    connector_config.authorize = flowie_cluster_peer_connector_test_authorize;
    connector_config.authorize_ctx = &auth_a;
    check_int_eq(flowie_cluster_peer_connector_create(&connector_config, &connector), TURBO_OK);
    check_int_eq(flowie_cluster_peer_connector_start(connector,
                                                     FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
                 TURBO_OK);
    deadline = turbo_monotonic_ms() + 7000u;
    while (turbo_monotonic_ms() < deadline) {
      check_int_eq(flowie_cluster_peer_connector_snapshot(connector, &connector_snapshot),
                   TURBO_OK);
      if (connector_snapshot.connect_attempts >= 2u) break;
      turbo_sleep_ms(1u);
    }
    check_true(connector_snapshot.connect_attempts >= 2u);
    check_int_eq(connector_snapshot.terminal_error, 0);
    check_int_eq(
        flowie_cluster_peer_listener_start(listener, FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
        TURBO_OK);
    while (turbo_monotonic_ms() < deadline) {
      check_int_eq(flowie_cluster_peer_connector_snapshot(connector, &connector_snapshot),
                   TURBO_OK);
      if (connector_snapshot.connected) break;
      turbo_sleep_ms(1u);
    }
    check_int_eq(connector_snapshot.connected, 1);
    check_int_eq(connector_snapshot.terminal_error, 0);
    check_true(connector_snapshot.connect_attempts >= 2u);
    check_int_eq(connector_snapshot.activated_links, 1);
    check_int_eq(flowie_cluster_peer_listener_snapshot(listener, &listener_snapshot), TURBO_OK);
    check_int_eq(listener_snapshot.registered_links, 1);

    frame.kind = FLOWIE_CLUSTER_PEER_FRAME_COMMAND;
    frame.operation = FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH;
    frame.shard_id = 0u;
    frame.owner_epoch = 1u;
    frame.connection_id = 1u;
    frame.connection_generation = 1u;
    frame.cluster_id = tstr_v_from_cstr("cluster-a");
    frame.listener_id = tstr_v_from_cstr("mqtt-main");
    frame.source_node_id = tstr_v_from_cstr("node-a");
    frame.target_node_id = tstr_v_from_cstr("node-b");
    frame.payload = tstr_v_from_cstr("cmd");
    memcpy(frame.source_boot_id, boot_a, sizeof(boot_a));
    memcpy(frame.target_boot_id, boot_b, sizeof(boot_b));
    for (index = 0u; index < sizeof(frame.correlation_id); ++index)
      frame.correlation_id[index] = (uint8_t)(index + 1u);
    check_int_eq(flowie_cluster_node_router_send(router_a, &frame, NULL, NULL), TURBO_OK);
    while (atomic_load_explicit(&route_b.frames, memory_order_acquire) == 0 &&
           turbo_monotonic_ms() < deadline)
      turbo_sleep_ms(1u);
    check_int_eq(atomic_load_explicit(&route_b.frames, memory_order_acquire), 1);

    check_int_eq(flowie_cluster_peer_connector_close(connector,
                                                     FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_connector_drain(connector,
                                                     FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_connector_destroy(connector), TURBO_OK);
    check_int_eq(
        flowie_cluster_peer_listener_close(listener, FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(
        flowie_cluster_peer_listener_drain(listener, FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_peer_listener_destroy(listener), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_unregister_shard(
                     router_b, 0u, flowie_cluster_peer_connector_route_receive, &route_b),
                 TURBO_OK);
    check_int_eq(flowie_cluster_node_router_close(router_a), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_close(router_b), TURBO_OK);
    check_int_eq(
        flowie_cluster_node_router_drain(router_a, FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(
        flowie_cluster_node_router_drain(router_b, FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router_a), TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router_b), TURBO_OK);
    tf_coronet_execution_stop(&execution_a);
    tf_coronet_execution_stop(&execution_b);
    tf_coronet_execution_destroy(&execution_a);
    tf_coronet_execution_destroy(&execution_b);
    tls_test_remove_file(ca_file);
    tls_test_remove_file(cert_file);
    tls_test_remove_file(key_file);
  }

  it("rejects the larger node as an initiator") {
    turbo_flow_coronet_execution_binding_t binding = {0};
    tf_coronet_execution_t execution;
    flowie_cluster_peer_connector_config_t config = FLOWIE_CLUSTER_PEER_CONNECTOR_CONFIG_INIT;
    flowie_cluster_peer_connector_auth_t auth;
    flowie_cluster_node_router_t *router;
    flowie_cluster_peer_connector_t *connector = NULL;
    uint8_t boot_a[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    uint8_t boot_b[FLOWIE_CLUSTER_BOOT_ID_SIZE];
    memset(&execution, 0, sizeof(execution));
    memset(&auth, 0, sizeof(auth));
    flowie_cluster_peer_connector_test_boot(boot_a, 0x10u);
    flowie_cluster_peer_connector_test_boot(boot_b, 0x20u);
    binding.size = sizeof(binding);
    binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    router = flowie_cluster_peer_connector_test_router("node-b", boot_b);
    config.execution = &execution;
    config.router = router;
    config.max_payload_size = 256u;
    config.queue_entries = 2u;
    config.queue_bytes = 4096u;
    config.socket_timeout_ms = 1000u;
    config.retry_delay_ms = 10u;
    config.remote_port = 1u;
    config.remote_host = tstr_v_from_cstr("localhost");
    config.cluster_id = tstr_v_from_cstr("cluster-a");
    config.local_node_id = tstr_v_from_cstr("node-b");
    config.remote_node_id = tstr_v_from_cstr("node-a");
    memcpy(config.local_boot_id, boot_b, sizeof(boot_b));
    memcpy(config.remote_boot_id, boot_a, sizeof(boot_a));
    config.ca_file = tstr_v_from_cstr("ca.pem");
    config.cert_file = tstr_v_from_cstr("cert.pem");
    config.key_file = tstr_v_from_cstr("key.pem");
    config.authorize = flowie_cluster_peer_connector_test_authorize;
    config.authorize_ctx = &auth;
    check_int_eq(flowie_cluster_peer_connector_create(&config, &connector), TURBO_EPERM);
    check_null(connector);
    check_int_eq(flowie_cluster_node_router_close(router), TURBO_OK);
    check_int_eq(
        flowie_cluster_node_router_drain(router, FLOWIE_CLUSTER_PEER_CONNECTOR_TEST_TIMEOUT_NS),
        TURBO_OK);
    check_int_eq(flowie_cluster_node_router_destroy(router), TURBO_OK);
    tf_coronet_execution_destroy(&execution);
  }
}
