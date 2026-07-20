#include "flowie.h"
#include "flowie_mqtt_client.h"
#include "flowie_test_socket.h"
#include "tls_test_support.h"

#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define FLOWIE_TRANSPORT_TEST_TIMEOUT_MS 10000u

typedef struct flowie_transport_client_state_s {
  atomic_int done;
  atomic_int status;
} flowie_transport_client_state_t;

typedef struct flowie_pipe_client_state_s {
  coro_context_t *context;
  const char *path;
  flowie_mqtt_version_t version;
  int done;
  int status;
} flowie_pipe_client_state_t;

static int flowie_transport_discard(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return TURBO_OK;
}

static turbo_flow_t *flowie_transport_flow(flowie_transport_t transport, unsigned short port,
                                           const char *path) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage discard\n"
                              "stage main {\n"
                              "  mqtt_in -> discard\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.transport = transport;
  config.host = transport == FLOWIE_TRANSPORT_PIPE ? NULL : "127.0.0.1";
  config.port = transport == FLOWIE_TRANSPORT_PIPE ? 0 : (int)port;
  config.path = path;
  config.max_connections = 2u;
  config.recv_timeout_ms = FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  config.manage_sessions = 1;
  config.max_sessions = 2u;
  config.max_subscriptions_per_session = 2u;
  config.max_inflight_per_session = 2u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_register_stage_ex(flow, "discard", flowie_transport_discard, NULL, NULL) !=
          TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static void flowie_transport_complete(flowie_transport_client_state_t *state, int status) {
  if (status != TURBO_OK) atomic_store_explicit(&state->status, status, memory_order_relaxed);
  atomic_store_explicit(&state->done, 1, memory_order_release);
}

static void flowie_transport_on_connect(flowie_mqtt_client_t *client, int status,
                                        const flowie_mqtt_control_packet_view_t *response,
                                        void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  (void)response;
  if (status == TURBO_OK) status = flowie_mqtt_client_ping(client);
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_ping(flowie_mqtt_client_t *client, int status,
                                     const flowie_mqtt_control_packet_view_t *response,
                                     void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  (void)response;
  if (status == TURBO_OK)
    status = flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  if (status != TURBO_OK) flowie_transport_complete(state, status);
}

static void flowie_transport_on_disconnect(flowie_mqtt_client_t *client, int status,
                                           const flowie_mqtt_control_packet_view_t *response,
                                           void *user_data) {
  (void)client;
  (void)response;
  flowie_transport_complete((flowie_transport_client_state_t *)user_data, status);
}

static void flowie_transport_on_error(flowie_mqtt_client_t *client, int status, void *user_data) {
  flowie_transport_client_state_t *state = (flowie_transport_client_state_t *)user_data;
  (void)client;
  if (!atomic_load_explicit(&state->done, memory_order_acquire))
    flowie_transport_complete(state, status);
}

static int flowie_transport_client_case(flowie_transport_t server_transport,
                                        flowie_mqtt_client_transport_t client_transport,
                                        const char *path, unsigned int client_number,
                                        flowie_mqtt_version_t version) {
  char client_id[48];
  flowie_transport_client_state_t state;
  flowie_mqtt_client_config_t client_config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_client_t *client = NULL;
  turbo_flow_t *flow = NULL;
  unsigned short port = 0u;
  uint64_t deadline;
  int rc;

  atomic_init(&state.done, 0);
  atomic_init(&state.status, TURBO_OK);
  port = flowie_test_port();
  if (port == 0u) return TURBO_EIO;
  flow = flowie_transport_flow(server_transport, port, path);
  if (!flow) return TURBO_EIO;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;

  client_config.transport = client_transport;
  client_config.host =
      server_transport == FLOWIE_TRANSPORT_TLS || server_transport == FLOWIE_TRANSPORT_WSS
          ? "localhost"
          : "127.0.0.1";
  client_config.port = (int)port;
  client_config.path = path;
  client_config.timeout_ms = FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  client_config.on_connect = flowie_transport_on_connect;
  client_config.on_ping = flowie_transport_on_ping;
  client_config.on_disconnect = flowie_transport_on_disconnect;
  client_config.on_error = flowie_transport_on_error;
  client_config.user_data = &state;
  rc = flowie_mqtt_client_create(&client_config, &client);
  if (rc != TURBO_OK) goto done;
  (void)snprintf(client_id, sizeof(client_id), "flowie-transport-%u", client_number);
  connect.version = version;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)client_id, strlen(client_id)};
  rc = flowie_mqtt_client_connect(client, &connect);
  if (rc != TURBO_OK) goto done;
  deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  while (!atomic_load_explicit(&state.done, memory_order_acquire) &&
         turbo_monotonic_ms() < deadline)
    turbo_sleep_ms(1u);
  rc = atomic_load_explicit(&state.done, memory_order_acquire)
           ? atomic_load_explicit(&state.status, memory_order_relaxed)
           : TURBO_ETIMEDOUT;

done:
  flowie_mqtt_client_destroy(client);
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

static int flowie_pipe_recv_exact(coro_socket_t *socket, uint8_t *output, size_t expected) {
  size_t offset = 0u;
  while (offset < expected) {
    char *data = NULL;
    size_t size = 0u;
    int rc = coro_socket_recv(socket, &data, &size);
    if (rc != TURBO_OK) return rc;
    if (!data || size == 0u || size > expected - offset) {
      coro_socket_free_recv(data);
      return TURBO_EPROTO;
    }
    memcpy(output + offset, data, size);
    offset += size;
    coro_socket_free_recv(data);
  }
  return TURBO_OK;
}

static void flowie_pipe_client(coro_t *coroutine, void *arg) {
  static const uint8_t connect_v31[] = {
      0x10u, 0x1du, 0x00u, 0x06u, 'M',   'Q',   'I',   's',   'd',   'p',   0x03u,
      0x02u, 0x00u, 0x1eu, 0x00u, 0x0fu, 'f',   'l',   'o',   'w',   'i',   'e',
      '-',   'p',   'i',   'p',   'e',   '-',   't',   'e',   's'};
  static const uint8_t connect_v311[] = {
      0x10u, 0x1bu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u, 0x02u, 0x00u, 0x1eu, 0x00u,
      0x0fu, 'f',   'l',   'o',   'w',   'i', 'e', '-', 'p',   'i',   'p',   'e',   '-',
      't',   'e',   's'};
  static const uint8_t connect_v5[] = {
      0x10u, 0x1cu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x05u, 0x02u, 0x00u, 0x1eu, 0x00u,
      0x00u, 0x0fu, 'f',   'l',   'o',   'w', 'i', 'e', '-',   'p',   'i',   'p',   'e',
      '-',   't',   'e',   's'};
  static const uint8_t connack_v311[] = {0x20u, 0x02u, 0x00u, 0x00u};
  static const uint8_t connack_v5[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                                       0x02u, 0x27u, 0x00u, 0x10u, 0x00u, 0x00u};
  static const uint8_t ping[] = {0xc0u, 0x00u};
  static const uint8_t pingresp[] = {0xd0u, 0x00u};
  flowie_pipe_client_state_t *state = (flowie_pipe_client_state_t *)arg;
  coro_socket_t *socket = coro_socket_create_pipe(state->context);
  const uint8_t *connect = NULL;
  size_t connect_size = 0u;
  const uint8_t *connack = NULL;
  size_t connack_size = 0u;
  uint8_t response[sizeof(connack_v5)];
  int rc = socket ? TURBO_OK : TURBO_ENOMEM;
  (void)coroutine;
  switch (state->version) {
    case FLOWIE_MQTT_VERSION_3_1:
      connect = connect_v31;
      connect_size = sizeof(connect_v31);
      connack = connack_v311;
      connack_size = sizeof(connack_v311);
      break;
    case FLOWIE_MQTT_VERSION_3_1_1:
      connect = connect_v311;
      connect_size = sizeof(connect_v311);
      connack = connack_v311;
      connack_size = sizeof(connack_v311);
      break;
    case FLOWIE_MQTT_VERSION_5:
      connect = connect_v5;
      connect_size = sizeof(connect_v5);
      connack = connack_v5;
      connack_size = sizeof(connack_v5);
      break;
    default:
      rc = TURBO_EINVAL;
      break;
  }
  if (rc == TURBO_OK) coro_socket_set_timeout(socket, FLOWIE_TRANSPORT_TEST_TIMEOUT_MS);
  if (rc == TURBO_OK) rc = coro_socket_connect_pipe(socket, state->path);
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)connect, connect_size);
  if (rc == TURBO_OK) rc = flowie_pipe_recv_exact(socket, response, connack_size);
  if (rc == TURBO_OK && memcmp(response, connack, connack_size) != 0) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = coro_socket_send(socket, (const char *)ping, sizeof(ping));
  if (rc == TURBO_OK) rc = flowie_pipe_recv_exact(socket, response, sizeof(pingresp));
  if (rc == TURBO_OK && memcmp(response, pingresp, sizeof(pingresp)) != 0) rc = TURBO_EPROTO;
  coro_socket_destroy(socket);
  state->status = rc;
  state->done = 1;
}

static int flowie_pipe_case(flowie_mqtt_version_t version) {
  char path[96];
  flowie_pipe_client_state_t state = {0};
  turbo_flow_t *flow = NULL;
  uint64_t deadline;
  int rc;
  (void)snprintf(path, sizeof(path), "pipe://flowie-transport-%llu",
                 (unsigned long long)turbo_monotonic_ms());
  flow = flowie_transport_flow(FLOWIE_TRANSPORT_PIPE, 0u, path);
  if (!flow) return TURBO_EIO;
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  state.context = coro_context_create(NULL);
  state.path = path;
  state.version = version;
  state.status = TURBO_EBUSY;
  if (!state.context) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = coro_context_spawn(state.context, flowie_pipe_client, &state);
  if (rc != TURBO_OK) goto done;
  deadline = turbo_monotonic_ms() + FLOWIE_TRANSPORT_TEST_TIMEOUT_MS;
  while (!state.done && turbo_monotonic_ms() < deadline) {
    rc = coro_context_run(state.context, TURBO_RUN_ONCE);
    if (rc != TURBO_OK) break;
  }
  if (rc == TURBO_OK) rc = state.done ? state.status : TURBO_ETIMEDOUT;

done:
  coro_context_destroy(state.context);
  if (flow) {
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  return rc;
}

spec("Flowie server transport contract") {
  it("serves MQTT 3.1, MQTT 3.1.1, and MQTT 5 over TCP, TLS, WS, and WSS") {
    char ca_path[512] = {0};
    char cert_path[512] = {0};
    char key_path[512] = {0};
    check_int_eq(tls_test_write_ca_file(ca_path, sizeof(ca_path)), 0);
    check_int_eq(
        tls_test_write_server_files(cert_path, sizeof(cert_path), key_path, sizeof(key_path)), 0);
    check_int_eq(tls_test_set_ca_file_env(ca_path), 0);
    check_int_eq(tls_test_set_server_env(cert_path, key_path), 0);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TCP,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, NULL, 9u,
                                              FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 10u,
                                              FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WS, FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
                                              "/mqtt", 11u, FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WSS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "/mqtt", 12u,
                                              FLOWIE_MQTT_VERSION_3_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TCP,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, NULL, 1u,
                                              FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 2u,
                                              FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WS, FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
                                              "/mqtt", 3u, FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WSS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "/mqtt", 4u,
                                              FLOWIE_MQTT_VERSION_3_1_1),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TCP,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TCP, NULL, 5u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_TLS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_TLS, NULL, 6u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WS, FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
                                              "/mqtt", 7u, FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    check_int_eq(flowie_transport_client_case(FLOWIE_TRANSPORT_WSS,
                                              FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "/mqtt", 8u,
                                              FLOWIE_MQTT_VERSION_5),
                 TURBO_OK);
    tls_test_clear_server_env();
    tls_test_clear_ca_env();
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    tls_test_remove_file(ca_path);
  }

  it("serves MQTT 3.1, MQTT 3.1.1, and MQTT 5 over Pipe") {
    check_int_eq(flowie_pipe_case(FLOWIE_MQTT_VERSION_3_1), TURBO_OK);
    check_int_eq(flowie_pipe_case(FLOWIE_MQTT_VERSION_3_1_1), TURBO_OK);
    check_int_eq(flowie_pipe_case(FLOWIE_MQTT_VERSION_5), TURBO_OK);
  }
}
