#include "flowie_server_application_internal.h"

#include "flow_coronet_execution.h"
#include "flowie_test_socket.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_server_application_fixture_s {
  char cert_file[512];
  char key_file[512];
  char *control_config_path;
  char *database_path;
  char *worker_config_path;
  char *protocol_store_path;
  unsigned short control_port;
  unsigned short worker_port;
} flowie_server_application_fixture_t;

typedef struct application_cluster_fixture_s {
  flowie_endpoint_cluster_complete_fn pending_complete;
  void *pending_complete_ctx;
  flowie_mqtt_version_t mqtt_version;
  atomic_size_t connect_calls;
  atomic_size_t lost_calls;
  atomic_size_t detach_calls;
} application_cluster_fixture_t;

static int application_test_recv_connack(flowie_test_socket_t socket,
                                         uint8_t expected_session_present) {
  uint8_t wire[32];
  if (flowie_test_recv_exact(socket, wire, 2u) != TURBO_OK || wire[0] != UINT8_C(0x20) ||
      wire[1] > sizeof(wire) - 2u ||
      flowie_test_recv_exact(socket, wire + 2u, wire[1]) != TURBO_OK || wire[1] < 3u ||
      wire[2] != expected_session_present || wire[3] != 0u)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static void application_cluster_complete_post(void *arg1, void *arg2) {
  static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
  application_cluster_fixture_t *fixture = (application_cluster_fixture_t *)arg1;
  flowie_endpoint_cluster_action_t action = FLOWIE_ENDPOINT_CLUSTER_ACTION_INIT;
  (void)arg2;
  if (!fixture || !fixture->pending_complete) return;
  action.mqtt_version = fixture->mqtt_version;
  action.packet = (flowie_mqtt_span_t){connack, sizeof(connack)};
  fixture->pending_complete(fixture->pending_complete_ctx, TURBO_OK, &action);
  fixture->pending_complete = NULL;
  fixture->pending_complete_ctx = NULL;
}

static int application_cluster_connect(void *ctx, uint64_t connection_id,
                                       uint64_t connection_generation,
                                       const flowie_mqtt_connect_view_t *connect,
                                       const turbo_flow_security_principal_t *principal,
                                       const flowie_endpoint_cluster_ingress_t *ingress,
                                       const flowie_endpoint_cluster_socket_port_t *socket_port,
                                       flowie_endpoint_cluster_complete_fn complete,
                                       void *complete_ctx) {
  application_cluster_fixture_t *fixture = (application_cluster_fixture_t *)ctx;
  (void)connection_id;
  (void)connection_generation;
  (void)principal;
  if (!fixture || !connect || !ingress || !ingress->remote_address ||
      !ingress->transport_peer_address || !socket_port || !socket_port->takeover_close ||
      !socket_port->apply_action || !complete || fixture->pending_complete)
    return TURBO_EINVAL;
  fixture->pending_complete = complete;
  fixture->pending_complete_ctx = complete_ctx;
  fixture->mqtt_version = connect->version;
  atomic_fetch_add_explicit(&fixture->connect_calls, 1u, memory_order_release);
  return coro_post(coro_context_current(), application_cluster_complete_post, fixture, NULL);
}

static int application_cluster_command(void *ctx, uint64_t connection_id,
                                       uint64_t connection_generation,
                                       flowie_endpoint_cluster_command_t command,
                                       flowie_mqtt_version_t mqtt_version,
                                       flowie_mqtt_span_t client_id, flowie_mqtt_span_t packet,
                                       flowie_endpoint_cluster_complete_fn complete,
                                       void *complete_ctx) {
  (void)ctx;
  (void)connection_id;
  (void)connection_generation;
  (void)command;
  (void)mqtt_version;
  (void)client_id;
  (void)packet;
  (void)complete;
  (void)complete_ctx;
  return TURBO_ENOTSUP;
}

static int
application_cluster_settle(void *ctx, uint64_t connection_id, uint64_t connection_generation,
                           flowie_mqtt_version_t mqtt_version, flowie_mqtt_span_t client_id,
                           const turbo_flow_protocol_settlement_request_t *settlement,
                           flowie_endpoint_cluster_complete_fn complete, void *complete_ctx) {
  (void)ctx;
  (void)connection_id;
  (void)connection_generation;
  (void)mqtt_version;
  (void)client_id;
  (void)settlement;
  (void)complete;
  (void)complete_ctx;
  return TURBO_ENOTSUP;
}

static int application_cluster_connection_lost(void *ctx, uint64_t connection_id,
                                               uint64_t connection_generation,
                                               flowie_mqtt_version_t mqtt_version,
                                               flowie_mqtt_span_t client_id) {
  application_cluster_fixture_t *fixture = (application_cluster_fixture_t *)ctx;
  (void)connection_id;
  (void)connection_generation;
  (void)mqtt_version;
  (void)client_id;
  if (!fixture) return TURBO_EINVAL;
  atomic_fetch_add_explicit(&fixture->lost_calls, 1u, memory_order_release);
  return TURBO_OK;
}

static void application_cluster_detach(void *ctx, uint64_t connection_id,
                                       uint64_t connection_generation) {
  application_cluster_fixture_t *fixture = (application_cluster_fixture_t *)ctx;
  (void)connection_id;
  (void)connection_generation;
  if (fixture) atomic_fetch_add_explicit(&fixture->detach_calls, 1u, memory_order_release);
}

static int
application_test_write_control_config(const flowie_server_application_fixture_t *fixture) {
  char yaml[4096];
  int size;
  if (!fixture || !fixture->control_config_path || !fixture->database_path ||
      fixture->control_port == 0u)
    return -1;
  size = snprintf(yaml, sizeof(yaml),
                  "version: 1\n"
                  "listener:\n"
                  "  host: 127.0.0.1\n"
                  "  port: %u\n"
                  "  tls:\n"
                  "    cert_file: '%s'\n"
                  "    key_file: '%s'\n"
                  "    client_auth: none\n"
                  "storage:\n"
                  "  sqlite:\n"
                  "    path: '%s'\n"
                  "management:\n"
                  "  rpc_path: /v2/control/rpc\n"
                  "  session:\n"
                  "    capacity: 64\n"
                  "    ttl_seconds: 3600\n"
                  "dashboard:\n"
                  "  enabled: true\n"
                  "auth:\n"
                  "  enabled: false\n",
                  (unsigned int)fixture->control_port, fixture->cert_file, fixture->key_file,
                  fixture->database_path);
  if (size <= 0 || (size_t)size >= sizeof(yaml)) return -1;
  return tt_write_file(fixture->control_config_path, yaml, (size_t)size);
}

static int
application_test_write_worker_config_transport(const flowie_server_application_fixture_t *fixture,
                                               const char *transport) {
  char yaml[2048];
  int size;
  if (!fixture || !fixture->worker_config_path || fixture->worker_port == 0u || !transport ||
      !transport[0])
    return -1;
  size = snprintf(yaml, sizeof(yaml),
                  "version: 1\n"
                  "runtime:\n"
                  "  ingress:\n"
                  "    workers: 1\n"
                  "    capacity: 16\n"
                  "profiles:\n"
                  "  flowie:\n"
                  "    endpoint: mqtt.endpoint\n"
                  "adapters:\n"
                  "  mqtt.endpoint:\n"
                  "    kind: flowie_endpoint\n"
                  "    config:\n"
                  "      transport: %s\n"
                  "      host: 127.0.0.1\n"
                  "      port: %u\n"
                  "      max_packet_size: 4096\n"
                  "      send_hwm_bytes: 4096\n"
                  "      slow_subscriber_policy: disconnect\n"
                  "      manage_sessions: true\n"
                  "      settlement_qos1: accepted\n"
                  "      settlement_qos2: accepted\n"
                  "      max_sessions: 16\n"
                  "      max_retained_messages: 16\n"
                  "      max_subscriptions_per_session: 8\n"
                  "      max_inflight_per_session: 8\n",
                  transport, (unsigned int)fixture->worker_port);
  if (size <= 0 || (size_t)size >= sizeof(yaml)) return -1;
  return tt_write_file(fixture->worker_config_path, yaml, (size_t)size);
}

static int
application_test_write_worker_config(const flowie_server_application_fixture_t *fixture) {
  return application_test_write_worker_config_transport(fixture, "tcp");
}

static int application_test_write_worker_config_protocol_channel(
    const flowie_server_application_fixture_t *fixture, const char *endpoint_store_fields,
    const char *database_path) {
  char database_field[1024];
  char yaml[4096];
  int database_size = 0;
  int size;
  if (!fixture || !fixture->worker_config_path || fixture->worker_port == 0u ||
      !endpoint_store_fields)
    return -1;
  database_field[0] = '\0';
  if (database_path) {
    database_size = snprintf(database_field, sizeof(database_field),
                             "      database_path: '%s'\n", database_path);
    if (database_size <= 0 || (size_t)database_size >= sizeof(database_field)) return -1;
  }
  size = snprintf(yaml, sizeof(yaml),
                  "version: 1\n"
                  "runtime:\n"
                  "  ingress:\n"
                  "    workers: 1\n"
                  "    capacity: 16\n"
                  "profiles:\n"
                  "  flowie:\n"
                  "    endpoint: mqtt.endpoint\n"
                  "channels:\n"
                  "  mqtt.protocol:\n"
                  "    kind: record_store\n"
                  "    config:\n"
                  "      backend: sqlite\n"
                  "%s"
                  "      namespace_name: mqtt.protocol\n"
                  "      busy_timeout_ms: 1000\n"
                  "      max_records: 32\n"
                  "      max_bytes: 2359296\n"
                  "      max_item_bytes: 73728\n"
                  "      max_key_size: 65540\n"
                  "      max_value_size: 8192\n"
                  "      max_batch_size: 16\n"
                  "adapters:\n"
                  "  mqtt.endpoint:\n"
                  "    kind: flowie_endpoint\n"
                  "    config:\n"
                  "      transport: tcp\n"
                  "      host: 127.0.0.1\n"
                  "      port: %u\n"
                  "      max_packet_size: 4096\n"
                  "      send_hwm_bytes: 4096\n"
                  "      slow_subscriber_policy: disconnect\n"
                  "      manage_sessions: true\n"
                  "      settlement_qos1: accepted\n"
                  "      settlement_qos2: accepted\n"
                  "      max_sessions: 16\n"
                  "      max_retained_messages: 16\n"
                  "      max_subscriptions_per_session: 8\n"
                  "      max_inflight_per_session: 8\n"
                  "%s",
                  database_field, (unsigned int)fixture->worker_port, endpoint_store_fields);
  if (size <= 0 || (size_t)size >= sizeof(yaml)) return -1;
  return tt_write_file(fixture->worker_config_path, yaml, (size_t)size);
}

static flowie_server_application_fixture_t application_test_fixture_open(void) {
  flowie_server_application_fixture_t fixture = {0};
  fixture.control_config_path = tt_make_temp_file("flowie-server-control", ".yml");
  fixture.database_path = tt_make_temp_file("flowie-server-control", ".sqlite3");
  fixture.worker_config_path = tt_make_temp_file("flowie-server-worker", ".yml");
  fixture.protocol_store_path = tt_make_temp_file("flowie-server-protocol", ".sqlite3");
  fixture.control_port = flowie_test_port();
  do {
    fixture.worker_port = flowie_test_port();
  } while (fixture.worker_port != 0u && fixture.worker_port == fixture.control_port);
  check_not_null(fixture.control_config_path);
  check_not_null(fixture.database_path);
  check_not_null(fixture.worker_config_path);
  check_not_null(fixture.protocol_store_path);
  check_true(fixture.control_port != 0u);
  check_true(fixture.worker_port != 0u);
  check_int_eq(tls_test_write_server_files(fixture.cert_file, sizeof(fixture.cert_file),
                                           fixture.key_file, sizeof(fixture.key_file)),
               0);
  check_int_eq(application_test_write_control_config(&fixture), 0);
  check_int_eq(application_test_write_worker_config(&fixture), 0);
  return fixture;
}

static void application_test_fixture_close(flowie_server_application_fixture_t *fixture) {
  if (!fixture) return;
  if (fixture->worker_config_path) {
    check_int_eq(tt_remove_file(fixture->worker_config_path), 0);
    free(fixture->worker_config_path);
  }
  if (fixture->database_path) {
    check_int_eq(tt_remove_file(fixture->database_path), 0);
    free(fixture->database_path);
  }
  if (fixture->protocol_store_path) {
    check_int_eq(tt_remove_file(fixture->protocol_store_path), 0);
    free(fixture->protocol_store_path);
  }
  if (fixture->control_config_path) {
    check_int_eq(tt_remove_file(fixture->control_config_path), 0);
    free(fixture->control_config_path);
  }
  tls_test_remove_file(fixture->key_file);
  tls_test_remove_file(fixture->cert_file);
  memset(fixture, 0, sizeof(*fixture));
}

static flowie_server_application_t *
application_test_create(const flowie_server_application_fixture_t *fixture,
                        flowie_server_application_error_t *error) {
  flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
  flowie_server_application_t *application = NULL;
  config.config_path = fixture->worker_config_path;
  config.graph_path = FLOWIE_TEST_GRAPH_PATH;
  config.control_config_path = fixture->control_config_path;
  check_int_eq(flowie_server_application_create(&config, &application, error), TURBO_OK);
  check_not_null(application);
  return application;
}

static int application_test_can_connect(unsigned short port) {
  flowie_test_socket_t socket_handle = flowie_test_connect(port);
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return 0;
  flowie_test_socket_close(socket_handle);
  return 1;
}

static flowie_test_socket_t application_test_reserve_port(unsigned short *port_out) {
  struct sockaddr_in address;
  flowie_test_socket_t socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  int address_size = (int)sizeof(address);
  BOOL exclusive = TRUE;
#else
  socklen_t address_size = (socklen_t)sizeof(address);
#endif
  if (port_out) *port_out = 0u;
  if (!port_out || socket_handle == FLOWIE_TEST_INVALID_SOCKET) return FLOWIE_TEST_INVALID_SOCKET;
#ifdef _WIN32
  if (setsockopt(socket_handle, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, (const char *)&exclusive,
                 (int)sizeof(exclusive)) != 0) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
#endif
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(socket_handle, (struct sockaddr *)&address, sizeof(address)) != 0 ||
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) != 0 ||
      listen(socket_handle, 1) != 0) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
  *port_out = ntohs(address.sin_port);
  return socket_handle;
}

spec("Flowie server application") {
  it("defines the standalone process-local protocol store at the application boundary") {
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    check_str_eq(config.protocol_store_path, FLOWIE_SERVER_DEFAULT_PROTOCOL_STORE_PATH);
  }

  it("rejects a file protocol path at the standalone application boundary") {
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;

    config.config_path = "unused-flowie.yml";
    config.graph_path = "unused-flowie.flow";
    config.protocol_store_path = "flowie-protocol.sqlite3";
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_EINVAL);
    check_null(application);
    check_int_eq(error.status, TURBO_EINVAL);
    check_str_eq(error.operation, "validate server configuration");
  }

  it("requires a shared endpoint owner lane when cluster ownership is injected") {
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_server_application_t *application = NULL;

    config.config_path = "unused-flowie.yml";
    config.graph_path = "unused-flowie.flow";
    config.endpoint_cluster = &cluster;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_EINVAL);
    check_null(application);
    check_int_eq(error.status, TURBO_EINVAL);
    check_str_eq(error.operation, "validate server configuration");
  }

  it("rejects competing borrowed and root-owned cluster generations") {
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    turbo_flow_coronet_execution_binding_t execution = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    flowie_server_application_t *application = NULL;

    config.config_path = "unused-flowie.yml";
    config.graph_path = "unused-flowie.flow";
    config.endpoint_execution = &execution;
    config.endpoint_cluster = &cluster;
    config.cluster = (const flowie_cluster_generation_config_t *)(uintptr_t)1u;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_EINVAL);
    check_null(application);
    check_int_eq(error.status, TURBO_EINVAL);
    check_str_eq(error.operation, "validate server configuration");
  }

  it("passes one trusted PROXY policy into the embedded plaintext MQTT endpoint") {
    static const char *const trusted_proxy_cidrs[] = {"127.0.0.1/32"};
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_endpoint_proxy_binding_t proxy = FLOWIE_ENDPOINT_PROXY_BINDING_INIT;
    flowie_server_application_t *application = NULL;

    proxy.trusted_peer_cidrs = trusted_proxy_cidrs;
    proxy.trusted_peer_count = 1u;
    proxy.max_header_bytes = 256u;
    proxy.header_timeout_ms = 1000u;
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    config.endpoint_proxy = &proxy;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_OK);
    check_not_null(application);
    check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
    application_test_fixture_close(&fixture);
  }

  it("starts each implicit standalone protocol store with empty memory state") {
    static const uint8_t persistent_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'd',   'u',   'r'};
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;

    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    for (int generation = 0; generation < 2; ++generation) {
      check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_OK);
      check_not_null(application);
      check_int_eq(flowie_server_application_start(application, &error), TURBO_OK);
      client = flowie_test_connect(fixture.worker_port);
      check_true(client != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                   TURBO_OK);
      check_int_eq(application_test_recv_connack(client, 0u), TURBO_OK);
      flowie_test_socket_close(client);
      client = FLOWIE_TEST_INVALID_SOCKET;
      check_int_eq(flowie_server_application_stop(application, &error), TURBO_OK);
      check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
      application = NULL;
    }
    application_test_fixture_close(&fixture);
  }

  it("starts each explicit SQLite memory protocol store with empty state") {
    static const uint8_t persistent_connect[] = {
        0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',   0x05u, 0x00u, 0x00u, 0x3cu,
        0x05u, 0x11u, 0x00u, 0x00u, 0x00u, 0x3cu, 0x00u, 0x03u, 'e',   'x',   'p'};
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;

    check_int_eq(application_test_write_worker_config_protocol_channel(
                     &fixture, "      protocol_store: mqtt.protocol\n", ":memory:"),
                 0);
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    for (int generation = 0; generation < 2; ++generation) {
      check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_OK);
      check_int_eq(flowie_server_application_start(application, &error), TURBO_OK);
      client = flowie_test_connect(fixture.worker_port);
      check_true(client != FLOWIE_TEST_INVALID_SOCKET);
      check_int_eq(flowie_test_send(client, persistent_connect, sizeof(persistent_connect)),
                   TURBO_OK);
      check_int_eq(application_test_recv_connack(client, 0u), TURBO_OK);
      flowie_test_socket_close(client);
      client = FLOWIE_TEST_INVALID_SOCKET;
      check_int_eq(flowie_server_application_stop(application, &error), TURBO_OK);
      check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
      application = NULL;
    }
    application_test_fixture_close(&fixture);
  }

  it("accepts legacy session_store as the sole protocol store field") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;

    check_int_eq(application_test_write_worker_config_protocol_channel(
                     &fixture, "      session_store: mqtt.protocol\n", ":memory:"),
                 0);
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_OK);
    check_not_null(application);
    check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
    application_test_fixture_close(&fixture);
  }

  it("rejects simultaneous protocol_store and legacy session_store fields") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;

    check_int_eq(application_test_write_worker_config_protocol_channel(
                     &fixture,
                     "      protocol_store: mqtt.protocol\n"
                     "      session_store: mqtt.protocol\n",
                     ":memory:"),
                 0);
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_EINVAL);
    check_null(application);
    check_int_eq(error.detail, FLOWIE_SERVER_APPLICATION_ERROR_WORKER);
    check_int_eq(error.worker.detail, FLOWIE_WORKER_ERROR_CONFIG);
    check_str_contains(error.worker.config.path, "protocol_store");
    application_test_fixture_close(&fixture);
  }

  it("rejects an explicit SQLite protocol channel without database_path") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;

    check_int_eq(application_test_write_worker_config_protocol_channel(
                     &fixture, "      protocol_store: mqtt.protocol\n", NULL),
                 0);
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    check_int_ne(flowie_server_application_create(&config, &application, &error), TURBO_OK);
    check_null(application);
    check_int_eq(error.detail, FLOWIE_SERVER_APPLICATION_ERROR_WORKER);
    check_int_eq(error.worker.detail, FLOWIE_WORKER_ERROR_CONFIG);
    check_str_contains(error.worker.config.path, "mqtt.protocol.config");
    check_str_contains(error.worker.config.message, "database_path");
    application_test_fixture_close(&fixture);
  }

  it("rejects a file-backed standalone protocol store") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = NULL;

    check_int_eq(application_test_write_worker_config_protocol_channel(
                     &fixture, "      protocol_store: mqtt.protocol\n", fixture.protocol_store_path),
                 0);
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_EINVAL);
    check_null(application);
    check_int_eq(error.detail, FLOWIE_SERVER_APPLICATION_ERROR_WORKER);
    check_int_eq(error.worker.detail, FLOWIE_WORKER_ERROR_CONFIG);
    check_str_contains(error.worker.config.path, "mqtt.protocol.config.database_path");
    check_str_contains(error.worker.config.message, ":memory:");
    application_test_fixture_close(&fixture);
  }

  it("binds embedded MQTT sockets to the injected cluster owner on one CoroNet lane") {
    static const uint8_t expected_connack[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                                               0x08u, 0x27u, 0x00u, 0x00u, 0x10u, 0x00u};
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_config_t config = FLOWIE_SERVER_APPLICATION_CONFIG_INIT;
    turbo_flow_coronet_execution_binding_t private_placement = {
        sizeof(turbo_flow_coronet_execution_binding_t), TURBO_FLOW_CORONET_EXECUTION_PRIVATE};
    turbo_flow_coronet_execution_binding_t endpoint_placement = {
        sizeof(turbo_flow_coronet_execution_binding_t),
        TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT};
    flowie_endpoint_cluster_binding_t cluster = FLOWIE_ENDPOINT_CLUSTER_BINDING_INIT;
    application_cluster_fixture_t cluster_fixture;
    tf_coronet_execution_t execution;
    flowie_server_application_t *application = NULL;
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    uint8_t connect_wire[128];
    uint8_t received[sizeof(expected_connack)];
    size_t connect_size = 0u;

    memset(&execution, 0, sizeof(execution));
    memset(&cluster_fixture, 0, sizeof(cluster_fixture));
    atomic_init(&cluster_fixture.connect_calls, 0u);
    atomic_init(&cluster_fixture.lost_calls, 0u);
    atomic_init(&cluster_fixture.detach_calls, 0u);
    check_int_eq(tf_coronet_execution_init(&execution, &private_placement), TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    endpoint_placement.context = execution.context;
    cluster.ctx = &cluster_fixture;
    cluster.request_timeout_ms = 1000u;
    cluster.connect = application_cluster_connect;
    cluster.command = application_cluster_command;
    cluster.settle = application_cluster_settle;
    cluster.connection_lost = application_cluster_connection_lost;
    cluster.detach = application_cluster_detach;
    config.config_path = fixture.worker_config_path;
    config.graph_path = FLOWIE_TEST_GRAPH_PATH;
    config.endpoint_execution = &endpoint_placement;
    config.endpoint_cluster = &cluster;
    check_int_eq(flowie_server_application_create(&config, &application, &error), TURBO_OK);
    check_not_null(application);
    check_int_eq(flowie_server_application_start(application, &error), TURBO_OK);
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)"cluster-app", 11u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, connect_wire, sizeof(connect_wire),
                                                   &connect_size),
                 FLOWIE_MQTT_PARSE_OK);
    client = flowie_test_connect(fixture.worker_port);
    check_true(client != FLOWIE_TEST_INVALID_SOCKET);
    check_int_eq(flowie_test_send(client, connect_wire, connect_size), TURBO_OK);
    for (size_t i = 0u; i < 2000u && atomic_load_explicit(&cluster_fixture.connect_calls,
                                                          memory_order_acquire) == 0u;
         ++i)
      turbo_sleep_ms(1u);
    check_uint_eq(atomic_load_explicit(&cluster_fixture.connect_calls, memory_order_acquire), 1u);
    check_int_eq(flowie_test_recv_exact(client, received, 5u), TURBO_OK);
    check_int_eq(flowie_test_recv_exact(client, received + 5u, sizeof(received) - 5u), TURBO_OK);
    check_mem_eq(received, expected_connack, sizeof(expected_connack));
    flowie_test_socket_close(client);
    client = FLOWIE_TEST_INVALID_SOCKET;
    for (size_t i = 0u; i < 2000u && atomic_load_explicit(&cluster_fixture.detach_calls,
                                                          memory_order_acquire) == 0u;
         ++i)
      turbo_sleep_ms(1u);
    check_uint_eq(atomic_load_explicit(&cluster_fixture.lost_calls, memory_order_acquire), 1u);
    check_uint_eq(atomic_load_explicit(&cluster_fixture.detach_calls, memory_order_acquire), 1u);
    check_int_eq(flowie_server_application_stop(application, &error), TURBO_OK);
    check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
    application_test_fixture_close(&fixture);
  }

  it("validates embedded Control without opening its database") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application;
    char *database_content;
    size_t database_size = 1u;

    application = application_test_create(&fixture, &error);
    database_content = tt_read_file(fixture.database_path, &database_size);
    check_not_null(database_content);
    check_size_eq(database_size, 0u);
    free(database_content);
    check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
    application_test_fixture_close(&fixture);
  }

  it("starts Control before MQTT and synchronously closes both listeners") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application = application_test_create(&fixture, &error);

    check_int_eq(flowie_server_application_start(application, &error), TURBO_OK);
    check_true(application_test_can_connect(fixture.control_port));
    check_true(application_test_can_connect(fixture.worker_port));
    check_int_eq(flowie_server_application_stop(application, &error), TURBO_OK);
    check_false(application_test_can_connect(fixture.worker_port));
    check_false(application_test_can_connect(fixture.control_port));
    check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
    application_test_fixture_close(&fixture);
  }

  it("rolls Control back when MQTT cannot bind") {
    flowie_server_application_fixture_t fixture = application_test_fixture_open();
    flowie_server_application_error_t error = FLOWIE_SERVER_APPLICATION_ERROR_INIT;
    flowie_server_application_t *application;
    flowie_test_socket_t reservation;

    reservation = application_test_reserve_port(&fixture.worker_port);
    check_true(reservation != FLOWIE_TEST_INVALID_SOCKET);
    check_true(fixture.worker_port != 0u);
    check_int_eq(application_test_write_worker_config(&fixture), 0);
    application = application_test_create(&fixture, &error);
    check_int_ne(flowie_server_application_start(application, &error), TURBO_OK);
    check_false(application_test_can_connect(fixture.control_port));
    flowie_test_socket_close(reservation);
    check_int_eq(flowie_server_application_destroy(application, &error), TURBO_OK);
    application_test_fixture_close(&fixture);
  }
}
