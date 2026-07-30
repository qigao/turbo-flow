#include "flowie_server_application_internal.h"

#include "flowie_test_socket.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flowie_server_application_fixture_s {
  char cert_file[512];
  char key_file[512];
  char *control_config_path;
  char *database_path;
  char *worker_config_path;
  unsigned short control_port;
  unsigned short worker_port;
} flowie_server_application_fixture_t;

static int application_test_write_control_config(
    const flowie_server_application_fixture_t *fixture) {
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
                  "  rpc_path: /v1/management/rpc\n"
                  "  session:\n"
                  "    capacity: 64\n"
                  "    ttl_seconds: 3600\n"
                  "dashboard:\n"
                  "  enabled: true\n"
                  "auth:\n"
                  "  enabled: false\n",
                  (unsigned int)fixture->control_port, fixture->cert_file,
                  fixture->key_file, fixture->database_path);
  if (size <= 0 || (size_t)size >= sizeof(yaml)) return -1;
  return tt_write_file(fixture->control_config_path, yaml, (size_t)size);
}

static int application_test_write_worker_config(
    const flowie_server_application_fixture_t *fixture) {
  char yaml[2048];
  int size;
  if (!fixture || !fixture->worker_config_path || fixture->worker_port == 0u) return -1;
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
                  "      transport: tcp\n"
                  "      host: 127.0.0.1\n"
                  "      port: %u\n"
                  "      max_packet_size: 4096\n"
                  "      coroutine_stack_size: 65536\n"
                  "      stream_recv_buffer_bytes: 4096\n"
                  "      send_hwm_bytes: 4096\n"
                  "      slow_subscriber_policy: disconnect\n"
                  "      manage_sessions: true\n"
                  "      settlement_qos1: accepted\n"
                  "      settlement_qos2: accepted\n"
                  "      max_sessions: 16\n"
                  "      max_retained_messages: 16\n"
                  "      max_subscriptions_per_session: 8\n"
                  "      max_inflight_per_session: 8\n",
                  (unsigned int)fixture->worker_port);
  if (size <= 0 || (size_t)size >= sizeof(yaml)) return -1;
  return tt_write_file(fixture->worker_config_path, yaml, (size_t)size);
}

static flowie_server_application_fixture_t application_test_fixture_open(void) {
  flowie_server_application_fixture_t fixture = {0};
  fixture.control_config_path = tt_make_temp_file("flowie-server-control", ".yml");
  fixture.database_path = tt_make_temp_file("flowie-server-control", ".sqlite3");
  fixture.worker_config_path = tt_make_temp_file("flowie-server-worker", ".yml");
  fixture.control_port = flowie_test_port();
  do {
    fixture.worker_port = flowie_test_port();
  } while (fixture.worker_port != 0u && fixture.worker_port == fixture.control_port);
  check_not_null(fixture.control_config_path);
  check_not_null(fixture.database_path);
  check_not_null(fixture.worker_config_path);
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
  if (fixture->control_config_path) {
    check_int_eq(tt_remove_file(fixture->control_config_path), 0);
    free(fixture->control_config_path);
  }
  tls_test_remove_file(fixture->key_file);
  tls_test_remove_file(fixture->cert_file);
  memset(fixture, 0, sizeof(*fixture));
}

static flowie_server_application_t *application_test_create(
    const flowie_server_application_fixture_t *fixture,
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
  if (!port_out || socket_handle == FLOWIE_TEST_INVALID_SOCKET)
    return FLOWIE_TEST_INVALID_SOCKET;
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
