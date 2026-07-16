#include "turbo_flow_rpc.h"

#include "tinytest.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
#endif

typedef struct rpc_test_state_s {
  char payload[128];
  size_t payload_len;
  atomic_int handler_called;
  atomic_int capture_called;
  int captured_content;
} rpc_test_state_t;

static int rpc_echo_handler(turbo_flow_msg_t *msg, void *ctx) {
  rpc_test_state_t *state = (rpc_test_state_t *)ctx;
  const char *method = turbo_flow_rpc_request_method(msg);
  if (!state || !method || strcmp(method, "echo") != 0) return TURBO_EINVAL;
  atomic_fetch_add_explicit(&state->handler_called, 1, memory_order_release);
  return TURBO_OK;
}

static int rpc_capture(turbo_flow_msg_t *msg, void *ctx) {
  rpc_test_state_t *state = (rpc_test_state_t *)ctx;
  if (!state || !msg || msg->payload.len > sizeof(state->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0) memcpy(state->payload, msg->payload.data, msg->payload.len);
  state->payload_len = msg->payload.len;
  state->captured_content = turbo_flow_msg_content_descriptor(msg) != NULL;
  atomic_fetch_add_explicit(&state->capture_called, 1, memory_order_release);
  return TURBO_OK;
}

static unsigned short rpc_test_port(void) {
  struct sockaddr_in addr;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int addr_len = (int)sizeof(addr);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
#else
  int socket_handle = -1;
  socklen_t addr_len = (socklen_t)sizeof(addr);
#endif
  unsigned short port = 0;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (socket_handle == INVALID_SOCKET) return 0;
#else
  if (socket_handle < 0) return 0;
#endif
  if (bind(socket_handle, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&addr, &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
  return port;
}

spec("turbo_flow_rpc") {
  it("registers RPC client and server from one resolved YAML snapshot") {
    static const char yaml[] = "version: 1\n"
                               "fragments:\n"
                               "  connection:\n"
                               "    remote:\n"
                               "      url: http://127.0.0.1:1/rpc\n"
                               "    local:\n"
                               "      port: 18082\n"
                               "      endpoint: /rpc\n"
                               "  timer:\n"
                               "    bounded:\n"
                               "      timeout_ms: 250\n"
                               "adapters:\n"
                               "  rpc.client:\n"
                               "    kind: rpc\n"
                               "    fragments:\n"
                               "      connection: remote\n"
                               "      timer: bounded\n"
                               "    config:\n"
                               "      method: echo\n"
                               "  rpc.server:\n"
                               "    kind: rpc\n"
                               "    fragments:\n"
                               "      connection: local\n"
                               "    config:\n"
                               "      max_request_size: 4096\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *client_schema;
    const turbo_flow_adapter_schema_t *server_schema;

    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_rpc_register_client_resolved_adapter(flow, resolved, "rpc.client"),
                 TURBO_OK);
    check_int_eq(turbo_flow_rpc_register_server_resolved_adapter(flow, resolved, "rpc.server"),
                 TURBO_OK);
    client_schema = turbo_flow_find_adapter_schema(flow, "rpc.client");
    server_schema = turbo_flow_find_adapter_schema(flow, "rpc.server");
    check_not_null(client_schema);
    check_not_null(server_schema);
    check_uint_eq(client_schema->roles, TURBO_FLOW_ADAPTER_TRANSFORM);
    check_uint_eq(server_schema->roles, TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("rejects host-only state in resolved RPC adapters") {
    static const char yaml[] = "version: 1\nadapters:\n  rpc.server:\n    kind: rpc\n    config:\n"
                               "      port: 18083\n      endpoint: /rpc\n      app: injected\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_rpc_register_server_resolved_adapter(flow, resolved, "rpc.server"),
                 TURBO_EINVAL);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("rejects negative client timeouts at registration") {
    turbo_flow_rpc_client_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1/rpc";
    config.method = "echo";
    config.timeout_ms = -1;
    check_int_eq(turbo_flow_rpc_register_client_adapter(flow, "rpc.invalid", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("round trips JSON params and results") {
    static const char *server_dsl = "source request adapter rpc.server.echo\n"
                                    "stage handler\n"
                                    "stage response adapter rpc.server.echo\n"
                                    "stage main {\n"
                                    "  request -> handler -> response\n"
                                    "}\n";
    static const char *client_dsl = "source input\n"
                                    "stage call adapter rpc.client.once\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> call -> capture\n"
                                    "}\n";
    const char params[] = "{\"value\":7}";
    unsigned short port = rpc_test_port();
    char url[128];
    turbo_flow_rpc_server_config_t server_config;
    turbo_flow_rpc_client_config_t client_config;
    turbo_flow_content_descriptor_t input_descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    turbo_flow_msg_t msg;
    turbo_flow_connection_snapshot_t server_connection;
    turbo_flow_connection_snapshot_t client_connection;
    rpc_test_state_t state;

    check_int_gt(port, 0);
    check_not_null(server_flow);
    check_not_null(client_flow);
    memset(&state, 0, sizeof(state));
    atomic_init(&state.handler_called, 0);
    atomic_init(&state.capture_called, 0);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.endpoint = "/rpc";
    server_config.max_request_size = 4096;
    check_int_eq(
        turbo_flow_rpc_register_server_adapter(server_flow, "rpc.server.echo", &server_config),
        TURBO_OK);
    memset(&server_connection, 0, sizeof(server_connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(
        turbo_flow_register_stage_ex(server_flow, "handler", rpc_echo_handler, &state, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_eq(server_connection.connections_current, 1);
    check_int_gt(snprintf(url, sizeof(url), "rpc://0.0.0.0:%u/rpc", (unsigned)port), 0);
    check_str_eq(server_connection.endpoint, url);

    snprintf(url, sizeof(url), "http://127.0.0.1:%u/rpc", (unsigned)port);
    memset(&client_config, 0, sizeof(client_config));
    client_config.url = url;
    client_config.method = "echo";
    client_config.timeout_ms = 2000;
    check_int_eq(
        turbo_flow_rpc_register_client_adapter(client_flow, "rpc.client.once", &client_config),
        TURBO_OK);
    memset(&client_connection, 0, sizeof(client_connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_str_eq(client_connection.endpoint, url);
    check_int_eq(turbo_flow_register_stage_ex(client_flow, "capture", rpc_capture, &state, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(client_flow), TURBO_OK);

    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(params);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_content_descriptor_from_media(&input_descriptor, TURBO_FLOW_DOMAIN_DATA,
                                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                                          "application/json", "rpc-request", NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_msg_set_content_descriptor(&msg, &input_descriptor), TURBO_OK);
    check_int_eq(turbo_flow_publish(client_flow, "input", &msg), TURBO_OK);
    check_int_eq(atomic_load_explicit(&state.handler_called, memory_order_acquire), 1);
    check_int_eq(atomic_load_explicit(&state.capture_called, memory_order_acquire), 1);
    check_size_eq(state.payload_len, sizeof(params) - 1);
    check_mem_eq(state.payload, params, sizeof(params) - 1);
    check_int_eq(state.captured_content, 0);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_eq(client_connection.connections_current, 0);
    check_size_eq(client_connection.in_flight_messages, 0);
    check_size_eq(client_connection.in_flight_bytes, 0);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_size_eq(server_connection.in_flight_messages, 0);
    check_size_eq(server_connection.in_flight_bytes, 0);

    turbo_flow_msg_cleanup(&msg);
    check_int_eq(turbo_flow_stop(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(client_connection.last_status, TURBO_ESHUTDOWN);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(server_connection.last_status, TURBO_ESHUTDOWN);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
  }

  it("publishes periodic calls as source messages") {
    static const char *server_dsl = "source request adapter rpc.server.poll\n"
                                    "stage handler\n"
                                    "stage response adapter rpc.server.poll\n"
                                    "stage main {\n"
                                    "  request -> handler -> response\n"
                                    "}\n";
    static const char *client_dsl = "source remote adapter rpc.client.poll\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  remote -> capture\n"
                                    "}\n";
    const char params[] = "{\"poll\":true}";
    unsigned short port = rpc_test_port();
    char url[128];
    turbo_flow_rpc_server_config_t server_config;
    turbo_flow_rpc_client_config_t client_config;
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    rpc_test_state_t state;

    check_int_gt(port, 0);
    check_not_null(server_flow);
    check_not_null(client_flow);
    memset(&state, 0, sizeof(state));
    atomic_init(&state.handler_called, 0);
    atomic_init(&state.capture_called, 0);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.endpoint = "/rpc-poll";
    check_int_eq(
        turbo_flow_rpc_register_server_adapter(server_flow, "rpc.server.poll", &server_config),
        TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(server_flow, "handler", rpc_echo_handler, &state, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(server_flow), TURBO_OK);

    snprintf(url, sizeof(url), "http://127.0.0.1:%u/rpc-poll", (unsigned)port);
    memset(&client_config, 0, sizeof(client_config));
    client_config.url = url;
    client_config.method = "echo";
    client_config.timeout_ms = 2000;
    client_config.poll_params = params;
    client_config.poll_interval_ms = 20;
    check_int_eq(
        turbo_flow_rpc_register_client_adapter(client_flow, "rpc.client.poll", &client_config),
        TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(client_flow, "capture", rpc_capture, &state, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(client_flow), TURBO_OK);
    for (int i = 0;
         i < 200 && atomic_load_explicit(&state.capture_called, memory_order_acquire) == 0; ++i) {
      turbo_sleep_ms(10);
    }
    check_int_gt(atomic_load_explicit(&state.handler_called, memory_order_acquire), 0);
    check_int_gt(atomic_load_explicit(&state.capture_called, memory_order_acquire), 0);
    check_size_eq(state.payload_len, sizeof(params) - 1);
    check_mem_eq(state.payload, params, sizeof(params) - 1);
    check_int_eq(turbo_flow_stop(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(server_flow), TURBO_OK);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
  }
}
