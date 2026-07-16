#include "data_bind.h"
#include "turbo_flow_http_client.h"
#include "turbo_flow_http_server.h"
#include "turbo_flow_observe.h"

#include "tinytest.h"
#include "turbo_str.h"
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

typedef struct http_capture_s {
  char payload[128];
  size_t payload_len;
  atomic_int called;
  turbo_flow_content_profile_t profile;
  turbo_flow_data_encoding_t encoding;
  char media_type[TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX + 1u];
} http_capture_t;

static int capture_response(turbo_flow_msg_t *msg, void *ctx) {
  http_capture_t *capture = (http_capture_t *)ctx;
  if (!msg || !capture || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  {
    const turbo_flow_content_descriptor_t *descriptor = turbo_flow_msg_content_descriptor(msg);
    if (descriptor) {
      capture->profile = descriptor->profile;
      capture->encoding = descriptor->encoding;
      memcpy(capture->media_type, descriptor->media_type, sizeof(capture->media_type));
    }
  }
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static unsigned short pick_loopback_port(void) {
  struct sockaddr_in addr;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int addr_len = (int)sizeof(addr);
#else
  int socket_handle = -1;
  socklen_t addr_len = (socklen_t)sizeof(addr);
#endif
  unsigned short port = 0;

#ifdef _WIN32
  {
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
  }
#endif
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
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

spec("turbo_flow_http") {
  it("registers HTTP client and server from one resolved YAML snapshot") {
    static const char yaml[] = "version: 1\n"
                               "fragments:\n"
                               "  connection:\n"
                               "    remote:\n"
                               "      url: http://127.0.0.1:1/orders\n"
                               "    local:\n"
                               "      port: 18081\n"
                               "      route: /orders\n"
                               "  timer:\n"
                               "    bounded:\n"
                               "      timeout_ms: 250\n"
                               "  thread:\n"
                               "    standard:\n"
                               "      headers: [\"Accept: application/json\"]\n"
                               "adapters:\n"
                               "  http.client:\n"
                               "    kind: http\n"
                               "    fragments:\n"
                               "      connection: remote\n"
                               "      timer: bounded\n"
                               "      thread: standard\n"
                               "    config:\n"
                               "      method: get\n"
                               "      max_response_size: 4096\n"
                               "  http.server:\n"
                               "    kind: http\n"
                               "    fragments:\n"
                               "      connection: local\n"
                               "    config:\n"
                               "      method: post\n"
                               "      max_body_size: 4096\n"
                               "      response_status: 202\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_graph_snapshot_t graph;
    const turbo_flow_adapter_schema_t *client_schema;
    const turbo_flow_adapter_schema_t *server_schema;
    static const char dsl[] = "source request adapter http.server\n"
                              "stage call adapter http.client\n"
                              "stage response adapter http.server\n"
                              "stage main {\n"
                              "  request -> call -> response\n"
                              "}\n";

    check_not_null(flow);
    check_not_null(observe);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_http_register_client_resolved_adapter(flow, resolved, "http.client"),
                 TURBO_OK);
    check_int_eq(turbo_flow_http_register_server_resolved_adapter(flow, resolved, "http.server"),
                 TURBO_OK);
    client_schema = turbo_flow_find_adapter_schema(flow, "http.client");
    server_schema = turbo_flow_find_adapter_schema(flow, "http.server");
    check_not_null(client_schema);
    check_not_null(server_schema);
    check_uint_eq(client_schema->roles, TURBO_FLOW_ADAPTER_TRANSFORM);
    check_uint_eq(server_schema->roles, TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, flow), TURBO_OK);
    memset(&graph, 0, sizeof(graph));
    check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
    check_size_eq(graph.runtime.adapter_count, 2u);
    check_size_eq(graph.resource_providers, 3u);
    check_int_eq(turbo_flow_observe_detach(observe), TURBO_OK);
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("rejects host-only and mistyped fields in resolved HTTP adapters") {
    static const char yaml[] =
        "version: 1\nadapters:\n  http.client:\n    kind: http\n    config:\n"
        "      url: http://127.0.0.1:1\n      method: get\n      client: injected\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_http_register_client_resolved_adapter(flow, resolved, "http.client"),
                 TURBO_EINVAL);
    check_size_eq(turbo_flow_adapter_count(flow), 0u);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_destroy(flow);
  }

  it("rejects negative client timeouts at registration") {
    turbo_flow_http_client_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1";
    config.method = TURBO_FLOW_HTTP_GET;
    config.timeout_ms = -1;
    check_int_eq(turbo_flow_http_register_client_adapter(flow, "http.invalid", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("exposes a typed credential-free HTTP client status document") {
    turbo_flow_http_client_config_t config;
    turbo_flow_resource_metadata_t before = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_metadata_t after = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = NULL;
    DataBindValue *value = NULL;
    tstr_t payload = NULL;
    int32_t last_status = 0;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1/private?token=url-secret";
    config.method = TURBO_FLOW_HTTP_POST;
    config.bearer_token = "bearer-secret";
    check_int_eq(turbo_flow_http_register_client_adapter(flow, "http.private", &config), TURBO_OK);
    check_size_eq(turbo_flow_resource_metadata_count(flow), 1u);
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &before), TURBO_OK);
    check_str_eq(before.uid, "http:http.private");
    check_str_eq(before.owner_name, "http.private");
    check_int_eq(before.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_int_eq(before.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_int_eq(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_str_eq(document.schema->schema_name, "TurboFlowHttpResource");
    check_str_eq(document.schema->type_name, "HttpClientStatus");
    check_uint_eq(document.schema->schema_id, 101u);
    check_uint_eq(document.schema->schema_version, 1u);
    check_int_eq(turbo_flow_resource_document_validate(&document, document.schema), TURBO_OK);
    {
      turbo_flow_resource_schema_t unknown = *document.schema;
      unknown.schema_version = 2u;
      check_int_eq(turbo_flow_resource_document_validate(&document, &unknown), TURBO_EPROTO);
    }
    check_int_eq(data_bind_create_from_text(document.schema->schema_text,
                                            strlen(document.schema->schema_text), &codec, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_parse_json(codec, document.schema->type_name,
                                      mem_buffer_const_data(document.payload),
                                      mem_buffer_used(document.payload), &value, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_validate_json(codec, document.schema->type_name,
                                         "{\"state\":0,\"last_status\":-1}",
                                         sizeof("{\"state\":0,\"last_status\":-1}") - 1u, &error),
                 DATA_BIND_ERR_TYPE_MISMATCH);
    check_int_eq(
        data_bind_validate_json(codec, document.schema->type_name,
                                "{\"state\":\"bad\",\"last_status\":-1,"
                                "\"connections_current\":\"0\",\"connection_limit\":\"1\","
                                "\"in_flight_messages\":\"0\",\"in_flight_bytes\":\"0\"}",
                                sizeof("{\"state\":\"bad\",\"last_status\":-1,"
                                       "\"connections_current\":\"0\",\"connection_limit\":\"1\","
                                       "\"in_flight_messages\":\"0\",\"in_flight_bytes\":\"0\"}") -
                                    1u,
                                &error),
        DATA_BIND_ERR_TYPE_MISMATCH);
    check_int_eq(data_bind_value_get_int32(data_bind_value_get(value, "last_status"), &last_status),
                 DATA_BIND_OK);
    check_int_eq(last_status, TURBO_ENOTCONN);
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_null(strstr(payload, "url-secret"));
    check_null(strstr(payload, "bearer-secret"));
    check_null(strstr(payload, "/private"));
    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &after), TURBO_OK);
    check_size_eq(after.generation, before.generation);
    check_size_eq(after.observed_generation, before.observed_generation);

    tstr_freep(&payload);
    data_bind_value_free(value);
    data_bind_free(codec);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
  }

  it("rejects structurally incomplete schema bindings at registration") {
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    turbo_flow_http_server_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    binding.schema.schema_name = "http.orders";
    memset(&config, 0, sizeof(config));
    config.port = pick_loopback_port();
    config.route = "/orders";
    config.method = TURBO_FLOW_HTTP_POST;
    config.content_binding = &binding;
    check_int_eq(turbo_flow_http_register_server_adapter(flow, "http.orders", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("round trips payloads through server and client adapters") {
    static const char *server_dsl = "source request adapter http.server.echo\n"
                                    "stage response adapter http.server.echo\n"
                                    "stage main {\n"
                                    "  request -> response\n"
                                    "}\n";
    static const char *client_dsl = "source input\n"
                                    "stage request adapter http.client.once\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  input -> request -> capture\n"
                                    "}\n";
    unsigned short port = pick_loopback_port();
    char url[128];
    turbo_flow_http_server_config_t server_config;
    turbo_flow_http_client_config_t client_config;
    turbo_flow_content_descriptor_t input_descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_msg_t msg;
    turbo_flow_connection_snapshot_t server_connection;
    turbo_flow_connection_snapshot_t client_connection;
    http_capture_t capture;

    check_int_gt(port, 0);
    check_not_null(server_flow);
    check_not_null(client_flow);
    check_not_null(observe);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.route = "/echo";
    server_config.method = TURBO_FLOW_HTTP_POST;
    server_config.response_content_type = "application/octet-stream";
    check_int_eq(
        turbo_flow_http_register_server_adapter(server_flow, "http.server.echo", &server_config),
        TURBO_OK);
    memset(&server_connection, 0, sizeof(server_connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_int_eq(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(server_flow, 0, &server_connection),
                 TURBO_OK);
    check_int_eq(server_connection.state, TURBO_FLOW_CONNECTION_READY);
    check_size_eq(server_connection.connections_current, 1);
    check_int_gt(snprintf(url, sizeof(url), "http://0.0.0.0:%u/echo", (unsigned)port), 0);
    check_str_eq(server_connection.endpoint, url);

    snprintf(url, sizeof(url), "http://127.0.0.1:%u/echo", (unsigned)port);
    memset(&client_config, 0, sizeof(client_config));
    client_config.url = url;
    client_config.method = TURBO_FLOW_HTTP_POST;
    client_config.timeout_ms = 2000;
    client_config.max_pump_iterations = 20000;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    check_int_eq(
        turbo_flow_http_register_client_adapter(client_flow, "http.client.once", &client_config),
        TURBO_OK);
    memset(&client_connection, 0, sizeof(client_connection));
    check_int_eq(turbo_flow_adapter_connection_snapshot_at(client_flow, 0, &client_connection),
                 TURBO_OK);
    check_int_eq(client_connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_str_eq(client_connection.endpoint, url);
    check_int_eq(
        turbo_flow_register_stage_ex(client_flow, "capture", capture_response, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_observe_attach(observe, client_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(client_flow), TURBO_OK);

    {
      static const char rule[] =
          "when graph.connection_providers == 1 then adapter http.client.once quiesce";
      turbo_flow_observe_graph_snapshot_t graph;
      turbo_flow_observe_control_facts_snapshot_t facts_snapshot;
      turbo_flow_control_facts_t facts = TURBO_FLOW_CONTROL_FACTS_INIT;
      check_int_eq(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
      check_size_eq(graph.connection_providers, 1);
      check_size_eq(graph.connections_current, 0);
      check_int_eq(turbo_flow_observe_control_facts(observe, &facts_snapshot, &facts), TURBO_OK);
      check_int_eq(turbo_flow_control_ex(client_flow, rule, sizeof(rule) - 1u, &facts, NULL),
                   TURBO_OK);
      check_int_eq(turbo_flow_control(client_flow, "adapter http.client.once resume",
                                      sizeof("adapter http.client.once resume") - 1u),
                   TURBO_OK);
    }

    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("round-trip");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_int_eq(turbo_flow_content_descriptor_from_media(&input_descriptor, TURBO_FLOW_DOMAIN_DATA,
                                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                                          "application/json", "request-body", NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_msg_set_content_descriptor(&msg, &input_descriptor), TURBO_OK);
    check_int_eq(turbo_flow_publish(client_flow, "input", &msg), TURBO_OK);
    check_int_eq(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_size_eq(capture.payload_len, 10);
    check_mem_eq(capture.payload, "round-trip", 10);
    check_int_eq(capture.profile, TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY);
    check_int_eq(capture.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_str_eq(capture.media_type, "application/octet-stream");
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
    check_int_eq(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("publishes periodic GET responses as source messages") {
    static const char *server_dsl = "source request adapter http.server.poll\n"
                                    "stage response adapter http.server.poll\n"
                                    "stage main {\n"
                                    "  request -> response\n"
                                    "}\n";
    static const char *client_dsl = "source remote adapter http.client.poll\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  remote -> capture\n"
                                    "}\n";
    unsigned short port = pick_loopback_port();
    char url[128];
    turbo_flow_http_server_config_t server_config;
    turbo_flow_http_client_config_t client_config;
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    http_capture_t capture;

    check_int_gt(port, 0);
    check_not_null(server_flow);
    check_not_null(client_flow);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.route = "/poll";
    server_config.method = TURBO_FLOW_HTTP_GET;
    check_int_eq(
        turbo_flow_http_register_server_adapter(server_flow, "http.server.poll", &server_config),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(server_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(server_flow), TURBO_OK);

    snprintf(url, sizeof(url), "http://127.0.0.1:%u/poll", (unsigned)port);
    memset(&client_config, 0, sizeof(client_config));
    client_config.url = url;
    client_config.method = TURBO_FLOW_HTTP_GET;
    client_config.timeout_ms = 2000;
    client_config.poll_interval_ms = 20;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    check_int_eq(
        turbo_flow_http_register_client_adapter(client_flow, "http.client.poll", &client_config),
        TURBO_OK);
    check_int_eq(
        turbo_flow_register_stage_ex(client_flow, "capture", capture_response, &capture, NULL),
        TURBO_OK);
    check_int_eq(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_start(client_flow), TURBO_OK);
    for (int i = 0; i < 200 && atomic_load_explicit(&capture.called, memory_order_acquire) == 0;
         ++i) {
      turbo_sleep_ms(10);
    }
    check_int_gt(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    check_int_eq(turbo_flow_stop(client_flow), TURBO_OK);
    check_int_eq(turbo_flow_stop(server_flow), TURBO_OK);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
  }

  it("enables explicit retries only for idempotent transform methods") {
    static const char *get_dsl = "source input\n"
                                 "stage request adapter http.client.get retry attempts 2 delay 1\n"
                                 "stage main {\n"
                                 "  input -> request\n"
                                 "}\n";
    static const char *post_dsl = "source input\n"
                                  "stage request adapter http.client.post retry attempts 2\n"
                                  "stage main {\n"
                                  "  input -> request\n"
                                  "}\n";
    turbo_flow_http_client_config_t config;
    turbo_flow_t *get_flow = turbo_flow_create();
    turbo_flow_t *post_flow = turbo_flow_create();

    check_not_null(get_flow);
    check_not_null(post_flow);
    memset(&config, 0, sizeof(config));
    config.url = "http://127.0.0.1:1/retry";
    config.method = TURBO_FLOW_HTTP_GET;
    check_int_eq(turbo_flow_http_register_client_adapter(get_flow, "http.client.get", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(get_flow, get_dsl, strlen(get_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(get_flow), TURBO_OK);

    config.method = TURBO_FLOW_HTTP_POST;
    check_int_eq(turbo_flow_http_register_client_adapter(post_flow, "http.client.post", &config),
                 TURBO_OK);
    check_int_eq(turbo_flow_parse_string(post_flow, post_dsl, strlen(post_dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(post_flow), TURBO_ENOTSUP);
    check_str_contains(turbo_flow_last_error(post_flow)->message, "adapter retry callback");

    turbo_flow_destroy(get_flow);
    turbo_flow_destroy(post_flow);
  }
}
