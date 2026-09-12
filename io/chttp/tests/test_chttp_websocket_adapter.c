#include "../../../tests/flow_operation_fixture.h"
#include "tinytest.h"

#include "../../cnet/tests/listener_source_tls_fixture.h"

#include "turbo_flow_chttp.h"

#include <salts/thread.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum {
  WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS = 5000,
  WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES = 64u * 1024u,
  WEBSOCKET_ADAPTER_TEST_H2_STREAM_CAPACITY = 4u
};

static native_io_backend_kind websocket_adapter_test_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static cnet_client_config websocket_adapter_test_network(size_t connections) {
  const cnet_client_config config = {.backend = websocket_adapter_test_backend(),
                                     .connection_capacity = connections,
                                     .command_capacity = 16u,
                                     .command_buffer_bytes = 64u * 1024u,
                                     .request_capacity = 8u,
                                     .completion_batch_capacity = 8u,
                                     .event_capacity = 16u,
                                     .event_buffer_bytes = 64u * 1024u,
                                     .max_send_bytes = 8192u,
                                     .receive_buffer_bytes = 4096u,
                                     .connect_timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS,
                                     .read_timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS,
                                     .write_timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS};
  return config;
}

static chttp_server_config websocket_adapter_test_server_config(void) {
  const chttp_server_config config = {.host = "127.0.0.1",
                                      .port = 0u,
                                      .backlog = 8u,
                                      .network = websocket_adapter_test_network(4u),
                                      .route_capacity = 2u,
                                      .middleware_capacity = 2u,
                                      .max_route_middleware_count = 2u,
                                      .max_route_param_count = 2u,
                                      .max_route_param_bytes = 64u,
                                      .max_target_bytes = 256u,
                                      .max_header_count = 16u,
                                      .max_header_bytes = 2048u,
                                      .max_request_body_bytes = 256u,
                                      .max_response_header_count = 16u,
                                      .max_response_header_bytes = 1024u,
                                      .max_response_body_bytes = 256u,
                                      .poll_slice_ms = 1u};
  return config;
}

static chttp_websocket_client_config websocket_adapter_test_client_config(void) {
  const chttp_websocket_client_config config = {.size = sizeof(config),
                                                .network = websocket_adapter_test_network(1u),
                                                .max_frame_bytes = 4096u,
                                                .max_message_bytes = 4096u,
                                                .max_buffered_input_bytes = 8192u,
                                                .max_handshake_header_bytes = 4096u,
                                                .event_capacity = 8u};
  return config;
}

static void websocket_adapter_test_enable_h2(chttp_server_config *server,
                                             chttp_websocket_client_config *client) {
  server->network.max_send_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  server->network.receive_buffer_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  server->enable_http2 = 1;
  server->h2_stream_capacity = WEBSOCKET_ADAPTER_TEST_H2_STREAM_CAPACITY;
  server->h2_input_buffer_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  server->h2_output_buffer_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  server->h2_hpack_dynamic_table_bytes = 4096u;
  server->h2_max_settings_count = 16u;
  client->network.max_send_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  client->network.receive_buffer_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  client->h2_input_buffer_bytes = WEBSOCKET_ADAPTER_TEST_H2_BUFFER_BYTES;
  client->h2_hpack_dynamic_table_bytes = 4096u;
  client->h2_max_settings_count = 16u;
}

typedef struct websocket_adapter_event_probe_s {
  atomic_uint text;
  atomic_uint binary;
  atomic_uint ping;
  atomic_uint pong;
  atomic_uint close;
} websocket_adapter_event_probe_t;

typedef struct websocket_adapter_gate_s {
  atomic_int entered;
  atomic_int release;
} websocket_adapter_gate_t;

typedef struct websocket_adapter_stop_s {
  turbo_flow_t *flow;
  atomic_int status;
} websocket_adapter_stop_t;

typedef struct websocket_adapter_stale_probe_s {
  turbo_flow_msg_t saved;
  atomic_int saved_ready;
  atomic_int completion_done;
  atomic_int completion_status;
} websocket_adapter_stale_probe_t;

static int websocket_adapter_gate(turbo_flow_msg_t *message, void *ctx) {
  websocket_adapter_gate_t *gate = (websocket_adapter_gate_t *)ctx;
  (void)message;
  atomic_store_explicit(&gate->entered, 1, memory_order_release);
  while (!atomic_load_explicit(&gate->release, memory_order_acquire))
    salts_sleep_ms(1u);
  return SALTS_OK;
}

static void websocket_adapter_stop_thread(void *ctx) {
  websocket_adapter_stop_t *stop = (websocket_adapter_stop_t *)ctx;
  atomic_store_explicit(&stop->status, turbo_flow_stop(stop->flow), memory_order_release);
}

static int websocket_adapter_capture(turbo_flow_msg_t *message, void *ctx) {
  websocket_adapter_stale_probe_t *probe = (websocket_adapter_stale_probe_t *)ctx;
  if (!probe || atomic_load_explicit(&probe->saved_ready, memory_order_acquire)) return SALTS_OK;
  if (message->type != TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT) return SALTS_OK;
  if (turbo_flow_msg_clone(&probe->saved, message) != SALTS_OK) return SALTS_ENOMEM;
  atomic_store_explicit(&probe->saved_ready, 1, memory_order_release);
  return SALTS_OK;
}

static void websocket_adapter_stale_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  websocket_adapter_stale_probe_t *probe = (websocket_adapter_stale_probe_t *)ctx;
  atomic_store_explicit(&probe->completion_status, result ? result->status : SALTS_EINVAL,
                        memory_order_relaxed);
  atomic_store_explicit(&probe->completion_done, 1, memory_order_release);
}

static int websocket_adapter_route_control(turbo_flow_msg_t *message, void *ctx) {
  websocket_adapter_event_probe_t *probe = (websocket_adapter_event_probe_t *)ctx;
  const turbo_flow_chttp_websocket_event_context_t *event =
      turbo_flow_chttp_websocket_event_context(message);
  if (!probe || !event || event->session.impl == NULL) return SALTS_EPROTO;
  switch (event->event_type) {
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT:
    atomic_fetch_add_explicit(&probe->text, 1u, memory_order_relaxed);
    if (message->payload.len == sizeof("send-ping") - 1u &&
        memcmp(message->payload.data, "send-ping", sizeof("send-ping") - 1u) == 0)
      message->type = TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PING;
    else if (message->payload.len == sizeof("send-pong") - 1u &&
             memcmp(message->payload.data, "send-pong", sizeof("send-pong") - 1u) == 0)
      message->type = TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PONG;
    else if (message->payload.len == sizeof("send-close") - 1u &&
             memcmp(message->payload.data, "send-close", sizeof("send-close") - 1u) == 0) {
      message->type = TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE;
      message->status = 1000;
    }
    break;
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_BINARY:
    atomic_fetch_add_explicit(&probe->binary, 1u, memory_order_relaxed);
    break;
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PING:
    atomic_fetch_add_explicit(&probe->ping, 1u, memory_order_relaxed);
    message->type = TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT;
    break;
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PONG:
    atomic_fetch_add_explicit(&probe->pong, 1u, memory_order_relaxed);
    message->type = TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT;
    break;
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE:
    atomic_fetch_add_explicit(&probe->close, 1u, memory_order_relaxed);
    break;
  default:
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

spec("TurboFlow CHTTP WebSocket adapter") {
  static websocket_adapter_gate_t *ws_cleanup_gate;
  static turbo_flow_t *ws_cleanup_flow;
  static turbo_flow_chttp_websocket_server_t *ws_cleanup_server;
  static chttp_websocket_client *ws_cleanup_client;

  after_each() {
    if (ws_cleanup_gate) atomic_store_explicit(&ws_cleanup_gate->release, 1, memory_order_release);
    if (ws_cleanup_client) {
      check_equal(
          chttp_websocket_client_destroy(ws_cleanup_client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
          SALTS_OK);
      ws_cleanup_client = NULL;
    }
    if (ws_cleanup_flow) {
      check_equal(turbo_flow_stop(ws_cleanup_flow), SALTS_OK);
      turbo_flow_destroy(ws_cleanup_flow);
      ws_cleanup_flow = NULL;
    }
    if (ws_cleanup_server) {
      check_equal(turbo_flow_chttp_websocket_server_destroy(ws_cleanup_server), SALTS_OK);
      ws_cleanup_server = NULL;
    }
    ws_cleanup_gate = NULL;
  }

  it("rejects impossible byte and session bounds before adapter registration") {
    chttp_server_config native_config = websocket_adapter_test_server_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = native_config.network.connection_capacity + 1u;
    config.frame_capacity = 8u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);

    config.session_capacity = native_config.network.connection_capacity;
    config.frame_capacity = 0u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);

    config.frame_capacity = 8u;
    config.max_buffered_input_bytes = config.max_frame_bytes;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);

    config.max_frame_bytes = native_config.network.max_send_bytes;
    config.max_message_bytes = config.max_frame_bytes;
    config.max_buffered_input_bytes =
        config.max_frame_bytes + TURBO_FLOW_CHTTP_WEBSOCKET_MAX_WIRE_HEADER_BYTES;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);
    turbo_flow_destroy(flow);
  }

  it("registers one managed bidirectional WebSocket boundary with declared frame schemas") {
    chttp_server_config native_config = websocket_adapter_test_server_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 8u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_equal(descriptor.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_equal(descriptor.uid, "chttp-websocket:ws.server");
    check_equal(descriptor.owner_name, "ws.server");
    check_equal(descriptor.role_flags,
                (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_SOURCE |
                           TURBO_FLOW_MANAGED_BOUNDARY_SINK));
    check_equal(descriptor.capability_flags, (uint32_t)0u);
    check_equal(descriptor.command_flags,
                (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE |
                           TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME));
    check_equal(descriptor.input.domain, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN);
    check_equal(descriptor.input.profile, TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA);
    check_equal(descriptor.input.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(descriptor.input.media_type, "application/octet-stream");
    check_equal(descriptor.input.schema_name, "CHTTPWebSocketCommand");
    check_equal(descriptor.input.type_name, "Frame");
    check_equal(descriptor.input.schema_version, (uint32_t)1u);
    check_equal(descriptor.input.identity, "ws.server");
    check_equal(descriptor.output.domain, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN);
    check_equal(descriptor.output.profile, TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA);
    check_equal(descriptor.output.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(descriptor.output.media_type, "application/octet-stream");
    check_equal(descriptor.output.schema_name, "CHTTPWebSocketEvent");
    check_equal(descriptor.output.type_name, "Frame");
    check_equal(descriptor.output.schema_version, (uint32_t)1u);
    check_equal(descriptor.output.identity, "ws.server");

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("derives deterministic distinct bounded WebSocket identities for long adapter names") {
    chttp_server_config native_config = websocket_adapter_test_server_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_t *first_server = NULL;
    turbo_flow_chttp_websocket_server_t *same_server = NULL;
    turbo_flow_chttp_websocket_server_t *different_server = NULL;
    turbo_flow_managed_boundary_descriptor_t first =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t same =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t different =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *first_flow = turbo_flow_create();
    turbo_flow_t *same_flow = turbo_flow_create();
    turbo_flow_t *different_flow = turbo_flow_create();
    char long_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 258u];
    char different_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 258u];

    memset(long_name, 'w', sizeof(long_name) - 1u);
    long_name[sizeof(long_name) - 1u] = '\0';
    memset(different_name, 'x', sizeof(different_name) - 1u);
    different_name[sizeof(different_name) - 1u] = '\0';
    check_not_null(first_flow);
    check_not_null(same_flow);
    check_not_null(different_flow);
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 8u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    config.flow = first_flow;
    config.adapter_name = long_name;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &first_server), SALTS_OK);
    config.flow = same_flow;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &same_server), SALTS_OK);
    config.flow = different_flow;
    config.adapter_name = different_name;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &different_server), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(first_flow, 0u, &first), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(same_flow, 0u, &same), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(different_flow, 0u, &different),
                SALTS_OK);
    check_equal(strncmp(first.owner_name, "xxh3-128:", sizeof("xxh3-128:") - 1u), 0);
    check_equal(strncmp(first.uid, "chttp-websocket:", sizeof("chttp-websocket:") - 1u), 0);
    check_equal(first.owner_name, same.owner_name);
    check_equal(first.uid, same.uid);
    check_not_equal(first.owner_name, different.owner_name);
    check_not_equal(first.uid, different.uid);
    check_true(strlen(first.owner_name) <= TURBO_FLOW_RESOURCE_OWNER_MAX);
    check_true(strlen(first.uid) <= TURBO_FLOW_RESOURCE_UID_MAX);
    turbo_flow_destroy(first_flow);
    turbo_flow_destroy(same_flow);
    turbo_flow_destroy(different_flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(first_server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_destroy(same_server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_destroy(different_server), SALTS_OK);
  }

  it("round-trips one HTTP/1.1 text frame through a Flow source and sink") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage capture operation test.capture\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> capture -> ws_out\n"
                             "}\n";
    static const char payload[] = "flow-websocket";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    websocket_adapter_stale_probe_t stale;
    chttp_websocket_connect_options connect_options = {.size = sizeof(connect_options)};
    chttp_websocket_client client = {0};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];

    check_not_null(flow);
    memset(&stale, 0, sizeof(stale));
    turbo_flow_msg_init(&stale.saved);
    atomic_init(&stale.saved_ready, 0);
    atomic_init(&stale.completion_done, 0);
    atomic_init(&stale.completion_status, SALTS_EBUSY);
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 8u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_capture_0 =
        flow_test_operation_init("test.capture", websocket_adapter_capture, &stale);
    operation_capture_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_capture_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_capture_0), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING);
    check_true(snapshot.bound_port != 0u);

    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    connect_options.uri = uri;
    connect_options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &connect_options, &http_status), SALTS_OK);
    check_equal(http_status, 101u);
    check_equal(chttp_websocket_client_send_text(&client, payload, sizeof(payload) - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    check_equal(event.message_type, CHTTP_WEBSOCKET_MESSAGE_TEXT);
    check_equal(event.size, sizeof(payload) - 1u);
    check_equal(memcmp(event.data, payload, sizeof(payload) - 1u), 0);

    check_equal(
        chttp_websocket_client_close(&client, 1000u, NULL, 0u, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
        SALTS_OK);
    check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.active_sessions == 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.active_sessions, (size_t)0u);
    check_equal(atomic_load_explicit(&stale.saved_ready, memory_order_acquire), 1);
    check_equal(turbo_flow_publish_async(flow, "ws_in", &stale.saved,
                                         websocket_adapter_stale_complete, &stale),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      if (atomic_load_explicit(&stale.completion_done, memory_order_acquire)) break;
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&stale.completion_done, memory_order_acquire), 1);
    check_equal(atomic_load_explicit(&stale.completion_status, memory_order_relaxed), SALTS_ENOENT);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
    turbo_flow_msg_cleanup(&stale.saved);
  }

  it("rejects a second session and retires the admitted session on peer disconnect") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_client first = {0};
    chttp_websocket_client second = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];

    check_not_null(flow);
    native_config.network.connection_capacity = 2u;
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 1u;
    config.frame_capacity = 4u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    options.uri = uri;
    options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;

    check_equal(chttp_websocket_client_init(&first, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&first, &options, &http_status), SALTS_OK);
    check_equal(http_status, 101u);
    http_status = 0u;
    check_equal(chttp_websocket_client_init(&second, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&second, &options, &http_status), SALTS_EPROTO);
    check_equal(http_status, 503u);
    check_equal(chttp_websocket_client_destroy(&second, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_sessions, (size_t)1u);
    check_equal(snapshot.sessions_opened, (uint64_t)1u);
    check_equal(snapshot.sessions_rejected, (uint64_t)1u);

    check_equal(chttp_websocket_client_destroy(&first, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.active_sessions == 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.active_sessions, (size_t)0u);
    check_equal(snapshot.sessions_closed, (uint64_t)1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("quiesces idle WebSocket sessions and resumes only new sessions") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_client first = {0};
    chttp_websocket_client second = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];

    check_not_null(flow);
    native_config.network.connection_capacity = 2u;
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 2u;
    config.frame_capacity = 4u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    options.uri = uri;
    options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;

    check_equal(chttp_websocket_client_init(&first, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&first, &options, &http_status), SALTS_OK);
    check_equal(http_status, 101u);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED);
    http_status = 0u;
    check_equal(chttp_websocket_client_init(&second, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&second, &options, &http_status), SALTS_EPROTO);
    check_equal(http_status, 503u);
    check_equal(chttp_websocket_client_destroy(&second, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);

    check_equal(snapshot.sessions_opened, (uint64_t)1u);
    check_equal(snapshot.sessions_rejected, (uint64_t)1u);

    check_equal(chttp_websocket_client_destroy(&first, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.active_sessions == 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.active_sessions, (size_t)0u);
    check_equal(snapshot.sessions_closed, (uint64_t)1u);
    check_equal(turbo_flow_chttp_websocket_server_resume(server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_resume(server), SALTS_OK);
    check_equal(chttp_websocket_client_init(&second, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&second, &options, &http_status), SALTS_OK);
    check_equal(http_status, 101u);
    check_equal(chttp_websocket_client_destroy(&second, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_resume(server), SALTS_ESHUTDOWN);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("maps binary and control events to typed Flow commands") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage route_control operation test.route_control\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> route_control -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    websocket_adapter_event_probe_t probe;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options connect_options = {.size = sizeof(connect_options)};
    chttp_websocket_client client = {0};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];

    memset(&probe, 0, sizeof(probe));
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 8u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_route_control_1 =
        flow_test_operation_init("test.route_control", websocket_adapter_route_control, &probe);
    operation_route_control_1.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_route_control_1.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_route_control_1), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    connect_options.uri = uri;
    connect_options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &connect_options, &http_status), SALTS_OK);

    check_equal(chttp_websocket_client_send_binary(&client, "binary", sizeof("binary") - 1u,
                                                   WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    check_equal(event.message_type, CHTTP_WEBSOCKET_MESSAGE_BINARY);
    check_equal(event.size, sizeof("binary") - 1u);

    check_equal(chttp_websocket_client_send_text(&client, "send-ping", sizeof("send-ping") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_PING);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    check_equal(event.message_type, CHTTP_WEBSOCKET_MESSAGE_TEXT);
    check_equal(atomic_load_explicit(&probe.pong, memory_order_relaxed), 1u);

    check_equal(chttp_websocket_client_send_ping(&client, "source-ping", sizeof("source-ping") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_PONG);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    check_equal(event.size, sizeof("source-ping") - 1u);

    check_equal(chttp_websocket_client_send_pong(&client, "source-pong", sizeof("source-pong") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    check_equal(event.size, sizeof("source-pong") - 1u);

    check_equal(chttp_websocket_client_send_text(&client, "send-close", sizeof("send-close") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_CLOSE);
    check_equal(event.close_code, 1000u);
    check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.active_sessions == 0u && snapshot.in_flight_frames == 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.active_sessions, (size_t)0u);
    check_equal(snapshot.in_flight_frames, (size_t)0u);
    check_true(snapshot.frames_admitted >= 6u);
    check_true(snapshot.commands_admitted >= 5u);
    check_true(snapshot.frames_rejected >= 1u);
    check_equal(snapshot.last_status, SALTS_EALREADY);
    check_equal(atomic_load_explicit(&probe.binary, memory_order_relaxed), 1u);
    check_equal(atomic_load_explicit(&probe.ping, memory_order_relaxed), 1u);
    check_equal(atomic_load_explicit(&probe.pong, memory_order_relaxed), 2u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("drains an admitted frame before Flow stop closes the CHTTP owner") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage gate operation test.gate\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> gate -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    websocket_adapter_gate_t gate;
    websocket_adapter_stop_t stop;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_client client = {0};
    salts_thread_t stop_thread = NULL;
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];

    atomic_init(&gate.entered, 0);
    atomic_init(&gate.release, 0);
    stop.flow = flow;
    atomic_init(&stop.status, SALTS_EBUSY);
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 4u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_gate_2 =
        flow_test_operation_init("test.gate", websocket_adapter_gate, &gate);
    operation_gate_2.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_gate_2.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_gate_2), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    options.uri = uri;
    options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
    check_equal(chttp_websocket_client_send_text(&client, "drain", sizeof("drain") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      if (atomic_load_explicit(&gate.entered, memory_order_acquire)) break;
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
    check_equal(salts_thread_create(&stop_thread, websocket_adapter_stop_thread, &stop), SALTS_OK);
    salts_sleep_ms(20u);
    check_equal(atomic_load_explicit(&stop.status, memory_order_acquire), SALTS_EBUSY);
    atomic_store_explicit(&gate.release, 1, memory_order_release);
    check_equal(salts_thread_join(&stop_thread), SALTS_OK);
    check_equal(atomic_load_explicit(&stop.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED);
    check_equal(snapshot.in_flight_frames, (size_t)0u);
    check_equal(snapshot.frames_completed, (uint64_t)1u);

    check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("drains accepted H1 and H2 frames before quiesced session close") {
    for (size_t protocol_index = 0u; protocol_index < 2u; ++protocol_index) {
      static const char *dsl = "source ws_in adapter ws.server\n"
                               "stage gate operation test.gate\n"
                               "stage ws_out adapter ws.server\n"
                               "stage main {\n"
                               "  ws_in -> gate -> ws_out\n"
                               "}\n";
      chttp_server_config native_config = websocket_adapter_test_server_config();
      chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
      turbo_flow_chttp_websocket_server_config_t config =
          TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
      turbo_flow_chttp_websocket_server_snapshot_t snapshot =
          TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
      turbo_flow_managed_boundary_descriptor_t descriptor =
          TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
      turbo_flow_managed_boundary_snapshot_t managed =
          TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
      turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      static websocket_adapter_gate_t gate;
      turbo_flow_chttp_websocket_server_t *server = NULL;
      chttp_websocket_connect_options options = {.size = sizeof(options)};
      static chttp_websocket_client client;
      chttp_websocket_event event = {0};
      unsigned int http_status = 0u;
      turbo_flow_t *flow = turbo_flow_create();
      char uri[128];

      atomic_init(&gate.entered, 0);
      atomic_init(&gate.release, 0);
      ws_cleanup_gate = &gate;
      ws_cleanup_flow = flow;
      if (protocol_index != 0u) {
        websocket_adapter_test_enable_h2(&native_config, &client_config);
        options.protocol = CHTTP_HTTP_2;
      }
      config.flow = flow;
      config.adapter_name = "ws.server";
      config.source_name = "ws_in";
      config.server = &native_config;
      config.path = "/flow";
      config.session_capacity = 4u;
      config.frame_capacity = 4u;
      config.max_frame_bytes = 4096u;
      config.max_message_bytes = 4096u;
      config.max_buffered_input_bytes = 8192u;
      check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
      ws_cleanup_server = server;
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      flow_test_operation_t operation_gate_3 =
          flow_test_operation_init("test.gate", websocket_adapter_gate, &gate);
      operation_gate_3.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation_gate_3.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation_gate_3), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                          (unsigned int)snapshot.bound_port) > 0);
      options.uri = uri;
      options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
      check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
      ws_cleanup_client = &client;
      check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
      check_equal(chttp_websocket_client_send_text(&client, "drain", sizeof("drain") - 1u,
                                                   WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                  SALTS_OK);
      for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
        if (atomic_load_explicit(&gate.entered, memory_order_acquire)) break;
        salts_sleep_ms(1u);
      }
      check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
      check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
      check_equal(managed.queue_depth, (uint64_t)0u);
      check_equal(managed.queue_capacity, (uint64_t)4u);
      check_equal(managed.in_flight, (uint64_t)1u);
      check_equal(managed.accepted, (uint64_t)1u);
      check_equal(managed.completed, (uint64_t)0u);
      check_equal(managed.rejected, (uint64_t)0u);
      check_equal(managed.backpressured, 0);
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
      command.expected_generation = managed.generation;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "websocket-drain-quiesce",
             sizeof("websocket-drain-quiesce"));
      check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
      check_equal(result.generation_after, managed.generation + 1u);
      managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_DRAINING);
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
      command.expected_generation = managed.generation;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "websocket-drain-resume",
             sizeof("websocket-drain-resume"));
      check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
      managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
      check_equal(chttp_websocket_client_send_text(&client, "late", sizeof("late") - 1u,
                                                   WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                  SALTS_OK);
      for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
        check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
        if (snapshot.frames_rejected != 0u) break;
        salts_sleep_ms(1u);
      }
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING);
      check_equal(snapshot.frames_rejected, (uint64_t)1u);
      check_equal(snapshot.in_flight_frames, (size_t)1u);
      check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_OK);
      atomic_store_explicit(&gate.release, 1, memory_order_release);
      check_equal(
          chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
          SALTS_OK);
      check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
      check_equal(event.size, sizeof("drain") - 1u);
      check_equal(memcmp(event.data, "drain", event.size), 0);
      check_equal(
          chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
          SALTS_OK);
      check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_CLOSE);
      check_equal(event.close_code, (uint16_t)1013u);
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.in_flight_frames, (size_t)0u);
      check_equal(snapshot.frames_completed, (uint64_t)1u);
      for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
        managed =
            (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
        check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
        if (managed.state == TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT) break;
        salts_sleep_ms(1u);
      }
      check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT);
      check_equal(managed.in_flight, (uint64_t)0u);
      check_equal(managed.accepted, (uint64_t)1u);
      check_equal(managed.completed, (uint64_t)1u);
      check_equal(managed.rejected, (uint64_t)2u);

      check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                  SALTS_OK);
      ws_cleanup_client = NULL;
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      ws_cleanup_flow = NULL;
      check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
      ws_cleanup_server = NULL;
    }
  }

  it("isolates two RFC 8441 sibling streams on one HTTP/2 connection") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    chttp_websocket_pool_config pool_config = {.size = sizeof(pool_config)};
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options),
                                               .timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS,
                                               .protocol = CHTTP_HTTP_2};
    chttp_websocket_pool pool = {0};
    chttp_websocket_session first = {0};
    chttp_websocket_session second = {0};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char first_uri[128];
    char second_uri[128];

    native_config.network.connection_capacity = 1u;
    websocket_adapter_test_enable_h2(&native_config, &client_config);
    pool_config.client = client_config;
    pool_config.session_capacity = 2u;
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow/:id";
    config.session_capacity = 2u;
    config.frame_capacity = 2u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(first_uri, sizeof(first_uri), "ws://127.0.0.1:%u/flow/first",
                        (unsigned int)snapshot.bound_port) > 0);
    check_true(snprintf(second_uri, sizeof(second_uri), "ws://127.0.0.1:%u/flow/second",
                        (unsigned int)snapshot.bound_port) > 0);
    check_equal(chttp_websocket_pool_init(&pool, &pool_config), SALTS_OK);
    options.uri = first_uri;
    check_equal(chttp_websocket_pool_open(&pool, &options, &first, &http_status), SALTS_OK);
    check_equal(http_status, 200u);
    options.uri = second_uri;
    check_equal(chttp_websocket_pool_open(&pool, &options, &second, &http_status), SALTS_OK);
    check_equal(http_status, 200u);

    check_equal(chttp_websocket_pool_send_text(&pool, first, "first", sizeof("first") - 1u,
                                               WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_pool_send_text(&pool, second, "second", sizeof("second") - 1u,
                                               WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(
        chttp_websocket_pool_receive(&pool, second, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
        SALTS_OK);
    check_equal(event.size, sizeof("second") - 1u);
    check_equal(memcmp(event.data, "second", sizeof("second") - 1u), 0);
    check_equal(
        chttp_websocket_pool_receive(&pool, first, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
        SALTS_OK);
    check_equal(event.size, sizeof("first") - 1u);
    check_equal(memcmp(event.data, "first", sizeof("first") - 1u), 0);

    check_equal(chttp_websocket_pool_close(&pool, first, 1000u, NULL, 0u,
                                           WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_pool_close(&pool, first, 1000u, NULL, 0u,
                                           WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_ENOENT);
    check_equal(chttp_websocket_pool_send_text(&pool, first, "stale", sizeof("stale") - 1u,
                                               WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_ENOENT);
    check_equal(chttp_websocket_pool_send_text(&pool, second, "alive", sizeof("alive") - 1u,
                                               WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(
        chttp_websocket_pool_receive(&pool, second, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
        SALTS_OK);
    check_equal(event.size, sizeof("alive") - 1u);
    check_equal(memcmp(event.data, "alive", sizeof("alive") - 1u), 0);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_sessions, (size_t)1u);
    check_equal(snapshot.sessions_opened, (uint64_t)2u);

    check_equal(chttp_websocket_pool_close(&pool, second, 1000u, NULL, 0u,
                                           WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_pool_destroy(&pool, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("closes only the saturated session when the bounded frame source has no slot") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage gate operation test.gate\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> gate -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    websocket_adapter_gate_t gate;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_client client = {0};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];
    int saw_close = 0;

    atomic_init(&gate.entered, 0);
    atomic_init(&gate.release, 0);
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 1u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_gate_4 =
        flow_test_operation_init("test.gate", websocket_adapter_gate, &gate);
    operation_gate_4.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_gate_4.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_gate_4), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    options.uri = uri;
    options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
    check_equal(chttp_websocket_client_send_text(&client, "first", sizeof("first") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      if (atomic_load_explicit(&gate.entered, memory_order_acquire)) break;
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)1u);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.backpressured, 1);
    check_equal(chttp_websocket_client_send_text(&client, "second", sizeof("second") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.frames_rejected != 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.frames_rejected, (uint64_t)1u);
    atomic_store_explicit(&gate.release, 1, memory_order_release);
    for (size_t attempt = 0u; attempt < 3u && !saw_close; ++attempt) {
      check_equal(
          chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
          SALTS_OK);
      saw_close = event.kind == CHTTP_WEBSOCKET_EVENT_CLOSE;
    }
    check_true(saw_close);
    check_equal(event.close_code, 1013u);
    check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snapshot.frames_rejected >= 1u);
    check_equal(snapshot.frame_capacity, (size_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("propagates bounded Flow ingress demand as a session-local close") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage gate operation test.gate\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> gate -> ws_out\n"
                             "}\n";
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    websocket_adapter_gate_t gate;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_client client = {0};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];
    int saw_close = 0;

    check_not_null(flow);
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.release, 0);
    ingress.workers = 1u;
    ingress.queue_capacity = 1u;
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/flow";
    config.session_capacity = 4u;
    config.frame_capacity = 2u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(turbo_flow_configure_async_ingress(flow, &ingress), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_gate_5 =
        flow_test_operation_init("test.gate", websocket_adapter_gate, &gate);
    operation_gate_5.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_gate_5.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_gate_5), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow",
                        (unsigned int)snapshot.bound_port) > 0);
    options.uri = uri;
    options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);

    check_equal(chttp_websocket_client_send_text(&client, "one", sizeof("one") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      if (atomic_load_explicit(&gate.entered, memory_order_acquire)) break;
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), 1);
    check_equal(chttp_websocket_client_send_text(&client, "two", sizeof("two") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.frames_admitted >= 2u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.frames_admitted, (uint64_t)2u);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.queue_capacity, (uint64_t)2u);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)2u);
    check_equal(managed.accepted, (uint64_t)2u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.backpressured, 1);
    check_equal(chttp_websocket_client_send_text(&client, "three", sizeof("three") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (size_t wait = 0u; wait < WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS; ++wait) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.frames_rejected != 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.frames_rejected, (uint64_t)1u);
    atomic_store_explicit(&gate.release, 1, memory_order_release);
    for (size_t attempt = 0u; attempt < 4u && !saw_close; ++attempt) {
      check_equal(
          chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
          SALTS_OK);
      saw_close = event.kind == CHTTP_WEBSOCKET_EVENT_CLOSE;
    }
    check_true(saw_close);
    check_equal(event.close_code, 1013u);
    check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snapshot.frames_rejected >= 1u);
    check_equal(snapshot.frame_capacity, (size_t)2u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("round-trips WSS with explicit CA SNI ALPN and subprotocol policy") {
    static const char *dsl = "source ws_in adapter ws.server\n"
                             "stage ws_out adapter ws.server\n"
                             "stage main {\n"
                             "  ws_in -> ws_out\n"
                             "}\n";
    static const char *const h1_alpn[] = {"http/1.1"};
    listener_source_tls_fixture_t fixture = {0};
    chttp_server_config native_config = websocket_adapter_test_server_config();
    chttp_websocket_client_config client_config = websocket_adapter_test_client_config();
    cnet_tls_server_config server_tls = {0};
    cnet_tls_client_config client_tls = {0};
    chttp_tls_profile profile = {0};
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_client client = {0};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[128];

    check_equal(listener_source_tls_fixture_init(&fixture), SALTS_OK);
    native_config.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    native_config.network.tls_handshake_timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    client_config.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    client_config.network.tls_handshake_timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    server_tls = (cnet_tls_server_config){.size = sizeof(server_tls),
                                          .cert_file = fixture.cert_path,
                                          .key_file = fixture.key_path,
                                          .client_auth = CNET_TLS_CLIENT_AUTH_NONE,
                                          .alpn_protocols = h1_alpn,
                                          .alpn_protocol_count = 1u};
    client_tls = (cnet_tls_client_config){.size = sizeof(client_tls),
                                          .ca_file = fixture.cert_path,
                                          .server_name = "localhost",
                                          .alpn_protocols = h1_alpn,
                                          .alpn_protocol_count = 1u};
    native_config.tls = &server_tls;
    config.flow = flow;
    config.adapter_name = "ws.server";
    config.source_name = "ws_in";
    config.server = &native_config;
    config.path = "/secure";
    config.subprotocol = "flow.v1";
    config.session_capacity = 4u;
    config.frame_capacity = 8u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    check_equal(chttp_tls_profile_init(&profile, &client_tls), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(snprintf(uri, sizeof(uri), "wss://127.0.0.1:%u/secure",
                        (unsigned int)snapshot.bound_port) > 0);
    options.uri = uri;
    options.tls = &profile;
    options.timeout_ms = WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS;
    options.subprotocol = "flow.v1";
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
    check_equal(http_status, 101u);
    check_equal(chttp_websocket_client_send_text(&client, "secure", sizeof("secure") - 1u,
                                                 WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS, &event),
                SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    check_equal(event.size, sizeof("secure") - 1u);
    check_equal(memcmp(event.data, "secure", sizeof("secure") - 1u), 0);
    check_equal(
        chttp_websocket_client_close(&client, 1000u, NULL, 0u, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
        SALTS_OK);
    check_equal(chttp_websocket_client_destroy(&client, WEBSOCKET_ADAPTER_TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
    check_equal(chttp_tls_profile_destroy(&profile), SALTS_OK);
    listener_source_tls_fixture_destroy(&fixture);
  }
}
