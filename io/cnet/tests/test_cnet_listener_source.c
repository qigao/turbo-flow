#include "../../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include "listener_source_tls_fixture.h"

#include <salts/clock.h>

#include <stdio.h>
#include <string.h>

extern int cnet_listener_source_header_cpp_probe(void);

static native_io_backend_kind listener_source_test_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static cnet_client_config listener_source_test_client_config(void) {
  const cnet_client_config config = {.backend = listener_source_test_backend(),
                                     .connection_capacity = 2u,
                                     .command_capacity = 8u,
                                     .request_capacity = 8u,
                                     .completion_batch_capacity = 4u,
                                     .event_capacity = 8u,
                                     .max_send_bytes = 256u,
                                     .receive_buffer_bytes = 256u};
  return config;
}

static turbo_flow_cnet_listener_source_config_t
listener_source_test_config(turbo_flow_t *flow, const cnet_listener_config *listener,
                            const cnet_client_config *client) {
  turbo_flow_cnet_listener_source_config_t config = TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
  config.flow = flow;
  config.source_name = "input";
  config.listener = listener;
  config.client = client;
  config.max_connections = client->connection_capacity;
  config.max_message_bytes = client->receive_buffer_bytes;
  config.scheduler_capacity = 8u;
  config.scheduler_max_steps_per_poll = 32u;
  config.first_message_id = 1u;
  return config;
}

typedef struct listener_source_graph_probe_s {
  size_t count;
  uint64_t ids[4];
  cnet_connection connections[4];
  char payloads[4][32];
} listener_source_graph_probe_t;

typedef struct listener_source_client_probe_s {
  size_t connected;
  size_t sent;
  size_t terminal;
  size_t received;
  cnet_connection received_connections[4];
  char received_payloads[4][32];
  int failed;
} listener_source_client_probe_t;

static int listener_source_graph_sink(turbo_flow_msg_t *message, void *ctx) {
  listener_source_graph_probe_t *probe = (listener_source_graph_probe_t *)ctx;
  size_t copy_size;
  if (!probe || !message || probe->count >= 4u) return SALTS_EPROTO;
  copy_size = message->payload.len;
  if (copy_size >= sizeof(probe->payloads[0])) return SALTS_EMSGSIZE;
  probe->ids[probe->count] = message->id;
  {
    const turbo_flow_cnet_listener_message_context_t *transport =
        turbo_flow_cnet_listener_message_context(message);
    if (!transport) return SALTS_EPROTO;
    probe->connections[probe->count] = transport->connection;
  }
  if (copy_size > 0u) memcpy(probe->payloads[probe->count], message->payload.data, copy_size);
  probe->payloads[probe->count][copy_size] = '\0';
  ++probe->count;
  return SALTS_OK;
}

static turbo_flow_t *listener_source_started_flow(listener_source_graph_probe_t *probe) {
  static const char dsl[] = "source input\n"
                            "stage sink operation test.sink\n"
                            "stage main {\n"
                            "  input -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  flow_test_operation_t operation_sink =
      flow_test_operation_init("test.sink", listener_source_graph_sink, probe);
  operation_sink.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_sink.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  if (!flow || turbo_flow_parse_string(flow, dsl, strlen(dsl)) != SALTS_OK ||
      flow_test_operation_register(flow, &operation_sink) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static void listener_source_client_state(void *ctx, cnet_connection connection,
                                         cnet_connection_state state, const cnet_error *error) {
  listener_source_client_probe_t *probe = (listener_source_client_probe_t *)ctx;
  (void)connection;
  if (state == CNET_CONNECTION_CONNECTED) ++probe->connected;
  if (state == CNET_CONNECTION_CLOSED || state == CNET_CONNECTION_FAILED) ++probe->terminal;
  if (state == CNET_CONNECTION_FAILED || error != NULL) probe->failed = 1;
}

static void listener_source_client_receive(void *ctx, cnet_connection connection,
                                           const cnet_receive_view *view) {
  listener_source_client_probe_t *probe = (listener_source_client_probe_t *)ctx;
  size_t index;
  if (!probe || !view || !view->data || view->size == 0u ||
      probe->received >= 4u || view->size >= sizeof(probe->received_payloads[0]))
    return;
  index = probe->received++;
  probe->received_connections[index] = connection;
  memcpy(probe->received_payloads[index], view->data, view->size);
  probe->received_payloads[index][view->size] = '\0';
}

static void listener_source_client_send(void *ctx, cnet_connection connection, size_t size) {
  listener_source_client_probe_t *probe = (listener_source_client_probe_t *)ctx;
  (void)connection;
  if (size == 0u) probe->failed = 1;
  ++probe->sent;
}

static int listener_source_poll_client(cnet_client *client, uint32_t timeout_ms) {
  size_t events = 0u;
  return cnet_client_poll(client, timeout_ms, &events);
}

spec("CNet listener source owner") {
  it("exposes a size-versioned C and C++ opaque contract") {
    turbo_flow_cnet_listener_source_config_t config = TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION);
    check_equal(snapshot.size, sizeof(snapshot));
    check_equal(snapshot.version, TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION);
    check_equal(snapshot.state, TURBO_FLOW_CNET_LISTENER_SOURCE_NEW);
    turbo_flow_cnet_listener_reply_request_t request =
        TURBO_FLOW_CNET_LISTENER_REPLY_REQUEST_INIT;
    turbo_flow_cnet_listener_reply_terminal_t terminal =
        TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_INIT;
    check_equal(snapshot.status, SALTS_OK);
    check_equal(request.size, sizeof(request));
    check_equal(request.version, TURBO_FLOW_CNET_LISTENER_REPLY_API_VERSION);
    check_equal(terminal.size, sizeof(terminal));
    check_equal(terminal.version, TURBO_FLOW_CNET_LISTENER_REPLY_API_VERSION);
    check_equal(terminal.kind, TURBO_FLOW_CNET_LISTENER_REPLY_TERMINAL_NONE);
    check_equal(cnet_listener_source_header_cpp_probe(), 0);
  }

  it("rejects invalid configuration before publishing an owner") {
    static const char dsl[] = "source input\n";
    cnet_client_config client = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 2u};
    cnet_listener_options options = CNET_LISTENER_OPTIONS_INIT;
    cnet_tls_server_config tls = {
        .size = sizeof(tls), .cert_file = "unused-cert.pem", .key_file = "unused-key.pem"};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)1;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &client);

    check_not_null(flow);
    check_equal(turbo_flow_cnet_listener_source_open(NULL, &source), SALTS_EINVAL);
    check_null(source);

    source = (turbo_flow_cnet_listener_source_t *)1;
    config.size = 0u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EBUSY);
    check_null(source);

    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    config.max_connections = 0u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    config.max_connections = client.connection_capacity + 1u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    config.max_message_bytes = client.receive_buffer_bytes + 1u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    config.scheduler_capacity = 0u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    config.scheduler_max_steps_per_poll = 0u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    config.first_message_id = 0u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    options.reuse_port = 2;
    config = listener_source_test_config(flow, &listener, &client);
    config.listener_options = &options;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = listener_source_test_config(flow, &listener, &client);
    config.tls = &tls;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_ENOTSUP);
    check_null(source);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails closed when TLS server credentials cannot be loaded") {
    cnet_client_config client = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    cnet_tls_server_config tls = {.size = sizeof(tls),
                                  .cert_file = "turbo-flow-missing-server-cert.pem",
                                  .key_file = "turbo-flow-missing-server-key.pem",
                                  .client_auth = CNET_TLS_CLIENT_AUTH_NONE};
    listener_source_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)1;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &client);

    check_not_null(flow);
    client.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    client.tls_handshake_timeout_ms = 1000u;
    config.client = &client;
    config.tls = &tls;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_EIO);
    check_null(source);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("isolates a rejected TLS peer and preserves its connection error") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config server_client = listener_source_test_client_config();
    cnet_client_config client_config = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    listener_source_tls_fixture_t tls_fixture = {0};
    cnet_tls_server_config server_tls = {.size = sizeof(server_tls),
                                         .client_auth = CNET_TLS_CLIENT_AUTH_NONE};
    listener_source_graph_probe_t graph_probe = {0};
    listener_source_client_probe_t client_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &server_client);
    cnet_client client = {0};
    cnet_connection connection = {0};
    cnet_connect_options connect = {0};
    char uri[96];
    uint64_t deadline;

    check_not_null(flow);
    check_equal(listener_source_tls_fixture_init(&tls_fixture), SALTS_OK);
    server_client.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    server_client.tls_handshake_timeout_ms = 1000u;
    server_tls.cert_file = tls_fixture.cert_path;
    server_tls.key_file = tls_fixture.key_path;
    config.client = &server_client;
    config.tls = &server_tls;
    config.max_connections = 1u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(cnet_client_init(&client, &client_config), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    connect.uri = uri;
    connect.observer = (cnet_observer){.on_state = listener_source_client_state,
                                       .on_receive = listener_source_client_receive,
                                       .user = &client_probe,
                                       .on_send = listener_source_client_send};
    check_equal(cnet_connect(&client, &connect, &connection), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (client_probe.connected == 0u && client_probe.terminal == 0u &&
           salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(client_probe.connected, 1u);
    check_equal(cnet_send(&client, connection, "plain", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.connections_failed == 0u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING);
    check_equal(snapshot.status, SALTS_OK);
    check_equal(snapshot.connections_failed, 1u);
    check_not_equal(snapshot.last_connection_status, SALTS_OK);
    check_greater(strlen(snapshot.last_connection_error_stage), 0u);
    check_equal(snapshot.active_connections, 0u);

    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(cnet_client_stop(&client, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&client), SALTS_OK);
    listener_source_tls_fixture_destroy(&tls_fixture);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("accepts verified TLS and publishes only decrypted bytes under demand") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config server_client = listener_source_test_client_config();
    cnet_client_config client_config = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    listener_source_tls_fixture_t tls_fixture = {0};
    cnet_tls_server_config server_tls = {.size = sizeof(server_tls),
                                         .client_auth = CNET_TLS_CLIENT_AUTH_NONE};
    cnet_tls_client_config client_tls = {.size = sizeof(client_tls), .server_name = "localhost"};
    listener_source_graph_probe_t graph_probe = {0};
    listener_source_client_probe_t client_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &server_client);
    cnet_client client = {0};
    cnet_connection connection = {0};
    cnet_connect_options connect = {0};
    char uri[96];
    uint64_t deadline;

    check_not_null(flow);
    check_equal(listener_source_tls_fixture_init(&tls_fixture), SALTS_OK);
    server_client.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    server_client.tls_handshake_timeout_ms = 1000u;
    client_config.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    client_config.tls_handshake_timeout_ms = 1000u;
    server_tls.cert_file = tls_fixture.cert_path;
    server_tls.key_file = tls_fixture.key_path;
    client_tls.ca_file = tls_fixture.cert_path;
    config.client = &server_client;
    config.tls = &server_tls;
    config.max_connections = 1u;
    config.first_message_id = 301u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(cnet_client_init(&client, &client_config), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    connect.uri = uri;
    connect.tls = &client_tls;
    connect.observer = (cnet_observer){.on_state = listener_source_client_state,
                                       .on_receive = listener_source_client_receive,
                                       .user = &client_probe,
                                       .on_send = listener_source_client_send};
    check_equal(cnet_connect(&client, &connect, &connection), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (client_probe.connected == 0u && client_probe.terminal == 0u &&
           snapshot.state != TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED &&
           salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(client_probe.connected, 1u);
    check_equal(client_probe.failed, 0);
    check_equal(cnet_send(&client, connection, "tls", 3u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count == 0u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.ids[0], 301u);
    check_equal(graph_probe.payloads[0], "tls");

    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(cnet_client_stop(&client, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&client), SALTS_OK);
    listener_source_tls_fixture_destroy(&tls_fixture);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("accepts TCP while admitting one owning message only after downstream demand") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config server_client = listener_source_test_client_config();
    cnet_client_config client_config = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 2u};
    listener_source_graph_probe_t graph_probe = {0};
    listener_source_client_probe_t client_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &server_client);
    cnet_client client = {0};
    cnet_connection connection = {0};
    cnet_connect_options connect = {0};
    char uri[96];
    uint64_t deadline;

    check_not_null(flow);
    config.max_connections = 1u;
    config.first_message_id = 101u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_not_null(source);
    check_equal(turbo_flow_cnet_listener_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING);
    check_true(snapshot.bound_port != 0u);

    check_equal(cnet_client_init(&client, &client_config), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    connect.uri = uri;
    connect.observer = (cnet_observer){.on_state = listener_source_client_state,
                                       .on_receive = listener_source_client_receive,
                                       .user = &client_probe,
                                       .on_send = listener_source_client_send};
    check_equal(cnet_connect(&client, &connect, &connection), SALTS_OK);

    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while ((client_probe.connected == 0u || snapshot.active_connections == 0u) &&
           salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(client_probe.connected, 1u);
    check_equal(snapshot.connections_accepted, 1u);
    check_equal(snapshot.active_connections, 1u);

    check_equal(cnet_send(&client, connection, "first", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (client_probe.sent == 0u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 0u, &snapshot), SALTS_OK);
    }
    check_equal(client_probe.sent, 1u);
    for (size_t index = 0u; index < 8u; ++index)
      check_equal(turbo_flow_cnet_listener_source_poll(source, 0u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 0u);
    check_false(snapshot.receive_pending);

    check_equal(turbo_flow_cnet_listener_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count == 0u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.ids[0], 101u);
    check_equal(graph_probe.payloads[0], "first");
    check_equal(snapshot.messages_received, 1u);
    check_equal(snapshot.bytes_received, 5u);

    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(cnet_client_stop(&client, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&client), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("holds a second TCP peer in backlog until the bounded slot is reusable") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config server_client = listener_source_test_client_config();
    cnet_client_config client_config = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 2u};
    listener_source_graph_probe_t graph_probe = {0};
    listener_source_client_probe_t client_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &server_client);
    cnet_client client = {0};
    cnet_connection first = {0};
    cnet_connection second = {0};
    cnet_connect_options connect = {0};
    char uri[96];
    uint64_t deadline;

    check_not_null(flow);
    config.max_connections = 1u;
    config.first_message_id = 201u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(cnet_client_init(&client, &client_config), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    connect.uri = uri;
    connect.observer = (cnet_observer){.on_state = listener_source_client_state,
                                       .on_receive = listener_source_client_receive,
                                       .user = &client_probe,
                                       .on_send = listener_source_client_send};
    check_equal(cnet_connect(&client, &connect, &first), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while ((client_probe.connected < 1u || snapshot.connections_accepted == 0u) &&
           salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(client_probe.connected, 1u);
    check_equal(snapshot.connections_accepted, 1u);

    check_equal(cnet_connect(&client, &connect, &second), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (client_probe.connected < 2u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(client_probe.connected, 2u);
    check_equal(snapshot.connections_accepted, 1u);
    check_equal(snapshot.active_connections, 1u);

    check_equal(cnet_close(&client, first), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.connections_accepted < 2u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.connections_accepted, 2u);
    check_equal(snapshot.connections_closed, 1u);
    check_equal(snapshot.active_connections, 1u);

    check_equal(cnet_send(&client, second, "reuse", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count == 0u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.ids[0], 201u);
    check_equal(graph_probe.payloads[0], "reuse");

    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(cnet_client_stop(&client, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&client), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("advances two connected peers round-robin under batched demand") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config server_client = listener_source_test_client_config();
    cnet_client_config client_config = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 2u};
    listener_source_graph_probe_t graph_probe = {0};
    listener_source_client_probe_t client_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &server_client);
    cnet_client client = {0};
    cnet_connection first = {0};
    cnet_connection second = {0};
    cnet_connect_options connect = {0};
    char uri[96];
    uint64_t deadline;
    int saw_left;
    int saw_right;

    check_not_null(flow);
    config.max_connections = 2u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(cnet_client_init(&client, &client_config), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    connect.uri = uri;
    connect.observer = (cnet_observer){.on_state = listener_source_client_state,
                                       .on_receive = listener_source_client_receive,
                                       .user = &client_probe,
                                       .on_send = listener_source_client_send};
    check_equal(cnet_connect(&client, &connect, &first), SALTS_OK);
    check_equal(cnet_connect(&client, &connect, &second), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while ((client_probe.connected < 2u || snapshot.active_connections < 2u) &&
           salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.active_connections, 2u);
    check_equal(cnet_send(&client, first, "left", 4u), SALTS_OK);
    check_equal(cnet_send(&client, second, "right", 5u), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_request(source, 2u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count < 2u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(graph_probe.count, 2u);
    saw_left = strcmp(graph_probe.payloads[0], "left") == 0 ||
               strcmp(graph_probe.payloads[1], "left") == 0;
    saw_right = strcmp(graph_probe.payloads[0], "right") == 0 ||
                strcmp(graph_probe.payloads[1], "right") == 0;
    check_true(saw_left);
    check_true(saw_right);

    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(cnet_client_stop(&client, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&client), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails oversized input without publishing a partial message") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config server_client = listener_source_test_client_config();
    cnet_client_config client_config = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    listener_source_graph_probe_t graph_probe = {0};
    listener_source_client_probe_t client_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &server_client);
    cnet_client client = {0};
    cnet_connection connection = {0};
    cnet_connect_options connect = {0};
    char uri[96];
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(flow);
    config.max_connections = 1u;
    config.max_message_bytes = 4u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(cnet_client_init(&client, &client_config), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    connect.uri = uri;
    connect.observer = (cnet_observer){.on_state = listener_source_client_state,
                                       .on_receive = listener_source_client_receive,
                                       .user = &client_probe,
                                       .on_send = listener_source_client_send};
    check_equal(cnet_connect(&client, &connect, &connection), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.active_connections == 0u && salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(turbo_flow_cnet_listener_source_request(source, 1u), SALTS_OK);
    check_equal(cnet_send(&client, connection, "large", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED &&
           salts_monotonic_ms() < deadline) {
      check_equal(listener_source_poll_client(&client, 1u), SALTS_OK);
      poll_status = turbo_flow_cnet_listener_source_poll(source, 1u, &snapshot);
      if (poll_status != SALTS_OK) break;
    }
    check_equal(poll_status, SALTS_EMSGSIZE);
    check_equal(snapshot.status, SALTS_EMSGSIZE);
    check_equal(snapshot.messages_received, 0u);
    check_equal(snapshot.bytes_received, 0u);
    check_equal(graph_probe.count, 0u);

    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(cnet_client_stop(&client, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&client), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("enforces stop before destroy and rejects post-stop work") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config client = listener_source_test_client_config();
    cnet_listener_config listener = {
        .backend = listener_source_test_backend(), .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    listener_source_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow = listener_source_started_flow(&graph_probe);
    turbo_flow_cnet_listener_source_t *source = NULL;
    turbo_flow_cnet_listener_source_snapshot_t snapshot =
        TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_listener_source_config_t config =
        listener_source_test_config(flow, &listener, &client);

    check_not_null(flow);
    config.max_connections = 1u;
    check_equal(turbo_flow_cnet_listener_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_EBUSY);
    check_equal(turbo_flow_cnet_listener_source_request(source, 0u), SALTS_EINVAL);
    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_listener_source_stop(source, TEST_TIMEOUT_MS), SALTS_EALREADY);
    check_equal(turbo_flow_cnet_listener_source_request(source, 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_listener_source_poll(source, 0u, &snapshot), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_listener_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
