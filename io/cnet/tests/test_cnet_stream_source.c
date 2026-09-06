#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include "stream_source_pipe_fixture.h"

#include <salts/clock.h>

#include <stdio.h>
#include <string.h>

int cnet_stream_source_header_cpp_probe(void);

static cnet_client_config stream_source_test_client_config(void) {
  const cnet_client_config config = {.backend =
#if defined(_WIN32)
                                         NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
                                         NATIVE_IO_BACKEND_EPOLL,
#else
                                         NATIVE_IO_BACKEND_KQUEUE,
#endif
                                     .connection_capacity = 1u,
                                     .command_capacity = 8u,
                                     .request_capacity = 4u,
                                     .completion_batch_capacity = 4u,
                                     .event_capacity = 8u,
                                     .max_send_bytes = 256u,
                                     .receive_buffer_bytes = 256u};
  return config;
}

static turbo_flow_cnet_stream_source_config_t
stream_source_test_config(turbo_flow_t *flow, const cnet_client_config *client) {
  turbo_flow_cnet_stream_source_config_t config = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
  config.flow = flow;
  config.source_name = "input";
  config.uri = "tcp://127.0.0.1:9";
  config.client = client;
  config.max_message_bytes = 256u;
  config.scheduler_capacity = 8u;
  config.scheduler_max_steps_per_poll = 32u;
  config.first_message_id = 1u;
  return config;
}

typedef struct stream_source_graph_probe_s {
  size_t count;
  uint64_t ids[2];
  char payloads[2][16];
} stream_source_graph_probe_t;

typedef struct stream_source_server_probe_s {
  size_t connected;
  size_t sent;
  size_t terminal;
  int failed;
} stream_source_server_probe_t;

static int stream_source_graph_sink(turbo_flow_msg_t *message, void *ctx) {
  stream_source_graph_probe_t *probe = (stream_source_graph_probe_t *)ctx;
  size_t copy_size;
  if (!probe || !message || probe->count >= 2u) return SALTS_EPROTO;
  copy_size = message->payload.len;
  if (copy_size >= sizeof(probe->payloads[0])) return SALTS_EMSGSIZE;
  probe->ids[probe->count] = message->id;
  if (copy_size > 0u) memcpy(probe->payloads[probe->count], message->payload.data, copy_size);
  probe->payloads[probe->count][copy_size] = '\0';
  ++probe->count;
  return SALTS_OK;
}

static turbo_flow_t *stream_source_started_flow(stream_source_graph_probe_t *probe) {
  static const char dsl[] = "source input\n"
                            "stage sink\n"
                            "stage main {\n"
                            "  input -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_parse_string(flow, dsl, strlen(dsl)) != SALTS_OK ||
      turbo_flow_register_stage_ex(flow, "sink", stream_source_graph_sink, probe, NULL) !=
          SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static void stream_source_server_state(void *ctx, cnet_connection connection,
                                       cnet_connection_state state, const cnet_error *error) {
  stream_source_server_probe_t *probe = (stream_source_server_probe_t *)ctx;
  (void)connection;
  if (state == CNET_CONNECTION_CONNECTED) ++probe->connected;
  if (state == CNET_CONNECTION_CLOSED || state == CNET_CONNECTION_FAILED) ++probe->terminal;
  if (state == CNET_CONNECTION_FAILED || error != NULL) probe->failed = 1;
}

static void stream_source_server_receive(void *ctx, cnet_connection connection,
                                         const cnet_receive_view *view) {
  (void)ctx;
  (void)connection;
  (void)view;
}

static void stream_source_server_send(void *ctx, cnet_connection connection, size_t size) {
  stream_source_server_probe_t *probe = (stream_source_server_probe_t *)ctx;
  (void)connection;
  if (size == 0u) probe->failed = 1;
  ++probe->sent;
}

static int stream_source_poll_server(cnet_client *server, uint32_t timeout_ms) {
  size_t events = 0u;
  return cnet_client_poll(server, timeout_ms, &events);
}

static int stream_source_connect_pair(turbo_flow_cnet_stream_source_t *source, cnet_client *server,
                                      cnet_listener *listener, const cnet_observer *server_observer,
                                      cnet_connection *server_connection,
                                      turbo_flow_cnet_stream_source_snapshot_t *snapshot,
                                      uint32_t timeout_ms) {
  uint64_t deadline = salts_monotonic_ms() + timeout_ms;
  int accepted = 0;
  while ((!accepted || snapshot->state != TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED) &&
         salts_monotonic_ms() < deadline) {
    int ready = 0;
    int rc = turbo_flow_cnet_stream_source_poll(source, 1u, snapshot);
    if (rc != SALTS_OK) return rc;
    if (!accepted) {
      rc = cnet_listener_wait(listener, 0u, &ready);
      if (rc != SALTS_OK) return rc;
      if (ready) {
        rc = cnet_listener_accept(listener, server, server_observer, server_connection);
        if (rc != SALTS_OK) return rc;
        accepted = 1;
      }
    }
    rc = stream_source_poll_server(server, 0u);
    if (rc != SALTS_OK) return rc;
  }
  return accepted && snapshot->state == TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED ? SALTS_OK
                                                                                : SALTS_ETIMEDOUT;
}

spec("CNet stream source owner") {
  it("exposes a size-versioned C and C++ opaque contract") {
    turbo_flow_cnet_stream_source_config_t config = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
    turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION);
    check_equal(snapshot.size, sizeof(snapshot));
    check_equal(snapshot.version, TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION);
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_NEW);
    check_equal(snapshot.status, SALTS_OK);
    check_equal(cnet_stream_source_header_cpp_probe(), 0);
  }

  it("rejects invalid configuration before publishing an owner") {
    cnet_client_config client = stream_source_test_client_config();
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)1;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);

    check_not_null(flow);
    check_equal(turbo_flow_cnet_stream_source_open(NULL, &source), SALTS_EINVAL);
    check_null(source);

    source = (turbo_flow_cnet_stream_source_t *)1;
    config.size = 0u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = stream_source_test_config(flow, &client);
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EBUSY);
    check_null(source);

    turbo_flow_destroy(flow);
  }

  it("rejects unsupported transport and invalid hard bounds") {
    static const char dsl[] = "source input\n";
    cnet_client_config client = stream_source_test_client_config();
    cnet_client_config tls_client = stream_source_test_client_config();
    cnet_tls_client_config tls = {.size = sizeof(tls)};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_cnet_stream_source_t *source = NULL;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    config.uri = "udp://127.0.0.1:9";
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_ENOTSUP);
    check_null(source);

    config.uri = "tls://127.0.0.1:443";
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_ENOTSUP);
    check_null(source);

    config.uri = "tcp://127.0.0.1:9";
    config.tls = &tls;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    tls_client.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    tls_client.tls_handshake_timeout_ms = 1000u;
    tls.ca_file = "turbo-flow-definitely-missing-ca.pem";
    config = stream_source_test_config(flow, &tls_client);
    config.uri = "tls://127.0.0.1:443";
    config.tls = &tls;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EIO);
    check_null(source);

    config.uri = "tcp://127.0.0.1:9";
    config.tls = NULL;
    config.first_message_id = 0u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = stream_source_test_config(flow, &client);
    config.max_message_bytes = client.receive_buffer_bytes + 1u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    config = stream_source_test_config(flow, &client);
    config.scheduler_capacity = 0u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_EINVAL);
    check_null(source);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("routes Pipe through the platform transport and preserves its connect failure") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config client = stream_source_test_client_config();
    stream_source_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow = stream_source_started_flow(&graph_probe);
    turbo_flow_cnet_stream_source_t *source = NULL;
    turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);
    char uri[128];
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(flow);
    check_true(native_io_backend_kind_supports_pipe(client.backend));
    check_greater(snprintf(uri, sizeof(uri), "pipe://turbo-flow-missing-%llu",
                           (unsigned long long)salts_monotonic_ms()),
                  0);
    config.uri = uri;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_OK);
    check_not_null(source);

    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED &&
           salts_monotonic_ms() < deadline) {
      poll_status = turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot);
      if (poll_status != SALTS_OK) break;
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_FAILED);
    check_equal(poll_status, snapshot.status);
    check_true(snapshot.error_stage[0] != '\0');
    check_equal(graph_probe.count, 0u);
    check_equal(turbo_flow_cnet_stream_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("applies the same demand and ownership contract to a platform Pipe") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config client = stream_source_test_client_config();
    stream_source_pipe_fixture_t pipe;
    stream_source_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow = stream_source_started_flow(&graph_probe);
    turbo_flow_cnet_stream_source_t *source = NULL;
    turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);
    char uri[640];
    uint64_t deadline;

    check_not_null(flow);
    check_true(native_io_backend_kind_supports_pipe(client.backend));
    check_equal(stream_source_pipe_fixture_start(&pipe), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "pipe://%s", pipe.name), 0);
    config.uri = uri;
    config.first_message_id = 301u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_poll(source, 0u, &snapshot), SALTS_OK);
    check_equal(stream_source_pipe_fixture_finish(&pipe), SALTS_OK);

    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED &&
           salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED);
    check_equal(stream_source_pipe_fixture_write(&pipe, "pipe", 4u), SALTS_OK);
    for (size_t i = 0u; i < 8u; ++i)
      check_equal(turbo_flow_cnet_stream_source_poll(source, 0u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 0u);
    check_false(snapshot.receive_pending);

    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count == 0u && salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.ids[0], 301u);
    check_equal(graph_probe.payloads[0], "pipe");

    check_equal(turbo_flow_cnet_stream_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_OK);
    stream_source_pipe_fixture_close(&pipe);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("admits one owning TCP message for each downstream demand") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config client = stream_source_test_client_config();
    cnet_client server = {0};
    cnet_listener listener = {0};
    cnet_listener_config listener_config = {
        .backend = client.backend, .host = "127.0.0.1", .port = 0u, .backlog = 2u};
    stream_source_server_probe_t server_probe = {0};
    stream_source_graph_probe_t graph_probe = {0};
    cnet_observer server_observer = {.on_state = stream_source_server_state,
                                     .on_receive = stream_source_server_receive,
                                     .user = &server_probe,
                                     .on_send = stream_source_server_send};
    turbo_flow_t *flow = stream_source_started_flow(&graph_probe);
    turbo_flow_cnet_stream_source_t *source = NULL;
    turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);
    cnet_connection server_connection = {0};
    uint16_t port = 0u;
    char uri[96];
    uint64_t deadline;

    check_not_null(flow);
    check_equal(cnet_client_init(&server, &client), SALTS_OK);
    check_equal(cnet_listener_init(&listener, &listener_config), SALTS_OK);
    check_equal(cnet_listener_port(&listener, &port), SALTS_OK);
    check_true(port != 0u);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);
    config.uri = uri;
    config.first_message_id = 101u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_OK);
    check_not_null(source);

    check_equal(stream_source_connect_pair(source, &server, &listener, &server_observer,
                                           &server_connection, &snapshot, TEST_TIMEOUT_MS),
                SALTS_OK);
    check_equal(server_probe.connected, 1u);
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED);

    check_equal(cnet_send(&server, server_connection, "first", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (server_probe.sent < 1u && salts_monotonic_ms() < deadline)
      check_equal(stream_source_poll_server(&server, 1u), SALTS_OK);
    check_equal(server_probe.sent, 1u);
    for (size_t i = 0u; i < 8u; ++i)
      check_equal(turbo_flow_cnet_stream_source_poll(source, 0u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 0u);
    check_false(snapshot.receive_pending);

    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count < 1u && salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.ids[0], 101u);
    check_equal(graph_probe.payloads[0], "first");

    check_equal(cnet_send(&server, server_connection, "second", 6u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (server_probe.sent < 2u && salts_monotonic_ms() < deadline)
      check_equal(stream_source_poll_server(&server, 1u), SALTS_OK);
    for (size_t i = 0u; i < 8u; ++i)
      check_equal(turbo_flow_cnet_stream_source_poll(source, 0u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 1u);

    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (graph_probe.count < 2u && salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot), SALTS_OK);
    check_equal(graph_probe.count, 2u);
    check_equal(graph_probe.ids[1], 102u);
    check_equal(graph_probe.payloads[1], "second");
    check_equal(snapshot.messages_received, 2u);
    check_equal(snapshot.bytes_received, 11u);

    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_EBUSY);
    check_equal(cnet_close(&server, server_connection), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED &&
           salts_monotonic_ms() < deadline) {
      check_equal(stream_source_poll_server(&server, 1u), SALTS_OK);
      check_equal(turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED);
    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_stream_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_stop(source, TEST_TIMEOUT_MS), SALTS_EALREADY);
    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_stream_source_poll(source, 0u, &snapshot), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_OK);
    check_equal(cnet_listener_close(&listener), SALTS_OK);
    check_equal(cnet_listener_destroy(&listener), SALTS_OK);
    check_equal(cnet_client_stop(&server, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&server), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("preserves an asynchronous connection failure until explicit stop") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config client = stream_source_test_client_config();
    cnet_listener listener = {0};
    cnet_listener_config listener_config = {
        .backend = client.backend, .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    stream_source_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow = stream_source_started_flow(&graph_probe);
    turbo_flow_cnet_stream_source_t *source = NULL;
    turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);
    uint16_t unused_port = 0u;
    char uri[96];
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(flow);
    check_equal(cnet_listener_init(&listener, &listener_config), SALTS_OK);
    check_equal(cnet_listener_port(&listener, &unused_port), SALTS_OK);
    check_equal(cnet_listener_close(&listener), SALTS_OK);
    check_equal(cnet_listener_destroy(&listener), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)unused_port), 0);
    config.uri = uri;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_EBUSY);

    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED &&
           salts_monotonic_ms() < deadline) {
      poll_status = turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot);
      if (poll_status != SALTS_OK) break;
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_FAILED);
    check_true(snapshot.status != SALTS_OK);
    check_equal(poll_status, snapshot.status);
    check_true(snapshot.error_stage[0] != '\0');
    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), snapshot.status);
    check_equal(graph_probe.count, 0u);

    check_equal(turbo_flow_cnet_stream_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails an oversized receive without publishing a partial message") {
    enum { TEST_TIMEOUT_MS = 5000 };
    cnet_client_config client = stream_source_test_client_config();
    cnet_client server = {0};
    cnet_listener listener = {0};
    cnet_listener_config listener_config = {
        .backend = client.backend, .host = "127.0.0.1", .port = 0u, .backlog = 1u};
    stream_source_server_probe_t server_probe = {0};
    stream_source_graph_probe_t graph_probe = {0};
    cnet_observer server_observer = {.on_state = stream_source_server_state,
                                     .on_receive = stream_source_server_receive,
                                     .user = &server_probe,
                                     .on_send = stream_source_server_send};
    turbo_flow_t *flow = stream_source_started_flow(&graph_probe);
    turbo_flow_cnet_stream_source_t *source = NULL;
    turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_source_config_t config = stream_source_test_config(flow, &client);
    cnet_connection server_connection = {0};
    uint16_t port = 0u;
    char uri[96];
    uint64_t deadline;
    int poll_status = SALTS_OK;

    check_not_null(flow);
    check_equal(cnet_client_init(&server, &client), SALTS_OK);
    check_equal(cnet_listener_init(&listener, &listener_config), SALTS_OK);
    check_equal(cnet_listener_port(&listener, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);
    config.uri = uri;
    config.max_message_bytes = 4u;
    check_equal(turbo_flow_cnet_stream_source_open(&config, &source), SALTS_OK);
    check_equal(stream_source_connect_pair(source, &server, &listener, &server_observer,
                                           &server_connection, &snapshot, TEST_TIMEOUT_MS),
                SALTS_OK);

    check_equal(turbo_flow_cnet_stream_source_request(source, 1u), SALTS_OK);
    check_equal(cnet_send(&server, server_connection, "large", 5u), SALTS_OK);
    deadline = salts_monotonic_ms() + TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED &&
           salts_monotonic_ms() < deadline) {
      check_equal(stream_source_poll_server(&server, 1u), SALTS_OK);
      poll_status = turbo_flow_cnet_stream_source_poll(source, 1u, &snapshot);
      if (poll_status != SALTS_OK) break;
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SOURCE_FAILED);
    check_equal(snapshot.status, SALTS_EMSGSIZE);
    check_equal(poll_status, SALTS_EMSGSIZE);
    check_equal(snapshot.messages_received, 0u);
    check_equal(snapshot.bytes_received, 0u);
    check_equal(graph_probe.count, 0u);

    check_equal(turbo_flow_cnet_stream_source_stop(source, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_source_destroy(source), SALTS_OK);
    check_equal(cnet_listener_close(&listener), SALTS_OK);
    check_equal(cnet_listener_destroy(&listener), SALTS_OK);
    check_equal(cnet_client_stop(&server, TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&server), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
