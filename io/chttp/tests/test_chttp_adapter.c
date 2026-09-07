#include "tinytest.h"
#include "turbo_flow_chttp.h"

#include <salts/clock.h>
#include <salts/thread.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { CHTTP_ADAPTER_TEST_TIMEOUT_MS = 5000 };

typedef struct chttp_adapter_probe_s {
  atomic_size_t sink_calls;
  atomic_size_t publication_calls;
  atomic_int publication_status;
  unsigned int status_code;
  uint32_t attempts;
  size_t headers_read;
  char payload[64];
  char reason[32];
} chttp_adapter_probe_t;

typedef struct chttp_adapter_deferred_probe_s {
  chttp_server_deferred handle;
  atomic_int acquired;
} chttp_adapter_deferred_probe_t;

static native_io_backend_kind chttp_adapter_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static cnet_client_config chttp_adapter_network(size_t connections) {
  const cnet_client_config config = {.backend = chttp_adapter_backend(),
                                     .connection_capacity = connections,
                                     .command_capacity = 16u,
                                     .request_capacity = 8u,
                                     .completion_batch_capacity = 8u,
                                     .event_capacity = 16u,
                                     .max_send_bytes = 4096u,
                                     .receive_buffer_bytes = 512u,
                                     .connect_timeout_ms = CHTTP_ADAPTER_TEST_TIMEOUT_MS,
                                     .read_timeout_ms = CHTTP_ADAPTER_TEST_TIMEOUT_MS,
                                     .write_timeout_ms = CHTTP_ADAPTER_TEST_TIMEOUT_MS};
  return config;
}

static chttp_server_config chttp_adapter_server_config(void) {
  const chttp_server_config config = {.host = "127.0.0.1",
                                      .port = 0u,
                                      .backlog = 8u,
                                      .network = chttp_adapter_network(4u),
                                      .route_capacity = 8u,
                                      .middleware_capacity = 1u,
                                      .max_route_middleware_count = 1u,
                                      .max_route_param_count = 1u,
                                      .max_route_param_bytes = 64u,
                                      .max_target_bytes = 256u,
                                      .max_header_count = 16u,
                                      .max_header_bytes = 1024u,
                                      .max_request_body_bytes = 512u,
                                      .max_response_header_count = 16u,
                                      .max_response_header_bytes = 1024u,
                                      .max_response_body_bytes = 512u,
                                      .poll_slice_ms = 1u};
  return config;
}

static chttp_client_config chttp_adapter_client_config(void) {
  const chttp_client_config config = {.network = chttp_adapter_network(2u),
                                      .request_capacity = 4u,
                                      .max_start_line_bytes = 256u,
                                      .max_header_count = 16u,
                                      .max_header_bytes = 1024u,
                                      .max_request_body_bytes = 512u,
                                      .max_response_body_bytes = 512u,
                                      .max_informational_responses = 2u};
  return config;
}

static chttp_server_config chttp_adapter_h2_server_config(void) {
  chttp_server_config config = chttp_adapter_server_config();
  config.network.max_send_bytes = 64u * 1024u;
  config.network.receive_buffer_bytes = 4096u;
  config.max_header_bytes = 4096u;
  config.max_request_body_bytes = 4096u;
  config.max_response_header_bytes = 4096u;
  config.max_response_body_bytes = 4096u;
  config.enable_http2 = 1;
  config.h2_stream_capacity = 4u;
  config.h2_input_buffer_bytes = 64u * 1024u;
  config.h2_output_buffer_bytes = 64u * 1024u;
  config.h2_hpack_dynamic_table_bytes = 4096u;
  config.h2_max_settings_count = 16u;
  return config;
}

static chttp_client_config chttp_adapter_h2_client_config(void) {
  chttp_client_config config = chttp_adapter_client_config();
  config.network.max_send_bytes = 64u * 1024u;
  config.network.receive_buffer_bytes = 4096u;
  config.max_header_bytes = 4096u;
  config.max_request_body_bytes = 4096u;
  config.max_response_body_bytes = 4096u;
  config.h2_input_buffer_bytes = 64u * 1024u;
  config.h2_hpack_dynamic_table_bytes = 4096u;
  config.h2_max_settings_count = 16u;
  return config;
}

static int chttp_adapter_echo(void *user, const chttp_server_request_view *request,
                              chttp_server_response *response) {
  (void)user;
  if (!request || request->method < CHTTP_METHOD_GET || request->method > CHTTP_METHOD_PATCH ||
      request->body_size != 4u || memcmp(request->body, "ping", 4u) != 0)
    return SALTS_EPROTO;
  return chttp_server_reply(response, 201u, "text/plain", "pong", 4u);
}

static int chttp_adapter_h2_isolation(void *user, const chttp_server_request_view *request,
                                      chttp_server_response *response) {
  static const char oversized_body[] =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  (void)user;
  if (!request || request->method != CHTTP_METHOD_GET) return SALTS_EPROTO;
  if (request->body_size == 5u && memcmp(request->body, "large", 5u) == 0)
    return chttp_server_reply(response, 200u, "text/plain", oversized_body,
                              sizeof(oversized_body) - 1u);
  if (request->body_size == 4u && memcmp(request->body, "ping", 4u) == 0)
    return chttp_server_reply(response, 200u, "text/plain", "pong", 4u);
  return SALTS_EPROTO;
}

static int chttp_adapter_defer(void *user, const chttp_server_request_view *request,
                               chttp_server_response *response) {
  chttp_adapter_deferred_probe_t *probe = (chttp_adapter_deferred_probe_t *)user;
  chttp_server_deferred handle = CHTTP_SERVER_DEFERRED_INIT;
  int status;
  (void)request;
  if (!probe) return SALTS_EINVAL;
  status = chttp_server_response_defer(response, &handle);
  if (status != SALTS_OK) return status;
  probe->handle = handle;
  atomic_store_explicit(&probe->acquired, 1, memory_order_release);
  return SALTS_OK;
}

static int chttp_adapter_close_after_reply(void *user, const chttp_server_request_view *request,
                                           chttp_server_response *response) {
  (void)user;
  (void)request;
  return chttp_server_reply(response, 200u, "text/plain", "fresh", 5u);
}

static int chttp_adapter_sink(turbo_flow_msg_t *message, void *ctx) {
  chttp_adapter_probe_t *probe = (chttp_adapter_probe_t *)ctx;
  const turbo_flow_chttp_response_context_t *response = turbo_flow_chttp_response_context(message);
  vstr reason = {0};
  size_t index;
  if (!probe || !response || message->payload.len >= sizeof(probe->payload) ||
      turbo_flow_chttp_response_reason(message, &reason) != SALTS_OK ||
      reason.len >= sizeof(probe->reason))
    return SALTS_EPROTO;
  memcpy(probe->payload, message->payload.data, message->payload.len);
  probe->payload[message->payload.len] = '\0';
  memcpy(probe->reason, reason.data, reason.len);
  probe->reason[reason.len] = '\0';
  probe->status_code = response->status_code;
  probe->attempts = response->attempts;
  for (index = 0u; index < response->header_count; ++index) {
    vstr name = {0};
    vstr value = {0};
    if (turbo_flow_chttp_response_header_at(message, index, &name, &value) != SALTS_OK ||
        !name.data || !value.data)
      return SALTS_EPROTO;
    ++probe->headers_read;
  }
  (void)atomic_fetch_add_explicit(&probe->sink_calls, 1u, memory_order_release);
  return SALTS_OK;
}

static void chttp_adapter_publication_complete(void *ctx,
                                               const turbo_flow_publish_result_t *result) {
  chttp_adapter_probe_t *probe = (chttp_adapter_probe_t *)ctx;
  atomic_store_explicit(&probe->publication_status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&probe->publication_calls, 1u, memory_order_release);
}

spec("TurboFlow CHTTP async client adapter") {
  it("supports six request methods and resumes with an owned response") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.request\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    const chttp_method methods[] = {CHTTP_METHOD_GET, CHTTP_METHOD_HEAD,   CHTTP_METHOD_POST,
                                    CHTTP_METHOD_PUT, CHTTP_METHOD_DELETE, CHTTP_METHOD_PATCH};
    size_t index;
    uint16_t port = 0u;
    char uri[64];

    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    for (index = 0u; index < sizeof(methods) / sizeof(methods[0]); ++index)
      check_equal(chttp_server_route(&server, methods[index], "/echo", chttp_adapter_echo, NULL),
                  SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    for (index = 0u; index < sizeof(methods) / sizeof(methods[0]); ++index) {
      turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
      turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
      turbo_flow_chttp_client_t *client = NULL;
      chttp_adapter_probe_t probe = {0};
      turbo_flow_msg_t message;
      turbo_flow_t *flow = turbo_flow_create();
      uint64_t deadline;

      check_not_null(flow);
      atomic_init(&probe.publication_status, SALTS_EALREADY);
      adapter_config.flow = flow;
      adapter_config.adapter_name = "http.request";
      adapter_config.client = &client_config;
      adapter_config.connection_uri = uri;
      adapter_config.authority = "127.0.0.1";
      adapter_config.target = "/echo";
      adapter_config.method = methods[index];
      check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &probe, NULL),
                  SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);

      turbo_flow_msg_init(&message);
      message.id = 17u + index;
      message.owned_payload = tstr_dup("ping");
      message.payload = tstr_to_v(message.owned_payload);
      check_equal(turbo_flow_publish_async(flow, "input", &message,
                                           chttp_adapter_publication_complete, &probe),
                  SALTS_OK);
      turbo_flow_msg_cleanup(&message);

      deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
      do {
        check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
      } while (atomic_load_explicit(&probe.publication_calls, memory_order_acquire) == 0u &&
               salts_monotonic_ms() < deadline);

      check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
      check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire), SALTS_OK);
      check_equal(atomic_load_explicit(&probe.sink_calls, memory_order_acquire), (size_t)1u);
      check_equal(probe.status_code, 201u);
      check_equal(probe.attempts, 1u);
      check_greater(probe.headers_read, (size_t)0u);
      check_equal(probe.payload, methods[index] == CHTTP_METHOD_HEAD ? "" : "pong");
      check_equal(probe.reason, "Created");
      check_equal(snapshot.active_requests, (size_t)0u);
      check_equal(snapshot.completed_requests, (uint64_t)1u);
      check_equal(snapshot.response_bytes,
                  methods[index] == CHTTP_METHOD_HEAD ? (uint64_t)0u : (uint64_t)4u);

      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    }
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("rejects capacity overflow and cancels an accepted request exactly once") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.deferred\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    const chttp_server_deferred_response deferred_response = {
        .size = sizeof(chttp_server_deferred_response),
        .status_code = 200u,
        .content_type = "text/plain",
        .body = "late",
        .body_size = 4u};
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_deferred_probe_t deferred = {.handle = CHTTP_SERVER_DEFERRED_INIT};
    chttp_adapter_probe_t first = {0};
    chttp_adapter_probe_t second = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint16_t port = 0u;
    char uri[64];

    check_not_null(flow);
    client_config.request_capacity = 1u;
    atomic_init(&deferred.acquired, 0);
    atomic_init(&first.publication_status, SALTS_EALREADY);
    atomic_init(&second.publication_status, SALTS_EALREADY);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_get(&server, "/deferred", chttp_adapter_defer, &deferred), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.deferred";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/deferred";
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &first, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.id = 101u;
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &first),
                SALTS_OK);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&deferred.acquired, memory_order_acquire) == 0 &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&deferred.acquired, memory_order_acquire), 1);
    check_equal(snapshot.active_requests, (size_t)1u);

    message.id = 102u;
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &second),
                SALTS_OK);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&second.publication_calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&second.publication_calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.publication_status, memory_order_acquire),
                SALTS_ENOBUFS);

    check_equal(turbo_flow_chttp_client_cancel(client, 101u), SALTS_OK);
    check_equal(turbo_flow_chttp_client_cancel(client, 101u), SALTS_EALREADY);
    check_equal(chttp_server_deferred_reply(&deferred.handle, &deferred_response), SALTS_OK);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&first.publication_calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&first.publication_calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.publication_status, memory_order_acquire),
                SALTS_ECANCELED);
    check_equal(atomic_load_explicit(&first.sink_calls, memory_order_acquire), (size_t)0u);
    check_equal(snapshot.canceled_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);
    check_equal(turbo_flow_chttp_client_cancel(client, 101u), SALTS_ENOENT);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("retries only replayable idempotent requests within one overall deadline") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.retry\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    const chttp_method methods[] = {CHTTP_METHOD_GET, CHTTP_METHOD_POST, CHTTP_METHOD_POST};
    const int explicit_idempotency[] = {0, 0, 1};
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    uint16_t closed_port = 0u;
    char uri[64];
    size_t index;

    client_config.network.connect_timeout_ms = 20u;
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &closed_port), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)closed_port), 0);

    for (index = 0u; index < sizeof(methods) / sizeof(methods[0]); ++index) {
      turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
      turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
      turbo_flow_chttp_client_t *client = NULL;
      chttp_adapter_probe_t probe = {0};
      turbo_flow_msg_t message;
      turbo_flow_t *flow = turbo_flow_create();
      uint64_t deadline;
      uint64_t started_at;

      check_not_null(flow);
      atomic_init(&probe.publication_status, SALTS_OK);
      adapter_config.flow = flow;
      adapter_config.adapter_name = "http.retry";
      adapter_config.client = &client_config;
      adapter_config.connection_uri = uri;
      adapter_config.authority = "127.0.0.1";
      adapter_config.target = "/unreachable";
      adapter_config.method = methods[index];
      adapter_config.overall_timeout_ms = 60u;
      adapter_config.max_attempts = 8u;
      adapter_config.retry_delay_ms = 20u;
      adapter_config.idempotent = explicit_idempotency[index];
      check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &probe, NULL),
                  SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);

      turbo_flow_msg_init(&message);
      message.id = 201u + index;
      message.owned_payload = tstr_dup("replayable");
      message.payload = tstr_to_v(message.owned_payload);
      started_at = salts_monotonic_ms();
      check_equal(turbo_flow_publish_async(flow, "input", &message,
                                           chttp_adapter_publication_complete, &probe),
                  SALTS_OK);
      turbo_flow_msg_cleanup(&message);
      deadline = started_at + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
      while (atomic_load_explicit(&probe.publication_calls, memory_order_acquire) == 0u &&
             salts_monotonic_ms() < deadline)
        check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);

      check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
      check_not_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                      SALTS_OK);
      check_equal(atomic_load_explicit(&probe.sink_calls, memory_order_acquire), (size_t)0u);
      check_equal(snapshot.completed_requests, (uint64_t)1u);
      if (methods[index] == CHTTP_METHOD_POST && !explicit_idempotency[index]) {
        check_equal(snapshot.retried_requests, (uint64_t)0u);
      } else {
        check_greater(snapshot.retried_requests, (uint64_t)0u);
        check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                    SALTS_ETIMEDOUT);
        check_less(salts_monotonic_ms() - started_at, (uint64_t)500u);
      }

      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    }
  }

  it("caps a long owner poll at the request overall deadline") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.deadline\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    const chttp_server_deferred_response deferred_response = {
        .size = sizeof(chttp_server_deferred_response),
        .status_code = 200u,
        .content_type = "text/plain",
        .body = "unused",
        .body_size = 6u};
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_deferred_probe_t deferred = {.handle = CHTTP_SERVER_DEFERRED_INIT};
    chttp_adapter_probe_t probe = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint64_t started_at;
    uint16_t port = 0u;
    char uri[64];

    check_not_null(flow);
    atomic_init(&deferred.acquired, 0);
    atomic_init(&probe.publication_status, SALTS_OK);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_get(&server, "/deadline", chttp_adapter_defer, &deferred), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.deadline";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/deadline";
    adapter_config.overall_timeout_ms = 40u;
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &probe, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.id = 301u;
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &probe),
                SALTS_OK);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&deferred.acquired, memory_order_acquire) == 0 &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&deferred.acquired, memory_order_acquire), 1);

    started_at = salts_monotonic_ms();
    while (atomic_load_explicit(&probe.publication_calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() - started_at < 2000u)
      check_equal(turbo_flow_chttp_client_poll(client, 1000u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                SALTS_ETIMEDOUT);
    check_less(salts_monotonic_ms() - started_at, (uint64_t)500u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(chttp_server_deferred_reply(&deferred.handle, &deferred_response), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("fails fast on contradictory protocol, TLS, retry and header configuration") {
    chttp_client_config client_config = chttp_adapter_client_config();
    turbo_flow_chttp_client_config_t config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_tls_profile tls = {0};
    chttp_header header = {"X-Test", "yes"};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "http.invalid";
    config.client = &client_config;
    config.connection_uri = "tcp://127.0.0.1:1";
    config.authority = "127.0.0.1";
    config.target = "/invalid";

    config.method = CHTTP_METHOD_OPTIONS;
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    check_null(client);
    config.method = CHTTP_METHOD_GET;
    config.max_attempts = 0u;
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    config.max_attempts = 1u;
    config.header_count = 1u;
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    config.headers = &header;
    config.header_count = client_config.max_header_count + 1u;
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    config.header_count = 0u;
    config.headers = NULL;
    config.protocol = CHTTP_HTTP_2;
    config.connection_uri = "pipe://explicit-h2-is-invalid";
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    config.protocol = CHTTP_HTTP_1_1;
    config.connection_uri = "tcp://127.0.0.1:1";
    config.tls = &tls;
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    config.connection_uri = "tls://127.0.0.1:1";
    config.tls = NULL;
    check_equal(turbo_flow_chttp_client_register(&config, &client), SALTS_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("reports an HTTP/2 to HTTP/1.1 protocol mismatch without fallback") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.protocol\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_h2_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_probe_t probe = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint16_t port = 0u;
    char uri[64];

    check_not_null(flow);
    atomic_init(&probe.publication_status, SALTS_OK);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_get(&server, "/protocol", chttp_adapter_echo, NULL), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.protocol";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/protocol";
    adapter_config.protocol = CHTTP_HTTP_2;
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &probe, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.id = 401u;
    message.owned_payload = tstr_dup("ping");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &probe),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    } while (atomic_load_explicit(&probe.publication_calls, memory_order_acquire) == 0u &&
             salts_monotonic_ms() < deadline);
    check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                SALTS_EPROTO);
    check_equal(atomic_load_explicit(&probe.sink_calls, memory_order_acquire), (size_t)0u);
    check_equal(snapshot.submitted_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("reports request and response body bounds without a fallback") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.bounds\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_probe_t request_limit = {0};
    chttp_adapter_probe_t response_limit = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint16_t port = 0u;
    char uri[64];

    check_not_null(flow);
    client_config.max_request_body_bytes = 4u;
    client_config.max_response_body_bytes = 3u;
    atomic_init(&request_limit.publication_status, SALTS_OK);
    atomic_init(&response_limit.publication_status, SALTS_OK);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_post(&server, "/bounds", chttp_adapter_echo, NULL), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.bounds";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/bounds";
    adapter_config.method = CHTTP_METHOD_POST;
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &response_limit, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.id = 301u;
    message.owned_payload = tstr_dup("12345");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &request_limit),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&request_limit.publication_calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&request_limit.publication_status, memory_order_acquire),
                SALTS_EMSGSIZE);

    turbo_flow_msg_init(&message);
    message.id = 302u;
    message.owned_payload = tstr_dup("ping");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &response_limit),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&response_limit.publication_calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&response_limit.publication_status, memory_order_acquire),
                SALTS_EMSGSIZE);
    check_equal(atomic_load_explicit(&response_limit.sink_calls, memory_order_acquire), (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("reconnects after an HTTP/1.1 response closes the pooled connection") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.h1\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_probe_t probe = {0};
    chttp_server_stats server_stats = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint16_t port = 0u;
    char uri[64];
    size_t index;

    check_not_null(flow);
    atomic_init(&probe.publication_status, SALTS_EALREADY);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_get(&server, "/close", chttp_adapter_close_after_reply, NULL),
                SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.h1";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/close";
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &probe, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    for (index = 0u; index < 2u; ++index) {
      if (index == 1u) {
        uint16_t rebound_port = 0u;
        check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
        check_equal(chttp_server_destroy(&server), SALTS_OK);
        memset(&server, 0, sizeof(server));
        server_config.port = port;
        check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
        check_equal(chttp_server_get(&server, "/close", chttp_adapter_close_after_reply, NULL),
                    SALTS_OK);
        check_equal(chttp_server_start(&server), SALTS_OK);
        check_equal(chttp_server_port(&server, &rebound_port), SALTS_OK);
        check_equal(rebound_port, port);
      }
      turbo_flow_msg_init(&message);
      message.id = 401u + index;
      check_equal(turbo_flow_publish_async(flow, "input", &message,
                                           chttp_adapter_publication_complete, &probe),
                  SALTS_OK);
      deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
      while (atomic_load_explicit(&probe.publication_calls, memory_order_acquire) < index + 1u &&
             salts_monotonic_ms() < deadline)
        check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
      check_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire), SALTS_OK);
    }
    check_equal(atomic_load_explicit(&probe.sink_calls, memory_order_acquire), (size_t)2u);
    check_equal(probe.payload, "fresh");
    check_equal(snapshot.completed_requests, (uint64_t)2u);
    check_equal(chttp_server_get_stats(&server, &server_stats), SALTS_OK);
    check_equal(server_stats.accepted_connections, (uint64_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("isolates a failed HTTP/2 stream from its sibling on one connection") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.h2\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_h2_server_config();
    chttp_client_config client_config = chttp_adapter_h2_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_probe_t failed = {0};
    chttp_adapter_probe_t succeeded = {0};
    chttp_adapter_probe_t sink = {0};
    chttp_server_stats server_stats = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint16_t port = 0u;
    char uri[64];

    check_not_null(flow);
    client_config.max_response_body_bytes = 16u;
    atomic_init(&failed.publication_status, SALTS_OK);
    atomic_init(&succeeded.publication_status, SALTS_EALREADY);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_get(&server, "/h2", chttp_adapter_h2_isolation, NULL), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.h2";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/h2";
    adapter_config.protocol = CHTTP_HTTP_2;
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &sink, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.id = 501u;
    message.owned_payload = tstr_dup("large");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &failed),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    turbo_flow_msg_init(&message);
    message.id = 502u;
    message.owned_payload = tstr_dup("ping");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &succeeded),
                SALTS_OK);
    turbo_flow_msg_cleanup(&message);

    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    } while ((atomic_load_explicit(&failed.publication_calls, memory_order_acquire) < 1u ||
              atomic_load_explicit(&succeeded.publication_calls, memory_order_acquire) < 1u) &&
             salts_monotonic_ms() < deadline);
    check_equal(atomic_load_explicit(&failed.publication_calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&failed.publication_status, memory_order_acquire),
                SALTS_EMSGSIZE);
    check_equal(atomic_load_explicit(&succeeded.publication_calls, memory_order_acquire),
                (size_t)1u);
    check_equal(atomic_load_explicit(&succeeded.publication_status, memory_order_acquire),
                SALTS_OK);
    check_equal(atomic_load_explicit(&sink.sink_calls, memory_order_acquire), (size_t)1u);
    check_equal(sink.payload, "pong");
    check_equal(snapshot.submitted_requests, (uint64_t)2u);
    check_equal(snapshot.completed_requests, (uint64_t)2u);
    check_equal(chttp_server_get_stats(&server, &server_stats), SALTS_OK);
    check_equal(server_stats.accepted_connections, (uint64_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }

  it("makes every accepted request terminal exactly once during shutdown") {
    static const char graph[] = "source input\n"
                                "stage request adapter http.shutdown\n"
                                "stage output\n"
                                "stage main {\n"
                                "  input -> request -> output\n"
                                "}\n";
    const chttp_server_deferred_response deferred_response = {
        .size = sizeof(chttp_server_deferred_response),
        .status_code = 200u,
        .content_type = "text/plain",
        .body = "unused",
        .body_size = 6u};
    chttp_server server = {0};
    chttp_server_config server_config = chttp_adapter_server_config();
    chttp_client_config client_config = chttp_adapter_client_config();
    turbo_flow_chttp_client_config_t adapter_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
    turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    turbo_flow_chttp_client_t *client = NULL;
    chttp_adapter_deferred_probe_t deferred = {.handle = CHTTP_SERVER_DEFERRED_INIT};
    chttp_adapter_probe_t probe = {0};
    turbo_flow_msg_t message;
    turbo_flow_t *flow = turbo_flow_create();
    uint64_t deadline;
    uint16_t port = 0u;
    char uri[64];

    check_not_null(flow);
    atomic_init(&deferred.acquired, 0);
    atomic_init(&probe.publication_status, SALTS_OK);
    check_equal(chttp_server_init(&server, &server_config), SALTS_OK);
    check_equal(chttp_server_get(&server, "/shutdown", chttp_adapter_defer, &deferred), SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);

    adapter_config.flow = flow;
    adapter_config.adapter_name = "http.shutdown";
    adapter_config.client = &client_config;
    adapter_config.connection_uri = uri;
    adapter_config.authority = "127.0.0.1";
    adapter_config.target = "/shutdown";
    adapter_config.stop_timeout_ms = 10u;
    check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", chttp_adapter_sink, &probe, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    turbo_flow_msg_init(&message);
    message.id = 601u;
    check_equal(turbo_flow_publish_async(flow, "input", &message,
                                         chttp_adapter_publication_complete, &probe),
                SALTS_OK);
    deadline = salts_monotonic_ms() + CHTTP_ADAPTER_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&deferred.acquired, memory_order_acquire) == 0 &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_chttp_client_poll(client, 1u, &snapshot), SALTS_OK);
    check_equal(atomic_load_explicit(&deferred.acquired, memory_order_acquire), 1);
    check_equal(snapshot.active_requests, (size_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
    check_not_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                    SALTS_OK);
    check_equal(atomic_load_explicit(&probe.sink_calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_chttp_client_snapshot(client, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_CLIENT_STOPPED);
    check_equal(snapshot.completed_requests, (uint64_t)1u);

    check_equal(chttp_server_deferred_reply(&deferred.handle, &deferred_response), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
    check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }
}
