#include "tinytest.h"

#include <salts/thread.h>

#include "turbo_flow_chttp.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

extern int chttp_server_header_cpp_probe(void);

static native_io_backend_kind chttp_server_adapter_backend(void) {
#ifdef _WIN32
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static chttp_server_config chttp_server_adapter_config(void) {
  const chttp_server_config config = {.host = "127.0.0.1",
                                      .port = 0u,
                                      .backlog = 2u,
                                      .network = {.backend = chttp_server_adapter_backend(),
                                                  .connection_capacity = 2u,
                                                  .command_capacity = 16u,
                                                  .request_capacity = 8u,
                                                  .completion_batch_capacity = 8u,
                                                  .event_capacity = 16u,
                                                  .max_send_bytes = 16384u,
                                                  .receive_buffer_bytes = 4096u,
                                                  .connect_timeout_ms = 5000u,
                                                  .read_timeout_ms = 5000u,
                                                  .write_timeout_ms = 5000u},
                                      .route_capacity = 2u,
                                      .middleware_capacity = 2u,
                                      .max_route_middleware_count = 2u,
                                      .max_route_param_count = 4u,
                                      .max_route_param_bytes = 128u,
                                      .max_target_bytes = 256u,
                                      .max_header_count = 16u,
                                      .max_header_bytes = 2048u,
                                      .max_request_body_bytes = 4096u,
                                      .max_response_header_count = 16u,
                                      .max_response_header_bytes = 2048u,
                                      .max_response_body_bytes = 4096u,
                                      .max_buffered_response_body_bytes = 4096u,
                                      .poll_slice_ms = 1u};
  return config;
}

static chttp_client_config chttp_server_adapter_client_config(void) {
  chttp_client_config config = {0};
  config.network = chttp_server_adapter_config().network;
  config.network.connection_capacity = 1u;
  config.request_capacity = 2u;
  config.max_start_line_bytes = 256u;
  config.max_header_count = 16u;
  config.max_header_bytes = 2048u;
  config.max_request_body_bytes = 4096u;
  config.max_response_body_bytes = 4096u;
  config.max_informational_responses = 2u;
  return config;
}

typedef struct chttp_server_adapter_probe_s {
  atomic_size_t calls;
  chttp_method method;
  cnet_stream_peer peer;
  char target[64];
  char path[32];
  char header_value[32];
  char param_value[32];
} chttp_server_adapter_probe_t;

typedef struct chttp_server_adapter_result_s {
  int status;
  unsigned int response_status;
  size_t body_size;
  char body[64];
} chttp_server_adapter_result_t;

typedef struct chttp_server_adapter_capture_s {
  turbo_flow_msg_t message;
  atomic_size_t calls;
} chttp_server_adapter_capture_t;

typedef struct chttp_server_adapter_completion_s {
  atomic_int called;
  atomic_int status;
} chttp_server_adapter_completion_t;

typedef struct chttp_server_adapter_gate_s {
  atomic_size_t entered;
  atomic_int allow_exit;
} chttp_server_adapter_gate_t;

typedef struct chttp_server_adapter_client_thread_s {
  uint16_t port;
  const char *target;
  const char *body;
  chttp_server_adapter_result_t result;
} chttp_server_adapter_client_thread_t;

typedef struct chttp_server_adapter_stop_thread_s {
  turbo_flow_t *flow;
  atomic_int result;
} chttp_server_adapter_stop_thread_t;

typedef struct chttp_server_adapter_snapshot_thread_s {
  const turbo_flow_chttp_server_t *server;
  atomic_int stop;
  atomic_int status;
  atomic_size_t observations;
} chttp_server_adapter_snapshot_thread_t;

typedef struct chttp_server_adapter_holding_sink_s {
  turbo_flow_async_terminal_claim_t claim;
  atomic_size_t submissions;
  atomic_size_t stops;
} chttp_server_adapter_holding_sink_t;

static int chttp_server_adapter_call_ex(uint16_t port, const char *target,
                                        const chttp_header *headers, size_t header_count,
                                        const char *body, chttp_server_adapter_result_t *result) {
  chttp_client_config client_config = chttp_server_adapter_client_config();
  chttp_client client = {0};
  chttp_options options = {0};
  chttp_response response = {0};
  chttp_error error = {0};
  char uri[64];
  int status;
  int destroy_status;
  if (!port || !target || !body || !result) return SALTS_EINVAL;
  memset(result, 0, sizeof(*result));
  if (snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port) <= 0)
    return SALTS_EINVAL;
  status = chttp_client_init(&client, &client_config);
  if (status != SALTS_OK) return status;
  options.connection_uri = uri;
  options.authority = "127.0.0.1";
  options.target = target;
  options.headers = headers;
  options.header_count = header_count;
  options.body = body;
  options.body_size = strlen(body);
  options.timeout_ms = 5000u;
  status = chttp_post(&client, &options, &response, &error);
  result->status = status;
  if (status == SALTS_OK) {
    result->response_status = response.status_code;
    result->body_size = response.body_size;
    if (response.body_size < sizeof(result->body)) {
      if (response.body_size != 0u) memcpy(result->body, response.body, response.body_size);
      result->body[response.body_size] = '\0';
    } else {
      status = SALTS_EMSGSIZE;
    }
  }
  chttp_response_destroy(&response);
  destroy_status = chttp_client_destroy(&client, 5000u);
  return status == SALTS_OK ? destroy_status : status;
}

static int chttp_server_adapter_call(uint16_t port, const char *target, const char *body,
                                     chttp_server_adapter_result_t *result) {
  return chttp_server_adapter_call_ex(port, target, NULL, 0u, body, result);
}

static int chttp_server_adapter_return_status(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  return ctx ? *(const int *)ctx : SALTS_EINVAL;
}

static int chttp_server_adapter_large_response(turbo_flow_msg_t *message, void *ctx) {
  const char *payload = (const char *)ctx;
  if (!message || !payload) return SALTS_EINVAL;
  message->owned_payload = tstr_dup(payload);
  if (!message->owned_payload) return SALTS_ENOMEM;
  message->payload = tstr_to_v(message->owned_payload);
  return SALTS_OK;
}

static int chttp_server_adapter_filled_response(turbo_flow_msg_t *message, void *ctx) {
  const size_t *payload_size = (const size_t *)ctx;
  if (!message || !payload_size || *payload_size == 0u) return SALTS_EINVAL;
  message->owned_payload = tstr_new_len(NULL, *payload_size);
  if (!message->owned_payload) return SALTS_ENOMEM;
  memset(message->owned_payload, 'x', *payload_size);
  message->payload = tstr_to_v(message->owned_payload);
  return SALTS_OK;
}

static int chttp_server_adapter_set_status(turbo_flow_msg_t *message, void *ctx) {
  if (!message || !ctx) return SALTS_EINVAL;
  message->status = *(const int *)ctx;
  return SALTS_OK;
}

static int chttp_server_adapter_capture_request(turbo_flow_msg_t *message, void *ctx) {
  chttp_server_adapter_capture_t *capture = (chttp_server_adapter_capture_t *)ctx;
  if (!message || !capture) return SALTS_EINVAL;
  if (atomic_fetch_add_explicit(&capture->calls, 1u, memory_order_acq_rel) == 0u)
    return turbo_flow_msg_clone(&capture->message, message);
  return SALTS_OK;
}

static void chttp_server_adapter_publish_complete(void *ctx,
                                                  const turbo_flow_publish_result_t *result) {
  chttp_server_adapter_completion_t *completion = (chttp_server_adapter_completion_t *)ctx;
  if (!completion) return;
  atomic_store_explicit(&completion->status, result ? result->status : SALTS_EINVAL,
                        memory_order_release);
  atomic_store_explicit(&completion->called, 1, memory_order_release);
}

static int chttp_server_adapter_gate(turbo_flow_msg_t *message, void *ctx) {
  chttp_server_adapter_gate_t *gate = (chttp_server_adapter_gate_t *)ctx;
  (void)message;
  if (!gate) return SALTS_EINVAL;
  (void)atomic_fetch_add_explicit(&gate->entered, 1u, memory_order_release);
  while (!atomic_load_explicit(&gate->allow_exit, memory_order_acquire))
    salts_sleep_ms(1u);
  return SALTS_OK;
}

static void chttp_server_adapter_client_thread(void *ctx) {
  chttp_server_adapter_client_thread_t *client = (chttp_server_adapter_client_thread_t *)ctx;
  if (!client) return;
  client->result.status =
      chttp_server_adapter_call(client->port, client->target, client->body, &client->result);
}

static void chttp_server_adapter_stop_thread(void *ctx) {
  chttp_server_adapter_stop_thread_t *stop = (chttp_server_adapter_stop_thread_t *)ctx;
  if (!stop) return;
  atomic_store_explicit(&stop->result, turbo_flow_stop(stop->flow), memory_order_release);
}

static void chttp_server_adapter_snapshot_thread(void *ctx) {
  chttp_server_adapter_snapshot_thread_t *probe =
      (chttp_server_adapter_snapshot_thread_t *)ctx;
  if (!probe) return;
  while (!atomic_load_explicit(&probe->stop, memory_order_acquire)) {
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    int status = turbo_flow_chttp_server_snapshot(probe->server, &snapshot);
    if (status == SALTS_OK) {
      if ((snapshot.state == TURBO_FLOW_CHTTP_SERVER_RUNNING ||
           snapshot.state == TURBO_FLOW_CHTTP_SERVER_STOPPING) &&
          snapshot.bound_port == 0u)
        status = SALTS_EPROTO;
      else if ((snapshot.state == TURBO_FLOW_CHTTP_SERVER_REGISTERED ||
                snapshot.state == TURBO_FLOW_CHTTP_SERVER_STARTING ||
                snapshot.state == TURBO_FLOW_CHTTP_SERVER_STOPPED ||
                snapshot.state == TURBO_FLOW_CHTTP_SERVER_DETACHED) &&
               snapshot.bound_port != 0u)
        status = SALTS_EPROTO;
    }
    if (status != SALTS_OK) {
      atomic_store_explicit(&probe->status, status, memory_order_release);
      return;
    }
    (void)atomic_fetch_add_explicit(&probe->observations, 1u, memory_order_relaxed);
    salts_thread_yield();
  }
}

static int chttp_server_adapter_holding_sink_submit(
    void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
    const turbo_flow_msg_t *message, turbo_flow_async_terminal_claim_t *claim) {
  chttp_server_adapter_holding_sink_t *sink =
      (chttp_server_adapter_holding_sink_t *)ctx;
  int status;
  (void)flow;
  (void)stage;
  if (!sink || !message || !claim) return SALTS_EINVAL;
  status = turbo_flow_async_terminal_claim_move(&sink->claim, claim);
  if (status == SALTS_OK)
    (void)atomic_fetch_add_explicit(&sink->submissions, 1u, memory_order_release);
  return status;
}

static void chttp_server_adapter_holding_sink_stop(void *ctx, turbo_flow_t *flow,
                                                   const turbo_flow_stage_plan_t *stage) {
  chttp_server_adapter_holding_sink_t *sink =
      (chttp_server_adapter_holding_sink_t *)ctx;
  (void)flow;
  if (!sink || !stage || stage->is_source) return;
  (void)atomic_fetch_add_explicit(&sink->stops, 1u, memory_order_release);
  if (sink->claim._impl)
    check_equal(turbo_flow_async_terminal_complete(&sink->claim, SALTS_ECANCELED, NULL), SALTS_OK);
}

static int chttp_server_adapter_copy_view(char *destination, size_t capacity, vstr value) {
  if (!destination || capacity == 0u || value.len >= capacity) return SALTS_EMSGSIZE;
  if (value.len != 0u) memcpy(destination, value.data, value.len);
  destination[value.len] = '\0';
  return SALTS_OK;
}

static int chttp_server_adapter_inspect(turbo_flow_msg_t *message, void *ctx) {
  chttp_server_adapter_probe_t *probe = (chttp_server_adapter_probe_t *)ctx;
  const turbo_flow_chttp_server_request_context_t *request =
      turbo_flow_chttp_server_request_context(message);
  vstr target = {0};
  vstr path = {0};
  size_t index;
  int found_header = 0;
  int found_param = 0;
  if (!probe || !request || !request->has_peer ||
      turbo_flow_chttp_server_request_target(message, &target) != SALTS_OK ||
      turbo_flow_chttp_server_request_path(message, &path) != SALTS_OK ||
      chttp_server_adapter_copy_view(probe->target, sizeof(probe->target), target) != SALTS_OK ||
      chttp_server_adapter_copy_view(probe->path, sizeof(probe->path), path) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  probe->method = request->method;
  probe->peer = request->peer;
  for (index = 0u; index < request->header_count; ++index) {
    vstr name = {0};
    vstr value = {0};
    if (turbo_flow_chttp_server_request_header_at(message, index, &name, &value) != SALTS_OK)
      return SALTS_EPROTO;
    if (name.len == sizeof("X-Test") - 1u && memcmp(name.data, "X-Test", name.len) == 0) {
      if (chttp_server_adapter_copy_view(probe->header_value, sizeof(probe->header_value), value) !=
          SALTS_OK)
        return SALTS_EPROTO;
      found_header = 1;
    }
  }
  for (index = 0u; index < request->param_count; ++index) {
    vstr name = {0};
    vstr value = {0};
    if (turbo_flow_chttp_server_request_param_at(message, index, &name, &value) != SALTS_OK)
      return SALTS_EPROTO;
    if (name.len == sizeof("id") - 1u && memcmp(name.data, "id", name.len) == 0) {
      if (chttp_server_adapter_copy_view(probe->param_value, sizeof(probe->param_value), value) !=
          SALTS_OK)
        return SALTS_EPROTO;
      found_param = 1;
    }
  }
  if (!found_header || !found_param || message->payload.len != 4u ||
      memcmp(message->payload.data, "ping", 4u) != 0) {
    return SALTS_EPROTO;
  }
  message->owned_payload = tstr_dup("accepted");
  if (!message->owned_payload) return SALTS_ENOMEM;
  message->payload = tstr_to_v(message->owned_payload);
  message->status = 201;
  (void)atomic_fetch_add_explicit(&probe->calls, 1u, memory_order_release);
  return SALTS_OK;
}

spec("TurboFlow CHTTP deferred server adapter") {
  it("exports a size-versioned C and C++ server contract") {
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_CHTTP_SERVER_API_VERSION);
    check_equal(snapshot.size, sizeof(snapshot));
    check_equal(snapshot.version, TURBO_FLOW_CHTTP_SERVER_API_VERSION);
    check_equal(chttp_server_header_cpp_probe(), 1);
  }

  it("rejects HTTP2 at flow start without starting an HTTP1 fallback") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    native_config.enable_http2 = 1;
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_not_null(server);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_ENOTSUP);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_FAILED);
    check_equal(snapshot.bound_port, 0u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("rejects session and impossible error-response configurations") {
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    native_config.session_capacity = 1u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);
    native_config.session_capacity = 0u;
    native_config.max_buffered_response_body_bytes = 0u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);
    native_config.max_buffered_response_body_bytes = 16u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);
    native_config.max_buffered_response_body_bytes = 4096u;
    config.unavailable_status = 204u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);
    config.unavailable_status = TURBO_FLOW_CHTTP_SERVER_DEFAULT_UNAVAILABLE_STATUS;
    config.error_content_type = "invalid\r\nheader";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);
    config.error_content_type = "text/plain";
    config.stop_timeout_ms = 0u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_EINVAL);
    check_null(server);

    turbo_flow_destroy(flow);
  }

  it("copies callback request views and replies once after the graph completes") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage inspect\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> inspect -> response\n"
                             "}\n";
    static const chttp_header headers[] = {{"X-Test", "copied"}};
    chttp_server_config native_config = chttp_server_adapter_config();
    chttp_client_config client_config = chttp_server_adapter_client_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();
    chttp_client client = {0};
    chttp_options options = {0};
    chttp_response response = {0};
    chttp_error error = {0};
    char uri[64];

    check_not_null(flow);
    atomic_init(&probe.calls, 0u);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow/:id";
    config.success_status = 202u;
    config.response_content_type = "text/plain";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "inspect", chttp_server_adapter_inspect, &probe, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_RUNNING);
    check_true(snapshot.bound_port != 0u);

    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    check_equal(chttp_client_init(&client, &client_config), SALTS_OK);
    options.connection_uri = uri;
    options.authority = "127.0.0.1";
    options.target = "/flow/42?mode=fast";
    options.headers = headers;
    options.header_count = sizeof(headers) / sizeof(headers[0]);
    options.body = "ping";
    options.body_size = 4u;
    options.timeout_ms = 5000u;
    check_equal(chttp_post(&client, &options, &response, &error), SALTS_OK);
    check_equal(response.status_code, 201u);
    check_equal(response.body_size, (size_t)8u);
    check_equal(response.body, "accepted", 8u);
    check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), (size_t)1u);
    check_equal(probe.method, CHTTP_METHOD_POST);
    check_equal(probe.target, "/flow/42?mode=fast");
    check_equal(probe.path, "/flow/42");
    check_equal(probe.header_value, "copied");
    check_equal(probe.param_value, "42");
    check_true(probe.peer.port != 0u);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);
    check_equal(snapshot.response_bytes, (uint64_t)8u);

    chttp_response_destroy(&response);
    check_equal(chttp_client_destroy(&client, 5000u), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("maps graph errors cancellation and timeout to one configured response") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage fail\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> fail -> response\n"
                             "}\n";
    static const int graph_statuses[] = {SALTS_EIO, SALTS_ECANCELED, SALTS_ETIMEDOUT};
    size_t index;

    for (index = 0u; index < sizeof(graph_statuses) / sizeof(graph_statuses[0]); ++index) {
      chttp_server_config native_config = chttp_server_adapter_config();
      turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
      turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
      turbo_flow_chttp_server_t *server = NULL;
      chttp_server_adapter_result_t result = {0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      config.flow = flow;
      config.adapter_name = "http.server";
      config.source_name = "http_in";
      config.server = &native_config;
      config.method = CHTTP_METHOD_POST;
      config.path = "/flow";
      config.graph_error_status = 598u;
      config.graph_error_body = "mapped";
      config.graph_error_body_size = sizeof("mapped") - 1u;
      check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
      check_equal(turbo_flow_register_stage_ex(flow, "fail", chttp_server_adapter_return_status,
                                               (void *)&graph_statuses[index], NULL),
                  SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result),
                  SALTS_OK);
      check_equal(result.response_status, 598u);
      check_equal(result.body_size, sizeof("mapped") - 1u);
      check_equal(result.body, "mapped");
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.active_requests, (size_t)0u);
      check_equal(snapshot.admitted_requests, (uint64_t)1u);
      check_equal(snapshot.completed_requests, (uint64_t)1u);

      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
    }
  }

  it("converts duplicate terminal responses into one graph-error reply") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage response_a adapter http.server\n"
                             "stage response_b adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> [response_a, response_b]\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_result_t result = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    config.graph_error_status = 597u;
    config.graph_error_body = "duplicate";
    config.graph_error_body_size = sizeof("duplicate") - 1u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result), SALTS_OK);
    check_equal(result.response_status, 597u);
    check_equal(result.body, "duplicate");
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("rejects stale request generations at the terminal boundary") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage capture\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> capture -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_capture_t capture = {0};
    chttp_server_adapter_completion_t completion = {0};
    chttp_server_adapter_result_t result = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    turbo_flow_msg_init(&capture.message);
    atomic_init(&capture.calls, 0u);
    atomic_init(&completion.called, 0);
    atomic_init(&completion.status, SALTS_EBUSY);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "capture", chttp_server_adapter_capture_request,
                                             &capture, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result), SALTS_OK);
    check_equal(result.response_status, TURBO_FLOW_CHTTP_SERVER_DEFAULT_SUCCESS_STATUS);
    check_equal(turbo_flow_publish_async(flow, "http_in", &capture.message,
                                         chttp_server_adapter_publish_complete, &completion),
                SALTS_OK);
    for (int wait = 0;
         wait < 2000 && !atomic_load_explicit(&completion.called, memory_order_acquire); ++wait)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&completion.called, memory_order_acquire), 1);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_ENOENT);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.completed_requests, (uint64_t)1u);

    turbo_flow_msg_cleanup(&capture.message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("bounds copied requests and terminal response bodies") {
    static const char *request_dsl = "source http_in adapter http.server\n"
                                     "stage response adapter http.server\n"
                                     "stage main {\n"
                                     "  http_in -> response\n"
                                     "}\n";
    static const char *response_dsl = "source http_in adapter http.server\n"
                                      "stage enlarge\n"
                                      "stage response adapter http.server\n"
                                      "stage main {\n"
                                      "  http_in -> enlarge -> response\n"
                                      "}\n";
    static const char oversized_response[] = "response-too-large";
    size_t pass;

    for (pass = 0u; pass < 2u; ++pass) {
      chttp_server_config native_config = chttp_server_adapter_config();
      turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
      turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
      turbo_flow_chttp_server_t *server = NULL;
      chttp_server_adapter_result_t result = {0};
      turbo_flow_t *flow = turbo_flow_create();

      check_not_null(flow);
      if (pass == 0u) config.max_request_message_bytes = 1u;
      else {
        native_config.max_response_body_bytes = 17u;
        native_config.max_buffered_response_body_bytes = 17u;
      }
      config.flow = flow;
      config.adapter_name = "http.server";
      config.source_name = "http_in";
      config.server = &native_config;
      config.method = CHTTP_METHOD_POST;
      config.path = "/flow";
      config.graph_error_status = 596u;
      config.graph_error_body = "bad";
      config.graph_error_body_size = sizeof("bad") - 1u;
      check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, pass == 0u ? request_dsl : response_dsl,
                                          pass == 0u ? strlen(request_dsl) : strlen(response_dsl)),
                  SALTS_OK);
      if (pass != 0u)
        check_equal(turbo_flow_register_stage_ex(flow, "enlarge",
                                                 chttp_server_adapter_large_response,
                                                 (void *)oversized_response, NULL),
                    SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result),
                  SALTS_OK);
      check_equal(result.response_status, pass == 0u ? 413u : 596u);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.active_requests, (size_t)0u);
      check_equal(snapshot.admitted_requests, pass == 0u ? (uint64_t)0u : (uint64_t)1u);
      check_equal(snapshot.rejected_requests, pass == 0u ? (uint64_t)1u : (uint64_t)0u);
      check_equal(snapshot.completed_requests, pass == 0u ? (uint64_t)0u : (uint64_t)1u);

      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
    }
  }

  it("cancels without a replacement response when deferred reply exhausts its buffer budget") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage enlarge\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> enlarge -> response\n"
                             "}\n";
    const size_t response_body_size = 8192u;
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_result_t result = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    native_config.max_response_body_bytes = response_body_size;
    native_config.max_buffered_response_body_bytes = response_body_size;
    native_config.buffer_capacity_bytes = response_body_size - 1u;
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "enlarge", chttp_server_adapter_filled_response,
                                             (void *)&response_body_size, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_true(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result) !=
               SALTS_OK);
    check_equal(result.response_status, 0u);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_FAILED);
    check_equal(snapshot.last_status, SALTS_ENOBUFS);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("rejects a nonempty terminal body for an HTTP status that forbids one") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage no_content\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> no_content -> response\n"
                             "}\n";
    const int no_content_status = 204;
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_result_t result = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    config.graph_error_status = 596u;
    config.graph_error_body = "bad";
    config.graph_error_body_size = sizeof("bad") - 1u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "no_content", chttp_server_adapter_set_status,
                                             (void *)&no_content_status, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result), SALTS_OK);
    check_equal(result.response_status, 596u);
    check_equal(result.body_size, sizeof("bad") - 1u);
    check_equal(memcmp(result.body, "bad", sizeof("bad") - 1u), 0);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("leaves oversized request headers at the bounded CHTTP parser boundary") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> response\n"
                             "}\n";
    static const chttp_header headers[] = {
        {"X-One", "1"}, {"X-Two", "2"}, {"X-Three", "3"}, {"X-Four", "4"}};
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_result_t result = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    native_config.max_header_count = 4u;
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(chttp_server_adapter_call_ex(snapshot.bound_port, "/flow", headers,
                                             sizeof(headers) / sizeof(headers[0]), "ping", &result),
                SALTS_OK);
    check_equal(result.response_status, 431u);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)0u);
    check_equal(snapshot.rejected_requests, (uint64_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("returns overload synchronously when bounded Flow ingress is full") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage gate\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> gate -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_gate_t gate = {0};
    chttp_server_adapter_client_thread_t clients[2] = {0};
    chttp_server_adapter_result_t rejected = {0};
    salts_thread_t threads[2] = {0};
    turbo_flow_t *flow = turbo_flow_create();
    size_t index;

    check_not_null(flow);
    native_config.network.connection_capacity = 4u;
    ingress.workers = 1u;
    ingress.queue_capacity = 1u;
    atomic_init(&gate.entered, 0u);
    atomic_init(&gate.allow_exit, 0);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_configure_async_ingress(flow, &ingress), SALTS_OK);
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "gate", chttp_server_adapter_gate, &gate, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);

    for (index = 0u; index < 2u; ++index) {
      clients[index].port = snapshot.bound_port;
      clients[index].target = "/flow";
      clients[index].body = index == 0u ? "one" : "two";
      check_equal(
          salts_thread_create(&threads[index], chttp_server_adapter_client_thread, &clients[index]),
          SALTS_OK);
      for (int wait = 0; wait < 5000; ++wait) {
        check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
        if (snapshot.admitted_requests >= index + 1u) break;
        salts_sleep_ms(1u);
      }
      check_equal(snapshot.admitted_requests, (uint64_t)(index + 1u));
    }
    check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), (size_t)1u);
    check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "three", &rejected),
                SALTS_OK);
    check_equal(rejected.response_status, TURBO_FLOW_CHTTP_SERVER_DEFAULT_OVERLOAD_STATUS);

    atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
    for (index = 0u; index < 2u; ++index) {
      check_equal(salts_thread_join(&threads[index]), SALTS_OK);
      check_equal(clients[index].result.status, SALTS_OK);
      check_equal(clients[index].result.response_status,
                  TURBO_FLOW_CHTTP_SERVER_DEFAULT_SUCCESS_STATUS);
    }
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)2u);
    check_equal(snapshot.rejected_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)2u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("drains accepted deferred work and rejects new admission during stop") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage gate\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> gate -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_gate_t gate = {0};
    chttp_server_adapter_client_thread_t accepted = {0};
    chttp_server_adapter_stop_thread_t stop = {0};
    chttp_server_adapter_result_t rejected = {0};
    turbo_flow_runtime_snapshot_t runtime = {0};
    salts_thread_t client_thread = NULL;
    salts_thread_t stop_thread = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    atomic_init(&gate.entered, 0u);
    atomic_init(&gate.allow_exit, 0);
    atomic_init(&stop.result, SALTS_EBUSY);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "gate", chttp_server_adapter_gate, &gate, NULL),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);

    accepted.port = snapshot.bound_port;
    accepted.target = "/flow";
    accepted.body = "accepted";
    check_equal(salts_thread_create(&client_thread, chttp_server_adapter_client_thread, &accepted),
                SALTS_OK);
    for (int wait = 0;
         wait < 5000 && atomic_load_explicit(&gate.entered, memory_order_acquire) == 0u; ++wait)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), (size_t)1u);

    stop.flow = flow;
    check_equal(salts_thread_create(&stop_thread, chttp_server_adapter_stop_thread, &stop),
                SALTS_OK);
    for (int wait = 0; wait < 5000; ++wait) {
      check_equal(turbo_flow_runtime_snapshot(flow, &runtime), SALTS_OK);
      if (!runtime.accepting_publishes) break;
      salts_sleep_ms(1u);
    }
    check_equal(runtime.accepting_publishes, 0);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_RUNNING);
    check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "late", &rejected),
                SALTS_OK);
    check_equal(rejected.response_status, TURBO_FLOW_CHTTP_SERVER_DEFAULT_UNAVAILABLE_STATUS);
    atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
    check_equal(salts_thread_join(&client_thread), SALTS_OK);
    check_equal(salts_thread_join(&stop_thread), SALTS_OK);
    check_equal(accepted.result.status, SALTS_OK);
    check_equal(accepted.result.response_status, TURBO_FLOW_CHTTP_SERVER_DEFAULT_SUCCESS_STATUS);
    check_equal(atomic_load_explicit(&stop.result, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_STOPPED);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);
    check_equal(snapshot.rejected_requests, (uint64_t)1u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("stops terminal sinks before a later-declared CHTTP source drains and restarts") {
    static const char *dsl = "stage held adapter holding.sink\n"
                             "source http_in adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> held\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    chttp_server_adapter_holding_sink_t sink = {0};
    chttp_server_adapter_client_thread_t client = {0};
    chttp_server_adapter_stop_thread_t stop = {0};
    salts_thread_t client_thread = NULL;
    salts_thread_t stop_thread = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    sink.claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    atomic_init(&sink.submissions, 0u);
    atomic_init(&sink.stops, 0u);
    adapter_ops.stop = chttp_server_adapter_holding_sink_stop;
    async_ops.submit = chttp_server_adapter_holding_sink_submit;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_equal(turbo_flow_register_async_terminal_adapter_ex(
                    flow, "holding.sink", &adapter_ops, &async_ops, &sink, &schema),
                SALTS_OK);
    native_config.network.write_timeout_ms = 100u;
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    config.stop_timeout_ms = 100u;
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);

    client.port = snapshot.bound_port;
    client.target = "/flow";
    client.body = "held";
    check_equal(salts_thread_create(&client_thread, chttp_server_adapter_client_thread, &client),
                SALTS_OK);
    for (int wait = 0;
         wait < 5000 && atomic_load_explicit(&sink.submissions, memory_order_acquire) == 0u; ++wait)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&sink.submissions, memory_order_acquire), (size_t)1u);

    stop.flow = flow;
    atomic_init(&stop.result, SALTS_EBUSY);
    check_equal(salts_thread_create(&stop_thread, chttp_server_adapter_stop_thread, &stop),
                SALTS_OK);
    check_equal(salts_thread_join(&client_thread), SALTS_OK);
    check_equal(salts_thread_join(&stop_thread), SALTS_OK);
    check_equal(atomic_load_explicit(&stop.result, memory_order_acquire), SALTS_OK);
    check_equal(atomic_load_explicit(&sink.stops, memory_order_acquire), (size_t)1u);
    check_equal(client.result.status, SALTS_OK);
    check_equal(client.result.response_status, TURBO_FLOW_CHTTP_SERVER_DEFAULT_GRAPH_ERROR_STATUS);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_STOPPED);
    check_equal(snapshot.active_requests, (size_t)0u);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("recreates the CHTTP owner when a stopped Flow restarts") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_result_t result = {0};
    chttp_server_adapter_snapshot_thread_t snapshot_probe = {0};
    salts_thread_t snapshot_thread = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    size_t cycle;

    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    snapshot_probe.server = server;
    atomic_init(&snapshot_probe.stop, 0);
    atomic_init(&snapshot_probe.status, SALTS_OK);
    atomic_init(&snapshot_probe.observations, 0u);
    check_equal(salts_thread_create(&snapshot_thread, chttp_server_adapter_snapshot_thread,
                                    &snapshot_probe),
                SALTS_OK);
    for (cycle = 0u; cycle < 8u; ++cycle) {
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_RUNNING);
      check_true(snapshot.bound_port != 0u);
      check_equal(chttp_server_adapter_call(snapshot.bound_port, "/flow", "ping", &result),
                  SALTS_OK);
      check_equal(result.response_status, TURBO_FLOW_CHTTP_SERVER_DEFAULT_SUCCESS_STATUS);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_STOPPED);
      check_equal(snapshot.bound_port, 0u);
    }
    atomic_store_explicit(&snapshot_probe.stop, 1, memory_order_release);
    check_equal(salts_thread_join(&snapshot_thread), SALTS_OK);
    check_equal(atomic_load_explicit(&snapshot_probe.status, memory_order_acquire), SALTS_OK);
    check_true(atomic_load_explicit(&snapshot_probe.observations, memory_order_relaxed) != 0u);
    check_equal(snapshot.admitted_requests, (uint64_t)8u);
    check_equal(snapshot.completed_requests, (uint64_t)8u);

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }
}
