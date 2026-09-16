#include "../../../tests/flow_operation_fixture.h"
#include "tinytest.h"

#include "../../cnet/tests/listener_source_tls_fixture.h"

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

static chttp_server_config chttp_server_adapter_h2_config(void) {
  chttp_server_config config = chttp_server_adapter_config();
  config.network.max_send_bytes = 64u * 1024u;
  config.enable_http2 = 1;
  config.h2_stream_capacity = 4u;
  config.h2_input_buffer_bytes = 64u * 1024u;
  config.h2_output_buffer_bytes = 64u * 1024u;
  config.h2_hpack_dynamic_table_bytes = 4096u;
  config.h2_max_settings_count = 16u;
  return config;
}

static chttp_client_config chttp_server_adapter_h2_client_config(void) {
  chttp_client_config config = chttp_server_adapter_client_config();
  config.network.max_send_bytes = 64u * 1024u;
  config.h2_input_buffer_bytes = 64u * 1024u;
  config.h2_hpack_dynamic_table_bytes = 4096u;
  config.h2_max_settings_count = 16u;
  return config;
}

typedef struct chttp_server_adapter_probe_s {
  atomic_size_t calls;
  unsigned int http_major;
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

typedef struct chttp_server_adapter_http_completion_s {
  size_t calls;
  int status;
  unsigned int response_status;
  char body[64];
  size_t body_size;
} chttp_server_adapter_http_completion_t;

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

typedef struct chttp_server_adapter_managed_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  turbo_flow_managed_boundary_descriptor_t descriptor;
  turbo_flow_managed_boundary_snapshot_t snapshot;
} chttp_server_adapter_managed_fixture_t;

static int chttp_server_adapter_fixture_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  chttp_server_adapter_managed_fixture_t *fixture =
      (chttp_server_adapter_managed_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int chttp_server_adapter_fixture_descriptor(
    void *ctx, turbo_flow_managed_boundary_descriptor_t *out) {
  chttp_server_adapter_managed_fixture_t *fixture =
      (chttp_server_adapter_managed_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out) ||
      out->version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION) {
    return SALTS_EINVAL;
  }
  *out = fixture->descriptor;
  return SALTS_OK;
}

static int chttp_server_adapter_fixture_snapshot(
    void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  chttp_server_adapter_managed_fixture_t *fixture =
      (chttp_server_adapter_managed_fixture_t *)ctx;
  if (!fixture || !out || out->size < sizeof(*out) ||
      out->version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION) {
    return SALTS_EINVAL;
  }
  *out = fixture->snapshot;
  return SALTS_OK;
}

static void chttp_server_adapter_fixture_init(chttp_server_adapter_managed_fixture_t *fixture,
                                              const char *uid, const char *owner) {
  memset(fixture, 0, sizeof(*fixture));
  fixture->metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  fixture->descriptor =
      (turbo_flow_managed_boundary_descriptor_t)TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  fixture->snapshot =
      (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  fixture->metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  fixture->metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(fixture->metadata.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->metadata.owner_name, owner, strlen(owner) + 1u);
  fixture->metadata.generation = 1u;
  fixture->metadata.observed_generation = 1u;
  fixture->descriptor.domain = fixture->metadata.domain;
  fixture->descriptor.kind = fixture->metadata.kind;
  memcpy(fixture->descriptor.uid, uid, strlen(uid) + 1u);
  memcpy(fixture->descriptor.owner_name, owner, strlen(owner) + 1u);
  fixture->descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SOURCE;
  check_equal(turbo_flow_content_descriptor_init(
                  &fixture->descriptor.output, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                  TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY, TURBO_FLOW_DATA_ENCODING_OPAQUE,
                  "application/octet-stream", owner),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(
                  &fixture->descriptor.output, "CHTTPServerRequest", "Body", 1u),
              SALTS_OK);
  memcpy(fixture->snapshot.uid, uid, strlen(uid) + 1u);
  fixture->snapshot.generation = 1u;
  fixture->snapshot.observed_generation = 1u;
  fixture->snapshot.state = TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  fixture->snapshot.queue_capacity = 1u;
}

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

static int chttp_server_adapter_fail_selected_payload(turbo_flow_msg_t *message, void *ctx) {
  (void)ctx;
  if (!message) return SALTS_EINVAL;
  if (message->payload.len == sizeof("fail") - 1u &&
      memcmp(message->payload.data, "fail", sizeof("fail") - 1u) == 0)
    return SALTS_EIO;
  return SALTS_OK;
}

static void chttp_server_adapter_http_complete(void *user, chttp_request request,
                                               const chttp_response_view *response,
                                               const chttp_error *error) {
  chttp_server_adapter_http_completion_t *completion =
      (chttp_server_adapter_http_completion_t *)user;
  (void)request;
  if (!completion) return;
  ++completion->calls;
  completion->status = error ? error->status : SALTS_OK;
  if (!response) return;
  completion->response_status = response->status_code;
  completion->body_size = response->body_size;
  if (response->body_size >= sizeof(completion->body)) {
    completion->status = SALTS_EMSGSIZE;
    return;
  }
  if (response->body_size != 0u) memcpy(completion->body, response->body, response->body_size);
  completion->body[response->body_size] = '\0';
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
  chttp_server_adapter_snapshot_thread_t *probe = (chttp_server_adapter_snapshot_thread_t *)ctx;
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

static int chttp_server_adapter_holding_sink_submit(void *ctx, turbo_flow_t *flow,
                                                    const turbo_flow_stage_plan_t *stage,
                                                    const turbo_flow_msg_t *message,
                                                    turbo_flow_async_terminal_claim_t *claim) {
  chttp_server_adapter_holding_sink_t *sink = (chttp_server_adapter_holding_sink_t *)ctx;
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
  chttp_server_adapter_holding_sink_t *sink = (chttp_server_adapter_holding_sink_t *)ctx;
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
  probe->http_major = request->http_major;
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

static int chttp_server_adapter_require_h2(turbo_flow_msg_t *message, void *ctx) {
  chttp_server_adapter_probe_t *probe = (chttp_server_adapter_probe_t *)ctx;
  const turbo_flow_chttp_server_request_context_t *request =
      turbo_flow_chttp_server_request_context(message);
  if (!probe || !request || request->http_major != 2u || request->http_minor != 0u)
    return SALTS_EPROTO;
  probe->http_major = request->http_major;
  (void)atomic_fetch_add_explicit(&probe->calls, 1u, memory_order_release);
  return SALTS_OK;
}

spec("TurboFlow CHTTP deferred server adapter") {
  static turbo_flow_t *quiesce_cleanup_flow;
  static turbo_flow_chttp_server_t *quiesce_cleanup_server;
  static chttp_async_client *quiesce_cleanup_client;
  static chttp_server_adapter_gate_t *quiesce_cleanup_gate;

  after_each() {
    if (quiesce_cleanup_gate)
      atomic_store_explicit(&quiesce_cleanup_gate->allow_exit, 1, memory_order_release);
    if (quiesce_cleanup_client) {
      check_equal(chttp_async_client_stop(quiesce_cleanup_client, 5000u), SALTS_OK);
      check_equal(chttp_async_client_destroy(quiesce_cleanup_client), SALTS_OK);
      quiesce_cleanup_client = NULL;
    }
    if (quiesce_cleanup_flow) {
      check_equal(turbo_flow_stop(quiesce_cleanup_flow), SALTS_OK);
      turbo_flow_destroy(quiesce_cleanup_flow);
      quiesce_cleanup_flow = NULL;
    }
    if (quiesce_cleanup_server) {
      check_equal(turbo_flow_chttp_server_destroy(quiesce_cleanup_server), SALTS_OK);
      quiesce_cleanup_server = NULL;
    }
    quiesce_cleanup_gate = NULL;
  }

  it("exports a size-versioned C and C++ server contract") {
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_CHTTP_SERVER_API_VERSION);
    check_equal(snapshot.size, sizeof(snapshot));
    check_equal(snapshot.version, TURBO_FLOW_CHTTP_SERVER_API_VERSION);
    check_equal(chttp_server_header_cpp_probe(), 1);
  }

  it("completes an h2c request through the deferred Flow adapter") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage require_h2 operation test.require_h2\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> require_h2 -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_h2_config();
    chttp_client_config client_config = chttp_server_adapter_h2_client_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();
    chttp_client client = {0};
    chttp_options options = {0};
    chttp_response response = {0};
    chttp_error error = {0};
    char uri[64];
    int start_status;

    check_not_null(flow);
    atomic_init(&probe.calls, 0u);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_not_null(server);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(descriptor.role_flags,
                (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_SOURCE |
                           TURBO_FLOW_MANAGED_BOUNDARY_SINK));
    check_equal(descriptor.command_flags,
                (uint32_t)(TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE |
                           TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME));
    check_equal(descriptor.capability_flags, (uint32_t)0u);
    check_equal(descriptor.domain, TURBO_FLOW_DOMAIN_IO_TRANSPORT);
    check_equal(descriptor.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_equal(descriptor.owner_name, "http.server");
    check_equal(descriptor.uid, "chttp-server:http.server");
    check_equal(descriptor.input.profile, TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY);
    check_equal(descriptor.input.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(descriptor.input.media_type, "application/octet-stream");
    check_equal(descriptor.input.schema_name, "CHTTPServerResponse");
    check_equal(descriptor.input.type_name, "Body");
    check_equal(descriptor.input.schema_version, (uint32_t)1u);
    check_equal(descriptor.output.profile, TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY);
    check_equal(descriptor.output.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(descriptor.output.media_type, "application/octet-stream");
    check_equal(descriptor.output.schema_name, "CHTTPServerRequest");
    check_equal(descriptor.output.type_name, "Body");
    check_equal(descriptor.output.schema_version, (uint32_t)1u);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_require_h2_0 =
        flow_test_operation_init("test.require_h2", chttp_server_adapter_require_h2, &probe);
    operation_require_h2_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_require_h2_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_require_h2_0), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    start_status = turbo_flow_start(flow);
    check_equal(start_status, SALTS_OK);
    if (start_status == SALTS_OK) {
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_RUNNING);
      check_true(snapshot.bound_port != 0u);
      check_greater(
          snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
      check_equal(chttp_client_init(&client, &client_config), SALTS_OK);
      options.connection_uri = uri;
      options.authority = "127.0.0.1";
      options.target = "/flow";
      options.body = "ping";
      options.body_size = 4u;
      options.timeout_ms = 5000u;
      options.protocol = CHTTP_HTTP_2;
      check_equal(chttp_post(&client, &options, &response, &error), SALTS_OK);
      check_equal(response.http_major, 2u);
      check_equal(response.status_code, 200u);
      check_equal(response.body, "ping", 4u);
      check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), (size_t)1u);
      check_equal(probe.http_major, 2u);
      chttp_response_destroy(&response);
      check_equal(chttp_client_destroy(&client, 5000u), SALTS_OK);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
    }

    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("keeps a deferred H2 sibling alive when one Flow run fails") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage fail_selected operation test.fail_selected\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> fail_selected -> response\n"
                             "}\n";
    chttp_server_config native_config = chttp_server_adapter_h2_config();
    chttp_client_config client_config = chttp_server_adapter_h2_client_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_http_completion_t failed = {0};
    chttp_server_adapter_http_completion_t sibling = {0};
    turbo_flow_t *flow = turbo_flow_create();
    chttp_async_client client = {0};
    chttp_request failed_request = {0};
    chttp_request sibling_request = {0};
    chttp_request_options options = {0};
    char uri[64];
    size_t completions = 0u;
    size_t polls = 0u;

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
    flow_test_operation_t operation_fail_selected_1 = flow_test_operation_init(
        "test.fail_selected", chttp_server_adapter_fail_selected_payload, NULL);
    check_equal(flow_test_operation_register(flow, &operation_fail_selected_1), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    check_equal(chttp_async_client_init(&client, &client_config), SALTS_OK);
    options.connection_uri = uri;
    options.authority = "127.0.0.1";
    options.target = "/flow";
    options.method = CHTTP_METHOD_POST;
    options.body = "fail";
    options.body_size = sizeof("fail") - 1u;
    options.on_complete = chttp_server_adapter_http_complete;
    options.user = &failed;
    options.protocol = CHTTP_HTTP_2;
    check_equal(chttp_async_client_submit(&client, &options, &failed_request), SALTS_OK);
    options.body = "good";
    options.body_size = sizeof("good") - 1u;
    options.user = &sibling;
    check_equal(chttp_async_client_submit(&client, &options, &sibling_request), SALTS_OK);
    while ((failed.calls == 0u || sibling.calls == 0u) && polls++ < 40u)
      check_equal(chttp_async_client_poll(&client, 250u, &completions), SALTS_OK);

    check_equal(failed.calls, (size_t)1u);
    check_equal(failed.status, SALTS_OK);
    check_equal(failed.response_status, 598u);
    check_equal(failed.body, "mapped");
    check_equal(sibling.calls, (size_t)1u);
    check_equal(sibling.status, SALTS_OK);
    check_equal(sibling.response_status, 200u);
    check_equal(sibling.body, "good");
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)2u);
    check_equal(snapshot.completed_requests, (uint64_t)2u);

    check_equal(chttp_async_client_stop(&client, 5000u), SALTS_OK);
    check_equal(chttp_async_client_destroy(&client), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("completes a deferred Flow response on TLS negotiated ALPN h2") {
    static const char *alpn[] = {"h2"};
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage require_h2 operation test.require_h2\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> require_h2 -> response\n"
                             "}\n";
    listener_source_tls_fixture_t fixture = {0};
    chttp_server_config native_config = chttp_server_adapter_h2_config();
    chttp_client_config client_config = chttp_server_adapter_h2_client_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_server_t *server = NULL;
    chttp_server_adapter_http_completion_t completion = {0};
    chttp_server_adapter_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();
    chttp_async_client client = {0};
    chttp_tls_profile profile = {0};
    cnet_tls_server_config server_tls = {0};
    cnet_tls_client_config client_tls = {0};
    chttp_request request = {0};
    chttp_request_options options = {0};
    char uri[64];
    size_t completions = 0u;
    size_t polls = 0u;

    check_not_null(flow);
    check_equal(listener_source_tls_fixture_init(&fixture), SALTS_OK);
    atomic_init(&probe.calls, 0u);
    native_config.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    native_config.network.tls_handshake_timeout_ms = 5000u;
    client_config.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    client_config.network.tls_handshake_timeout_ms = 5000u;
    server_tls = (cnet_tls_server_config){.size = sizeof(server_tls),
                                          .cert_file = fixture.cert_path,
                                          .key_file = fixture.key_path,
                                          .client_auth = CNET_TLS_CLIENT_AUTH_NONE,
                                          .alpn_protocols = alpn,
                                          .alpn_protocol_count = 1u};
    client_tls = (cnet_tls_client_config){.size = sizeof(client_tls),
                                          .ca_file = fixture.cert_path,
                                          .server_name = "localhost",
                                          .alpn_protocols = alpn,
                                          .alpn_protocol_count = 1u};
    native_config.tls = &server_tls;
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_require_h2_2 =
        flow_test_operation_init("test.require_h2", chttp_server_adapter_require_h2, &probe);
    operation_require_h2_2.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_require_h2_2.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_require_h2_2), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_greater(
        snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u", (unsigned int)snapshot.bound_port), 0);
    check_equal(chttp_tls_profile_init(&profile, &client_tls), SALTS_OK);
    check_equal(chttp_async_client_init(&client, &client_config), SALTS_OK);
    options.connection_uri = uri;
    options.authority = "localhost";
    options.target = "/flow";
    options.method = CHTTP_METHOD_POST;
    options.body = "secure";
    options.body_size = sizeof("secure") - 1u;
    options.on_complete = chttp_server_adapter_http_complete;
    options.user = &completion;
    options.tls = &profile;
    options.protocol = CHTTP_HTTP_2;
    check_equal(chttp_async_client_submit(&client, &options, &request), SALTS_OK);
    while (completion.calls == 0u && polls++ < 120u)
      check_equal(chttp_async_client_poll(&client, 25u, &completions), SALTS_OK);

    check_equal(completion.calls, (size_t)1u);
    check_equal(completion.status, SALTS_OK);
    check_equal(completion.response_status, 200u);
    check_equal(completion.body, "secure");
    check_equal(atomic_load_explicit(&probe.calls, memory_order_acquire), (size_t)1u);
    check_equal(probe.http_major, 2u);
    check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.active_requests, (size_t)0u);
    check_equal(snapshot.admitted_requests, (uint64_t)1u);
    check_equal(snapshot.completed_requests, (uint64_t)1u);

    check_equal(chttp_async_client_stop(&client, 5000u), SALTS_OK);
    check_equal(chttp_async_client_destroy(&client), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
    check_equal(chttp_tls_profile_destroy(&profile), SALTS_OK);
    listener_source_tls_fixture_destroy(&fixture);
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

  it("rolls back duplicate adapter and managed UID registration before retry") {
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_t *first = NULL;
    turbo_flow_chttp_server_t *duplicate = NULL;
    turbo_flow_chttp_server_t *retry = NULL;
    turbo_flow_managed_boundary_provider_ops_t boundary_ops =
        TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
    chttp_server_adapter_managed_fixture_t fixture;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_t *resource_flow = turbo_flow_create();

    check_not_null(flow);
    check_not_null(resource_flow);
    config.flow = flow;
    config.adapter_name = "http.server";
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    check_equal(turbo_flow_chttp_server_register(&config, &first), SALTS_OK);
    check_equal(turbo_flow_chttp_server_register(&config, &duplicate), SALTS_EALREADY);
    check_null(duplicate);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(first), SALTS_OK);

    chttp_server_adapter_fixture_init(&fixture, "chttp-server:http.server", "existing-owner");
    boundary_ops.resource.metadata = chttp_server_adapter_fixture_metadata;
    boundary_ops.descriptor = chttp_server_adapter_fixture_descriptor;
    boundary_ops.snapshot = chttp_server_adapter_fixture_snapshot;
    check_equal(turbo_flow_register_managed_boundary_provider(
                    resource_flow, fixture.metadata.owner_name, &boundary_ops, &fixture),
                SALTS_OK);
    config.flow = resource_flow;
    check_equal(turbo_flow_chttp_server_register(&config, &duplicate), SALTS_EALREADY);
    check_null(duplicate);
    check_null(turbo_flow_find_adapter_schema(resource_flow, "http.server"));
    check_equal(turbo_flow_managed_boundary_count(resource_flow), (size_t)1u);
    config.adapter_name = "http.retry";
    check_equal(turbo_flow_chttp_server_register(&config, &retry), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(resource_flow), (size_t)2u);
    turbo_flow_destroy(resource_flow);
    check_equal(turbo_flow_chttp_server_destroy(retry), SALTS_OK);
  }

  it("derives stable distinct bounded identities for long valid adapter names") {
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_t *first_server = NULL;
    turbo_flow_chttp_server_t *same_server = NULL;
    turbo_flow_chttp_server_t *long_server = NULL;
    turbo_flow_chttp_server_t *same_long_server = NULL;
    turbo_flow_chttp_server_t *different_long_server = NULL;
    turbo_flow_managed_boundary_descriptor_t first =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t same =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t long_identity =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t same_long_identity =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_descriptor_t different_long_identity =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_t *first_flow = turbo_flow_create();
    turbo_flow_t *same_flow = turbo_flow_create();
    turbo_flow_t *long_flow = turbo_flow_create();
    turbo_flow_t *same_long_flow = turbo_flow_create();
    turbo_flow_t *different_long_flow = turbo_flow_create();
    char name_255[256];
    char name_long[TURBO_FLOW_RESOURCE_OWNER_MAX + 258u];
    char different_name_long[TURBO_FLOW_RESOURCE_OWNER_MAX + 258u];

    memset(name_255, 'a', sizeof(name_255) - 1u);
    name_255[sizeof(name_255) - 1u] = '\0';
    memset(name_long, 'b', sizeof(name_long) - 1u);
    name_long[sizeof(name_long) - 1u] = '\0';
    memset(different_name_long, 'c', sizeof(different_name_long) - 1u);
    different_name_long[sizeof(different_name_long) - 1u] = '\0';
    check_not_null(first_flow);
    check_not_null(same_flow);
    check_not_null(long_flow);
    check_not_null(same_long_flow);
    check_not_null(different_long_flow);
    config.adapter_name = name_255;
    config.source_name = "http_in";
    config.server = &native_config;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    config.flow = first_flow;
    check_equal(turbo_flow_chttp_server_register(&config, &first_server), SALTS_OK);
    config.flow = same_flow;
    check_equal(turbo_flow_chttp_server_register(&config, &same_server), SALTS_OK);
    config.flow = long_flow;
    config.adapter_name = name_long;
    check_equal(turbo_flow_chttp_server_register(&config, &long_server), SALTS_OK);
    config.flow = same_long_flow;
    check_equal(turbo_flow_chttp_server_register(&config, &same_long_server), SALTS_OK);
    config.flow = different_long_flow;
    config.adapter_name = different_name_long;
    check_equal(turbo_flow_chttp_server_register(&config, &different_long_server), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(first_flow, 0u, &first), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(same_flow, 0u, &same), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(long_flow, 0u, &long_identity),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(same_long_flow, 0u,
                                                           &same_long_identity),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(different_long_flow, 0u,
                                                           &different_long_identity),
                SALTS_OK);
    check_equal(first.owner_name, same.owner_name);
    check_equal(first.uid, same.uid);
    check_equal(first.owner_name, name_255);
    check_equal(strncmp(long_identity.owner_name, "xxh3-128:", sizeof("xxh3-128:") - 1u),
                0);
    check_equal(long_identity.owner_name, same_long_identity.owner_name);
    check_equal(long_identity.uid, same_long_identity.uid);
    check_not_equal(long_identity.owner_name, different_long_identity.owner_name);
    check_not_equal(long_identity.uid, different_long_identity.uid);
    check_true(strlen(first.owner_name) <= TURBO_FLOW_RESOURCE_OWNER_MAX);
    check_true(strlen(first.uid) <= TURBO_FLOW_RESOURCE_UID_MAX);
    check_not_equal(first.owner_name, long_identity.owner_name);
    check_not_equal(first.uid, long_identity.uid);
    turbo_flow_destroy(first_flow);
    turbo_flow_destroy(same_flow);
    turbo_flow_destroy(long_flow);
    turbo_flow_destroy(same_long_flow);
    turbo_flow_destroy(different_long_flow);
    check_equal(turbo_flow_chttp_server_destroy(first_server), SALTS_OK);
    check_equal(turbo_flow_chttp_server_destroy(same_server), SALTS_OK);
    check_equal(turbo_flow_chttp_server_destroy(long_server), SALTS_OK);
    check_equal(turbo_flow_chttp_server_destroy(same_long_server), SALTS_OK);
    check_equal(turbo_flow_chttp_server_destroy(different_long_server), SALTS_OK);
  }

  it("copies callback request views and replies once after the graph completes") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage inspect operation test.inspect\n"
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
    flow_test_operation_t operation_inspect_3 =
        flow_test_operation_init("test.inspect", chttp_server_adapter_inspect, &probe);
    operation_inspect_3.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_inspect_3.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_inspect_3), SALTS_OK);
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
                             "stage fail operation test.fail\n"
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
      flow_test_operation_t operation_fail_4 = flow_test_operation_init(
          "test.fail", chttp_server_adapter_return_status, (void *)&graph_statuses[index]);
      operation_fail_4.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation_fail_4.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation_fail_4), SALTS_OK);
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
                             "stage capture operation test.capture\n"
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
    flow_test_operation_t operation_capture_5 =
        flow_test_operation_init("test.capture", chttp_server_adapter_capture_request, &capture);
    operation_capture_5.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_capture_5.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_capture_5), SALTS_OK);
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
                                      "stage enlarge operation test.enlarge\n"
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
      if (pass != 0u) {
        flow_test_operation_t operation_enlarge_6 = flow_test_operation_init(
            "test.enlarge", chttp_server_adapter_large_response, (void *)oversized_response);
        operation_enlarge_6.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
        operation_enlarge_6.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
        check_equal(flow_test_operation_register(flow, &operation_enlarge_6), SALTS_OK);
      }
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
                             "stage enlarge operation test.enlarge\n"
                             "stage response adapter http.server\n"
                             "stage main {\n"
                             "  http_in -> enlarge -> response\n"
                             "}\n";
    const size_t response_body_size = 8192u;
    chttp_server_config native_config = chttp_server_adapter_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
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
    flow_test_operation_t operation_enlarge_7 = flow_test_operation_init(
        "test.enlarge", chttp_server_adapter_filled_response, (void *)&response_body_size);
    operation_enlarge_7.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_enlarge_7.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_enlarge_7), SALTS_OK);
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
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("rejects a nonempty terminal body for an HTTP status that forbids one") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage no_content operation test.no_content\n"
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
    flow_test_operation_t operation_no_content_8 = flow_test_operation_init(
        "test.no_content", chttp_server_adapter_set_status, (void *)&no_content_status);
    operation_no_content_8.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_no_content_8.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_no_content_8), SALTS_OK);
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
                             "stage gate operation test.gate\n"
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
    flow_test_operation_t operation_gate_9 =
        flow_test_operation_init("test.gate", chttp_server_adapter_gate, &gate);
    operation_gate_9.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_gate_9.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_gate_9), SALTS_OK);
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

  it("quiesces H1 and H2 admission while accepted responses drain and explicitly resumes") {
    static const char dsl[] = "source http_in adapter http.server\n"
                              "stage gate operation test.gate\n"
                              "stage response adapter http.server\n"
                              "stage main {\n"
                              "  http_in -> gate -> response\n"
                              "}\n";
    enum { MAX_POLLS = 100, POLL_MS = 50 };
    const chttp_protocol protocols[] = {CHTTP_HTTP_1_1, CHTTP_HTTP_2};
    for (size_t index = 0u; index < sizeof(protocols) / sizeof(protocols[0]); ++index) {
      chttp_server_config native_config =
          index == 0u ? chttp_server_adapter_config() : chttp_server_adapter_h2_config();
      chttp_client_config client_config = index == 0u ? chttp_server_adapter_client_config()
                                                      : chttp_server_adapter_h2_client_config();
      turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
      turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
      turbo_flow_managed_boundary_descriptor_t descriptor =
          TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
      turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
      turbo_flow_resource_command_result_t command_result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      turbo_flow_chttp_server_t *server = NULL;
      static chttp_server_adapter_gate_t gate;
      static chttp_server_adapter_http_completion_t accepted, rejected, resumed;
      static chttp_async_client client;
      chttp_request requests[3] = {0};
      chttp_request_options options = {0};
      turbo_flow_t *flow = turbo_flow_create();
      char uri[64];
      size_t completions;
      uint16_t port;
      /* H1 cannot multiplex a second request on the held response connection. */
      client_config.network.connection_capacity = 2u;
      atomic_init(&gate.entered, 0u);
      atomic_init(&gate.allow_exit, 0);
      accepted = (chttp_server_adapter_http_completion_t){0};
      rejected = (chttp_server_adapter_http_completion_t){0};
      resumed = (chttp_server_adapter_http_completion_t){0};
      quiesce_cleanup_gate = &gate;
      quiesce_cleanup_flow = flow;
      check_not_null(flow);
      config.flow = flow;
      config.adapter_name = "http.server";
      config.source_name = "http_in";
      config.server = &native_config;
      config.method = CHTTP_METHOD_POST;
      config.path = "/flow";
      check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
      quiesce_cleanup_server = server;
      check_equal(turbo_flow_chttp_server_quiesce(server), SALTS_ESHUTDOWN);
      check_equal(turbo_flow_chttp_server_resume(server), SALTS_ESHUTDOWN);
      check_equal(turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u), SALTS_OK);
      flow_test_operation_t operation_gate_10 =
          flow_test_operation_init("test.gate", chttp_server_adapter_gate, &gate);
      operation_gate_10.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation_gate_10.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation_gate_10), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      port = snapshot.bound_port;
      check_greater(snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port), 0);
      check_equal(chttp_async_client_init(&client, &client_config), SALTS_OK);
      quiesce_cleanup_client = &client;
      options.connection_uri = uri;
      options.authority = "127.0.0.1";
      options.target = "/flow";
      options.method = CHTTP_METHOD_POST;
      options.body = "accepted";
      options.body_size = sizeof("accepted") - 1u;
      options.protocol = protocols[index];
      options.on_complete = chttp_server_adapter_http_complete;
      options.user = &accepted;
      check_equal(chttp_async_client_submit(&client, &options, &requests[0]), SALTS_OK);
      for (size_t poll = 0u;
           poll < MAX_POLLS && atomic_load_explicit(&gate.entered, memory_order_acquire) == 0u;
           ++poll)
        check_equal(chttp_async_client_poll(&client, POLL_MS, &completions), SALTS_OK);
      check_equal(atomic_load_explicit(&gate.entered, memory_order_acquire), (size_t)1u);
      check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
      command.expected_generation = managed.generation;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "http-quiesce-1", sizeof("http-quiesce-1"));
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_OK);
      check_equal(command_result.generation_before, managed.generation);
      check_equal(command_result.generation_after, managed.generation + 1u);
      check_equal(command_result.observed_generation, managed.generation + 1u);
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_DRAINING);
      check_equal(managed.in_flight, (uint64_t)1u);
      check_equal(managed.accepted, (uint64_t)1u);
      check_equal(managed.completed, (uint64_t)0u);
      command_result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_OK);
      check_equal(command_result.replayed, 1);
      check_equal(command_result.generation_after, managed.generation);
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
      command.expected_generation = managed.generation - 1u;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "http-quiesce-stale", sizeof("http-quiesce-stale"));
      command_result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_EBUSY);
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
      command.expected_generation = managed.generation;
      command.deadline_ns = 1u;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "http-quiesce-expired", sizeof("http-quiesce-expired"));
      command_result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_ETIMEDOUT);
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT;
      command.expected_generation = managed.generation;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "http-replace-masked", sizeof("http-replace-masked"));
      memcpy(command.endpoint_host, "127.0.0.1", sizeof("127.0.0.1"));
      command.endpoint_port = 80;
      command_result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_ENOTSUP);
      check_equal(turbo_flow_chttp_server_quiesce(server), SALTS_OK);
      check_equal(turbo_flow_chttp_server_quiesce(server), SALTS_OK);
      {
        turbo_flow_managed_boundary_snapshot_t unchanged =
            TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
        check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &unchanged), SALTS_OK);
        check_equal(unchanged.generation, managed.generation);
      }
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_QUIESCED);
      check_equal(snapshot.active_requests, (size_t)1u);
      options.user = &rejected;
      check_equal(chttp_async_client_submit(&client, &options, &requests[1]), SALTS_OK);
      for (size_t poll = 0u; poll < MAX_POLLS && rejected.calls == 0u; ++poll)
        check_equal(chttp_async_client_poll(&client, POLL_MS, &completions), SALTS_OK);
      check_equal(rejected.calls, (size_t)1u);
      check_equal(rejected.status, SALTS_OK);
      check_equal(rejected.response_status, config.unavailable_status);
      check_equal(accepted.calls, (size_t)0u);
      atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
      for (size_t poll = 0u; poll < MAX_POLLS && accepted.calls == 0u; ++poll)
        check_equal(chttp_async_client_poll(&client, POLL_MS, &completions), SALTS_OK);
      check_equal(accepted.calls, (size_t)1u);
      check_equal(accepted.status, SALTS_OK);
      check_equal(accepted.response_status, config.success_status);
      check_equal(accepted.body, "accepted");
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_QUIESCED);
      check_equal(snapshot.active_requests, (size_t)0u);
      check_equal(snapshot.admitted_requests, (uint64_t)1u);
      check_equal(snapshot.completed_requests, (uint64_t)1u);
      check_equal(snapshot.rejected_requests, (uint64_t)1u);
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT);
      check_equal(managed.accepted, (uint64_t)1u);
      check_equal(managed.completed, (uint64_t)1u);
      check_equal(managed.rejected, (uint64_t)1u);
      check_equal(managed.in_flight, (uint64_t)0u);
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
      command.expected_generation = managed.generation;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "http-resume-1", sizeof("http-resume-1"));
      command_result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_OK);
      check_equal(command_result.generation_after, managed.generation + 1u);
      check_equal(turbo_flow_chttp_server_resume(server), SALTS_OK);
      check_equal(turbo_flow_chttp_server_resume(server), SALTS_OK);
      options.user = &resumed;
      check_equal(chttp_async_client_submit(&client, &options, &requests[2]), SALTS_OK);
      for (size_t poll = 0u; poll < MAX_POLLS && resumed.calls == 0u; ++poll)
        check_equal(chttp_async_client_poll(&client, POLL_MS, &completions), SALTS_OK);
      check_equal(resumed.calls, (size_t)1u);
      check_equal(resumed.response_status, config.success_status);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.bound_port, port);
      check_equal(snapshot.completed_requests, (uint64_t)2u);
      check_equal(chttp_async_client_stop(&client, 5000u), SALTS_OK);
      check_equal(chttp_async_client_destroy(&client), SALTS_OK);
      quiesce_cleanup_client = NULL;
      check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
      check_equal(turbo_flow_chttp_server_quiesce(server), SALTS_OK);
      {
        turbo_flow_managed_boundary_snapshot_t quiesced =
            TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
        check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &quiesced), SALTS_OK);
        check_equal(quiesced.generation, managed.generation + 1u);
        check_equal(quiesced.state, TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT);
      }
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_resume(server), SALTS_ESHUTDOWN);
      command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
      command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
      command.expected_generation = managed.generation + 1u;
      memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
      memcpy(command.idempotency_key, "http-resume-stopped", sizeof("http-resume-stopped"));
      command_result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
      check_equal(turbo_flow_resource_command(flow, &command, &command_result), SALTS_ESHUTDOWN);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_server_snapshot(server, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_SERVER_RUNNING);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      quiesce_cleanup_flow = NULL;
      check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
      quiesce_cleanup_server = NULL;
    }
    check_equal(turbo_flow_chttp_server_quiesce(NULL), SALTS_EINVAL);
    check_equal(turbo_flow_chttp_server_resume(NULL), SALTS_EINVAL);
  }

  it("drains accepted deferred work and rejects new admission during stop") {
    static const char *dsl = "source http_in adapter http.server\n"
                             "stage gate operation test.gate\n"
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
    flow_test_operation_t operation_gate_11 =
        flow_test_operation_init("test.gate", chttp_server_adapter_gate, &gate);
    operation_gate_11.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_gate_11.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_gate_11), SALTS_OK);
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
    check_equal(turbo_flow_register_async_terminal_adapter_ex(flow, "holding.sink", &adapter_ops,
                                                              &async_ops, &sink, &schema),
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
