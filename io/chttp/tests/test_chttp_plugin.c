#include "../../../tests/flow_operation_fixture.h"
#include "../../../tests/install_chttp_plugin_consumer/chttp_plugin_fixtures.h"
#include "../../cnet/tests/listener_source_tls_fixture.h"
#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"
#include <cflow/publishers.h>
#include <http_client/http.h>
#include <http_server/http.h>
#include <salts/clock.h>
#include <salts/thread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { PLUGIN_TEST_YAML_BYTES = 16384 };
static int replace_once(const char *input, const char *needle, const char *replacement,
                        char output[PLUGIN_TEST_YAML_BYTES]) {
  const char *at = strstr(input, needle);
  if (!at) return SALTS_ENOENT;
  size_t prefix = (size_t)(at - input);
  size_t total = strlen(input) - strlen(needle) + strlen(replacement);
  if (total >= PLUGIN_TEST_YAML_BYTES) return SALTS_ENOSPC;
  memcpy(output, input, prefix);
  memcpy(output + prefix, replacement, strlen(replacement));
  memcpy(output + prefix + strlen(replacement), at + strlen(needle),
         strlen(at + strlen(needle)) + 1u);
  return SALTS_OK;
}

static void replace_field(char yaml[PLUGIN_TEST_YAML_BYTES], const char *before,
                          const char *after) {
  char temporary[PLUGIN_TEST_YAML_BYTES];
  check_equal(replace_once(yaml, before, after, temporary), SALTS_OK);
  memcpy(yaml, temporary, strlen(temporary) + 1u);
}

static void tls_path_field(char yaml[PLUGIN_TEST_YAML_BYTES], const char *key, const char *path) {
  char before[128], after[1024];
  snprintf(before, sizeof(before), "%s: \"\"", key);
  check_less(snprintf(after, sizeof(after), "%s: \"%s\"", key, path), (int)sizeof(after));
  for (char *p = after; *p; ++p)
    if (*p == '\\') *p = '/';
  replace_field(yaml, before, after);
}

static void tls_yaml(char yaml[PLUGIN_TEST_YAML_BYTES]) {
  char io_buffer[128];
  snprintf(io_buffer, sizeof(io_buffer), "network_tls_io_buffer_bytes: %u",
           (unsigned)CNET_TLS_MIN_IO_BUFFER_BYTES);
  replace_field(yaml, "tls_enabled: false", "tls_enabled: true");
  replace_field(yaml, "network_tls_handshake_timeout_ms: 0",
                "network_tls_handshake_timeout_ms: 5000");
  replace_field(yaml, "network_tls_io_buffer_bytes: 0", io_buffer);
  replace_field(yaml, "tls_alpn: []", "tls_alpn: [\"h2\"]");
}

static int preflight_yaml(const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
                          size_t index, const char *name, const char *yaml) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_resolved_config_t *resolved = NULL;
  int rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error);
  if (rc == SALTS_OK)
    rc = catalog->adapter_providers[index].preflight(catalog->adapter_providers[index].ctx,
                                                     resolved, name, &error);
  turbo_flow_resolved_config_destroy(resolved);
  return rc;
}

static turbo_flow_plugin_host_t *plugin_host(size_t capacity) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_host_t *host = NULL;
  config.module_capacity = 2u;
  config.transactional_adapter_provider_capacity = capacity;
  check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
  return host;
}

static int plugin_sink(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

typedef struct traffic_fixture_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_generation_t *generation;
  turbo_flow_plugin_generation_t *cleanup_generation;
} traffic_fixture_t;

static void rejected_generation_preserves_flow(traffic_fixture_t *owner,
                                               turbo_flow_plugin_catalog_snapshot_t *snapshot,
                                               const char *yaml) {
  static const char graph[] =
      "source input\nstage request adapter client\nstage output operation test.output\nstage "
      "main {\n input -> request -> output\n}\n";
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_generation_config_t config = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_plugin_generation_t *generation = NULL;
  turbo_flow_t *flow = turbo_flow_create(), *original = flow;
  check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
  int rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &config, NULL, &generation,
                                             &owner->cleanup_generation, &error);
  check_not_equal(rc, SALTS_OK);
  check_true(flow == original);
  check_null(generation);
  if (owner->cleanup_generation) {
    check_equal(turbo_flow_plugin_generation_destroy(owner->cleanup_generation, 1000u, &error),
                SALTS_OK);
    owner->cleanup_generation = NULL;
  }
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_destroy(flow);
}

static void lexical_preflight(const char *input, size_t provider, const char *name,
                              const char *before, const char *after, int accepted) {
  turbo_flow_plugin_host_t *host = plugin_host(3u);
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  char yaml[PLUGIN_TEST_YAML_BYTES];
  check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &error), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
              SALTS_OK);
  check_equal(replace_once(input, before, after, yaml), SALTS_OK);
  int rc = preflight_yaml(&catalog, provider, name, yaml);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  if (accepted) check_equal(rc, SALTS_OK);
  else check_not_equal(rc, SALTS_OK);
}

static void traffic_open_path(traffic_fixture_t *f, const char *path, const char *yaml,
                              const char *graph, turbo_flow_stage_fn sink, void *ctx) {
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  f->cleanup_generation = NULL;
  f->host = plugin_host(3u);
  check_equal(turbo_flow_plugin_host_load(f->host, path, &pe), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(f->host, &snapshot, &pe), SALTS_OK);
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), SALTS_OK);
  check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
  flow_test_operation_t operation_output_0 = flow_test_operation_init("test.output", sink, ctx);
  operation_output_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_output_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  if (sink) check_equal(flow_test_operation_register(flow, &operation_output_0), SALTS_OK);
  int rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, NULL, &f->generation,
                                               &f->cleanup_generation, &error);
  info("traffic generation: %s %s", error.path, error.message);
  check_equal(rc, SALTS_OK);
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(f->generation)), SALTS_OK);
}
static void traffic_open(traffic_fixture_t *f, const char *yaml, const char *graph,
                         turbo_flow_stage_fn sink, void *ctx) {
  traffic_open_path(f, TURBO_FLOW_CHTTP_PLUGIN_PATH, yaml, graph, sink, ctx);
}
static void traffic_close(traffic_fixture_t *f) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  if (f->cleanup_generation) {
    check_equal(turbo_flow_plugin_generation_destroy(f->cleanup_generation, 1000u, &error),
                SALTS_OK);
    f->cleanup_generation = NULL;
  }
  if (f->generation) {
    check_equal(turbo_flow_plugin_generation_destroy(f->generation, 1000u, &error), SALTS_OK);
    f->generation = NULL;
  }
  check_equal(turbo_flow_plugin_host_destroy(f->host, 1000u, &pe), SALTS_OK);
}
static cnet_client_config traffic_network(void) {
  cnet_client_config n = {0};
#if defined(_WIN32)
  n.backend = NATIVE_IO_BACKEND_IOCP;
#elif defined(__APPLE__)
  n.backend = NATIVE_IO_BACKEND_KQUEUE;
#else
  n.backend = NATIVE_IO_BACKEND_EPOLL;
#endif
  n.connection_capacity = 4u;
  n.command_capacity = 16u;
  n.request_capacity = 16u;
  n.completion_batch_capacity = 16u;
  n.event_capacity = 16u;
  n.max_send_bytes = 65536u;
  n.receive_buffer_bytes = 4096u;
  n.connect_timeout_ms = 1000u;
  n.read_timeout_ms = 1000u;
  n.write_timeout_ms = 1000u;
  return n;
}
static chttp_server_config traffic_server_config(void) {
  chttp_server_config n = {0};
  n.host = "127.0.0.1";
  n.backlog = 8u;
  n.network = traffic_network();
  n.route_capacity = 1u;
  n.max_target_bytes = 256u;
  n.max_header_count = 16u;
  n.max_header_bytes = 1024u;
  n.max_request_body_bytes = 512u;
  n.max_response_header_count = 16u;
  n.max_response_header_bytes = 1024u;
  n.max_response_body_bytes = 512u;
  n.poll_slice_ms = 1u;
  n.enable_http2 = 1;
  n.h2_stream_capacity = 4u;
  n.h2_input_buffer_bytes = 65536u;
  n.h2_output_buffer_bytes = 65536u;
  n.h2_hpack_dynamic_table_bytes = 4096u;
  n.h2_max_settings_count = 16u;
  return n;
}
static chttp_client_config traffic_client_config(void) {
  chttp_client_config n = {0};
  n.network = traffic_network();
  n.request_capacity = 4u;
  n.max_start_line_bytes = 256u;
  n.max_header_count = 16u;
  n.max_header_bytes = 1024u;
  n.max_request_body_bytes = 512u;
  n.max_response_body_bytes = 512u;
  n.max_informational_responses = 2u;
  n.h2_input_buffer_bytes = 65536u;
  n.h2_hpack_dynamic_table_bytes = 4096u;
  n.h2_max_settings_count = 16u;
  return n;
}
static uint16_t traffic_port(void) {
  chttp_server s = {0};
  chttp_server_config config = traffic_server_config();
  uint16_t port = 0u;
  check_equal(chttp_server_init(&s, &config), SALTS_OK);
  check_equal(chttp_server_start(&s), SALTS_OK);
  check_equal(chttp_server_port(&s, &port), SALTS_OK);
  check_equal(chttp_server_stop(&s, 1000u), SALTS_OK);
  check_equal(chttp_server_destroy(&s), SALTS_OK);
  return port;
}
static void traffic_h2(const char *input, char output[PLUGIN_TEST_YAML_BYTES]) {
  static const char *const from[] = {
      "protocol: \"h1\"",          "h2_stream_capacity: 0",           "h2_input_buffer_bytes: 0",
      "h2_output_buffer_bytes: 0", "h2_hpack_dynamic_table_bytes: 0", "h2_max_settings_count: 0"};
  static const char *const to[] = {"protocol: \"h1_h2\"",
                                   "h2_stream_capacity: 4",
                                   "h2_input_buffer_bytes: 65536",
                                   "h2_output_buffer_bytes: 65536",
                                   "h2_hpack_dynamic_table_bytes: 4096",
                                   "h2_max_settings_count: 16"};
  char temp[PLUGIN_TEST_YAML_BYTES];
  check_equal(replace_once(input, from[0], to[0], output), SALTS_OK);
  for (size_t i = 1u; i < sizeof(from) / sizeof(from[0]); ++i) {
    check_equal(replace_once(output, from[i], to[i], temp), SALTS_OK);
    memcpy(output, temp, strlen(temp) + 1u);
  }
}
static int traffic_echo(void *ctx, const chttp_server_request_view *request,
                        chttp_server_response *response) {
  (void)ctx;
  return chttp_server_reply(response, 200u, "application/octet-stream", request->body,
                            request->body_size);
}
typedef struct traffic_probe_s {
  atomic_int done;
  atomic_int status;
  atomic_int received;
} traffic_probe_t;
static int traffic_sink(turbo_flow_msg_t *message, void *ctx) {
  traffic_probe_t *p = (traffic_probe_t *)ctx;
  if (message->payload.len != 4u || memcmp(message->payload.data, "ping", 4u)) return SALTS_EPROTO;
  atomic_fetch_add(&p->received, 1);
  return SALTS_OK;
}
static void traffic_done(void *ctx, const turbo_flow_publish_result_t *result) {
  traffic_probe_t *p = (traffic_probe_t *)ctx;
  atomic_store(&p->status, result->status);
  atomic_fetch_add(&p->done, 1);
}

enum { ISOLATION_REQUESTS = 2, ISOLATION_TIMEOUT_MS = 3000 };
typedef struct isolation_peer_s {
  chttp_server_deferred requests[ISOLATION_REQUESTS];
  atomic_int admitted;
} isolation_peer_t;

static int isolation_defer(void *ctx, const chttp_server_request_view *request,
                           chttp_server_response *response) {
  isolation_peer_t *peer = ctx;
  (void)request;
  int index = atomic_load(&peer->admitted);
  if (index >= ISOLATION_REQUESTS) return SALTS_ENOSPC;
  int rc = chttp_server_response_defer(response, &peer->requests[index]);
  if (rc == SALTS_OK) atomic_fetch_add(&peer->admitted, 1);
  return rc;
}

typedef struct teardown_probe_s {
  atomic_int entered, release, exited;
  int completions, status;
  unsigned response_status;
} teardown_probe_t;
static int teardown_hold(turbo_flow_msg_t *message, void *ctx) {
  teardown_probe_t *probe = ctx;
  (void)message;
  atomic_fetch_add(&probe->entered, 1);
  uint64_t deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
  while (!atomic_load(&probe->release) && salts_monotonic_ms() < deadline)
    salts_sleep_ms(1u);
  atomic_fetch_add(&probe->exited, 1);
  return atomic_load(&probe->release) ? SALTS_OK : SALTS_ETIMEDOUT;
}
static void teardown_http_done(void *ctx, chttp_request request,
                               const chttp_response_view *response, const chttp_error *error) {
  teardown_probe_t *probe = ctx;
  (void)request;
  ++probe->completions;
  probe->status = error ? error->status : SALTS_OK;
  probe->response_status = response ? response->status_code : 0u;
}

static void start_failure_destroy(traffic_fixture_t *owner, const char *path, const char *yaml,
                                  const char *graph, int client) {
  owner->host = plugin_host(3u);
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  check_equal(turbo_flow_plugin_host_load(owner->host, path, &pe), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(owner->host, &snapshot, &pe), SALTS_OK);
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), SALTS_OK);
  check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
  if (client) {
    flow_test_operation_t operation_output_1 =
        flow_test_operation_init("test.output", plugin_sink, NULL);
    check_equal(flow_test_operation_register(flow, &operation_output_1), SALTS_OK);
  }
  check_equal(turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, NULL,
                                                  &owner->generation, &owner->cleanup_generation,
                                                  &error),
              SALTS_OK);
  if (owner->cleanup_generation) {
    check_equal(turbo_flow_plugin_generation_destroy(owner->cleanup_generation, 1000u, &error),
                SALTS_OK);
    owner->cleanup_generation = NULL;
  }
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  int start_status = turbo_flow_start(turbo_flow_plugin_generation_flow(owner->generation));
  if (client) {
    check_equal(start_status, SALTS_ENOMEM);
  } else {
    check_not_equal(start_status, SALTS_OK);
  }
  check_equal(turbo_flow_plugin_host_destroy(owner->host, 1000u, &pe), SALTS_EBUSY);
  int destroy_status = turbo_flow_plugin_generation_destroy(owner->generation, 1000u, &error);
  info("start failure destroy: %d %s %s", destroy_status, error.path, error.message);
  check_equal(destroy_status, SALTS_OK);
  owner->generation = NULL;
  check_equal(turbo_flow_plugin_host_destroy(owner->host, 1000u, &pe), SALTS_OK);
  owner->host = NULL;
}

spec("chttp_plugin") {
  it("destroys a plugin client generation after native init allocation failure") {
    traffic_fixture_t owner = {0};
    start_failure_destroy(
        &owner, CHTTP_DELIVERY_FIXTURE_5, client_yaml,
        "source input\nstage request adapter client\nstage output operation test.output\nstage "
        "main {\n input -> request -> output\n}\n",
        1);
    if (owner.host) traffic_close(&owner);
  }
  for (size_t kind = 0u; kind < 2u; ++kind) {
    it(kind == 0u ? "destroys a server generation after an occupied listener start failure"
                  : "destroys a websocket generation after an occupied listener start failure") {
      static const char *const yamls[] = {server_yaml, websocket_yaml};
      static const char *const graphs[] = {"source input adapter server\nstage output adapter "
                                           "server\nstage main {\n input -> output\n}\n",
                                           "source input adapter websocket\nstage output adapter "
                                           "websocket\nstage main {\n input -> output\n}\n"};
      chttp_server listener = {0};
      chttp_server_config config = traffic_server_config();
      uint16_t port = 0u;
      char field[64], yaml[PLUGIN_TEST_YAML_BYTES];
      check_equal(chttp_server_init(&listener, &config), SALTS_OK);
      check_equal(chttp_server_start(&listener), SALTS_OK);
      check_equal(chttp_server_port(&listener, &port), SALTS_OK);
      snprintf(field, sizeof(field), "bind_port: %u", (unsigned)port);
      check_equal(replace_once(yamls[kind], "bind_port: 0", field, yaml), SALTS_OK);
      traffic_fixture_t owner = {0};
      start_failure_destroy(&owner, TURBO_FLOW_CHTTP_PLUGIN_PATH, yaml, graphs[kind], 0);
      if (owner.host) traffic_close(&owner);
      check_equal(chttp_server_stop(&listener, 1000u), SALTS_OK);
      check_equal(chttp_server_destroy(&listener), SALTS_OK);
    }
  }
  it("retains a server-owned accepted request after owner quiesce timeout and releases it on "
     "retry") {
    static const char graph[] = "source input adapter server\nstage output operation "
                                "test.output\nstage reply adapter server\n"
                                "stage main {\n input -> output -> reply\n}\n";
    traffic_fixture_t f = {0};
    teardown_probe_t probe = {0};
    char yaml[PLUGIN_TEST_YAML_BYTES], port_field[64], uri[128];
    uint16_t port = traffic_port();
    snprintf(port_field, sizeof(port_field), "bind_port: %u", (unsigned)port);
    check_equal(replace_once(server_yaml, "bind_port: 0", port_field, yaml), SALTS_OK);
    traffic_open_path(&f, CHTTP_DELIVERY_FIXTURE_3, yaml, graph, teardown_hold, &probe);
    turbo_flow_t *original = turbo_flow_plugin_generation_flow(f.generation);
    chttp_async_client client = {0};
    chttp_client_config nc = traffic_client_config();
    chttp_request_options options = {0};
    chttp_request request = {0};
    snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned)port);
    options.connection_uri = uri;
    options.authority = "127.0.0.1";
    options.target = "/echo";
    options.method = CHTTP_METHOD_POST;
    options.body = "ping";
    options.body_size = 4u;
    options.on_complete = teardown_http_done;
    options.user = &probe;
    check_equal(chttp_async_client_init(&client, &nc), SALTS_OK);
    check_equal(chttp_async_client_submit(&client, &options, &request), SALTS_OK);
    size_t completions = 0u;
    uint64_t deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
    while (!atomic_load(&probe.entered) && salts_monotonic_ms() < deadline)
      check_equal(chttp_async_client_poll(&client, 1u, &completions), SALTS_OK);
    check_equal(atomic_load(&probe.entered), 1);
    check_equal(atomic_load(&probe.exited), 0);
    check_equal(probe.completions, 0);
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    /* Native Source owns this publication; no external run/claim or caller lease exists. */
    check_equal(turbo_flow_plugin_generation_destroy(f.generation, 0u, &error), SALTS_ETIMEDOUT);
    check_not_null(strstr(error.path, ".owner.quiesce"));
    check_true(turbo_flow_plugin_generation_flow(f.generation) == original);
    check_equal(turbo_flow_plugin_generation_owner_count(f.generation), 1u);
    check_equal(turbo_flow_plugin_host_module_count(f.host), 1u);
    check_equal(turbo_flow_plugin_host_destroy(f.host, 0u, &pe), SALTS_EBUSY);
    check_equal(probe.completions, 0);
    atomic_store(&probe.release, 1);
    deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
    while (!probe.completions && salts_monotonic_ms() < deadline)
      check_equal(chttp_async_client_poll(&client, 1u, &completions), SALTS_OK);
    check_equal(probe.completions, 1);
    check_equal(probe.status, SALTS_OK);
    check_equal(probe.response_status, 200u);
    check_equal(atomic_load(&probe.exited), 1);
    traffic_close(&f);
    check_equal(chttp_async_client_stop(&client, 1000u), SALTS_OK);
    check_equal(chttp_async_client_destroy(&client), SALTS_OK);
    check_equal(probe.completions, 1);
  }
  it("retains an open WebSocket after owner quiesce busy and safely tears it down on retry") {
    static const char graph[] = "source input adapter websocket\nstage output adapter websocket\n"
                                "stage main {\n input -> output\n}\n";
    traffic_fixture_t f = {0};
    char yaml[PLUGIN_TEST_YAML_BYTES], port_field[64], uri[128];
    uint16_t port = traffic_port();
    snprintf(port_field, sizeof(port_field), "bind_port: %u", (unsigned)port);
    check_equal(replace_once(websocket_yaml, "bind_port: 0", port_field, yaml), SALTS_OK);
    traffic_open_path(&f, CHTTP_DELIVERY_FIXTURE_4, yaml, graph, NULL, NULL);
    chttp_websocket_client client = {0};
    chttp_websocket_client_config nc = {.size = sizeof(nc)};
    nc.network = traffic_network();
    nc.max_frame_bytes = 512u;
    nc.max_message_bytes = 1024u;
    nc.max_buffered_input_bytes = 4096u;
    nc.max_handshake_header_bytes = 4096u;
    nc.event_capacity = 8u;
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_event event = {0};
    unsigned status = 0u;
    snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/echo", (unsigned)port);
    options.uri = uri;
    options.timeout_ms = 1000u;
    options.protocol = CHTTP_HTTP_1_1;
    check_equal(chttp_websocket_client_init(&client, &nc), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &status), SALTS_OK);
    check_equal(status, 101u);
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_generation_destroy(f.generation, 0u, &error), SALTS_EBUSY);
    check_not_null(strstr(error.path, ".owner.quiesce"));
    check_equal(turbo_flow_plugin_generation_owner_count(f.generation), 1u);
    check_equal(turbo_flow_plugin_host_destroy(f.host, 0u, &pe), SALTS_EBUSY);
    check_equal(chttp_websocket_client_send_text(&client, "ping", 4u, 1000u), SALTS_OK);
    check_equal(chttp_websocket_client_receive(&client, 1000u, &event), SALTS_OK);
    check_equal(event.size, 4u);
    check_equal(memcmp(event.data, "ping", 4u), 0);
    traffic_close(&f);
    check_equal(chttp_websocket_client_receive(&client, 1000u, &event), SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_CLOSE);
    check_equal(chttp_websocket_client_destroy(&client, 1000u), SALTS_OK);
  }
  it("retains the plugin until an active Graph run and its accepted emit claim settle") {
    static const char graph[] =
        "source input\nstage request adapter client\nstage output operation test.output\nstage "
        "main {\n input -> request -> output\n}\n";
    chttp_server server = {0};
    chttp_server_config nc = traffic_server_config();
    isolation_peer_t peer = {0};
    traffic_fixture_t f = {0};
    traffic_probe_t sink = {0};
    char yaml[PLUGIN_TEST_YAML_BYTES], uri[128];
    uint16_t port = 0u;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    turbo_flow_run_config_t rc = TURBO_FLOW_RUN_CONFIG_INIT;
    cflow_publisher publisher = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_msg_t message;
    turbo_flow_msg_init(&message);
    message.id = 1u;
    message.owned_payload = tstr_dup("ping");
    message.payload = tstr_to_v(message.owned_payload);
    check_equal(chttp_server_init(&server, &nc), SALTS_OK);
    check_equal(chttp_server_route(&server, CHTTP_METHOD_POST, "/echo", isolation_defer, &peer),
                SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned)port);
    check_equal(replace_once(client_yaml, "tcp://127.0.0.1:9", uri, yaml), SALTS_OK);
    traffic_open(&f, yaml, graph, traffic_sink, &sink);
    check_true(cflow_scheduler_inline_init(&scheduler));
    check_true(cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &message, 1u));
    rc.scheduler = &scheduler;
    check_equal(turbo_flow_plugin_generation_lease_acquire(f.generation), SALTS_OK);
    check_equal(turbo_flow_run_open(turbo_flow_plugin_generation_flow(f.generation), "input",
                                    &publisher, &rc, &run),
                SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    uint64_t deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
    while (atomic_load(&peer.admitted) != 1 && salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_plugin_generation_poll(f.generation, 1u, &error), SALTS_OK);
    check_equal(atomic_load(&peer.admitted), 1);
    check_not_null(peer.requests[0].impl);
    check_equal(turbo_flow_run_snapshot(run, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_ACTIVE);
    check_equal(result.values, 0u);
    check_equal(turbo_flow_plugin_generation_destroy(f.generation, 0u, &error), SALTS_EBUSY);
    check_equal(turbo_flow_plugin_host_destroy(f.host, 0u, &pe), SALTS_EBUSY);
    chttp_server_deferred_response reply = {.size = sizeof(reply),
                                            .status_code = 200u,
                                            .content_type = "application/octet-stream",
                                            .body = "ping",
                                            .body_size = 4u};
    check_equal(chttp_server_deferred_reply(&peer.requests[0], &reply), SALTS_OK);
    deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
    int wait_status = SALTS_ETIMEDOUT;
    while (wait_status == SALTS_ETIMEDOUT && salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_plugin_generation_poll(f.generation, 1u, &error), SALTS_OK);
      wait_status = turbo_flow_run_wait(run, 0u, &result);
    }
    check_equal(wait_status, SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(result.values, 1u);
    check_equal(atomic_load(&sink.received), 1);
    turbo_flow_run_close(run);
    check_equal(turbo_flow_plugin_generation_lease_release(f.generation), SALTS_OK);
    cflow_scheduler_destroy(&scheduler);
    turbo_flow_msg_cleanup(&message);
    traffic_close(&f);
    check_equal(atomic_load(&sink.received), 1);
    check_equal(chttp_server_stop(&server, 1000u), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }
  it("rolls back the first real owner after second-owner allocation failure with exact allocator "
     "pairing") {
    static const char graph[] =
        "source input\nsource inbound adapter server\nstage request adapter client\n"
        "stage reply adapter server\nstage output operation test.output\nstage main {\n"
        " input -> request -> output\n inbound -> reply\n}\n";
    char yaml[PLUGIN_TEST_YAML_BYTES];
    const char *server = strstr(server_yaml, "  server:");
    check_not_null(server);
    check_less(snprintf(yaml, sizeof(yaml), "%s%s", client_yaml, server), (int)sizeof(yaml));
    turbo_flow_plugin_host_t *host = plugin_host(3u);
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_t *cleanup_generation = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_equal(turbo_flow_plugin_host_load(host, CHTTP_DELIVERY_FIXTURE_0, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &ce), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
    flow_test_operation_t operation_output_2 =
        flow_test_operation_init("test.output", plugin_sink, NULL);
    check_equal(flow_test_operation_register(flow, &operation_output_2), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, NULL,
                                                    &generation, &cleanup_generation, &ce),
                SALTS_ENOMEM);
    check_null(flow);
    check_null(generation);
    if (cleanup_generation) {
      check_equal(turbo_flow_plugin_generation_destroy(cleanup_generation, 1000u, &ce), SALTS_OK);
      cleanup_generation = NULL;
    }
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    /* Fixture quiesce checks root + first owner allocation, exactly one owner free,
     * and failed third allocation. Destroy checks the remaining root free. */
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &pe), SALTS_OK);
  }
  it("rolls back earlier kinds when a distinct DLL already owns the second or third kind") {
    static const char *const fixtures[] = {CHTTP_DELIVERY_FIXTURE_1, CHTTP_DELIVERY_FIXTURE_2};
    static const char *const kinds[] = {"chttp.server", "chttp.websocket_server"};
    for (size_t i = 0u; i < 2u; ++i) {
      turbo_flow_plugin_host_t *host = plugin_host(6u);
      turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
      turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
          TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
      check_equal(turbo_flow_plugin_host_load(host, fixtures[i], &pe), SALTS_OK);
      for (size_t attempt = 0u; attempt < 2u; ++attempt) {
        check_not_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &pe),
                        SALTS_OK);
        check_equal(pe.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
        check_equal(turbo_flow_plugin_host_module_count(host), 1u);
        check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 1u);
      }
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
      check_equal(
          turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
          SALTS_OK);
      check_equal(catalog.adapter_provider_count, 1u);
      check_equal(catalog.adapter_providers[0].kind, kinds[i]);
      check_equal(preflight_yaml(&catalog, 0u, i ? "websocket" : "server",
                                 i ? websocket_yaml : server_yaml),
                  SALTS_OK);
      turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &pe), SALTS_OK);
    }
  }
  it("keeps an accepted H2 sibling alive when the peer cancels one shared-session stream") {
    static const char graph[] =
        "source input\nstage request adapter client\nstage output operation test.output\nstage "
        "main {\n input -> request -> output\n}\n";
    chttp_server server = {0};
    chttp_server_config nc = traffic_server_config();
    chttp_server_stats stats = {0};
    isolation_peer_t peer = {0};
    traffic_fixture_t f = {0};
    traffic_probe_t probes[ISOLATION_REQUESTS] = {0}, sink = {0};
    char yaml[PLUGIN_TEST_YAML_BYTES], uri[128];
    uint16_t port = 0u;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(chttp_server_init(&server, &nc), SALTS_OK);
    check_equal(chttp_server_route(&server, CHTTP_METHOD_POST, "/echo", isolation_defer, &peer),
                SALTS_OK);
    check_equal(chttp_server_start(&server), SALTS_OK);
    check_equal(chttp_server_port(&server, &port), SALTS_OK);
    snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned)port);
    check_equal(replace_once(client_yaml, "tcp://127.0.0.1:9", uri, yaml), SALTS_OK);
    replace_field(yaml, "protocol: \"h1\"", "protocol: \"h2\"");
    traffic_open(&f, yaml, graph, traffic_sink, &sink);
    check_equal(turbo_flow_plugin_generation_lease_acquire(f.generation), SALTS_OK);
    for (size_t i = 0u; i < ISOLATION_REQUESTS; ++i) {
      turbo_flow_msg_t message;
      turbo_flow_msg_init(&message);
      message.id = i + 1u;
      message.owned_payload = tstr_dup("ping");
      message.payload = tstr_to_v(message.owned_payload);
      check_equal(turbo_flow_publish_async(turbo_flow_plugin_generation_flow(f.generation), "input",
                                           &message, traffic_done, &probes[i]),
                  SALTS_OK);
      turbo_flow_msg_cleanup(&message);
    }
    uint64_t deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
    while (atomic_load(&peer.admitted) < ISOLATION_REQUESTS && salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_plugin_generation_poll(f.generation, 1u, &error), SALTS_OK);
    check_equal(atomic_load(&peer.admitted), ISOLATION_REQUESTS);
    check_equal(chttp_server_get_stats(&server, &stats), SALTS_OK);
    check_equal(stats.accepted_connections, (uint64_t)1u);
    check_equal(stats.active_connections, (size_t)1u);
    check_equal(atomic_load(&probes[0].done) + atomic_load(&probes[1].done), 0);
    check_equal(turbo_flow_plugin_host_destroy(f.host, 0u, &pe), SALTS_EBUSY);
    check_equal(chttp_server_deferred_cancel(&peer.requests[0]), SALTS_OK);
    chttp_server_deferred_response reply = {.size = sizeof(reply),
                                            .status_code = 200u,
                                            .content_type = "application/octet-stream",
                                            .body = "ping",
                                            .body_size = 4u};
    check_equal(chttp_server_deferred_reply(&peer.requests[1], &reply), SALTS_OK);
    deadline = salts_monotonic_ms() + ISOLATION_TIMEOUT_MS;
    while (atomic_load(&probes[0].done) + atomic_load(&probes[1].done) < ISOLATION_REQUESTS &&
           salts_monotonic_ms() < deadline)
      check_equal(turbo_flow_plugin_generation_poll(f.generation, 1u, &error), SALTS_OK);
    check_equal(atomic_load(&probes[0].done), 1);
    check_equal(atomic_load(&probes[1].done), 1);
    int successes =
        (atomic_load(&probes[0].status) == SALTS_OK) + (atomic_load(&probes[1].status) == SALTS_OK);
    check_equal(successes, 1);
    check_equal(atomic_load(&sink.received), 1);
    check_equal(chttp_server_get_stats(&server, &stats), SALTS_OK);
    check_equal(stats.accepted_connections, (uint64_t)1u);
    check_equal(turbo_flow_plugin_generation_lease_release(f.generation), SALTS_OK);
    traffic_close(&f);
    check_equal(atomic_load(&probes[0].done), 1);
    check_equal(atomic_load(&probes[1].done), 1);
    check_equal(chttp_server_stop(&server, 1000u), SALTS_OK);
    check_equal(chttp_server_destroy(&server), SALTS_OK);
  }
  it("rejects a WebSocket subprotocol containing a non-token slash in preflight") {
    lexical_preflight(websocket_yaml, 2u, "websocket", "subprotocol: \"\"",
                      "subprotocol: \"chat/v1\"", 0);
  }
  it("rejects a response content type containing a control byte in preflight") {
    lexical_preflight(server_yaml, 1u, "server",
                      "response_content_type: \"application/octet-stream\"",
                      "response_content_type: \"text/plain\\u0001\"", 0);
  }
  it("rejects an error content type containing a control byte in preflight") {
    lexical_preflight(server_yaml, 1u, "server", "error_content_type: \"text/plain\"",
                      "error_content_type: \"text/plain\\u0001\"", 0);
  }
  it("accepts a complete HTTP token or an absent WebSocket subprotocol") {
    lexical_preflight(websocket_yaml, 2u, "websocket", "subprotocol: \"\"",
                      "subprotocol: \"chat-v1.!#$%&'*+^_`|~\"", 1);
    lexical_preflight(websocket_yaml, 2u, "websocket", "subprotocol: \"\"", "subprotocol: \"\"", 1);
  }
  it("accepts content type header values with legal spaces tabs and parameters") {
    lexical_preflight(server_yaml, 1u, "server",
                      "response_content_type: \"application/octet-stream\"",
                      "response_content_type: \"text/plain;\\tcharset=utf-8\"", 1);
    lexical_preflight(server_yaml, 1u, "server", "error_content_type: \"text/plain\"",
                      "error_content_type: \"text/plain; charset=utf-8\"", 1);
  }
  it("round trips WebSocket frames through the plugin on H1 and explicit H1 plus H2 listeners") {
    static const char graph[] = "source input adapter websocket\nstage output adapter "
                                "websocket\nstage main {\n input -> output\n}\n";
    for (int mode = 0; mode < 3; ++mode) {
      int h2 = mode != 0, tls = mode == 2;
      listener_source_tls_fixture_t certificates = {0};
      static const char *const alpn[] = {"h2"};
      chttp_tls_profile profile = {0};
      traffic_fixture_t f = {0};
      char yaml[PLUGIN_TEST_YAML_BYTES], h2_yaml[PLUGIN_TEST_YAML_BYTES];
      char port_field[64], uri[128];
      uint16_t port = traffic_port();
      snprintf(port_field, sizeof(port_field), "bind_port: %u", (unsigned)port);
      check_equal(replace_once(websocket_yaml, "bind_port: 0", port_field, yaml), SALTS_OK);
      if (h2) traffic_h2(yaml, h2_yaml);
      if (tls) {
        check_equal(listener_source_tls_fixture_init(&certificates), SALTS_OK);
        tls_yaml(h2_yaml);
        replace_field(h2_yaml, "tls_alpn: [\"h2\"]", "tls_alpn: [\"h2\", \"http/1.1\"]");
        tls_path_field(h2_yaml, "tls_cert_file", certificates.cert_path);
        tls_path_field(h2_yaml, "tls_key_file", certificates.key_path);
        cnet_tls_client_config trust = {.size = sizeof(trust),
                                        .ca_file = certificates.cert_path,
                                        .server_name = "localhost",
                                        .alpn_protocols = alpn,
                                        .alpn_protocol_count = 1u};
        check_equal(chttp_tls_profile_init(&profile, &trust), SALTS_OK);
      }
      traffic_open(&f, h2 ? h2_yaml : yaml, graph, NULL, NULL);
      chttp_websocket_client client = {0};
      chttp_websocket_client_config nc = {.size = sizeof(nc)};
      nc.network = traffic_network();
      nc.max_frame_bytes = 512u;
      nc.max_message_bytes = 1024u;
      nc.max_buffered_input_bytes = 4096u;
      nc.max_handshake_header_bytes = 4096u;
      nc.event_capacity = 8u;
      nc.h2_input_buffer_bytes = 65536u;
      nc.h2_hpack_dynamic_table_bytes = 4096u;
      nc.h2_max_settings_count = 16u;
      chttp_websocket_connect_options options = {.size = sizeof(options)};
      chttp_websocket_event event = {0};
      unsigned int status = 0u;
      if (tls) {
        nc.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
        nc.network.tls_handshake_timeout_ms = 5000u;
        options.tls = &profile;
      }
      snprintf(uri, sizeof(uri), "%s://127.0.0.1:%u/echo", tls ? "wss" : "ws", (unsigned)port);
      options.uri = uri;
      options.timeout_ms = 1000u;
      options.protocol = h2 ? CHTTP_HTTP_2 : CHTTP_HTTP_1_1;
      check_equal(chttp_websocket_client_init(&client, &nc), SALTS_OK);
      check_equal(chttp_websocket_client_connect(&client, &options, &status), SALTS_OK);
      check_equal(status, h2 ? 200u : 101u);
      turbo_flow_plugin_error_t busy = TURBO_FLOW_PLUGIN_ERROR_INIT;
      check_equal(turbo_flow_plugin_host_destroy(f.host, 0u, &busy), SALTS_EBUSY);
      check_equal(chttp_websocket_client_send_text(&client, "ping", 4u, 1000u), SALTS_OK);
      check_equal(chttp_websocket_client_receive(&client, 1000u, &event), SALTS_OK);
      check_equal(event.size, 4u);
      check_equal(memcmp(event.data, "ping", 4u), 0);
      check_equal(chttp_websocket_client_close(&client, 1000u, NULL, 0u, 1000u), SALTS_OK);
      check_equal(chttp_websocket_client_destroy(&client, 1000u), SALTS_OK);
      traffic_close(&f);
      if (tls) {
        check_equal(chttp_tls_profile_destroy(&profile), SALTS_OK);
        listener_source_tls_fixture_destroy(&certificates);
      }
    }
  }
  it("rolls back registered server owners when compile rejects a nonterminal response stage") {
    static const char graph[] =
        "source input adapter server\nstage reply adapter server\nstage "
        "output operation test.output\nstage main {\n input -> reply -> output\n}\n";
    turbo_flow_plugin_host_t *host = plugin_host(3u);
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_t *cleanup_generation = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(server_yaml, strlen(server_yaml), &resolved, &error),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
    flow_test_operation_t operation_output_3 =
        flow_test_operation_init("test.output", plugin_sink, NULL);
    check_equal(flow_test_operation_register(flow, &operation_output_3), SALTS_OK);
    check_not_equal(turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, NULL,
                                                        &generation, &cleanup_generation, &error),
                    SALTS_OK);
    check_null(generation);
    check_null(flow);
    if (cleanup_generation) {
      check_equal(turbo_flow_plugin_generation_destroy(cleanup_generation, 1000u, &error),
                  SALTS_OK);
      cleanup_generation = NULL;
    }
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &pe), SALTS_OK);
  }
  it("polls plugin client requests through H1 and multiplexed H2 with owned response payloads") {
    static const char graph[] =
        "source input\nstage request adapter client\nstage output operation test.output\nstage "
        "main {\n input -> request -> output\n}\n";
    for (int mode = 0; mode < 3; ++mode) {
      int h2 = mode != 0, tls = mode == 2;
      listener_source_tls_fixture_t certificates = {0};
      static const char *const alpn[] = {"h2"};
      cnet_tls_server_config server_tls = {0};
      chttp_server server = {0};
      chttp_server_config nc = traffic_server_config();
      uint16_t port = 0u;
      char uri[128], yaml[PLUGIN_TEST_YAML_BYTES], protocol_yaml[PLUGIN_TEST_YAML_BYTES];
      traffic_fixture_t f = {0};
      traffic_probe_t probe = {0};
      if (tls) {
        check_equal(listener_source_tls_fixture_init(&certificates), SALTS_OK);
        server_tls = (cnet_tls_server_config){.size = sizeof(server_tls),
                                              .cert_file = certificates.cert_path,
                                              .key_file = certificates.key_path,
                                              .client_auth = CNET_TLS_CLIENT_AUTH_NONE,
                                              .alpn_protocols = alpn,
                                              .alpn_protocol_count = 1u};
        nc.tls = &server_tls;
        nc.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
        nc.network.tls_handshake_timeout_ms = 5000u;
      }
      check_equal(chttp_server_init(&server, &nc), SALTS_OK);
      check_equal(chttp_server_route(&server, CHTTP_METHOD_POST, "/echo", traffic_echo, NULL),
                  SALTS_OK);
      check_equal(chttp_server_start(&server), SALTS_OK);
      check_equal(chttp_server_port(&server, &port), SALTS_OK);
      snprintf(uri, sizeof(uri), "%s://127.0.0.1:%u", tls ? "tls" : "tcp", (unsigned)port);
      check_equal(replace_once(client_yaml, "tcp://127.0.0.1:9", uri, yaml), SALTS_OK);
      check_equal(replace_once(yaml, "protocol: \"h1\"",
                               h2 ? "protocol: \"h2\"" : "protocol: \"h1\"", protocol_yaml),
                  SALTS_OK);
      if (tls) {
        tls_yaml(protocol_yaml);
        tls_path_field(protocol_yaml, "tls_ca_file", certificates.cert_path);
        replace_field(protocol_yaml, "tls_server_name: \"\"", "tls_server_name: \"localhost\"");
      }
      traffic_open(&f, protocol_yaml, graph, traffic_sink, &probe);
      check_equal(turbo_flow_plugin_generation_lease_acquire(f.generation), SALTS_OK);
      for (uint64_t i = 1u; i <= 2u; ++i) {
        turbo_flow_msg_t message;
        turbo_flow_msg_init(&message);
        message.id = i;
        message.owned_payload = tstr_dup("ping");
        message.payload = tstr_to_v(message.owned_payload);
        check_equal(turbo_flow_publish_async(turbo_flow_plugin_generation_flow(f.generation),
                                             "input", &message, traffic_done, &probe),
                    SALTS_OK);
        turbo_flow_msg_cleanup(&message);
      }
      uint64_t deadline = salts_monotonic_ms() + 3000u;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      while (atomic_load(&probe.done) < 2 && salts_monotonic_ms() < deadline)
        check_equal(turbo_flow_plugin_generation_poll(f.generation, 1u, &error), SALTS_OK);
      check_equal(atomic_load(&probe.done), 2);
      check_equal(atomic_load(&probe.status), SALTS_OK);
      check_equal(atomic_load(&probe.received), 2);
      if (h2) {
        chttp_server_stats stats = {0};
        check_equal(chttp_server_get_stats(&server, &stats), SALTS_OK);
        check_equal(stats.accepted_connections, (uint64_t)1u);
        check_equal(stats.requests, (uint64_t)2u);
      }
      check_equal(turbo_flow_plugin_generation_lease_release(f.generation), SALTS_OK);
      traffic_close(&f);
      check_equal(chttp_server_stop(&server, 1000u), SALTS_OK);
      check_equal(chttp_server_destroy(&server), SALTS_OK);
      if (tls) listener_source_tls_fixture_destroy(&certificates);
    }
  }
  it("serves deferred H1 and H2 replies through the plugin source and terminal") {
    static const char graph[] = "source input adapter server\nstage output adapter server\nstage "
                                "main {\n input -> output\n}\n";
    for (int mode = 0; mode < 3; ++mode) {
      int h2 = mode != 0, tls = mode == 2;
      listener_source_tls_fixture_t certificates = {0};
      static const char *const alpn[] = {"h2"};
      cnet_tls_client_config client_tls = {0};
      chttp_tls_profile profile = {0};
      traffic_fixture_t f = {0};
      char yaml[PLUGIN_TEST_YAML_BYTES], h2_yaml[PLUGIN_TEST_YAML_BYTES];
      char port_field[64], uri[128];
      uint16_t port = traffic_port();
      snprintf(port_field, sizeof(port_field), "bind_port: %u", (unsigned)port);
      check_equal(replace_once(server_yaml, "bind_port: 0", port_field, yaml), SALTS_OK);
      if (h2) traffic_h2(yaml, h2_yaml);
      if (tls) {
        check_equal(listener_source_tls_fixture_init(&certificates), SALTS_OK);
        tls_yaml(h2_yaml);
        /* h1_h2 policy explicitly requires both advertised protocols. */
        replace_field(h2_yaml, "tls_alpn: [\"h2\"]", "tls_alpn: [\"h2\", \"http/1.1\"]");
        tls_path_field(h2_yaml, "tls_cert_file", certificates.cert_path);
        tls_path_field(h2_yaml, "tls_key_file", certificates.key_path);
        client_tls = (cnet_tls_client_config){.size = sizeof(client_tls),
                                              .ca_file = certificates.cert_path,
                                              .server_name = "localhost",
                                              .alpn_protocols = alpn,
                                              .alpn_protocol_count = 1u};
        check_equal(chttp_tls_profile_init(&profile, &client_tls), SALTS_OK);
      }
      traffic_open(&f, h2 ? h2_yaml : yaml, graph, NULL, NULL);
      chttp_client client = {0};
      chttp_client_config nc = traffic_client_config();
      chttp_options options = {0};
      chttp_response response = {0};
      chttp_error error = {0};
      if (tls) {
        nc.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
        nc.network.tls_handshake_timeout_ms = 5000u;
        options.tls = &profile;
      }
      snprintf(uri, sizeof(uri), "%s://127.0.0.1:%u", tls ? "tls" : "tcp", (unsigned)port);
      options.connection_uri = uri;
      options.authority = "127.0.0.1";
      options.target = "/echo";
      options.body = "ping";
      options.body_size = 4u;
      options.timeout_ms = 1000u;
      options.protocol = h2 ? CHTTP_HTTP_2 : CHTTP_HTTP_1_1;
      check_equal(chttp_client_init(&client, &nc), SALTS_OK);
      check_equal(chttp_post(&client, &options, &response, &error), SALTS_OK);
      check_equal(response.status_code, 200u);
      check_equal(response.body_size, 4u);
      check_equal(memcmp(response.body, "ping", 4u), 0);
      chttp_response_destroy(&response);
      check_equal(chttp_client_destroy(&client, 1000u), SALTS_OK);
      traffic_close(&f);
      if (tls) {
        check_equal(chttp_tls_profile_destroy(&profile), SALTS_OK);
        listener_source_tls_fixture_destroy(&certificates);
      }
    }
  }
  it("materializes starts and detaches every native owner while generation pins the DLL") {
    static const char *const yamls[] = {client_yaml, server_yaml, websocket_yaml};
    static const char *const graphs[] = {
        "source input\nstage request adapter client\nstage "
        "output operation test.output\nstage main {\n input -> request -> output\n}\n",
        "source input adapter server\nstage output adapter "
        "server\nstage main {\n input -> output\n}\n",
        "source input adapter websocket\nstage output adapter "
        "websocket\nstage main {\n input -> output\n}\n"};
    for (size_t i = 0u; i < 3u; ++i) {
      turbo_flow_plugin_host_t *host = plugin_host(3u);
      turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
      turbo_flow_resolved_config_t *resolved = NULL;
      turbo_flow_plugin_generation_t *generation = NULL;
      turbo_flow_plugin_generation_t *cleanup_generation = NULL;
      turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &pe), SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
      check_equal(turbo_flow_config_resolve_yaml(yamls[i], strlen(yamls[i]), &resolved, &error),
                  SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, graphs[i], strlen(graphs[i])), SALTS_OK);
      if (i == 0u) {
        flow_test_operation_t operation_output_4 =
            flow_test_operation_init("test.output", plugin_sink, NULL);
        check_equal(flow_test_operation_register(flow, &operation_output_4), SALTS_OK);
      }
      int create_rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, NULL,
                                                          &generation, &cleanup_generation, &error);
      info("kind %zu generation: %s %s", i, error.path, error.message);
      check_equal(create_rc, SALTS_OK);
      check_null(flow);
      check_equal(turbo_flow_plugin_generation_owner_count(generation), 1u);
      if (cleanup_generation) {
        check_equal(turbo_flow_plugin_generation_destroy(cleanup_generation, 1000u, &error),
                    SALTS_OK);
        cleanup_generation = NULL;
      }
      turbo_flow_resolved_config_destroy(resolved);
      turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
      check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
      if (i == 0u) check_equal(turbo_flow_plugin_generation_poll(generation, 0u, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &pe), SALTS_EBUSY);
      check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &pe), SALTS_OK);
    }
  }
  it("validates all explicit policies and rejects malformed fields before materialization") {
    turbo_flow_plugin_host_t *host = plugin_host(3u);
    traffic_fixture_t rejected_owner = {host, NULL, NULL};
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    char yaml[PLUGIN_TEST_YAML_BYTES];
    check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    check_equal(preflight_yaml(&catalog, 0u, "client", client_yaml), SALTS_OK);
    check_equal(preflight_yaml(&catalog, 1u, "server", server_yaml), SALTS_OK);
    check_equal(preflight_yaml(&catalog, 2u, "websocket", websocket_yaml), SALTS_OK);
    static const struct {
      const char *before;
      const char *after;
    } invalid[] = {{"schema_version: 1", "schema_version: 2"},
                   {"poll_budget_ms: 1", "poll_budget_ms: \"1\""},
                   {"poll_budget_ms: 1", "poll_budget_ms: 0"},
                   {"poll_budget_ms: 1", "unknown_budget: 1"},
                   {"network_command_capacity: 16", "network_command_capacity: 3"},
                   {"request_capacity: 4", "request_capacity: 0"},
                   {"request_capacity: 4", "request_capacity: 18446744073709551616"},
                   {"tcp://127.0.0.1:9", "tcp://127.0.0.1"},
                   {"tcp://127.0.0.1:9", "tls://127.0.0.1:9"},
                   {"protocol: \"h1\"", "protocol: \"auto\""},
                   {"tls_alpn: []", "tls_alpn: [\"h2\"]"},
                   {"header_values: []", "header_values: [\"extra\"]"},
                   {"max_header_count: 16", "max_header_count: 2"},
                   {"max_attempts: 1", "max_attempts: false"}};
    for (size_t i = 0u; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
      info("invalid case %zu: %s", i, invalid[i].after);
      check_equal(replace_once(client_yaml, invalid[i].before, invalid[i].after, yaml), SALTS_OK);
      check_not_equal(preflight_yaml(&catalog, 0u, "client", yaml), SALTS_OK);
      rejected_generation_preserves_flow(&rejected_owner, snapshot, yaml);
      check_null(rejected_owner.cleanup_generation);
    }
    check_equal(replace_once(server_yaml, "protocol: \"h1\"", "protocol: \"h2\"", yaml), SALTS_OK);
    check_equal(preflight_yaml(&catalog, 1u, "server", yaml), SALTS_ENOTSUP);
    check_equal(
        replace_once(server_yaml, "bind_host: \"127.0.0.1\"", "bind_host: \"localhost\"", yaml),
        SALTS_OK);
    check_not_equal(preflight_yaml(&catalog, 1u, "server", yaml), SALTS_OK);
    check_equal(replace_once(server_yaml, "path: \"/echo\"", "path: \"/echo?query\"", yaml),
                SALTS_OK);
    check_not_equal(preflight_yaml(&catalog, 1u, "server", yaml), SALTS_OK);
    check_equal(replace_once(server_yaml, "path: \"/echo\"", "path: \"/:id\"", yaml), SALTS_OK);
    check_not_equal(preflight_yaml(&catalog, 1u, "server", yaml), SALTS_OK);
    char header_yaml[PLUGIN_TEST_YAML_BYTES];
    check_equal(replace_once(client_yaml, "protocol: \"h1\"", "protocol: \"h2\"", header_yaml),
                SALTS_OK);
    check_equal(replace_once(header_yaml, "network_max_send_bytes: 65536",
                             "network_max_send_bytes: 4096", yaml),
                SALTS_OK);
    check_not_equal(preflight_yaml(&catalog, 0u, "client", yaml), SALTS_OK);
    check_equal(
        replace_once(client_yaml, "header_names: []", "header_names: [\"Host\"]", header_yaml),
        SALTS_OK);
    check_equal(
        replace_once(header_yaml, "header_values: []", "header_values: [\"override\"]", yaml),
        SALTS_OK);
    check_not_equal(preflight_yaml(&catalog, 0u, "client", yaml), SALTS_OK);
    check_equal(replace_once(websocket_yaml, "max_buffered_input_bytes: 1024",
                             "max_buffered_input_bytes: 512", yaml),
                SALTS_OK);
    check_not_equal(preflight_yaml(&catalog, 2u, "websocket", yaml), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }
  it("loads all three kinds through one DLL and preserves catalog on duplicate load") {
    turbo_flow_plugin_host_t *host = plugin_host(6u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    static const char *const kinds[] = {"chttp.client", "chttp.server", "chttp.websocket_server"};
    check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 3u);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    check_equal(catalog.adapter_provider_count, 3u);
    for (size_t i = 0u; i < 3u; ++i)
      check_equal(catalog.adapter_providers[i].kind, kinds[i]);
    check_not_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &error),
                    SALTS_OK);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 3u);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }
  it("rolls back partial registration when the third kind exceeds catalog capacity") {
    turbo_flow_plugin_host_t *host = plugin_host(2u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &error),
                SALTS_ENOSPC);
    check_equal(turbo_flow_plugin_host_module_count(host), 0u);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 0u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }
}
