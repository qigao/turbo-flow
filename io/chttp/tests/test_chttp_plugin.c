#include "chttp_plugin_fixtures.h"
#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"
#include <chttp/chttp.h>
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

typedef struct traffic_fixture_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_generation_t *generation;
} traffic_fixture_t;
static void traffic_open(traffic_fixture_t *f, const char *yaml, const char *graph,
                         turbo_flow_stage_fn sink, void *ctx) {
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  f->host = plugin_host(3u);
  check_equal(turbo_flow_plugin_host_load(f->host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &pe), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(f->host, &snapshot, &pe), SALTS_OK);
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), SALTS_OK);
  check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
  if (sink) check_equal(turbo_flow_register_stage_ex(flow, "output", sink, ctx, NULL), SALTS_OK);
  int rc =
      turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, &f->generation, &error);
  info("traffic generation: %s %s", error.path, error.message);
  check_equal(rc, SALTS_OK);
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(f->generation)), SALTS_OK);
}
static void traffic_close(traffic_fixture_t *f) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  check_equal(turbo_flow_plugin_generation_destroy(f->generation, 1000u, &error), SALTS_OK);
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

spec("chttp_plugin") {
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
    for (int h2 = 0; h2 < 2; ++h2) {
      traffic_fixture_t f = {0};
      char yaml[PLUGIN_TEST_YAML_BYTES], h2_yaml[PLUGIN_TEST_YAML_BYTES];
      char port_field[64], uri[128];
      uint16_t port = traffic_port();
      snprintf(port_field, sizeof(port_field), "bind_port: %u", (unsigned)port);
      check_equal(replace_once(websocket_yaml, "bind_port: 0", port_field, yaml), SALTS_OK);
      if (h2) traffic_h2(yaml, h2_yaml);
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
      snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/echo", (unsigned)port);
      options.uri = uri;
      options.timeout_ms = 1000u;
      options.protocol = h2 ? CHTTP_HTTP_2 : CHTTP_HTTP_1_1;
      check_equal(chttp_websocket_client_init(&client, &nc), SALTS_OK);
      check_equal(chttp_websocket_client_connect(&client, &options, &status), SALTS_OK);
      check_equal(status, h2 ? 200u : 101u);
      check_equal(chttp_websocket_client_send_text(&client, "ping", 4u, 1000u), SALTS_OK);
      check_equal(chttp_websocket_client_receive(&client, 1000u, &event), SALTS_OK);
      check_equal(event.size, 4u);
      check_equal(memcmp(event.data, "ping", 4u), 0);
      check_equal(chttp_websocket_client_close(&client, 1000u, NULL, 0u, 1000u), SALTS_OK);
      check_equal(chttp_websocket_client_destroy(&client, 1000u), SALTS_OK);
      traffic_close(&f);
    }
  }
  it("rolls back registered server owners when compile rejects a nonterminal response stage") {
    static const char graph[] = "source input adapter server\nstage reply adapter server\nstage "
                                "output\nstage main {\n input -> reply -> output\n}\n";
    turbo_flow_plugin_host_t *host = plugin_host(3u);
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(server_yaml, strlen(server_yaml), &resolved, &error),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "output", plugin_sink, NULL, NULL), SALTS_OK);
    check_not_equal(
        turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, &generation, &error),
        SALTS_OK);
    check_null(generation);
    check_null(flow);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &pe), SALTS_OK);
  }
  it("polls plugin client requests through H1 and multiplexed H2 with owned response payloads") {
    static const char graph[] = "source input\nstage request adapter client\nstage output\nstage "
                                "main {\n input -> request -> output\n}\n";
    for (int h2 = 0; h2 < 2; ++h2) {
      chttp_server server = {0};
      chttp_server_config nc = traffic_server_config();
      uint16_t port = 0u;
      char uri[128], yaml[PLUGIN_TEST_YAML_BYTES], protocol_yaml[PLUGIN_TEST_YAML_BYTES];
      traffic_fixture_t f = {0};
      traffic_probe_t probe = {0};
      check_equal(chttp_server_init(&server, &nc), SALTS_OK);
      check_equal(chttp_server_route(&server, CHTTP_METHOD_POST, "/echo", traffic_echo, NULL),
                  SALTS_OK);
      check_equal(chttp_server_start(&server), SALTS_OK);
      check_equal(chttp_server_port(&server, &port), SALTS_OK);
      snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned)port);
      check_equal(replace_once(client_yaml, "tcp://127.0.0.1:9", uri, yaml), SALTS_OK);
      check_equal(replace_once(yaml, "protocol: \"h1\"",
                               h2 ? "protocol: \"h2\"" : "protocol: \"h1\"", protocol_yaml),
                  SALTS_OK);
      traffic_open(&f, protocol_yaml, graph, traffic_sink, &probe);
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
      traffic_close(&f);
      check_equal(chttp_server_stop(&server, 1000u), SALTS_OK);
      check_equal(chttp_server_destroy(&server), SALTS_OK);
    }
  }
  it("serves deferred H1 and H2 replies through the plugin source and terminal") {
    static const char graph[] = "source input adapter server\nstage output adapter server\nstage "
                                "main {\n input -> output\n}\n";
    for (int h2 = 0; h2 < 2; ++h2) {
      traffic_fixture_t f = {0};
      char yaml[PLUGIN_TEST_YAML_BYTES], h2_yaml[PLUGIN_TEST_YAML_BYTES];
      char port_field[64], uri[128];
      uint16_t port = traffic_port();
      snprintf(port_field, sizeof(port_field), "bind_port: %u", (unsigned)port);
      check_equal(replace_once(server_yaml, "bind_port: 0", port_field, yaml), SALTS_OK);
      if (h2) traffic_h2(yaml, h2_yaml);
      traffic_open(&f, h2 ? h2_yaml : yaml, graph, NULL, NULL);
      chttp_client client = {0};
      chttp_client_config nc = traffic_client_config();
      chttp_options options = {0};
      chttp_response response = {0};
      chttp_error error = {0};
      snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned)port);
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
    }
  }
  it("materializes starts and detaches every native owner while generation pins the DLL") {
    static const char *const yamls[] = {client_yaml, server_yaml, websocket_yaml};
    static const char *const graphs[] = {"source input\nstage request adapter client\nstage "
                                         "output\nstage main {\n input -> request -> output\n}\n",
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
      turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CHTTP_PLUGIN_PATH, &pe), SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
      check_equal(turbo_flow_config_resolve_yaml(yamls[i], strlen(yamls[i]), &resolved, &error),
                  SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, graphs[i], strlen(graphs[i])), SALTS_OK);
      if (i == 0u)
        check_equal(turbo_flow_register_stage_ex(flow, "output", plugin_sink, NULL, NULL),
                    SALTS_OK);
      int create_rc =
          turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, &generation, &error);
      info("kind %zu generation: %s %s", i, error.path, error.message);
      check_equal(create_rc, SALTS_OK);
      check_null(flow);
      check_equal(turbo_flow_plugin_generation_owner_count(generation), 1u);
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
