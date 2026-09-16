#include <http_client/http.h>
#include <stdatomic.h>

static atomic_int chttp_stop_fault_status;
static atomic_uint chttp_stop_fault_calls;

/* Only the isolated adapter build redirects its stop call here. */
int chttp_test_client_stop(chttp_async_client *client, uint32_t timeout_ms) {
  const int status =
      atomic_exchange_explicit(&chttp_stop_fault_status, SALTS_OK, memory_order_relaxed);
  atomic_fetch_add_explicit(&chttp_stop_fault_calls, 1u, memory_order_relaxed);
  return status == SALTS_OK ? chttp_async_client_stop(client, timeout_ms) : status;
}

/* Reuse the real protocol fixtures and rerun their contracts against the
 * isolated adapter; no alternate protocol implementation is supplied. */
#include "test_chttp_adapter.c"

spec("CHTTP bounded client stop failure") {
  it("returns stop failure once and retains native storage until explicit retry") {
    for (unsigned int protocol = 0u; protocol < 2u; ++protocol) {
      static const char graph[] = "source input\n"
                                  "stage request adapter http.shutdown\n"
                                  "stage output operation test.output\n"
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
      chttp_server_config server_config =
          protocol ? chttp_adapter_h2_server_config() : chttp_adapter_server_config();
      chttp_client_config client_config =
          protocol ? chttp_adapter_h2_client_config() : chttp_adapter_client_config();
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
      adapter_config.protocol = protocol ? CHTTP_HTTP_2 : CHTTP_HTTP_1_1;
      adapter_config.stop_timeout_ms = CHTTP_ADAPTER_TEST_TIMEOUT_MS;
      check_equal(turbo_flow_chttp_client_register(&adapter_config, &client), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
      flow_test_operation_t operation_output_0 =
          flow_test_operation_init("test.output", chttp_adapter_sink, &probe);
      operation_output_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation_output_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation_output_0), SALTS_OK);
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

      atomic_store_explicit(&chttp_stop_fault_status, SALTS_ETIMEDOUT, memory_order_relaxed);
      atomic_store_explicit(&chttp_stop_fault_calls, 0u, memory_order_relaxed);
      check_equal(turbo_flow_stop(flow), SALTS_ETIMEDOUT);
      check_equal(atomic_load_explicit(&chttp_stop_fault_calls, memory_order_relaxed), 1u);
      check_equal(turbo_flow_chttp_client_snapshot(client, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_CLIENT_FAILED);
      check_equal(snapshot.last_status, SALTS_ETIMEDOUT);
      check_equal(turbo_flow_chttp_client_destroy(client), SALTS_EBUSY);
      check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
      check_equal(chttp_server_deferred_reply(&deferred.handle, &deferred_response), SALTS_OK);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      check_equal(atomic_load_explicit(&chttp_stop_fault_calls, memory_order_relaxed), 2u);
      check_equal(atomic_load_explicit(&probe.publication_calls, memory_order_acquire), (size_t)1u);
      check_not_equal(atomic_load_explicit(&probe.publication_status, memory_order_acquire),
                      SALTS_OK);
      check_equal(atomic_load_explicit(&probe.sink_calls, memory_order_acquire), (size_t)0u);
      check_equal(turbo_flow_chttp_client_snapshot(client, &snapshot), SALTS_OK);
      check_equal(snapshot.state, TURBO_FLOW_CHTTP_CLIENT_STOPPED);
      check_equal(snapshot.completed_requests, (uint64_t)1u);

      check_equal(turbo_flow_start(flow), SALTS_EALREADY);
      check_equal(turbo_flow_chttp_client_snapshot(client, &snapshot), SALTS_OK);
      check_equal(snapshot.completed_requests, (uint64_t)1u);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_client_destroy(client), SALTS_OK);
      check_equal(chttp_server_stop(&server, CHTTP_ADAPTER_TEST_TIMEOUT_MS), SALTS_OK);
      check_equal(chttp_server_destroy(&server), SALTS_OK);
    }
  }
}
