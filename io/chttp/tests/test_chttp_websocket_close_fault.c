#include "tinytest.h"
#include "turbo_flow_chttp.h"
#include "salts/thread.h"
#include <stdatomic.h>
#include <stdio.h>

enum { CLOSE_TEST_TIMEOUT_MS = 5000 };
static atomic_int close_failure;
static atomic_uint close_calls;
int chttp_test_delayed_completion_close(turbo_flow_chttp_websocket_server_t *server);

/* The test support target redirects the adapter's native call here. */
int chttp_test_websocket_close(const chttp_server_websocket_session *session, uint16_t code,
                               const void *reason, size_t reason_size) {
  int status = atomic_exchange_explicit(&close_failure, SALTS_OK, memory_order_relaxed);
  atomic_fetch_add_explicit(&close_calls, 1u, memory_order_relaxed);
  return status == SALTS_OK ? chttp_server_websocket_close(session, code, reason, reason_size)
                            : status;
}

spec("CHTTP WebSocket close admission failure") {
  it("retains failed close ownership and retries only on explicit quiesce") {
    static const char graph[] = "source input adapter ws\n"
                                "stage output adapter ws\n"
                                "stage main {\n  input -> output\n}\n";
    chttp_server_config native = {0};
    chttp_websocket_client_config client_config = {.size = sizeof(client_config)};
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    chttp_websocket_client client = {0};
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    char uri[128];
    native.host = "127.0.0.1";
    native.backlog = 2u;
#if defined(_WIN32)
    native.network.backend = NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
    native.network.backend = NATIVE_IO_BACKEND_EPOLL;
#else
    native.network.backend = NATIVE_IO_BACKEND_KQUEUE;
#endif
    native.network.connection_capacity = 2u;
    native.network.command_capacity = 8u;
    native.network.command_buffer_bytes = 8192u;
    native.network.request_capacity = 8u;
    native.network.completion_batch_capacity = 8u;
    native.network.event_capacity = 8u;
    native.network.event_buffer_bytes = 8192u;
    native.network.max_send_bytes = 8192u;
    native.network.receive_buffer_bytes = 4096u;
    native.route_capacity = 1u;
    native.middleware_capacity = 1u;
    native.max_route_middleware_count = 1u;
    native.max_route_param_count = 1u;
    native.max_route_param_bytes = 128u;
    native.max_target_bytes = 256u;
    native.max_header_count = 16u;
    native.max_header_bytes = 4096u;
    native.max_request_body_bytes = 4096u;
    native.max_response_header_count = 16u;
    native.max_response_header_bytes = 1024u;
    native.max_response_body_bytes = 4096u;
    native.max_buffered_response_body_bytes = 4096u;
    native.poll_slice_ms = 1u;
    client_config.network = native.network;
    client_config.max_frame_bytes = 4096u;
    client_config.max_message_bytes = 4096u;
    client_config.max_buffered_input_bytes = 8192u;
    client_config.max_handshake_header_bytes = 4096u;
    client_config.event_capacity = 8u;
    config.flow = flow;
    config.adapter_name = "ws";
    config.source_name = "input";
    config.path = "/flow";
    config.server = &native;
    config.session_capacity = 2u;
    config.frame_capacity = 2u;
    config.max_frame_bytes = 4096u;
    config.max_message_bytes = 4096u;
    config.max_buffered_input_bytes = 8192u;
    atomic_init(&close_failure, SALTS_OK);
    atomic_init(&close_calls, 0u);
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow", snapshot.bound_port), 0);
    options.uri = uri;
    options.timeout_ms = CLOSE_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
    atomic_store_explicit(&close_failure, SALTS_ENOBUFS, memory_order_relaxed);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_ENOBUFS);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED);
    check_equal(snapshot.last_status, SALTS_ENOBUFS);
    check_equal(snapshot.active_sessions, (size_t)1u);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_EBUSY);
    check_equal(chttp_test_delayed_completion_close(server), SALTS_OK);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    check_equal(chttp_websocket_client_send_text(&client, "late", sizeof("late") - 1u,
                                                 CLOSE_TEST_TIMEOUT_MS), SALTS_OK);
    for (unsigned int elapsed = 0u; elapsed < CLOSE_TEST_TIMEOUT_MS; ++elapsed) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.frames_rejected != 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.frames_rejected, (uint64_t)1u);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_OK);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 2u);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_OK);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 2u);
    check_equal(chttp_websocket_client_receive(&client, CLOSE_TEST_TIMEOUT_MS, &event), SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_CLOSE);
    check_equal(event.close_code, (uint16_t)1013u);
    check_equal(chttp_websocket_client_destroy(&client, CLOSE_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING);
    check_equal(snapshot.active_sessions, (size_t)0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }
}
