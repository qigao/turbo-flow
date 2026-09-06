#include "tinytest.h"
#include "turbo_flow_cnet.h"

#include "stream_source_pipe_fixture.h"

#include <salts/clock.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { STREAM_SINK_TEST_TIMEOUT_MS = 5000 };

typedef struct stream_sink_completion_s {
  atomic_size_t calls;
  atomic_int status;
} stream_sink_completion_t;

static cnet_client_config stream_sink_client_config(void) {
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

static void stream_sink_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  stream_sink_completion_t *completion = (stream_sink_completion_t *)ctx;
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

spec("TurboFlow CNet stream sink") {
  it("settles a Pipe terminal only after CNet reports the full write") {
    static const char graph[] = "source input\n"
                                "stage output adapter cnet.pipe.out\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    cnet_client_config client = stream_sink_client_config();
    stream_source_pipe_fixture_t pipe;
    turbo_flow_cnet_stream_sink_t *sink = NULL;
    turbo_flow_cnet_stream_sink_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
    turbo_flow_cnet_stream_sink_config_t config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
    stream_sink_completion_t first;
    stream_sink_completion_t second;
    turbo_flow_msg_t first_message;
    turbo_flow_msg_t second_message;
    turbo_flow_t *flow = turbo_flow_create();
    char uri[640];
    char received[32] = {0};
    uint64_t deadline;

    atomic_init(&first.calls, 0u);
    atomic_init(&first.status, SALTS_EALREADY);
    atomic_init(&second.calls, 0u);
    atomic_init(&second.status, SALTS_EALREADY);
    check_not_null(flow);
    check_equal(stream_source_pipe_fixture_start(&pipe), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "pipe://%s", pipe.name), 0);
    config.flow = flow;
    config.adapter_name = "cnet.pipe.out";
    config.uri = uri;
    config.client = &client;
    config.max_message_bytes = 256u;
    config.stop_timeout_ms = STREAM_SINK_TEST_TIMEOUT_MS;
    check_equal(turbo_flow_cnet_stream_sink_register(&config, &sink), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_cnet_stream_sink_poll(sink, 0u, &snapshot), SALTS_OK);
    check_equal(stream_source_pipe_fixture_finish(&pipe), SALTS_OK);

    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (snapshot.state != TURBO_FLOW_CNET_STREAM_SINK_CONNECTED &&
           salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    }
    check_equal(snapshot.state, TURBO_FLOW_CNET_STREAM_SINK_CONNECTED);

    turbo_flow_msg_init(&first_message);
    first_message.owned_payload = tstr_dup("pipe-terminal");
    first_message.payload = tstr_to_v(first_message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &first_message, stream_sink_complete, &first),
        SALTS_OK);
    turbo_flow_msg_cleanup(&first_message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    do {
      check_equal(turbo_flow_cnet_stream_sink_snapshot(sink, &snapshot), SALTS_OK);
      if (snapshot.active_requests == 0u) salts_sleep_ms(1u);
    } while (snapshot.active_requests == 0u && salts_monotonic_ms() < deadline);
    check_equal(snapshot.active_requests, (size_t)1u);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);

    turbo_flow_msg_init(&second_message);
    second_message.owned_payload = tstr_dup("capacity-full");
    second_message.payload = tstr_to_v(second_message.owned_payload);
    check_equal(
        turbo_flow_publish_async(flow, "input", &second_message, stream_sink_complete, &second),
        SALTS_OK);
    turbo_flow_msg_cleanup(&second_message);
    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&second.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_ENOSPC);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)0u);

    deadline = salts_monotonic_ms() + STREAM_SINK_TEST_TIMEOUT_MS;
    while (atomic_load_explicit(&first.calls, memory_order_acquire) == 0u &&
           salts_monotonic_ms() < deadline) {
      check_equal(turbo_flow_cnet_stream_sink_poll(sink, 1u, &snapshot), SALTS_OK);
    }
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_OK);
    check_equal(snapshot.messages_sent, (uint64_t)1u);
    check_equal(snapshot.bytes_sent, (uint64_t)(sizeof("pipe-terminal") - 1u));
    check_equal(stream_source_pipe_fixture_read(&pipe, received, sizeof("pipe-terminal") - 1u),
                SALTS_OK);
    check_equal(received, "pipe-terminal");

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_cnet_stream_sink_destroy(sink), SALTS_OK);
    stream_source_pipe_fixture_close(&pipe);
  }
}
