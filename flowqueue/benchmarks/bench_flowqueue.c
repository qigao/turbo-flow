#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow.h"
#include "turbo_flow_queue.h"

#include <string.h>

enum {
  FLOWQUEUE_BENCH_CAPACITY = 1024,
  FLOWQUEUE_BENCH_BATCH = 256,
  FLOWQUEUE_BENCH_SAMPLES = 1000,
  FLOWQUEUE_BENCH_PAYLOAD_SIZE = 64
};

static turbo_flow_t *flowqueue_bench_sink(turbo_flow_queue_t *queue) {
  static const char dsl[] = "source input\n"
                            "stage enqueue adapter queue.sink\n"
                            "stage main {\n"
                            "  input -> enqueue\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_queue_register_sink_adapter(flow, "queue.sink", queue) != TURBO_OK ||
      turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK || turbo_flow_start(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int flowqueue_bench_publish(turbo_flow_t *flow, const char *payload, size_t payload_size) {
  turbo_flow_msg_t message;
  int status;
  turbo_flow_msg_init(&message);
  message.owned_payload = tstr_new_len(payload, payload_size);
  if (!message.owned_payload) return TURBO_ENOMEM;
  message.payload = tstr_to_v(message.owned_payload);
  status = turbo_flow_publish(flow, "input", &message);
  turbo_flow_msg_cleanup(&message);
  return status;
}

spec("flowqueue benchmark") {
  bench("bounded Disruptor message cycles") {
    static const char payload[FLOWQUEUE_BENCH_PAYLOAD_SIZE] = {1};
    turbo_flow_queue_config_t config;
    turbo_flow_queue_t *queue;
    turbo_flow_t *sink;
    int status = TURBO_OK;

    memset(&config, 0, sizeof(config));
    config.resource_uid = "queue:benchmark";
    config.owner_name = "flowqueue-benchmark";
    config.capacity = FLOWQUEUE_BENCH_CAPACITY;
    config.max_payload_size = FLOWQUEUE_BENCH_PAYLOAD_SIZE;
    config.full_policy = TURBO_FLOW_QUEUE_FULL_FAIL;
    queue = turbo_flow_queue_create(&config);
    check_not_null(queue);
    sink = flowqueue_bench_sink(queue);
    check_not_null(sink);

    benchmark_io("FlowQueue publish+claim+ack", FLOWQUEUE_BENCH_SAMPLES,
                 FLOWQUEUE_BENCH_BATCH,
                 FLOWQUEUE_BENCH_BATCH * FLOWQUEUE_BENCH_PAYLOAD_SIZE) {
      for (size_t i = 0u; i < FLOWQUEUE_BENCH_BATCH && status == TURBO_OK; ++i)
        status = flowqueue_bench_publish(sink, payload, sizeof(payload));
      for (size_t i = 0u; i < FLOWQUEUE_BENCH_BATCH && status == TURBO_OK; ++i) {
        turbo_flow_queue_claim_t claim = TURBO_FLOW_QUEUE_CLAIM_INIT;
        status = turbo_flow_queue_claim(queue, &claim);
        if (status == TURBO_OK) status = turbo_flow_queue_claim_ack(queue, claim.token);
      }
    }

    check_int_eq(status, TURBO_OK);
    check_int_eq(turbo_flow_stop(sink), TURBO_OK);
    turbo_flow_destroy(sink);
    check_int_eq(turbo_flow_queue_destroy(queue), TURBO_OK);
  }
}
