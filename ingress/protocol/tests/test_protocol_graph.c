#include "../../../tests/flow_operation_fixture.h"
#include "salts_error.h"
#include "salts_thread.h"
#include "tinytest.h"
#include "turbo_flow_protocol_graph.h"

#include <stdatomic.h>
#include <string.h>

typedef struct graph_probe_s {
  size_t calls;
  uint64_t message_id;
  uint32_t message_type;
  turbo_flow_msg_t retained;
  char operation[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  uint8_t payload[32];
  size_t payload_size;
} graph_probe_t;

typedef struct graph_async_gate_s {
  atomic_int entered;
  atomic_int allow_exit;
} graph_async_gate_t;

typedef struct graph_completion_probe_s {
  atomic_int calls;
  atomic_int valid_contract;
  atomic_int status;
  atomic_uint_fast64_t delivery_id;
  atomic_uint_fast64_t session_id;
  atomic_uint_fast64_t session_generation;
} graph_completion_probe_t;

typedef struct graph_publish_thread_s {
  turbo_flow_protocol_graph_sink_t *sink;
  turbo_flow_protocol_publish_request_t *request;
  turbo_flow_protocol_publish_disposition_t disposition;
  atomic_int returned;
  int status;
} graph_publish_thread_t;

static int graph_async_gate_stage(turbo_flow_msg_t *message, void *ctx) {
  graph_async_gate_t *gate = (graph_async_gate_t *)ctx;
  if (!gate || !message) return SALTS_EINVAL;
  atomic_store_explicit(&gate->entered, 1, memory_order_release);
  while (!atomic_load_explicit(&gate->allow_exit, memory_order_acquire))
    salts_sleep_ms(1);
  return SALTS_OK;
}

static void graph_completion_record(
    void *ctx, const turbo_flow_protocol_graph_completion_t *completion) {
  graph_completion_probe_t *probe = (graph_completion_probe_t *)ctx;
  if (!probe || !completion) return;
  atomic_store_explicit(
      &probe->valid_contract,
      completion->size >= sizeof(*completion) &&
          completion->abi_version == TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION,
      memory_order_relaxed);
  atomic_store_explicit(&probe->status, completion->status, memory_order_relaxed);
  atomic_store_explicit(&probe->delivery_id, completion->delivery_id, memory_order_relaxed);
  atomic_store_explicit(&probe->session_id, completion->session_id, memory_order_relaxed);
  atomic_store_explicit(&probe->session_generation, completion->session_generation,
                        memory_order_relaxed);
  atomic_fetch_add_explicit(&probe->calls, 1, memory_order_release);
}

static void graph_publish_thread_run(void *ctx) {
  graph_publish_thread_t *publish = (graph_publish_thread_t *)ctx;
  publish->status = turbo_flow_protocol_graph_publish(
      publish->sink, publish->request, &publish->disposition);
  atomic_store_explicit(&publish->returned, 1, memory_order_release);
}

static int graph_probe_stage(turbo_flow_msg_t *message, void *ctx) {
  graph_probe_t *probe = (graph_probe_t *)ctx;
  const turbo_flow_protocol_metadata_t *metadata =
      turbo_flow_protocol_graph_metadata(message);
  if (!probe || !message || !metadata ||
      message->payload.len > sizeof(probe->payload))
    return SALTS_EPROTO;
  probe->calls++;
  probe->message_id = message->id;
  probe->message_type = message->type;
  probe->payload_size = message->payload.len;
  memcpy(probe->payload, message->payload.data, message->payload.len);
  memcpy(probe->operation, metadata->operation, sizeof(probe->operation));
  return turbo_flow_msg_clone(&probe->retained, message);
}

spec("protocol graph bridge") {
  it("publishes a neutral protocol message without an MQTT boundary") {
    static const char *dsl = "source protocol_in\n"
                             "stage inspect operation test.inspect\n"
                             "stage main {\n"
                             "  protocol_in -> inspect\n"
                             "}\n";
    static const uint8_t payload[] = {0x01u, 0x02u, 0x03u};
    graph_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_protocol_graph_sink_t sink = TURBO_FLOW_PROTOCOL_GRAPH_SINK_INIT;
    turbo_flow_protocol_message_output_t message =
        TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_protocol_publish_request_t request =
        TURBO_FLOW_PROTOCOL_PUBLISH_REQUEST_INIT;
    turbo_flow_protocol_publish_disposition_t disposition =
        (turbo_flow_protocol_publish_disposition_t)0;
    const turbo_flow_protocol_metadata_t *retained_metadata;

    check_not_null(flow);
    turbo_flow_msg_init(&probe.retained);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_inspect_0 =
        flow_test_operation_init("test.inspect", graph_probe_stage, &probe);
    operation_inspect_0.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_inspect_0.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_inspect_0), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    message.payload = (uint8_t *)payload;
    message.payload_capacity = sizeof(payload);
    message.payload_size = sizeof(payload);
    message.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    message.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
    message.metadata.message_type = 17u;
    memcpy(message.metadata.device_id, "charger-1", sizeof("charger-1"));
    memcpy(message.metadata.operation, "Heartbeat", sizeof("Heartbeat"));
    request.delivery_id = 42u;
    request.session_id = 7u;
    request.session_generation = 1u;
    request.message = &message;
    sink.flow = flow;
    sink.source_name = "protocol_in";

    check_equal(turbo_flow_protocol_graph_publish(&sink, &request,
                                                  &disposition),
                 SALTS_OK);
    check_equal(disposition, TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED);
    check_equal(probe.calls, 1u);
    check_equal(probe.message_id, 42u);
    check_equal(probe.message_type, 17u);
    check_equal(probe.operation, "Heartbeat");
    check_equal(probe.payload_size, sizeof(payload));
    check_equal(probe.payload, payload, sizeof(payload));
    retained_metadata = turbo_flow_protocol_graph_metadata(&probe.retained);
    check_not_null(retained_metadata);
    check_equal(retained_metadata->operation, "Heartbeat");
    check_equal(probe.retained.payload.len, sizeof(payload));
    check_equal(probe.retained.payload.data, payload, sizeof(payload));

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&probe.retained);
    turbo_flow_destroy(flow);
  }

  it("admits async protocol messages without blocking and completes once") {
    static const char *dsl = "source protocol_in\n"
                             "stage gate operation test.gate\n"
                             "stage main {\n"
                             "  protocol_in -> gate\n"
                             "}\n";
    static const uint8_t payload[] = {0x10u, 0x20u};
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    turbo_flow_protocol_graph_sink_t sink = TURBO_FLOW_PROTOCOL_GRAPH_SINK_INIT;
    turbo_flow_protocol_message_output_t message = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_protocol_publish_request_t request = TURBO_FLOW_PROTOCOL_PUBLISH_REQUEST_INIT;
    graph_async_gate_t gate;
    graph_completion_probe_t completion;
    graph_publish_thread_t publish;
    salts_thread_t thread;
    turbo_flow_t *flow = turbo_flow_create();
    int returned_before_release;

    memset(&publish, 0, sizeof(publish));
    atomic_init(&gate.entered, 0);
    atomic_init(&gate.allow_exit, 0);
    atomic_init(&completion.calls, 0);
    atomic_init(&completion.valid_contract, 0);
    atomic_init(&completion.status, SALTS_EBUSY);
    atomic_init(&completion.delivery_id, 0u);
    atomic_init(&completion.session_id, 0u);
    atomic_init(&completion.session_generation, 0u);
    atomic_init(&publish.returned, 0);
    ingress.workers = 1u;
    ingress.queue_capacity = 2u;
    check_not_null(flow);
    check_equal(turbo_flow_configure_async_ingress(flow, &ingress), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_gate_1 =
        flow_test_operation_init("test.gate", graph_async_gate_stage, &gate);
    operation_gate_1.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_gate_1.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_gate_1), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    message.payload = (uint8_t *)payload;
    message.payload_capacity = sizeof(payload);
    message.payload_size = sizeof(payload);
    message.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    message.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
    message.metadata.message_type = 21u;
    request.delivery_id = 99u;
    request.session_id = 8u;
    request.session_generation = 3u;
    request.message = &message;
    sink.flow = flow;
    sink.source_name = "protocol_in";
    sink.source_handoff = TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED;
    sink.completion = graph_completion_record;
    sink.completion_ctx = &completion;
    publish.sink = &sink;
    publish.request = &request;
    publish.status = SALTS_EBUSY;

    check_equal(salts_thread_create(&thread, graph_publish_thread_run, &publish), SALTS_OK);
    for (int i = 0; i < 1000 && !atomic_load_explicit(&gate.entered, memory_order_acquire); ++i)
      salts_sleep_ms(1);
    returned_before_release = atomic_load_explicit(&publish.returned, memory_order_acquire);
    atomic_store_explicit(&gate.allow_exit, 1, memory_order_release);
    check_equal(salts_thread_join(&thread), SALTS_OK);
    for (int i = 0; i < 1000 && !atomic_load_explicit(&completion.calls, memory_order_acquire); ++i)
      salts_sleep_ms(1);

    check_equal(returned_before_release, 1);
    check_equal(publish.status, SALTS_OK);
    check_equal(publish.disposition, TURBO_FLOW_PROTOCOL_PUBLISH_PENDING);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), 1);
    check_equal(atomic_load_explicit(&completion.valid_contract, memory_order_relaxed), 1);
    check_equal(atomic_load_explicit(&completion.status, memory_order_relaxed), SALTS_OK);
    check_equal(atomic_load_explicit(&completion.delivery_id, memory_order_relaxed), 99u);
    check_equal(atomic_load_explicit(&completion.session_id, memory_order_relaxed), 8u);
    check_equal(atomic_load_explicit(&completion.session_generation, memory_order_relaxed), 3u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("preserves the V1 inline sink ABI") {
    typedef struct legacy_protocol_graph_sink_s {
      size_t size;
      uint32_t abi_version;
      turbo_flow_t *flow;
      const char *source_name;
    } legacy_protocol_graph_sink_t;
    static const char *dsl = "source protocol_in\n"
                             "stage inspect operation test.inspect\n"
                             "stage main {\n"
                             "  protocol_in -> inspect\n"
                             "}\n";
    static const uint8_t payload[] = {0x55u};
    graph_probe_t probe = {0};
    legacy_protocol_graph_sink_t legacy = {
        sizeof(legacy), TURBO_FLOW_PROTOCOL_GRAPH_ABI_VERSION, NULL, "protocol_in"};
    turbo_flow_protocol_message_output_t message = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_protocol_publish_request_t request = TURBO_FLOW_PROTOCOL_PUBLISH_REQUEST_INIT;
    turbo_flow_protocol_publish_disposition_t disposition =
        (turbo_flow_protocol_publish_disposition_t)0;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    turbo_flow_msg_init(&probe.retained);
    legacy.flow = flow;
    message.payload = (uint8_t *)payload;
    message.payload_capacity = sizeof(payload);
    message.payload_size = sizeof(payload);
    message.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    message.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
    message.metadata.message_type = 1u;
    request.delivery_id = 1u;
    request.session_id = 1u;
    request.session_generation = 1u;
    request.message = &message;
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_inspect_2 =
        flow_test_operation_init("test.inspect", graph_probe_stage, &probe);
    operation_inspect_2.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_inspect_2.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_inspect_2), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_protocol_graph_publish(
                    &legacy, &request, &disposition),
                SALTS_OK);
    check_equal(disposition, TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED);
    check_equal(probe.calls, 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&probe.retained);
    turbo_flow_destroy(flow);
  }

  it("does not enter pending or complete when bounded admission rejects") {
    static const char *dsl = "source protocol_in\n"
                             "stage inspect operation test.inspect\n"
                             "stage main {\n"
                             "  protocol_in -> inspect\n"
                             "}\n";
    static const uint8_t payload[] = {0x01u};
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    turbo_flow_protocol_graph_sink_t sink = TURBO_FLOW_PROTOCOL_GRAPH_SINK_INIT;
    turbo_flow_protocol_message_output_t message = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_protocol_publish_request_t request = TURBO_FLOW_PROTOCOL_PUBLISH_REQUEST_INIT;
    turbo_flow_protocol_publish_disposition_t disposition =
        (turbo_flow_protocol_publish_disposition_t)0;
    graph_completion_probe_t completion;
    graph_probe_t probe = {0};
    turbo_flow_t *flow = turbo_flow_create();

    atomic_init(&completion.calls, 0);
    atomic_init(&completion.valid_contract, 0);
    atomic_init(&completion.status, SALTS_EBUSY);
    atomic_init(&completion.delivery_id, 0u);
    atomic_init(&completion.session_id, 0u);
    atomic_init(&completion.session_generation, 0u);
    turbo_flow_msg_init(&probe.retained);
    ingress.workers = 1u;
    ingress.queue_capacity = 1u;
    ingress.max_message_bytes = 1u;
    ingress.max_inflight_bytes = 1u;
    check_not_null(flow);
    check_equal(turbo_flow_configure_async_ingress(flow, &ingress), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    flow_test_operation_t operation_inspect_3 =
        flow_test_operation_init("test.inspect", graph_probe_stage, &probe);
    operation_inspect_3.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    operation_inspect_3.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &operation_inspect_3), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    message.payload = (uint8_t *)payload;
    message.payload_capacity = sizeof(payload);
    message.payload_size = sizeof(payload);
    message.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    message.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
    message.metadata.message_type = 1u;
    request.delivery_id = 2u;
    request.session_id = 1u;
    request.session_generation = 1u;
    request.message = &message;
    sink.flow = flow;
    sink.source_name = "protocol_in";
    sink.source_handoff = TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED;
    sink.completion = graph_completion_record;
    sink.completion_ctx = &completion;

    check_equal(turbo_flow_protocol_graph_publish(&sink, &request, &disposition),
                SALTS_ENOSPC);
    check_equal(disposition, (turbo_flow_protocol_publish_disposition_t)0);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), 0);
    check_equal(probe.calls, 0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_msg_cleanup(&probe.retained);
    turbo_flow_destroy(flow);
  }
}
