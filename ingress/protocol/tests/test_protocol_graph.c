#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_protocol_graph.h"

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

static int graph_probe_stage(turbo_flow_msg_t *message, void *ctx) {
  graph_probe_t *probe = (graph_probe_t *)ctx;
  const turbo_flow_protocol_metadata_t *metadata =
      turbo_flow_protocol_graph_metadata(message);
  if (!probe || !message || !metadata ||
      message->payload.len > sizeof(probe->payload))
    return TURBO_EPROTO;
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
                             "stage inspect\n"
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
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_register_stage_ex(flow, "inspect", graph_probe_stage,
                                              &probe, NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

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

    check_int_eq(turbo_flow_protocol_graph_publish(&sink, &request,
                                                  &disposition),
                 TURBO_OK);
    check_int_eq(disposition, TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED);
    check_size_eq(probe.calls, 1u);
    check_uint_eq(probe.message_id, 42u);
    check_uint_eq(probe.message_type, 17u);
    check_str_eq(probe.operation, "Heartbeat");
    check_size_eq(probe.payload_size, sizeof(payload));
    check_mem_eq(probe.payload, payload, sizeof(payload));
    retained_metadata = turbo_flow_protocol_graph_metadata(&probe.retained);
    check_not_null(retained_metadata);
    check_str_eq(retained_metadata->operation, "Heartbeat");
    check_size_eq(probe.retained.payload.len, sizeof(payload));
    check_mem_eq(probe.retained.payload.data, payload, sizeof(payload));

    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_msg_cleanup(&probe.retained);
    turbo_flow_destroy(flow);
  }
}
