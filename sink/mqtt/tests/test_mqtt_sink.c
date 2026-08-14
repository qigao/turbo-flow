#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_mqtt_sink.h"

#include <string.h>

spec("mqtt sink") {
  it("maps a caller-owned batch without owning an I/O connection") {
    static const uint8_t payloads[][3] = {{1u, 2u, 3u}, {4u, 5u, 6u}};
    char topics[2][128];
    turbo_flow_mqtt_sink_config_t config = TURBO_FLOW_MQTT_SINK_CONFIG_INIT;
    turbo_flow_protocol_message_output_t inputs[2];
    turbo_flow_mqtt_sink_message_t outputs[2];
    turbo_flow_mqtt_sink_batch_t batch = TURBO_FLOW_MQTT_SINK_BATCH_INIT;
    size_t mapped = 0u;

    for (size_t i = 0u; i < 2u; ++i) {
      inputs[i] = (turbo_flow_protocol_message_output_t)TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
      outputs[i] = (turbo_flow_mqtt_sink_message_t)TURBO_FLOW_MQTT_SINK_MESSAGE_INIT;
      inputs[i].payload = (uint8_t *)payloads[i];
      inputs[i].payload_capacity = sizeof(payloads[i]);
      inputs[i].payload_size = sizeof(payloads[i]);
      inputs[i].metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
      inputs[i].metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
      memcpy(inputs[i].metadata.device_id, i == 0u ? "charger-1" : "charger-2",
             sizeof("charger-1"));
      memcpy(inputs[i].metadata.operation, "Heartbeat", sizeof("Heartbeat"));
      outputs[i].topic = topics[i];
      outputs[i].topic_capacity = sizeof(topics[i]);
    }
    batch.inputs = inputs;
    batch.outputs = outputs;
    batch.message_count = 2u;

    check_int_eq(turbo_flow_mqtt_sink_map_batch(&config, &batch, &mapped), TURBO_OK);
    check_size_eq(mapped, 2u);
    check_str_eq(outputs[0].topic, "ingress/ocpp/default/charger-1/up/Heartbeat");
    check_str_eq(outputs[1].topic, "ingress/ocpp/default/charger-2/up/Heartbeat");
    check_ptr_eq(outputs[0].payload, payloads[0]);
    check_ptr_eq(outputs[1].payload, payloads[1]);
    check_size_eq(outputs[0].payload_size, sizeof(payloads[0]));
    check_int_eq(outputs[0].qos, 1u);
    check_int_eq(outputs[0].retain, 0u);
  }

  it("reports only the successfully mapped prefix") {
    static const uint8_t payloads[][1] = {{1u}, {2u}, {3u}};
    char topics[3][128];
    turbo_flow_mqtt_sink_config_t config = TURBO_FLOW_MQTT_SINK_CONFIG_INIT;
    turbo_flow_protocol_message_output_t inputs[3];
    turbo_flow_mqtt_sink_message_t outputs[3];
    turbo_flow_mqtt_sink_batch_t batch = TURBO_FLOW_MQTT_SINK_BATCH_INIT;
    size_t mapped = SIZE_MAX;

    for (size_t i = 0u; i < 3u; ++i) {
      inputs[i] = (turbo_flow_protocol_message_output_t)TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
      outputs[i] = (turbo_flow_mqtt_sink_message_t)TURBO_FLOW_MQTT_SINK_MESSAGE_INIT;
      inputs[i].payload = (uint8_t *)payloads[i];
      inputs[i].payload_capacity = sizeof(payloads[i]);
      inputs[i].payload_size = sizeof(payloads[i]);
      inputs[i].metadata.protocol = TURBO_FLOW_PROTOCOL_COAP;
      inputs[i].metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_DOWN;
      memcpy(inputs[i].metadata.device_id, "sensor-1", sizeof("sensor-1"));
      memcpy(inputs[i].metadata.operation, "update", sizeof("update"));
      outputs[i].topic = topics[i];
      outputs[i].topic_capacity = sizeof(topics[i]);
    }
    memcpy(inputs[1].metadata.operation, "invalid/+", sizeof("invalid/+"));
    batch.inputs = inputs;
    batch.outputs = outputs;
    batch.message_count = 3u;

    check_int_eq(turbo_flow_mqtt_sink_map_batch(&config, &batch, &mapped), TURBO_EPROTO);
    check_size_eq(mapped, 1u);
    check_str_eq(outputs[0].topic, "ingress/coap/default/sensor-1/down/update");
    check_size_eq(outputs[1].topic_size, 0u);
    check_null(outputs[1].payload);
    check_size_eq(outputs[2].topic_size, 0u);
    check_null(outputs[2].payload);
  }

  it("rejects a batch over its configured bound before mapping") {
    static const uint8_t payload[] = {1u};
    char topics[2][128];
    turbo_flow_mqtt_sink_config_t config = TURBO_FLOW_MQTT_SINK_CONFIG_INIT;
    turbo_flow_protocol_message_output_t inputs[2];
    turbo_flow_mqtt_sink_message_t outputs[2];
    turbo_flow_mqtt_sink_batch_t batch = TURBO_FLOW_MQTT_SINK_BATCH_INIT;
    size_t mapped = SIZE_MAX;

    for (size_t i = 0u; i < 2u; ++i) {
      inputs[i] = (turbo_flow_protocol_message_output_t)TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
      outputs[i] = (turbo_flow_mqtt_sink_message_t)TURBO_FLOW_MQTT_SINK_MESSAGE_INIT;
      inputs[i].payload = (uint8_t *)payload;
      inputs[i].payload_capacity = sizeof(payload);
      inputs[i].payload_size = sizeof(payload);
      inputs[i].metadata.protocol = TURBO_FLOW_PROTOCOL_COAP;
      inputs[i].metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
      memcpy(inputs[i].metadata.device_id, "sensor-1", sizeof("sensor-1"));
      memcpy(inputs[i].metadata.operation, "update", sizeof("update"));
      outputs[i].topic = topics[i];
      outputs[i].topic_capacity = sizeof(topics[i]);
    }
    config.max_batch_size = 1u;
    batch.inputs = inputs;
    batch.outputs = outputs;
    batch.message_count = 2u;

    check_int_eq(turbo_flow_mqtt_sink_map_batch(&config, &batch, &mapped), TURBO_EMSGSIZE);
    check_size_eq(mapped, 0u);
    check_size_eq(outputs[0].topic_size, 0u);
    check_null(outputs[0].payload);
  }

  it("keeps the original config ABI on the scalar compatibility path") {
    static const uint8_t payload[] = {9u};
    char topic[128];
    turbo_flow_mqtt_sink_config_t config = TURBO_FLOW_MQTT_SINK_CONFIG_INIT;
    turbo_flow_protocol_message_output_t input = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
    turbo_flow_mqtt_sink_message_t output = TURBO_FLOW_MQTT_SINK_MESSAGE_INIT;

    config.size = TURBO_FLOW_MQTT_SINK_CONFIG_V1_SIZE;
    config.max_batch_size = TURBO_FLOW_MQTT_SINK_MAX_BATCH_SIZE + 1u;
    input.payload = (uint8_t *)payload;
    input.payload_capacity = sizeof(payload);
    input.payload_size = sizeof(payload);
    input.metadata.protocol = TURBO_FLOW_PROTOCOL_OCPP;
    input.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
    memcpy(input.metadata.device_id, "charger-1", sizeof("charger-1"));
    memcpy(input.metadata.operation, "Heartbeat", sizeof("Heartbeat"));
    output.topic = topic;
    output.topic_capacity = sizeof(topic);

    check_int_eq(turbo_flow_mqtt_sink_map(&config, &input, &output), TURBO_OK);
    check_str_eq(output.topic, "ingress/ocpp/default/charger-1/up/Heartbeat");
    check_ptr_eq(output.payload, payload);
  }
}
