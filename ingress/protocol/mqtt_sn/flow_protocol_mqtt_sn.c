#include "flow_protocol_plugin_support.h"

#include "turbo_error.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static atomic_uint FLOW_MQTT_SN_NEXT_TOPIC_ID = 1u;

static uint16_t flow_mqtt_sn_next_topic_id(atomic_uint *counter) {
  unsigned current;
  unsigned next;
  if (!counter) return 0u;
  current = atomic_load_explicit(counter, memory_order_relaxed);
  for (;;) {
    next = current >= UINT16_MAX - 1u ? 1u : current + 1u;
    if (atomic_compare_exchange_weak_explicit(
            counter, &current, next, memory_order_relaxed,
            memory_order_relaxed))
      return (uint16_t)next;
  }
}

static int flow_mqtt_sn_header(
    const turbo_flow_protocol_frame_view_t *frame, size_t *type_offset,
    uint8_t *type) {
  size_t declared_size;
  if (!frame || !frame->data || !type_offset || !type ||
      frame->data_size < 2u)
    return TURBO_EPROTO;
  if (frame->data[0] == 1u) {
    if (frame->data_size < 4u) return TURBO_EPROTO;
    declared_size = ((size_t)frame->data[1] << 8u) | frame->data[2];
    *type_offset = 3u;
    if (declared_size < 256u) return TURBO_EPROTO;
  } else {
    declared_size = frame->data[0];
    *type_offset = 1u;
  }
  if (declared_size != frame->data_size ||
      *type_offset >= frame->data_size)
    return TURBO_EPROTO;
  *type = frame->data[*type_offset];
  return TURBO_OK;
}

static int flow_mqtt_sn_inspect(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata) {
  size_t type_offset;
  uint8_t type;
  const char *operation = NULL;
  (void)ctx;
  (void)configured_version;
  if (!metadata ||
      flow_mqtt_sn_header(frame, &type_offset, &type) != TURBO_OK)
    return TURBO_EPROTO;
  switch (type) {
  case 0x04u:
    operation = "connect";
    break;
  case 0x05u:
    operation = "connack";
    break;
  case 0x06u:
    operation = "will-topic-request";
    break;
  case 0x07u:
    operation = "will-topic";
    break;
  case 0x08u:
    operation = "will-message-request";
    break;
  case 0x09u:
    operation = "will-message";
    break;
  case 0x0au:
    operation = "register";
    if (frame->data_size < type_offset + 5u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 3u] << 8u) |
        frame->data[type_offset + 4u];
    break;
  case 0x0bu:
    operation = "register-ack";
    if (frame->data_size != type_offset + 6u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 3u] << 8u) |
        frame->data[type_offset + 4u];
    break;
  case 0x0cu:
    operation = "publish";
    if (frame->data_size < type_offset + 6u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 4u] << 8u) |
        frame->data[type_offset + 5u];
    break;
  case 0x0du:
    operation = "puback";
    if (frame->data_size != type_offset + 6u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 3u] << 8u) |
        frame->data[type_offset + 4u];
    break;
  case 0x0eu:
    operation = "pubcomp";
    if (frame->data_size != type_offset + 3u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 1u] << 8u) |
        frame->data[type_offset + 2u];
    break;
  case 0x0fu:
    operation = "pubrec";
    if (frame->data_size != type_offset + 3u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 1u] << 8u) |
        frame->data[type_offset + 2u];
    break;
  case 0x10u:
    operation = "pubrel";
    if (frame->data_size != type_offset + 3u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 1u] << 8u) |
        frame->data[type_offset + 2u];
    break;
  case 0x12u:
    operation = "subscribe";
    if (frame->data_size < type_offset + 5u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 2u] << 8u) |
        frame->data[type_offset + 3u];
    break;
  case 0x13u:
    operation = "suback";
    if (frame->data_size != type_offset + 7u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 4u] << 8u) |
        frame->data[type_offset + 5u];
    break;
  case 0x14u:
    operation = "unsubscribe";
    if (frame->data_size < type_offset + 5u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 2u] << 8u) |
        frame->data[type_offset + 3u];
    break;
  case 0x15u:
    operation = "unsuback";
    if (frame->data_size != type_offset + 3u) return TURBO_EPROTO;
    metadata->sequence =
        ((uint64_t)frame->data[type_offset + 1u] << 8u) |
        frame->data[type_offset + 2u];
    break;
  case 0x16u:
    operation = "pingreq";
    break;
  case 0x17u:
    operation = "pingresp";
    break;
  case 0x18u:
    operation = "disconnect";
    break;
  case 0x1au:
    operation = "will-topic-update";
    break;
  case 0x1bu:
    operation = "will-topic-response";
    break;
  case 0x1cu:
    operation = "will-message-update";
    break;
  case 0x1du:
    operation = "will-message-response";
    break;
  default:
    break;
  }
  metadata->message_type = type;
  return operation
             ? flow_protocol_metadata_text(metadata->operation,
                                          sizeof(metadata->operation),
                                          operation)
             : flow_protocol_metadata_format(metadata->operation,
                                            sizeof(metadata->operation),
                                            "type-%02x", type);
}

static uint8_t flow_mqtt_sn_return_code(int status) {
  if (status == TURBO_OK) return 0u;
  if (status == TURBO_EBUSY || status == TURBO_ENOSPC ||
      status == TURBO_ENOBUFS)
    return 1u;
  if (status == TURBO_ENOTSUP) return 3u;
  return 2u;
}

static int flow_mqtt_sn_reply(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_output_t *output) {
  size_t type_offset;
  uint8_t type;
  uint8_t return_code = flow_mqtt_sn_return_code(status);
  uint16_t topic_id;
  uint8_t flags;
  (void)configured_version;
  if (!ctx || !output || !output->data ||
      flow_mqtt_sn_header(request, &type_offset, &type) != TURBO_OK)
    return TURBO_EINVAL;
  switch (type) {
  case 0x04u:
    if (request->data_size < type_offset + 6u ||
        request->data[type_offset + 2u] != 1u)
      return TURBO_EPROTO;
    if (output->capacity < 3u) return TURBO_EMSGSIZE;
    output->data[0] = 3u;
    output->data[1] =
        (request->data[type_offset + 1u] & UINT8_C(0x08)) != 0u
            ? UINT8_C(0x06)
            : UINT8_C(0x05);
    output->data[2] = return_code;
    output->data_size =
        output->data[1] == UINT8_C(0x06) ? 2u : 3u;
    output->data[0] = (uint8_t)output->data_size;
    return TURBO_OK;
  case 0x07u:
    if (output->capacity < 2u) return TURBO_EMSGSIZE;
    output->data[0] = 2u;
    output->data[1] = UINT8_C(0x08);
    output->data_size = 2u;
    return TURBO_OK;
  case 0x09u:
    if (output->capacity < 3u) return TURBO_EMSGSIZE;
    output->data[0] = 3u;
    output->data[1] = UINT8_C(0x05);
    output->data[2] = return_code;
    output->data_size = 3u;
    return TURBO_OK;
  case 0x0au:
    if (request->data_size < type_offset + 5u) return TURBO_EPROTO;
    if (output->capacity < 7u) return TURBO_EMSGSIZE;
    topic_id = ((uint16_t)request->data[type_offset + 1u] << 8u) |
               request->data[type_offset + 2u];
    if (topic_id == 0u && return_code == 0u) {
      topic_id = flow_mqtt_sn_next_topic_id((atomic_uint *)ctx);
    }
    output->data[0] = 7u;
    output->data[1] = UINT8_C(0x0b);
    output->data[2] = (uint8_t)(topic_id >> 8u);
    output->data[3] = (uint8_t)topic_id;
    output->data[4] = request->data[type_offset + 3u];
    output->data[5] = request->data[type_offset + 4u];
    output->data[6] = return_code;
    output->data_size = 7u;
    return TURBO_OK;
  case 0x0cu:
    if (request->data_size < type_offset + 6u) return TURBO_EPROTO;
    flags = request->data[type_offset + 1u];
    if (((flags >> 5u) & 0x03u) == 0u && return_code == 0u) {
      output->data_size = 0u;
      return TURBO_OK;
    }
    if (((flags >> 5u) & 0x03u) == 2u && return_code == 0u) {
      if (output->capacity < 4u) return TURBO_EMSGSIZE;
      output->data[0] = 4u;
      output->data[1] = UINT8_C(0x0f);
      output->data[2] = request->data[type_offset + 4u];
      output->data[3] = request->data[type_offset + 5u];
      output->data_size = 4u;
      return TURBO_OK;
    }
    if (output->capacity < 7u) return TURBO_EMSGSIZE;
    output->data[0] = 7u;
    output->data[1] = UINT8_C(0x0d);
    memcpy(output->data + 2u, request->data + type_offset + 2u, 4u);
    output->data[6] = return_code;
    output->data_size = 7u;
    return TURBO_OK;
  case 0x10u:
    if (request->data_size != type_offset + 3u) return TURBO_EPROTO;
    if (output->capacity < 4u) return TURBO_EMSGSIZE;
    output->data[0] = 4u;
    output->data[1] = UINT8_C(0x0e);
    output->data[2] = request->data[type_offset + 1u];
    output->data[3] = request->data[type_offset + 2u];
    output->data_size = 4u;
    return TURBO_OK;
  case 0x12u:
    if (request->data_size < type_offset + 5u) return TURBO_EPROTO;
    if (output->capacity < 8u) return TURBO_EMSGSIZE;
    flags = request->data[type_offset + 1u];
    topic_id = flow_mqtt_sn_next_topic_id((atomic_uint *)ctx);
    if ((flags & 0x03u) != 0u && request->data_size >= type_offset + 6u)
      topic_id = ((uint16_t)request->data[type_offset + 4u] << 8u) |
                 request->data[type_offset + 5u];
    if (topic_id == 0u || topic_id == UINT16_MAX) return TURBO_ERANGE;
    output->data[0] = 8u;
    output->data[1] = UINT8_C(0x13);
    output->data[2] = flags & UINT8_C(0x60);
    output->data[3] = (uint8_t)(topic_id >> 8u);
    output->data[4] = (uint8_t)topic_id;
    output->data[5] = request->data[type_offset + 2u];
    output->data[6] = request->data[type_offset + 3u];
    output->data[7] = return_code;
    output->data_size = 8u;
    return TURBO_OK;
  case 0x14u:
    if (request->data_size < type_offset + 5u) return TURBO_EPROTO;
    if (output->capacity < 4u) return TURBO_EMSGSIZE;
    output->data[0] = 4u;
    output->data[1] = UINT8_C(0x15);
    output->data[2] = request->data[type_offset + 2u];
    output->data[3] = request->data[type_offset + 3u];
    output->data_size = 4u;
    return TURBO_OK;
  case 0x16u:
    if (output->capacity < 2u) return TURBO_EMSGSIZE;
    output->data[0] = 2u;
    output->data[1] = UINT8_C(0x17);
    output->data_size = 2u;
    return TURBO_OK;
  case 0x18u:
    if (output->capacity < 2u) return TURBO_EMSGSIZE;
    output->data[0] = 2u;
    output->data[1] = UINT8_C(0x18);
    output->data_size = 2u;
    return TURBO_OK;
  case 0x1au:
  case 0x1cu:
    if (output->capacity < 3u) return TURBO_EMSGSIZE;
    output->data[0] = 3u;
    output->data[1] =
        type == UINT8_C(0x1a) ? UINT8_C(0x1b) : UINT8_C(0x1d);
    output->data[2] = return_code;
    output->data_size = 3u;
    return TURBO_OK;
  default:
    output->data_size = 0u;
    return TURBO_OK;
  }
}

static int flow_mqtt_sn_topic_id(const char *text, uint16_t *out) {
  char *end = NULL;
  unsigned long value;
  if (!text || !text[0] || !out) return TURBO_EINVAL;
  value = strtoul(text, &end, 10);
  if (!end || *end != '\0' || value == 0ul || value >= UINT16_MAX)
    return TURBO_EINVAL;
  *out = (uint16_t)value;
  return TURBO_OK;
}

static int flow_mqtt_sn_encode(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_command_view_t *command,
    turbo_flow_protocol_frame_output_t *output) {
  uint16_t topic_id;
  uint16_t message_id;
  size_t frame_size;
  (void)ctx;
  (void)configured_version;
  if (!command || !output || !output->data) return TURBO_EINVAL;
  if (strcmp(command->operation, "publish") == 0) {
    if (flow_mqtt_sn_topic_id(command->resource, &topic_id) != TURBO_OK ||
        command->sequence == 0u || command->sequence > UINT16_MAX)
      return TURBO_EINVAL;
    message_id = (uint16_t)command->sequence;
    frame_size = 7u + command->payload_size;
    if (frame_size > UINT8_MAX || frame_size > output->capacity)
      return TURBO_EMSGSIZE;
    output->data[0] = (uint8_t)frame_size;
    output->data[1] = UINT8_C(0x0c);
    output->data[2] = UINT8_C(0x20); /* QoS 1, normal topic id */
    output->data[3] = (uint8_t)(topic_id >> 8u);
    output->data[4] = (uint8_t)topic_id;
    output->data[5] = (uint8_t)(message_id >> 8u);
    output->data[6] = (uint8_t)message_id;
    if (command->payload_size > 0u)
      memcpy(output->data + 7u, command->payload, command->payload_size);
    output->data_size = frame_size;
    return TURBO_OK;
  }
  if (strcmp(command->operation, "pingreq") == 0 ||
      strcmp(command->operation, "disconnect") == 0) {
    if (output->capacity < 2u) return TURBO_EMSGSIZE;
    output->data[0] = 2u;
    output->data[1] =
        strcmp(command->operation, "pingreq") == 0 ? UINT8_C(0x16)
                                                    : UINT8_C(0x18);
    output->data_size = 2u;
    return TURBO_OK;
  }
  return TURBO_ENOTSUP;
}

static const char *const FLOW_MQTT_SN_VERSIONS[] = {"1.2"};
static const flow_protocol_plugin_descriptor_t FLOW_MQTT_SN_DESCRIPTOR = {
    "mqtt-sn", TURBO_FLOW_PROTOCOL_MQTT_SN, "1.2",
    FLOW_MQTT_SN_VERSIONS,
    sizeof(FLOW_MQTT_SN_VERSIONS) / sizeof(FLOW_MQTT_SN_VERSIONS[0]),
    flow_mqtt_sn_inspect, flow_mqtt_sn_reply, flow_mqtt_sn_encode,
    &FLOW_MQTT_SN_NEXT_TOPIC_ID};

static const turbo_flow_protocol_plugin_api_t FLOW_MQTT_SN_API = {
    sizeof(turbo_flow_protocol_plugin_api_t),
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
    "mqtt-sn",
    TURBO_FLOW_PROTOCOL_MQTT_SN,
    TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
        TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE |
        TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE,
    (void *)&FLOW_MQTT_SN_DESCRIPTOR,
    flow_protocol_plugin_open,
    flow_protocol_plugin_close};

const turbo_flow_protocol_plugin_api_t *
turbo_flow_protocol_plugin_get_api(void) {
  return &FLOW_MQTT_SN_API;
}
