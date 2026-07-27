#include "turbo_flow_gateway.h"
#include "turbo_flow_gateway_plugin.h"

#include "turbo_error.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOW_GATEWAY_STATE_OPEN = 1,
  FLOW_GATEWAY_STATE_CLOSED
};

struct turbo_flow_gateway_s {
  atomic_int state;
  turbo_flow_gateway_protocol_t protocol;
  turbo_flow_gateway_capabilities_t capabilities;
  size_t max_frame_size;
  char gateway_name[TURBO_FLOW_GATEWAY_OPERATION_MAX + 1u];
  char topic_prefix[TURBO_FLOW_GATEWAY_TOPIC_PREFIX_MAX + 1u];
  char tenant[TURBO_FLOW_GATEWAY_TENANT_MAX + 1u];
  char protocol_version[TURBO_FLOW_GATEWAY_VERSION_MAX + 1u];
  turbo_flow_gateway_codec_ops_t ops;
  void *codec_ctx;
};

typedef struct flow_gateway_topic_s {
  char device_id[TURBO_FLOW_GATEWAY_DEVICE_ID_MAX + 1u];
  char operation[TURBO_FLOW_GATEWAY_OPERATION_MAX + 1u];
} flow_gateway_topic_t;

static int flow_gateway_protocol_valid(turbo_flow_gateway_protocol_t protocol) {
  return protocol >= TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN &&
         protocol <= TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808;
}

static const char *
flow_gateway_protocol_name(turbo_flow_gateway_protocol_t protocol) {
  switch (protocol) {
  case TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN:
    return "mqtt-sn";
  case TURBO_FLOW_GATEWAY_PROTOCOL_COAP:
    return "coap";
  case TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M:
    return "lwm2m";
  case TURBO_FLOW_GATEWAY_PROTOCOL_OCPP:
    return "ocpp";
  case TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960:
    return "gbt32960";
  case TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808:
    return "jtt808";
  default:
    return NULL;
  }
}

static int flow_gateway_text_copy(char *out, size_t capacity,
                                  const char *text) {
  size_t length;
  if (!out || capacity == 0u || !text) return TURBO_EINVAL;
  for (length = 0u; length < capacity && text[length] != '\0'; ++length) {
  }
  if (length == 0u) return TURBO_EINVAL;
  if (length >= capacity) return TURBO_EMSGSIZE;
  memcpy(out, text, length + 1u);
  return TURBO_OK;
}

static int flow_gateway_segment_valid(const char *text, size_t length) {
  if (!text || length == 0u) return 0;
  for (size_t i = 0u; i < length; ++i) {
    const unsigned char c = (unsigned char)text[i];
    if (c <= 0x20u || c >= 0x7fu || c == '/' || c == '+' || c == '#') return 0;
  }
  return 1;
}

static int flow_gateway_cstr_segment_valid(const char *text, size_t maximum) {
  size_t length;
  if (!text) return 0;
  for (length = 0u; length <= maximum && text[length] != '\0'; ++length) {
  }
  return length <= maximum && flow_gateway_segment_valid(text, length);
}

static int flow_gateway_frame_validate(const turbo_flow_gateway_t *gateway,
                                       const turbo_flow_gateway_frame_view_t *frame) {
  if (!gateway || !frame || frame->size < sizeof(*frame) ||
      frame->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION || !frame->data ||
      frame->data_size == 0u)
    return TURBO_EINVAL;
  if (frame->data_size > gateway->max_frame_size) return TURBO_EMSGSIZE;
  if (frame->protocol_version && frame->protocol_version[0] != '\0' &&
      strcmp(frame->protocol_version, gateway->protocol_version) != 0)
    return TURBO_ENOTSUP;
  if (frame->device_id && frame->device_id[0] != '\0' &&
      !flow_gateway_cstr_segment_valid(frame->device_id,
                                       TURBO_FLOW_GATEWAY_DEVICE_ID_MAX))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_gateway_inspect(turbo_flow_gateway_t *gateway,
                                const turbo_flow_gateway_frame_view_t *frame,
                                turbo_flow_gateway_direction_t direction,
                                turbo_flow_gateway_metadata_t *metadata) {
  turbo_flow_gateway_metadata_t inspected = TURBO_FLOW_GATEWAY_METADATA_INIT;
  int rc = flow_gateway_frame_validate(gateway, frame);
  if (rc != TURBO_OK) return rc;
  if (atomic_load_explicit(&gateway->state, memory_order_acquire) !=
      FLOW_GATEWAY_STATE_OPEN)
    return TURBO_EBUSY;
  rc = gateway->ops.inspect(gateway->codec_ctx, gateway->protocol_version,
                            frame, &inspected);
  if (rc != TURBO_OK) return rc;
  if (inspected.size < sizeof(inspected) ||
      inspected.abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      !flow_gateway_cstr_segment_valid(inspected.operation,
                                       TURBO_FLOW_GATEWAY_OPERATION_MAX))
    return TURBO_EPROTO;
  if (inspected.device_id[0] == '\0') {
    if (!frame->device_id || frame->device_id[0] == '\0') return TURBO_EPROTO;
    rc = flow_gateway_text_copy(inspected.device_id,
                                sizeof(inspected.device_id), frame->device_id);
    if (rc != TURBO_OK) return rc;
  } else if (frame->device_id && frame->device_id[0] != '\0' &&
             strcmp(inspected.device_id, frame->device_id) != 0) {
    return TURBO_EPROTO;
  }
  if (!flow_gateway_cstr_segment_valid(inspected.device_id,
                                       TURBO_FLOW_GATEWAY_DEVICE_ID_MAX))
    return TURBO_EPROTO;
  inspected.protocol = gateway->protocol;
  inspected.direction = direction;
  rc = flow_gateway_text_copy(inspected.protocol_version,
                              sizeof(inspected.protocol_version),
                              gateway->protocol_version);
  if (rc != TURBO_OK) return rc;
  *metadata = inspected;
  return TURBO_OK;
}

static size_t flow_gateway_topic_size(const turbo_flow_gateway_t *gateway,
                                      const turbo_flow_gateway_metadata_t *metadata) {
  const char *protocol = flow_gateway_protocol_name(gateway->protocol);
  const char *direction =
      metadata->direction == TURBO_FLOW_GATEWAY_DIRECTION_UP ? "up" : "down";
  return strlen(gateway->topic_prefix) + 1u + strlen(protocol) + 1u +
         strlen(gateway->tenant) + 1u + strlen(metadata->device_id) + 1u +
         strlen(direction) + 1u + strlen(metadata->operation);
}

static void flow_gateway_topic_write(
    const turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_metadata_t *metadata, char *out) {
  const char *parts[6];
  parts[0] = gateway->topic_prefix;
  parts[1] = flow_gateway_protocol_name(gateway->protocol);
  parts[2] = gateway->tenant;
  parts[3] = metadata->device_id;
  parts[4] =
      metadata->direction == TURBO_FLOW_GATEWAY_DIRECTION_UP ? "up" : "down";
  parts[5] = metadata->operation;
  for (size_t i = 0u; i < 6u; ++i) {
    const size_t length = strlen(parts[i]);
    memcpy(out, parts[i], length);
    out += length;
    if (i + 1u < 6u) *out++ = '/';
  }
  *out = '\0';
}

int turbo_flow_gateway_create(
    const turbo_flow_gateway_open_request_t *request, const char *gateway_name,
    const char *default_protocol_version,
    turbo_flow_gateway_capabilities_t capabilities,
    const turbo_flow_gateway_codec_ops_t *ops, void *ctx,
    turbo_flow_gateway_t **out) {
  static const turbo_flow_gateway_capabilities_t required =
      TURBO_FLOW_GATEWAY_CAP_INGRESS | TURBO_FLOW_GATEWAY_CAP_EGRESS |
      TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE;
  turbo_flow_gateway_t *gateway;
  const char *version;
  int rc;
  if (out) *out = NULL;
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      !flow_gateway_protocol_valid(request->protocol) || !gateway_name ||
      !default_protocol_version || !request->topic_prefix || !request->tenant ||
      request->max_frame_size == 0u || (capabilities & required) != required ||
      !ops || ops->size < offsetof(turbo_flow_gateway_codec_ops_t, reply) ||
      ops->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION || !ops->inspect ||
      !out)
    return TURBO_EINVAL;
  if (!flow_gateway_cstr_segment_valid(request->topic_prefix,
                                       TURBO_FLOW_GATEWAY_TOPIC_PREFIX_MAX) ||
      !flow_gateway_cstr_segment_valid(request->tenant,
                                       TURBO_FLOW_GATEWAY_TENANT_MAX) ||
      !flow_gateway_cstr_segment_valid(gateway_name,
                                       TURBO_FLOW_GATEWAY_OPERATION_MAX))
    return TURBO_EINVAL;
  version = request->protocol_version && request->protocol_version[0] != '\0'
                ? request->protocol_version
                : default_protocol_version;
  gateway = (turbo_flow_gateway_t *)calloc(1u, sizeof(*gateway));
  if (!gateway) return TURBO_ENOMEM;
  gateway->protocol = request->protocol;
  gateway->capabilities = capabilities;
  gateway->max_frame_size = request->max_frame_size;
  memset(&gateway->ops, 0, sizeof(gateway->ops));
  memcpy(&gateway->ops, ops,
         ops->size < sizeof(gateway->ops) ? ops->size : sizeof(gateway->ops));
  gateway->codec_ctx = ctx;
  rc = flow_gateway_text_copy(gateway->gateway_name,
                              sizeof(gateway->gateway_name), gateway_name);
  if (rc == TURBO_OK)
    rc = flow_gateway_text_copy(gateway->topic_prefix,
                                sizeof(gateway->topic_prefix),
                                request->topic_prefix);
  if (rc == TURBO_OK)
    rc = flow_gateway_text_copy(gateway->tenant, sizeof(gateway->tenant),
                                request->tenant);
  if (rc == TURBO_OK)
    rc = flow_gateway_text_copy(gateway->protocol_version,
                                sizeof(gateway->protocol_version), version);
  if (rc != TURBO_OK) {
    free(gateway);
    return rc;
  }
  atomic_init(&gateway->state, FLOW_GATEWAY_STATE_OPEN);
  *out = gateway;
  return TURBO_OK;
}

void turbo_flow_gateway_destroy(turbo_flow_gateway_t *gateway) {
  if (!gateway) return;
  atomic_store_explicit(&gateway->state, FLOW_GATEWAY_STATE_CLOSED,
                        memory_order_release);
  free(gateway);
}

int turbo_flow_gateway_ingress(turbo_flow_gateway_t *gateway,
                               const turbo_flow_gateway_frame_view_t *frame,
                               turbo_flow_gateway_mqtt_output_t *output) {
  turbo_flow_gateway_metadata_t metadata = TURBO_FLOW_GATEWAY_METADATA_INIT;
  size_t topic_size;
  int rc;
  if (!output || output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION || !output->topic ||
      output->topic_capacity == 0u || !output->payload)
    return TURBO_EINVAL;
  output->topic_size = 0u;
  output->payload_size = 0u;
  output->metadata = (turbo_flow_gateway_metadata_t)
      TURBO_FLOW_GATEWAY_METADATA_INIT;
  rc = flow_gateway_inspect(gateway, frame, TURBO_FLOW_GATEWAY_DIRECTION_UP,
                            &metadata);
  if (rc != TURBO_OK) return rc;
  topic_size = flow_gateway_topic_size(gateway, &metadata);
  if (topic_size >= output->topic_capacity ||
      frame->data_size > output->payload_capacity)
    return TURBO_EMSGSIZE;
  flow_gateway_topic_write(gateway, &metadata, output->topic);
  memmove(output->payload, frame->data, frame->data_size);
  output->topic_size = topic_size;
  output->payload_size = frame->data_size;
  output->qos = 1u;
  output->retain = 0u;
  output->metadata = metadata;
  return TURBO_OK;
}

static int flow_gateway_topic_segment_copy(char *out, size_t capacity,
                                           const char *text, size_t length) {
  if (!out || !text || length == 0u) return TURBO_EPROTO;
  if (length >= capacity) return TURBO_EMSGSIZE;
  if (!flow_gateway_segment_valid(text, length)) return TURBO_EPROTO;
  memcpy(out, text, length);
  out[length] = '\0';
  return TURBO_OK;
}

static int flow_gateway_topic_parse(const turbo_flow_gateway_t *gateway,
                                    const turbo_flow_gateway_mqtt_view_t *message,
                                    flow_gateway_topic_t *parsed) {
  const char *segments[6];
  size_t lengths[6] = {0u};
  size_t segment = 0u;
  size_t start = 0u;
  const char *protocol = flow_gateway_protocol_name(gateway->protocol);
  if (!message->topic || message->topic_size == 0u || !protocol) return TURBO_EINVAL;
  for (size_t i = 0u; i <= message->topic_size; ++i) {
    if (i != message->topic_size && message->topic[i] != '/') continue;
    if (segment >= 6u || i == start) return TURBO_EPROTO;
    segments[segment] = message->topic + start;
    lengths[segment] = i - start;
    segment++;
    start = i + 1u;
  }
  if (segment != 6u ||
      lengths[0] != strlen(gateway->topic_prefix) ||
      memcmp(segments[0], gateway->topic_prefix, lengths[0]) != 0 ||
      lengths[1] != strlen(protocol) ||
      memcmp(segments[1], protocol, lengths[1]) != 0 ||
      lengths[2] != strlen(gateway->tenant) ||
      memcmp(segments[2], gateway->tenant, lengths[2]) != 0 ||
      lengths[4] != 4u || memcmp(segments[4], "down", 4u) != 0)
    return TURBO_EPROTO;
  if (flow_gateway_topic_segment_copy(parsed->device_id,
                                      sizeof(parsed->device_id), segments[3],
                                      lengths[3]) != TURBO_OK)
    return lengths[3] >= sizeof(parsed->device_id) ? TURBO_EMSGSIZE
                                                   : TURBO_EPROTO;
  return flow_gateway_topic_segment_copy(
      parsed->operation, sizeof(parsed->operation), segments[5], lengths[5]);
}

int turbo_flow_gateway_egress(turbo_flow_gateway_t *gateway,
                              const turbo_flow_gateway_mqtt_view_t *message,
                              turbo_flow_gateway_frame_output_t *output) {
  turbo_flow_gateway_frame_view_t frame = TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
  turbo_flow_gateway_metadata_t metadata = TURBO_FLOW_GATEWAY_METADATA_INIT;
  flow_gateway_topic_t parsed = {{0}, {0}};
  int rc;
  if (!gateway || !message || message->size < sizeof(*message) ||
      message->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      !message->payload || message->payload_size == 0u || !output ||
      output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION || !output->data)
    return TURBO_EINVAL;
  output->data_size = 0u;
  output->metadata = (turbo_flow_gateway_metadata_t)
      TURBO_FLOW_GATEWAY_METADATA_INIT;
  rc = flow_gateway_topic_parse(gateway, message, &parsed);
  if (rc != TURBO_OK) return rc;
  frame.data = message->payload;
  frame.data_size = message->payload_size;
  frame.device_id = parsed.device_id;
  frame.protocol_version = gateway->protocol_version;
  rc = flow_gateway_inspect(gateway, &frame,
                            TURBO_FLOW_GATEWAY_DIRECTION_DOWN, &metadata);
  if (rc != TURBO_OK) return rc;
  if (strcmp(metadata.operation, parsed.operation) != 0) return TURBO_EPROTO;
  if (message->payload_size > output->capacity) return TURBO_EMSGSIZE;
  memmove(output->data, message->payload, message->payload_size);
  output->data_size = message->payload_size;
  output->metadata = metadata;
  return TURBO_OK;
}

static int flow_gateway_frame_output_validate(
    turbo_flow_gateway_frame_output_t *output) {
  if (!output || output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      !output->data || output->capacity == 0u)
    return TURBO_EINVAL;
  output->data_size = 0u;
  output->metadata = (turbo_flow_gateway_metadata_t)
      TURBO_FLOW_GATEWAY_METADATA_INIT;
  return TURBO_OK;
}

static int flow_gateway_generated_frame_validate(
    turbo_flow_gateway_t *gateway, const char *device_id,
    turbo_flow_gateway_frame_output_t *output) {
  turbo_flow_gateway_frame_view_t generated =
      TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
  turbo_flow_gateway_metadata_t metadata = TURBO_FLOW_GATEWAY_METADATA_INIT;
  int rc;
  if (output->data_size == 0u) return TURBO_OK;
  if (output->data_size > output->capacity ||
      output->data_size > gateway->max_frame_size)
    return TURBO_EMSGSIZE;
  generated.data = output->data;
  generated.data_size = output->data_size;
  generated.device_id = device_id;
  generated.protocol_version = gateway->protocol_version;
  rc = flow_gateway_inspect(gateway, &generated,
                            TURBO_FLOW_GATEWAY_DIRECTION_DOWN, &metadata);
  if (rc != TURBO_OK) {
    output->data_size = 0u;
    return rc;
  }
  output->metadata = metadata;
  return TURBO_OK;
}

int turbo_flow_gateway_reply(
    turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_frame_view_t *request, int status,
    turbo_flow_gateway_frame_output_t *output) {
  turbo_flow_gateway_metadata_t request_metadata =
      TURBO_FLOW_GATEWAY_METADATA_INIT;
  int rc = flow_gateway_frame_output_validate(output);
  if (rc != TURBO_OK || !gateway) return TURBO_EINVAL;
  if (!gateway->ops.reply ||
      (gateway->capabilities & TURBO_FLOW_GATEWAY_CAP_PROTOCOL_REPLY) == 0u)
    return TURBO_ENOTSUP;
  rc = flow_gateway_inspect(gateway, request,
                            TURBO_FLOW_GATEWAY_DIRECTION_UP,
                            &request_metadata);
  if (rc != TURBO_OK) return rc;
  rc = gateway->ops.reply(gateway->codec_ctx, gateway->protocol_version,
                          request, status, output);
  if (rc != TURBO_OK) {
    output->data_size = 0u;
    return rc;
  }
  if (output->data_size == 0u) {
    request_metadata.direction = TURBO_FLOW_GATEWAY_DIRECTION_DOWN;
    output->metadata = request_metadata;
    return TURBO_OK;
  }
  return flow_gateway_generated_frame_validate(
      gateway, request_metadata.device_id, output);
}

static int flow_gateway_command_validate(
    const turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_command_view_t *command) {
  if (!gateway || !command || command->size < sizeof(*command) ||
      command->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION ||
      !flow_gateway_cstr_segment_valid(
          command->device_id, TURBO_FLOW_GATEWAY_DEVICE_ID_MAX) ||
      !flow_gateway_cstr_segment_valid(
          command->operation, TURBO_FLOW_GATEWAY_OPERATION_MAX) ||
      (!command->payload && command->payload_size != 0u) ||
      command->payload_size > gateway->max_frame_size)
    return TURBO_EINVAL;
  if (command->resource) {
    size_t resource_size = 0u;
    while (resource_size <= TURBO_FLOW_GATEWAY_RESOURCE_MAX &&
           command->resource[resource_size] != '\0')
      resource_size++;
    if (resource_size > TURBO_FLOW_GATEWAY_RESOURCE_MAX)
      return TURBO_EMSGSIZE;
  }
  if (command->correlation_id &&
      command->correlation_id[0] != '\0' &&
      !flow_gateway_cstr_segment_valid(
          command->correlation_id, TURBO_FLOW_GATEWAY_CORRELATION_MAX))
    return TURBO_EINVAL;
  return TURBO_OK;
}

int turbo_flow_gateway_encode(
    turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_command_view_t *command,
    turbo_flow_gateway_frame_output_t *output) {
  int rc = flow_gateway_frame_output_validate(output);
  if (rc != TURBO_OK) return rc;
  rc = flow_gateway_command_validate(gateway, command);
  if (rc != TURBO_OK) return rc;
  if (!gateway->ops.encode ||
      (gateway->capabilities & TURBO_FLOW_GATEWAY_CAP_COMMAND_ENCODE) == 0u)
    return TURBO_ENOTSUP;
  rc = gateway->ops.encode(gateway->codec_ctx, gateway->protocol_version,
                           command, output);
  if (rc != TURBO_OK) {
    output->data_size = 0u;
    return rc;
  }
  rc = flow_gateway_generated_frame_validate(
      gateway, command->device_id, output);
  if (rc != TURBO_OK) return rc;
  if (strcmp(output->metadata.operation, command->operation) != 0) {
    output->data_size = 0u;
    output->metadata = (turbo_flow_gateway_metadata_t)
        TURBO_FLOW_GATEWAY_METADATA_INIT;
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_gateway_get_info(const turbo_flow_gateway_t *gateway,
                                turbo_flow_gateway_info_t *out) {
  turbo_flow_gateway_info_t info = TURBO_FLOW_GATEWAY_INFO_INIT;
  if (!gateway || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_GATEWAY_ABI_VERSION)
    return TURBO_EINVAL;
  if (atomic_load_explicit(&gateway->state, memory_order_acquire) !=
      FLOW_GATEWAY_STATE_OPEN)
    return TURBO_EBUSY;
  info.protocol = gateway->protocol;
  info.capabilities = gateway->capabilities;
  info.max_frame_size = gateway->max_frame_size;
  memcpy(info.gateway, gateway->gateway_name,
         strlen(gateway->gateway_name) + 1u);
  memcpy(info.protocol_version, gateway->protocol_version,
         strlen(gateway->protocol_version) + 1u);
  *out = info;
  return TURBO_OK;
}
