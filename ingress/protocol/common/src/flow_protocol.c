#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"

#include "salts_error.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOW_PROTOCOL_STATE_OPEN = 1,
  FLOW_PROTOCOL_STATE_CLOSED
};

struct turbo_flow_protocol_s {
  atomic_int state;
  turbo_flow_protocol_kind_t protocol;
  turbo_flow_protocol_capabilities_t capabilities;
  size_t max_frame_size;
  char name[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
  turbo_flow_protocol_codec_ops_t ops;
  void *codec_ctx;
};

static int flow_protocol_kind_valid(turbo_flow_protocol_kind_t protocol) {
  return protocol >= TURBO_FLOW_PROTOCOL_MQTT_SN &&
         protocol <= TURBO_FLOW_PROTOCOL_JTT_808;
}

const char *turbo_flow_protocol_kind_name(turbo_flow_protocol_kind_t protocol) {
  switch (protocol) {
  case TURBO_FLOW_PROTOCOL_MQTT_SN:
    return "mqtt-sn";
  case TURBO_FLOW_PROTOCOL_COAP:
    return "coap";
  case TURBO_FLOW_PROTOCOL_LWM2M:
    return "lwm2m";
  case TURBO_FLOW_PROTOCOL_OCPP:
    return "ocpp";
  case TURBO_FLOW_PROTOCOL_GBT_32960:
    return "gbt32960";
  case TURBO_FLOW_PROTOCOL_JTT_808:
    return "jtt808";
  default:
    return NULL;
  }
}

static int flow_protocol_text_copy(char *out, size_t capacity,
                                  const char *text) {
  size_t length;
  if (!out || capacity == 0u || !text) return SALTS_EINVAL;
  for (length = 0u; length < capacity && text[length] != '\0'; ++length) {
  }
  if (length == 0u) return SALTS_EINVAL;
  if (length >= capacity) return SALTS_EMSGSIZE;
  memcpy(out, text, length + 1u);
  return SALTS_OK;
}

static int flow_protocol_segment_valid(const char *text, size_t length) {
  if (!text || length == 0u) return 0;
  for (size_t i = 0u; i < length; ++i) {
    const unsigned char c = (unsigned char)text[i];
    if (c <= 0x20u || c >= 0x7fu || c == '/' || c == '+' || c == '#') return 0;
  }
  return 1;
}

static int flow_protocol_cstr_segment_valid(const char *text, size_t maximum) {
  size_t length;
  if (!text) return 0;
  for (length = 0u; length <= maximum && text[length] != '\0'; ++length) {
  }
  return length <= maximum && flow_protocol_segment_valid(text, length);
}

static int flow_protocol_frame_validate(const turbo_flow_protocol_t *protocol,
                                       const turbo_flow_protocol_frame_view_t *frame) {
  if (!protocol || !frame || frame->size < sizeof(*frame) ||
      frame->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION || !frame->data ||
      frame->data_size == 0u)
    return SALTS_EINVAL;
  if (frame->data_size > protocol->max_frame_size) return SALTS_EMSGSIZE;
  if (frame->protocol_version && frame->protocol_version[0] != '\0' &&
      strcmp(frame->protocol_version, protocol->protocol_version) != 0)
    return SALTS_ENOTSUP;
  if (frame->device_id && frame->device_id[0] != '\0' &&
      !flow_protocol_cstr_segment_valid(frame->device_id,
                                       TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX))
    return SALTS_EINVAL;
  return SALTS_OK;
}

static int flow_protocol_metadata_finalize(
    const turbo_flow_protocol_t *protocol,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_direction_t direction,
    turbo_flow_protocol_metadata_t *inspected,
    turbo_flow_protocol_metadata_t *metadata) {
  int rc;
  if (!protocol || !frame || !inspected || !metadata ||
      inspected->size < sizeof(*inspected) ||
      inspected->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      !flow_protocol_cstr_segment_valid(inspected->operation,
                                       TURBO_FLOW_PROTOCOL_OPERATION_MAX))
    return SALTS_EPROTO;
  if (inspected->device_id[0] == '\0') {
    if (!frame->device_id || frame->device_id[0] == '\0') return SALTS_EPROTO;
    rc = flow_protocol_text_copy(inspected->device_id,
                                 sizeof(inspected->device_id), frame->device_id);
    if (rc != SALTS_OK) return rc;
  } else if (frame->device_id && frame->device_id[0] != '\0' &&
             strcmp(inspected->device_id, frame->device_id) != 0) {
    return SALTS_EPROTO;
  }
  if (!flow_protocol_cstr_segment_valid(inspected->device_id,
                                        TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX))
    return SALTS_EPROTO;
  inspected->protocol = protocol->protocol;
  inspected->direction = direction;
  rc = flow_protocol_text_copy(inspected->protocol_version,
                               sizeof(inspected->protocol_version),
                               protocol->protocol_version);
  if (rc != SALTS_OK) return rc;
  *metadata = *inspected;
  return SALTS_OK;
}

static int flow_protocol_inspect(turbo_flow_protocol_t *protocol,
                                const turbo_flow_protocol_frame_view_t *frame,
                                turbo_flow_protocol_direction_t direction,
                                turbo_flow_protocol_metadata_t *metadata) {
  turbo_flow_protocol_metadata_t inspected = TURBO_FLOW_PROTOCOL_METADATA_INIT;
  int rc = flow_protocol_frame_validate(protocol, frame);
  if (rc != SALTS_OK) return rc;
  if (atomic_load_explicit(&protocol->state, memory_order_acquire) !=
      FLOW_PROTOCOL_STATE_OPEN)
    return SALTS_EBUSY;
  rc = protocol->ops.inspect(protocol->codec_ctx, protocol->protocol_version,
                             frame, &inspected);
  if (rc != SALTS_OK) return rc;
  return flow_protocol_metadata_finalize(protocol, frame, direction, &inspected,
                                         metadata);
}

int turbo_flow_protocol_create(
    const turbo_flow_protocol_open_request_t *request, const char *name,
    const char *default_protocol_version,
    turbo_flow_protocol_capabilities_t capabilities,
    const turbo_flow_protocol_codec_ops_t *ops, void *ctx,
    turbo_flow_protocol_t **out) {
  static const turbo_flow_protocol_capabilities_t required =
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE;
  turbo_flow_protocol_t *protocol;
  const char *version;
  int rc;
  if (out) *out = NULL;
  if (!request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      !flow_protocol_kind_valid(request->protocol) || !name ||
      !default_protocol_version ||
      request->max_frame_size == 0u || (capabilities & required) != required ||
      !ops || ops->size < offsetof(turbo_flow_protocol_codec_ops_t, reply) ||
      ops->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION || !ops->inspect ||
      !out)
    return SALTS_EINVAL;
  if (!flow_protocol_cstr_segment_valid(name,
                                       TURBO_FLOW_PROTOCOL_OPERATION_MAX))
    return SALTS_EINVAL;
  version = request->protocol_version && request->protocol_version[0] != '\0'
                ? request->protocol_version
                : default_protocol_version;
  protocol = (turbo_flow_protocol_t *)calloc(1u, sizeof(*protocol));
  if (!protocol) return SALTS_ENOMEM;
  protocol->protocol = request->protocol;
  protocol->capabilities = capabilities;
  protocol->max_frame_size = request->max_frame_size;
  memset(&protocol->ops, 0, sizeof(protocol->ops));
  memcpy(&protocol->ops, ops,
         ops->size < sizeof(protocol->ops) ? ops->size : sizeof(protocol->ops));
  protocol->codec_ctx = ctx;
  rc = flow_protocol_text_copy(protocol->name, sizeof(protocol->name), name);
  if (rc == SALTS_OK)
    rc = flow_protocol_text_copy(protocol->protocol_version,
                                sizeof(protocol->protocol_version), version);
  if (rc != SALTS_OK) {
    free(protocol);
    return rc;
  }
  atomic_init(&protocol->state, FLOW_PROTOCOL_STATE_OPEN);
  *out = protocol;
  return SALTS_OK;
}

void turbo_flow_protocol_destroy(turbo_flow_protocol_t *protocol) {
  if (!protocol) return;
  atomic_store_explicit(&protocol->state, FLOW_PROTOCOL_STATE_CLOSED,
                        memory_order_release);
  free(protocol);
}

static int flow_protocol_message_output_reset(
    turbo_flow_protocol_message_output_t *output) {
  if (!output || output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION || !output->payload)
    return SALTS_EINVAL;
  output->payload_size = 0u;
  output->metadata =
      (turbo_flow_protocol_metadata_t)TURBO_FLOW_PROTOCOL_METADATA_INIT;
  return SALTS_OK;
}

static int flow_protocol_semantic_output_reset(
    turbo_flow_protocol_semantic_output_t *output) {
  if (!output || output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      (!output->data && output->capacity != 0u))
    return SALTS_EINVAL;
  output->data_size = 0u;
  output->semantic_type = TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE;
  output->media_type[0] = '\0';
  return SALTS_OK;
}

int turbo_flow_protocol_decode(turbo_flow_protocol_t *protocol,
                              const turbo_flow_protocol_frame_view_t *frame,
                              turbo_flow_protocol_message_output_t *output) {
  turbo_flow_protocol_metadata_t metadata = TURBO_FLOW_PROTOCOL_METADATA_INIT;
  int rc = flow_protocol_message_output_reset(output);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_inspect(protocol, frame, TURBO_FLOW_PROTOCOL_DIRECTION_UP,
                             &metadata);
  if (rc != SALTS_OK) return rc;
  if (frame->data_size > output->payload_capacity) return SALTS_EMSGSIZE;
  memmove(output->payload, frame->data, frame->data_size);
  output->payload_size = frame->data_size;
  output->metadata = metadata;
  return SALTS_OK;
}

int turbo_flow_protocol_decode_semantic(
    turbo_flow_protocol_t *protocol, const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_message_output_t *raw_output,
    turbo_flow_protocol_semantic_output_t *semantic_output) {
  turbo_flow_protocol_metadata_t inspected = TURBO_FLOW_PROTOCOL_METADATA_INIT;
  turbo_flow_protocol_metadata_t metadata = TURBO_FLOW_PROTOCOL_METADATA_INIT;
  turbo_flow_protocol_semantic_output_t semantic;
  uint8_t *semantic_data;
  size_t semantic_capacity;
  int rc = flow_protocol_message_output_reset(raw_output);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_semantic_output_reset(semantic_output);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_frame_validate(protocol, frame);
  if (rc != SALTS_OK) return rc;
  if (atomic_load_explicit(&protocol->state, memory_order_acquire) !=
      FLOW_PROTOCOL_STATE_OPEN)
    return SALTS_EBUSY;
  if (!protocol->ops.decode_semantic) return SALTS_ENOTSUP;
  if (frame->data_size > raw_output->payload_capacity) return SALTS_EMSGSIZE;

  semantic = *semantic_output;
  semantic_data = semantic.data;
  semantic_capacity = semantic.capacity;
  rc = protocol->ops.decode_semantic(protocol->codec_ctx,
                                     protocol->protocol_version, frame,
                                     &inspected, &semantic);
  if (rc != SALTS_OK) return rc;
  if (semantic.data != semantic_data || semantic.capacity != semantic_capacity ||
      semantic.data_size > semantic.capacity ||
      memchr(semantic.media_type, '\0', sizeof(semantic.media_type)) == NULL)
    return SALTS_EPROTO;
  rc = flow_protocol_metadata_finalize(protocol, frame,
                                       TURBO_FLOW_PROTOCOL_DIRECTION_UP,
                                       &inspected, &metadata);
  if (rc != SALTS_OK) return rc;

  memmove(raw_output->payload, frame->data, frame->data_size);
  raw_output->payload_size = frame->data_size;
  raw_output->metadata = metadata;
  *semantic_output = semantic;
  return SALTS_OK;
}

static int flow_protocol_frame_output_validate(
    turbo_flow_protocol_frame_output_t *output) {
  if (!output || output->size < sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      !output->data || output->capacity == 0u)
    return SALTS_EINVAL;
  output->data_size = 0u;
  output->metadata = (turbo_flow_protocol_metadata_t)
      TURBO_FLOW_PROTOCOL_METADATA_INIT;
  return SALTS_OK;
}

static int flow_protocol_generated_frame_validate(
    turbo_flow_protocol_t *protocol, const char *device_id,
    turbo_flow_protocol_frame_output_t *output) {
  turbo_flow_protocol_frame_view_t generated =
      TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  turbo_flow_protocol_metadata_t metadata = TURBO_FLOW_PROTOCOL_METADATA_INIT;
  int rc;
  if (output->data_size == 0u) return SALTS_OK;
  if (output->data_size > output->capacity ||
      output->data_size > protocol->max_frame_size)
    return SALTS_EMSGSIZE;
  generated.data = output->data;
  generated.data_size = output->data_size;
  generated.device_id = device_id;
  generated.protocol_version = protocol->protocol_version;
  rc = flow_protocol_inspect(protocol, &generated,
                            TURBO_FLOW_PROTOCOL_DIRECTION_DOWN, &metadata);
  if (rc != SALTS_OK) {
    output->data_size = 0u;
    return rc;
  }
  output->metadata = metadata;
  return SALTS_OK;
}

int turbo_flow_protocol_reply(
    turbo_flow_protocol_t *protocol,
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_output_t *output) {
  turbo_flow_protocol_metadata_t request_metadata =
      TURBO_FLOW_PROTOCOL_METADATA_INIT;
  int rc = flow_protocol_frame_output_validate(output);
  if (rc != SALTS_OK || !protocol) return SALTS_EINVAL;
  if (!protocol->ops.reply ||
      (protocol->capabilities & TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY) == 0u)
    return SALTS_ENOTSUP;
  rc = flow_protocol_inspect(protocol, request,
                            TURBO_FLOW_PROTOCOL_DIRECTION_UP,
                            &request_metadata);
  if (rc != SALTS_OK) return rc;
  rc = protocol->ops.reply(protocol->codec_ctx, protocol->protocol_version,
                          request, status, output);
  if (rc != SALTS_OK) {
    output->data_size = 0u;
    return rc;
  }
  if (output->data_size == 0u) {
    request_metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_DOWN;
    output->metadata = request_metadata;
    return SALTS_OK;
  }
  return flow_protocol_generated_frame_validate(
      protocol, request_metadata.device_id, output);
}

static int flow_protocol_command_validate(
    const turbo_flow_protocol_t *protocol,
    const turbo_flow_protocol_command_view_t *command) {
  if (!protocol || !command || command->size < sizeof(*command) ||
      command->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      !flow_protocol_cstr_segment_valid(
          command->device_id, TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX) ||
      !flow_protocol_cstr_segment_valid(
          command->operation, TURBO_FLOW_PROTOCOL_OPERATION_MAX) ||
      (!command->payload && command->payload_size != 0u) ||
      command->payload_size > protocol->max_frame_size)
    return SALTS_EINVAL;
  if (command->resource) {
    size_t resource_size = 0u;
    while (resource_size <= TURBO_FLOW_PROTOCOL_RESOURCE_MAX &&
           command->resource[resource_size] != '\0')
      resource_size++;
    if (resource_size > TURBO_FLOW_PROTOCOL_RESOURCE_MAX)
      return SALTS_EMSGSIZE;
  }
  if (command->correlation_id &&
      command->correlation_id[0] != '\0' &&
      !flow_protocol_cstr_segment_valid(
          command->correlation_id, TURBO_FLOW_PROTOCOL_CORRELATION_MAX))
    return SALTS_EINVAL;
  return SALTS_OK;
}

int turbo_flow_protocol_encode(
    turbo_flow_protocol_t *protocol,
    const turbo_flow_protocol_command_view_t *command,
    turbo_flow_protocol_frame_output_t *output) {
  int rc = flow_protocol_frame_output_validate(output);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_command_validate(protocol, command);
  if (rc != SALTS_OK) return rc;
  if (!protocol->ops.encode ||
      (protocol->capabilities & TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE) == 0u)
    return SALTS_ENOTSUP;
  rc = protocol->ops.encode(protocol->codec_ctx, protocol->protocol_version,
                           command, output);
  if (rc != SALTS_OK) {
    output->data_size = 0u;
    return rc;
  }
  rc = flow_protocol_generated_frame_validate(
      protocol, command->device_id, output);
  if (rc != SALTS_OK) return rc;
  if (strcmp(output->metadata.operation, command->operation) != 0) {
    output->data_size = 0u;
    output->metadata = (turbo_flow_protocol_metadata_t)
        TURBO_FLOW_PROTOCOL_METADATA_INIT;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_protocol_get_info(const turbo_flow_protocol_t *protocol,
                                turbo_flow_protocol_info_t *out) {
  turbo_flow_protocol_info_t info = TURBO_FLOW_PROTOCOL_INFO_INIT;
  if (!protocol || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION)
    return SALTS_EINVAL;
  if (atomic_load_explicit(&protocol->state, memory_order_acquire) !=
      FLOW_PROTOCOL_STATE_OPEN)
    return SALTS_EBUSY;
  info.protocol = protocol->protocol;
  info.capabilities = protocol->capabilities;
  info.max_frame_size = protocol->max_frame_size;
  memcpy(info.name, protocol->name, strlen(protocol->name) + 1u);
  memcpy(info.protocol_version, protocol->protocol_version,
         strlen(protocol->protocol_version) + 1u);
  *out = info;
  return SALTS_OK;
}
