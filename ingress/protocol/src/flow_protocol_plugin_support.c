#include "flow_protocol_plugin_support.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdint.h>
#include <string.h>

static int flow_protocol_version_supported(
    const flow_protocol_plugin_descriptor_t *descriptor, const char *version) {
  if (!descriptor || !version) return 0;
  for (size_t i = 0u; i < descriptor->version_count; ++i) {
    if (strcmp(version, descriptor->versions[i]) == 0) return 1;
  }
  return 0;
}

int flow_protocol_plugin_open(
    void *ctx, const turbo_flow_protocol_open_request_t *request,
    turbo_flow_protocol_service_t *service) {
  const flow_protocol_plugin_descriptor_t *descriptor =
      (const flow_protocol_plugin_descriptor_t *)ctx;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  turbo_flow_protocol_capabilities_t capabilities =
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE;
  const char *version;
  turbo_flow_protocol_t *protocol = NULL;
  int rc;
  if (!descriptor || !request || !service ||
      request->protocol != descriptor->protocol)
    return TURBO_EINVAL;
  version = request->protocol_version && request->protocol_version[0] != '\0'
                ? request->protocol_version
                : descriptor->default_version;
  if (!flow_protocol_version_supported(descriptor, version))
    return TURBO_ENOTSUP;
  ops.inspect = descriptor->inspect;
  ops.reply = descriptor->reply;
  ops.encode = descriptor->encode;
  if (ops.reply) capabilities |= TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY;
  if (ops.encode) capabilities |= TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE;
  rc = turbo_flow_protocol_create(
      request, descriptor->name, descriptor->default_version,
      capabilities, &ops, descriptor->inspect_ctx,
      &protocol);
  if (rc != TURBO_OK) return rc;
  service->protocol = descriptor->protocol;
  service->instance = protocol;
  service->owner = protocol;
  return TURBO_OK;
}

void flow_protocol_plugin_close(void *ctx,
                               turbo_flow_protocol_service_t *service) {
  (void)ctx;
  if (!service) return;
  turbo_flow_protocol_destroy((turbo_flow_protocol_t *)service->owner);
  service->instance = NULL;
  service->owner = NULL;
}

int flow_protocol_metadata_text(char *out, size_t capacity,
                               const char *text) {
  const size_t length = text ? strlen(text) : 0u;
  return flow_protocol_metadata_text_n(out, capacity, text, length);
}

int flow_protocol_metadata_text_n(char *out, size_t capacity, const char *text,
                                 size_t length) {
  if (!out || capacity == 0u || !text || length == 0u) return TURBO_EINVAL;
  if (length >= capacity) return TURBO_EMSGSIZE;
  if (memchr(text, '\0', length) != NULL) return TURBO_EPROTO;
  memcpy(out, text, length);
  out[length] = '\0';
  return TURBO_OK;
}

int flow_protocol_metadata_format(char *out, size_t capacity,
                                 const char *format, unsigned value) {
  const int written =
      out && capacity > 0u && format
          ? snprintf(out, capacity, format, value)
          : -1;
  return written > 0 && (size_t)written < capacity ? TURBO_OK
                                                   : TURBO_EMSGSIZE;
}

int flow_protocol_coap_inspect(const turbo_flow_protocol_frame_view_t *frame,
                              turbo_flow_protocol_metadata_t *metadata,
                              int lwm2m) {
  size_t offset;
  uint8_t token_length;
  uint8_t code;
  const char *operation;
  if (!frame || !metadata || frame->data_size < 4u) return TURBO_EPROTO;
  if ((frame->data[0] >> 6u) != 1u) return TURBO_EPROTO;
  token_length = frame->data[0] & 0x0fu;
  if (token_length > 8u || frame->data_size < 4u + token_length)
    return TURBO_EPROTO;
  offset = 4u + token_length;
  while (offset < frame->data_size) {
    const uint8_t header = frame->data[offset++];
    size_t delta = header >> 4u;
    size_t length = header & 0x0fu;
    if (header == 0xffu) {
      if (offset >= frame->data_size) return TURBO_EPROTO;
      offset = frame->data_size;
      break;
    }
    if (delta == 15u || length == 15u) return TURBO_EPROTO;
    if (delta == 13u) {
      if (offset >= frame->data_size) return TURBO_EPROTO;
      delta = 13u + frame->data[offset++];
    } else if (delta == 14u) {
      if (frame->data_size - offset < 2u) return TURBO_EPROTO;
      delta = 269u + ((size_t)frame->data[offset] << 8u) +
              frame->data[offset + 1u];
      offset += 2u;
    }
    if (length == 13u) {
      if (offset >= frame->data_size) return TURBO_EPROTO;
      length = 13u + frame->data[offset++];
    } else if (length == 14u) {
      if (frame->data_size - offset < 2u) return TURBO_EPROTO;
      length = 269u + ((size_t)frame->data[offset] << 8u) +
               frame->data[offset + 1u];
      offset += 2u;
    }
    if (delta > UINT16_MAX || length > frame->data_size - offset)
      return TURBO_EPROTO;
    offset += length;
  }
  code = frame->data[1];
  if (code == 0u)
    operation = "empty";
  else if ((code >> 5u) == 0u) {
    switch (code & 0x1fu) {
    case 1u:
      operation = lwm2m ? "read" : "get";
      break;
    case 2u:
      operation = lwm2m ? "execute" : "post";
      break;
    case 3u:
      operation = lwm2m ? "write" : "put";
      break;
    case 4u:
      operation = "delete";
      break;
    default:
      operation = NULL;
      break;
    }
  } else {
    operation = "response";
  }
  metadata->message_type = code;
  metadata->sequence =
      ((uint64_t)frame->data[2] << 8u) | (uint64_t)frame->data[3];
  if (token_length > 0u) {
    static const char hex[] = "0123456789abcdef";
    if ((size_t)token_length * 2u >= sizeof(metadata->correlation_id))
      return TURBO_EMSGSIZE;
    for (size_t i = 0u; i < token_length; ++i) {
      metadata->correlation_id[i * 2u] =
          hex[frame->data[4u + i] >> 4u];
      metadata->correlation_id[i * 2u + 1u] =
          hex[frame->data[4u + i] & 0x0fu];
    }
    metadata->correlation_id[(size_t)token_length * 2u] = '\0';
  }
  return operation
             ? flow_protocol_metadata_text(metadata->operation,
                                          sizeof(metadata->operation),
                                          operation)
             : flow_protocol_metadata_format(metadata->operation,
                                            sizeof(metadata->operation),
                                            "code-%02x", code);
}

static uint8_t flow_protocol_coap_response_code(uint8_t request_code,
                                               int status) {
  if (status != TURBO_OK) return UINT8_C(0xa0); /* 5.00 */
  switch (request_code) {
  case 0u:
    return 0u;
  case 1u:
    return UINT8_C(0x45); /* 2.05 Content */
  case 2u:
  case 3u:
    return UINT8_C(0x44); /* 2.04 Changed */
  case 4u:
    return UINT8_C(0x42); /* 2.02 Deleted */
  default:
    return UINT8_C(0x80); /* 4.00 Bad Request */
  }
}

int flow_protocol_coap_reply(
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_output_t *output, int lwm2m,
    uint16_t separate_message_id) {
  uint8_t type;
  uint8_t token_length;
  uint8_t code;
  size_t response_size;
  (void)lwm2m;
  if (!request || !request->data || request->data_size < 4u || !output ||
      !output->data)
    return TURBO_EINVAL;
  type = (request->data[0] >> 4u) & 0x03u;
  token_length = request->data[0] & 0x0fu;
  code = request->data[1];
  if (token_length > 8u || request->data_size < 4u + token_length)
    return TURBO_EPROTO;
  if ((code >> 5u) != 0u || type == 2u || type == 3u) {
    output->data_size = 0u;
    return TURBO_OK;
  }
  response_size = 4u + token_length;
  if (response_size > output->capacity) return TURBO_EMSGSIZE;
  output->data[0] = (uint8_t)(0x40u | token_length);
  if (type == 0u) {
    output->data[0] |= UINT8_C(0x20); /* ACK */
    output->data[2] = request->data[2];
    output->data[3] = request->data[3];
  } else {
    output->data[0] |= UINT8_C(0x10); /* NON */
    if (separate_message_id == 0u) return TURBO_ERANGE;
    output->data[2] = (uint8_t)(separate_message_id >> 8u);
    output->data[3] = (uint8_t)separate_message_id;
  }
  output->data[1] = flow_protocol_coap_response_code(code, status);
  if (token_length > 0u)
    memcpy(output->data + 4u, request->data + 4u, token_length);
  output->data_size = response_size;
  return TURBO_OK;
}

static int flow_protocol_coap_code(const char *operation, int lwm2m,
                                  uint8_t *out) {
  if (!operation || !out) return TURBO_EINVAL;
  if (strcmp(operation, lwm2m ? "read" : "get") == 0)
    *out = 1u;
  else if (strcmp(operation, lwm2m ? "execute" : "post") == 0)
    *out = 2u;
  else if (strcmp(operation, lwm2m ? "write" : "put") == 0)
    *out = 3u;
  else if (strcmp(operation, "delete") == 0)
    *out = 4u;
  else
    return TURBO_ENOTSUP;
  return TURBO_OK;
}

static int flow_protocol_coap_option_write(uint8_t *data, size_t capacity,
                                          size_t *offset, uint16_t delta,
                                          const char *value,
                                          size_t value_size) {
  uint8_t delta_nibble;
  uint8_t length_nibble;
  size_t required = 1u + value_size;
  if (!data || !offset || (!value && value_size != 0u)) return TURBO_EINVAL;
  if (delta < 13u)
    delta_nibble = (uint8_t)delta;
  else if (delta < 269u) {
    delta_nibble = 13u;
    required++;
  } else {
    delta_nibble = 14u;
    required += 2u;
  }
  if (value_size < 13u)
    length_nibble = (uint8_t)value_size;
  else if (value_size < 269u) {
    length_nibble = 13u;
    required++;
  } else if (value_size <= UINT16_MAX + 269u) {
    length_nibble = 14u;
    required += 2u;
  } else {
    return TURBO_EMSGSIZE;
  }
  if (required > capacity - *offset) return TURBO_EMSGSIZE;
  data[(*offset)++] = (uint8_t)((delta_nibble << 4u) | length_nibble);
  if (delta_nibble == 13u)
    data[(*offset)++] = (uint8_t)(delta - 13u);
  else if (delta_nibble == 14u) {
    const uint16_t encoded = (uint16_t)(delta - 269u);
    data[(*offset)++] = (uint8_t)(encoded >> 8u);
    data[(*offset)++] = (uint8_t)encoded;
  }
  if (length_nibble == 13u)
    data[(*offset)++] = (uint8_t)(value_size - 13u);
  else if (length_nibble == 14u) {
    const uint16_t encoded = (uint16_t)(value_size - 269u);
    data[(*offset)++] = (uint8_t)(encoded >> 8u);
    data[(*offset)++] = (uint8_t)encoded;
  }
  if (value_size > 0u) {
    memcpy(data + *offset, value, value_size);
    *offset += value_size;
  }
  return TURBO_OK;
}

int flow_protocol_coap_encode(
    const turbo_flow_protocol_command_view_t *command,
    turbo_flow_protocol_frame_output_t *output, int lwm2m) {
  const char *resource;
  const char *segment;
  size_t token_size;
  size_t offset;
  uint16_t message_id;
  uint8_t code;
  int first_option = 1;
  int rc;
  if (!command || !output || !output->data || !command->resource)
    return TURBO_EINVAL;
  rc = flow_protocol_coap_code(command->operation, lwm2m, &code);
  if (rc != TURBO_OK) return rc;
  token_size = command->correlation_id ? strlen(command->correlation_id) : 0u;
  if (token_size > 8u) return TURBO_EMSGSIZE;
  message_id = (uint16_t)command->sequence;
  if (message_id == 0u) return TURBO_EINVAL;
  if (4u + token_size > output->capacity) return TURBO_EMSGSIZE;
  output->data[0] = (uint8_t)(0x40u | token_size); /* v1 CON */
  output->data[1] = code;
  output->data[2] = (uint8_t)(message_id >> 8u);
  output->data[3] = (uint8_t)message_id;
  if (token_size > 0u)
    memcpy(output->data + 4u, command->correlation_id, token_size);
  offset = 4u + token_size;
  resource = command->resource;
  while (*resource == '/') resource++;
  if (*resource == '\0') return TURBO_EINVAL;
  segment = resource;
  for (;;) {
    const char *slash = strchr(segment, '/');
    const size_t segment_size =
        slash ? (size_t)(slash - segment) : strlen(segment);
    if (segment_size == 0u) return TURBO_EINVAL;
    rc = flow_protocol_coap_option_write(
        output->data, output->capacity, &offset,
        first_option ? UINT16_C(11) : UINT16_C(0), segment, segment_size);
    if (rc != TURBO_OK) return rc;
    first_option = 0;
    if (!slash) break;
    segment = slash + 1;
  }
  if (command->payload_size > 0u) {
    if (offset >= output->capacity ||
        command->payload_size > output->capacity - offset - 1u)
      return TURBO_EMSGSIZE;
    output->data[offset++] = UINT8_C(0xff);
    memcpy(output->data + offset, command->payload, command->payload_size);
    offset += command->payload_size;
  }
  output->data_size = offset;
  return TURBO_OK;
}
