#include "flow_gateway_plugin_support.h"

#include "turbo_error.h"

#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#define FLOW_JTT808_CAPTURE_SIZE 32u
#define FLOW_JTT808_LEGACY_HEADER_SIZE 12u
#define FLOW_JTT808_2019_HEADER_SIZE 17u

static atomic_uint FLOW_JTT808_NEXT_SERIAL = 1u;

static uint16_t flow_jtt808_next_serial(atomic_uint *counter) {
  unsigned current;
  unsigned next;
  if (!counter) return 0u;
  current = atomic_load_explicit(counter, memory_order_relaxed);
  for (;;) {
    next = current >= UINT16_MAX ? 1u : current + 1u;
    if (atomic_compare_exchange_weak_explicit(
            counter, &current, next, memory_order_relaxed,
            memory_order_relaxed))
      return (uint16_t)next;
  }
}

static int flow_jtt808_unescape(
    const turbo_flow_gateway_frame_view_t *frame, uint8_t *capture,
    size_t capture_size, size_t *decoded_size, uint8_t *xor_value) {
  size_t count = 0u;
  uint8_t checksum = 0u;
  if (!frame || frame->data_size < 2u || frame->data[0] != 0x7eu ||
      frame->data[frame->data_size - 1u] != 0x7eu)
    return TURBO_EPROTO;
  for (size_t i = 1u; i + 1u < frame->data_size; ++i) {
    uint8_t value = frame->data[i];
    if (value == 0x7du) {
      if (++i + 1u >= frame->data_size) return TURBO_EPROTO;
      if (frame->data[i] == 0x01u)
        value = 0x7du;
      else if (frame->data[i] == 0x02u)
        value = 0x7eu;
      else
        return TURBO_EPROTO;
    }
    if (count < capture_size) capture[count] = value;
    checksum ^= value;
    count++;
  }
  *decoded_size = count;
  *xor_value = checksum;
  return TURBO_OK;
}

static int flow_jtt808_device_id(const uint8_t *bcd, size_t bcd_size,
                                 char *out, size_t capacity) {
  if (!bcd || !out || capacity <= bcd_size * 2u) return TURBO_EINVAL;
  for (size_t i = 0u; i < bcd_size; ++i) {
    const uint8_t high = bcd[i] >> 4u;
    const uint8_t low = bcd[i] & 0x0fu;
    if (high > 9u || low > 9u) return TURBO_EPROTO;
    out[i * 2u] = (char)('0' + high);
    out[i * 2u + 1u] = (char)('0' + low);
  }
  out[bcd_size * 2u] = '\0';
  return TURBO_OK;
}

static int flow_jtt808_inspect(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_frame_view_t *frame,
    turbo_flow_gateway_metadata_t *metadata) {
  uint8_t header[FLOW_JTT808_CAPTURE_SIZE] = {0};
  size_t decoded_size = 0u;
  size_t header_size;
  size_t base_header_size;
  size_t body_size;
  size_t phone_offset;
  size_t phone_size;
  size_t serial_offset;
  uint16_t message_id;
  uint16_t properties;
  uint8_t checksum;
  const char *operation;
  int rc;
  (void)ctx;
  (void)configured_version;
  rc = flow_jtt808_unescape(frame, header, sizeof(header), &decoded_size,
                            &checksum);
  if (rc != TURBO_OK || checksum != 0u || decoded_size < 5u)
    return TURBO_EPROTO;
  message_id = ((uint16_t)header[0] << 8u) | header[1];
  properties = ((uint16_t)header[2] << 8u) | header[3];
  body_size = properties & 0x03ffu;
  if ((properties & 0x4000u) != 0u) {
    header_size = FLOW_JTT808_2019_HEADER_SIZE;
    phone_offset = 5u;
    phone_size = 10u;
    serial_offset = 15u;
    if (decoded_size < header_size + 1u || header[4] != 1u)
      return TURBO_EPROTO;
  } else {
    header_size = FLOW_JTT808_LEGACY_HEADER_SIZE;
    phone_offset = 4u;
    phone_size = 6u;
    serial_offset = 10u;
  }
  base_header_size = header_size;
  if ((properties & 0x2000u) != 0u) {
    uint16_t package_total;
    uint16_t package_index;
    header_size += 4u;
    if (decoded_size < header_size + 1u) return TURBO_EPROTO;
    package_total =
        ((uint16_t)header[base_header_size] << 8u) |
        header[base_header_size + 1u];
    package_index =
        ((uint16_t)header[base_header_size + 2u] << 8u) |
        header[base_header_size + 3u];
    if (package_total == 0u || package_index == 0u ||
        package_index > package_total)
      return TURBO_EPROTO;
  }
  if (body_size > SIZE_MAX - header_size - 1u ||
      decoded_size != header_size + body_size + 1u)
    return TURBO_EPROTO;
  rc = flow_jtt808_device_id(header + phone_offset, phone_size,
                             metadata->device_id,
                             sizeof(metadata->device_id));
  if (rc != TURBO_OK) return rc;
  metadata->message_type = message_id;
  metadata->sequence =
      ((uint64_t)header[serial_offset] << 8u) |
      header[serial_offset + 1u];
  switch (message_id) {
  case 0x0001u:
    operation = "terminal-ack";
    break;
  case 0x0100u:
    operation = "register";
    break;
  case 0x0102u:
    operation = "authenticate";
    break;
  case 0x0200u:
    operation = "location";
    break;
  case 0x0201u:
    operation = "location-query-response";
    break;
  case 0x0704u:
    operation = "batch-location";
    break;
  case 0x0900u:
    operation = "transparent-data";
    break;
  case 0x8001u:
    operation = "platform-ack";
    break;
  case 0x8100u:
    operation = "register-response";
    break;
  case 0x8103u:
    operation = "set-parameters";
    break;
  case 0x8104u:
    operation = "query-parameters";
    break;
  case 0x8105u:
    operation = "terminal-control";
    break;
  case 0x8201u:
    operation = "location-query";
    break;
  case 0x8300u:
    operation = "text";
    break;
  case 0x8400u:
    operation = "call";
    break;
  case 0x8900u:
    operation = "platform-transparent-data";
    break;
  default:
    operation = NULL;
    break;
  }
  return operation
             ? flow_gateway_metadata_text(metadata->operation,
                                          sizeof(metadata->operation),
                                          operation)
             : flow_gateway_metadata_format(metadata->operation,
                                            sizeof(metadata->operation),
                                            "message-%04x", message_id);
}

static int flow_jtt808_bcd_write(const char *digits, size_t digit_count,
                                 uint8_t *out) {
  if (!digits || !out || (digit_count & 1u) != 0u) return TURBO_EINVAL;
  for (size_t i = 0u; i < digit_count; i += 2u) {
    if (digits[i] < '0' || digits[i] > '9' ||
        digits[i + 1u] < '0' || digits[i + 1u] > '9')
      return TURBO_EINVAL;
    out[i / 2u] =
        (uint8_t)(((digits[i] - '0') << 4u) | (digits[i + 1u] - '0'));
  }
  return TURBO_OK;
}

static int flow_jtt808_frame_write(
    uint16_t message_id, const char *device_id, int version_2019,
    uint16_t serial, const uint8_t *body, size_t body_size,
    turbo_flow_gateway_frame_output_t *output) {
  uint8_t header[FLOW_JTT808_2019_HEADER_SIZE];
  uint16_t properties;
  size_t header_size =
      version_2019 ? FLOW_JTT808_2019_HEADER_SIZE
                   : FLOW_JTT808_LEGACY_HEADER_SIZE;
  size_t raw_size;
  size_t offset = 0u;
  uint8_t checksum = 0u;
  int rc;
  if (!device_id || serial == 0u || (!body && body_size != 0u) ||
      !output || !output->data)
    return TURBO_EINVAL;
  if (body_size > 0x03ffu ||
      strlen(device_id) != (version_2019 ? 20u : 12u))
    return TURBO_EMSGSIZE;
  properties = (uint16_t)body_size;
  if (version_2019) properties |= UINT16_C(0x4000);
  header[0] = (uint8_t)(message_id >> 8u);
  header[1] = (uint8_t)message_id;
  header[2] = (uint8_t)(properties >> 8u);
  header[3] = (uint8_t)properties;
  if (version_2019) {
    header[4] = 1u;
    rc = flow_jtt808_bcd_write(device_id, 20u, header + 5u);
    header[15] = (uint8_t)(serial >> 8u);
    header[16] = (uint8_t)serial;
  } else {
    rc = flow_jtt808_bcd_write(device_id, 12u, header + 4u);
    header[10] = (uint8_t)(serial >> 8u);
    header[11] = (uint8_t)serial;
  }
  if (rc != TURBO_OK) return rc;
  raw_size = header_size + body_size + 1u;
  if (raw_size > (SIZE_MAX - 2u) / 2u ||
      raw_size * 2u + 2u > output->capacity)
    return TURBO_EMSGSIZE;
  output->data[offset++] = UINT8_C(0x7e);
  for (size_t i = 0u; i <= header_size + body_size; ++i) {
    uint8_t value;
    if (i < header_size)
      value = header[i];
    else if (i < header_size + body_size)
      value = body[i - header_size];
    else {
      value = checksum;
    }
    if (i < header_size + body_size) checksum ^= value;
    if (value == UINT8_C(0x7e)) {
      output->data[offset++] = UINT8_C(0x7d);
      output->data[offset++] = UINT8_C(0x02);
    } else if (value == UINT8_C(0x7d)) {
      output->data[offset++] = UINT8_C(0x7d);
      output->data[offset++] = UINT8_C(0x01);
    } else {
      output->data[offset++] = value;
    }
  }
  output->data[offset++] = UINT8_C(0x7e);
  output->data_size = offset;
  return TURBO_OK;
}

static int flow_jtt808_reply(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_frame_view_t *request, int status,
    turbo_flow_gateway_frame_output_t *output) {
  uint8_t header[FLOW_JTT808_CAPTURE_SIZE] = {0};
  uint8_t body[5];
  uint8_t checksum = 0u;
  size_t decoded_size = 0u;
  size_t phone_offset;
  size_t phone_size;
  size_t serial_offset;
  uint16_t properties;
  char device_id[21];
  uint16_t next;
  int version_2019;
  int rc;
  (void)configured_version;
  if (!ctx) return TURBO_EINVAL;
  rc = flow_jtt808_unescape(request, header, sizeof(header), &decoded_size,
                            &checksum);
  if (rc != TURBO_OK || checksum != 0u || decoded_size < 5u)
    return TURBO_EPROTO;
  properties = ((uint16_t)header[2] << 8u) | header[3];
  version_2019 = (properties & UINT16_C(0x4000)) != 0u;
  phone_offset = version_2019 ? 5u : 4u;
  phone_size = version_2019 ? 10u : 6u;
  serial_offset = version_2019 ? 15u : 10u;
  rc = flow_jtt808_device_id(header + phone_offset, phone_size, device_id,
                             sizeof(device_id));
  if (rc != TURBO_OK) return rc;
  body[0] = header[serial_offset];
  body[1] = header[serial_offset + 1u];
  body[2] = header[0];
  body[3] = header[1];
  body[4] = status == TURBO_OK ? 0u : status == TURBO_ENOTSUP ? 3u : 1u;
  next = flow_jtt808_next_serial((atomic_uint *)ctx);
  if (next == 0u) return TURBO_ERANGE;
  return flow_jtt808_frame_write(UINT16_C(0x8001), device_id, version_2019,
                                 next, body, sizeof(body), output);
}

static int flow_jtt808_message_id(const char *operation, uint16_t *out) {
  if (!operation || !out) return TURBO_EINVAL;
  if (strcmp(operation, "platform-ack") == 0)
    *out = UINT16_C(0x8001);
  else if (strcmp(operation, "register-response") == 0)
    *out = UINT16_C(0x8100);
  else if (strcmp(operation, "set-parameters") == 0)
    *out = UINT16_C(0x8103);
  else if (strcmp(operation, "query-parameters") == 0)
    *out = UINT16_C(0x8104);
  else if (strcmp(operation, "terminal-control") == 0)
    *out = UINT16_C(0x8105);
  else if (strcmp(operation, "location-query") == 0)
    *out = UINT16_C(0x8201);
  else if (strcmp(operation, "text") == 0)
    *out = UINT16_C(0x8300);
  else if (strcmp(operation, "call") == 0)
    *out = UINT16_C(0x8400);
  else if (strcmp(operation, "platform-transparent-data") == 0)
    *out = UINT16_C(0x8900);
  else
    return TURBO_ENOTSUP;
  return TURBO_OK;
}

static int flow_jtt808_encode(
    void *ctx, const char *configured_version,
    const turbo_flow_gateway_command_view_t *command,
    turbo_flow_gateway_frame_output_t *output) {
  uint16_t message_id;
  int rc;
  (void)ctx;
  (void)configured_version;
  if (!command || command->sequence == 0u ||
      command->sequence > UINT16_MAX)
    return TURBO_EINVAL;
  rc = flow_jtt808_message_id(command->operation, &message_id);
  if (rc != TURBO_OK) return rc;
  return flow_jtt808_frame_write(
      message_id, command->device_id, 1, (uint16_t)command->sequence,
      command->payload, command->payload_size, output);
}

static const char *const FLOW_JTT808_VERSIONS[] = {"2019-A1"};
static const flow_gateway_plugin_descriptor_t FLOW_JTT808_DESCRIPTOR = {
    "jtt808", TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808, "2019-A1",
    FLOW_JTT808_VERSIONS,
    sizeof(FLOW_JTT808_VERSIONS) / sizeof(FLOW_JTT808_VERSIONS[0]),
    flow_jtt808_inspect, flow_jtt808_reply, flow_jtt808_encode,
    &FLOW_JTT808_NEXT_SERIAL};

static const turbo_flow_gateway_plugin_api_t FLOW_JTT808_API = {
    sizeof(turbo_flow_gateway_plugin_api_t),
    TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_GATEWAY_PLUGIN_API_VERSION_MINOR,
    "jtt808",
    TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808,
    TURBO_FLOW_GATEWAY_CAP_INGRESS | TURBO_FLOW_GATEWAY_CAP_EGRESS |
        TURBO_FLOW_GATEWAY_CAP_RAW_PRESERVE |
        TURBO_FLOW_GATEWAY_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_GATEWAY_CAP_COMMAND_ENCODE,
    (void *)&FLOW_JTT808_DESCRIPTOR,
    flow_gateway_plugin_open,
    flow_gateway_plugin_close};

const turbo_flow_gateway_plugin_api_t *
turbo_flow_gateway_plugin_get_api(void) {
  return &FLOW_JTT808_API;
}
