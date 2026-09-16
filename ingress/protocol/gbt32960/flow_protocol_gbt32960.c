#include "flow_protocol_plugin_support.h"

#include "salts_error.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define FLOW_GBT32960_HEADER_SIZE 24u
#define FLOW_GBT32960_FRAME_OVERHEAD 25u

static int flow_gbt32960_inspect(void *ctx, const char *configured_version,
                                 const turbo_flow_protocol_frame_view_t *frame,
                                 turbo_flow_protocol_metadata_t *metadata) {
  size_t body_size;
  size_t expected_size;
  uint8_t checksum = 0u;
  const char *operation;
  (void)ctx;
  (void)configured_version;
  if (!frame || !metadata || frame->data_size < FLOW_GBT32960_FRAME_OVERHEAD ||
      frame->data[0] != 0x23u || frame->data[1] != 0x23u)
    return SALTS_EPROTO;
  if (frame->data[3] != 0x01u && frame->data[3] != 0x02u && frame->data[3] != 0x03u &&
      frame->data[3] != 0xfeu)
    return SALTS_EPROTO;
  if (frame->data[21] == 0u) return SALTS_EPROTO;
  body_size = ((size_t)frame->data[22] << 8u) | frame->data[23];
  if (body_size > SIZE_MAX - FLOW_GBT32960_FRAME_OVERHEAD) return SALTS_EMSGSIZE;
  expected_size = body_size + FLOW_GBT32960_FRAME_OVERHEAD;
  if (frame->data_size != expected_size) return SALTS_EPROTO;
  for (size_t i = 2u; i + 1u < frame->data_size; ++i)
    checksum ^= frame->data[i];
  if (checksum != frame->data[frame->data_size - 1u]) return SALTS_EPROTO;
  for (size_t i = 0u; i < 17u; ++i) {
    const uint8_t value = frame->data[4u + i];
    if (value <= 0x20u || value >= 0x7fu || value == '/' || value == '+' || value == '#')
      return SALTS_EPROTO;
    metadata->device_id[i] = (char)value;
  }
  metadata->device_id[17] = '\0';
  switch (frame->data[2]) {
  case 0x01u:
    operation = "vehicle-login";
    break;
  case 0x02u:
    operation = "realtime-data";
    break;
  case 0x03u:
    operation = "reissue-data";
    break;
  case 0x04u:
    operation = "vehicle-logout";
    break;
  case 0x05u:
    operation = "platform-login";
    break;
  case 0x06u:
    operation = "platform-logout";
    break;
  case 0x07u:
    operation = "heartbeat";
    break;
  case 0x08u:
    operation = "time-sync";
    break;
  default:
    operation = NULL;
    break;
  }
  metadata->message_type = frame->data[2];
  return operation ? flow_protocol_metadata_text(metadata->operation, sizeof(metadata->operation),
                                                 operation)
                   : flow_protocol_metadata_format(metadata->operation, sizeof(metadata->operation),
                                                   "command-%02x", frame->data[2]);
}

static uint8_t flow_gbt32960_response(int status) {
  if (status == SALTS_OK) return UINT8_C(0x01);
  if (status == SALTS_EALREADY) return UINT8_C(0x03);
  return UINT8_C(0x02);
}

static int flow_gbt32960_frame_write(uint8_t command, uint8_t response, const char *device_id,
                                     const uint8_t *body, size_t body_size,
                                     turbo_flow_protocol_frame_output_t *output) {
  size_t frame_size;
  uint8_t checksum = 0u;
  if (!device_id || strlen(device_id) != 17u || !output || !output->data ||
      (!body && body_size != 0u))
    return SALTS_EINVAL;
  if (body_size > UINT16_MAX) return SALTS_EMSGSIZE;
  frame_size = FLOW_GBT32960_FRAME_OVERHEAD + body_size;
  if (frame_size > output->capacity) return SALTS_EMSGSIZE;
  output->data[0] = UINT8_C(0x23);
  output->data[1] = UINT8_C(0x23);
  output->data[2] = command;
  output->data[3] = response;
  memcpy(output->data + 4u, device_id, 17u);
  output->data[21] = UINT8_C(0x01); /* unencrypted */
  output->data[22] = (uint8_t)(body_size >> 8u);
  output->data[23] = (uint8_t)body_size;
  if (body_size > 0u) memcpy(output->data + 24u, body, body_size);
  for (size_t i = 2u; i + 1u < frame_size; ++i)
    checksum ^= output->data[i];
  output->data[frame_size - 1u] = checksum;
  output->data_size = frame_size;
  return SALTS_OK;
}

static int flow_gbt32960_reply(void *ctx, const char *configured_version,
                               const turbo_flow_protocol_frame_view_t *request, int status,
                               turbo_flow_protocol_frame_output_t *output) {
  char device_id[18];
  (void)ctx;
  (void)configured_version;
  if (!request || !request->data || request->data_size < FLOW_GBT32960_FRAME_OVERHEAD)
    return SALTS_EINVAL;
  memcpy(device_id, request->data + 4u, 17u);
  device_id[17] = '\0';
  return flow_gbt32960_frame_write(request->data[2], flow_gbt32960_response(status), device_id,
                                   NULL, 0u, output);
}

static int flow_gbt32960_command(const char *operation, uint8_t *out) {
  if (!operation || !out) return SALTS_EINVAL;
  if (strcmp(operation, "vehicle-login") == 0) *out = UINT8_C(0x01);
  else if (strcmp(operation, "realtime-data") == 0) *out = UINT8_C(0x02);
  else if (strcmp(operation, "reissue-data") == 0) *out = UINT8_C(0x03);
  else if (strcmp(operation, "vehicle-logout") == 0) *out = UINT8_C(0x04);
  else if (strcmp(operation, "platform-login") == 0) *out = UINT8_C(0x05);
  else if (strcmp(operation, "platform-logout") == 0) *out = UINT8_C(0x06);
  else if (strcmp(operation, "heartbeat") == 0) *out = UINT8_C(0x07);
  else if (strcmp(operation, "time-sync") == 0) *out = UINT8_C(0x08);
  else return SALTS_ENOTSUP;
  return SALTS_OK;
}

static int flow_gbt32960_encode(void *ctx, const char *configured_version,
                                const turbo_flow_protocol_command_view_t *command,
                                turbo_flow_protocol_frame_output_t *output) {
  uint8_t command_id;
  int rc;
  (void)ctx;
  (void)configured_version;
  if (!command) return SALTS_EINVAL;
  rc = flow_gbt32960_command(command->operation, &command_id);
  if (rc != SALTS_OK) return rc;
  return flow_gbt32960_frame_write(command_id, UINT8_C(0xfe), command->device_id, command->payload,
                                   command->payload_size, output);
}

static const char *const FLOW_GBT32960_VERSIONS[] = {"2025"};
static const flow_protocol_plugin_descriptor_t FLOW_GBT32960_DESCRIPTOR = {
    "gbt32960",
    TURBO_FLOW_PROTOCOL_GBT_32960,
    "2025",
    FLOW_GBT32960_VERSIONS,
    sizeof(FLOW_GBT32960_VERSIONS) / sizeof(FLOW_GBT32960_VERSIONS[0]),
    flow_gbt32960_inspect,
    flow_gbt32960_reply,
    flow_gbt32960_encode,
    NULL};

static const turbo_flow_protocol_plugin_api_t FLOW_GBT32960_API = {
    sizeof(turbo_flow_protocol_plugin_api_t),
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
    "gbt32960",
    TURBO_FLOW_PROTOCOL_GBT_32960,
    TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
        TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE,
    (void *)&FLOW_GBT32960_DESCRIPTOR,
    flow_protocol_plugin_open,
    flow_protocol_plugin_close};

FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT("gbt32960", "1.0.0", FLOW_GBT32960_API)
