#include "flow_protocol_plugin_support.h"

#include "salts_error.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define FLOW_JTT808_CAPTURE_SIZE 32u
#define FLOW_JTT808_LEGACY_HEADER_SIZE 12u
#define FLOW_JTT808_2019_HEADER_SIZE 17u
#define FLOW_JTT808_MAX_HEADER_SIZE (FLOW_JTT808_2019_HEADER_SIZE + 4u)
#define FLOW_JTT808_TRANSPARENT_JSON 0x01u

static atomic_uint FLOW_JTT808_NEXT_SERIAL = 1u;

static uint16_t flow_jtt808_next_serial(atomic_uint *counter) {
  unsigned current;
  unsigned next;
  if (!counter) return 0u;
  current = atomic_load_explicit(counter, memory_order_relaxed);
  for (;;) {
    next = current >= UINT16_MAX ? 1u : current + 1u;
    if (atomic_compare_exchange_weak_explicit(counter, &current, next, memory_order_relaxed,
                                              memory_order_relaxed))
      return (uint16_t)next;
  }
}

static int flow_jtt808_unescape(const turbo_flow_protocol_frame_view_t *frame, uint8_t *capture,
                                size_t capture_size, size_t *decoded_size, uint8_t *xor_value) {
  size_t count = 0u;
  uint8_t checksum = 0u;
  if (!frame || frame->data_size < 2u || frame->data[0] != 0x7eu ||
      frame->data[frame->data_size - 1u] != 0x7eu)
    return SALTS_EPROTO;
  for (size_t i = 1u; i + 1u < frame->data_size; ++i) {
    uint8_t value = frame->data[i];
    if (value == 0x7du) {
      if (++i + 1u >= frame->data_size) return SALTS_EPROTO;
      if (frame->data[i] == 0x01u) value = 0x7du;
      else if (frame->data[i] == 0x02u) value = 0x7eu;
      else return SALTS_EPROTO;
    }
    if (count < capture_size) capture[count] = value;
    checksum ^= value;
    count++;
  }
  *decoded_size = count;
  *xor_value = checksum;
  return SALTS_OK;
}

static int flow_jtt808_device_id(const uint8_t *bcd, size_t bcd_size, char *out, size_t capacity) {
  if (!bcd || !out || capacity <= bcd_size * 2u) return SALTS_EINVAL;
  for (size_t i = 0u; i < bcd_size; ++i) {
    const uint8_t high = bcd[i] >> 4u;
    const uint8_t low = bcd[i] & 0x0fu;
    if (high > 9u || low > 9u) return SALTS_EPROTO;
    out[i * 2u] = (char)('0' + high);
    out[i * 2u + 1u] = (char)('0' + low);
  }
  out[bcd_size * 2u] = '\0';
  return SALTS_OK;
}

static int flow_jtt808_decode_ingress(
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata,
    turbo_flow_protocol_semantic_output_t *semantic) {
  uint8_t header[FLOW_JTT808_MAX_HEADER_SIZE] = {0};
  size_t decoded_size = 0u;
  size_t header_size = 0u;
  size_t base_header_size = 0u;
  size_t body_size = 0u;
  size_t phone_offset = 0u;
  size_t phone_size = 0u;
  size_t serial_offset = 0u;
  uint16_t message_id = 0u;
  uint16_t properties = 0u;
  uint8_t checksum = 0u;
  uint32_t semantic_type = TURBO_FLOW_PROTOCOL_SEMANTIC_TYPE_NONE;
  int header_known = 0;
  const char *operation;
  int rc;

  if (!frame || !frame->data || !metadata || frame->data_size < 2u ||
      frame->data[0] != 0x7eu || frame->data[frame->data_size - 1u] != 0x7eu)
    return SALTS_EPROTO;
  if (semantic &&
      (semantic->size < sizeof(*semantic) ||
       semantic->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
       (!semantic->data && semantic->capacity != 0u)))
    return SALTS_EINVAL;

  for (size_t i = 1u; i + 1u < frame->data_size; ++i) {
    uint8_t value = frame->data[i];
    if (value == 0x7du) {
      if (++i + 1u >= frame->data_size) return SALTS_EPROTO;
      if (frame->data[i] == 0x01u)
        value = 0x7du;
      else if (frame->data[i] == 0x02u)
        value = 0x7eu;
      else
        return SALTS_EPROTO;
    }

    checksum ^= value;
    if (decoded_size < sizeof(header)) header[decoded_size] = value;

    if (!header_known && decoded_size == 3u) {
      message_id = ((uint16_t)header[0] << 8u) | header[1];
      properties = ((uint16_t)header[2] << 8u) | header[3];
      body_size = properties & 0x03ffu;
      base_header_size =
          (properties & 0x4000u) != 0u ? FLOW_JTT808_2019_HEADER_SIZE
                                       : FLOW_JTT808_LEGACY_HEADER_SIZE;
      header_size = base_header_size + (((properties & 0x2000u) != 0u) ? 4u : 0u);
      if (header_size > sizeof(header)) return SALTS_EPROTO;
      header_known = 1;
    }

    if (header_known && decoded_size >= header_size &&
        decoded_size < header_size + body_size &&
        semantic && message_id == UINT16_C(0x0900)) {
      const size_t body_index = decoded_size - header_size;
      if (body_index == 0u) {
        semantic_type = value;
      } else {
        const size_t semantic_index = body_index - 1u;
        if (semantic_index >= semantic->capacity) return SALTS_EMSGSIZE;
        if (!semantic->data) return SALTS_EINVAL;
        semantic->data[semantic_index] = value;
      }
    }
    decoded_size++;
  }

  if (!header_known || checksum != 0u ||
      body_size > SIZE_MAX - header_size - 1u ||
      decoded_size != header_size + body_size + 1u)
    return SALTS_EPROTO;

  if ((properties & 0x4000u) != 0u) {
    phone_offset = 5u;
    phone_size = 10u;
    serial_offset = 15u;
    if (base_header_size != FLOW_JTT808_2019_HEADER_SIZE || header[4] != 1u)
      return SALTS_EPROTO;
  } else {
    phone_offset = 4u;
    phone_size = 6u;
    serial_offset = 10u;
    if (base_header_size != FLOW_JTT808_LEGACY_HEADER_SIZE)
      return SALTS_EPROTO;
  }

  if ((properties & 0x2000u) != 0u) {
    const uint16_t package_total =
        ((uint16_t)header[base_header_size] << 8u) |
        header[base_header_size + 1u];
    const uint16_t package_index =
        ((uint16_t)header[base_header_size + 2u] << 8u) |
        header[base_header_size + 3u];
    if (package_total == 0u || package_index == 0u ||
        package_index > package_total)
      return SALTS_EPROTO;
    if (semantic) return SALTS_ENOTSUP;
  }

  rc = flow_jtt808_device_id(header + phone_offset, phone_size,
                             metadata->device_id,
                             sizeof(metadata->device_id));
  if (rc != SALTS_OK) return rc;
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

  if (semantic) {
    if (message_id != UINT16_C(0x0900)) return SALTS_ENOTSUP;
    if (body_size == 0u) return SALTS_EPROTO;
    semantic->semantic_type = semantic_type;
    semantic->data_size = body_size - 1u;
    semantic->media_type[0] = '\0';
    if (semantic_type == FLOW_JTT808_TRANSPARENT_JSON)
      memcpy(semantic->media_type, "application/json", sizeof("application/json"));
  }

  return operation
             ? flow_protocol_metadata_text(metadata->operation,
                                           sizeof(metadata->operation),
                                           operation)
             : flow_protocol_metadata_format(metadata->operation,
                                             sizeof(metadata->operation),
                                             "message-%04x", message_id);
}

static int flow_jtt808_inspect(void *ctx, const char *configured_version,
                               const turbo_flow_protocol_frame_view_t *frame,
                               turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  return flow_jtt808_decode_ingress(frame, metadata, NULL);
}

static int flow_jtt808_decode_semantic(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata,
    turbo_flow_protocol_semantic_output_t *output) {
  (void)ctx;
  (void)configured_version;
  return flow_jtt808_decode_ingress(frame, metadata, output);
}

static int flow_jtt808_bcd_write(const char *digits, size_t digit_count, uint8_t *out) {
  if (!digits || !out || (digit_count & 1u) != 0u) return SALTS_EINVAL;
  for (size_t i = 0u; i < digit_count; i += 2u) {
    if (digits[i] < '0' || digits[i] > '9' || digits[i + 1u] < '0' || digits[i + 1u] > '9')
      return SALTS_EINVAL;
    out[i / 2u] = (uint8_t)(((digits[i] - '0') << 4u) | (digits[i + 1u] - '0'));
  }
  return SALTS_OK;
}

static int flow_jtt808_frame_header(
    uint16_t message_id, const char *device_id, int version_2019,
    uint16_t serial, size_t body_size, uint8_t *header,
    size_t header_capacity, size_t *header_size_out) {
  uint16_t properties;
  const size_t header_size =
      version_2019 ? FLOW_JTT808_2019_HEADER_SIZE
                   : FLOW_JTT808_LEGACY_HEADER_SIZE;
  int rc;
  if (header_size_out) *header_size_out = 0u;
  if (!device_id || serial == 0u || !header || !header_size_out ||
      header_capacity < header_size)
    return SALTS_EINVAL;
  if (body_size > 0x03ffu ||
      strlen(device_id) != (version_2019 ? 20u : 12u))
    return SALTS_EMSGSIZE;

  properties = (uint16_t)body_size;
  if (version_2019) properties |= UINT16_C(0x4000);
  memset(header, 0, header_size);
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
  if (rc != SALTS_OK) return rc;
  *header_size_out = header_size;
  return SALTS_OK;
}

static size_t flow_jtt808_escaped_size(const uint8_t *data, size_t size) {
  size_t encoded = 0u;
  if (!data && size != 0u) return SIZE_MAX;
  for (size_t i = 0u; i < size; ++i) {
    if (encoded == SIZE_MAX) return SIZE_MAX;
    encoded += data[i] == UINT8_C(0x7e) || data[i] == UINT8_C(0x7d)
                   ? 2u
                   : 1u;
  }
  return encoded;
}

static int flow_jtt808_escape_write(const uint8_t *data, size_t size,
                                    uint8_t *out, size_t capacity,
                                    size_t *written_out) {
  size_t written = 0u;
  if (written_out) *written_out = 0u;
  if ((!data && size != 0u) || !out || !written_out) return SALTS_EINVAL;
  for (size_t i = 0u; i < size; ++i) {
    const uint8_t value = data[i];
    const size_t needed =
        value == UINT8_C(0x7e) || value == UINT8_C(0x7d) ? 2u : 1u;
    if (needed > capacity - written) return SALTS_EMSGSIZE;
    if (value == UINT8_C(0x7e)) {
      out[written++] = UINT8_C(0x7d);
      out[written++] = UINT8_C(0x02);
    } else if (value == UINT8_C(0x7d)) {
      out[written++] = UINT8_C(0x7d);
      out[written++] = UINT8_C(0x01);
    } else {
      out[written++] = value;
    }
  }
  *written_out = written;
  return SALTS_OK;
}

static int flow_jtt808_frame_write(uint16_t message_id, const char *device_id,
                                   int version_2019, uint16_t serial,
                                   const uint8_t *body, size_t body_size,
                                   turbo_flow_protocol_frame_output_t *output) {
  uint8_t header[FLOW_JTT808_2019_HEADER_SIZE];
  size_t header_size = 0u;
  size_t offset = 0u;
  size_t written = 0u;
  size_t escaped_header;
  size_t escaped_body;
  size_t escaped_checksum;
  uint8_t checksum = 0u;
  int rc;
  if ((!body && body_size != 0u) || !output || !output->data)
    return SALTS_EINVAL;
  rc = flow_jtt808_frame_header(message_id, device_id, version_2019,
                                serial, body_size, header, sizeof(header),
                                &header_size);
  if (rc != SALTS_OK) return rc;
  for (size_t i = 0u; i < header_size; ++i) checksum ^= header[i];
  for (size_t i = 0u; i < body_size; ++i) checksum ^= body[i];

  escaped_header = flow_jtt808_escaped_size(header, header_size);
  escaped_body = flow_jtt808_escaped_size(body, body_size);
  escaped_checksum = flow_jtt808_escaped_size(&checksum, 1u);
  if (escaped_header == SIZE_MAX || escaped_body == SIZE_MAX ||
      escaped_checksum == SIZE_MAX ||
      escaped_header > SIZE_MAX - escaped_body ||
      escaped_header + escaped_body > SIZE_MAX - escaped_checksum ||
      escaped_header + escaped_body + escaped_checksum > SIZE_MAX - 2u)
    return SALTS_ERANGE;
  if (escaped_header + escaped_body + escaped_checksum + 2u >
      output->capacity)
    return SALTS_EMSGSIZE;

  output->data[offset++] = UINT8_C(0x7e);
  rc = flow_jtt808_escape_write(header, header_size, output->data + offset,
                                output->capacity - offset, &written);
  if (rc != SALTS_OK) return rc;
  offset += written;
  if (body_size != 0u) {
    rc = flow_jtt808_escape_write(body, body_size, output->data + offset,
                                  output->capacity - offset, &written);
    if (rc != SALTS_OK) return rc;
    offset += written;
  }
  rc = flow_jtt808_escape_write(&checksum, 1u, output->data + offset,
                                output->capacity - offset, &written);
  if (rc != SALTS_OK) return rc;
  offset += written;
  output->data[offset++] = UINT8_C(0x7e);
  output->data_size = offset;
  return SALTS_OK;
}

static int flow_jtt808_frame_slices_write(
    uint16_t message_id, const char *device_id, int version_2019,
    uint16_t serial, const uint8_t *body, size_t body_size,
    turbo_flow_protocol_frame_slices_output_t *output) {
  uint8_t header[FLOW_JTT808_2019_HEADER_SIZE];
  uint8_t checksum = 0u;
  size_t header_size = 0u;
  size_t escaped_header;
  size_t escaped_body;
  size_t escaped_checksum;
  size_t needed_segments;
  size_t count = 0u;
  size_t total = 0u;
  mem_buffer_t *delimiter_buffer = NULL;
  mem_buffer_t *header_buffer = NULL;
  mem_buffer_t *body_buffer = NULL;
  mem_buffer_t *checksum_buffer = NULL;
  mem_slice_t slices[5] = {{0}};
  size_t written = 0u;
  int rc;

  if ((!body && body_size != 0u) || !output || !output->segments)
    return SALTS_EINVAL;
  rc = flow_jtt808_frame_header(message_id, device_id, version_2019,
                                serial, body_size, header, sizeof(header),
                                &header_size);
  if (rc != SALTS_OK) return rc;

  for (size_t i = 0u; i < header_size; ++i) checksum ^= header[i];
  for (size_t i = 0u; i < body_size; ++i) checksum ^= body[i];
  escaped_header = flow_jtt808_escaped_size(header, header_size);
  escaped_body = flow_jtt808_escaped_size(body, body_size);
  escaped_checksum = flow_jtt808_escaped_size(&checksum, 1u);
  if (escaped_header == SIZE_MAX || escaped_body == SIZE_MAX ||
      escaped_checksum == SIZE_MAX)
    return SALTS_ERANGE;
  needed_segments = body_size != 0u ? 5u : 4u;
  if (output->segment_capacity < needed_segments) return SALTS_ENOSPC;

  delimiter_buffer = mem_get_buffer(mem_global(), 2u);
  header_buffer = mem_get_buffer(mem_global(), escaped_header);
  if (body_size != 0u)
    body_buffer = mem_get_buffer(mem_global(), escaped_body);
  checksum_buffer = mem_get_buffer(mem_global(), escaped_checksum);
  if (!delimiter_buffer || !header_buffer ||
      (body_size != 0u && !body_buffer) || !checksum_buffer) {
    rc = SALTS_ENOMEM;
    goto fail;
  }

  ((uint8_t *)mem_buffer_data(delimiter_buffer))[0] = UINT8_C(0x7e);
  ((uint8_t *)mem_buffer_data(delimiter_buffer))[1] = UINT8_C(0x7e);
  mem_set_used(delimiter_buffer, 2u);

  rc = flow_jtt808_escape_write(
      header, header_size, (uint8_t *)mem_buffer_data(header_buffer),
      escaped_header, &written);
  if (rc != SALTS_OK || written != escaped_header) {
    rc = rc != SALTS_OK ? rc : SALTS_EPROTO;
    goto fail;
  }
  mem_set_used(header_buffer, written);

  if (body_size != 0u) {
    rc = flow_jtt808_escape_write(
        body, body_size, (uint8_t *)mem_buffer_data(body_buffer),
        escaped_body, &written);
    if (rc != SALTS_OK || written != escaped_body) {
      rc = rc != SALTS_OK ? rc : SALTS_EPROTO;
      goto fail;
    }
    mem_set_used(body_buffer, written);
  }

  rc = flow_jtt808_escape_write(
      &checksum, 1u, (uint8_t *)mem_buffer_data(checksum_buffer),
      escaped_checksum, &written);
  if (rc != SALTS_OK || written != escaped_checksum) {
    rc = rc != SALTS_OK ? rc : SALTS_EPROTO;
    goto fail;
  }
  mem_set_used(checksum_buffer, written);

  slices[count++] = mem_slice(delimiter_buffer, 0u, 1u);
  slices[count++] = mem_slice(header_buffer, 0u, escaped_header);
  if (body_size != 0u)
    slices[count++] = mem_slice(body_buffer, 0u, escaped_body);
  slices[count++] = mem_slice(checksum_buffer, 0u, escaped_checksum);
  slices[count++] = mem_slice(delimiter_buffer, 1u, 1u);
  for (size_t i = 0u; i < count; ++i) {
    if (!slices[i].buffer || slices[i].length == 0u) {
      rc = SALTS_EPROTO;
      goto fail;
    }
    if (total > SIZE_MAX - slices[i].length) {
      rc = SALTS_ERANGE;
      goto fail;
    }
    total += slices[i].length;
  }

  for (size_t i = 0u; i < count; ++i) {
    output->segments[i] = slices[i];
    slices[i] = (mem_slice_t){0};
  }
  output->segment_count = count;
  output->data_size = total;
  mem_buffer_release(checksum_buffer);
  mem_buffer_release(body_buffer);
  mem_buffer_release(header_buffer);
  mem_buffer_release(delimiter_buffer);
  return SALTS_OK;

fail:
  for (size_t i = 0u; i < sizeof(slices) / sizeof(slices[0]); ++i)
    mem_slice_release(&slices[i]);
  mem_buffer_release(checksum_buffer);
  mem_buffer_release(body_buffer);
  mem_buffer_release(header_buffer);
  mem_buffer_release(delimiter_buffer);
  return rc;
}

static int flow_jtt808_reply_prepare(
    void *ctx, const turbo_flow_protocol_frame_view_t *request, int status,
    char device_id[21], int *version_2019_out, uint8_t body[5],
    uint16_t *next_out) {
  uint8_t header[FLOW_JTT808_CAPTURE_SIZE] = {0};
  uint8_t checksum = 0u;
  size_t decoded_size = 0u;
  size_t phone_offset;
  size_t phone_size;
  size_t serial_offset;
  uint16_t properties;
  uint16_t next;
  int version_2019;
  int rc;
  if (!ctx || !request || !device_id || !version_2019_out || !body ||
      !next_out)
    return SALTS_EINVAL;
  rc = flow_jtt808_unescape(request, header, sizeof(header), &decoded_size,
                            &checksum);
  if (rc != SALTS_OK || checksum != 0u || decoded_size < 5u)
    return SALTS_EPROTO;
  properties = ((uint16_t)header[2] << 8u) | header[3];
  version_2019 = (properties & UINT16_C(0x4000)) != 0u;
  phone_offset = version_2019 ? 5u : 4u;
  phone_size = version_2019 ? 10u : 6u;
  serial_offset = version_2019 ? 15u : 10u;
  rc = flow_jtt808_device_id(header + phone_offset, phone_size, device_id,
                             21u);
  if (rc != SALTS_OK) return rc;
  body[0] = header[serial_offset];
  body[1] = header[serial_offset + 1u];
  body[2] = header[0];
  body[3] = header[1];
  body[4] = status == SALTS_OK ? 0u : status == SALTS_ENOTSUP ? 3u : 1u;
  next = flow_jtt808_next_serial((atomic_uint *)ctx);
  if (next == 0u) return SALTS_ERANGE;
  *version_2019_out = version_2019;
  *next_out = next;
  return SALTS_OK;
}

static int flow_jtt808_reply(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_output_t *output) {
  uint8_t body[5];
  char device_id[21];
  uint16_t next = 0u;
  int version_2019 = 0;
  int rc;
  (void)configured_version;
  rc = flow_jtt808_reply_prepare(ctx, request, status, device_id,
                                 &version_2019, body, &next);
  if (rc != SALTS_OK) return rc;
  return flow_jtt808_frame_write(UINT16_C(0x8001), device_id, version_2019,
                                 next, body, sizeof(body), output);
}

static int flow_jtt808_reply_slices(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *request, int status,
    turbo_flow_protocol_frame_slices_output_t *output) {
  uint8_t body[5];
  char device_id[21];
  uint16_t next = 0u;
  int version_2019 = 0;
  int rc;
  (void)configured_version;
  rc = flow_jtt808_reply_prepare(ctx, request, status, device_id,
                                 &version_2019, body, &next);
  if (rc != SALTS_OK) return rc;
  rc = flow_jtt808_frame_slices_write(
      UINT16_C(0x8001), device_id, version_2019, next, body, sizeof(body),
      output);
  if (rc != SALTS_OK) return rc;
  output->metadata.message_type = UINT32_C(0x8001);
  output->metadata.sequence = next;
  memcpy(output->metadata.device_id, device_id, strlen(device_id) + 1u);
  memcpy(output->metadata.operation, "platform-ack",
         sizeof("platform-ack"));
  return SALTS_OK;
}

static int flow_jtt808_message_id(const char *operation, uint16_t *out) {
  if (!operation || !out) return SALTS_EINVAL;
  if (strcmp(operation, "platform-ack") == 0) *out = UINT16_C(0x8001);
  else if (strcmp(operation, "register-response") == 0) *out = UINT16_C(0x8100);
  else if (strcmp(operation, "set-parameters") == 0) *out = UINT16_C(0x8103);
  else if (strcmp(operation, "query-parameters") == 0) *out = UINT16_C(0x8104);
  else if (strcmp(operation, "terminal-control") == 0) *out = UINT16_C(0x8105);
  else if (strcmp(operation, "location-query") == 0) *out = UINT16_C(0x8201);
  else if (strcmp(operation, "text") == 0) *out = UINT16_C(0x8300);
  else if (strcmp(operation, "call") == 0) *out = UINT16_C(0x8400);
  else if (strcmp(operation, "platform-transparent-data") == 0) *out = UINT16_C(0x8900);
  else return SALTS_ENOTSUP;
  return SALTS_OK;
}

static int flow_jtt808_encode(void *ctx, const char *configured_version,
                              const turbo_flow_protocol_command_view_t *command,
                              turbo_flow_protocol_frame_output_t *output) {
  uint16_t message_id;
  int rc;
  (void)ctx;
  (void)configured_version;
  if (!command || command->sequence == 0u || command->sequence > UINT16_MAX) return SALTS_EINVAL;
  rc = flow_jtt808_message_id(command->operation, &message_id);
  if (rc != SALTS_OK) return rc;
  return flow_jtt808_frame_write(message_id, command->device_id, 1, (uint16_t)command->sequence,
                                 command->payload, command->payload_size, output);
}

static const char *const FLOW_JTT808_VERSIONS[] = {"2019-A1"};
static const flow_protocol_plugin_descriptor_t FLOW_JTT808_DESCRIPTOR = {
    "jtt808",
    TURBO_FLOW_PROTOCOL_JTT_808,
    "2019-A1",
    FLOW_JTT808_VERSIONS,
    sizeof(FLOW_JTT808_VERSIONS) / sizeof(FLOW_JTT808_VERSIONS[0]),
    flow_jtt808_inspect,
    flow_jtt808_reply,
    flow_jtt808_encode,
    &FLOW_JTT808_NEXT_SERIAL,
    flow_jtt808_decode_semantic,
    flow_jtt808_reply_slices};

static const turbo_flow_protocol_plugin_api_t FLOW_JTT808_API = {
    sizeof(turbo_flow_protocol_plugin_api_t),
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR,
    "jtt808",
    TURBO_FLOW_PROTOCOL_JTT_808,
    TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
        TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY |
        TURBO_FLOW_PROTOCOL_CAP_COMMAND_ENCODE | TURBO_FLOW_PROTOCOL_CAP_SEMANTIC_DECODE,
    (void *)&FLOW_JTT808_DESCRIPTOR,
    flow_protocol_plugin_open,
    flow_protocol_plugin_close};

FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT("jtt808", "1.0.0", FLOW_JTT808_API)
