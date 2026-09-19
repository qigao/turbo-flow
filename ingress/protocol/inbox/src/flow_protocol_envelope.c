#include "flow_protocol_envelope_internal.h"

#include "salts_error.h"
#include "turbo_flow_protocol_inbox_envelope.h"

#include <stdint.h>
#include <string.h>

static int envelope_text_size(const char *text, size_t capacity, size_t *out) {
  size_t length = 0u;
  if (!text || !out || capacity == 0u) return SALTS_EINVAL;
  while (length < capacity && text[length] != '\0') ++length;
  if (length == capacity) return SALTS_EINVAL;
  *out = length;
  return SALTS_OK;
}

static int envelope_segment_valid(const char *text, size_t length) {
  if (!text || length == 0u) return 0;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char c = (unsigned char)text[index];
    if (c <= 0x20u || c >= 0x7fu || c == '/' || c == '+' || c == '#') return 0;
  }
  return 1;
}

static int envelope_text_valid(const char *text, size_t length, int allow_empty) {
  if (!text || (!allow_empty && length == 0u)) return 0;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char c = (unsigned char)text[index];
    if (c < 0x20u || c >= 0x7fu) return 0;
  }
  return 1;
}

static int envelope_message_validate(const turbo_flow_protocol_message_output_t *message,
                                     size_t max_payload_bytes, size_t *protocol_version_size,
                                     size_t *device_id_size, size_t *operation_size,
                                     size_t *correlation_id_size) {
  const turbo_flow_protocol_metadata_t *metadata;
  int rc;
  if (!message || message->size != sizeof(*message) ||
      message->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      (!message->payload && message->payload_size != 0u) ||
      message->payload_size > message->payload_capacity)
    return SALTS_EINVAL;
  if (message->payload_size > max_payload_bytes) return SALTS_EMSGSIZE;
  metadata = &message->metadata;
  if (metadata->size != sizeof(*metadata) ||
      metadata->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      metadata->protocol < TURBO_FLOW_PROTOCOL_MQTT_SN ||
      metadata->protocol > TURBO_FLOW_PROTOCOL_JTT_808 ||
      metadata->direction < TURBO_FLOW_PROTOCOL_DIRECTION_UP ||
      metadata->direction > TURBO_FLOW_PROTOCOL_DIRECTION_DOWN)
    return SALTS_EINVAL;
  rc = envelope_text_size(metadata->protocol_version, sizeof(metadata->protocol_version),
                          protocol_version_size);
  if (rc != SALTS_OK ||
      !envelope_text_valid(metadata->protocol_version, *protocol_version_size, 0))
    return SALTS_EINVAL;
  rc = envelope_text_size(metadata->device_id, sizeof(metadata->device_id), device_id_size);
  if (rc != SALTS_OK || !envelope_segment_valid(metadata->device_id, *device_id_size))
    return SALTS_EINVAL;
  rc = envelope_text_size(metadata->operation, sizeof(metadata->operation), operation_size);
  if (rc != SALTS_OK || !envelope_segment_valid(metadata->operation, *operation_size))
    return SALTS_EINVAL;
  rc = envelope_text_size(metadata->correlation_id, sizeof(metadata->correlation_id),
                          correlation_id_size);
  if (rc != SALTS_OK ||
      !envelope_text_valid(metadata->correlation_id, *correlation_id_size, 1))
    return SALTS_EINVAL;
  return SALTS_OK;
}

static int envelope_required_size(size_t payload_size, size_t protocol_version_size,
                                  size_t device_id_size, size_t operation_size,
                                  size_t correlation_id_size, size_t *out) {
  size_t required = TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_FIXED_BYTES;
  const size_t lengths[] = {protocol_version_size, device_id_size, operation_size,
                            correlation_id_size, payload_size};
  if (!out) return SALTS_EINVAL;
  for (size_t index = 0u; index < TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_VARIABLE_FIELDS; ++index) {
    if (lengths[index] > UINT32_MAX ||
        required > SIZE_MAX - TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_LENGTH_BYTES ||
        lengths[index] > SIZE_MAX - required - TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_LENGTH_BYTES)
      return SALTS_EMSGSIZE;
    required += TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_LENGTH_BYTES + lengths[index];
  }
  *out = required;
  return SALTS_OK;
}

int flow_protocol_envelope_encode(const turbo_flow_protocol_message_output_t *message,
                                  size_t max_payload_bytes, void *buffer, size_t capacity,
                                  size_t *written) {
  const turbo_flow_protocol_metadata_t *metadata;
  ProtocolInboxEnvelope_builder_t builder;
  size_t protocol_version_size = 0u;
  size_t device_id_size = 0u;
  size_t operation_size = 0u;
  size_t correlation_id_size = 0u;
  size_t required_size = 0u;
  int rc;
  if (written) *written = 0u;
  if (!buffer || capacity == 0u || !written || max_payload_bytes == 0u)
    return SALTS_EINVAL;
  rc = envelope_message_validate(message, max_payload_bytes, &protocol_version_size,
                                 &device_id_size, &operation_size, &correlation_id_size);
  if (rc != SALTS_OK) return rc;
  rc = envelope_required_size(message->payload_size, protocol_version_size, device_id_size,
                              operation_size, correlation_id_size, &required_size);
  if (rc != SALTS_OK) return rc;
  if (required_size > capacity) return SALTS_EMSGSIZE;
  metadata = &message->metadata;
  if (!ProtocolInboxEnvelope_builder_bind(&builder, buffer, required_size) ||
      !ProtocolInboxEnvelope_envelopeVersion_set(&builder, ProtocolEnvelopeVersion_V1) ||
      !ProtocolInboxEnvelope_protocol_set(&builder, (ProtocolKind_t)metadata->protocol) ||
      !ProtocolInboxEnvelope_direction_set(&builder, (ProtocolDirection_t)metadata->direction) ||
      !ProtocolInboxEnvelope_messageType_set(&builder, metadata->message_type) ||
      !ProtocolInboxEnvelope_sequence_set(&builder, metadata->sequence) ||
      !ProtocolInboxEnvelope_protocolVersion_set(&builder, metadata->protocol_version,
                                                 protocol_version_size) ||
      !ProtocolInboxEnvelope_deviceId_set(&builder, metadata->device_id, device_id_size) ||
      !ProtocolInboxEnvelope_operation_set(&builder, metadata->operation, operation_size) ||
      !ProtocolInboxEnvelope_correlationId_set(&builder, metadata->correlation_id,
                                               correlation_id_size) ||
      !ProtocolInboxEnvelope_payload_set(&builder, message->payload, message->payload_size))
    return SALTS_EMSGSIZE;
  *written = required_size;
  return SALTS_OK;
}

int flow_protocol_envelope_content_descriptor(turbo_flow_content_descriptor_t *content) {
  int rc;
  if (!content) return SALTS_EINVAL;
  rc = turbo_flow_content_descriptor_init(
      content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA,
      TURBO_FLOW_DATA_ENCODING_TBE, TURBO_FLOW_PROTOCOL_ENVELOPE_MEDIA_TYPE,
      TURBO_FLOW_PROTOCOL_ENVELOPE_CONTENT_IDENTITY);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_content_descriptor_declare_schema(
      content, TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_NAME, TURBO_FLOW_PROTOCOL_ENVELOPE_TYPE_NAME,
      TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_VERSION);
}
