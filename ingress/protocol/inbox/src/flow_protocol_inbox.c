#include "turbo_flow_protocol_inbox.h"

#include "salts_error.h"
#include "turbo_flow_protocol_inbox_envelope.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_protocol_inbox_s {
  turbo_flow_inbox_t *inbox;
  turbo_flow_protocol_inbox_identity_ops_t identity_ops;
  void *identity_ctx;
  uint8_t *scratch;
  size_t max_payload_bytes;
  size_t max_envelope_bytes;
};

static int flow_protocol_inbox_inbox_valid(const turbo_flow_inbox_t *inbox) {
  const turbo_flow_inbox_ops_v2_t *ops;
  if (!inbox || inbox->size != sizeof(*inbox) || inbox->version != TURBO_FLOW_INBOX_API_VERSION ||
      !inbox->ops || !inbox->ctx) {
    return 0;
  }
  ops = inbox->ops;
  return ops->size == sizeof(*ops) && ops->version == TURBO_FLOW_INBOX_API_VERSION && ops->admit &&
         ops->claim && ops->complete && ops->fail && ops->retry && ops->discard && ops->forget &&
         ops->scan_failed && ops->scan_history && ops->close && ops->snapshot && ops->destroy;
}

static int flow_protocol_inbox_text_size(const char *text, size_t capacity, size_t *out) {
  size_t length = 0u;
  if (!text || !out || capacity == 0u) return SALTS_EINVAL;
  while (length < capacity && text[length] != '\0')
    ++length;
  if (length == capacity) return SALTS_EINVAL;
  *out = length;
  return SALTS_OK;
}

static int flow_protocol_inbox_segment_valid(const char *text, size_t length) {
  if (!text || length == 0u) return 0;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char c = (unsigned char)text[index];
    if (c <= 0x20u || c >= 0x7fu || c == '/' || c == '+' || c == '#') return 0;
  }
  return 1;
}

static int flow_protocol_inbox_text_valid(const char *text, size_t length, int allow_empty) {
  if (!text || (!allow_empty && length == 0u)) return 0;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char c = (unsigned char)text[index];
    if (c < 0x20u || c >= 0x7fu) return 0;
  }
  return 1;
}

static int flow_protocol_inbox_message_validate(const turbo_flow_protocol_message_output_t *message,
                                                size_t max_payload_bytes,
                                                size_t *protocol_version_size,
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
      metadata->direction > TURBO_FLOW_PROTOCOL_DIRECTION_DOWN) {
    return SALTS_EINVAL;
  }
  rc = flow_protocol_inbox_text_size(metadata->protocol_version, sizeof(metadata->protocol_version),
                                     protocol_version_size);
  if (rc != SALTS_OK ||
      !flow_protocol_inbox_text_valid(metadata->protocol_version, *protocol_version_size, 0))
    return SALTS_EINVAL;
  rc = flow_protocol_inbox_text_size(metadata->device_id, sizeof(metadata->device_id),
                                     device_id_size);
  if (rc != SALTS_OK || !flow_protocol_inbox_segment_valid(metadata->device_id, *device_id_size))
    return SALTS_EINVAL;
  rc = flow_protocol_inbox_text_size(metadata->operation, sizeof(metadata->operation),
                                     operation_size);
  if (rc != SALTS_OK || !flow_protocol_inbox_segment_valid(metadata->operation, *operation_size))
    return SALTS_EINVAL;
  rc = flow_protocol_inbox_text_size(metadata->correlation_id, sizeof(metadata->correlation_id),
                                     correlation_id_size);
  if (rc != SALTS_OK) return rc;
  if (!flow_protocol_inbox_text_valid(metadata->correlation_id, *correlation_id_size, 1))
    return SALTS_EINVAL;
  return SALTS_OK;
}

static int
flow_protocol_inbox_identity_validate(const turbo_flow_protocol_inbox_identity_t *identity) {
  if (!identity || identity->size != sizeof(*identity) ||
      identity->abi_version != TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION) {
    return SALTS_EPROTO;
  }
  if (!identity->source_id.data || identity->source_id.len == 0u ||
      identity->source_id.len > TURBO_FLOW_PROTOCOL_INBOX_SOURCE_ID_MAX ||
      !identity->admission_id.data || identity->admission_id.len == 0u ||
      identity->admission_id.len > TURBO_FLOW_PROTOCOL_INBOX_ADMISSION_ID_MAX ||
      (identity->correlation.len != 0u && !identity->correlation.data) ||
      identity->correlation.len > TURBO_FLOW_PROTOCOL_INBOX_CORRELATION_MAX) {
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int flow_protocol_inbox_required_size(size_t payload_size, size_t protocol_version_size,
                                             size_t device_id_size, size_t operation_size,
                                             size_t correlation_id_size, size_t *out) {
  size_t required = TURBO_FLOW_PROTOCOL_INBOX_TBE_FIXED_BYTES;
  const size_t lengths[] = {protocol_version_size, device_id_size, operation_size,
                            correlation_id_size, payload_size};
  if (!out) return SALTS_EINVAL;
  for (size_t index = 0u; index < TURBO_FLOW_PROTOCOL_INBOX_TBE_VARIABLE_FIELDS; ++index) {
    if (lengths[index] > UINT32_MAX ||
        required > SIZE_MAX - TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES ||
        lengths[index] > SIZE_MAX - required - TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES) {
      return SALTS_EMSGSIZE;
    }
    required += TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES + lengths[index];
  }
  *out = required;
  return SALTS_OK;
}

static int flow_protocol_inbox_encode(turbo_flow_protocol_inbox_t *adapter,
                                      const turbo_flow_protocol_message_output_t *message,
                                      size_t protocol_version_size, size_t device_id_size,
                                      size_t operation_size, size_t correlation_id_size,
                                      size_t required_size) {
  const turbo_flow_protocol_metadata_t *metadata = &message->metadata;
  ProtocolInboxEnvelope_builder_t builder;
  if (required_size > adapter->max_envelope_bytes ||
      !ProtocolInboxEnvelope_builder_bind(&builder, adapter->scratch, required_size) ||
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
      !ProtocolInboxEnvelope_payload_set(&builder, message->payload, message->payload_size)) {
    return SALTS_EMSGSIZE;
  }
  return SALTS_OK;
}

int turbo_flow_protocol_inbox_create(const turbo_flow_protocol_inbox_config_t *config,
                                     turbo_flow_protocol_inbox_t **out) {
  turbo_flow_protocol_inbox_t *adapter;
  size_t required_capacity;
  if (out) *out = NULL;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION ||
      !flow_protocol_inbox_inbox_valid(config->inbox) || !config->identity_ops ||
      config->identity_ops->size != sizeof(*config->identity_ops) ||
      config->identity_ops->abi_version != TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION ||
      !config->identity_ops->resolve || !out || config->max_payload_bytes == 0u ||
      config->max_payload_bytes > UINT32_MAX ||
      config->max_payload_bytes > SIZE_MAX - TURBO_FLOW_PROTOCOL_INBOX_ENVELOPE_OVERHEAD) {
    return SALTS_EINVAL;
  }
  required_capacity = config->max_payload_bytes + TURBO_FLOW_PROTOCOL_INBOX_ENVELOPE_OVERHEAD;
  if (config->max_envelope_bytes < required_capacity) return SALTS_EINVAL;
  adapter = (turbo_flow_protocol_inbox_t *)calloc(1u, sizeof(*adapter));
  if (!adapter) return SALTS_ENOMEM;
  adapter->scratch = (uint8_t *)malloc(config->max_envelope_bytes);
  if (!adapter->scratch) {
    free(adapter);
    return SALTS_ENOMEM;
  }
  adapter->inbox = config->inbox;
  adapter->identity_ops = *config->identity_ops;
  adapter->identity_ctx = config->identity_ctx;
  adapter->max_payload_bytes = config->max_payload_bytes;
  adapter->max_envelope_bytes = config->max_envelope_bytes;
  *out = adapter;
  return SALTS_OK;
}

void turbo_flow_protocol_inbox_destroy(turbo_flow_protocol_inbox_t *adapter) {
  if (!adapter) return;
  free(adapter->scratch);
  free(adapter);
}

int turbo_flow_protocol_inbox_admit(void *ctx,
                                    const turbo_flow_protocol_source_admit_request_t *request) {
  turbo_flow_protocol_inbox_t *adapter = (turbo_flow_protocol_inbox_t *)ctx;
  turbo_flow_protocol_inbox_identity_request_t identity_request =
      TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_REQUEST_INIT;
  turbo_flow_protocol_inbox_identity_t identity = TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_INIT;
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
  size_t protocol_version_size = 0u;
  size_t device_id_size = 0u;
  size_t operation_size = 0u;
  size_t correlation_id_size = 0u;
  size_t required_size = 0u;
  int rc;
  if (!adapter || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION ||
      request->delivery_id == 0u || request->session_id == 0u ||
      request->session_generation == 0u || !request->message) {
    return SALTS_EINVAL;
  }
  rc = flow_protocol_inbox_message_validate(request->message, adapter->max_payload_bytes,
                                            &protocol_version_size, &device_id_size,
                                            &operation_size, &correlation_id_size);
  if (rc != SALTS_OK) return rc;
  identity_request.message = request->message;
  rc = adapter->identity_ops.resolve(adapter->identity_ctx, &identity_request, &identity);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_inbox_identity_validate(&identity);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_inbox_required_size(request->message->payload_size, protocol_version_size,
                                         device_id_size, operation_size, correlation_id_size,
                                         &required_size);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_inbox_encode(adapter, request->message, protocol_version_size, device_id_size,
                                  operation_size, correlation_id_size, required_size);
  if (rc != SALTS_OK) return rc;

  turbo_flow_inbox_record_init(&record);
  record.source_id = identity.source_id;
  record.admission_id = identity.admission_id;
  record.source_sequence = identity.source_sequence;
  record.timestamp_ns = identity.timestamp_ns;
  record.message_type = request->message->metadata.message_type;
  record.correlation = identity.correlation;
  record.payload = vstr_from_buf((const char *)adapter->scratch, required_size);
  rc = turbo_flow_content_descriptor_init(
      &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA,
      TURBO_FLOW_DATA_ENCODING_TBE, TURBO_FLOW_PROTOCOL_INBOX_MEDIA_TYPE,
      TURBO_FLOW_PROTOCOL_INBOX_CONTENT_IDENTITY);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(
      &record.content, TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_NAME, TURBO_FLOW_PROTOCOL_INBOX_TYPE_NAME,
      TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_VERSION);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_inbox_admit(adapter->inbox, &record, &receipt);
}
