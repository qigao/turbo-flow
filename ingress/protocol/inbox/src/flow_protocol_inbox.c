#include "turbo_flow_protocol_inbox.h"

#include "flow_protocol_envelope_internal.h"
#include "salts_error.h"

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
  size_t required_size = 0u;
  int rc;
  if (!adapter || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION ||
      request->delivery_id == 0u || request->session_id == 0u ||
      request->session_generation == 0u || !request->message) {
    return SALTS_EINVAL;
  }

  identity_request.message = request->message;
  rc = adapter->identity_ops.resolve(adapter->identity_ctx, &identity_request, &identity);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_inbox_identity_validate(&identity);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_envelope_encode(request->message, adapter->max_payload_bytes,
                                     adapter->scratch, adapter->max_envelope_bytes,
                                     &required_size);
  if (rc != SALTS_OK) return rc;

  turbo_flow_inbox_record_init(&record);
  record.source_id = identity.source_id;
  record.admission_id = identity.admission_id;
  record.source_sequence = identity.source_sequence;
  record.timestamp_ns = identity.timestamp_ns;
  record.message_type = request->message->metadata.message_type;
  record.correlation = identity.correlation;
  record.payload = vstr_from_buf((const char *)adapter->scratch, required_size);
  rc = flow_protocol_envelope_content_descriptor(&record.content);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_inbox_admit(adapter->inbox, &record, &receipt);
}
