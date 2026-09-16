#include "../../../tests/flow_operation_fixture.h"
#include "data_bind.h"
#include "salts_error.h"
#include "tinytest.h"
#include "turbo_flow_inbox_source.h"
#include "turbo_flow_protocol_inbox.h"
#include "turbo_flow_protocol_inbox_envelope.h"
#include "turbo_flow_protocol_plugin.h"

#include <stdint.h>
#include <string.h>

typedef struct protocol_inbox_identity_probe_s {
  size_t calls;
  int status;
  int malformed_output;
  int bad_output_version;
  int empty_source_id;
  int empty_admission_id;
  char source_id[128];
  char admission_id[256];
  char correlation[256];
  uint64_t source_sequence;
  uint64_t timestamp_ns;
} protocol_inbox_identity_probe_t;

typedef struct protocol_inbox_graph_probe_s {
  size_t business_calls;
  size_t sink_calls;
} protocol_inbox_graph_probe_t;

typedef struct protocol_inbox_provider_probe_s {
  size_t admit_calls;
  int admit_status;
} protocol_inbox_provider_probe_t;

static int
protocol_inbox_identity_resolve(void *ctx,
                                const turbo_flow_protocol_inbox_identity_request_t *request,
                                turbo_flow_protocol_inbox_identity_t *identity) {
  protocol_inbox_identity_probe_t *probe = (protocol_inbox_identity_probe_t *)ctx;
  if (!probe || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION || !request->message ||
      !identity || identity->size != sizeof(*identity) ||
      identity->abi_version != TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION) {
    return SALTS_EINVAL;
  }
  probe->calls++;
  if (probe->status != SALTS_OK) return probe->status;
  identity->source_id = vstr_from_buf(probe->source_id, strlen(probe->source_id));
  identity->admission_id = vstr_from_buf(probe->admission_id, strlen(probe->admission_id));
  identity->correlation = vstr_from_buf(probe->correlation, strlen(probe->correlation));
  identity->source_sequence = probe->source_sequence;
  identity->timestamp_ns = probe->timestamp_ns;
  if (probe->malformed_output < 0) identity->size--;
  else if (probe->malformed_output > 0) identity->size++;
  if (probe->bad_output_version) identity->abi_version++;
  if (probe->empty_source_id) identity->source_id.len = 0u;
  if (probe->empty_admission_id) identity->admission_id.len = 0u;
  return SALTS_OK;
}

static int protocol_inbox_provider_admit(void *ctx, const turbo_flow_inbox_record_t *record,
                                         turbo_flow_inbox_receipt_t *receipt) {
  protocol_inbox_provider_probe_t *probe = (protocol_inbox_provider_probe_t *)ctx;
  if (!probe || !record || !receipt) return SALTS_EINVAL;
  probe->admit_calls++;
  return probe->admit_status;
}

static int protocol_inbox_provider_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  (void)ctx;
  (void)claim;
  return SALTS_ENOENT;
}

static int protocol_inbox_provider_complete(void *ctx, uint64_t record_id, uint64_t claim_token) {
  (void)ctx;
  (void)record_id;
  (void)claim_token;
  return SALTS_ENOTSUP;
}

static int protocol_inbox_provider_fail(void *ctx, uint64_t record_id, uint64_t claim_token,
                                        int status) {
  (void)ctx;
  (void)record_id;
  (void)claim_token;
  (void)status;
  return SALTS_ENOTSUP;
}

static int protocol_inbox_provider_record(void *ctx, uint64_t record_id) {
  (void)ctx;
  (void)record_id;
  return SALTS_ENOTSUP;
}

static int protocol_inbox_provider_scan_failed(void *ctx, uint64_t after_record_id,
                                               turbo_flow_inbox_failed_entry_t *entries,
                                               size_t capacity, size_t *out_count) {
  (void)ctx;
  (void)after_record_id;
  (void)entries;
  (void)capacity;
  (void)out_count;
  return SALTS_ENOTSUP;
}

static int protocol_inbox_provider_scan_history(void *ctx, uint64_t after_record_id,
                                                turbo_flow_inbox_history_entry_t *entries,
                                                size_t capacity, size_t *out_count) {
  (void)ctx;
  (void)after_record_id;
  (void)entries;
  (void)capacity;
  (void)out_count;
  return SALTS_ENOTSUP;
}

static int protocol_inbox_provider_close(void *ctx) {
  (void)ctx;
  return SALTS_OK;
}

static int protocol_inbox_provider_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  (void)ctx;
  (void)snapshot;
  return SALTS_ENOTSUP;
}

static int protocol_inbox_provider_destroy(void *ctx) {
  (void)ctx;
  return SALTS_OK;
}

static const turbo_flow_inbox_ops_v2_t protocol_inbox_provider_ops = {
    sizeof(protocol_inbox_provider_ops),  TURBO_FLOW_INBOX_API_VERSION,
    protocol_inbox_provider_admit,        protocol_inbox_provider_claim,
    protocol_inbox_provider_complete,     protocol_inbox_provider_fail,
    protocol_inbox_provider_record,       protocol_inbox_provider_record,
    protocol_inbox_provider_record,       protocol_inbox_provider_scan_failed,
    protocol_inbox_provider_scan_history, protocol_inbox_provider_close,
    protocol_inbox_provider_snapshot,     protocol_inbox_provider_destroy};

static protocol_inbox_identity_probe_t protocol_inbox_identity_probe(void) {
  protocol_inbox_identity_probe_t probe = {0};
  memcpy(probe.source_id, "socket.telemetry", sizeof("socket.telemetry"));
  memcpy(probe.admission_id, "device-7:41", sizeof("device-7:41"));
  memcpy(probe.correlation, "trace-41", sizeof("trace-41"));
  probe.source_sequence = 41u;
  probe.timestamp_ns = UINT64_C(123456789);
  return probe;
}

static turbo_flow_protocol_message_output_t protocol_inbox_message(uint8_t *payload,
                                                                   size_t payload_size) {
  turbo_flow_protocol_message_output_t message = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  message.payload = payload;
  message.payload_capacity = payload_size;
  message.payload_size = payload_size;
  message.metadata.protocol = TURBO_FLOW_PROTOCOL_COAP;
  message.metadata.direction = TURBO_FLOW_PROTOCOL_DIRECTION_UP;
  message.metadata.message_type = 17u;
  message.metadata.sequence = UINT64_C(9007199254740993);
  memcpy(message.metadata.protocol_version, "RFC7252", sizeof("RFC7252"));
  memcpy(message.metadata.device_id, "device-7", sizeof("device-7"));
  memcpy(message.metadata.operation, "telemetry", sizeof("telemetry"));
  memcpy(message.metadata.correlation_id, "coap-token-41", sizeof("coap-token-41"));
  return message;
}

static int protocol_inbox_source_inspect(void *ctx, const char *configured_version,
                                         const turbo_flow_protocol_frame_view_t *frame,
                                         turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || !frame->data || frame->data_size == 0u || !metadata) return SALTS_EPROTO;
  metadata->message_type = frame->data[0];
  metadata->sequence = frame->data_size;
  memcpy(metadata->operation, "telemetry", sizeof("telemetry"));
  memcpy(metadata->correlation_id, "source-feed", sizeof("source-feed"));
  return SALTS_OK;
}

static turbo_flow_protocol_source_admit_request_t
protocol_inbox_request(const turbo_flow_protocol_message_output_t *message) {
  turbo_flow_protocol_source_admit_request_t request =
      TURBO_FLOW_PROTOCOL_SOURCE_ADMIT_REQUEST_INIT;
  request.delivery_id = 1u;
  request.session_id = 2u;
  request.session_generation = 3u;
  request.message = message;
  return request;
}

static turbo_flow_inbox_memory_config_t protocol_inbox_memory_config(size_t max_records) {
  turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
  config.max_records = max_records;
  config.max_total_bytes = 16384u;
  config.max_record_bytes = 4096u;
  config.max_claims = max_records;
  return config;
}

static void protocol_inbox_check_vstr(vstr actual, const char *expected) {
  size_t expected_size = strlen(expected);
  check_equal(actual.len, expected_size);
  check_equal(actual.data, expected, expected_size);
}

static int protocol_inbox_envelope_decode_v1(DataBind *codec, const void *data, size_t size,
                                             ProtocolInboxEnvelope_t *envelope,
                                             DataBindError *error) {
  if (!codec || !data || size == 0u || !envelope || !error) return SALTS_EINVAL;
  if (ProtocolInboxEnvelope_from_bin(codec, envelope, data, size, error) != DATA_BIND_OK ||
      envelope->envelopeVersion != ProtocolEnvelopeVersion_V1 ||
      !ProtocolKind_is_valid(envelope->protocol) ||
      !ProtocolDirection_is_valid(envelope->direction))
    return SALTS_EPROTO;
  return SALTS_OK;
}

static int protocol_inbox_graph_message_validate(const turbo_flow_msg_t *message) {
  const turbo_flow_inbox_source_context_t *context;
  const turbo_flow_content_descriptor_t *content;
  ProtocolInboxEnvelope_t envelope;
  DataBind *codec = NULL;
  DataBindError error = DATA_BIND_ERROR_INIT;
  int status = SALTS_EPROTO;

  if (!message) return SALTS_EINVAL;
  context = turbo_flow_inbox_source_context(message);
  content = turbo_flow_msg_content_descriptor(message);
  if (!context || !content || message->id != context->record_id ||
      context->source_sequence != UINT64_C(41) || message->ts_ns != UINT64_C(123456789) ||
      message->type != 17u || content->domain != TURBO_FLOW_DOMAIN_DATA ||
      content->profile != TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA ||
      content->encoding != TURBO_FLOW_DATA_ENCODING_TBE ||
      strcmp(content->schema_name, TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_NAME) != 0 ||
      strcmp(content->type_name, TURBO_FLOW_PROTOCOL_INBOX_TYPE_NAME) != 0 ||
      content->schema_version != TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_VERSION ||
      strcmp(content->identity, TURBO_FLOW_PROTOCOL_INBOX_CONTENT_IDENTITY) != 0)
    return SALTS_EPROTO;
  if (turbo_flow_inbox_source_source_id(message).len != sizeof("socket.telemetry") - 1u ||
      memcmp(turbo_flow_inbox_source_source_id(message).data, "socket.telemetry",
             sizeof("socket.telemetry") - 1u) != 0 ||
      turbo_flow_inbox_source_admission_id(message).len != sizeof("device-7:41") - 1u ||
      memcmp(turbo_flow_inbox_source_admission_id(message).data, "device-7:41",
             sizeof("device-7:41") - 1u) != 0)
    return SALTS_EPROTO;

  ProtocolInboxEnvelope_init(&envelope);
  if (TurboFlowProtocolInbox_codec_create(&codec, &error) != DATA_BIND_OK) return SALTS_EPROTO;
  if (protocol_inbox_envelope_decode_v1(codec, message->payload.data, message->payload.len,
                                        &envelope, &error) == SALTS_OK &&
      envelope.protocol == ProtocolKind_Coap && envelope.direction == ProtocolDirection_Up &&
      envelope.messageType == 17u && envelope.sequence == sizeof((uint8_t[]){17u, 2u, 3u}) &&
      strcmp(envelope.protocolVersion, "test") == 0 && strcmp(envelope.deviceId, "device-7") == 0 &&
      strcmp(envelope.operation, "telemetry") == 0 &&
      strcmp(envelope.correlationId, "source-feed") == 0 &&
      tbe_bytes_t_size(&envelope.payload) == sizeof((uint8_t[]){17u, 2u, 3u}) &&
      memcmp(tbe_bytes_t_data_const(&envelope.payload), (uint8_t[]){17u, 2u, 3u},
             sizeof((uint8_t[]){17u, 2u, 3u})) == 0) {
    status = SALTS_OK;
  }
  data_bind_free(codec);
  ProtocolInboxEnvelope_clear(&envelope);
  return status;
}

static int protocol_inbox_graph_business(turbo_flow_msg_t *message, void *ctx) {
  protocol_inbox_graph_probe_t *probe = (protocol_inbox_graph_probe_t *)ctx;
  int rc = protocol_inbox_graph_message_validate(message);
  if (rc != SALTS_OK || !probe) return rc != SALTS_OK ? rc : SALTS_EINVAL;
  probe->business_calls++;
  return SALTS_OK;
}

static int protocol_inbox_graph_sink(turbo_flow_msg_t *message, void *ctx) {
  protocol_inbox_graph_probe_t *probe = (protocol_inbox_graph_probe_t *)ctx;
  int rc = protocol_inbox_graph_message_validate(message);
  if (rc != SALTS_OK || !probe) return rc != SALTS_OK ? rc : SALTS_EINVAL;
  probe->sink_calls++;
  return SALTS_OK;
}

static turbo_flow_t *protocol_inbox_graph_create(protocol_inbox_graph_probe_t *probe) {
  static const char dsl[] = "source protocol_records\n"
                            "stage business operation test.protocol.business\n"
                            "stage sink operation test.protocol.sink\n"
                            "stage main {\n"
                            "  protocol_records -> business -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  flow_test_operation_t business =
      flow_test_operation_init("test.protocol.business", protocol_inbox_graph_business, probe);
  flow_test_operation_t sink =
      flow_test_operation_init("test.protocol.sink", protocol_inbox_graph_sink, probe);
  if (!flow) return NULL;
  business.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  business.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  sink.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  sink.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  if (turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u) != SALTS_OK ||
      flow_test_operation_register(flow, &business) != SALTS_OK ||
      flow_test_operation_register(flow, &sink) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int protocol_inbox_adapter_create(turbo_flow_inbox_t *inbox,
                                         protocol_inbox_identity_probe_t *probe,
                                         size_t max_payload_bytes,
                                         turbo_flow_protocol_inbox_t **out) {
  static const turbo_flow_protocol_inbox_identity_ops_t identity_ops = {
      sizeof(turbo_flow_protocol_inbox_identity_ops_t), TURBO_FLOW_PROTOCOL_INBOX_ABI_VERSION,
      protocol_inbox_identity_resolve};
  turbo_flow_protocol_inbox_config_t config = TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT;
  config.inbox = inbox;
  config.identity_ops = &identity_ops;
  config.identity_ctx = probe;
  config.max_payload_bytes = max_payload_bytes;
  config.max_envelope_bytes = max_payload_bytes + TURBO_FLOW_PROTOCOL_INBOX_ENVELOPE_OVERHEAD;
  return turbo_flow_protocol_inbox_create(&config, out);
}

static void protocol_inbox_cleanup(turbo_flow_protocol_inbox_t *adapter,
                                   turbo_flow_inbox_t *inbox) {
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  turbo_flow_protocol_inbox_destroy(adapter);
  while (turbo_flow_inbox_claim(inbox, &claim) == SALTS_OK) {
    check_equal(turbo_flow_inbox_complete(inbox, &claim), SALTS_OK);
    claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  }
  check_equal(turbo_flow_inbox_close(inbox), SALTS_OK);
  check_equal(turbo_flow_inbox_destroy(inbox), SALTS_OK);
}

spec("protocol Inbox admission") {
  it("rejects every non-exact adapter and identity ABI layout") {
    uint8_t payload[] = {1u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_identity_ops_t identity_ops =
        TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_OPS_INIT;
    turbo_flow_protocol_inbox_config_t config = TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    identity_ops.resolve = protocol_inbox_identity_resolve;
    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    config.inbox = &inbox;
    config.identity_ops = &identity_ops;
    config.identity_ctx = &probe;

    config.size--;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    check_null(adapter);
    config = (turbo_flow_protocol_inbox_config_t)TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT;
    config.inbox = &inbox;
    config.identity_ops = &identity_ops;
    config.identity_ctx = &probe;
    config.size++;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    config = (turbo_flow_protocol_inbox_config_t)TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT;
    config.inbox = &inbox;
    config.identity_ops = &identity_ops;
    config.identity_ctx = &probe;
    config.abi_version++;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    config = (turbo_flow_protocol_inbox_config_t)TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT;
    config.inbox = &inbox;
    config.identity_ops = &identity_ops;
    config.identity_ctx = &probe;
    identity_ops.size--;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    identity_ops.size += 2u;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    identity_ops =
        (turbo_flow_protocol_inbox_identity_ops_t)TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_OPS_INIT;
    identity_ops.resolve = protocol_inbox_identity_resolve;
    identity_ops.abi_version++;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    identity_ops =
        (turbo_flow_protocol_inbox_identity_ops_t)TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_OPS_INIT;
    identity_ops.resolve = protocol_inbox_identity_resolve;
    config.identity_ops = &identity_ops;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_OK);

    request.size--;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    request = protocol_inbox_request(&message);
    request.size++;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    request = protocol_inbox_request(&message);
    request.abi_version++;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    request = protocol_inbox_request(&message);
    probe.status = SALTS_EIO;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EIO);
    probe.status = SALTS_OK;
    probe.malformed_output = -1;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EPROTO);
    probe.malformed_output = 1;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EPROTO);
    probe.malformed_output = 0;
    probe.bad_output_version = 1;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EPROTO);
    probe.bad_output_version = 0;
    probe.empty_source_id = 1;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EPROTO);
    probe.empty_source_id = 0;
    probe.empty_admission_id = 1;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EPROTO);
    check_equal(probe.calls, 6u);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("propagates arbitrary exact Inbox provider failures unchanged") {
    uint8_t payload[] = {1u};
    protocol_inbox_identity_probe_t identity = protocol_inbox_identity_probe();
    protocol_inbox_provider_probe_t provider = {0u, SALTS_EIO};
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_t inbox = {sizeof(inbox), TURBO_FLOW_INBOX_API_VERSION,
                                &protocol_inbox_provider_ops, &provider};
    turbo_flow_protocol_inbox_t *adapter = NULL;

    check_equal(protocol_inbox_adapter_create(&inbox, &identity, sizeof(payload), &adapter),
                SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EIO);
    check_equal(provider.admit_calls, 1u);
    check_equal(identity.calls, 1u);
    turbo_flow_protocol_inbox_destroy(adapter);
  }

  it("writes the canonical descriptor and lossless TBE envelope") {
    uint8_t payload[] = {0xdeu, 0xadu, 0xbeu, 0xefu};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    ProtocolInboxEnvelope_t envelope;
    DataBind *codec = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, sizeof(payload), &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    protocol_inbox_check_vstr(claim.record.source_id, "socket.telemetry");
    protocol_inbox_check_vstr(claim.record.admission_id, "device-7:41");
    protocol_inbox_check_vstr(claim.record.correlation, "trace-41");
    check_equal(claim.record.source_sequence, UINT64_C(41));
    check_equal(claim.record.timestamp_ns, UINT64_C(123456789));
    check_equal(claim.record.message_type, 17u);
    check_equal(claim.record.content.domain, TURBO_FLOW_DOMAIN_DATA);
    check_equal(claim.record.content.profile, TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA);
    check_equal(claim.record.content.encoding, TURBO_FLOW_DATA_ENCODING_TBE);
    check_bits(claim.record.content.flags, TURBO_FLOW_CONTENT_SCHEMA_DECLARED);
    check_equal(claim.record.content.media_type, TURBO_FLOW_PROTOCOL_INBOX_MEDIA_TYPE);
    check_equal(claim.record.content.schema_name, TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_NAME);
    check_equal(claim.record.content.type_name, TURBO_FLOW_PROTOCOL_INBOX_TYPE_NAME);
    check_equal(claim.record.content.schema_version, TURBO_FLOW_PROTOCOL_INBOX_SCHEMA_VERSION);
    check_equal(claim.record.content.identity, TURBO_FLOW_PROTOCOL_INBOX_CONTENT_IDENTITY);
    {
      static const uint8_t expected_fixed[] = {
          0x01u, 0x00u, 0x00u, 0x00u, 0x02u, 0x00u, 0x00u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u,
          0x11u, 0x00u, 0x00u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u, 0x00u, 0x20u, 0x00u};
      size_t expected_size = TURBO_FLOW_PROTOCOL_INBOX_TBE_FIXED_BYTES +
                             TURBO_FLOW_PROTOCOL_INBOX_TBE_VARIABLE_FIELDS *
                                 TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES +
                             sizeof("RFC7252") - 1u + sizeof("device-7") - 1u +
                             sizeof("telemetry") - 1u + sizeof("coap-token-41") - 1u +
                             sizeof(payload);
      check_equal(ProtocolInboxEnvelope_BLOCK_LENGTH, TURBO_FLOW_PROTOCOL_INBOX_TBE_FIXED_BYTES);
      check_equal(claim.record.payload.len, expected_size);
      check_equal(claim.record.payload.data, expected_fixed, sizeof(expected_fixed));
    }

    ProtocolInboxEnvelope_init(&envelope);
    check_equal(TurboFlowProtocolInbox_codec_create(&codec, &error), DATA_BIND_OK);
    check_equal(ProtocolInboxEnvelope_from_bin(codec, &envelope, claim.record.payload.data,
                                               claim.record.payload.len, &error),
                DATA_BIND_OK);
    check_equal(envelope.envelopeVersion, ProtocolEnvelopeVersion_V1);
    check_equal(envelope.protocol, ProtocolKind_Coap);
    check_equal(envelope.direction, ProtocolDirection_Up);
    check_equal(envelope.messageType, 17u);
    check_equal(envelope.sequence, UINT64_C(9007199254740993));
    check_equal(envelope.protocolVersion, "RFC7252");
    check_equal(envelope.deviceId, "device-7");
    check_equal(envelope.operation, "telemetry");
    check_equal(envelope.correlationId, "coap-token-41");
    check_equal(tbe_bytes_t_size(&envelope.payload), sizeof(payload));
    check_equal(tbe_bytes_t_data_const(&envelope.payload), payload, sizeof(payload));
    data_bind_free(codec);
    ProtocolInboxEnvelope_clear(&envelope);

    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("rejects truncated malformed future and native-layout durable envelopes") {
    uint8_t payload[] = {0xdeu, 0xadu, 0xbeu, 0xefu};
    uint8_t encoded[4096];
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    ProtocolInboxEnvelope_t envelope;
    DataBind *codec = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, sizeof(payload), &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_true(claim.record.payload.len < sizeof(encoded));
    memcpy(encoded, claim.record.payload.data, claim.record.payload.len);
    check_equal(TurboFlowProtocolInbox_codec_create(&codec, &error), DATA_BIND_OK);

    ProtocolInboxEnvelope_init(&envelope);
    check_not_equal(ProtocolInboxEnvelope_from_bin(codec, &envelope, encoded,
                                                   claim.record.payload.len - 1u, &error),
                    DATA_BIND_OK);
    ProtocolInboxEnvelope_clear(&envelope);

    memset(encoded + TURBO_FLOW_PROTOCOL_INBOX_TBE_FIXED_BYTES, 0xff,
           TURBO_FLOW_PROTOCOL_INBOX_TBE_LENGTH_BYTES);
    ProtocolInboxEnvelope_init(&envelope);
    check_not_equal(
        ProtocolInboxEnvelope_from_bin(codec, &envelope, encoded, claim.record.payload.len, &error),
        DATA_BIND_OK);
    ProtocolInboxEnvelope_clear(&envelope);

    memcpy(encoded, claim.record.payload.data, claim.record.payload.len);
    encoded[0] = 2u;
    encoded[1] = 0u;
    encoded[2] = 0u;
    encoded[3] = 0u;
    ProtocolInboxEnvelope_init(&envelope);
    check_equal(protocol_inbox_envelope_decode_v1(codec, encoded, claim.record.payload.len,
                                                  &envelope, &error),
                SALTS_EPROTO);
    ProtocolInboxEnvelope_clear(&envelope);

    ProtocolInboxEnvelope_init(&envelope);
    check_true(sizeof(message) <= sizeof(encoded));
    memcpy(encoded, &message, sizeof(message));
    check_equal(
        protocol_inbox_envelope_decode_v1(codec, encoded, sizeof(message), &envelope, &error),
        SALTS_EPROTO);
    ProtocolInboxEnvelope_clear(&envelope);

    data_bind_free(codec);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("calls the identity provider once per decoded frame and leaves Inbox unclaimed") {
    uint8_t first_payload[] = {1u};
    uint8_t second_payload[] = {2u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t first =
        protocol_inbox_message(first_payload, sizeof(first_payload));
    turbo_flow_protocol_message_output_t second =
        protocol_inbox_message(second_payload, sizeof(second_payload));
    turbo_flow_protocol_source_admit_request_t first_request = protocol_inbox_request(&first);
    turbo_flow_protocol_source_admit_request_t second_request = protocol_inbox_request(&second);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(2u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, 1u, &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &first_request), SALTS_OK);
    memcpy(probe.admission_id, "device-7:42", sizeof("device-7:42"));
    probe.source_sequence = 42u;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &second_request), SALTS_OK);
    check_equal(probe.calls, 2u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, 2u);
    check_equal(snapshot.pending_records, 2u);
    check_equal(snapshot.in_flight_claims, 0u);
    check_equal(snapshot.completed, UINT64_C(0));
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("runs one admitted protocol envelope only after explicit Inbox Source demand") {
    const uint8_t frame[] = {17u, 2u, 3u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    protocol_inbox_graph_probe_t graph_probe = {0};
    turbo_flow_protocol_open_request_t open = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
    turbo_flow_protocol_codec_ops_t codec_ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
    turbo_flow_protocol_source_config_t source_config = TURBO_FLOW_PROTOCOL_SOURCE_CONFIG_INIT;
    turbo_flow_protocol_source_ops_t source_ops = TURBO_FLOW_PROTOCOL_SOURCE_OPS_INIT;
    turbo_flow_protocol_source_session_open_request_t session =
        TURBO_FLOW_PROTOCOL_SOURCE_SESSION_OPEN_REQUEST_INIT;
    turbo_flow_protocol_source_feed_result_t feed = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_inbox_source_config_t driver_config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
    turbo_flow_inbox_source_result_t driver_result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *driver = NULL;
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow = NULL;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, sizeof(frame), &adapter), SALTS_OK);
    open.protocol = TURBO_FLOW_PROTOCOL_COAP;
    open.protocol_version = "test";
    open.max_frame_size = 64u;
    codec_ops.inspect = protocol_inbox_source_inspect;
    check_equal(turbo_flow_protocol_create(&open, "protocol-inbox-test", "test",
                                           TURBO_FLOW_PROTOCOL_CAP_INGRESS |
                                               TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                                               TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
                                           &codec_ops, NULL, &protocol),
                SALTS_OK);
    source_config.max_sessions = 1u;
    source_config.max_frame_size = 64u;
    source_config.max_buffered_bytes = 128u;
    source_ops.admit = turbo_flow_protocol_inbox_admit;
    check_equal(
        turbo_flow_protocol_source_create(protocol, &source_config, &source_ops, adapter, &source),
        SALTS_OK);
    session.session_id = 7u;
    session.generation = 1u;
    session.device_id = "device-7";
    session.protocol_version = "test";
    check_equal(turbo_flow_protocol_source_session_open(source, &session), SALTS_OK);
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 7u, 1u, frame, sizeof(frame), &feed),
        SALTS_OK);
    check_equal(feed.frames_admitted, 1u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, 1u);
    check_equal(snapshot.in_flight_claims, 0u);
    check_equal(snapshot.completed, UINT64_C(0));
    check_equal(graph_probe.business_calls, 0u);
    check_equal(graph_probe.sink_calls, 0u);

    check_true(cflow_scheduler_inline_init(&scheduler));
    flow = protocol_inbox_graph_create(&graph_probe);
    check_not_null(flow);
    driver_config.inbox = &inbox;
    driver_config.flow = flow;
    driver_config.graph_source_name = "protocol_records";
    driver_config.scheduler = &scheduler;
    driver_config.max_message_bytes = 4096u;
    check_equal(turbo_flow_inbox_source_create(&driver_config, &driver), SALTS_OK);
    check_equal(turbo_flow_inbox_source_request(driver), SALTS_OK);
    check_equal(graph_probe.business_calls, 1u);
    check_equal(graph_probe.sink_calls, 1u);
    check_equal(turbo_flow_inbox_source_poll(driver, &driver_result), SALTS_OK);
    check_equal(driver_result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(driver_result.graph_status, SALTS_OK);
    check_equal(driver_result.settlement_status, SALTS_OK);
    snapshot = (turbo_flow_inbox_snapshot_t)TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, 0u);
    check_equal(snapshot.in_flight_claims, 0u);
    check_equal(snapshot.completed, UINT64_C(1));

    check_equal(turbo_flow_inbox_source_destroy(driver), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    cflow_scheduler_destroy(&scheduler);

    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("preserves stable replay idempotency and rejects conflicting content") {
    uint8_t original_payload[] = {3u};
    uint8_t conflicting_payload[] = {4u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t original =
        protocol_inbox_message(original_payload, sizeof(original_payload));
    turbo_flow_protocol_message_output_t conflicting =
        protocol_inbox_message(conflicting_payload, sizeof(conflicting_payload));
    turbo_flow_protocol_source_admit_request_t original_request = protocol_inbox_request(&original);
    turbo_flow_protocol_source_admit_request_t conflicting_request =
        protocol_inbox_request(&conflicting);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, 1u, &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &original_request), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &original_request), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &conflicting_request), SALTS_EPROTO);
    check_equal(probe.calls, 3u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, 1u);
    check_equal(snapshot.admitted, UINT64_C(1));
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("propagates Inbox capacity errors exactly") {
    uint8_t payload[] = {5u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, 1u, &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_OK);
    memcpy(probe.admission_id, "device-7:42", sizeof("device-7:42"));
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_ENOSPC);
    check_equal(probe.calls, 2u);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("propagates closed Inbox admission errors exactly") {
    uint8_t payload[] = {5u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, 1u, &adapter), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_ESHUTDOWN);
    check_equal(probe.calls, 1u);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("copies borrowed identity and payload bytes before the callback returns") {
    uint8_t payload[] = {6u, 7u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    ProtocolInboxEnvelope_t envelope;
    DataBind *codec = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, sizeof(payload), &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_OK);
    memset(probe.source_id, 'x', sizeof(probe.source_id) - 1u);
    probe.source_id[sizeof(probe.source_id) - 1u] = '\0';
    memset(probe.admission_id, 'y', sizeof(probe.admission_id) - 1u);
    probe.admission_id[sizeof(probe.admission_id) - 1u] = '\0';
    memset(probe.correlation, 'z', sizeof(probe.correlation) - 1u);
    probe.correlation[sizeof(probe.correlation) - 1u] = '\0';
    memset(payload, 0xff, sizeof(payload));

    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    protocol_inbox_check_vstr(claim.record.source_id, "socket.telemetry");
    protocol_inbox_check_vstr(claim.record.admission_id, "device-7:41");
    protocol_inbox_check_vstr(claim.record.correlation, "trace-41");
    ProtocolInboxEnvelope_init(&envelope);
    check_equal(TurboFlowProtocolInbox_codec_create(&codec, &error), DATA_BIND_OK);
    check_equal(ProtocolInboxEnvelope_from_bin(codec, &envelope, claim.record.payload.data,
                                               claim.record.payload.len, &error),
                DATA_BIND_OK);
    {
      const uint8_t expected[] = {6u, 7u};
      check_equal(tbe_bytes_t_data_const(&envelope.payload), expected, sizeof(expected));
    }
    data_bind_free(codec);
    ProtocolInboxEnvelope_clear(&envelope);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("fails fast at configured payload, envelope, and metadata bounds") {
    uint8_t payload[] = {1u, 2u, 3u, 4u, 5u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_identity_ops_t identity_ops =
        TURBO_FLOW_PROTOCOL_INBOX_IDENTITY_OPS_INIT;
    turbo_flow_protocol_inbox_config_t config = TURBO_FLOW_PROTOCOL_INBOX_CONFIG_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    identity_ops.resolve = protocol_inbox_identity_resolve;
    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    config.inbox = &inbox;
    config.identity_ops = &identity_ops;
    config.identity_ctx = &probe;
    config.max_payload_bytes = 4u;
    config.max_envelope_bytes =
        config.max_payload_bytes + TURBO_FLOW_PROTOCOL_INBOX_ENVELOPE_OVERHEAD - 1u;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_EINVAL);
    check_null(adapter);
    config.max_envelope_bytes++;
    check_equal(turbo_flow_protocol_inbox_create(&config, &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EMSGSIZE);
    message.payload_size = 4u;
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_OK);
    memset(message.metadata.protocol_version, 'v', sizeof(message.metadata.protocol_version));
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    check_equal(probe.calls, 1u);
    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("rejects unnormalized metadata before resolving durable identity") {
    uint8_t payload[] = {1u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;

    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, sizeof(payload), &adapter), SALTS_OK);

    message.metadata.protocol_version[0] = '\0';
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    memcpy(message.metadata.protocol_version, "RFC7252", sizeof("RFC7252"));
    message.metadata.device_id[0] = '\0';
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    memcpy(message.metadata.device_id, "device-7", sizeof("device-7"));
    message.metadata.operation[0] = '\0';
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    memcpy(message.metadata.operation, "telemetry", sizeof("telemetry"));
    message.metadata.operation[1] = '/';
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    memcpy(message.metadata.operation, "telemetry", sizeof("telemetry"));
    message.metadata.device_id[1] = '+';
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    memcpy(message.metadata.device_id, "device-7", sizeof("device-7"));
    message.metadata.correlation_id[1] = '\n';
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_EINVAL);
    check_equal(probe.calls, 0u);

    protocol_inbox_cleanup(adapter, &inbox);
  }

  it("preserves printable protocol versions and opaque correlation text") {
    uint8_t payload[] = {1u};
    protocol_inbox_identity_probe_t probe = protocol_inbox_identity_probe();
    turbo_flow_protocol_message_output_t message = protocol_inbox_message(payload, sizeof(payload));
    turbo_flow_protocol_source_admit_request_t request = protocol_inbox_request(&message);
    turbo_flow_inbox_memory_config_t memory = protocol_inbox_memory_config(1u);
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_protocol_inbox_t *adapter = NULL;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    ProtocolInboxEnvelope_t envelope;
    DataBind *codec = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;

    memcpy(message.metadata.protocol_version, "proto/1 draft", sizeof("proto/1 draft"));
    memcpy(message.metadata.correlation_id, "site/a+b#c", sizeof("site/a+b#c"));
    check_equal(turbo_flow_inbox_memory_create(&memory, &inbox), SALTS_OK);
    check_equal(protocol_inbox_adapter_create(&inbox, &probe, sizeof(payload), &adapter), SALTS_OK);
    check_equal(turbo_flow_protocol_inbox_admit(adapter, &request), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    ProtocolInboxEnvelope_init(&envelope);
    check_equal(TurboFlowProtocolInbox_codec_create(&codec, &error), DATA_BIND_OK);
    check_equal(ProtocolInboxEnvelope_from_bin(codec, &envelope, claim.record.payload.data,
                                               claim.record.payload.len, &error),
                DATA_BIND_OK);
    check_equal(envelope.protocolVersion, "proto/1 draft");
    check_equal(envelope.correlationId, "site/a+b#c");
    data_bind_free(codec);
    ProtocolInboxEnvelope_clear(&envelope);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    protocol_inbox_cleanup(adapter, &inbox);
  }
}
