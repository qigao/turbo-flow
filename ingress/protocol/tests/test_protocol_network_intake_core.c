#include "tinytest.h"

#include "flow_protocol_network_intake_internal.h"
#include "turbo_flow_cnet.h"
#include "turbo_flow_protocol_plugin.h"

#include <salts/clock.h>

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

typedef struct intake_completion_probe_s {
  atomic_size_t calls;
  atomic_int status;
} intake_completion_probe_t;

typedef struct intake_semantic_probe_s {
  size_t decode_calls;
} intake_semantic_probe_t;

typedef struct intake_mapper_probe_s {
  size_t calls;
  int status;
} intake_mapper_probe_t;

static int intake_test_jtt_inspect(void *ctx, const char *configured_version,
                                   const turbo_flow_protocol_frame_view_t *frame,
                                   turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || frame->data_size < 3u || frame->data[0] != 0x7eu ||
      frame->data[frame->data_size - 1u] != 0x7eu)
    return SALTS_EPROTO;
  metadata->message_type = UINT32_C(0x0200);
  metadata->sequence = frame->data[1];
  memcpy(metadata->device_id, "013800138000", sizeof("013800138000"));
  memcpy(metadata->operation, "location", sizeof("location"));
  return SALTS_OK;
}

static int intake_test_coap_inspect(void *ctx, const char *configured_version,
                                    const turbo_flow_protocol_frame_view_t *frame,
                                    turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || frame->data_size != 4u || frame->data[0] != 0x40u) return SALTS_EPROTO;
  metadata->message_type = frame->data[1];
  metadata->sequence = ((uint64_t)frame->data[2] << 8u) | frame->data[3];
  memcpy(metadata->operation, "get", sizeof("get"));
  return SALTS_OK;
}

static int intake_test_coap_semantic(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata,
    turbo_flow_protocol_semantic_output_t *output) {
  static const uint8_t canonical[] = "{\"age\":21}";
  intake_semantic_probe_t *probe = (intake_semantic_probe_t *)ctx;
  (void)configured_version;
  if (!probe || !frame || frame->data_size != 4u || frame->data[0] != 0x40u ||
      !metadata || !output || output->size != sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      !output->data || output->capacity < sizeof(canonical) - 1u)
    return SALTS_EINVAL;
  probe->decode_calls++;
  metadata->message_type = frame->data[1];
  metadata->sequence = ((uint64_t)frame->data[2] << 8u) | frame->data[3];
  memcpy(metadata->operation, "post", sizeof("post"));
  memcpy(output->data, canonical, sizeof(canonical) - 1u);
  output->data_size = sizeof(canonical) - 1u;
  output->semantic_type = 50u;
  memcpy(output->media_type, "application/json", sizeof("application/json"));
  return SALTS_OK;
}

static turbo_flow_protocol_t *
intake_test_semantic_protocol(intake_semantic_probe_t *probe) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  turbo_flow_protocol_t *protocol = NULL;
  request.protocol = TURBO_FLOW_PROTOCOL_COAP;
  request.protocol_version = "RFC7252";
  request.max_frame_size = 64u;
  ops.inspect = intake_test_coap_inspect;
  ops.decode_semantic = intake_test_coap_semantic;
  check_equal(turbo_flow_protocol_create(
                  &request, "coap-semantic-test", "RFC7252",
                  TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE |
                      TURBO_FLOW_PROTOCOL_CAP_SEMANTIC_DECODE,
                  &ops, probe, &protocol),
              SALTS_OK);
  check_not_null(protocol);
  return protocol;
}

static int intake_test_mapper_preflight(
    void *ctx, const turbo_flow_protocol_mapper_preflight_request_t *request,
    turbo_flow_protocol_mapper_contract_t *contract) {
  (void)ctx;
  (void)request;
  (void)contract;
  return SALTS_ENOTSUP;
}

static int intake_test_mapper_map(
    void *ctx, const turbo_flow_protocol_mapper_request_t *request,
    turbo_flow_protocol_mapper_output_t *output) {
  intake_mapper_probe_t *probe = (intake_mapper_probe_t *)ctx;
  if (!probe || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      request->protocol != TURBO_FLOW_PROTOCOL_COAP ||
      !request->profile || strcmp(request->profile, "applicant-json") != 0 ||
      request->metadata.message_type != 2u ||
      request->semantic_type != 50u ||
      !request->semantic_media_type ||
      strcmp(request->semantic_media_type, "application/json") != 0 ||
      !request->semantic_data || !output || output->size != sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !output->payload || request->semantic_size > output->payload_capacity)
    return SALTS_EINVAL;
  probe->calls++;
  if (probe->status != SALTS_OK) return probe->status;
  memcpy(output->payload, request->semantic_data, request->semantic_size);
  output->payload_size = request->semantic_size;
  if (turbo_flow_content_descriptor_init(
          &output->content, TURBO_FLOW_DOMAIN_DATA,
          TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_JSON,
          "application/json", "intake.mapper") != SALTS_OK)
    return SALTS_EPROTO;
  return turbo_flow_content_descriptor_declare_schema(
      &output->content, "rulesforge.Applicant.data", "Applicant", 1u);
}

static turbo_flow_protocol_mapper_v1_t
intake_test_mapper(intake_mapper_probe_t *probe) {
  turbo_flow_protocol_mapper_v1_t mapper = TURBO_FLOW_PROTOCOL_MAPPER_V1_INIT;
  mapper.name = "fixture.mapper";
  mapper.protocol = TURBO_FLOW_PROTOCOL_COAP;
  mapper.profile = "applicant-json";
  mapper.max_semantic_bytes = 64u;
  mapper.max_output_bytes = 64u;
  mapper.ctx = probe;
  mapper.preflight = intake_test_mapper_preflight;
  mapper.map = intake_test_mapper_map;
  return mapper;
}

static turbo_flow_protocol_mapper_contract_t intake_test_mapper_contract(void) {
  turbo_flow_protocol_mapper_contract_t contract = TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
  contract.protocol = TURBO_FLOW_PROTOCOL_COAP;
  contract.message_type = 2u;
  contract.semantic_type = 50u;
  memcpy(contract.profile, "applicant-json", sizeof("applicant-json"));
  contract.max_semantic_bytes = 64u;
  contract.max_output_bytes = 64u;
  check_equal(turbo_flow_content_descriptor_init(
                  &contract.content, TURBO_FLOW_DOMAIN_DATA,
                  TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_JSON,
                  "application/json", "intake.mapper"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(
                  &contract.content, "rulesforge.Applicant.data", "Applicant", 1u),
              SALTS_OK);
  return contract;
}

static turbo_flow_protocol_t *intake_test_protocol(turbo_flow_protocol_kind_t kind) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  turbo_flow_protocol_t *protocol = NULL;
  const char *name;
  const char *version;

  request.protocol = kind;
  request.max_frame_size = 64u;
  if (kind == TURBO_FLOW_PROTOCOL_JTT_808) {
    name = "jtt808-test";
    version = "2019-A1";
    ops.inspect = intake_test_jtt_inspect;
  } else {
    name = "coap-test";
    version = "RFC7252";
    ops.inspect = intake_test_coap_inspect;
  }
  request.protocol_version = version;
  check_equal(turbo_flow_protocol_create(
                  &request, name, version,
                  TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
                  &ops, NULL, &protocol),
              SALTS_OK);
  check_not_null(protocol);
  return protocol;
}

static flow_protocol_network_intake_settings_t
intake_test_settings(turbo_flow_protocol_kind_t kind) {
  flow_protocol_network_intake_settings_t settings;
  memset(&settings, 0, sizeof(settings));
  settings.protocol_kind = kind;
  settings.schema_version = 2u;
  settings.transport_kind = kind == TURBO_FLOW_PROTOCOL_JTT_808
                                ? FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP
                                : FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP;
  memcpy(settings.protocol_provider, kind == TURBO_FLOW_PROTOCOL_JTT_808 ? "jtt808" : "coap",
         kind == TURBO_FLOW_PROTOCOL_JTT_808 ? sizeof("jtt808") : sizeof("coap"));
  memcpy(settings.protocol_version,
         kind == TURBO_FLOW_PROTOCOL_JTT_808 ? "2019-A1" : "RFC7252",
         kind == TURBO_FLOW_PROTOCOL_JTT_808 ? sizeof("2019-A1") : sizeof("RFC7252"));
  memcpy(settings.source_id, kind == TURBO_FLOW_PROTOCOL_JTT_808 ? "fleet.test" : "coap.test",
         kind == TURBO_FLOW_PROTOCOL_JTT_808 ? sizeof("fleet.test") : sizeof("coap.test"));
  memcpy(settings.source_adapter_name, "wire.input", sizeof("wire.input"));
  memcpy(settings.decoder_adapter_name, "protocol.decode", sizeof("protocol.decode"));
  settings.max_sessions = 1u;
  settings.max_frame_size = 64u;
  settings.max_pending_claims = 4u;
  settings.max_pending_bytes = 256u;
  settings.source_scheduler_max_steps = 2u;
  settings.source_max_message_bytes = 64u;
  return settings;
}

static turbo_flow_inbox_t intake_test_inbox(size_t max_records) {
  turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
  turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
  config.max_records = max_records;
  config.max_total_bytes = 4096u;
  config.max_record_bytes = 4096u;
  config.max_claims = max_records;
  check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
  return inbox;
}

static turbo_flow_t *intake_test_flow_with_mapper(
    flow_protocol_network_intake_sink_t **sink_out, turbo_flow_protocol_t *protocol,
    turbo_flow_inbox_t *inbox, const flow_protocol_network_intake_settings_t *settings,
    const turbo_flow_protocol_mapper_v1_t *mapper,
    const turbo_flow_protocol_mapper_contract_t *mapper_contract,
    turbo_flow_t **downstream_out, turbo_flow_durable_buffer_binding_t **binding_out) {
  static const char graph[] = "source input\n"
                              "stage decode adapter protocol.decode\n"
                              "stage main {\n"
                              "  input -> decode\n"
                              "}\n";
  static const char downstream_graph[] = "source decoded\n"
                                         "buffer intake resource protocol.store\n"
                                         "stage main {\n"
                                         "  decoded -> intake\n"
                                         "}\n";
  turbo_flow_durable_buffer_binding_config_t binding_config =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  flow_protocol_network_intake_sink_config_t config;
  turbo_flow_t *downstream = turbo_flow_create();
  turbo_flow_t *flow = turbo_flow_create();
  check_not_null(downstream);
  check_not_null(flow);
  check_equal(turbo_flow_parse_string(downstream, downstream_graph,
                                      sizeof(downstream_graph) - 1u),
              SALTS_OK);
  binding_config.resource_name = "protocol.store";
  binding_config.inbox = inbox;
  binding_config.identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
  binding_config.max_message_bytes = 4096u;
  check_equal(turbo_flow_durable_buffer_bind(downstream, &binding_config, binding_out), SALTS_OK);
  check_not_null(*binding_out);
  check_equal(turbo_flow_compile(downstream), SALTS_OK);
  check_equal(turbo_flow_start(downstream), SALTS_OK);
  check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
  memset(&config, 0, sizeof(config));
  config.flow = flow;
  config.adapter_name = "protocol.decode";
  config.protocol = protocol;
  config.downstream_flow = downstream;
  config.decoded_source_name = "decoded";
  config.settings = settings;
  config.mapper = mapper;
  config.mapper_contract = mapper_contract;
  check_equal(flow_protocol_network_intake_sink_create(&config, sink_out), SALTS_OK);
  check_not_null(*sink_out);
  check_equal(flow_protocol_network_intake_sink_register(*sink_out), SALTS_OK);
  check_equal(turbo_flow_compile(flow), SALTS_OK);
  check_equal(turbo_flow_start(flow), SALTS_OK);
  *downstream_out = downstream;
  return flow;
}

static turbo_flow_t *intake_test_flow(
    flow_protocol_network_intake_sink_t **sink_out, turbo_flow_protocol_t *protocol,
    turbo_flow_inbox_t *inbox, const flow_protocol_network_intake_settings_t *settings,
    turbo_flow_t **downstream_out, turbo_flow_durable_buffer_binding_t **binding_out) {
  return intake_test_flow_with_mapper(sink_out, protocol, inbox, settings, NULL, NULL,
                                      downstream_out, binding_out);
}

static void intake_completion(void *ctx, const turbo_flow_publish_result_t *result) {
  intake_completion_probe_t *probe = (intake_completion_probe_t *)ctx;
  atomic_store_explicit(&probe->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&probe->calls, 1u, memory_order_release);
}

static void intake_probe_init(intake_completion_probe_t *probe) {
  atomic_init(&probe->calls, 0u);
  atomic_init(&probe->status, SALTS_EALREADY);
}

static void intake_wait_calls(intake_completion_probe_t *probe, size_t expected) {
  for (size_t attempt = 0u;
       attempt < 5000u && atomic_load_explicit(&probe->calls, memory_order_acquire) < expected;
       ++attempt)
    salts_sleep_ms(1u);
}

static void intake_wait_backpressure(flow_protocol_network_intake_sink_t *sink, int expected) {
  flow_protocol_network_intake_sink_metrics_t metrics;
  for (size_t attempt = 0u; attempt < 5000u; ++attempt) {
    memset(&metrics, 0, sizeof(metrics));
    flow_protocol_network_intake_sink_metrics(sink, &metrics);
    if ((metrics.backpressured != 0) == (expected != 0)) return;
    salts_sleep_ms(1u);
  }
}

static turbo_flow_msg_t intake_listener_message(uint64_t id, uint32_t slot, uint64_t generation,
                                                const uint8_t *data, size_t size) {
  turbo_flow_msg_t message;
  const size_t storage_size = sizeof(turbo_flow_cnet_listener_message_context_t) + size;
  mem_buffer_t *buffer = mem_get_buffer(mem_global(), storage_size);
  turbo_flow_cnet_listener_message_context_t *context;
  check_not_null(buffer);
  context = (turbo_flow_cnet_listener_message_context_t *)mem_buffer_data(buffer);
  memset(context, 0, sizeof(*context));
  context->size = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_V1_SIZE;
  context->version = TURBO_FLOW_CNET_LISTENER_MESSAGE_CONTEXT_API_VERSION;
  context->connection.slot = slot;
  context->connection.generation = generation;
  memcpy(mem_buffer_data(buffer) + sizeof(*context), data, size);
  mem_set_used(buffer, storage_size);
  turbo_flow_msg_init(&message);
  message.id = id;
  message.buffer = buffer;
  message.transport_context = context;
  message.payload = vstr_from_buf(mem_buffer_const_data(buffer) + sizeof(*context), size);
  return message;
}

static turbo_flow_msg_t intake_packet_message(uint64_t id, uint32_t slot, uint64_t generation,
                                              const uint8_t *data, size_t size) {
  turbo_flow_msg_t message;
  const size_t storage_size = sizeof(turbo_flow_cnet_packet_message_context_t) + size;
  mem_buffer_t *buffer = mem_get_buffer(mem_global(), storage_size);
  turbo_flow_cnet_packet_message_context_t *context;
  check_not_null(buffer);
  context = (turbo_flow_cnet_packet_message_context_t *)mem_buffer_data(buffer);
  memset(context, 0, sizeof(*context));
  context->size = TURBO_FLOW_CNET_PACKET_MESSAGE_CONTEXT_V1_SIZE;
  context->version = TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION;
  context->session.slot = slot;
  context->session.generation = generation;
  context->info.protocol = CNET_PACKET_UDP;
  context->info.peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  context->info.peer.address[0] = 127u;
  context->info.peer.address[3] = 1u;
  context->info.peer.port = 54321u;
  memcpy(mem_buffer_data(buffer) + sizeof(*context), data, size);
  mem_set_used(buffer, storage_size);
  turbo_flow_msg_init(&message);
  message.id = id;
  message.buffer = buffer;
  message.transport_context = context;
  message.payload = vstr_from_buf(mem_buffer_const_data(buffer) + sizeof(*context), size);
  return message;
}

static int intake_publish(turbo_flow_t *flow, turbo_flow_msg_t *message,
                          intake_completion_probe_t *completion) {
  int rc = turbo_flow_publish_async(flow, "input", message, intake_completion, completion);
  turbo_flow_msg_cleanup(message);
  return rc;
}

static turbo_flow_inbox_record_t intake_prefill_record(void) {
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_id = vstr_from_buf("prefill", sizeof("prefill") - 1u);
  record.admission_id = vstr_from_buf("prefill", sizeof("prefill") - 1u);
  record.source_sequence = 1u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "prefill"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "prefill.v1", "Record",
                                                           1u),
              SALTS_OK);
  record.payload = vstr_from_buf("x", 1u);
  return record;
}

static void intake_free_one_record(turbo_flow_inbox_t *inbox) {
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  uint64_t record_id;
  check_equal(turbo_flow_inbox_claim(inbox, &claim), SALTS_OK);
  record_id = claim.record_id;
  check_equal(turbo_flow_inbox_complete(inbox, &claim), SALTS_OK);
  check_equal(turbo_flow_inbox_forget(inbox, record_id), SALTS_OK);
}

static void intake_destroy(turbo_flow_t *flow, flow_protocol_network_intake_sink_t *sink,
                           turbo_flow_t *downstream,
                           turbo_flow_durable_buffer_binding_t *binding,
                           turbo_flow_protocol_t *protocol, turbo_flow_inbox_t *inbox) {
  check_equal(turbo_flow_stop(flow), SALTS_OK);
  turbo_flow_destroy(flow);
  flow_protocol_network_intake_sink_destroy(sink);
  check_equal(turbo_flow_stop(downstream), SALTS_OK);
  check_equal(turbo_flow_durable_buffer_unbind(binding), SALTS_OK);
  turbo_flow_destroy(downstream);
  turbo_flow_protocol_destroy(protocol);
  check_equal(turbo_flow_inbox_close(inbox), SALTS_OK);
  check_equal(turbo_flow_inbox_destroy(inbox), SALTS_OK);
}

spec("protocol network intake core") {
  it("retains a capacity-blocked JT/T frame and retries it without re-feeding bytes") {
    static const uint8_t first_half[] = {0x7eu, 0x01u};
    static const uint8_t second_half[] = {0x7eu};
    static const uint8_t replay[] = {0x7eu, 0x01u, 0x7eu};
    turbo_flow_protocol_t *protocol = intake_test_protocol(TURBO_FLOW_PROTOCOL_JTT_808);
    flow_protocol_network_intake_settings_t settings =
        intake_test_settings(TURBO_FLOW_PROTOCOL_JTT_808);
    turbo_flow_inbox_t inbox = intake_test_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow =
        intake_test_flow(&sink, protocol, &inbox, &settings, &downstream, &binding);
    turbo_flow_inbox_record_t prefill = intake_prefill_record();
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    intake_completion_probe_t first;
    intake_completion_probe_t second;
    intake_completion_probe_t replay_completion;
    turbo_flow_msg_t message;

    check_equal(turbo_flow_inbox_admit(&inbox, &prefill, &receipt), SALTS_OK);
    intake_probe_init(&first);
    message = intake_listener_message(1u, 1u, 1u, first_half, sizeof(first_half));
    check_equal(intake_publish(flow, &message, &first), SALTS_OK);
    intake_wait_calls(&first, 1u);
    check_equal(atomic_load_explicit(&first.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_OK);

    intake_probe_init(&second);
    message = intake_listener_message(2u, 1u, 1u, second_half, sizeof(second_half));
    check_equal(intake_publish(flow, &message, &second), SALTS_OK);
    intake_wait_backpressure(sink, 1);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)0u);

    intake_free_one_record(&inbox);
    check_equal(flow_protocol_network_intake_sink_retry(sink), SALTS_OK);
    intake_wait_calls(&second, 1u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.admitted, (uint64_t)2u);
    check_equal(snapshot.pending_records, 1u);

    intake_probe_init(&replay_completion);
    message = intake_listener_message(3u, 1u, 1u, replay, sizeof(replay));
    check_equal(intake_publish(flow, &message, &replay_completion), SALTS_OK);
    intake_wait_calls(&replay_completion, 1u);
    check_equal(atomic_load_explicit(&replay_completion.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.admitted, (uint64_t)2u);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(claim.record.timestamp_ns, (uint64_t)0u);
    check_equal(claim.record.source_sequence, (uint64_t)1u);
    check_equal(claim.record.source_id.len, strlen(settings.source_id));
    check_equal(claim.record.source_id.data, settings.source_id, strlen(settings.source_id));
    receipt.record_id = claim.record_id;
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_OK);

    intake_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }

  it("fences parser state on TCP generation reuse and rejects an older generation") {
    static const uint8_t partial[] = {0x7eu, 0x01u};
    static const uint8_t fresh[] = {0x7eu, 0x02u, 0x7eu};
    turbo_flow_protocol_t *protocol = intake_test_protocol(TURBO_FLOW_PROTOCOL_JTT_808);
    flow_protocol_network_intake_settings_t settings =
        intake_test_settings(TURBO_FLOW_PROTOCOL_JTT_808);
    turbo_flow_inbox_t inbox = intake_test_inbox(2u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow =
        intake_test_flow(&sink, protocol, &inbox, &settings, &downstream, &binding);
    intake_completion_probe_t first;
    intake_completion_probe_t second;
    intake_completion_probe_t stale;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_msg_t message;

    intake_probe_init(&first);
    message = intake_listener_message(10u, 1u, 1u, partial, sizeof(partial));
    check_equal(intake_publish(flow, &message, &first), SALTS_OK);
    intake_wait_calls(&first, 1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_OK);

    intake_probe_init(&second);
    message = intake_listener_message(11u, 1u, 2u, fresh, sizeof(fresh));
    check_equal(intake_publish(flow, &message, &second), SALTS_OK);
    intake_wait_calls(&second, 1u);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, 1u);

    intake_probe_init(&stale);
    message = intake_listener_message(12u, 1u, 1u, fresh, sizeof(fresh));
    check_equal(intake_publish(flow, &message, &stale), SALTS_OK);
    intake_wait_calls(&stale, 1u);
    check_equal(atomic_load_explicit(&stale.status, memory_order_acquire), SALTS_EPROTO);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, 1u);

    intake_free_one_record(&inbox);
    intake_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }

  it("maps codec semantic content to canonical business bytes before durable admission") {
    static const uint8_t coap_post[] = {0x40u, 0x02u, 0x12u, 0x34u};
    static const char canonical[] = "{\"age\":21}";
    intake_semantic_probe_t semantic = {0};
    intake_mapper_probe_t mapper_probe = {0, SALTS_OK};
    turbo_flow_protocol_t *protocol = intake_test_semantic_protocol(&semantic);
    flow_protocol_network_intake_settings_t settings =
        intake_test_settings(TURBO_FLOW_PROTOCOL_COAP);
    turbo_flow_protocol_mapper_v1_t mapper = intake_test_mapper(&mapper_probe);
    turbo_flow_protocol_mapper_contract_t contract = intake_test_mapper_contract();
    turbo_flow_inbox_t inbox = intake_test_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow;
    intake_completion_probe_t completion;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_msg_t message;
    uint64_t record_id;

    settings.schema_version = 3u;
    memcpy(settings.mapper_semantic_media_type, "application/json",
           sizeof("application/json"));
    flow = intake_test_flow_with_mapper(&sink, protocol, &inbox, &settings,
                                        &mapper, &contract, &downstream, &binding);

    intake_probe_init(&completion);
    message = intake_packet_message(20u, 1u, 1u, coap_post, sizeof(coap_post));
    check_equal(intake_publish(flow, &message, &completion), SALTS_OK);
    intake_wait_calls(&completion, 1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(semantic.decode_calls, (size_t)1u);
    check_equal(mapper_probe.calls, (size_t)1u);

    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(claim.record.content.domain, TURBO_FLOW_DOMAIN_DATA);
    check_equal(claim.record.content.encoding, TURBO_FLOW_DATA_ENCODING_JSON);
    check_equal(claim.record.content.schema_version, 1u);
    check_equal(claim.record.content.schema_name, "rulesforge.Applicant.data");
    check_equal(claim.record.content.type_name, "Applicant");
    check_equal(claim.record.payload.len, sizeof(canonical) - 1u);
    check_equal(claim.record.payload.data, canonical, sizeof(canonical) - 1u);
    record_id = claim.record_id;
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, record_id), SALTS_OK);
    intake_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }

  it("fails mapping before durable admission without raw-envelope fallback") {
    static const uint8_t coap_post[] = {0x40u, 0x02u, 0x12u, 0x35u};
    intake_semantic_probe_t semantic = {0};
    intake_mapper_probe_t mapper_probe = {0, SALTS_EPROTO};
    turbo_flow_protocol_t *protocol = intake_test_semantic_protocol(&semantic);
    flow_protocol_network_intake_settings_t settings =
        intake_test_settings(TURBO_FLOW_PROTOCOL_COAP);
    turbo_flow_protocol_mapper_v1_t mapper = intake_test_mapper(&mapper_probe);
    turbo_flow_protocol_mapper_contract_t contract = intake_test_mapper_contract();
    turbo_flow_inbox_t inbox = intake_test_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow;
    intake_completion_probe_t completion;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_msg_t message;

    settings.schema_version = 3u;
    memcpy(settings.mapper_semantic_media_type, "application/json",
           sizeof("application/json"));
    flow = intake_test_flow_with_mapper(&sink, protocol, &inbox, &settings,
                                        &mapper, &contract, &downstream, &binding);

    intake_probe_init(&completion);
    message = intake_packet_message(21u, 1u, 1u, coap_post, sizeof(coap_post));
    check_equal(intake_publish(flow, &message, &completion), SALTS_OK);
    intake_wait_calls(&completion, 1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_EPROTO);
    check_equal(semantic.decode_calls, (size_t)1u);
    check_equal(mapper_probe.calls, (size_t)1u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)0u);
    check_equal(snapshot.admitted, (uint64_t)0u);
    intake_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }

  it("derives stable CoAP device identity from UDP peer context") {
    static const uint8_t coap_get[] = {0x40u, 0x01u, 0x12u, 0x34u};
    turbo_flow_protocol_t *protocol = intake_test_protocol(TURBO_FLOW_PROTOCOL_COAP);
    flow_protocol_network_intake_settings_t settings = intake_test_settings(TURBO_FLOW_PROTOCOL_COAP);
    turbo_flow_inbox_t inbox = intake_test_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow =
        intake_test_flow(&sink, protocol, &inbox, &settings, &downstream, &binding);
    intake_completion_probe_t completion;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_msg_t message;
    uint64_t record_id;

    intake_probe_init(&completion);
    message = intake_packet_message(1u, 1u, 1u, coap_get, sizeof(coap_get));
    check_equal(intake_publish(flow, &message, &completion), SALTS_OK);
    intake_wait_calls(&completion, 1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_true(claim.record.admission_id.len > sizeof("coap/udp4-7f000001-54321/") - 1u);
    check_equal(memcmp(claim.record.admission_id.data, "coap/udp4-7f000001-54321/",
                       sizeof("coap/udp4-7f000001-54321/") - 1u),
                0);
    check_equal(claim.record.timestamp_ns, (uint64_t)0u);
    record_id = claim.record_id;
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, record_id), SALTS_OK);

    intake_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }
}
