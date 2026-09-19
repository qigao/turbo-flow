#include "tinytest.h"

#include "flow_protocol_network_intake_internal.h"
#include "turbo_flow_cnet.h"
#include "turbo_flow_protocol_plugin.h"

#include <salts/clock.h>

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

typedef struct intake_edge_completion_s {
  atomic_size_t calls;
  atomic_int status;
} intake_edge_completion_t;

static int intake_edge_inspect(void *ctx, const char *configured_version,
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

static turbo_flow_protocol_t *intake_edge_protocol(void) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  turbo_flow_protocol_t *protocol = NULL;
  request.protocol = TURBO_FLOW_PROTOCOL_JTT_808;
  request.protocol_version = "2019-A1";
  request.max_frame_size = 64u;
  ops.inspect = intake_edge_inspect;
  check_equal(turbo_flow_protocol_create(
                  &request, "jtt808-edge", "2019-A1",
                  TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
                      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE,
                  &ops, NULL, &protocol),
              SALTS_OK);
  return protocol;
}

static flow_protocol_network_intake_settings_t intake_edge_settings(void) {
  flow_protocol_network_intake_settings_t settings;
  memset(&settings, 0, sizeof(settings));
  settings.protocol_kind = TURBO_FLOW_PROTOCOL_JTT_808;
  settings.transport_kind = FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP;
  memcpy(settings.protocol_provider, "jtt808", sizeof("jtt808"));
  memcpy(settings.protocol_version, "2019-A1", sizeof("2019-A1"));
  memcpy(settings.source_id, "fleet.edge", sizeof("fleet.edge"));
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

static turbo_flow_inbox_t intake_edge_inbox(size_t max_records) {
  turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
  turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
  config.max_records = max_records;
  config.max_total_bytes = 4096u;
  config.max_record_bytes = 4096u;
  config.max_claims = max_records;
  check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
  return inbox;
}

static turbo_flow_t *intake_edge_flow(
    flow_protocol_network_intake_sink_t **sink_out, turbo_flow_protocol_t *protocol,
    turbo_flow_inbox_t *inbox, const flow_protocol_network_intake_settings_t *settings,
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
  check_equal(flow_protocol_network_intake_sink_create(&config, sink_out), SALTS_OK);
  check_equal(flow_protocol_network_intake_sink_register(*sink_out), SALTS_OK);
  check_equal(turbo_flow_compile(flow), SALTS_OK);
  check_equal(turbo_flow_start(flow), SALTS_OK);
  *downstream_out = downstream;
  return flow;
}

static turbo_flow_msg_t intake_edge_listener_message(uint64_t id, uint64_t generation,
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
  context->connection.slot = 1u;
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

static turbo_flow_msg_t intake_edge_malformed_message(uint64_t id, const uint8_t *data,
                                                      size_t size) {
  turbo_flow_msg_t message;
  mem_buffer_t *buffer = mem_get_buffer(mem_global(), size);
  check_not_null(buffer);
  memcpy(mem_buffer_data(buffer), data, size);
  mem_set_used(buffer, size);
  turbo_flow_msg_init(&message);
  message.id = id;
  message.buffer = buffer;
  message.payload = vstr_from_buf(mem_buffer_const_data(buffer), size);
  return message;
}

static void intake_edge_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  intake_edge_completion_t *completion = (intake_edge_completion_t *)ctx;
  atomic_store_explicit(&completion->status, result->status, memory_order_relaxed);
  (void)atomic_fetch_add_explicit(&completion->calls, 1u, memory_order_release);
}

static void intake_edge_completion_init(intake_edge_completion_t *completion) {
  atomic_init(&completion->calls, 0u);
  atomic_init(&completion->status, SALTS_EALREADY);
}

static void intake_edge_wait(intake_edge_completion_t *completion, size_t expected) {
  for (size_t i = 0u;
       i < 5000u && atomic_load_explicit(&completion->calls, memory_order_acquire) < expected; ++i)
    salts_sleep_ms(1u);
}

static int intake_edge_publish(turbo_flow_t *flow, turbo_flow_msg_t *message,
                               intake_edge_completion_t *completion) {
  int rc = turbo_flow_publish_async(flow, "input", message, intake_edge_complete, completion);
  turbo_flow_msg_cleanup(message);
  return rc;
}

static turbo_flow_inbox_record_t intake_edge_prefill(void) {
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

static void intake_edge_free_one(turbo_flow_inbox_t *inbox) {
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  uint64_t id;
  check_equal(turbo_flow_inbox_claim(inbox, &claim), SALTS_OK);
  id = claim.record_id;
  check_equal(turbo_flow_inbox_complete(inbox, &claim), SALTS_OK);
  check_equal(turbo_flow_inbox_forget(inbox, id), SALTS_OK);
}

static void intake_edge_destroy(
    turbo_flow_t *flow, flow_protocol_network_intake_sink_t *sink, turbo_flow_t *downstream,
    turbo_flow_durable_buffer_binding_t *binding, turbo_flow_protocol_t *protocol,
    turbo_flow_inbox_t *inbox) {
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

spec("protocol network intake edge ownership") {
  it("retains one multi-frame claim until every copied frame is admitted") {
    static const uint8_t two_frames[] = {0x7eu, 0x01u, 0x7eu, 0x7eu, 0x02u, 0x7eu};
    turbo_flow_protocol_t *protocol = intake_edge_protocol();
    flow_protocol_network_intake_settings_t settings = intake_edge_settings();
    turbo_flow_inbox_t inbox = intake_edge_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow =
        intake_edge_flow(&sink, protocol, &inbox, &settings, &downstream, &binding);
    intake_edge_completion_t completion;
    flow_protocol_network_intake_sink_metrics_t metrics;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_msg_t message;

    intake_edge_completion_init(&completion);
    message = intake_edge_listener_message(1u, 1u, two_frames, sizeof(two_frames));
    check_equal(intake_edge_publish(flow, &message, &completion), SALTS_OK);
    memset(&metrics, 0, sizeof(metrics));
    flow_protocol_network_intake_sink_metrics(sink, &metrics);
    check_true(metrics.backpressured);
    check_equal(metrics.pending_claims, (size_t)1u);
    check_equal(metrics.frames_admitted, (uint64_t)1u);
    check_equal(atomic_load_explicit(&completion.calls, memory_order_acquire), (size_t)0u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)1u);

    intake_edge_free_one(&inbox);
    check_equal(flow_protocol_network_intake_sink_retry(sink), SALTS_OK);
    intake_edge_wait(&completion, 1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_OK);
    flow_protocol_network_intake_sink_metrics(sink, &metrics);
    check_false(metrics.backpressured);
    check_equal(metrics.pending_claims, (size_t)0u);
    check_equal(metrics.frames_admitted, (uint64_t)2u);

    intake_edge_free_one(&inbox);
    intake_edge_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }

  it("queues later claims without feeding and cancel completes every retained claim") {
    static const uint8_t first_frame[] = {0x7eu, 0x01u, 0x7eu};
    static const uint8_t second_frame[] = {0x7eu, 0x02u, 0x7eu};
    turbo_flow_protocol_t *protocol = intake_edge_protocol();
    flow_protocol_network_intake_settings_t settings = intake_edge_settings();
    turbo_flow_inbox_t inbox = intake_edge_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow =
        intake_edge_flow(&sink, protocol, &inbox, &settings, &downstream, &binding);
    turbo_flow_inbox_record_t prefill = intake_edge_prefill();
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    intake_edge_completion_t first;
    intake_edge_completion_t second;
    flow_protocol_network_intake_sink_metrics_t metrics;
    turbo_flow_msg_t message;

    check_equal(turbo_flow_inbox_admit(&inbox, &prefill, &receipt), SALTS_OK);
    intake_edge_completion_init(&first);
    message = intake_edge_listener_message(1u, 1u, first_frame, sizeof(first_frame));
    check_equal(intake_edge_publish(flow, &message, &first), SALTS_OK);
    flow_protocol_network_intake_sink_metrics(sink, &metrics);
    check_true(metrics.backpressured);
    check_equal(metrics.pending_claims, (size_t)1u);
    check_equal(metrics.frames_admitted, (uint64_t)0u);

    intake_edge_completion_init(&second);
    message = intake_edge_listener_message(2u, 1u, second_frame, sizeof(second_frame));
    check_equal(intake_edge_publish(flow, &message, &second), SALTS_OK);
    flow_protocol_network_intake_sink_metrics(sink, &metrics);
    check_equal(metrics.pending_claims, (size_t)2u);
    check_equal(metrics.frames_admitted, (uint64_t)0u);
    check_equal(atomic_load_explicit(&second.calls, memory_order_acquire), (size_t)0u);

    flow_protocol_network_intake_sink_cancel(sink, SALTS_ECANCELED);
    intake_edge_wait(&first, 1u);
    intake_edge_wait(&second, 1u);
    check_equal(atomic_load_explicit(&first.status, memory_order_acquire), SALTS_ECANCELED);
    check_equal(atomic_load_explicit(&second.status, memory_order_acquire), SALTS_ECANCELED);
    flow_protocol_network_intake_sink_metrics(sink, &metrics);
    check_equal(metrics.pending_claims, (size_t)0u);
    check_equal(metrics.active_sessions, (size_t)0u);
    check_equal(metrics.terminal_status, SALTS_ECANCELED);

    intake_edge_free_one(&inbox);
    intake_edge_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }

  it("fails a malformed transport context before Inbox admission") {
    static const uint8_t frame[] = {0x7eu, 0x01u, 0x7eu};
    turbo_flow_protocol_t *protocol = intake_edge_protocol();
    flow_protocol_network_intake_settings_t settings = intake_edge_settings();
    turbo_flow_inbox_t inbox = intake_edge_inbox(1u);
    flow_protocol_network_intake_sink_t *sink = NULL;
    turbo_flow_t *downstream = NULL;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_t *flow =
        intake_edge_flow(&sink, protocol, &inbox, &settings, &downstream, &binding);
    intake_edge_completion_t completion;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_msg_t message;

    intake_edge_completion_init(&completion);
    message = intake_edge_malformed_message(1u, frame, sizeof(frame));
    check_equal(intake_edge_publish(flow, &message, &completion), SALTS_OK);
    intake_edge_wait(&completion, 1u);
    check_equal(atomic_load_explicit(&completion.status, memory_order_acquire), SALTS_EPROTO);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)0u);

    intake_edge_destroy(flow, sink, downstream, binding, protocol, &inbox);
  }
}
