#include "../../tests/flow_operation_fixture.h"
#include "salts_error.h"
#include "tinytest.h"
#include "turbo_flow_inbox_source.h"

#include <string.h>

int flow_inbox_source_cpp_header_probe(void);

typedef struct inbox_source_probe_s {
  int business_status;
  size_t business_calls;
  size_t sink_calls;
  uint64_t record_id;
  uint64_t source_sequence;
  char source_id[32];
  char admission_id[32];
  char correlation[32];
} inbox_source_probe_t;

typedef struct settlement_fixture_s {
  turbo_flow_inbox_t backing;
  int first_complete_status;
  size_t complete_calls;
  size_t fail_calls;
} settlement_fixture_t;

static turbo_flow_inbox_record_t inbox_source_record(const char *source_id,
                                                     const char *admission_id) {
  static const char payload[] = "{\"value\":41}";
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_id = vstr_from_buf(source_id, strlen(source_id));
  record.partition_key = record.source_id;
  record.admission_id = vstr_from_buf(admission_id, strlen(admission_id));
  record.source_sequence = 41u;
  record.timestamp_ns = 42u;
  record.message_type = 7u;
  record.message_flags = 9u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "orders.created"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "orders.v1",
                                                           "OrderCreated", 1u),
              SALTS_OK);
  record.correlation = record.admission_id;
  record.payload = vstr_from_buf(payload, sizeof(payload) - 1u);
  return record;
}

static int inbox_source_read_metadata(const turbo_flow_msg_t *message,
                                      inbox_source_probe_t *probe) {
  const turbo_flow_inbox_source_context_t *context;
  const turbo_flow_content_descriptor_t *content = turbo_flow_msg_content_descriptor(message);
  vstr source_id;
  vstr admission_id;
  vstr correlation;

  if (!message || !probe || !content) return SALTS_EPROTO;
  context = turbo_flow_inbox_source_context(message);
  source_id = turbo_flow_inbox_source_source_id(message);
  admission_id = turbo_flow_inbox_source_admission_id(message);
  correlation = turbo_flow_inbox_source_correlation(message);
  if (!context || context->record_id == 0u || context->source_sequence != 41u ||
      context->source_id_size == 0u || context->admission_id_size == 0u ||
      context->correlation_size == 0u || message->id != context->record_id ||
      message->ts_ns != 42u || message->type != 7u || message->flags != 9u ||
      content->schema_version != 1u || strcmp(content->schema_name, "orders.v1") != 0)
    return SALTS_EPROTO;
  if (!source_id.data || !admission_id.data || !correlation.data ||
      source_id.len != context->source_id_size || admission_id.len != context->admission_id_size ||
      correlation.len != context->correlation_size || source_id.len >= sizeof(probe->source_id) ||
      admission_id.len >= sizeof(probe->admission_id) ||
      correlation.len >= sizeof(probe->correlation))
    return SALTS_EPROTO;
  memcpy(probe->source_id, source_id.data, source_id.len);
  memcpy(probe->admission_id, admission_id.data, admission_id.len);
  memcpy(probe->correlation, correlation.data, correlation.len);
  probe->source_id[source_id.len] = '\0';
  probe->admission_id[admission_id.len] = '\0';
  probe->correlation[correlation.len] = '\0';
  probe->record_id = context->record_id;
  probe->source_sequence = context->source_sequence;
  return SALTS_OK;
}

static int inbox_source_business(turbo_flow_msg_t *message, void *ctx) {
  inbox_source_probe_t *probe = (inbox_source_probe_t *)ctx;
  int status = inbox_source_read_metadata(message, probe);
  if (status != SALTS_OK) return status;
  ++probe->business_calls;
  return probe->business_status;
}

static int inbox_source_sink(turbo_flow_msg_t *message, void *ctx) {
  inbox_source_probe_t *probe = (inbox_source_probe_t *)ctx;
  int status = inbox_source_read_metadata(message, probe);
  if (status != SALTS_OK) return status;
  ++probe->sink_calls;
  return SALTS_OK;
}

static turbo_flow_t *inbox_source_flow(inbox_source_probe_t *probe) {
  static const char dsl[] = "source orders\n"
                            "stage business operation test.business\n"
                            "stage sink operation test.sink\n"
                            "stage main {\n"
                            "  orders -> business -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  flow_test_operation_t business =
      flow_test_operation_init("test.business", inbox_source_business, probe);
  flow_test_operation_t sink = flow_test_operation_init("test.sink", inbox_source_sink, probe);
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

static int settlement_admit(void *ctx, const turbo_flow_inbox_record_t *record,
                            turbo_flow_inbox_receipt_t *receipt) {
  settlement_fixture_t *fixture = (settlement_fixture_t *)ctx;
  return turbo_flow_inbox_admit(&fixture->backing, record, receipt);
}

static int settlement_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  settlement_fixture_t *fixture = (settlement_fixture_t *)ctx;
  return turbo_flow_inbox_claim(&fixture->backing, claim);
}
static int settlement_claim_ex(void *ctx, const turbo_flow_inbox_claim_request_t *request,
                               turbo_flow_inbox_claim_t *claim) {
  return turbo_flow_inbox_claim_ex(&((settlement_fixture_t *)ctx)->backing, request, claim);
}


static int settlement_complete(void *ctx, uint64_t record_id, uint64_t claim_token) {
  settlement_fixture_t *fixture = (settlement_fixture_t *)ctx;
  ++fixture->complete_calls;
  if (fixture->first_complete_status != SALTS_OK) {
    int status = fixture->first_complete_status;
    fixture->first_complete_status = SALTS_OK;
    if (status == SALTS_EALREADY) {
      int backing_status =
          fixture->backing.ops->complete(fixture->backing.ctx, record_id, claim_token);
      if (backing_status != SALTS_OK) return backing_status;
    } else if (status == SALTS_ECANCELED) {
      int backing_status =
          fixture->backing.ops->fail(fixture->backing.ctx, record_id, claim_token, SALTS_ECANCELED);
      if (backing_status != SALTS_OK) return backing_status;
    }
    return status;
  }
  return fixture->backing.ops->complete(fixture->backing.ctx, record_id, claim_token);
}

static int settlement_fail(void *ctx, uint64_t record_id, uint64_t claim_token, int status) {
  settlement_fixture_t *fixture = (settlement_fixture_t *)ctx;
  ++fixture->fail_calls;
  return fixture->backing.ops->fail(fixture->backing.ctx, record_id, claim_token, status);
}

static int settlement_retry(void *ctx, uint64_t record_id) {
  return turbo_flow_inbox_retry(&((settlement_fixture_t *)ctx)->backing, record_id);
}

static int settlement_discard(void *ctx, uint64_t record_id) {
  return turbo_flow_inbox_discard(&((settlement_fixture_t *)ctx)->backing, record_id);
}

static int settlement_forget(void *ctx, uint64_t record_id) {
  return turbo_flow_inbox_forget(&((settlement_fixture_t *)ctx)->backing, record_id);
}

static int settlement_scan_failed(void *ctx, uint64_t after_record_id,
                                  turbo_flow_inbox_failed_entry_t *entries, size_t capacity,
                                  size_t *out_count) {
  return turbo_flow_inbox_scan_failed(&((settlement_fixture_t *)ctx)->backing, after_record_id,
                                      entries, capacity, out_count);
}

static int settlement_scan_history(void *ctx, uint64_t after_record_id,
                                   turbo_flow_inbox_history_entry_t *entries, size_t capacity,
                                   size_t *out_count) {
  return turbo_flow_inbox_scan_history(&((settlement_fixture_t *)ctx)->backing, after_record_id,
                                       entries, capacity, out_count);
}

static int settlement_close(void *ctx) {
  return turbo_flow_inbox_close(&((settlement_fixture_t *)ctx)->backing);
}

static int settlement_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  return turbo_flow_inbox_snapshot(&((settlement_fixture_t *)ctx)->backing, snapshot);
}

static int settlement_destroy(void *ctx) {
  return turbo_flow_inbox_destroy(&((settlement_fixture_t *)ctx)->backing);
}

static const turbo_flow_inbox_ops_v2_t settlement_ops = {.size = sizeof(settlement_ops),
                                                         .version = TURBO_FLOW_INBOX_API_VERSION,
                                                         .admit = settlement_admit,
                                                         .claim = settlement_claim,
                                                         .claim_ex = settlement_claim_ex,
                                                         .complete = settlement_complete,
                                                         .fail = settlement_fail,
                                                         .retry = settlement_retry,
                                                         .discard = settlement_discard,
                                                         .forget = settlement_forget,
                                                         .scan_failed = settlement_scan_failed,
                                                         .scan_history = settlement_scan_history,
                                                         .close = settlement_close,
                                                         .snapshot = settlement_snapshot,
                                                         .destroy = settlement_destroy};

static int inbox_source_fixture_open(settlement_fixture_t *fixture, int first_complete_status,
                                     turbo_flow_inbox_t *inbox) {
  turbo_flow_inbox_memory_config_t memory = turbo_flow_inbox_memory_config_default();
  memory.max_records = 2u;
  memory.max_total_bytes = 512u;
  memory.max_record_bytes = 256u;
  memory.max_claims = 1u;
  fixture->first_complete_status = first_complete_status;
  *inbox =
      (turbo_flow_inbox_t){sizeof(*inbox), TURBO_FLOW_INBOX_API_VERSION, &settlement_ops, fixture};
  return turbo_flow_inbox_memory_create(&memory, &fixture->backing);
}

static int inbox_source_open(turbo_flow_inbox_t *inbox, turbo_flow_t *flow,
                             cflow_scheduler *scheduler, turbo_flow_inbox_source_t **source) {
  turbo_flow_inbox_source_config_t config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
  config.inbox = inbox;
  config.flow = flow;
  config.graph_source_name = "orders";
  config.scheduler = scheduler;
  config.max_message_bytes = 256u;
  return turbo_flow_inbox_source_create(&config, source);
}

static void inbox_source_cleanup(turbo_flow_inbox_source_t *source, turbo_flow_inbox_t *inbox,
                                 turbo_flow_t *flow, cflow_scheduler *scheduler) {
  check_equal(turbo_flow_inbox_source_destroy(source), SALTS_OK);
  check_equal(turbo_flow_inbox_close(inbox), SALTS_OK);
  check_equal(turbo_flow_inbox_destroy(inbox), SALTS_OK);
  check_equal(turbo_flow_stop(flow), SALTS_OK);
  turbo_flow_destroy(flow);
  cflow_scheduler_destroy(scheduler);
}

spec("Inbox-to-Graph source run") {
  it("exposes the exact current ABI to C++ consumers") {
    check_equal(flow_inbox_source_cpp_header_probe(), 0);
  }

  it("rejects short and unknown config layouts without compatibility handling") {
    turbo_flow_inbox_source_config_t config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    config.size = sizeof(config) - 1u;
    check_equal(turbo_flow_inbox_source_create(&config, &source), SALTS_EINVAL);
    check_null(source);
    config.size = sizeof(config);
    config.version = TURBO_FLOW_INBOX_SOURCE_API_VERSION + 1u;
    check_equal(turbo_flow_inbox_source_create(&config, &source), SALTS_EINVAL);
    check_null(source);
  }

  it("fails an oversized claimed record before Graph execution without leaking the claim") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("http.orders", "http-oversized");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_inbox_source_config_t config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;
    size_t failed_count = 0u;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_OK, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    config.inbox = &inbox;
    config.flow = flow;
    config.graph_source_name = "orders";
    config.scheduler = &scheduler;
    config.max_message_bytes = sizeof(turbo_flow_inbox_source_context_t) + 1u;
    check_equal(turbo_flow_inbox_source_create(&config, &source), SALTS_OK);

    check_equal(turbo_flow_inbox_source_request(source), SALTS_ENOSPC);
    check_equal(probe.business_calls, (size_t)0u);
    check_equal(probe.sink_calls, (size_t)0u);
    check_equal(fixture.complete_calls, (size_t)0u);
    check_equal(fixture.fail_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, &failed, 1u, &failed_count), SALTS_OK);
    check_equal(failed_count, (size_t)1u);
    check_equal(failed.record_id, receipt.record_id);
    check_equal(failed.status, SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("cancels an active run and fails its claim without executing the Graph") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("socket.orders", "socket-cancel");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;
    size_t failed_count = 0u;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_OK, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);

    check_equal(turbo_flow_inbox_source_cancel(source, &result), SALTS_ECANCELED);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_FAILED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(result.graph_status, SALTS_ECANCELED);
    check_equal(result.settlement_status, SALTS_OK);
    check_equal(probe.business_calls, (size_t)0u);
    check_equal(probe.sink_calls, (size_t)0u);
    check_equal(fixture.fail_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, &failed, 1u, &failed_count), SALTS_OK);
    check_equal(failed_count, (size_t)1u);
    check_equal(failed.record_id, receipt.record_id);
    check_equal(failed.status, SALTS_ECANCELED);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("starts one record and settles only after poll observes terminal Graph state") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("http.orders", "http-41");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_OK, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(probe.business_calls, (size_t)0u);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);

    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check_equal(probe.business_calls, (size_t)0u);
    check_equal(fixture.complete_calls, (size_t)0u);
    {
      turbo_flow_inbox_source_result_t invalid_result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
      invalid_result.size -= 1u;
      check_equal(turbo_flow_inbox_source_poll(source, &invalid_result), SALTS_EINVAL);
      invalid_result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
      invalid_result.version += 1u;
      check_equal(turbo_flow_inbox_source_poll(source, &invalid_result), SALTS_EINVAL);
      check_equal(probe.business_calls, (size_t)0u);
      check_equal(fixture.complete_calls, (size_t)0u);
    }
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE);
    check_equal(result.record_id, receipt.record_id);
    check_equal(fixture.complete_calls, (size_t)0u);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);
    check_equal(fixture.complete_calls, (size_t)0u);

    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(turbo_flow_inbox_source_cancel(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(result.graph_status, SALTS_OK);
    check_equal(result.settlement_status, SALTS_OK);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(probe.sink_calls, (size_t)1u);
    check_equal(probe.record_id, receipt.record_id);
    check_equal(probe.source_sequence, (uint64_t)41u);
    check_equal(probe.source_id, "http.orders");
    check_equal(probe.admission_id, "http-41");
    check_equal(probe.correlation, "http-41");
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_ENOENT);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.completed, (uint64_t)1u);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("allows an explicit inline Scheduler but still defers Inbox settlement to poll") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("mqtt.orders", "mqtt-inline");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_OK, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_inline_init(&scheduler));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);

    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(probe.sink_calls, (size_t)1u);
    check_equal(fixture.complete_calls, (size_t)0u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(fixture.complete_calls, (size_t)1u);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("settles Graph failure once and never replays Graph or settlement implicitly") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("socket.orders", "socket-41");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {.business_status = SALTS_EPROTO};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;
    size_t failed_count = 0u;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_OK, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);

    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_EPROTO);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_FAILED);
    check_equal(result.graph_status, SALTS_EPROTO);
    check_equal(result.settlement_status, SALTS_OK);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(probe.sink_calls, (size_t)0u);
    check_equal(fixture.fail_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_ENOENT);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(fixture.fail_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, &failed, 1u, &failed_count), SALTS_OK);
    check_equal(failed_count, (size_t)1u);
    check_equal(failed.record_id, receipt.record_id);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("keeps transient settlement pending until explicit retry without Graph replay") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("ws.orders", "ws-41");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_EBUSY, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);

    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_EBUSY);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_PENDING);
    check_equal(result.graph_status, SALTS_OK);
    check_equal(result.settlement_status, SALTS_EBUSY);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_EBUSY);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_source_destroy(source), SALTS_EBUSY);

    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(turbo_flow_inbox_source_retry_settlement(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(fixture.complete_calls, (size_t)2u);
    check_equal(probe.business_calls, (size_t)1u);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("marks EALREADY settlement unknown until explicit reconciliation") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("mqtt.orders", "mqtt-41");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_EALREADY, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);

    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_EALREADY);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN);
    check_equal(result.graph_status, SALTS_OK);
    check_equal(result.settlement_status, SALTS_EALREADY);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_EBUSY);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_source_destroy(source), SALTS_EBUSY);

    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(turbo_flow_inbox_source_retry_settlement(source, &result), SALTS_EALREADY);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_SETTLEMENT_UNKNOWN);
    check_equal(fixture.complete_calls, (size_t)1u);

    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(turbo_flow_inbox_source_reconcile_settlement(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(probe.business_calls, (size_t)1u);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }

  it("reports provider owner loss without replaying the completed Graph") {
    settlement_fixture_t fixture = {.backing = TURBO_FLOW_INBOX_INIT};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_record_t record = inbox_source_record("socket.orders", "socket-owner-lost");
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    inbox_source_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;

    check_equal(inbox_source_fixture_open(&fixture, SALTS_ECANCELED, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_flow(&probe);
    check_not_null(flow);
    check_equal(inbox_source_open(&inbox, flow, &scheduler, &source), SALTS_OK);
    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);

    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_ECANCELED);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_OWNER_LOST_UNKNOWN);
    check_equal(result.record_id, receipt.record_id);
    check_equal(result.graph_status, SALTS_OK);
    check_equal(result.settlement_status, SALTS_ECANCELED);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(probe.sink_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_ENOENT);
    check_equal(fixture.complete_calls, (size_t)1u);
    check_equal(probe.business_calls, (size_t)1u);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);

    inbox_source_cleanup(source, &inbox, flow, &scheduler);
  }
}
