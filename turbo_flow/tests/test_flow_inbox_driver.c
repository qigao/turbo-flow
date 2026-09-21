#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_inbox_source.h"
#include "../src/flow_inbox_driver_internal.h"
#include "../src/flow_projection_owner_internal.h"

#include <salts/clock.h>
#include <stdatomic.h>
#include <string.h>

typedef struct driver_probe_s {
  size_t calls;
  int status;
  int saw_null_transport;
  int saw_payload;
  int saw_durable_identity;
  int saw_durable_claim;
} driver_probe_t;

typedef struct async_probe_s {
  turbo_flow_async_terminal_claim_t claim;
  atomic_size_t submissions;
} async_probe_t;

static turbo_flow_inbox_record_t driver_record(const char *admission, const char *payload) {
  static const char source[] = "durable.intake";
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_id = vstr_from_buf(source, sizeof(source) - 1u);
  record.partition_key = record.source_id;
  record.admission_id = vstr_from_buf(admission, strlen(admission));
  record.source_sequence = 41u;
  record.timestamp_ns = 9001u;
  record.message_type = 7u;
  record.message_flags = 9u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "driver.message"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "driver.v1",
                                                           "DriverMessage", 1u),
              SALTS_OK);
  record.correlation = record.admission_id;
  record.payload = vstr_from_buf(payload, strlen(payload));
  return record;
}

static int driver_sink(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                       turbo_flow_msg_t *message) {
  driver_probe_t *probe = (driver_probe_t *)ctx;
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  (void)flow;
  (void)stage;
  ++probe->calls;
  probe->saw_null_transport = message->transport_context == NULL;
  probe->saw_payload = message->payload.len == sizeof("buffer-payload") - 1u &&
                       memcmp(message->payload.data, "buffer-payload",
                              sizeof("buffer-payload") - 1u) == 0;
  probe->saw_durable_identity =
      turbo_flow_msg_durable_identity(message, &identity) == SALTS_OK &&
      identity.source_sequence == 41u &&
      identity.source_id.len == sizeof("durable.intake") - 1u &&
      memcmp(identity.source_id.data, "durable.intake", identity.source_id.len) == 0 &&
      identity.admission_id.len > 0u && identity.correlation.len == identity.admission_id.len &&
      memcmp(identity.correlation.data, identity.admission_id.data, identity.admission_id.len) == 0;
  probe->saw_durable_claim = flow_msg_has_durable_claim(message);
  return probe->status;
}

static int async_submit(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                        const turbo_flow_msg_t *message,
                        turbo_flow_async_terminal_claim_t *claim) {
  async_probe_t *probe = (async_probe_t *)ctx;
  (void)flow;
  (void)stage;
  check_null(message->transport_context);
  if (turbo_flow_async_terminal_claim_move(&probe->claim, claim) != SALTS_OK)
    return SALTS_EBUSY;
  (void)atomic_fetch_add_explicit(&probe->submissions, 1u, memory_order_release);
  return SALTS_OK;
}

static void wait_for_submission(async_probe_t *probe) {
  for (size_t attempt = 0u;
       attempt < 5000u && atomic_load_explicit(&probe->submissions, memory_order_acquire) == 0u;
       ++attempt)
    salts_sleep_ms(1u);
}

static int bind_memory(turbo_flow_t *flow, const char *resource, turbo_flow_inbox_t *inbox,
                       turbo_flow_durable_buffer_binding_t **binding) {
  turbo_flow_inbox_memory_config_t memory = turbo_flow_inbox_memory_config_default();
  turbo_flow_durable_buffer_binding_config_t config =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  int rc = turbo_flow_inbox_memory_create(&memory, inbox);
  if (rc != SALTS_OK) return rc;
  config.resource_name = resource;
  config.inbox = inbox;
  return turbo_flow_durable_buffer_bind(flow, &config, binding);
}

static flow_inbox_driver_t *open_driver(turbo_flow_t *flow, turbo_flow_inbox_t *inbox,
                                        uint32_t origin_stage, cflow_scheduler *scheduler) {
  flow_inbox_driver_config_t config = {inbox, flow, FLOW_INBOX_DRIVER_BUFFER, origin_stage,
                                       scheduler, 512u};
  flow_inbox_driver_t *driver = NULL;
  check_equal(flow_inbox_driver_create(&config, &driver), SALTS_OK);
  return driver;
}

static void cleanup_driver(flow_inbox_driver_t *driver, turbo_flow_t *flow,
                           turbo_flow_durable_buffer_binding_t *binding,
                           turbo_flow_inbox_t *inbox, cflow_scheduler *scheduler) {
  check_equal(flow_inbox_driver_destroy(driver), SALTS_OK);
  check_equal(turbo_flow_stop(flow), SALTS_OK);
  check_equal(turbo_flow_durable_buffer_unbind(binding), SALTS_OK);
  check_equal(turbo_flow_inbox_close(inbox), SALTS_OK);
  check_equal(turbo_flow_inbox_destroy(inbox), SALTS_OK);
  turbo_flow_destroy(flow);
  cflow_scheduler_destroy(scheduler);
}

spec("Internal Inbox claim-to-Graph driver") {
  it("resumes after its buffer without re-admission and preserves generic durable identity") {
    static const char graph[] = "source input\n"
                                "buffer intake resource intake.store\n"
                                "stage sink adapter test.sink\n"
                                "stage main {\n"
                                "  input -> intake -> sink\n"
                                "}\n";
    driver_probe_t probe = {0};
    turbo_flow_adapter_ops_t ops = {0};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_inbox_record_t record = driver_record("one", "buffer-payload");
    flow_inbox_driver_t *driver;
    int origin;
    ops.consume = driver_sink;
    check_not_null(flow);
    check_equal(turbo_flow_register_adapter(flow, "test.sink", &ops, &probe), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(bind_memory(flow, "intake.store", &inbox, &binding), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(cflow_scheduler_inline_init(&scheduler));
    origin = turbo_flow_find_stage(flow, "intake");
    check_true(origin >= 0);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    driver = open_driver(flow, &inbox, (uint32_t)origin, &scheduler);

    check_equal(flow_inbox_driver_request(driver), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.saw_null_transport, 1);
    check_equal(probe.saw_payload, 1);
    check_equal(probe.saw_durable_identity, 1);
    check_equal(probe.saw_durable_claim, 1);
    check_equal(flow_inbox_driver_poll(driver, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)0u);
    check_equal(snapshot.completed, (uint64_t)1u);

    cleanup_driver(driver, flow, binding, &inbox, &scheduler);
  }

  it("treats the next buffer as a new execution cut") {
    static const char graph[] = "source input\n"
                                "buffer first resource first.store\n"
                                "stage middle adapter test.middle\n"
                                "buffer second resource second.store\n"
                                "stage sink adapter test.sink\n"
                                "stage main {\n"
                                "  input -> first -> middle -> second -> sink\n"
                                "}\n";
    driver_probe_t middle = {0};
    driver_probe_t sink = {0};
    turbo_flow_adapter_ops_t middle_ops = {0}, sink_ops = {0};
    turbo_flow_inbox_t first = TURBO_FLOW_INBOX_INIT, second = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_t *first_binding = NULL, *second_binding = NULL;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t second_snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_inbox_record_t record = driver_record("cut", "buffer-payload");
    flow_inbox_driver_t *driver;
    int origin;
    middle_ops.consume = driver_sink;
    sink_ops.consume = driver_sink;
    check_not_null(flow);
    check_equal(turbo_flow_register_adapter(flow, "test.middle", &middle_ops, &middle), SALTS_OK);
    check_equal(turbo_flow_register_adapter(flow, "test.sink", &sink_ops, &sink), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(bind_memory(flow, "first.store", &first, &first_binding), SALTS_OK);
    check_equal(bind_memory(flow, "second.store", &second, &second_binding), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(cflow_scheduler_inline_init(&scheduler));
    origin = turbo_flow_find_stage(flow, "first");
    check_equal(turbo_flow_inbox_admit(&first, &record, &receipt), SALTS_OK);
    driver = open_driver(flow, &first, (uint32_t)origin, &scheduler);

    check_equal(flow_inbox_driver_request(driver), SALTS_OK);
    check_equal(flow_inbox_driver_poll(driver, &result), SALTS_OK);
    check_equal(middle.calls, (size_t)1u);
    check_equal(sink.calls, (size_t)0u);
    check_equal(turbo_flow_inbox_snapshot(&second, &second_snapshot), SALTS_OK);
    check_equal(second_snapshot.pending_records, (size_t)1u);

    check_equal(flow_inbox_driver_destroy(driver), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(second_binding), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(first_binding), SALTS_OK);
    {
      turbo_flow_inbox_claim_t second_claim = TURBO_FLOW_INBOX_CLAIM_INIT;
      check_equal(turbo_flow_inbox_claim(&second, &second_claim), SALTS_OK);
      check_equal(turbo_flow_inbox_complete(&second, &second_claim), SALTS_OK);
    }
    check_equal(turbo_flow_inbox_close(&second), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&second), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&first), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&first), SALTS_OK);
    turbo_flow_destroy(flow);
    cflow_scheduler_destroy(&scheduler);
  }

  it("settles cancellation without running or replaying the Graph") {
    static const char graph[] = "source input\n"
                                "buffer intake resource intake.store\n"
                                "stage sink adapter test.sink\n"
                                "stage main {\n"
                                "  input -> intake -> sink\n"
                                "}\n";
    driver_probe_t probe = {.status = SALTS_EPROTO};
    turbo_flow_adapter_ops_t ops = {0};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_inbox_record_t record = driver_record("failure", "buffer-payload");
    flow_inbox_driver_t *driver;
    int origin;
    ops.consume = driver_sink;
    check_equal(turbo_flow_register_adapter(flow, "test.sink", &ops, &probe), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(bind_memory(flow, "intake.store", &inbox, &binding), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    origin = turbo_flow_find_stage(flow, "intake");
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    driver = open_driver(flow, &inbox, (uint32_t)origin, &scheduler);
    check_equal(flow_inbox_driver_request(driver), SALTS_OK);
    check_equal(flow_inbox_driver_cancel(driver, &result), SALTS_ECANCELED);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_FAILED);
    check_equal(result.graph_status, SALTS_ECANCELED);
    check_equal(probe.calls, (size_t)0u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.failed_records, (size_t)1u);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);
    cleanup_driver(driver, flow, binding, &inbox, &scheduler);
  }

  it("settles a synchronous Graph failure once") {
    static const char graph[] = "source input\n"
                                "buffer intake resource intake.store\n"
                                "stage sink adapter test.sink\n"
                                "stage main {\n"
                                "  input -> intake -> sink\n"
                                "}\n";
    driver_probe_t probe = {.status = SALTS_EPROTO};
    turbo_flow_adapter_ops_t ops = {0};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_inbox_record_t record = driver_record("sync-failure", "buffer-payload");
    flow_inbox_driver_t *driver;
    int origin;
    ops.consume = driver_sink;
    check_equal(turbo_flow_register_adapter(flow, "test.sink", &ops, &probe), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(bind_memory(flow, "intake.store", &inbox, &binding), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(cflow_scheduler_inline_init(&scheduler));
    origin = turbo_flow_find_stage(flow, "intake");
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    driver = open_driver(flow, &inbox, (uint32_t)origin, &scheduler);

    check_equal(flow_inbox_driver_request(driver), SALTS_OK);
    check_equal(flow_inbox_driver_poll(driver, &result), SALTS_EPROTO);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_FAILED);
    check_equal(result.graph_status, SALTS_EPROTO);
    check_equal(probe.calls, (size_t)1u);
    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(flow_inbox_driver_poll(driver, &result), SALTS_ENOENT);
    check_equal(probe.calls, (size_t)1u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.failed_records, (size_t)1u);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);

    cleanup_driver(driver, flow, binding, &inbox, &scheduler);
  }

  it("holds the Inbox claim until async-terminal completion") {
    static const char graph[] = "source input\n"
                                "buffer intake resource intake.store\n"
                                "stage output adapter async.out\n"
                                "stage main {\n"
                                "  input -> intake -> output\n"
                                "}\n";
    async_probe_t probe = {0};
    turbo_flow_adapter_ops_t adapter_ops = {0};
    turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
    turbo_flow_adapter_schema_t schema = {0};
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_inbox_record_t record = driver_record("async", "buffer-payload");
    flow_inbox_driver_t *driver;
    int origin;
    probe.claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
    atomic_init(&probe.submissions, 0u);
    async_ops.submit = async_submit;
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
    check_equal(turbo_flow_register_async_terminal_adapter_ex(
                    flow, "async.out", &adapter_ops, &async_ops, &probe, &schema),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(bind_memory(flow, "intake.store", &inbox, &binding), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_true(cflow_scheduler_inline_init(&scheduler));
    origin = turbo_flow_find_stage(flow, "intake");
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    driver = open_driver(flow, &inbox, (uint32_t)origin, &scheduler);

    check_equal(flow_inbox_driver_request(driver), SALTS_OK);
    wait_for_submission(&probe);
    check_equal(atomic_load_explicit(&probe.submissions, memory_order_acquire), (size_t)1u);
    check_equal(flow_inbox_driver_poll(driver, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.in_flight_claims, (size_t)1u);
    check_equal(snapshot.completed, (uint64_t)0u);
    check_equal(turbo_flow_async_terminal_complete(&probe.claim, SALTS_OK, NULL), SALTS_OK);
    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(flow_inbox_driver_poll(driver, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);

    cleanup_driver(driver, flow, binding, &inbox, &scheduler);
  }
}
