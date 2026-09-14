#include "../../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow_inbox_source.h"
#include "turbo_flow_turbodb.h"

#include <salts_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define INBOX_SOURCE_TEST_MAX_RECORDS 4u
#define INBOX_SOURCE_TEST_MAX_TOTAL_BYTES 256u
#define INBOX_SOURCE_TEST_MAX_RECORD_BYTES 128u
#define INBOX_SOURCE_TEST_MAX_CLAIMS 1u

typedef struct inbox_source_db_fixture_s {
  char *path;
  orm_option_t filename;
  orm_config_t database;
} inbox_source_db_fixture_t;

typedef struct inbox_source_db_probe_s {
  size_t calls;
  uint64_t record_id;
  uint64_t source_sequence;
} inbox_source_db_probe_t;

static const char INBOX_SOURCE_META_DDL[] =
    "CREATE TABLE orders_inbox_meta_v2 ("
    "singleton_id integer primary key not null, schema_magic text not null, "
    "schema_version integer not null, generation bigint not null, owner_state integer not null, "
    "next_record_id bigint not null, next_claim_token bigint not null, max_records bigint not "
    "null, max_total_bytes bigint not null, max_record_bytes bigint not null, max_claims bigint "
    "not null, records bigint not null, history_records bigint not null, pending_records bigint "
    "not null, failed_records bigint not null, in_flight_claims bigint not null, retained_bytes "
    "bigint not null, admitted bigint not null, completed bigint not null, failed bigint not "
    "null, retried bigint not null, discarded bigint not null)";

static const char INBOX_SOURCE_RECORDS_DDL[] =
    "CREATE TABLE orders_inbox_records_v2 ("
    "record_id bigint primary key not null, phase integer not null, claim_generation bigint not "
    "null, claim_token bigint not null, failure_status integer not null, failure_kind integer not "
    "null, terminal_kind integer not null, envelope_schema text not null, "
    "envelope_schema_version integer not null, source_id bytea not null, admission_id bytea not "
    "null, source_sequence_be bytea not null, timestamp_ns_be bytea not null, message_type bigint "
    "not null, message_flags bigint not null, content_domain integer not null, content_profile "
    "integer not null, content_encoding integer not null, content_flags bigint not null, "
    "content_schema_version bigint not null, content_media_type text not null, "
    "content_schema_name text not null, content_type_name text not null, content_identity text not "
    "null, correlation bytea not null, payload bytea not null, retained_bytes bigint not null)";

static const char INBOX_SOURCE_DEDUPE_INDEX_DDL[] =
    "CREATE UNIQUE INDEX orders_inbox_records_v2_admission ON "
    "orders_inbox_records_v2(source_id, admission_id)";

static const char INBOX_SOURCE_PHASE_INDEX_DDL[] =
    "CREATE INDEX orders_inbox_records_v2_phase ON orders_inbox_records_v2(phase, record_id)";

static const char INBOX_SOURCE_META_ROW[] =
    "INSERT INTO orders_inbox_meta_v2 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 2, 0, 0, 1, 1, 4, 256, 128, 1, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";

static void inbox_source_db_execute(orm_connection_t *connection, const char *sql,
                                    orm_error_t *error) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  check_equal(orm_raw(connection, orm_view(sql), &query, error), ORM_STATUS_OK);
  check_equal(orm_query_execute(query, &result, error), ORM_STATUS_OK);
  orm_result_destroy(result);
  orm_query_destroy(query);
}

static void inbox_source_db_fixture_init(inbox_source_db_fixture_t *fixture) {
  orm_connection_t *connection = NULL;
  orm_error_t error;

  memset(fixture, 0, sizeof(*fixture));
  fixture->path = tt_make_temp_file("turbo-flow-turbodb-inbox-source", ".sqlite3");
  check_not_null(fixture->path);
  orm_config(&fixture->database);
  fixture->filename.keyword = orm_view("filename");
  fixture->filename.value = orm_view(fixture->path);
  fixture->database.driver = orm_view("sqlite");
  fixture->database.options = &fixture->filename;
  fixture->database.option_count = 1u;

  orm_error_init(&error);
  check_equal(orm_connect(&fixture->database, &connection, &error), ORM_STATUS_OK);
  check_not_null(connection);
  inbox_source_db_execute(connection, INBOX_SOURCE_META_DDL, &error);
  inbox_source_db_execute(connection, INBOX_SOURCE_RECORDS_DDL, &error);
  inbox_source_db_execute(connection, INBOX_SOURCE_DEDUPE_INDEX_DDL, &error);
  inbox_source_db_execute(connection, INBOX_SOURCE_PHASE_INDEX_DDL, &error);
  inbox_source_db_execute(connection, INBOX_SOURCE_META_ROW, &error);
  orm_disconnect(connection);
}

static void inbox_source_db_fixture_destroy(inbox_source_db_fixture_t *fixture) {
  check_not_null(fixture);
  check_not_null(fixture->path);
  check_equal(tt_remove_file(fixture->path), 0);
  free(fixture->path);
  memset(fixture, 0, sizeof(*fixture));
}

static turbo_flow_turbodb_inbox_config_t
inbox_source_db_config(const inbox_source_db_fixture_t *fixture) {
  turbo_flow_turbodb_inbox_config_t config = turbo_flow_turbodb_inbox_config_default();
  config.database = &fixture->database;
  config.namespace_name = "orders";
  config.max_records = INBOX_SOURCE_TEST_MAX_RECORDS;
  config.max_total_bytes = INBOX_SOURCE_TEST_MAX_TOTAL_BYTES;
  config.max_record_bytes = INBOX_SOURCE_TEST_MAX_RECORD_BYTES;
  config.max_claims = INBOX_SOURCE_TEST_MAX_CLAIMS;
  config.connection_count = 2u;
  return config;
}

static turbo_flow_inbox_record_t inbox_source_db_record(void) {
  static const char source_id[] = "http.orders";
  static const char admission_id[] = "http-41";
  static const char correlation[] = "order-41";
  static const char payload[] = "{\"value\":41}";
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_id = vstr_from_buf(source_id, sizeof(source_id) - 1u);
  record.admission_id = vstr_from_buf(admission_id, sizeof(admission_id) - 1u);
  record.source_sequence = 41u;
  record.timestamp_ns = 42u;
  record.message_type = 17u;
  record.message_flags = 23u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "orders/created"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "orders.v2",
                                                           "OrderCreated", 3u),
              SALTS_OK);
  record.correlation = vstr_from_buf(correlation, sizeof(correlation) - 1u);
  record.payload = vstr_from_buf(payload, sizeof(payload) - 1u);
  return record;
}

static int inbox_source_db_sink(turbo_flow_msg_t *message, void *ctx) {
  static const char expected_source[] = "http.orders";
  static const char expected_payload[] = "{\"value\":41}";
  inbox_source_db_probe_t *probe = (inbox_source_db_probe_t *)ctx;
  const turbo_flow_inbox_source_context_t *context = turbo_flow_inbox_source_context(message);
  vstr source_id = turbo_flow_inbox_source_source_id(message);

  if (!probe || !context || context->record_id != message->id || context->source_sequence != 41u ||
      source_id.len != sizeof(expected_source) - 1u ||
      memcmp(source_id.data, expected_source, source_id.len) != 0 ||
      message->payload.len != sizeof(expected_payload) - 1u ||
      memcmp(message->payload.data, expected_payload, message->payload.len) != 0)
    return SALTS_EPROTO;
  ++probe->calls;
  probe->record_id = context->record_id;
  probe->source_sequence = context->source_sequence;
  return SALTS_OK;
}

static turbo_flow_t *inbox_source_db_flow(inbox_source_db_probe_t *probe) {
  static const char dsl[] = "source orders\n"
                            "stage sink operation test.turbodb.sink\n"
                            "stage main {\n"
                            "  orders -> sink\n"
                            "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  flow_test_operation_t sink =
      flow_test_operation_init("test.turbodb.sink", inbox_source_db_sink, probe);
  if (!flow) return NULL;
  sink.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  sink.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  if (turbo_flow_parse_string(flow, dsl, sizeof(dsl) - 1u) != SALTS_OK ||
      flow_test_operation_register(flow, &sink) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

spec("TurboDB Inbox-to-Graph source run") {
  it("persists completed history through the shared non-blocking driver") {
    inbox_source_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t inbox_config;
    turbo_flow_inbox_source_config_t source_config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
    turbo_flow_inbox_record_t record = inbox_source_db_record();
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_source_result_t result = TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    turbo_flow_inbox_source_t *source = NULL;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    inbox_source_db_probe_t probe = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_t *flow;
    orm_error_t error;
    size_t history_count = 0u;

    inbox_source_db_fixture_init(&fixture);
    inbox_config = inbox_source_db_config(&fixture);
    check_equal(turbo_flow_turbodb_inbox_create(&inbox_config, &inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 4u));
    flow = inbox_source_db_flow(&probe);
    check_not_null(flow);
    source_config.inbox = &inbox;
    source_config.flow = flow;
    source_config.graph_source_name = "orders";
    source_config.scheduler = &scheduler;
    source_config.max_message_bytes = 256u;
    check_equal(turbo_flow_inbox_source_create(&source_config, &source), SALTS_OK);

    check_equal(turbo_flow_inbox_source_request(source), SALTS_OK);
    check_equal(probe.calls, (size_t)0u);
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_GRAPH_ACTIVE);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);
    check_equal(probe.calls, (size_t)1u);

    result = (turbo_flow_inbox_source_result_t)TURBO_FLOW_INBOX_SOURCE_RESULT_INIT;
    check_equal(turbo_flow_inbox_source_poll(source, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_INBOX_SOURCE_COMPLETED);
    check_equal(result.record_id, receipt.record_id);
    check_equal(result.graph_status, SALTS_OK);
    check_equal(result.settlement_status, SALTS_OK);
    check_equal(probe.record_id, receipt.record_id);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(snapshot.history_records, (size_t)1u);
    check_equal(snapshot.completed, (uint64_t)1u);
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, &history, 1u, &history_count), SALTS_OK);
    check_equal(history_count, (size_t)1u);
    check_equal(history.record_id, receipt.record_id);
    check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);

    check_equal(turbo_flow_inbox_source_destroy(source), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);

    inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    history = (turbo_flow_inbox_history_entry_t)TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    history_count = 0u;
    check_equal(turbo_flow_turbodb_inbox_create(&inbox_config, &inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, &history, 1u, &history_count), SALTS_OK);
    check_equal(history_count, (size_t)1u);
    check_equal(history.record_id, receipt.record_id);
    check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_source_db_fixture_destroy(&fixture);
  }
}
