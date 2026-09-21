#include "data_bind.h"
#include "tinytest.h"
#include "turbo_flow_protocol_envelope.h"
#include "turbo_flow_protocol_inbox_envelope.h"
#include "turbo_flow_turbodb.h"

#include <salts_error.h>
#include <salts_thread.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TURBO_FLOW_TURBODB_INBOX_CRASH_HELPER
  #error "test_turbodb_inbox requires the literal crash helper executable"
#endif

#define INBOX_TEST_MAX_RECORDS 4u
#define INBOX_TEST_MAX_TOTAL_BYTES 256u
#define INBOX_TEST_MAX_RECORD_BYTES 128u
#define INBOX_TEST_MAX_CLAIMS 2u

typedef struct inbox_db_fixture_s {
  char *path;
  orm_option_t filename;
  orm_config_t database;
} inbox_db_fixture_t;

typedef struct inbox_settle_worker_s {
  turbo_flow_inbox_t *inbox;
  turbo_flow_inbox_claim_t claim;
  int status;
} inbox_settle_worker_t;

static void inbox_complete_worker(void *arg) {
  inbox_settle_worker_t *worker = (inbox_settle_worker_t *)arg;
  worker->status = turbo_flow_inbox_complete(worker->inbox, &worker->claim);
}

static const char INBOX_META_DDL[] =
    "CREATE TABLE orders_inbox_meta_v3 ("
    "singleton_id integer primary key not null, schema_magic text not null, "
    "schema_version integer not null, generation bigint not null, owner_state integer not null, "
    "next_record_id bigint not null, next_claim_token bigint not null, max_records bigint not "
    "null, max_total_bytes bigint not null, max_record_bytes bigint not null, max_claims bigint "
    "not null, records bigint not null, history_records bigint not null, pending_records bigint "
    "not null, failed_records bigint not null, in_flight_claims bigint not null, retained_bytes "
    "bigint not null, admitted bigint "
    "not null, completed bigint not null, failed bigint not null, retried bigint not null, "
    "discarded bigint not null)";

static const char INBOX_RECORDS_DDL[] =
    "CREATE TABLE orders_inbox_records_v3 ("
    "record_id bigint primary key not null, phase integer not null, claim_generation bigint not "
    "null, "
    "claim_token bigint not null, failure_status integer not null, failure_kind integer not null, "
    "terminal_kind integer not null, envelope_schema text not null, envelope_schema_version "
    "integer not null, source_id bytea not null, partition_key bytea not null, "
    "admission_id bytea not null, source_sequence_be "
    "bytea not null, timestamp_ns_be bytea not null, message_type bigint not null, message_flags "
    "bigint not null, content_domain integer not null, content_profile integer not null, "
    "content_encoding integer not null, content_flags bigint not null, content_schema_version "
    "bigint not null, content_media_type text not null, content_schema_name text not null, "
    "content_type_name text not null, content_identity text not null, correlation bytea not null, "
    "payload bytea not null, retained_bytes bigint not null)";

static const char INBOX_RECORDS_NULLABLE_PRIMARY_KEY_DDL[] =
    "CREATE TABLE orders_inbox_records_v3 ("
    "record_id bigint primary key, phase integer not null, claim_generation bigint not null, "
    "claim_token bigint not null, failure_status integer not null, failure_kind integer not null, "
    "terminal_kind integer not null, envelope_schema text not null, envelope_schema_version "
    "integer not null, source_id bytea not null, partition_key bytea not null, "
    "admission_id bytea not null, source_sequence_be "
    "bytea not null, timestamp_ns_be bytea not null, message_type bigint not null, message_flags "
    "bigint not null, content_domain integer not null, content_profile integer not null, "
    "content_encoding integer not null, content_flags bigint not null, content_schema_version "
    "bigint not null, content_media_type text not null, content_schema_name text not null, "
    "content_type_name text not null, content_identity text not null, correlation bytea not null, "
    "payload bytea not null, retained_bytes bigint not null)";

static const char INBOX_DEDUPE_INDEX_DDL[] =
    "CREATE UNIQUE INDEX orders_inbox_records_v3_admission ON "
    "orders_inbox_records_v3(source_id, admission_id)";

static const char INBOX_PHASE_INDEX_DDL[] =
    "CREATE INDEX orders_inbox_records_v3_phase ON orders_inbox_records_v3(phase, record_id)";

static const char INBOX_META_V3_ROW[] =
    "INSERT INTO orders_inbox_meta_v3 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 3, 0, 0, 1, 1, 4, 256, 128, 2, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";

static const char INBOX_META_OLD_ROW[] =
    "INSERT INTO orders_inbox_meta_v3 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 2, 0, 0, 1, 1, 4, 256, 128, 2, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";

static void inbox_db_fixture_init(inbox_db_fixture_t *fixture) {
  memset(fixture, 0, sizeof(*fixture));
  fixture->path = tt_make_temp_file("turbo-flow-turbodb-inbox", ".sqlite3");
  check_not_null(fixture->path);
  orm_config(&fixture->database);
  fixture->filename.keyword = orm_view("filename");
  fixture->filename.value = orm_view(fixture->path);
  fixture->database.driver = orm_view("sqlite");
  fixture->database.options = &fixture->filename;
  fixture->database.option_count = 1u;
}

static void inbox_db_fixture_destroy(inbox_db_fixture_t *fixture) {
  if (!fixture || !fixture->path) return;
  check_equal(tt_remove_file(fixture->path), 0);
  free(fixture->path);
  memset(fixture, 0, sizeof(*fixture));
}

static orm_connection_t *inbox_db_connect(inbox_db_fixture_t *fixture, orm_error_t *error) {
  orm_connection_t *connection = NULL;
  orm_error_init(error);
  check_equal(orm_connect(&fixture->database, &connection, error), ORM_STATUS_OK);
  check_not_null(connection);
  return connection;
}

static void inbox_db_execute(orm_connection_t *connection, const char *sql, orm_error_t *error) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;

  check_equal(orm_raw(connection, orm_view(sql), &query, error), ORM_STATUS_OK);
  check_equal(orm_query_execute(query, &result, error), ORM_STATUS_OK);
  orm_result_destroy(result);
  orm_query_destroy(query);
}

static int64_t inbox_db_read_int64(orm_connection_t *connection, const char *sql,
                                   orm_error_t *error) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  uint64_t rows = 0u;
  int64_t value = -1;

  check_equal(orm_raw(connection, orm_view(sql), &query, error), ORM_STATUS_OK);
  check_equal(orm_query_execute(query, &result, error), ORM_STATUS_OK);
  check_equal(orm_result_row_count(result, &rows, error), ORM_STATUS_OK);
  check_equal(rows, (uint64_t)1u);
  check_equal(orm_result_get_int64(result, 0u, 0u, &value, error), ORM_STATUS_OK);
  orm_result_destroy(result);
  orm_query_destroy(query);
  return value;
}

static void inbox_db_check_text(orm_connection_t *connection, const char *sql, const char *expected,
                                orm_error_t *error) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  orm_string_view_t value = {0};

  check_equal(orm_raw(connection, orm_view(sql), &query, error), ORM_STATUS_OK);
  check_equal(orm_query_execute(query, &result, error), ORM_STATUS_OK);
  check_equal(orm_result_get_text(result, 0u, 0u, &value, error), ORM_STATUS_OK);
  check_equal(value.len, strlen(expected));
  check_equal(memcmp(value.data, expected, value.len), 0);
  orm_result_destroy(result);
  orm_query_destroy(query);
}

static void inbox_db_provision_with_records_ddl(inbox_db_fixture_t *fixture, int old_schema,
                                                const char *records_ddl) {
  orm_error_t error;
  orm_connection_t *connection = inbox_db_connect(fixture, &error);

  inbox_db_execute(connection, INBOX_META_DDL, &error);
  inbox_db_execute(connection, records_ddl, &error);
  inbox_db_execute(connection, INBOX_DEDUPE_INDEX_DDL, &error);
  inbox_db_execute(connection, INBOX_PHASE_INDEX_DDL, &error);
  inbox_db_execute(connection, old_schema ? INBOX_META_OLD_ROW : INBOX_META_V3_ROW, &error);
  orm_disconnect(connection);
}

static void inbox_db_provision(inbox_db_fixture_t *fixture, int old_schema) {
  inbox_db_provision_with_records_ddl(fixture, old_schema, INBOX_RECORDS_DDL);
}

static turbo_flow_turbodb_inbox_config_t inbox_test_config(const inbox_db_fixture_t *fixture) {
  turbo_flow_turbodb_inbox_config_t config = turbo_flow_turbodb_inbox_config_default();
  config.database = &fixture->database;
  config.namespace_name = "orders";
  config.max_records = INBOX_TEST_MAX_RECORDS;
  config.max_total_bytes = INBOX_TEST_MAX_TOTAL_BYTES;
  config.max_record_bytes = INBOX_TEST_MAX_RECORD_BYTES;
  config.max_claims = INBOX_TEST_MAX_CLAIMS;
  config.connection_count = 2u;
  return config;
}

static turbo_flow_inbox_record_t inbox_test_record(const char *source_id, const char *admission_id,
                                                   const char *correlation, const char *payload,
                                                   uint64_t source_sequence,
                                                   uint64_t timestamp_ns) {
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_id = vstr_from_buf(source_id, strlen(source_id));
  record.partition_key = record.source_id;
  record.admission_id = vstr_from_buf(admission_id, strlen(admission_id));
  record.source_sequence = source_sequence;
  record.timestamp_ns = timestamp_ns;
  record.message_type = 17u;
  record.message_flags = 23u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "orders/created"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "orders.v2",
                                                           "OrderCreated", 3u),
              SALTS_OK);
  record.correlation = vstr_from_buf(correlation, strlen(correlation));
  record.payload = vstr_from_buf(payload, strlen(payload));
  return record;
}

static void check_view(vstr actual, vstr expected) {
  check_equal(actual.len, expected.len);
  check_equal(memcmp(actual.data, expected.data, expected.len), 0);
}

static void check_claim_matches(const turbo_flow_inbox_claim_t *claim,
                                const turbo_flow_inbox_record_t *record) {
  check_view(claim->record.source_id, record->source_id);
  check_view(claim->record.partition_key, record->partition_key);
  check_view(claim->record.admission_id, record->admission_id);
  check_equal(claim->record.source_sequence, record->source_sequence);
  check_equal(claim->record.timestamp_ns, record->timestamp_ns);
  check_equal(claim->record.message_type, record->message_type);
  check_equal(claim->record.message_flags, record->message_flags);
  check_equal(claim->record.content.domain, record->content.domain);
  check_equal(claim->record.content.profile, record->content.profile);
  check_equal(claim->record.content.encoding, record->content.encoding);
  check_equal(claim->record.content.flags, record->content.flags);
  check_equal(claim->record.content.schema_version, record->content.schema_version);
  check_equal(strcmp(claim->record.content.media_type, record->content.media_type), 0);
  check_equal(strcmp(claim->record.content.schema_name, record->content.schema_name), 0);
  check_equal(strcmp(claim->record.content.type_name, record->content.type_name), 0);
  check_equal(strcmp(claim->record.content.identity, record->content.identity), 0);
  check_view(claim->record.correlation, record->correlation);
  check_view(claim->record.payload, record->payload);
}

static void inbox_test_seed_completed_record(inbox_db_fixture_t *fixture) {
  turbo_flow_turbodb_inbox_config_t config = inbox_test_config(fixture);
  turbo_flow_inbox_record_t record =
      inbox_test_record("http.orders", "preflight-1", "order-1", "payload", 1u, 2u);
  turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
  orm_error_t error;

  check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
  check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
  check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
  check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
  check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
  check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
}

static void inbox_test_close_created(turbo_flow_inbox_t *inbox, int create_status) {
  if (create_status != SALTS_OK) return;
  check_equal(turbo_flow_inbox_close(inbox), SALTS_OK);
  check_equal(turbo_flow_inbox_destroy(inbox), SALTS_OK);
}

spec("TurboDB durable inbox v3") {
  it("provides v3 defaults and rejects non-file-backed SQLite or other drivers") {
    turbo_flow_turbodb_inbox_config_t config = turbo_flow_turbodb_inbox_config_default();
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_config_t database;
    orm_option_t filename;
    orm_error_t error;

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_TURBODB_INBOX_API_VERSION);
    check_equal(config.open_mode, TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE);
    check_equal(config.expected_generation, (uint64_t)0u);
    check_equal(config.connection_count, TURBO_FLOW_TURBODB_INBOX_DEFAULT_CONNECTIONS);

    orm_config(&database);
    filename = (orm_option_t){orm_view("filename"), orm_view(":memory:")};
    database.driver = orm_view("sqlite");
    database.options = &filename;
    database.option_count = 1u;
    config.database = &database;
    config.namespace_name = "orders";
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_ENOTSUP);
    check_null(inbox.ops);

    database.driver = orm_view("postgresql");
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_ENOTSUP);
    check_null(inbox.ops);
  }

  it("rejects a nested ORM configuration ABI mismatch before using its fields") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    config = inbox_test_config(&fixture);
    fixture.database.struct_size = 0u;
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_EINVAL);
    check_null(inbox.ops);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects required indexes owned by another table") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "DROP INDEX orders_inbox_records_v3_admission", &error);
    inbox_db_execute(connection,
                     "CREATE TABLE decoy (source_id bytea not null, admission_id bytea not null)",
                     &error);
    inbox_db_execute(connection,
                     "CREATE UNIQUE INDEX orders_inbox_records_v3_admission ON "
                     "decoy(source_id, admission_id)",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects a partial required index") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "DROP INDEX orders_inbox_records_v3_admission", &error);
    inbox_db_execute(connection,
                     "CREATE UNIQUE INDEX orders_inbox_records_v3_admission ON "
                     "orders_inbox_records_v3(source_id, admission_id) WHERE phase=0",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects persisted records whose descriptor fields are not coherent") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);

    inbox_db_execute(connection, "UPDATE orders_inbox_records_v3 SET content_profile=1", &error);
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);

    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET content_profile=0, "
                     "content_schema_version=0",
                     &error);
    inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);

    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET content_schema_version=3, "
                     "content_flags=9",
                     &error);
    inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);

    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET content_flags=1, content_encoding=5, "
                     "content_media_type='application/json'",
                     &error);
    inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);

    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET content_encoding=1, "
                     "content_schema_name='orders'||char(0)||'.v2'",
                     &error);
    inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    orm_disconnect(connection);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects CLOSED metadata with an in-flight record") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET phase=1, terminal_kind=0, "
                     "claim_generation=1, claim_token=1",
                     &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_meta_v3 SET records=1, history_records=0, "
                     "in_flight_claims=1",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects CLOSED metadata with a pending record") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET phase=0, claim_generation=0, "
                     "claim_token=0, terminal_kind=0",
                     &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_meta_v3 SET records=1, history_records=0, "
                     "pending_records=1, completed=0",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    if (rc == SALTS_OK) {
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
      inbox_test_close_created(&inbox, rc);
    }
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects impossible cumulative metadata counters") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET admitted=0", &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects non-canonical SQLite storage classes") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET source_id=CAST('http.orders' AS TEXT)",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects a claimed record from a generation other than the active owner") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    config.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
    config.expected_generation = 1u;
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET phase=1, terminal_kind=0, "
                     "claim_generation=9, claim_token=1",
                     &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_meta_v3 SET owner_state=1, records=1, "
                     "history_records=0, in_flight_claims=1",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    if (rc == SALTS_OK) {
      connection = inbox_db_connect(&fixture, &error);
      inbox_db_execute(connection,
                       "UPDATE orders_inbox_records_v3 SET phase=0, claim_generation=0, "
                       "claim_token=0",
                       &error);
      inbox_db_execute(connection,
                       "UPDATE orders_inbox_meta_v3 SET pending_records=1, "
                       "in_flight_claims=0",
                       &error);
      orm_disconnect(connection);
      turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
      inbox_test_close_created(&inbox, rc);
    }
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects takeover before owner-lost accounting can overflow") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    config.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
    config.expected_generation = 1u;
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET phase=1, terminal_kind=0, "
                     "claim_generation=1, claim_token=1",
                     &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_meta_v3 SET owner_state=1, records=1, "
                     "history_records=0, in_flight_claims=1, failed=9223372036854775807",
                     &error);
    orm_disconnect(connection);

    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_ERANGE);
    if (rc == SALTS_OK) {
      connection = inbox_db_connect(&fixture, &error);
      inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET failed=0", &error);
      orm_disconnect(connection);
      check_equal(turbo_flow_inbox_discard(&inbox, 1u), SALTS_OK);
      inbox_test_close_created(&inbox, rc);
    }
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects persisted next identifiers that do not exceed retained identifiers") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    inbox_test_seed_completed_record(&fixture);
    config = inbox_test_config(&fixture);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET next_record_id=1", &error);
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);

    inbox_db_execute(
        connection, "UPDATE orders_inbox_meta_v3 SET next_record_id=2, next_claim_token=1", &error);
    inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    orm_disconnect(connection);
    inbox_db_fixture_destroy(&fixture);
  }

  it("opens an exhausted identifier namespace but rejects identifier allocation") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("http.orders", "exhausted-1", "order-1", "payload", 1u, 2u);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_meta_v3 SET next_record_id=9223372036854775807, "
                     "next_claim_token=9223372036854775807",
                     &error);
    orm_disconnect(connection);

    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_ERANGE);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_ENOENT);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("opens a namespace with an exhausted admit counter so existing work can drain") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_turbodb_inbox_config_t takeover;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t old_inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_t new_inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("http.orders", "drain-1", "order-1", "payload", 1u, 2u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &old_inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&old_inbox, &record, &receipt), SALTS_OK);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET admitted=9223372036854775807",
                     &error);
    orm_disconnect(connection);

    takeover = config;
    takeover.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
    takeover.expected_generation = 1u;
    check_equal(turbo_flow_turbodb_inbox_create(&takeover, &new_inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&old_inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&new_inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&new_inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&new_inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&new_inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects each transition before its cumulative counter can overflow") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("http.orders", "overflow-1", "order-1", "payload", 1u, 2u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET admitted=9223372036854775807",
                     &error);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_ERANGE);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET admitted=0", &error);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);

    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET completed=9223372036854775807",
                     &error);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_ERANGE);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET completed=0", &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET failed=9223372036854775807",
                     &error);
    check_equal(turbo_flow_inbox_fail(&inbox, &claim, SALTS_EIO), SALTS_ERANGE);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET failed=0", &error);
    check_equal(turbo_flow_inbox_fail(&inbox, &claim, SALTS_EIO), SALTS_OK);

    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET retried=9223372036854775807",
                     &error);
    check_equal(turbo_flow_inbox_retry(&inbox, receipt.record_id), SALTS_ERANGE);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET retried=0", &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET discarded=9223372036854775807",
                     &error);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_ERANGE);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET discarded=0", &error);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);
    orm_disconnect(connection);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects missing old and inconsistent v3 metadata without repair") {
    inbox_db_fixture_t missing;
    inbox_db_fixture_t old;
    inbox_db_fixture_t invalid;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;

    inbox_db_fixture_init(&missing);
    config = inbox_test_config(&missing);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_EPROTO);
    connection = inbox_db_connect(&missing, &error);
    check_equal(inbox_db_read_int64(connection,
                                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND "
                                    "name='orders_inbox_meta_v3'",
                                    &error),
                (int64_t)0);
    orm_disconnect(connection);
    inbox_db_fixture_destroy(&missing);

    inbox_db_fixture_init(&old);
    inbox_db_provision(&old, 1);
    config = inbox_test_config(&old);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_EPROTO);
    connection = inbox_db_connect(&old, &error);
    check_equal(
        inbox_db_read_int64(connection, "SELECT schema_version FROM orders_inbox_meta_v3", &error),
        (int64_t)2);
    orm_disconnect(connection);
    inbox_db_fixture_destroy(&old);

    inbox_db_fixture_init(&invalid);
    inbox_db_provision(&invalid, 0);
    config = inbox_test_config(&invalid);
    connection = inbox_db_connect(&invalid, &error);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET schema_magic='wrong'", &error);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_EPROTO);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_meta_v3 SET schema_magic='turbo-flow.turbodb.inbox', "
                     "max_records=5",
                     &error);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_EPROTO);
    inbox_db_execute(connection, "UPDATE orders_inbox_meta_v3 SET max_records=4, singleton_id=2",
                     &error);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_EPROTO);
    orm_disconnect(connection);
    inbox_db_fixture_destroy(&invalid);
  }

  it("rejects a nullable records primary key in the exact v3 schema") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_error_t error;
    int rc;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision_with_records_ddl(&fixture, 0, INBOX_RECORDS_NULLABLE_PRIMARY_KEY_DDL);
    config = inbox_test_config(&fixture);
    rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
    check_equal(rc, SALTS_EPROTO);
    inbox_test_close_created(&inbox, rc);
    inbox_db_fixture_destroy(&fixture);
  }

  it("persists idempotent records and canonical unsigned values across a clean reopen") {
    const char correlation[] = "order-41";
    const char payload[] = "{\"id\":41}";
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_record_t conflict;
    turbo_flow_inbox_record_t partition_conflict;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t replay = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t rejected = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t partition_rejected = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;
    size_t history_count = 0u;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("http.orders", "request-41", correlation, payload, UINT64_MAX,
                               UINT64_C(0x8000000000000001));
    conflict = record;
    conflict.message_flags += 1u;
    partition_conflict = record;
    partition_conflict.partition_key =
        vstr_from_buf("other-partition", sizeof("other-partition") - 1u);

    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)1u);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &replay), SALTS_OK);
    check_equal(replay.record_id, receipt.record_id);
    check_equal(turbo_flow_inbox_admit(&inbox, &conflict, &rejected), SALTS_EPROTO);
    check_equal(rejected.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_admit(&inbox, &partition_conflict, &partition_rejected),
                SALTS_EPROTO);
    check_equal(partition_rejected.record_id, (uint64_t)0u);

    connection = inbox_db_connect(&fixture, &error);
    inbox_db_check_text(connection, "SELECT hex(source_sequence_be) FROM orders_inbox_records_v3",
                        "FFFFFFFFFFFFFFFF", &error);
    inbox_db_check_text(connection, "SELECT hex(timestamp_ns_be) FROM orders_inbox_records_v3",
                        "8000000000000001", &error);
    check_equal(
        inbox_db_read_int64(connection, "SELECT owner_state FROM orders_inbox_meta_v3", &error),
        (int64_t)1);
    orm_disconnect(connection);

    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(claim.record_id, receipt.record_id);
    check_claim_matches(&claim, &record);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(snapshot.history_records, (size_t)1u);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);

    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    replay = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
    rejected = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &replay), SALTS_OK);
    check_equal(replay.record_id, receipt.record_id);
    check_equal(turbo_flow_inbox_admit(&inbox, &conflict, &rejected), SALTS_EPROTO);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)2u);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(snapshot.history_records, (size_t)1u);
    check_equal(snapshot.admitted, (uint64_t)1u);
    check_equal(snapshot.completed, (uint64_t)1u);
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, &history, 1u, &history_count), SALTS_OK);
    check_equal(history_count, (size_t)1u);
    check_equal(history.record_id, receipt.record_id);
    check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_ENOENT);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("preserves a canonical protocol envelope across explicit owner takeover") {
    static const uint8_t payload[] = {0xdeu, 0xadu};
    enum {
      encoded_capacity =
          TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_FIXED_BYTES +
          TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_VARIABLE_FIELDS *
              TURBO_FLOW_PROTOCOL_ENVELOPE_TBE_LENGTH_BYTES +
          1u + 1u + 1u + 0u + sizeof(payload)
    };
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_turbodb_inbox_config_t takeover;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t old_inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_t recovered_inbox = TURBO_FLOW_INBOX_INIT;
    ProtocolInboxEnvelope_builder_t builder;
    ProtocolInboxEnvelope_t envelope;
    DataBind *codec = NULL;
    orm_error_t error;
    DataBindError bind_error = DATA_BIND_ERROR_INIT;
    uint8_t encoded[encoded_capacity];

    check_true(ProtocolInboxEnvelope_builder_bind(&builder, encoded, sizeof(encoded)));
    check_true(
        ProtocolInboxEnvelope_envelopeVersion_set(&builder, ProtocolEnvelopeVersion_V1));
    check_true(ProtocolInboxEnvelope_protocol_set(&builder, ProtocolKind_Coap));
    check_true(ProtocolInboxEnvelope_direction_set(&builder, ProtocolDirection_Up));
    check_true(ProtocolInboxEnvelope_messageType_set(&builder, 17u));
    check_true(ProtocolInboxEnvelope_sequence_set(&builder, UINT64_C(9007199254740993)));
    check_true(ProtocolInboxEnvelope_protocolVersion_set(&builder, "v", 1u));
    check_true(ProtocolInboxEnvelope_deviceId_set(&builder, "d", 1u));
    check_true(ProtocolInboxEnvelope_operation_set(&builder, "o", 1u));
    check_true(ProtocolInboxEnvelope_correlationId_set(&builder, "", 0u));
    check_true(ProtocolInboxEnvelope_payload_set(&builder, payload, sizeof(payload)));

    turbo_flow_inbox_record_init(&record);
    record.source_id = vstr_from_buf("s", 1u);
    record.partition_key = record.source_id;
    record.admission_id = vstr_from_buf("a", 1u);
    record.source_sequence = UINT64_C(41);
    record.timestamp_ns = UINT64_C(123456789);
    record.message_type = 17u;
    check_equal(turbo_flow_content_descriptor_init(
                    &record.content, TURBO_FLOW_DOMAIN_DATA,
                    TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_TBE,
                    TURBO_FLOW_PROTOCOL_ENVELOPE_MEDIA_TYPE,
                    TURBO_FLOW_PROTOCOL_ENVELOPE_CONTENT_IDENTITY),
                SALTS_OK);
    check_equal(turbo_flow_content_descriptor_declare_schema(
                    &record.content, TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_NAME,
                    TURBO_FLOW_PROTOCOL_ENVELOPE_TYPE_NAME,
                    TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_VERSION),
                SALTS_OK);
    record.payload = vstr_from_buf(encoded, sizeof(encoded));

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &old_inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&old_inbox, &record, &receipt), SALTS_OK);

    takeover = config;
    takeover.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
    takeover.expected_generation = 1u;
    check_equal(turbo_flow_turbodb_inbox_create(&takeover, &recovered_inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&old_inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&recovered_inbox, &claim), SALTS_OK);
    check_true(claim.record_id != 0u);
    check_view(claim.record.source_id, vstr_from_buf("s", 1u));
    check_view(claim.record.admission_id, vstr_from_buf("a", 1u));
    check_equal(claim.record.source_sequence, UINT64_C(41));
    check_equal(claim.record.timestamp_ns, UINT64_C(123456789));
    check_equal(claim.record.content.domain, TURBO_FLOW_DOMAIN_DATA);
    check_equal(claim.record.content.profile, TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA);
    check_equal(claim.record.content.encoding, TURBO_FLOW_DATA_ENCODING_TBE);
    check_equal(strcmp(claim.record.content.schema_name, TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_NAME),
                0);
    check_equal(strcmp(claim.record.content.type_name, TURBO_FLOW_PROTOCOL_ENVELOPE_TYPE_NAME), 0);
    check_equal(claim.record.content.schema_version,
                TURBO_FLOW_PROTOCOL_ENVELOPE_SCHEMA_VERSION);
    ProtocolInboxEnvelope_init(&envelope);
    check_equal(TurboFlowProtocolInbox_codec_create(&codec, &bind_error), DATA_BIND_OK);
    check_equal(ProtocolInboxEnvelope_from_bin(codec, &envelope, claim.record.payload.data,
                                               claim.record.payload.len, &bind_error),
                DATA_BIND_OK);
    check_equal(envelope.envelopeVersion, ProtocolEnvelopeVersion_V1);
    check_equal(envelope.protocol, ProtocolKind_Coap);
    check_equal(envelope.direction, ProtocolDirection_Up);
    check_equal(envelope.messageType, 17u);
    check_equal(envelope.sequence, UINT64_C(9007199254740993));
    check_equal(envelope.protocolVersion, "v");
    check_equal(envelope.deviceId, "d");
    check_equal(envelope.operation, "o");
    check_equal(tbe_bytes_t_size(&envelope.payload), sizeof(payload));
    check_equal(tbe_bytes_t_data_const(&envelope.payload), payload, sizeof(payload));
    data_bind_free(codec);
    ProtocolInboxEnvelope_clear(&envelope);
    check_equal(turbo_flow_inbox_complete(&recovered_inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&recovered_inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&recovered_inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("scans processing failures in pages before explicit retry or discard") {
    const char *admissions[] = {"request-51", "request-52", "request-53"};
    const int statuses[] = {SALTS_EIO, SALTS_EPROTO, SALTS_EINVAL};
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_receipt_t receipts[3] = {TURBO_FLOW_INBOX_RECEIPT_INIT,
                                              TURBO_FLOW_INBOX_RECEIPT_INIT,
                                              TURBO_FLOW_INBOX_RECEIPT_INIT};
    turbo_flow_inbox_failed_entry_t page[2] = {TURBO_FLOW_INBOX_FAILED_ENTRY_INIT,
                                               TURBO_FLOW_INBOX_FAILED_ENTRY_INIT};
    turbo_flow_inbox_history_entry_t history[3] = {TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT,
                                                   TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT,
                                                   TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT};
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_error_t error;
    size_t count = 0u;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    for (size_t index = 0u; index < 3u; ++index) {
      turbo_flow_inbox_record_t record = inbox_test_record(
          "http.orders", admissions[index], admissions[index], "failed", 51u + index, 151u + index);
      check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipts[index]), SALTS_OK);
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(turbo_flow_inbox_fail(&inbox, &claim, statuses[index]), SALTS_OK);
    }

    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, page, 2u, &count), SALTS_OK);
    check_equal(count, (size_t)2u);
    for (size_t index = 0u; index < 2u; ++index) {
      check_equal(page[index].record_id, receipts[index].record_id);
      check_equal(page[index].status, statuses[index]);
      check_equal(page[index].kind, TURBO_FLOW_INBOX_FAILURE_PROCESSING);
    }
    page[0] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    page[1] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    check_equal(turbo_flow_inbox_scan_failed(&inbox, receipts[1].record_id, page, 2u, &count),
                SALTS_OK);
    check_equal(count, (size_t)1u);
    check_equal(page[0].record_id, receipts[2].record_id);
    check_equal(page[0].kind, TURBO_FLOW_INBOX_FAILURE_PROCESSING);

    check_equal(turbo_flow_inbox_retry(&inbox, receipts[0].record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_discard(&inbox, receipts[1].record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_discard(&inbox, receipts[2].record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, history, 3u, &count), SALTS_OK);
    check_equal(count, (size_t)3u);
    check_equal(history[0].kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);
    check_equal(history[1].kind, TURBO_FLOW_INBOX_TERMINAL_DISCARDED);
    check_equal(history[2].kind, TURBO_FLOW_INBOX_TERMINAL_DISCARDED);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("keeps an active owner exclusive and takeover marks old claims owner-lost") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_turbodb_inbox_config_t takeover;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t old_claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t recovered = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t old_inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_t blocked = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_t new_inbox = TURBO_FLOW_INBOX_INIT;
    orm_error_t error;
    size_t count = 0u;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("websocket.orders", "frame-71", "order-71", "recover", 71u, 171u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &old_inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&old_inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&old_inbox, &old_claim), SALTS_OK);

    check_equal(turbo_flow_turbodb_inbox_create(&config, &blocked, &error), SALTS_EBUSY);
    check_null(blocked.ops);
    takeover = config;
    takeover.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
    takeover.expected_generation = 0u;
    check_equal(turbo_flow_turbodb_inbox_create(&takeover, &blocked, &error), SALTS_EINVAL);
    takeover.expected_generation = 2u;
    check_equal(turbo_flow_turbodb_inbox_create(&takeover, &blocked, &error), SALTS_EBUSY);
    takeover.expected_generation = 1u;
    check_equal(turbo_flow_turbodb_inbox_create(&takeover, &new_inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&new_inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)2u);
    check_equal(snapshot.pending_records, (size_t)0u);
    check_equal(snapshot.failed_records, (size_t)1u);
    check_equal(turbo_flow_inbox_claim(&new_inbox, &recovered), SALTS_ENOENT);

    count = 99u;
    check_equal(turbo_flow_inbox_scan_failed(&old_inbox, 0u, &failed, 1u, &count), SALTS_EBUSY);
    check_equal(count, (size_t)0u);
    check_equal(turbo_flow_inbox_scan_history(&old_inbox, 0u, NULL, 0u, &count), SALTS_EBUSY);
    check_equal(count, (size_t)0u);
    check_equal(turbo_flow_inbox_destroy(&old_inbox), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_complete(&old_inbox, &old_claim), SALTS_ECANCELED);
    check_equal(old_claim.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_close(&old_inbox), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_destroy(&old_inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_scan_failed(&new_inbox, 0u, &failed, 1u, &count), SALTS_OK);
    check_equal(count, (size_t)1u);
    check_equal(failed.record_id, receipt.record_id);
    check_not_equal(failed.status, SALTS_OK);
    check_equal(failed.kind, TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN);
    check_equal(turbo_flow_inbox_retry(&new_inbox, failed.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&new_inbox, &recovered), SALTS_OK);
    check_equal(recovered.record_id, receipt.record_id);
    check_claim_matches(&recovered, &record);
    check_equal(turbo_flow_inbox_complete(&new_inbox, &recovered), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&new_inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&new_inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("recovers committed pending and claimed records after a literal process crash") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_turbodb_inbox_config_t takeover;
    turbo_flow_inbox_t blocked = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_t recovered = TURBO_FLOW_INBOX_INIT;
    turbo_flow_inbox_claim_t pending = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t retried = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_inbox_history_entry_t history[2] = {
        TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT, TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT};
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    orm_connection_t *connection = NULL;
    orm_error_t error;
    char command[4096];
    int command_size;
    size_t count = 0u;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);

    command_size = snprintf(command, sizeof(command), "\"%s\" \"%s\"",
                            TURBO_FLOW_TURBODB_INBOX_CRASH_HELPER, fixture.path);
    check_true(command_size > 0 && (size_t)command_size < sizeof(command));
    check_not_equal(system(command), 0);

    connection = inbox_db_connect(&fixture, &error);
    check_equal(inbox_db_read_int64(
                    connection, "SELECT generation FROM orders_inbox_meta_v3", &error),
                (int64_t)1);
    check_equal(inbox_db_read_int64(
                    connection, "SELECT owner_state FROM orders_inbox_meta_v3", &error),
                (int64_t)1);
    check_equal(inbox_db_read_int64(
                    connection, "SELECT records FROM orders_inbox_meta_v3", &error),
                (int64_t)2);
    check_equal(inbox_db_read_int64(
                    connection, "SELECT pending_records FROM orders_inbox_meta_v3", &error),
                (int64_t)1);
    check_equal(inbox_db_read_int64(
                    connection, "SELECT in_flight_claims FROM orders_inbox_meta_v3", &error),
                (int64_t)1);
    orm_disconnect(connection);
    connection = NULL;

    config = inbox_test_config(&fixture);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &blocked, &error), SALTS_EBUSY);
    check_null(blocked.ops);

    takeover = config;
    takeover.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER;
    takeover.expected_generation = 1u;
    check_equal(turbo_flow_turbodb_inbox_create(&takeover, &recovered, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&recovered, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)2u);
    check_equal(snapshot.records, (size_t)2u);
    check_equal(snapshot.pending_records, (size_t)1u);
    check_equal(snapshot.failed_records, (size_t)1u);
    check_equal(snapshot.in_flight_claims, (size_t)0u);

    check_equal(turbo_flow_inbox_claim(&recovered, &pending), SALTS_OK);
    check_view(pending.record.admission_id,
               vstr_from_buf("pending-after-crash", sizeof("pending-after-crash") - 1u));
    check_equal(turbo_flow_inbox_complete(&recovered, &pending), SALTS_OK);

    check_equal(turbo_flow_inbox_scan_failed(&recovered, 0u, &failed, 1u, &count), SALTS_OK);
    check_equal(count, (size_t)1u);
    check_equal(failed.kind, TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN);
    check_equal(turbo_flow_inbox_retry(&recovered, failed.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&recovered, &retried), SALTS_OK);
    check_equal(retried.record_id, failed.record_id);
    check_view(retried.record.admission_id,
               vstr_from_buf("claimed-before-crash", sizeof("claimed-before-crash") - 1u));
    check_equal(turbo_flow_inbox_complete(&recovered, &retried), SALTS_OK);

    count = 0u;
    check_equal(turbo_flow_inbox_scan_history(&recovered, 0u, history, 2u, &count), SALTS_OK);
    check_equal(count, (size_t)2u);
    check_equal(history[0].kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);
    check_equal(history[1].kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);

    check_equal(turbo_flow_inbox_close(&recovered), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&recovered), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("claims the oldest eligible canonical partition transactionally") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t records[3];
    turbo_flow_inbox_receipt_t receipts[3] = {
        TURBO_FLOW_INBOX_RECEIPT_INIT, TURBO_FLOW_INBOX_RECEIPT_INIT,
        TURBO_FLOW_INBOX_RECEIPT_INIT};
    turbo_flow_inbox_claim_t first = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t second = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t blocked = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_request_t request = TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    vstr excluded[1];
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    records[0] = inbox_test_record("same-source", "a-1", "a-1", "item", 1u, 101u);
    records[1] = inbox_test_record("same-source", "a-2", "a-2", "item", 2u, 102u);
    records[2] = inbox_test_record("same-source", "b-1", "b-1", "item", 3u, 103u);
    records[0].partition_key = vstr_from_buf("partition-A", sizeof("partition-A") - 1u);
    records[1].partition_key = vstr_from_buf("partition-A", sizeof("partition-A") - 1u);
    records[2].partition_key = vstr_from_buf("partition-B", sizeof("partition-B") - 1u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    for (size_t index = 0u; index < 3u; ++index)
      check_equal(turbo_flow_inbox_admit(&inbox, &records[index], &receipts[index]), SALTS_OK);

    request.ordering = TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &first), SALTS_OK);
    check_equal(first.record_id, receipts[0].record_id);
    excluded[0] = first.record.partition_key;
    request.excluded_partitions = excluded;
    request.excluded_partition_count = 1u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &second), SALTS_OK);
    check_equal(second.record_id, receipts[2].record_id);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)1u);
    check_equal(snapshot.in_flight_claims, (size_t)2u);

    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &blocked), SALTS_ENOSPC);
    check_equal(blocked.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_complete(&inbox, &second), SALTS_OK);
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &blocked), SALTS_ENOENT);
    check_equal(turbo_flow_inbox_complete(&inbox, &first), SALTS_OK);

    request.excluded_partitions = NULL;
    request.excluded_partition_count = 0u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &blocked), SALTS_OK);
    check_equal(blocked.record_id, receipts[1].record_id);
    check_equal(turbo_flow_inbox_complete(&inbox, &blocked), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("allows exactly one concurrent settlement of copied claims") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    inbox_settle_worker_t workers[2] = {0};
    salts_thread_t threads[2] = {NULL, NULL};
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("mqtt.orders", "message-concurrent", "order-concurrent", "item", 91u,
                               191u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    for (size_t index = 0u; index < 2u; ++index) {
      workers[index].inbox = &inbox;
      workers[index].claim = claim;
      workers[index].status = SALTS_EIO;
      check_equal(salts_thread_create(&threads[index], inbox_complete_worker, &workers[index]),
                  SALTS_OK);
    }
    for (size_t index = 0u; index < 2u; ++index) {
      check_equal(salts_thread_join(&threads[index]), SALTS_OK);
      salts_thread_destroy(&threads[index]);
    }
    check_true((workers[0].status == SALTS_OK && workers[1].status == SALTS_EALREADY) ||
               (workers[1].status == SALTS_OK && workers[0].status == SALTS_EALREADY));
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(snapshot.history_records, (size_t)1u);
    check_equal(snapshot.in_flight_claims, (size_t)0u);
    check_equal(snapshot.completed, (uint64_t)1u);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("rejects embedded NUL descriptor text introduced after open") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection = NULL;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("http.orders", "request-nul", "order-nul", "item", 92u, 192u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);

    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET content_media_type="
                     "CAST(X'6170706C69636174696F6E2F6A736F6E006576696C' AS TEXT)",
                     &error);
    orm_disconnect(connection);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_EPROTO);
    check_equal(claim.record_id, (uint64_t)0u);

    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection,
                     "UPDATE orders_inbox_records_v3 SET content_media_type='application/json'",
                     &error);
    orm_disconnect(connection);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("drains after close and enforces persisted capacity boundaries") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t records[5];
    turbo_flow_inbox_receipt_t receipts[5] = {
        TURBO_FLOW_INBOX_RECEIPT_INIT, TURBO_FLOW_INBOX_RECEIPT_INIT, TURBO_FLOW_INBOX_RECEIPT_INIT,
        TURBO_FLOW_INBOX_RECEIPT_INIT, TURBO_FLOW_INBOX_RECEIPT_INIT};
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    for (size_t index = 0u; index < 5u; ++index) {
      static const char *ids[] = {"request-81", "request-82", "request-83", "request-84",
                                  "request-85"};
      records[index] = inbox_test_record("mqtt.orders", ids[index], ids[index], "item", 81u + index,
                                         181u + index);
    }
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    for (size_t index = 0u; index < INBOX_TEST_MAX_RECORDS; ++index) {
      check_equal(turbo_flow_inbox_admit(&inbox, &records[index], &receipts[index]), SALTS_OK);
    }
    check_equal(turbo_flow_inbox_admit(&inbox, &records[4], &receipts[4]), SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_forget(&inbox, receipts[0].record_id), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(claim.record_id, receipts[0].record_id);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &records[4], &receipts[4]), SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_forget(&inbox, receipts[0].record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &records[4], &receipts[4]), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &records[4], &receipts[4]), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_EBUSY);
    for (size_t index = 0u; index < INBOX_TEST_MAX_RECORDS; ++index) {
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    }
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }

  it("returns a datastore error without admitting into an in-memory fallback") {
    inbox_db_fixture_t fixture;
    turbo_flow_turbodb_inbox_config_t config;
    turbo_flow_inbox_record_t record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    orm_connection_t *connection;
    orm_error_t error;

    inbox_db_fixture_init(&fixture);
    inbox_db_provision(&fixture, 0);
    config = inbox_test_config(&fixture);
    record = inbox_test_record("http.orders", "request-91", "order-91", "database", 91u, 191u);
    check_equal(turbo_flow_turbodb_inbox_create(&config, &inbox, &error), SALTS_OK);
    connection = inbox_db_connect(&fixture, &error);
    inbox_db_execute(connection, "DROP TABLE orders_inbox_records_v3", &error);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_EIO);
    check_equal(receipt.record_id, (uint64_t)0u);
    check_equal(inbox_db_read_int64(connection, "SELECT records FROM orders_inbox_meta_v3", &error),
                (int64_t)0);
    check_equal(
        inbox_db_read_int64(connection, "SELECT admitted FROM orders_inbox_meta_v3", &error),
        (int64_t)0);
    inbox_db_execute(connection, INBOX_RECORDS_DDL, &error);
    inbox_db_execute(connection, INBOX_DEDUPE_INDEX_DDL, &error);
    inbox_db_execute(connection, INBOX_PHASE_INDEX_DDL, &error);
    orm_disconnect(connection);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    inbox_db_fixture_destroy(&fixture);
  }
}
