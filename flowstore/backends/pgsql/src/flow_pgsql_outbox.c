#include "turbo_flow_pgsql.h"

#include "flow_timer.h"
#include "fmt.h"
#include "libpq-fe.h"
#include "turbo_thread.h"

#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_PGSQL_OUTBOX_TABLE "turbo_flow_outbox"
#define FLOW_PGSQL_OUTBOX_CAPACITY_LOCK_SEED "607599061"
#define FLOW_PGSQL_OUTBOX_ROW_LOCK_SEED "607599065"
#define FLOW_PGSQL_OUTBOX_STATE_PENDING "pending"
#define FLOW_PGSQL_OUTBOX_STATE_RETRY_WAIT "retry_wait"
#define FLOW_PGSQL_OUTBOX_STATE_DEAD_LETTER "dead_letter"
#define FLOW_PGSQL_OUTBOX_STATE_ARCHIVED "archived"

typedef struct flow_pgsql_outbox_adapter_s {
  tstr resource_uid;
  tstr resource_owner;
  tstr conninfo;
  tstr outbox_name;
  turbo_flow_pgsql_outbox_role_t role;
  size_t capacity;
  size_t max_payload_size;
  uint32_t poll_interval_ms;
  size_t claim_scan_limit;
  int create_table;
  turbo_flow_pgsql_outbox_completion_t completion;
  uint32_t max_delivery_attempts;
  uint32_t retry_delay_ms;
  uint32_t archive_ttl_ms;
  PGconn *connection;
  PGcancel *cancel;
  turbo_mutex_t lock;
  int lock_initialized;
  tf_timer_t poll_wait;
  int poll_wait_initialized;
  turbo_thread_t thread;
  int thread_started;
  atomic_int started;
  atomic_int last_status;
  atomic_uint_fast64_t accepted;
  atomic_uint_fast64_t delivered;
  atomic_uint_fast64_t requeued;
  atomic_uint_fast64_t failures;
  turbo_flow_t *flow;
  tstr source_name;
} flow_pgsql_outbox_adapter_t;

typedef struct flow_pgsql_outbox_record_s {
  tstr row_id;
  tstr payload;
  uint32_t message_type;
  uint32_t message_flags;
  uint32_t delivery_attempts;
} flow_pgsql_outbox_record_t;

static const turbo_flow_option_field_t FLOW_PGSQL_OUTBOX_FIELDS[] = {
    {"conninfo", TURBO_FLOW_OPTION_SECRET,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_SECRET_VALUE, 0u, 0u, NULL, 0u},
    {"outbox_name", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"capacity", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u,
     TURBO_FLOW_PGSQL_OUTBOX_MAX_CAPACITY, NULL, 0u},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u,
     TURBO_FLOW_PGSQL_OUTBOX_MAX_PAYLOAD_SIZE, NULL, 0u},
    {"poll_interval_ms", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN, 1u, 0u, NULL, 0u},
    {"claim_scan_limit", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u,
     TURBO_FLOW_PGSQL_OUTBOX_MAX_CLAIM_SCAN, NULL, 0u},
    {"create_table", TURBO_FLOW_OPTION_BOOL, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"completion", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"max_delivery_attempts", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MAX, 0u,
     TURBO_FLOW_PGSQL_OUTBOX_MAX_DELIVERY_ATTEMPTS, NULL, 0u},
    {"retry_delay_ms", TURBO_FLOW_OPTION_U32, 0u, 0u, 0u, NULL, 0u},
    {"archive_ttl_ms", TURBO_FLOW_OPTION_U32, 0u, 0u, 0u, NULL, 0u}};

static const turbo_flow_adapter_schema_t FLOW_PGSQL_OUTBOX_SINK_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_POSTGRESQL,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_PGSQL_OUTBOX_FIELDS,
    sizeof(FLOW_PGSQL_OUTBOX_FIELDS) / sizeof(FLOW_PGSQL_OUTBOX_FIELDS[0])};

static const turbo_flow_adapter_schema_t FLOW_PGSQL_OUTBOX_SOURCE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_POSTGRESQL,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_PGSQL_OUTBOX_FIELDS,
    sizeof(FLOW_PGSQL_OUTBOX_FIELDS) / sizeof(FLOW_PGSQL_OUTBOX_FIELDS[0])};

static const char FLOW_PGSQL_OUTBOX_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowPostgreSqlOutboxResource [id(303), version(1)];\n"
    "message PostgreSqlOutboxStatus {\n"
    "  bool started;\n"
    "  uint32 role;\n"
    "  int32 last_status;\n"
    "  string accepted;\n"
    "  string delivered;\n"
    "  string requeued;\n"
    "  string failures;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_PGSQL_OUTBOX_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_STORAGE,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowPostgreSqlOutboxResource",
    "PostgreSqlOutboxStatus",
    303u,
    1u,
    FLOW_PGSQL_OUTBOX_STATUS_SCHEMA_TEXT};

static int flow_pgsql_outbox_result_status(PGresult *result, ExecStatusType expected) {
  if (!result) return TURBO_EIO;
  return PQresultStatus(result) == expected ? TURBO_OK : TURBO_EIO;
}

static int flow_pgsql_outbox_exec_simple(PGconn *connection, const char *sql,
                                         ExecStatusType expected) {
  PGresult *result;
  int rc;
  if (!connection || !sql) return TURBO_EINVAL;
  result = PQexec(connection, sql);
  rc = flow_pgsql_outbox_result_status(result, expected);
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_schema_prepare(flow_pgsql_outbox_adapter_t *adapter) {
  static const char ddl[] =
      "CREATE TABLE IF NOT EXISTS " FLOW_PGSQL_OUTBOX_TABLE " ("
      "outbox_name text NOT NULL,"
      "id bigint GENERATED BY DEFAULT AS IDENTITY,"
      "payload bytea NOT NULL,"
      "message_type bigint NOT NULL DEFAULT 0,"
      "message_flags bigint NOT NULL DEFAULT 0,"
      "origin_protocol integer,"
      "origin_protocol_version bigint,"
      "origin_session_id numeric(20,0),"
      "delivery_state text NOT NULL DEFAULT '" FLOW_PGSQL_OUTBOX_STATE_PENDING "',"
      "delivery_attempts bigint NOT NULL DEFAULT 0,"
      "available_at timestamptz NOT NULL DEFAULT clock_timestamp(),"
      "completed_at timestamptz,"
      "last_error integer,"
      "created_at timestamptz NOT NULL DEFAULT clock_timestamp(),"
      "PRIMARY KEY(outbox_name,id));"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE
      " ADD COLUMN IF NOT EXISTS message_type bigint NOT NULL DEFAULT 0;"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE
      " ADD COLUMN IF NOT EXISTS message_flags bigint NOT NULL DEFAULT 0;"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE " ADD COLUMN IF NOT EXISTS origin_protocol integer;"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE
      " ADD COLUMN IF NOT EXISTS origin_protocol_version bigint;"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE
      " ADD COLUMN IF NOT EXISTS origin_session_id numeric(20,0);"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE " ADD COLUMN IF NOT EXISTS delivery_state text NOT "
      "NULL DEFAULT '" FLOW_PGSQL_OUTBOX_STATE_PENDING "';"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE
      " ADD COLUMN IF NOT EXISTS delivery_attempts bigint NOT NULL DEFAULT 0;"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE
      " ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT clock_timestamp();"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE " ADD COLUMN IF NOT EXISTS completed_at timestamptz;"
      "ALTER TABLE " FLOW_PGSQL_OUTBOX_TABLE " ADD COLUMN IF NOT EXISTS last_error integer;"
      "CREATE INDEX IF NOT EXISTS turbo_flow_outbox_pending_idx ON " FLOW_PGSQL_OUTBOX_TABLE
      "(outbox_name,id);"
      "CREATE INDEX IF NOT EXISTS turbo_flow_outbox_ready_idx ON " FLOW_PGSQL_OUTBOX_TABLE
      "(outbox_name,available_at,id) WHERE delivery_state IN ('" FLOW_PGSQL_OUTBOX_STATE_PENDING
      "','" FLOW_PGSQL_OUTBOX_STATE_RETRY_WAIT "');";
  static const char validate_legacy[] =
      "SELECT outbox_name::text,id::text,payload::bytea,message_type::text,message_flags::text,"
      "origin_protocol::text,origin_protocol_version::text,origin_session_id::text FROM "
      FLOW_PGSQL_OUTBOX_TABLE " LIMIT 0";
  static const char validate_lifecycle[] =
      "SELECT outbox_name::text,id::text,payload::bytea,message_type::text,message_flags::text,"
      "origin_protocol::text,origin_protocol_version::text,origin_session_id::text,"
      "delivery_state::text,delivery_attempts::text,available_at,completed_at,last_error::text FROM "
      FLOW_PGSQL_OUTBOX_TABLE " LIMIT 0";
  PGresult *result;
  int lifecycle;
  int rc;
  if (!adapter || !adapter->connection) return TURBO_EINVAL;
  lifecycle = adapter->completion == TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE ||
              adapter->max_delivery_attempts != 0u || adapter->archive_ttl_ms != 0u;
  if (adapter->create_table) {
    rc = flow_pgsql_outbox_exec_simple(adapter->connection, ddl, PGRES_COMMAND_OK);
    if (rc != TURBO_OK) return rc;
  }
  result = PQexec(adapter->connection, lifecycle ? validate_lifecycle : validate_legacy);
  rc = flow_pgsql_outbox_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQnfields(result) != (lifecycle ? 13 : 8)) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_connect(flow_pgsql_outbox_adapter_t *adapter) {
  int rc;
  if (!adapter || adapter->connection) return TURBO_EINVAL;
  adapter->connection = PQconnectdb(adapter->conninfo);
  if (!adapter->connection || PQstatus(adapter->connection) != CONNECTION_OK) {
    if (adapter->connection) PQfinish(adapter->connection);
    adapter->connection = NULL;
    return TURBO_EIO;
  }
  adapter->cancel = PQgetCancel(adapter->connection);
  if (!adapter->cancel) {
    PQfinish(adapter->connection);
    adapter->connection = NULL;
    return TURBO_ENOMEM;
  }
  rc = flow_pgsql_outbox_schema_prepare(adapter);
  if (rc != TURBO_OK) {
    PQfreeCancel(adapter->cancel);
    adapter->cancel = NULL;
    PQfinish(adapter->connection);
    adapter->connection = NULL;
  }
  return rc;
}

static void flow_pgsql_outbox_disconnect(flow_pgsql_outbox_adapter_t *adapter) {
  if (!adapter) return;
  if (adapter->cancel) PQfreeCancel(adapter->cancel);
  adapter->cancel = NULL;
  if (adapter->connection) PQfinish(adapter->connection);
  adapter->connection = NULL;
}

static int flow_pgsql_outbox_parse_count(PGresult *result, size_t *out) {
  const char *value;
  char *end = NULL;
  unsigned long long converted;
  if (!result || !out || PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1 ||
      PQnfields(result) != 1 || PQgetisnull(result, 0, 0))
    return TURBO_EPROTO;
  value = PQgetvalue(result, 0, 0);
  errno = 0;
  converted = strtoull(value, &end, 10);
  if (errno != 0 || !end || *end != '\0' || converted > SIZE_MAX) return TURBO_EPROTO;
  *out = (size_t)converted;
  return TURBO_OK;
}

static int flow_pgsql_outbox_sink_commit(flow_pgsql_outbox_adapter_t *adapter,
                                         const turbo_flow_msg_t *msg) {
  static const char capacity_lock[] =
      "SELECT pg_advisory_xact_lock(hashtextextended($1," FLOW_PGSQL_OUTBOX_CAPACITY_LOCK_SEED "))";
  static const char count_rows[] =
      "SELECT count(*)::text FROM " FLOW_PGSQL_OUTBOX_TABLE " WHERE outbox_name=$1";
  static const char count_active_rows[] =
      "SELECT count(*)::text FROM " FLOW_PGSQL_OUTBOX_TABLE
      " WHERE outbox_name=$1 AND delivery_state IN ('" FLOW_PGSQL_OUTBOX_STATE_PENDING "','" 
      FLOW_PGSQL_OUTBOX_STATE_RETRY_WAIT "')";
  static const char insert_row[] =
      "INSERT INTO " FLOW_PGSQL_OUTBOX_TABLE
      "(outbox_name,payload,message_type,message_flags) "
      "VALUES($1,$2::bytea,$3::bigint,$4::bigint)";
  const char *one_value[1] = {adapter->outbox_name};
  const char *insert_values[4] = {0};
  int insert_lengths[4] = {0};
  int insert_formats[4] = {0};
  char message_type[32];
  char message_flags[32];
  PGresult *result = NULL;
  size_t depth = 0u;
  int rc;
  int written;
  written = snprintf(message_type, sizeof(message_type), "%u", msg->type);
  if (written < 0 || (size_t)written >= sizeof(message_type)) return TURBO_ERANGE;
  written = snprintf(message_flags, sizeof(message_flags), "%u", msg->flags);
  if (written < 0 || (size_t)written >= sizeof(message_flags)) return TURBO_ERANGE;
  rc = flow_pgsql_outbox_exec_simple(adapter->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(adapter->connection, capacity_lock, 1, NULL, one_value, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_TUPLES_OK);
  if (result) PQclear(result);
  if (rc != TURBO_OK) goto rollback;
  result = PQexecParams(adapter->connection,
                        adapter->completion == TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE ||
                                adapter->max_delivery_attempts != 0u
                            ? count_active_rows
                            : count_rows,
                        1, NULL, one_value, NULL, NULL, 0);
  rc = flow_pgsql_outbox_parse_count(result, &depth);
  if (result) PQclear(result);
  if (rc != TURBO_OK) goto rollback;
  if (depth >= adapter->capacity) {
    rc = TURBO_ENOSPC;
    goto rollback;
  }
  insert_values[0] = adapter->outbox_name;
  insert_values[1] = msg->payload.data ? msg->payload.data : "";
  insert_values[2] = message_type;
  insert_values[3] = message_flags;
  insert_lengths[1] = (int)msg->payload.len;
  insert_formats[1] = 1;
  result = PQexecParams(adapter->connection, insert_row, 4, NULL, insert_values, insert_lengths,
                        insert_formats, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_COMMAND_OK);
  if (result) PQclear(result);
  if (rc != TURBO_OK) goto rollback;
  rc = flow_pgsql_outbox_exec_simple(adapter->connection, "COMMIT", PGRES_COMMAND_OK);
  if (rc == TURBO_OK) return TURBO_OK;

rollback:
  (void)flow_pgsql_outbox_exec_simple(adapter->connection, "ROLLBACK", PGRES_COMMAND_OK);
  return rc;
}

static int flow_pgsql_outbox_lock_row(flow_pgsql_outbox_adapter_t *adapter, const char *row_id,
                                      int *locked) {
  static const char sql[] = "SELECT pg_try_advisory_lock(hashtextextended($1 || ':' || "
                            "$2," FLOW_PGSQL_OUTBOX_ROW_LOCK_SEED "))";
  const char *values[2] = {adapter->outbox_name, row_id};
  PGresult *result;
  int rc;
  if (!locked) return TURBO_EINVAL;
  *locked = 0;
  result = PQexecParams(adapter->connection, sql, 2, NULL, values, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK &&
      (PQntuples(result) != 1 || PQnfields(result) != 1 || PQgetisnull(result, 0, 0)))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    const char *value = PQgetvalue(result, 0, 0);
    if (strcmp(value, "t") == 0) *locked = 1;
    else if (strcmp(value, "f") != 0) rc = TURBO_EPROTO;
  }
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_unlock_row(flow_pgsql_outbox_adapter_t *adapter, const char *row_id) {
  static const char sql[] =
      "SELECT pg_advisory_unlock(hashtextextended($1 || ':' || $2," FLOW_PGSQL_OUTBOX_ROW_LOCK_SEED
      "))";
  const char *values[2] = {adapter->outbox_name, row_id};
  PGresult *result;
  int rc;
  result = PQexecParams(adapter->connection, sql, 2, NULL, values, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1 ||
                         PQgetisnull(result, 0, 0) || strcmp(PQgetvalue(result, 0, 0), "t") != 0))
    rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_parse_u64(const char *text, uint64_t maximum, uint64_t *out) {
  char *end = NULL;
  unsigned long long value;
  if (!text || !text[0] || !out) return TURBO_EINVAL;
  for (const unsigned char *cursor = (const unsigned char *)text; *cursor; ++cursor) {
    if (*cursor < (unsigned char)'0' || *cursor > (unsigned char)'9') return TURBO_EPROTO;
  }
  errno = 0;
  value = strtoull(text, &end, 10);
  if (errno != 0 || !end || *end != '\0' || value > maximum) return TURBO_EPROTO;
  *out = (uint64_t)value;
  return TURBO_OK;
}

static void flow_pgsql_outbox_record_cleanup(flow_pgsql_outbox_record_t *record) {
  if (!record) return;
  tstr_freep(&record->row_id);
  tstr_freep(&record->payload);
  *record = (flow_pgsql_outbox_record_t){0};
}

static int flow_pgsql_outbox_metadata_read(PGresult *selected,
                                           flow_pgsql_outbox_record_t *record, int lifecycle) {
  uint64_t value;
  int rc;
  if (!selected || !record || PQresultStatus(selected) != PGRES_TUPLES_OK ||
      PQntuples(selected) != 1 || PQnfields(selected) != (lifecycle ? 3 : 2) ||
      PQgetisnull(selected, 0, 0) ||
      PQgetisnull(selected, 0, 1))
    return TURBO_EPROTO;
  rc = flow_pgsql_outbox_parse_u64(PQgetvalue(selected, 0, 0), UINT32_MAX, &value);
  if (rc != TURBO_OK) return rc;
  record->message_type = (uint32_t)value;
  rc = flow_pgsql_outbox_parse_u64(PQgetvalue(selected, 0, 1), UINT32_MAX, &value);
  if (rc != TURBO_OK) return rc;
  record->message_flags = (uint32_t)value;
  if (!lifecycle) return TURBO_OK;
  if (PQgetisnull(selected, 0, 2)) return TURBO_EPROTO;
  rc = flow_pgsql_outbox_parse_u64(PQgetvalue(selected, 0, 2),
                                   TURBO_FLOW_PGSQL_OUTBOX_MAX_DELIVERY_ATTEMPTS, &value);
  if (rc != TURBO_OK) return rc;
  record->delivery_attempts = (uint32_t)value;
  return TURBO_OK;
}

static int flow_pgsql_outbox_claim(flow_pgsql_outbox_adapter_t *adapter,
                                   flow_pgsql_outbox_record_t *record) {
  static const char candidates_sql[] = "SELECT id::text FROM " FLOW_PGSQL_OUTBOX_TABLE
                                       " WHERE outbox_name=$1 ORDER BY id LIMIT $2::integer";
  static const char lifecycle_candidates_sql[] =
      "SELECT id::text FROM " FLOW_PGSQL_OUTBOX_TABLE
      " WHERE outbox_name=$1 AND delivery_state IN ('" FLOW_PGSQL_OUTBOX_STATE_PENDING "','" 
      FLOW_PGSQL_OUTBOX_STATE_RETRY_WAIT
      "') AND available_at<=clock_timestamp() ORDER BY available_at,id LIMIT $2::integer";
  static const char metadata_sql[] =
      "SELECT message_type::text,message_flags::text FROM " FLOW_PGSQL_OUTBOX_TABLE
      " WHERE outbox_name=$1 AND id=$2::bigint";
  static const char lifecycle_metadata_sql[] =
      "SELECT message_type::text,message_flags::text,delivery_attempts::text FROM "
      FLOW_PGSQL_OUTBOX_TABLE " WHERE outbox_name=$1 AND id=$2::bigint";
  static const char payload_sql[] =
      "SELECT payload FROM " FLOW_PGSQL_OUTBOX_TABLE " WHERE outbox_name=$1 AND id=$2::bigint";
  char scan_limit[32];
  const char *candidate_values[2] = {adapter->outbox_name, scan_limit};
  PGresult *candidates = NULL;
  int lifecycle;
  int written;
  int rc;
  if (!record || record->row_id || record->payload) return TURBO_EINVAL;
  lifecycle = adapter->completion == TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE ||
              adapter->max_delivery_attempts != 0u || adapter->archive_ttl_ms != 0u;
  written = snprintf(scan_limit, sizeof(scan_limit), "%zu", adapter->claim_scan_limit);
  if (written < 0 || (size_t)written >= sizeof(scan_limit)) return TURBO_ERANGE;
  candidates = PQexecParams(adapter->connection,
                            lifecycle ? lifecycle_candidates_sql : candidates_sql, 2, NULL,
                            candidate_values, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(candidates, PGRES_TUPLES_OK);
  if (rc != TURBO_OK) goto done;
  if (PQnfields(candidates) != 1) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = TURBO_ENOENT;
  for (int row = 0; row < PQntuples(candidates); ++row) {
    const char *candidate_id;
    const char *payload_values[2];
    PGresult *selected = NULL;
    int locked = 0;
    if (PQgetisnull(candidates, row, 0)) {
      rc = TURBO_EPROTO;
      break;
    }
    candidate_id = PQgetvalue(candidates, row, 0);
    rc = flow_pgsql_outbox_lock_row(adapter, candidate_id, &locked);
    if (rc != TURBO_OK) break;
    if (!locked) {
      rc = TURBO_ENOENT;
      continue;
    }
    record->row_id = tstr_dup(candidate_id);
    if (!record->row_id) {
      (void)flow_pgsql_outbox_unlock_row(adapter, candidate_id);
      rc = TURBO_ENOMEM;
      break;
    }
    payload_values[0] = adapter->outbox_name;
    payload_values[1] = record->row_id;
    selected = PQexecParams(adapter->connection,
                            lifecycle ? lifecycle_metadata_sql : metadata_sql, 2, NULL,
                            payload_values, NULL, NULL, 0);
    rc = flow_pgsql_outbox_metadata_read(selected, record, lifecycle);
    if (selected) PQclear(selected);
    selected = NULL;
    if (rc != TURBO_OK) goto selected_done;
    selected =
        PQexecParams(adapter->connection, payload_sql, 2, NULL, payload_values, NULL, NULL, 1);
    rc = flow_pgsql_outbox_result_status(selected, PGRES_TUPLES_OK);
    if (rc == TURBO_OK &&
        (PQntuples(selected) != 1 || PQnfields(selected) != 1 || PQgetisnull(selected, 0, 0)))
      rc = PQntuples(selected) == 0 ? TURBO_ENOENT : TURBO_EPROTO;
    if (rc == TURBO_OK) {
      int payload_size = PQgetlength(selected, 0, 0);
      if (payload_size < 0 || (size_t)payload_size > adapter->max_payload_size) rc = TURBO_EMSGSIZE;
      else {
        record->payload = tstr_new_len(PQgetvalue(selected, 0, 0), (size_t)payload_size);
        if (!record->payload) rc = TURBO_ENOMEM;
      }
    }
selected_done:
    if (selected) PQclear(selected);
    if (rc == TURBO_OK) break;
    (void)flow_pgsql_outbox_unlock_row(adapter, record->row_id);
    flow_pgsql_outbox_record_cleanup(record);
    if (rc != TURBO_ENOENT) break;
  }

done:
  if (candidates) PQclear(candidates);
  return rc;
}

static int flow_pgsql_outbox_delete(flow_pgsql_outbox_adapter_t *adapter, const char *row_id) {
  static const char sql[] =
      "DELETE FROM " FLOW_PGSQL_OUTBOX_TABLE " WHERE outbox_name=$1 AND id=$2::bigint";
  const char *values[2] = {adapter->outbox_name, row_id};
  PGresult *result;
  const char *affected;
  int rc;
  result = PQexecParams(adapter->connection, sql, 2, NULL, values, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK) {
    affected = PQcmdTuples(result);
    if (!affected || strcmp(affected, "1") != 0) rc = TURBO_EALREADY;
  }
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_update_state(flow_pgsql_outbox_adapter_t *adapter, const char *row_id,
                                          const char *state, uint32_t attempts, int error,
                                          uint32_t delay_ms) {
  static const char sql[] =
      "UPDATE " FLOW_PGSQL_OUTBOX_TABLE
      " SET delivery_state=$3,delivery_attempts=$4::bigint,"
      "last_error=CASE WHEN $3='" FLOW_PGSQL_OUTBOX_STATE_ARCHIVED
      "' THEN NULL ELSE $5::integer END,"
      "available_at=clock_timestamp()+($6::bigint*interval '1 millisecond'),"
      "completed_at=CASE WHEN $3='" FLOW_PGSQL_OUTBOX_STATE_ARCHIVED
      "' THEN clock_timestamp() ELSE NULL END "
      "WHERE outbox_name=$1 AND id=$2::bigint";
  const char *values[6] = {adapter->outbox_name, row_id, state, NULL, NULL, NULL};
  char attempts_text[32];
  char error_text[32];
  char delay_text[32];
  PGresult *result;
  const char *affected;
  int written;
  int rc;
  written = snprintf(attempts_text, sizeof(attempts_text), "%u", attempts);
  if (written < 0 || (size_t)written >= sizeof(attempts_text)) return TURBO_ERANGE;
  written = snprintf(error_text, sizeof(error_text), "%d", error);
  if (written < 0 || (size_t)written >= sizeof(error_text)) return TURBO_ERANGE;
  written = snprintf(delay_text, sizeof(delay_text), "%u", delay_ms);
  if (written < 0 || (size_t)written >= sizeof(delay_text)) return TURBO_ERANGE;
  values[3] = attempts_text;
  values[4] = error_text;
  values[5] = delay_text;
  result = PQexecParams(adapter->connection, sql, 6, NULL, values, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK) {
    affected = PQcmdTuples(result);
    if (!affected || strcmp(affected, "1") != 0) rc = TURBO_EALREADY;
  }
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_expire_archived(flow_pgsql_outbox_adapter_t *adapter) {
  static const char sql[] =
      "DELETE FROM " FLOW_PGSQL_OUTBOX_TABLE
      " WHERE outbox_name=$1 AND delivery_state='" FLOW_PGSQL_OUTBOX_STATE_ARCHIVED
      "' AND completed_at<=clock_timestamp()-($2::bigint*interval '1 millisecond')";
  const char *values[2] = {adapter->outbox_name, NULL};
  char ttl_text[32];
  PGresult *result;
  int written;
  int rc;
  if (adapter->archive_ttl_ms == 0u) return TURBO_OK;
  written = snprintf(ttl_text, sizeof(ttl_text), "%u", adapter->archive_ttl_ms);
  if (written < 0 || (size_t)written >= sizeof(ttl_text)) return TURBO_ERANGE;
  values[1] = ttl_text;
  result = PQexecParams(adapter->connection, sql, 2, NULL, values, NULL, NULL, 0);
  rc = flow_pgsql_outbox_result_status(result, PGRES_COMMAND_OK);
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_outbox_source_once(flow_pgsql_outbox_adapter_t *adapter) {
  flow_pgsql_outbox_record_t record = {0};
  turbo_flow_msg_t message;
  int rc;
  int delivery_rc;
  int unlock_rc;
  rc = flow_pgsql_outbox_claim(adapter, &record);
  if (rc != TURBO_OK) return rc;
  turbo_flow_msg_init(&message);
  message.type = record.message_type;
  message.flags = record.message_flags;
  if (adapter->max_delivery_attempts != 0u)
    message.execution_attempt = record.delivery_attempts + 1u;
  message.owned_payload = tstr_move(&record.payload);
  message.payload = tstr_to_v(message.owned_payload);
  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &message);
  turbo_flow_msg_cleanup(&message);
  delivery_rc = rc;
  if (delivery_rc == TURBO_OK) {
    if (adapter->completion == TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE) {
      rc = flow_pgsql_outbox_update_state(adapter, record.row_id,
                                          FLOW_PGSQL_OUTBOX_STATE_ARCHIVED,
                                          record.delivery_attempts, TURBO_OK, 0u);
      if (rc == TURBO_OK) rc = flow_pgsql_outbox_expire_archived(adapter);
    } else {
      rc = flow_pgsql_outbox_delete(adapter, record.row_id);
    }
  } else if (adapter->max_delivery_attempts != 0u) {
    uint32_t attempts = record.delivery_attempts + 1u;
    const int dead_letter = attempts >= adapter->max_delivery_attempts;
    rc = flow_pgsql_outbox_update_state(
        adapter, record.row_id,
        dead_letter ? FLOW_PGSQL_OUTBOX_STATE_DEAD_LETTER
                    : FLOW_PGSQL_OUTBOX_STATE_RETRY_WAIT,
        attempts, delivery_rc, dead_letter ? 0u : adapter->retry_delay_ms);
    if (rc == TURBO_OK)
      (void)atomic_fetch_add_explicit(&adapter->requeued, 1u, memory_order_relaxed);
  } else {
    (void)atomic_fetch_add_explicit(&adapter->requeued, 1u, memory_order_relaxed);
  }
  unlock_rc = flow_pgsql_outbox_unlock_row(adapter, record.row_id);
  flow_pgsql_outbox_record_cleanup(&record);
  if (rc == TURBO_OK && unlock_rc != TURBO_OK) rc = unlock_rc;
  if (rc == TURBO_OK && delivery_rc == TURBO_OK)
    (void)atomic_fetch_add_explicit(&adapter->delivered, 1u, memory_order_relaxed);
  return rc;
}

static void flow_pgsql_outbox_source_thread(void *arg) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)arg;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    if (tf_timer_wait_for_ms(&adapter->poll_wait, 1u) == TURBO_ESHUTDOWN) return;
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    int rc = flow_pgsql_outbox_source_once(adapter);
    if (rc == TURBO_OK) continue;
    if (rc == TURBO_ENOENT) {
      if (tf_timer_wait_for_ms(&adapter->poll_wait, adapter->poll_interval_ms) == TURBO_ESHUTDOWN)
        break;
      continue;
    }
    atomic_store_explicit(&adapter->last_status, rc, memory_order_release);
    (void)atomic_fetch_add_explicit(&adapter->failures, 1u, memory_order_relaxed);
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    break;
  }
}

static int flow_pgsql_outbox_start(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)ctx;
  int rc;
  if (!adapter || !flow || !stage ||
      ((adapter->role == TURBO_FLOW_PGSQL_OUTBOX_SOURCE) != (stage->is_source != 0)))
    return TURBO_EINVAL;
  turbo_mutex_lock(&adapter->lock);
  if (atomic_load_explicit(&adapter->started, memory_order_acquire) || adapter->connection) {
    turbo_mutex_unlock(&adapter->lock);
    return TURBO_EALREADY;
  }
  if (adapter->role == TURBO_FLOW_PGSQL_OUTBOX_SOURCE) {
    adapter->source_name = tstr_dup(stage->name);
    if (!adapter->source_name) {
      turbo_mutex_unlock(&adapter->lock);
      return TURBO_ENOMEM;
    }
    adapter->flow = flow;
  }
  rc = flow_pgsql_outbox_connect(adapter);
  if (rc != TURBO_OK) {
    tstr_freep(&adapter->source_name);
    adapter->flow = NULL;
    turbo_mutex_unlock(&adapter->lock);
    return rc;
  }
  tf_timer_reset(&adapter->poll_wait);
  atomic_store_explicit(&adapter->last_status, TURBO_OK, memory_order_release);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  if (adapter->role == TURBO_FLOW_PGSQL_OUTBOX_SOURCE) {
    rc = turbo_thread_create(&adapter->thread, flow_pgsql_outbox_source_thread, adapter);
    if (rc != TURBO_OK) {
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      flow_pgsql_outbox_disconnect(adapter);
      tstr_freep(&adapter->source_name);
      adapter->flow = NULL;
      turbo_mutex_unlock(&adapter->lock);
      return rc;
    }
    adapter->thread_started = 1;
  }
  turbo_mutex_unlock(&adapter->lock);
  return TURBO_OK;
}

static int flow_pgsql_outbox_consume(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)ctx;
  int rc;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || adapter->role != TURBO_FLOW_PGSQL_OUTBOX_SINK ||
      (msg->payload.len > 0u && !msg->payload.data) || msg->payload.len > INT_MAX)
    return TURBO_EINVAL;
  if (msg->payload.len > adapter->max_payload_size) return TURBO_EMSGSIZE;
  turbo_mutex_lock(&adapter->lock);
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire) || !adapter->connection)
    rc = TURBO_ESHUTDOWN;
  else rc = flow_pgsql_outbox_sink_commit(adapter, msg);
  turbo_mutex_unlock(&adapter->lock);
  if (rc == TURBO_OK) {
    (void)atomic_fetch_add_explicit(&adapter->accepted, 1u, memory_order_relaxed);
  }
  if (rc != TURBO_OK) {
    atomic_store_explicit(&adapter->last_status, rc, memory_order_release);
    (void)atomic_fetch_add_explicit(&adapter->failures, 1u, memory_order_relaxed);
  }
  return rc;
}

static void flow_pgsql_outbox_stop(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)ctx;
  char cancel_error[256];
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  if (adapter->poll_wait_initialized) tf_timer_stop(&adapter->poll_wait);
  if (adapter->thread_started && adapter->cancel)
    (void)PQcancel(adapter->cancel, cancel_error, (int)sizeof(cancel_error));
  if (adapter->thread_started) {
    (void)turbo_thread_join(&adapter->thread);
    adapter->thread_started = 0;
  }
  if (adapter->lock_initialized) turbo_mutex_lock(&adapter->lock);
  flow_pgsql_outbox_disconnect(adapter);
  adapter->flow = NULL;
  tstr_freep(&adapter->source_name);
  if (adapter->lock_initialized) turbo_mutex_unlock(&adapter->lock);
}

static int flow_pgsql_outbox_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)ctx;
  int written;
  if (!adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  *out = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  out->domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  out->kind = TURBO_FLOW_RESOURCE_STORAGE;
  out->generation = 1u;
  out->observed_generation = 1u;
  written = snprintf(out->uid, sizeof(out->uid), "%s", adapter->resource_uid);
  if (written < 0 || (size_t)written >= sizeof(out->uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(out->owner_name, sizeof(out->owner_name), "%s", adapter->resource_owner);
  return written < 0 || (size_t)written >= sizeof(out->owner_name) ? TURBO_ENAMETOOLONG : TURBO_OK;
}

static int flow_pgsql_outbox_resource_document(void *ctx,
                                               turbo_flow_resource_document_kind_t document_kind,
                                               turbo_flow_resource_document_t *out) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  tstr payload;
  int rc;
  if (!adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  rc = flow_pgsql_outbox_resource_metadata(adapter, &metadata);
  if (rc != TURBO_OK) return rc;
  payload = tstr_format(
      "{\"started\":{},\"role\":{},\"last_status\":{},\"accepted\":\"{}\","
      "\"delivered\":\"{}\",\"requeued\":\"{}\",\"failures\":\"{}\"}",
      atomic_load_explicit(&adapter->started, memory_order_acquire) ? "true" : "false",
      (unsigned)adapter->role, atomic_load_explicit(&adapter->last_status, memory_order_acquire),
      atomic_load_explicit(&adapter->accepted, memory_order_relaxed),
      atomic_load_explicit(&adapter->delivered, memory_order_relaxed),
      atomic_load_explicit(&adapter->requeued, memory_order_relaxed),
      atomic_load_explicit(&adapter->failures, memory_order_relaxed));
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(
      out, &metadata, &FLOW_PGSQL_OUTBOX_STATUS_SCHEMA, payload, tstr_len(payload));
  tstr_freep(&payload);
  return rc;
}

static void flow_pgsql_outbox_shutdown(void *ctx) {
  flow_pgsql_outbox_adapter_t *adapter = (flow_pgsql_outbox_adapter_t *)ctx;
  if (!adapter) return;
  flow_pgsql_outbox_stop(adapter, NULL, NULL);
  if (adapter->poll_wait_initialized) tf_timer_destroy(&adapter->poll_wait);
  if (adapter->lock_initialized) turbo_mutex_destroy(&adapter->lock);
  tstr_freep(&adapter->resource_uid);
  tstr_freep(&adapter->resource_owner);
  tstr_freep(&adapter->conninfo);
  tstr_freep(&adapter->outbox_name);
  free(adapter);
}

int turbo_flow_pgsql_register_outbox_adapter(turbo_flow_t *flow, const char *name,
                                             const turbo_flow_pgsql_outbox_config_t *config) {
  flow_pgsql_outbox_adapter_t *adapter;
  turbo_flow_pgsql_outbox_completion_t completion =
      TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_DELETE;
  uint32_t max_delivery_attempts = 0u;
  uint32_t retry_delay_ms = 0u;
  uint32_t archive_ttl_ms = 0u;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  int rc;
  if (config && config->size >= sizeof(*config)) {
    completion = config->completion;
    max_delivery_attempts = config->max_delivery_attempts;
    retry_delay_ms = config->retry_delay_ms;
    archive_ttl_ms = config->archive_ttl_ms;
  }
  if (!flow || !name || !name[0] || !config ||
      config->size < TURBO_FLOW_PGSQL_OUTBOX_CONFIG_V1_SIZE ||
      (config->size > TURBO_FLOW_PGSQL_OUTBOX_CONFIG_V1_SIZE &&
       config->size < sizeof(*config)) ||
      config->version != TURBO_FLOW_PGSQL_OUTBOX_API_VERSION ||
      (config->role != TURBO_FLOW_PGSQL_OUTBOX_SINK &&
       config->role != TURBO_FLOW_PGSQL_OUTBOX_SOURCE) ||
      !config->conninfo || !config->conninfo[0] || !config->outbox_name ||
      !config->outbox_name[0] || strlen(config->outbox_name) > TURBO_FLOW_PGSQL_OUTBOX_NAME_MAX ||
      config->capacity == 0u || config->capacity > TURBO_FLOW_PGSQL_OUTBOX_MAX_CAPACITY ||
      config->max_payload_size == 0u ||
      config->max_payload_size > TURBO_FLOW_PGSQL_OUTBOX_MAX_PAYLOAD_SIZE ||
      config->poll_interval_ms == 0u || config->claim_scan_limit == 0u ||
      config->claim_scan_limit > TURBO_FLOW_PGSQL_OUTBOX_MAX_CLAIM_SCAN ||
      (config->create_table != 0 && config->create_table != 1) ||
      (completion != TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_DELETE &&
       completion != TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE) ||
      max_delivery_attempts > TURBO_FLOW_PGSQL_OUTBOX_MAX_DELIVERY_ATTEMPTS ||
      ((max_delivery_attempts == 0u) != (retry_delay_ms == 0u)) ||
      (archive_ttl_ms != 0u && completion != TURBO_FLOW_PGSQL_OUTBOX_COMPLETION_ARCHIVE))
    return TURBO_EINVAL;
  adapter = (flow_pgsql_outbox_adapter_t *)calloc(1u, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->last_status, TURBO_OK);
  atomic_init(&adapter->accepted, 0u);
  atomic_init(&adapter->delivered, 0u);
  atomic_init(&adapter->requeued, 0u);
  atomic_init(&adapter->failures, 0u);
  adapter->resource_uid = tstr_format("postgresql-outbox:{}", name);
  adapter->resource_owner = tstr_dup(name);
  adapter->conninfo = tstr_dup(config->conninfo);
  adapter->outbox_name = tstr_dup(config->outbox_name);
  adapter->role = config->role;
  adapter->capacity = config->capacity;
  adapter->max_payload_size = config->max_payload_size;
  adapter->poll_interval_ms = config->poll_interval_ms;
  adapter->claim_scan_limit = config->claim_scan_limit;
  adapter->create_table = config->create_table;
  adapter->completion = completion;
  adapter->max_delivery_attempts = max_delivery_attempts;
  adapter->retry_delay_ms = retry_delay_ms;
  adapter->archive_ttl_ms = archive_ttl_ms;
  if (!adapter->resource_uid || !adapter->resource_owner || !adapter->conninfo ||
      !adapter->outbox_name) {
    flow_pgsql_outbox_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (tstr_len(adapter->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    flow_pgsql_outbox_shutdown(adapter);
    return TURBO_ENAMETOOLONG;
  }
  turbo_mutex_init(&adapter->lock);
  adapter->lock_initialized = 1;
  rc = tf_timer_init(&adapter->poll_wait);
  if (rc != TURBO_OK) {
    flow_pgsql_outbox_shutdown(adapter);
    return rc;
  }
  adapter->poll_wait_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_pgsql_outbox_start;
  ops.consume = flow_pgsql_outbox_consume;
  ops.stop = flow_pgsql_outbox_stop;
  ops.shutdown = flow_pgsql_outbox_shutdown;
  resource.owner_name = name;
  resource.ops.metadata = flow_pgsql_outbox_resource_metadata;
  resource.ops.document = flow_pgsql_outbox_resource_document;
  resource.ctx = adapter;
  rc = turbo_flow_register_adapter_with_resources(flow, name, &ops, adapter,
                                                  config->role == TURBO_FLOW_PGSQL_OUTBOX_SOURCE
                                                      ? &FLOW_PGSQL_OUTBOX_SOURCE_SCHEMA
                                                      : &FLOW_PGSQL_OUTBOX_SINK_SCHEMA,
                                                  &resource, 1u);
  return rc;
}
