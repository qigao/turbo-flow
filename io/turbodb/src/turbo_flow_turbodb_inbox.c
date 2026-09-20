#include "turbo_flow_turbodb.h"

#include <cstl/vec.h>
#include <salts_buffer.h>
#include <salts_error.h>
#include <salts_thread.h>

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { INBOX_OWNER_CLOSED = 0, INBOX_OWNER_ACTIVE = 1 };
enum { INBOX_PHASE_PENDING = 0, INBOX_PHASE_CLAIMED = 1, INBOX_PHASE_FAILED = 2 };
enum { INBOX_PHASE_TOMBSTONE = 3 };
enum {
  INBOX_LEASE_FREE = 0,
  INBOX_LEASE_RESERVED = 1,
  INBOX_LEASE_ACTIVE = 2,
  INBOX_LEASE_SETTLING = 3
};
enum { INBOX_PREFLIGHT_DESCRIPTOR_PAGE = 64 };

typedef struct inbox_connection_slot_s {
  orm_connection_t *connection;
  bool busy;
} inbox_connection_slot_t;

typedef struct inbox_lease_s {
  int state;
  uint64_t record_id;
  uint64_t claim_token;
  mem_buffer_t *storage;
  turbo_flow_inbox_record_t record;
} inbox_lease_t;

typedef struct inbox_owner_s {
  salts_mutex_t mutex;
  vec_t connections;
  vec_t leases;
  size_t connection_count;
  size_t max_claims;
  size_t max_records;
  size_t max_total_bytes;
  size_t max_record_bytes;
  uint64_t generation;
  bool accepting;
  bool closing;
  char meta_table[96];
  char records_table[96];
} inbox_owner_t;

typedef struct inbox_meta_s {
  int64_t generation, owner_state, next_record_id, next_claim_token;
  int64_t records, history_records, pending_records, failed_records, in_flight_claims;
  int64_t retained_bytes;
  int64_t admitted, completed, failed, retried, discarded;
} inbox_meta_t;

static inbox_connection_slot_t *inbox_connection_at(inbox_owner_t *owner, size_t index) {
  return (inbox_connection_slot_t *)vec_at(&owner->connections, index);
}

static inbox_lease_t *inbox_lease_at(inbox_owner_t *owner, size_t index) {
  return (inbox_lease_t *)vec_at(&owner->leases, index);
}

static int inbox_orm_status(orm_status_t status) {
  switch (status) {
  case ORM_STATUS_OK:
    return SALTS_OK;
  case ORM_STATUS_INVALID_ARGUMENT:
    return SALTS_EINVAL;
  case ORM_STATUS_ABI_MISMATCH:
  case ORM_STATUS_TYPE_ERROR:
  case ORM_STATUS_NULL_VALUE:
  case ORM_STATUS_CONSTRAINT:
    return SALTS_EPROTO;
  case ORM_STATUS_OUT_OF_MEMORY:
    return SALTS_ENOMEM;
  case ORM_STATUS_OUT_OF_RANGE:
    return SALTS_ERANGE;
  case ORM_STATUS_LIMIT_EXCEEDED:
    return SALTS_ENOSPC;
  case ORM_STATUS_BUSY:
  case ORM_STATUS_INVALID_STATE:
    return SALTS_EBUSY;
  case ORM_STATUS_UNSUPPORTED:
    return SALTS_ENOTSUP;
  default:
    return SALTS_EIO;
  }
}

static int inbox_exec(orm_connection_t *connection, orm_transaction_t *transaction, const char *sql,
                      const orm_value_t *values, size_t value_count, orm_result_t **out,
                      orm_error_t *error) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  orm_status_t status = orm_raw(connection, orm_view(sql), &query, error);
  if (status != ORM_STATUS_OK) return inbox_orm_status(status);
  for (size_t i = 0u; i < value_count; ++i) {
    status = orm_query_bind(query, values[i], error);
    if (status != ORM_STATUS_OK) break;
  }
  if (status == ORM_STATUS_OK) {
    status = transaction ? orm_query_execute_in_transaction(query, transaction, &result, error)
                         : orm_query_execute(query, &result, error);
  }
  orm_query_destroy(query);
  if (status != ORM_STATUS_OK) {
    orm_result_destroy(result);
    return inbox_orm_status(status);
  }
  if (out) *out = result;
  else orm_result_destroy(result);
  return SALTS_OK;
}

static int inbox_affected_one(orm_result_t *result, orm_error_t *error) {
  uint64_t affected = 0u;
  orm_status_t status = orm_result_affected_rows(result, &affected, error);
  if (status != ORM_STATUS_OK) return inbox_orm_status(status);
  return affected == 1u ? SALTS_OK : SALTS_EBUSY;
}

static inbox_connection_slot_t *inbox_connection_acquire(inbox_owner_t *owner) {
  inbox_connection_slot_t *slot = NULL;
  salts_mutex_lock(&owner->mutex);
  for (size_t i = 0u; i < owner->connection_count; ++i) {
    inbox_connection_slot_t *candidate = inbox_connection_at(owner, i);
    if (candidate && !candidate->busy) {
      candidate->busy = true;
      slot = candidate;
      break;
    }
  }
  salts_mutex_unlock(&owner->mutex);
  return slot;
}

static void inbox_connection_release(inbox_owner_t *owner, inbox_connection_slot_t *slot) {
  salts_mutex_lock(&owner->mutex);
  slot->busy = false;
  salts_mutex_unlock(&owner->mutex);
}

static bool inbox_is_accepting(inbox_owner_t *owner) {
  bool accepting;
  salts_mutex_lock(&owner->mutex);
  accepting = owner->accepting;
  salts_mutex_unlock(&owner->mutex);
  return accepting;
}

static int inbox_begin(inbox_connection_slot_t *slot, orm_transaction_t **out, orm_error_t *error) {
  return inbox_orm_status(
      orm_transaction_begin(slot->connection, ORM_ISOLATION_SERIALIZABLE, out, error));
}

static int inbox_finish(orm_transaction_t *transaction, int rc, orm_error_t *error) {
  if (!transaction) return rc;
  if (rc == SALTS_OK) {
    orm_status_t commit_status = orm_transaction_commit(transaction, error);
    if (commit_status != ORM_STATUS_OK) {
      orm_error_t ignored;
      orm_error_init(&ignored);
      (void)orm_transaction_rollback(transaction, &ignored);
    }
    rc = inbox_orm_status(commit_status);
  } else {
    (void)orm_transaction_rollback(transaction, error);
  }
  orm_transaction_destroy(transaction);
  return rc;
}

static int inbox_get_i64(const orm_result_t *result, uint64_t column, int64_t *out,
                         orm_error_t *error) {
  return inbox_orm_status(orm_result_get_int64(result, 0u, column, out, error));
}

static int inbox_read_meta(inbox_owner_t *owner, inbox_connection_slot_t *slot,
                           orm_transaction_t *transaction, inbox_meta_t *meta, orm_error_t *error) {
  char sql[512];
  orm_result_t *result = NULL;
  uint64_t rows = 0u;
  int rc;
  (void)snprintf(
      sql, sizeof(sql),
      "SELECT generation,owner_state,next_record_id,next_claim_token,records,"
      "history_records,pending_records,failed_records,in_flight_claims,retained_bytes,admitted,"
      "completed,failed,retried,discarded FROM %s WHERE singleton_id=1",
      owner->meta_table);
  rc = inbox_exec(slot->connection, transaction, sql, NULL, 0u, &result, error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  int64_t *fields[] = {&meta->generation,       &meta->owner_state,    &meta->next_record_id,
                       &meta->next_claim_token, &meta->records,        &meta->history_records,
                       &meta->pending_records,  &meta->failed_records, &meta->in_flight_claims,
                       &meta->retained_bytes,   &meta->admitted,       &meta->completed,
                       &meta->failed,           &meta->retried,        &meta->discarded};
  for (uint64_t i = 0u; rc == SALTS_OK && i < 15u; ++i)
    rc = inbox_get_i64(result, i, fields[i], error);
  orm_result_destroy(result);
  return rc;
}

static void inbox_u64_be(uint64_t value, unsigned char bytes[8]) {
  for (size_t i = 0u; i < 8u; ++i)
    bytes[7u - i] = (unsigned char)(value >> (i * 8u));
}

static uint64_t inbox_be_u64(const unsigned char *bytes) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | bytes[i];
  return value;
}

static int inbox_text_equal(orm_string_view_t actual, const char *expected) {
  size_t length = strlen(expected);
  return actual.len == length && memcmp(actual.data, expected, length) == 0;
}

static int inbox_view_equal(vstr left, vstr right) {
  return left.len == right.len && (left.len == 0u || memcmp(left.data, right.data, left.len) == 0);
}

typedef struct inbox_column_contract_s {
  const char *name;
  const char *type;
  bool primary_key;
} inbox_column_contract_t;

static int inbox_text_equal_ascii_case(orm_string_view_t actual, const char *expected) {
  size_t length = strlen(expected);
  if (actual.len != length) return 0;
  for (size_t i = 0u; i < length; ++i) {
    unsigned char left = (unsigned char)actual.data[i];
    unsigned char right = (unsigned char)expected[i];
    if (left >= 'a' && left <= 'z') left = (unsigned char)(left - ('a' - 'A'));
    if (right >= 'a' && right <= 'z') right = (unsigned char)(right - ('a' - 'A'));
    if (left != right) return 0;
  }
  return 1;
}

static int inbox_validate_columns(inbox_connection_slot_t *slot, orm_transaction_t *transaction,
                                  const char *table, const inbox_column_contract_t *columns,
                                  size_t count, orm_error_t *error) {
  char sql[256];
  orm_result_t *result = NULL;
  uint64_t rows = 0u;
  int rc;
  (void)snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", table);
  rc = inbox_exec(slot->connection, transaction, sql, NULL, 0u, &result, error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  if (rc == SALTS_OK && rows != count) rc = SALTS_EPROTO;
  for (uint64_t row = 0u; rc == SALTS_OK && row < rows; ++row) {
    orm_string_view_t name = {0};
    orm_string_view_t type = {0};
    int64_t not_null = 0;
    int64_t primary_key = 0;
    rc = inbox_orm_status(orm_result_get_text(result, row, 1u, &name, error));
    if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_get_text(result, row, 2u, &type, error));
    if (rc == SALTS_OK)
      rc = inbox_orm_status(orm_result_get_int64(result, row, 3u, &not_null, error));
    if (rc == SALTS_OK)
      rc = inbox_orm_status(orm_result_get_int64(result, row, 5u, &primary_key, error));
    if (rc == SALTS_OK && (!inbox_text_equal(name, columns[row].name) ||
                           !inbox_text_equal_ascii_case(type, columns[row].type) ||
                           primary_key != (columns[row].primary_key ? 1 : 0) || not_null != 1))
      rc = SALTS_EPROTO;
  }
  orm_result_destroy(result);
  return rc;
}

static int inbox_validate_index(inbox_connection_slot_t *slot, orm_transaction_t *transaction,
                                const char *table, const char *index, const char *first,
                                const char *second, bool unique, orm_error_t *error) {
  char sql[256];
  orm_result_t *result = NULL;
  uint64_t rows = 0u;
  int rc;
  (void)snprintf(sql, sizeof(sql),
                 "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?1 AND "
                 "tbl_name=?2");
  orm_value_t identity[] = {orm_text(index), orm_text(table)};
  rc = inbox_exec(slot->connection, transaction, sql, identity, 2u, &result, error);
  int64_t matches = 0;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 0u, &matches, error);
  orm_result_destroy(result);
  result = NULL;
  if (rc == SALTS_OK && matches != 1) rc = SALTS_EPROTO;

  (void)snprintf(sql, sizeof(sql), "PRAGMA index_list(%s)", table);
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, transaction, sql, NULL, 0u, &result, error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  matches = 0;
  for (uint64_t row = 0u; rc == SALTS_OK && row < rows; ++row) {
    orm_string_view_t name = {0};
    int64_t actual_unique = 0;
    int64_t partial = 0;
    rc = inbox_orm_status(orm_result_get_text(result, row, 1u, &name, error));
    if (rc == SALTS_OK && inbox_text_equal(name, index)) {
      rc = inbox_orm_status(orm_result_get_int64(result, row, 2u, &actual_unique, error));
      if (rc == SALTS_OK)
        rc = inbox_orm_status(orm_result_get_int64(result, row, 4u, &partial, error));
      if (rc == SALTS_OK && (actual_unique != (unique ? 1 : 0) || partial != 0 || matches != 0))
        rc = SALTS_EPROTO;
      if (rc == SALTS_OK) matches = 1;
    }
  }
  orm_result_destroy(result);
  result = NULL;
  if (rc == SALTS_OK && matches != 1) rc = SALTS_EPROTO;

  (void)snprintf(sql, sizeof(sql), "PRAGMA index_info(%s)", index);
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, transaction, sql, NULL, 0u, &result, error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  if (rc == SALTS_OK && rows != 2u) rc = SALTS_EPROTO;
  for (uint64_t row = 0u; rc == SALTS_OK && row < rows; ++row) {
    orm_string_view_t name = {0};
    rc = inbox_orm_status(orm_result_get_text(result, row, 2u, &name, error));
    if (rc == SALTS_OK && !inbox_text_equal(name, row == 0u ? first : second)) rc = SALTS_EPROTO;
  }
  orm_result_destroy(result);
  return rc;
}

static int inbox_validate_durability(inbox_connection_slot_t *slot, orm_transaction_t *transaction,
                                     orm_error_t *error) {
  orm_result_t *result = NULL;
  orm_string_view_t journal_mode = {0};
  int64_t synchronous = 0;
  uint64_t rows = 0u;
  int rc =
      inbox_exec(slot->connection, transaction, "PRAGMA journal_mode", NULL, 0u, &result, error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  if (rc == SALTS_OK)
    rc = inbox_orm_status(orm_result_get_text(result, 0u, 0u, &journal_mode, error));
  if (rc == SALTS_OK && (inbox_text_equal_ascii_case(journal_mode, "OFF") ||
                         inbox_text_equal_ascii_case(journal_mode, "MEMORY")))
    rc = SALTS_ENOTSUP;
  orm_result_destroy(result);
  result = NULL;

  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, transaction, "PRAGMA synchronous", NULL, 0u, &result, error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 0u, &synchronous, error);
  if (rc == SALTS_OK && synchronous < 2) rc = SALTS_ENOTSUP;
  orm_result_destroy(result);
  return rc;
}

static int inbox_validate_descriptors(inbox_owner_t *owner, inbox_connection_slot_t *slot,
                                      orm_transaction_t *transaction, orm_error_t *error) {
  char sql[512];
  int64_t after_record_id = 0;
  uint64_t rows = 0u;
  int rc = SALTS_OK;
  (void)snprintf(sql, sizeof(sql),
                 "SELECT record_id,content_domain,content_profile,content_encoding,content_flags,"
                 "content_schema_version,content_media_type,content_schema_name,content_type_name,"
                 "content_identity FROM %s WHERE record_id>?1 ORDER BY record_id LIMIT %u",
                 owner->records_table, INBOX_PREFLIGHT_DESCRIPTOR_PAGE);
  do {
    orm_result_t *result = NULL;
    orm_value_t after = orm_i64(after_record_id);
    rc = inbox_exec(slot->connection, transaction, sql, &after, 1u, &result, error);
    if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
    if (rc == SALTS_OK && rows > INBOX_PREFLIGHT_DESCRIPTOR_PAGE) rc = SALTS_EPROTO;
    for (uint64_t row = 0u; rc == SALTS_OK && row < rows; ++row) {
      int64_t values[6] = {0};
      orm_string_view_t texts[4] = {{0}};
      turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
      for (uint64_t column = 0u; rc == SALTS_OK && column < 6u; ++column)
        rc = inbox_orm_status(orm_result_get_int64(result, row, column, &values[column], error));
      for (uint64_t column = 0u; rc == SALTS_OK && column < 4u; ++column)
        rc = inbox_orm_status(orm_result_get_text(result, row, 6u + column, &texts[column], error));
      if (rc == SALTS_OK &&
          (values[0] <= after_record_id || values[1] < INT_MIN || values[1] > INT_MAX ||
           values[2] < INT_MIN || values[2] > INT_MAX || values[3] < INT_MIN ||
           values[3] > INT_MAX || values[4] < 0 || values[4] > UINT32_MAX || values[5] < 0 ||
           values[5] > UINT32_MAX || texts[0].len > TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX ||
           texts[1].len > TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX ||
           texts[2].len > TURBO_FLOW_CONTENT_TYPE_NAME_MAX ||
           texts[3].len > TURBO_FLOW_CONTENT_IDENTITY_MAX))
        rc = SALTS_EPROTO;
      for (size_t index = 0u; rc == SALTS_OK && index < 4u; ++index)
        if (texts[index].len != 0u && memchr(texts[index].data, '\0', texts[index].len) != NULL)
          rc = SALTS_EPROTO;
      if (rc == SALTS_OK) {
        descriptor.domain = (turbo_flow_domain_t)values[1];
        descriptor.profile = (turbo_flow_content_profile_t)values[2];
        descriptor.encoding = (turbo_flow_data_encoding_t)values[3];
        descriptor.flags = (uint32_t)values[4];
        descriptor.schema_version = (uint32_t)values[5];
        memcpy(descriptor.media_type, texts[0].data, texts[0].len);
        descriptor.media_type[texts[0].len] = '\0';
        memcpy(descriptor.schema_name, texts[1].data, texts[1].len);
        descriptor.schema_name[texts[1].len] = '\0';
        memcpy(descriptor.type_name, texts[2].data, texts[2].len);
        descriptor.type_name[texts[2].len] = '\0';
        memcpy(descriptor.identity, texts[3].data, texts[3].len);
        descriptor.identity[texts[3].len] = '\0';
        if (descriptor.domain != TURBO_FLOW_DOMAIN_DATA ||
            (descriptor.flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u ||
            turbo_flow_content_descriptor_check(&descriptor) != SALTS_OK)
          rc = SALTS_EPROTO;
      }
      if (rc == SALTS_OK) after_record_id = values[0];
    }
    orm_result_destroy(result);
  } while (rc == SALTS_OK && rows == INBOX_PREFLIGHT_DESCRIPTOR_PAGE);
  return rc;
}

static int inbox_preflight(inbox_owner_t *owner, inbox_connection_slot_t *slot,
                           orm_transaction_t *transaction,
                           const turbo_flow_turbodb_inbox_config_t *config, orm_error_t *error) {
  static const inbox_column_contract_t meta_columns[] = {
      {"singleton_id", "integer", true},     {"schema_magic", "text", false},
      {"schema_version", "integer", false},  {"generation", "bigint", false},
      {"owner_state", "integer", false},     {"next_record_id", "bigint", false},
      {"next_claim_token", "bigint", false}, {"max_records", "bigint", false},
      {"max_total_bytes", "bigint", false},  {"max_record_bytes", "bigint", false},
      {"max_claims", "bigint", false},       {"records", "bigint", false},
      {"history_records", "bigint", false},  {"pending_records", "bigint", false},
      {"failed_records", "bigint", false},   {"in_flight_claims", "bigint", false},
      {"retained_bytes", "bigint", false},   {"admitted", "bigint", false},
      {"completed", "bigint", false},        {"failed", "bigint", false},
      {"retried", "bigint", false},          {"discarded", "bigint", false}};
  static const inbox_column_contract_t record_columns[] = {
      {"record_id", "bigint", true},
      {"phase", "integer", false},
      {"claim_generation", "bigint", false},
      {"claim_token", "bigint", false},
      {"failure_status", "integer", false},
      {"failure_kind", "integer", false},
      {"terminal_kind", "integer", false},
      {"envelope_schema", "text", false},
      {"envelope_schema_version", "integer", false},
      {"source_id", "bytea", false},
      {"admission_id", "bytea", false},
      {"source_sequence_be", "bytea", false},
      {"timestamp_ns_be", "bytea", false},
      {"message_type", "bigint", false},
      {"message_flags", "bigint", false},
      {"content_domain", "integer", false},
      {"content_profile", "integer", false},
      {"content_encoding", "integer", false},
      {"content_flags", "bigint", false},
      {"content_schema_version", "bigint", false},
      {"content_media_type", "text", false},
      {"content_schema_name", "text", false},
      {"content_type_name", "text", false},
      {"content_identity", "text", false},
      {"correlation", "bytea", false},
      {"payload", "bytea", false},
      {"retained_bytes", "bigint", false}};
  char sql[4096];
  orm_result_t *result = NULL;
  orm_string_view_t magic = {0};
  uint64_t rows = 0u;
  int64_t values[21] = {0};
  int rc;
  rc = inbox_validate_durability(slot, transaction, error);
  if (rc == SALTS_OK)
    rc = inbox_validate_columns(slot, transaction, owner->meta_table, meta_columns,
                                sizeof(meta_columns) / sizeof(meta_columns[0]), error);
  if (rc == SALTS_OK)
    rc = inbox_validate_columns(slot, transaction, owner->records_table, record_columns,
                                sizeof(record_columns) / sizeof(record_columns[0]), error);
  if (rc == SALTS_OK) {
    char index[128];
    (void)snprintf(index, sizeof(index), "%s_admission", owner->records_table);
    rc = inbox_validate_index(slot, transaction, owner->records_table, index, "source_id",
                              "admission_id", true, error);
    if (rc == SALTS_OK) {
      (void)snprintf(index, sizeof(index), "%s_phase", owner->records_table);
      rc = inbox_validate_index(slot, transaction, owner->records_table, index, "phase",
                                "record_id", false, error);
    }
  }
  if (rc != SALTS_OK) return rc == SALTS_EIO ? SALTS_EPROTO : rc;
  (void)snprintf(
      sql, sizeof(sql),
      "SELECT singleton_id,schema_magic,schema_version,generation,owner_state,next_record_id,"
      "next_claim_token,max_records,max_total_bytes,max_record_bytes,max_claims,records,"
      "history_records,pending_records,failed_records,in_flight_claims,retained_bytes,admitted,"
      "completed,failed,"
      "retried,discarded FROM %s",
      owner->meta_table);
  rc = inbox_exec(slot->connection, transaction, sql, NULL, 0u, &result, error);
  if (rc != SALTS_OK) return rc == SALTS_EIO ? SALTS_EPROTO : rc;
  rc = inbox_orm_status(orm_result_row_count(result, &rows, error));
  if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 0u, &values[0], error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_get_text(result, 0u, 1u, &magic, error));
  for (uint64_t i = 2u; rc == SALTS_OK && i < 22u; ++i)
    rc = inbox_get_i64(result, i, &values[i - 1u], error);
  if (rc == SALTS_OK &&
      (values[0] != 1 || !inbox_text_equal(magic, "turbo-flow.turbodb.inbox") ||
       values[1] != TURBO_FLOW_TURBODB_INBOX_SCHEMA_VERSION || values[2] < 0 ||
       (values[3] != INBOX_OWNER_CLOSED && values[3] != INBOX_OWNER_ACTIVE) || values[4] < 1 ||
       values[5] < 1 || values[6] != (int64_t)config->max_records ||
       values[7] != (int64_t)config->max_total_bytes ||
       values[8] != (int64_t)config->max_record_bytes || values[9] != (int64_t)config->max_claims))
    rc = SALTS_EPROTO;
  for (size_t i = 10u; rc == SALTS_OK && i < 21u; ++i)
    if (values[i] < 0) rc = SALTS_EPROTO;
  if (rc == SALTS_OK &&
      (values[10] > values[6] || values[11] > values[6] || values[11] > values[6] - values[10] ||
       values[12] > values[10] || values[13] > values[10] || values[14] > values[10] ||
       values[13] > values[10] - values[12] || values[14] != values[10] - values[12] - values[13] ||
       values[15] > values[7] || (values[3] == INBOX_OWNER_CLOSED && values[10] != 0)))
    rc = SALTS_EPROTO;
  if (rc == SALTS_OK &&
      (values[10] > values[16] || values[11] > values[16] - values[10] || values[17] > values[16] ||
       values[20] > values[16] - values[17] || values[11] > values[17] + values[20] ||
       values[13] > values[18] || values[19] > values[18] || values[20] > values[18]))
    rc = SALTS_EPROTO;
  orm_result_destroy(result);
  if (rc != SALTS_OK) return rc;
  (void)snprintf(
      sql, sizeof(sql),
      "SELECT count(*),coalesce(sum(CASE WHEN phase IN (0,1,2) THEN 1 ELSE 0 END),0),"
      "coalesce(sum(CASE WHEN phase=3 THEN 1 ELSE 0 END),0),"
      "coalesce(sum(CASE WHEN phase=0 THEN 1 ELSE 0 END),0),"
      "coalesce(sum(CASE WHEN phase=2 THEN 1 ELSE 0 END),0),"
      "coalesce(sum(CASE WHEN phase=1 THEN 1 ELSE 0 END),0),coalesce(sum(retained_bytes),0),"
      "coalesce(sum(CASE WHEN envelope_schema='%s' AND envelope_schema_version=%u THEN 1 ELSE 0 "
      "END),0),coalesce(max(record_id),0),coalesce(max(claim_token),0) "
      "FROM %s",
      TURBO_FLOW_INBOX_RECORD_SCHEMA, TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION, owner->records_table);
  result = NULL;
  rc = inbox_exec(slot->connection, transaction, sql, NULL, 0u, &result, error);
  if (rc != SALTS_OK) return rc == SALTS_EIO ? SALTS_EPROTO : rc;
  int64_t aggregate[10];
  for (uint64_t i = 0u; rc == SALTS_OK && i < 10u; ++i)
    rc = inbox_get_i64(result, i, &aggregate[i], error);
  if (rc == SALTS_OK &&
      (aggregate[0] != values[10] + values[11] || aggregate[1] != values[10] ||
       aggregate[2] != values[11] || aggregate[3] != values[12] || aggregate[4] != values[13] ||
       aggregate[5] != values[14] || aggregate[6] != values[15] || aggregate[7] != aggregate[0] ||
       aggregate[8] >= values[4] || aggregate[9] >= values[5]))
    rc = SALTS_EPROTO;
  orm_result_destroy(result);
  if (rc == SALTS_OK && ((uint64_t)aggregate[0] > config->max_records ||
                         (uint64_t)aggregate[6] > config->max_total_bytes)) {
    return SALTS_ENOSPC;
  }
  if (rc != SALTS_OK) return rc;

  (void)snprintf(
      sql, sizeof(sql),
      "SELECT count(*) FROM %s WHERE record_id IS NULL OR typeof(record_id)!='integer' OR "
      "typeof(phase)!='integer' OR typeof(claim_generation)!='integer' OR "
      "typeof(claim_token)!='integer' OR typeof(failure_status)!='integer' OR "
      "typeof(failure_kind)!='integer' OR typeof(terminal_kind)!='integer' OR "
      "typeof(envelope_schema)!='text' OR typeof(envelope_schema_version)!='integer' OR "
      "typeof(source_id)!='blob' OR typeof(admission_id)!='blob' OR "
      "typeof(source_sequence_be)!='blob' OR typeof(timestamp_ns_be)!='blob' OR "
      "typeof(message_type)!='integer' OR typeof(message_flags)!='integer' OR "
      "typeof(content_domain)!='integer' OR typeof(content_profile)!='integer' OR "
      "typeof(content_encoding)!='integer' OR typeof(content_flags)!='integer' OR "
      "typeof(content_schema_version)!='integer' OR typeof(content_media_type)!='text' OR "
      "typeof(content_schema_name)!='text' OR typeof(content_type_name)!='text' OR "
      "typeof(content_identity)!='text' OR typeof(correlation)!='blob' OR "
      "typeof(payload)!='blob' OR typeof(retained_bytes)!='integer' OR record_id<=0 OR "
      "phase NOT IN (0,1,2,3) OR "
      "retained_bytes<0 OR retained_bytes>?1 OR "
      "retained_bytes!=length(source_id)+length(admission_id)+length(correlation)+length(payload) "
      "OR length(source_id)=0 OR length(admission_id)=0 OR length(source_sequence_be)!=8 OR "
      "length(timestamp_ns_be)!=8 OR envelope_schema!='%s' OR envelope_schema_version!=%u OR "
      "message_type<0 OR message_type>4294967295 OR message_flags<0 OR "
      "message_flags>4294967295 OR (phase=0 AND (claim_generation!=0 OR claim_token!=0 OR "
      "failure_status!=0 OR terminal_kind!=0)) OR (phase=1 AND (claim_generation<=0 OR "
      "claim_generation!=?2 OR claim_token<=0 OR failure_status!=0 OR terminal_kind!=0)) OR "
      "(phase=2 AND "
      "(failure_status=0 OR failure_kind NOT IN (1,2) OR terminal_kind!=0)) OR "
      "(phase=3 AND terminal_kind NOT IN (1,2))",
      owner->records_table, TURBO_FLOW_INBOX_RECORD_SCHEMA, TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION);
  orm_value_t validation_values[] = {orm_i64((int64_t)config->max_record_bytes),
                                     orm_i64(values[2])};
  result = NULL;
  rc = inbox_exec(slot->connection, transaction, sql, validation_values, 2u, &result, error);
  int64_t invalid_records = 0;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 0u, &invalid_records, error);
  if (rc == SALTS_OK && invalid_records != 0) rc = SALTS_EPROTO;
  orm_result_destroy(result);
  return rc == SALTS_OK ? inbox_validate_descriptors(owner, slot, transaction, error) : rc;
}

static inbox_lease_t *inbox_lease_reserve(inbox_owner_t *owner) {
  inbox_lease_t *lease = NULL;
  salts_mutex_lock(&owner->mutex);
  for (size_t i = 0u; i < owner->max_claims; ++i) {
    inbox_lease_t *candidate = inbox_lease_at(owner, i);
    if (candidate && candidate->state == INBOX_LEASE_FREE) {
      candidate->state = INBOX_LEASE_RESERVED;
      lease = candidate;
      break;
    }
  }
  salts_mutex_unlock(&owner->mutex);
  return lease;
}

static inbox_lease_t *inbox_lease_begin_settle(inbox_owner_t *owner, uint64_t id, uint64_t token) {
  inbox_lease_t *lease = NULL;
  salts_mutex_lock(&owner->mutex);
  for (size_t i = 0u; i < owner->max_claims; ++i) {
    inbox_lease_t *candidate = inbox_lease_at(owner, i);
    if (candidate && candidate->state == INBOX_LEASE_ACTIVE && candidate->record_id == id &&
        candidate->claim_token == token) {
      candidate->state = INBOX_LEASE_SETTLING;
      lease = candidate;
      break;
    }
  }
  salts_mutex_unlock(&owner->mutex);
  return lease;
}

static void inbox_lease_activate(inbox_owner_t *owner, inbox_lease_t *lease, uint64_t record_id,
                                 uint64_t claim_token) {
  salts_mutex_lock(&owner->mutex);
  lease->record_id = record_id;
  lease->claim_token = claim_token;
  lease->state = INBOX_LEASE_ACTIVE;
  salts_mutex_unlock(&owner->mutex);
}

static void inbox_lease_restore(inbox_owner_t *owner, inbox_lease_t *lease) {
  salts_mutex_lock(&owner->mutex);
  if (lease->state == INBOX_LEASE_SETTLING) lease->state = INBOX_LEASE_ACTIVE;
  salts_mutex_unlock(&owner->mutex);
}

static bool inbox_has_live_lease(inbox_owner_t *owner) {
  bool live = false;
  salts_mutex_lock(&owner->mutex);
  for (size_t i = 0u; i < owner->max_claims; ++i) {
    const inbox_lease_t *lease = inbox_lease_at(owner, i);
    if (lease && lease->state != INBOX_LEASE_FREE) {
      live = true;
      break;
    }
  }
  salts_mutex_unlock(&owner->mutex);
  return live;
}

static void inbox_lease_release(inbox_owner_t *owner, inbox_lease_t *lease) {
  mem_buffer_t *storage;
  salts_mutex_lock(&owner->mutex);
  storage = lease->storage;
  memset(lease, 0, sizeof(*lease));
  salts_mutex_unlock(&owner->mutex);
  mem_buffer_release(storage);
}

static int inbox_record_from_result(const inbox_owner_t *owner, const orm_result_t *result,
                                    inbox_lease_t *lease, orm_error_t *error) {
  orm_string_view_t texts[6];
  orm_blob_t blobs[6];
  int64_t ints[9];
  size_t total = 1u;
  int rc = SALTS_OK;
  for (uint64_t i = 0; rc == SALTS_OK && i < 9u; ++i)
    rc = inbox_get_i64(result, i, &ints[i], error);
  for (uint64_t i = 0; rc == SALTS_OK && i < 6u; ++i)
    rc = inbox_orm_status(orm_result_get_text(result, 0u, 9u + i, &texts[i], error));
  for (uint64_t i = 0; rc == SALTS_OK && i < 6u; ++i)
    rc = inbox_orm_status(orm_result_get_blob(result, 0u, 15u + i, &blobs[i], error));
  if (rc != SALTS_OK) return rc;
  if (ints[0] != TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION || ints[1] < 0 || ints[1] > UINT32_MAX ||
      ints[2] < 0 || ints[2] > UINT32_MAX || ints[3] < 0 || ints[3] > UINT32_MAX || ints[4] < 0 ||
      ints[4] > UINT32_MAX || ints[5] < 0 || ints[5] > UINT32_MAX || ints[6] < 0 ||
      ints[6] > UINT32_MAX || ints[7] < 0 || ints[7] > UINT32_MAX || ints[8] < 0 ||
      !inbox_text_equal(texts[0], TURBO_FLOW_INBOX_RECORD_SCHEMA) ||
      texts[1].len > TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX ||
      texts[2].len > TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX ||
      texts[3].len > TURBO_FLOW_CONTENT_TYPE_NAME_MAX ||
      texts[4].len > TURBO_FLOW_CONTENT_IDENTITY_MAX || blobs[0].size == 0u ||
      blobs[1].size == 0u || blobs[2].size != 8u || blobs[3].size != 8u) {
    return SALTS_EPROTO;
  }
  for (size_t i = 0u; i < 5u; ++i) {
    if (texts[i].len != 0u && memchr(texts[i].data, '\0', texts[i].len) != NULL)
      return SALTS_EPROTO;
  }
  if (blobs[0].size > SIZE_MAX - blobs[1].size ||
      blobs[4].size > SIZE_MAX - blobs[0].size - blobs[1].size ||
      blobs[5].size > SIZE_MAX - blobs[0].size - blobs[1].size - blobs[4].size) {
    return SALTS_ERANGE;
  }
  size_t retained_bytes = blobs[0].size + blobs[1].size + blobs[4].size + blobs[5].size;
  if (retained_bytes != (size_t)ints[8] || retained_bytes > owner->max_record_bytes) {
    return SALTS_EPROTO;
  }
  for (size_t i = 0; i < 6u; ++i) {
    if (texts[i].len > SIZE_MAX - total) return SALTS_ERANGE;
    total += texts[i].len + 1u;
  }
  for (size_t i = 0; i < 6u; ++i) {
    if (blobs[i].size > SIZE_MAX - total) return SALTS_ERANGE;
    total += blobs[i].size;
  }
  lease->storage = mem_get_buffer(mem_global(), total);
  if (!lease->storage) return SALTS_ENOMEM;
  char *cursor = mem_buffer_data(lease->storage);
  char *copied_text[6];
  for (size_t i = 0; i < 6u; ++i) {
    copied_text[i] = cursor;
    memcpy(cursor, texts[i].data, texts[i].len);
    cursor[texts[i].len] = '\0';
    cursor += texts[i].len + 1u;
  }
  vstr copied_blob[6];
  for (size_t i = 0; i < 6u; ++i) {
    memcpy(cursor, blobs[i].data, blobs[i].size);
    copied_blob[i] = vstr_from_buf(cursor, blobs[i].size);
    cursor += blobs[i].size;
  }
  mem_set_used(lease->storage, total);
  turbo_flow_inbox_record_init(&lease->record);
  lease->record.envelope_schema = copied_text[0];
  lease->record.envelope_schema_version = (uint32_t)ints[0];
  lease->record.message_type = (uint32_t)ints[1];
  lease->record.message_flags = (uint32_t)ints[2];
  lease->record.content.domain = (turbo_flow_domain_t)ints[3];
  lease->record.content.profile = (turbo_flow_content_profile_t)ints[4];
  lease->record.content.encoding = (turbo_flow_data_encoding_t)ints[5];
  lease->record.content.flags = (uint32_t)ints[6];
  lease->record.content.schema_version = (uint32_t)ints[7];
  memcpy(lease->record.content.media_type, copied_text[1], texts[1].len + 1u);
  memcpy(lease->record.content.schema_name, copied_text[2], texts[2].len + 1u);
  memcpy(lease->record.content.type_name, copied_text[3], texts[3].len + 1u);
  memcpy(lease->record.content.identity, copied_text[4], texts[4].len + 1u);
  lease->record.source_id = copied_blob[0];
  lease->record.admission_id = copied_blob[1];
  lease->record.source_sequence = inbox_be_u64(blobs[2].data);
  lease->record.timestamp_ns = inbox_be_u64(blobs[3].data);
  lease->record.correlation = copied_blob[4];
  lease->record.payload = copied_blob[5];
  if (lease->record.content.domain != TURBO_FLOW_DOMAIN_DATA ||
      (lease->record.content.flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u ||
      turbo_flow_content_descriptor_check(&lease->record.content) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int inbox_admit(void *ctx, const turbo_flow_inbox_record_t *record,
                       turbo_flow_inbox_receipt_t *receipt) {
  inbox_owner_t *owner = ctx;
  inbox_connection_slot_t *slot = inbox_connection_acquire(owner);
  orm_error_t error;
  orm_transaction_t *txn = NULL;
  orm_result_t *result = NULL;
  char sql[2048];
  int rc = SALTS_OK;
  inbox_meta_t meta = {0};
  uint64_t rows = 0;
  uint64_t output_record_id = 0u;
  size_t bytes = 0u;
  int size_status = SALTS_OK;
  if (record->source_id.len > SIZE_MAX - record->admission_id.len) {
    size_status = SALTS_ERANGE;
  } else {
    bytes = record->source_id.len + record->admission_id.len;
    if (record->correlation.len > SIZE_MAX - bytes) {
      size_status = SALTS_ERANGE;
    } else {
      bytes += record->correlation.len;
      if (record->payload.len > SIZE_MAX - bytes) size_status = SALTS_ERANGE;
      else bytes += record->payload.len;
    }
  }
  if (!slot) return SALTS_EBUSY;
  orm_error_init(&error);
  rc = inbox_begin(slot, &txn, &error);
  if (rc == SALTS_OK) rc = inbox_read_meta(owner, slot, txn, &meta, &error);
  if (rc == SALTS_OK && meta.generation != (int64_t)owner->generation) rc = SALTS_EBUSY;
  (void)snprintf(
      sql, sizeof(sql),
      "SELECT "
      "record_id,envelope_schema_version,message_type,message_flags,content_domain,"
      "content_profile,content_encoding,content_flags,content_schema_version,envelope_"
      "schema,content_media_type,content_schema_name,content_type_name,content_identity,"
      "'' ,source_id,admission_id,source_sequence_be,timestamp_ns_be,correlation,payload "
      "FROM %s WHERE source_id=?1 AND admission_id=?2",
      owner->records_table);
  orm_value_t key[2] = {orm_blob(record->source_id.data, record->source_id.len),
                        orm_blob(record->admission_id.data, record->admission_id.len)};
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, key, 2u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, &error));
  if (rc == SALTS_OK && rows == 1u) {
    int64_t id;
    inbox_lease_t temp = {0};
    rc = inbox_get_i64(result, 0u, &id, &error);
    /* Reuse the materializer by selecting the same projection without record_id. */
    orm_result_destroy(result);
    result = NULL;
    (void)snprintf(
        sql, sizeof(sql),
        "SELECT "
        "envelope_schema_version,message_type,message_flags,content_domain,content_profile,content_"
        "encoding,content_flags,content_schema_version,retained_bytes,envelope_schema,content_"
        "media_type,content_schema_name,content_type_name,content_identity,'' "
        ",source_id,admission_id,source_sequence_be,timestamp_ns_be,correlation,payload FROM %s "
        "WHERE "
        "record_id=?1",
        owner->records_table);
    orm_value_t idv = orm_i64(id);
    if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, &idv, 1u, &result, &error);
    if (rc == SALTS_OK) rc = inbox_record_from_result(owner, result, &temp, &error);
    if (rc == SALTS_OK) {
      const turbo_flow_content_descriptor_t *a = &temp.record.content, *b = &record->content;
      bool equal =
          temp.record.source_sequence == record->source_sequence &&
          temp.record.timestamp_ns == record->timestamp_ns &&
          temp.record.message_type == record->message_type &&
          temp.record.message_flags == record->message_flags && a->domain == b->domain &&
          a->profile == b->profile && a->encoding == b->encoding && a->flags == b->flags &&
          a->schema_version == b->schema_version && strcmp(a->media_type, b->media_type) == 0 &&
          strcmp(a->schema_name, b->schema_name) == 0 && strcmp(a->type_name, b->type_name) == 0 &&
          strcmp(a->identity, b->identity) == 0 &&
          inbox_view_equal(temp.record.correlation, record->correlation) &&
          inbox_view_equal(temp.record.payload, record->payload);
      rc = equal ? SALTS_OK : SALTS_EPROTO;
      if (equal) output_record_id = (uint64_t)id;
    }
    mem_buffer_release(temp.storage);
  } else if (rc == SALTS_OK && rows == 0u) {
    orm_result_destroy(result);
    result = NULL;
    if (!inbox_is_accepting(owner)) rc = SALTS_ESHUTDOWN;
    if (rc == SALTS_OK && size_status != SALTS_OK) rc = size_status;
    if (rc == SALTS_OK && bytes > owner->max_record_bytes) rc = SALTS_ENOSPC;
    if (rc == SALTS_OK && (meta.records + meta.history_records >= (int64_t)owner->max_records ||
                           bytes > owner->max_total_bytes - (size_t)meta.retained_bytes))
      rc = SALTS_ENOSPC;
    else if (rc == SALTS_OK && (meta.next_record_id == INT64_MAX || meta.admitted == INT64_MAX))
      rc = SALTS_ERANGE;
    unsigned char seq[8], stamp[8];
    inbox_u64_be(record->source_sequence, seq);
    inbox_u64_be(record->timestamp_ns, stamp);
    orm_value_t v[] = {orm_i64(meta.next_record_id),
                       orm_text(TURBO_FLOW_INBOX_RECORD_SCHEMA),
                       orm_i64(TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION),
                       orm_blob(record->source_id.data, record->source_id.len),
                       orm_blob(record->admission_id.data, record->admission_id.len),
                       orm_blob(seq, 8),
                       orm_blob(stamp, 8),
                       orm_i64(record->message_type),
                       orm_i64(record->message_flags),
                       orm_i64(record->content.domain),
                       orm_i64(record->content.profile),
                       orm_i64(record->content.encoding),
                       orm_i64(record->content.flags),
                       orm_i64(record->content.schema_version),
                       orm_text(record->content.media_type),
                       orm_text(record->content.schema_name),
                       orm_text(record->content.type_name),
                       orm_text(record->content.identity),
                       orm_blob(record->correlation.data, record->correlation.len),
                       orm_blob(record->payload.data, record->payload.len),
                       orm_i64((int64_t)bytes)};
    (void)snprintf(
        sql, sizeof(sql),
        "INSERT INTO %s VALUES "
        "(?1,0,0,0,0,1,0,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        owner->records_table);
    if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, v, 21u, NULL, &error);
    orm_value_t mv[] = {orm_i64((int64_t)bytes), orm_i64((int64_t)owner->generation)};
    (void)snprintf(sql, sizeof(sql),
                   "UPDATE %s SET "
                   "next_record_id=next_record_id+1,records=records+1,pending_records=pending_"
                   "records+1,retained_bytes=retained_bytes+?1,admitted=admitted+1 WHERE "
                   "singleton_id=1 AND generation=?2",
                   owner->meta_table);
    if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, mv, 2u, &result, &error);
    if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
    if (rc == SALTS_OK) output_record_id = (uint64_t)meta.next_record_id;
  } else if (rc == SALTS_OK) rc = SALTS_EPROTO;
  orm_result_destroy(result);
  rc = inbox_finish(txn, rc, &error);
  inbox_connection_release(owner, slot);
  if (rc == SALTS_OK) receipt->record_id = output_record_id;
  return rc;
}

static int inbox_claim_request_valid(
    const turbo_flow_inbox_claim_request_t *request) {
  size_t total_bytes = 0u;
  if (!request || request->size != sizeof(*request) ||
      request->version != TURBO_FLOW_INBOX_API_VERSION)
    return 0;
  if (request->ordering == TURBO_FLOW_INBOX_CLAIM_ORDER_GLOBAL)
    return request->excluded_partition_count == 0u;
  if (request->ordering != TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_SOURCE_ID ||
      request->excluded_partition_count > TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_PARTITIONS ||
      (request->excluded_partition_count != 0u && !request->excluded_partitions))
    return 0;
  for (size_t i = 0u; i < request->excluded_partition_count; ++i) {
    const vstr key = request->excluded_partitions[i];
    if (!key.data || key.len == 0u ||
        key.len > TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_BYTES - total_bytes)
      return 0;
    total_bytes += key.len;
    for (size_t j = 0u; j < i; ++j)
      if (inbox_view_equal(key, request->excluded_partitions[j])) return 0;
  }
  return 1;
}

static int inbox_claim_select(void *ctx,
                              const turbo_flow_inbox_claim_request_t *request,
                              turbo_flow_inbox_claim_t *claim) {
  inbox_owner_t *owner = ctx;
  inbox_lease_t *lease;
  inbox_connection_slot_t *slot;
  orm_error_t error;
  orm_transaction_t *txn = NULL;
  orm_result_t *result = NULL;
  inbox_meta_t meta = {0};
  orm_value_t excluded[TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_PARTITIONS];
  char sql[2048];
  size_t used = 0u;
  uint64_t rows = 0u;
  int64_t id = 0;
  int rc;

  if (!inbox_claim_request_valid(request)) return SALTS_EINVAL;
  lease = inbox_lease_reserve(owner);
  if (!lease) return SALTS_ENOSPC;
  slot = inbox_connection_acquire(owner);
  if (!slot) {
    inbox_lease_release(owner, lease);
    return SALTS_EBUSY;
  }

  orm_error_init(&error);
  rc = inbox_begin(slot, &txn, &error);
  if (rc == SALTS_OK) rc = inbox_read_meta(owner, slot, txn, &meta, &error);
  if (rc == SALTS_OK && meta.generation != (int64_t)owner->generation) rc = SALTS_EBUSY;

  if (rc == SALTS_OK) {
    int written = snprintf(sql, sizeof(sql), "SELECT record_id FROM %s WHERE phase=0",
                           owner->records_table);
    if (written < 0 || (size_t)written >= sizeof(sql)) rc = SALTS_ERANGE;
    else used = (size_t)written;
  }
  if (rc == SALTS_OK &&
      request->ordering == TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_SOURCE_ID &&
      request->excluded_partition_count != 0u) {
    int written = snprintf(sql + used, sizeof(sql) - used, " AND source_id NOT IN (");
    if (written < 0 || (size_t)written >= sizeof(sql) - used) rc = SALTS_ERANGE;
    else used += (size_t)written;
    for (size_t i = 0u; rc == SALTS_OK && i < request->excluded_partition_count; ++i) {
      written = snprintf(sql + used, sizeof(sql) - used, "%s?%zu", i == 0u ? "" : ",", i + 1u);
      if (written < 0 || (size_t)written >= sizeof(sql) - used) rc = SALTS_ERANGE;
      else {
        used += (size_t)written;
        excluded[i] = orm_blob(request->excluded_partitions[i].data,
                               request->excluded_partitions[i].len);
      }
    }
    if (rc == SALTS_OK) {
      written = snprintf(sql + used, sizeof(sql) - used, ")");
      if (written < 0 || (size_t)written >= sizeof(sql) - used) rc = SALTS_ERANGE;
      else used += (size_t)written;
    }
  }
  if (rc == SALTS_OK) {
    int written = snprintf(sql + used, sizeof(sql) - used, " ORDER BY record_id LIMIT 1");
    if (written < 0 || (size_t)written >= sizeof(sql) - used) rc = SALTS_ERANGE;
  }

  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, txn, sql,
                    request->ordering == TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_SOURCE_ID
                        ? excluded : NULL,
                    request->ordering == TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_SOURCE_ID
                        ? request->excluded_partition_count : 0u,
                    &result, &error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, &error));
  if (rc == SALTS_OK && rows == 0u) rc = SALTS_ENOENT;
  if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 0u, &id, &error);
  if (rc == SALTS_OK && meta.in_flight_claims >= (int64_t)owner->max_claims) rc = SALTS_ENOSPC;
  if (rc == SALTS_OK && meta.next_claim_token == INT64_MAX) rc = SALTS_ERANGE;
  orm_result_destroy(result);
  result = NULL;

  (void)snprintf(
      sql, sizeof(sql),
      "SELECT "
      "envelope_schema_version,message_type,message_flags,content_domain,content_profile,content_"
      "encoding,content_flags,content_schema_version,retained_bytes,envelope_schema,content_media_"
      "type,content_schema_name,content_type_name,content_identity,'' "
      ",source_id,admission_id,source_sequence_be,timestamp_ns_be,correlation,payload FROM %s "
      "WHERE record_id=?1",
      owner->records_table);
  orm_value_t idv = orm_i64(id);
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, &idv, 1u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_record_from_result(owner, result, lease, &error);
  orm_result_destroy(result);
  result = NULL;

  orm_value_t uv[] = {orm_i64((int64_t)owner->generation), orm_i64(meta.next_claim_token),
                      orm_i64(id)};
  (void)snprintf(
      sql, sizeof(sql),
      "UPDATE %s SET phase=1,claim_generation=?1,claim_token=?2 WHERE record_id=?3 AND phase=0",
      owner->records_table);
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, uv, 3u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
  orm_result_destroy(result);
  result = NULL;

  orm_value_t generation = orm_i64((int64_t)owner->generation);
  (void)snprintf(sql, sizeof(sql),
                 "UPDATE %s SET "
                 "next_claim_token=next_claim_token+1,pending_records=pending_records-1,in_flight_"
                 "claims=in_flight_claims+1 WHERE singleton_id=1 AND generation=?1",
                 owner->meta_table);
  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, txn, sql, &generation, 1u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
  orm_result_destroy(result);

  rc = inbox_finish(txn, rc, &error);
  inbox_connection_release(owner, slot);
  if (rc != SALTS_OK) {
    inbox_lease_release(owner, lease);
    return rc;
  }
  inbox_lease_activate(owner, lease, (uint64_t)id, (uint64_t)meta.next_claim_token);
  claim->record_id = lease->record_id;
  claim->claim_token = lease->claim_token;
  claim->record = lease->record;
  return SALTS_OK;
}

static int inbox_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  const turbo_flow_inbox_claim_request_t request = TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT;
  return inbox_claim_select(ctx, &request, claim);
}

static int inbox_claim_ex(void *ctx, const turbo_flow_inbox_claim_request_t *request,
                          turbo_flow_inbox_claim_t *claim) {
  return inbox_claim_select(ctx, request, claim);
}

static int inbox_claim_transition(inbox_owner_t *owner, uint64_t id, uint64_t token, int status,
                                  bool complete) {
  if (id > INT64_MAX || token > INT64_MAX) return SALTS_ERANGE;
  inbox_lease_t *lease = inbox_lease_begin_settle(owner, id, token);
  if (!lease) return SALTS_EALREADY;
  inbox_connection_slot_t *slot = inbox_connection_acquire(owner);
  if (!slot) {
    inbox_lease_restore(owner, lease);
    return SALTS_EBUSY;
  }
  orm_error_t error;
  orm_transaction_t *txn = NULL;
  orm_result_t *result = NULL;
  inbox_meta_t meta = {0};
  char sql[768];
  int rc;
  orm_error_init(&error);
  rc = inbox_begin(slot, &txn, &error);
  if (rc == SALTS_OK) rc = inbox_read_meta(owner, slot, txn, &meta, &error);
  if (rc == SALTS_OK && meta.generation != (int64_t)owner->generation) rc = SALTS_EBUSY;
  if (rc == SALTS_OK &&
      ((complete && meta.completed == INT64_MAX) || (!complete && meta.failed == INT64_MAX)))
    rc = SALTS_ERANGE;
  orm_value_t v[] = {orm_i64(id), orm_i64(token), orm_i64((int64_t)owner->generation)};
  if (complete)
    (void)snprintf(
        sql, sizeof(sql),
        "UPDATE %s SET phase=3,terminal_kind=1 WHERE record_id=?1 AND claim_token=?2 AND "
        "claim_generation=?3 AND phase=1",
        owner->records_table);
  else
    (void)snprintf(sql, sizeof(sql),
                   "UPDATE %s SET phase=2,failure_status=%d,failure_kind=1,terminal_kind=0 "
                   "WHERE record_id=?1 AND "
                   "claim_token=?2 AND claim_generation=?3 AND phase=1",
                   owner->records_table, status);
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, v, 3, &result, &error);
  if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
  orm_result_destroy(result);
  result = NULL;
  orm_value_t generation = orm_i64((int64_t)owner->generation);
  if (complete)
    (void)snprintf(sql, sizeof(sql),
                   "UPDATE %s SET "
                   "records=records-1,history_records=history_records+1,"
                   "in_flight_claims=in_flight_claims-1,completed=completed+1 "
                   "WHERE singleton_id=1 AND generation=?1",
                   owner->meta_table);
  else
    (void)snprintf(sql, sizeof(sql),
                   "UPDATE %s SET "
                   "in_flight_claims=in_flight_claims-1,failed_records=failed_records+1,failed="
                   "failed+1 WHERE singleton_id=1 AND generation=?1",
                   owner->meta_table);
  if (rc == SALTS_OK) rc = inbox_exec(slot->connection, txn, sql, &generation, 1u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
  orm_result_destroy(result);
  rc = inbox_finish(txn, rc, &error);
  if (rc != SALTS_OK) {
    inbox_meta_t current = {0};
    orm_error_t read_error;
    orm_error_init(&read_error);
    if (inbox_read_meta(owner, slot, NULL, &current, &read_error) == SALTS_OK &&
        current.generation != (int64_t)owner->generation) {
      inbox_lease_release(owner, lease);
      rc = SALTS_ECANCELED;
      lease = NULL;
    }
  }
  inbox_connection_release(owner, slot);
  if (rc == SALTS_OK) inbox_lease_release(owner, lease);
  else if (lease) inbox_lease_restore(owner, lease);
  return rc;
}
static int inbox_complete(void *c, uint64_t i, uint64_t t) {
  return inbox_claim_transition(c, i, t, SALTS_OK, true);
}
static int inbox_fail(void *c, uint64_t i, uint64_t t, int s) {
  return inbox_claim_transition(c, i, t, s, false);
}

static int inbox_failed_transition(void *ctx, uint64_t id, bool discard) {
  inbox_owner_t *o = ctx;
  if (id > INT64_MAX) return SALTS_ERANGE;
  inbox_connection_slot_t *s = inbox_connection_acquire(o);
  if (!s) return SALTS_EBUSY;
  orm_error_t e;
  orm_transaction_t *t = NULL;
  orm_result_t *r = NULL;
  inbox_meta_t meta = {0};
  char q[768];
  int rc;
  int64_t phase = 0;
  orm_error_init(&e);
  rc = inbox_begin(s, &t, &e);
  if (rc == SALTS_OK) rc = inbox_read_meta(o, s, t, &meta, &e);
  if (rc == SALTS_OK && meta.generation != (int64_t)o->generation) rc = SALTS_EBUSY;
  if (rc == SALTS_OK &&
      ((discard && meta.discarded == INT64_MAX) || (!discard && meta.retried == INT64_MAX)))
    rc = SALTS_ERANGE;
  orm_value_t v[] = {orm_i64(id), orm_i64((int64_t)o->generation)};
  (void)snprintf(q, sizeof(q), "SELECT phase FROM %s WHERE record_id=?1", o->records_table);
  if (rc == SALTS_OK) rc = inbox_exec(s->connection, t, q, v, 1, &r, &e);
  uint64_t rows = 0;
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(r, &rows, &e));
  if (rc == SALTS_OK && rows == 0) rc = SALTS_ENOENT;
  else if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  if (rc == SALTS_OK) rc = inbox_get_i64(r, 0, &phase, &e);
  if (rc == SALTS_OK && phase != INBOX_PHASE_FAILED) rc = SALTS_EBUSY;
  orm_result_destroy(r);
  r = NULL;
  if (discard)
    (void)snprintf(q, sizeof(q),
                   "UPDATE %s SET phase=3,terminal_kind=2 WHERE record_id=?1 AND phase=2",
                   o->records_table);
  else
    (void)snprintf(q, sizeof(q),
                   "UPDATE %s SET phase=0,claim_generation=0,claim_token=0,failure_status=0,"
                   "failure_kind=1,terminal_kind=0 "
                   "WHERE record_id=?1 AND phase=2",
                   o->records_table);
  if (rc == SALTS_OK) rc = inbox_exec(s->connection, t, q, v, 1, &r, &e);
  if (rc == SALTS_OK) rc = inbox_affected_one(r, &e);
  orm_result_destroy(r);
  r = NULL;
  orm_value_t generation = orm_i64((int64_t)o->generation);
  if (discard)
    (void)snprintf(q, sizeof(q),
                   "UPDATE %s SET "
                   "records=records-1,history_records=history_records+1,"
                   "failed_records=failed_records-1,discarded=discarded+1 "
                   "WHERE singleton_id=1 AND generation=?1",
                   o->meta_table);
  else
    (void)snprintf(q, sizeof(q),
                   "UPDATE %s SET "
                   "failed_records=failed_records-1,pending_records=pending_records+1,retried="
                   "retried+1 WHERE singleton_id=1 AND generation=?1",
                   o->meta_table);
  if (rc == SALTS_OK) rc = inbox_exec(s->connection, t, q, &generation, 1u, &r, &e);
  if (rc == SALTS_OK) rc = inbox_affected_one(r, &e);
  orm_result_destroy(r);
  rc = inbox_finish(t, rc, &e);
  inbox_connection_release(o, s);
  return rc;
}
static int inbox_retry(void *c, uint64_t i) { return inbox_failed_transition(c, i, false); }
static int inbox_discard(void *c, uint64_t i) { return inbox_failed_transition(c, i, true); }

static int inbox_forget(void *ctx, uint64_t id) {
  inbox_owner_t *owner = ctx;
  if (id > INT64_MAX) return SALTS_ERANGE;
  inbox_connection_slot_t *slot = inbox_connection_acquire(owner);
  if (!slot) return SALTS_EBUSY;
  orm_error_t error;
  orm_transaction_t *transaction = NULL;
  orm_result_t *result = NULL;
  char sql[768];
  int64_t retained_bytes = 0;
  uint64_t rows = 0u;
  int rc;
  orm_error_init(&error);
  rc = inbox_begin(slot, &transaction, &error);
  orm_value_t id_value = orm_i64((int64_t)id);
  (void)snprintf(sql, sizeof(sql), "SELECT phase,retained_bytes FROM %s WHERE record_id=?1",
                 owner->records_table);
  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, transaction, sql, &id_value, 1u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, &error));
  if (rc == SALTS_OK && rows == 0u) rc = SALTS_ENOENT;
  else if (rc == SALTS_OK && rows != 1u) rc = SALTS_EPROTO;
  int64_t phase = 0;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 0u, &phase, &error);
  if (rc == SALTS_OK && phase != INBOX_PHASE_TOMBSTONE) rc = SALTS_EBUSY;
  if (rc == SALTS_OK) rc = inbox_get_i64(result, 1u, &retained_bytes, &error);
  orm_result_destroy(result);
  result = NULL;
  (void)snprintf(sql, sizeof(sql), "DELETE FROM %s WHERE record_id=?1 AND phase=3",
                 owner->records_table);
  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, transaction, sql, &id_value, 1u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
  orm_result_destroy(result);
  result = NULL;
  orm_value_t meta_values[] = {orm_i64(retained_bytes), orm_i64((int64_t)owner->generation)};
  (void)snprintf(sql, sizeof(sql),
                 "UPDATE %s SET history_records=history_records-1,"
                 "retained_bytes=retained_bytes-?1 WHERE singleton_id=1 AND "
                 "generation=?2",
                 owner->meta_table);
  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, transaction, sql, meta_values, 2u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_affected_one(result, &error);
  orm_result_destroy(result);
  rc = inbox_finish(transaction, rc, &error);
  inbox_connection_release(owner, slot);
  return rc;
}

static int inbox_scan_failed(void *ctx, uint64_t after, turbo_flow_inbox_failed_entry_t *entries,
                             size_t cap, size_t *out) {
  inbox_owner_t *o = ctx;
  *out = 0u;
  if (after > INT64_MAX || cap > INT64_MAX) return SALTS_ERANGE;
  inbox_connection_slot_t *s = inbox_connection_acquire(o);
  if (!s) return SALTS_EBUSY;
  orm_error_t e;
  orm_transaction_t *transaction = NULL;
  orm_result_t *r = NULL;
  inbox_meta_t meta = {0};
  char q[512];
  orm_error_init(&e);
  int rc = inbox_begin(s, &transaction, &e);
  if (rc == SALTS_OK) rc = inbox_read_meta(o, s, transaction, &meta, &e);
  if (rc == SALTS_OK && meta.generation != (int64_t)o->generation) rc = SALTS_EBUSY;
  (void)snprintf(q, sizeof(q),
                 "SELECT record_id,failure_status,failure_kind FROM %s WHERE phase=2 AND "
                 "record_id>?1 ORDER BY record_id LIMIT ?2",
                 o->records_table);
  orm_value_t v[] = {orm_i64((int64_t)after), orm_i64((int64_t)cap)};
  if (rc == SALTS_OK) rc = inbox_exec(s->connection, transaction, q, v, 2, &r, &e);
  uint64_t rows = 0;
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(r, &rows, &e));
  if (rc == SALTS_OK && rows > cap) rc = SALTS_EPROTO;
  for (uint64_t i = 0; rc == SALTS_OK && i < rows; ++i) {
    int64_t a = 0, b = 0, c = 0;
    rc = inbox_orm_status(orm_result_get_int64(r, i, 0, &a, &e));
    if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_get_int64(r, i, 1, &b, &e));
    if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_get_int64(r, i, 2, &c, &e));
    if (rc == SALTS_OK && (a <= 0 || b < INT_MIN || b > INT_MAX ||
                           (c != TURBO_FLOW_INBOX_FAILURE_PROCESSING &&
                            c != TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN))) {
      rc = SALTS_EPROTO;
    }
    if (rc == SALTS_OK) {
      entries[i].record_id = (uint64_t)a;
      entries[i].status = (int)b;
      entries[i].kind = (turbo_flow_inbox_failure_kind_t)c;
    }
  }
  orm_result_destroy(r);
  rc = inbox_finish(transaction, rc, &e);
  inbox_connection_release(o, s);
  if (rc == SALTS_OK) {
    *out = (size_t)rows;
  } else {
    for (uint64_t i = 0u; i < rows && i < cap; ++i)
      entries[i] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
  }
  return rc;
}

static int inbox_scan_history(void *ctx, uint64_t after, turbo_flow_inbox_history_entry_t *entries,
                              size_t cap, size_t *out) {
  inbox_owner_t *owner = ctx;
  *out = 0u;
  if (after > INT64_MAX || cap > INT64_MAX) return SALTS_ERANGE;
  inbox_connection_slot_t *slot = inbox_connection_acquire(owner);
  if (!slot) return SALTS_EBUSY;
  orm_error_t error;
  orm_transaction_t *transaction = NULL;
  orm_result_t *result = NULL;
  inbox_meta_t meta = {0};
  char sql[512];
  uint64_t rows = 0u;
  int rc;
  orm_error_init(&error);
  rc = inbox_begin(slot, &transaction, &error);
  if (rc == SALTS_OK) rc = inbox_read_meta(owner, slot, transaction, &meta, &error);
  if (rc == SALTS_OK && meta.generation != (int64_t)owner->generation) rc = SALTS_EBUSY;
  (void)snprintf(sql, sizeof(sql),
                 "SELECT record_id,terminal_kind FROM %s WHERE phase=3 AND record_id>?1 "
                 "ORDER BY record_id LIMIT ?2",
                 owner->records_table);
  orm_value_t values[] = {orm_i64((int64_t)after), orm_i64((int64_t)cap)};
  if (rc == SALTS_OK)
    rc = inbox_exec(slot->connection, transaction, sql, values, 2u, &result, &error);
  if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_row_count(result, &rows, &error));
  if (rc == SALTS_OK && rows > cap) rc = SALTS_EPROTO;
  for (uint64_t row = 0u; rc == SALTS_OK && row < rows; ++row) {
    int64_t record_id = 0;
    int64_t kind = 0;
    rc = inbox_orm_status(orm_result_get_int64(result, row, 0u, &record_id, &error));
    if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_get_int64(result, row, 1u, &kind, &error));
    if (rc == SALTS_OK && (record_id <= 0 || kind < TURBO_FLOW_INBOX_TERMINAL_COMPLETED ||
                           kind > TURBO_FLOW_INBOX_TERMINAL_DISCARDED))
      rc = SALTS_EPROTO;
    if (rc == SALTS_OK) {
      entries[row].record_id = (uint64_t)record_id;
      entries[row].kind = (turbo_flow_inbox_terminal_kind_t)kind;
    }
  }
  orm_result_destroy(result);
  rc = inbox_finish(transaction, rc, &error);
  inbox_connection_release(owner, slot);
  if (rc == SALTS_OK) {
    *out = (size_t)rows;
  } else {
    for (uint64_t row = 0u; row < rows && row < cap; ++row)
      entries[row] = (turbo_flow_inbox_history_entry_t)TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
  }
  return rc;
}

static int inbox_close(void *ctx) {
  inbox_owner_t *o = ctx;
  inbox_connection_slot_t *s = inbox_connection_acquire(o);
  if (!s) return SALTS_EBUSY;
  salts_mutex_lock(&o->mutex);
  if (!o->accepting) {
    int rc = o->closing ? SALTS_EBUSY : SALTS_OK;
    salts_mutex_unlock(&o->mutex);
    inbox_connection_release(o, s);
    return rc;
  }
  o->accepting = false;
  o->closing = true;
  for (size_t i = 0u; i < o->connection_count; ++i) {
    inbox_connection_slot_t *candidate = inbox_connection_at(o, i);
    if (candidate != s && candidate && candidate->busy) {
      o->accepting = true;
      o->closing = false;
      salts_mutex_unlock(&o->mutex);
      inbox_connection_release(o, s);
      return SALTS_EBUSY;
    }
  }
  salts_mutex_unlock(&o->mutex);
  orm_error_t e;
  orm_transaction_t *transaction = NULL;
  orm_result_t *r = NULL;
  char q[384];
  orm_error_init(&e);
  orm_value_t v = orm_i64((int64_t)o->generation);
  (void)snprintf(q, sizeof(q),
                 "UPDATE %s SET owner_state=owner_state WHERE singleton_id=1 AND generation=?1",
                 o->meta_table);
  int rc = inbox_begin(s, &transaction, &e);
  if (rc == SALTS_OK) rc = inbox_exec(s->connection, transaction, q, &v, 1, &r, &e);
  if (rc == SALTS_OK) rc = inbox_affected_one(r, &e);
  orm_result_destroy(r);
  rc = inbox_finish(transaction, rc, &e);
  inbox_connection_release(o, s);
  salts_mutex_lock(&o->mutex);
  o->closing = false;
  if (rc != SALTS_OK) o->accepting = true;
  salts_mutex_unlock(&o->mutex);
  return rc;
}
static int inbox_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *x) {
  inbox_owner_t *o = ctx;
  inbox_connection_slot_t *s = inbox_connection_acquire(o);
  if (!s) return SALTS_EBUSY;
  orm_error_t e;
  inbox_meta_t m = {0};
  orm_error_init(&e);
  int rc = inbox_read_meta(o, s, NULL, &m, &e);
  inbox_connection_release(o, s);
  if (rc != SALTS_OK) return rc;
  if (m.generation != (int64_t)o->generation) return SALTS_EBUSY;
  x->generation = o->generation;
  x->accepting = inbox_is_accepting(o);
  x->records = (size_t)m.records;
  x->history_records = (size_t)m.history_records;
  x->pending_records = (size_t)m.pending_records;
  x->failed_records = (size_t)m.failed_records;
  x->in_flight_claims = (size_t)m.in_flight_claims;
  x->retained_bytes = (size_t)m.retained_bytes;
  x->admitted = (uint64_t)m.admitted;
  x->completed = (uint64_t)m.completed;
  x->failed = (uint64_t)m.failed;
  x->retried = (uint64_t)m.retried;
  x->discarded = (uint64_t)m.discarded;
  return SALTS_OK;
}

static void inbox_owner_free(inbox_owner_t *o) {
  if (!o) return;
  for (size_t i = 0; i < vec_size(&o->connections); ++i) {
    inbox_connection_slot_t *slot = inbox_connection_at(o, i);
    if (slot && slot->connection) orm_disconnect(slot->connection);
  }
  for (size_t i = 0; i < vec_size(&o->leases); ++i) {
    inbox_lease_t *lease = inbox_lease_at(o, i);
    if (lease) mem_buffer_release(lease->storage);
  }
  if (o->mutex) salts_mutex_destroy(&o->mutex);
  vec_destroy(&o->connections);
  vec_destroy(&o->leases);
  free(o);
}
static int inbox_destroy(void *ctx) {
  inbox_owner_t *o = ctx;
  inbox_connection_slot_t *s = inbox_connection_acquire(o);
  if (!s) return SALTS_EBUSY;
  orm_error_t e;
  inbox_meta_t m = {0};
  orm_transaction_t *transaction = NULL;
  orm_error_init(&e);
  int rc = inbox_begin(s, &transaction, &e);
  if (rc == SALTS_OK) rc = inbox_read_meta(o, s, transaction, &m, &e);
  if (rc == SALTS_OK && m.generation != (int64_t)o->generation) {
    (void)inbox_finish(transaction, SALTS_EBUSY, &e);
    inbox_connection_release(o, s);
    if (inbox_has_live_lease(o)) return SALTS_EBUSY;
    inbox_owner_free(o);
    return SALTS_OK;
  }
  if (rc == SALTS_OK && (o->accepting || m.records != 0 || m.in_flight_claims != 0))
    rc = SALTS_EBUSY;
  orm_result_t *r = NULL;
  char q[384];
  orm_value_t v = orm_i64((int64_t)o->generation);
  (void)snprintf(q, sizeof(q), "UPDATE %s SET owner_state=0 WHERE singleton_id=1 AND generation=?1",
                 o->meta_table);
  if (rc == SALTS_OK) rc = inbox_exec(s->connection, transaction, q, &v, 1, &r, &e);
  if (rc == SALTS_OK) rc = inbox_affected_one(r, &e);
  orm_result_destroy(r);
  rc = inbox_finish(transaction, rc, &e);
  inbox_connection_release(o, s);
  if (rc == SALTS_OK) inbox_owner_free(o);
  return rc;
}

static const turbo_flow_inbox_ops_v2_t inbox_ops = {.size = sizeof(turbo_flow_inbox_ops_v2_t),
                                                    .version = TURBO_FLOW_INBOX_API_VERSION,
                                                    .admit = inbox_admit,
                                                    .claim = inbox_claim,
                                                    .claim_ex = inbox_claim_ex,
                                                    .complete = inbox_complete,
                                                    .fail = inbox_fail,
                                                    .retry = inbox_retry,
                                                    .discard = inbox_discard,
                                                    .forget = inbox_forget,
                                                    .scan_failed = inbox_scan_failed,
                                                    .scan_history = inbox_scan_history,
                                                    .close = inbox_close,
                                                    .snapshot = inbox_snapshot,
                                                    .destroy = inbox_destroy};

turbo_flow_turbodb_inbox_config_t turbo_flow_turbodb_inbox_config_default(void) {
  turbo_flow_inbox_memory_config_t m = turbo_flow_inbox_memory_config_default();
  turbo_flow_turbodb_inbox_config_t c;
  memset(&c, 0, sizeof(c));
  c.size = sizeof(c);
  c.version = TURBO_FLOW_TURBODB_INBOX_API_VERSION;
  c.max_records = m.max_records;
  c.max_total_bytes = m.max_total_bytes;
  c.max_record_bytes = m.max_record_bytes;
  c.max_claims = m.max_claims;
  c.connection_count = TURBO_FLOW_TURBODB_INBOX_DEFAULT_CONNECTIONS;
  c.open_mode = TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE;
  return c;
}

static bool inbox_vstr_equal(vstr a, const char *b) {
  size_t n = strlen(b);
  return a.len == n && a.data && memcmp(a.data, b, n) == 0;
}
static bool inbox_config_valid(const turbo_flow_turbodb_inbox_config_t *c) {
  if (!c || c->size != sizeof(*c) || c->version != TURBO_FLOW_TURBODB_INBOX_API_VERSION ||
      !c->database || !c->namespace_name)
    return false;
  if (c->database->struct_size != sizeof(*c->database) ||
      c->database->abi_version != ORM_C_ABI_VERSION ||
      (c->database->option_count != 0u && !c->database->options))
    return false;
  if (!c->max_records || !c->max_total_bytes || !c->max_record_bytes ||
      c->max_record_bytes > c->max_total_bytes || !c->max_claims ||
      c->max_claims > c->max_records || !c->connection_count ||
      c->connection_count > TURBO_FLOW_TURBODB_INBOX_MAX_CONNECTIONS ||
      c->max_records > INT64_MAX || c->max_total_bytes > INT64_MAX ||
      c->max_record_bytes > INT64_MAX || c->max_claims > INT64_MAX)
    return false;
  size_t n = strlen(c->namespace_name);
  if (!n || n > TURBO_FLOW_TURBODB_INBOX_NAMESPACE_MAX ||
      !((c->namespace_name[0] >= 'A' && c->namespace_name[0] <= 'Z') ||
        (c->namespace_name[0] >= 'a' && c->namespace_name[0] <= 'z') ||
        c->namespace_name[0] == '_'))
    return false;
  for (size_t i = 1; i < n; ++i)
    if (!((c->namespace_name[i] >= 'A' && c->namespace_name[i] <= 'Z') ||
          (c->namespace_name[i] >= 'a' && c->namespace_name[i] <= 'z') ||
          (c->namespace_name[i] >= '0' && c->namespace_name[i] <= '9') ||
          c->namespace_name[i] == '_'))
      return false;
  return (c->open_mode == TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE && c->expected_generation == 0) ||
         (c->open_mode == TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER && c->expected_generation > 0 &&
          c->expected_generation < INT64_MAX);
}

int turbo_flow_turbodb_inbox_create(const turbo_flow_turbodb_inbox_config_t *c,
                                    turbo_flow_inbox_t *out, orm_error_t *error) {
  if (error) orm_error_init(error);
  if (!out || out->size != sizeof(*out) || out->version != TURBO_FLOW_INBOX_API_VERSION ||
      out->ops || out->ctx || !error || !inbox_config_valid(c))
    return SALTS_EINVAL;
  if (!inbox_vstr_equal(c->database->driver, "sqlite")) return SALTS_ENOTSUP;
  bool file = false;
  for (uint32_t i = 0; i < c->database->option_count; ++i)
    if (inbox_vstr_equal(c->database->options[i].keyword, "filename")) {
      vstr v = c->database->options[i].value;
      file = v.data && v.len && !(v.len == 8 && memcmp(v.data, ":memory:", 8) == 0);
    }
  if (!file) return SALTS_ENOTSUP;
  inbox_owner_t *o = calloc(1, sizeof(*o));
  if (!o) return SALTS_ENOMEM;
  if (vec_init_bytes(&o->connections, sizeof(inbox_connection_slot_t),
                     _Alignof(inbox_connection_slot_t), c->connection_count) != STL_OK ||
      vec_resize(&o->connections, c->connection_count) != STL_OK ||
      vec_init_bytes(&o->leases, sizeof(inbox_lease_t), _Alignof(inbox_lease_t), c->max_claims) !=
          STL_OK ||
      vec_resize(&o->leases, c->max_claims) != STL_OK) {
    vec_destroy(&o->connections);
    vec_destroy(&o->leases);
    free(o);
    return SALTS_ENOMEM;
  }
  o->connection_count = c->connection_count;
  o->max_claims = c->max_claims;
  o->max_records = c->max_records;
  o->max_total_bytes = c->max_total_bytes;
  o->max_record_bytes = c->max_record_bytes;
  o->accepting = true;
  (void)snprintf(o->meta_table, sizeof(o->meta_table), "%s_inbox_meta_v2", c->namespace_name);
  (void)snprintf(o->records_table, sizeof(o->records_table), "%s_inbox_records_v2",
                 c->namespace_name);
  salts_mutex_init(&o->mutex);
  if (!o->mutex) {
    inbox_owner_free(o);
    return SALTS_ENOMEM;
  }
  for (size_t i = 0; i < o->connection_count; ++i) {
    inbox_connection_slot_t *connection = inbox_connection_at(o, i);
    orm_status_t s = orm_connect(c->database, &connection->connection, error);
    if (s != ORM_STATUS_OK) {
      int rc = inbox_orm_status(s);
      inbox_owner_free(o);
      return rc;
    }
  }
  inbox_connection_slot_t *control = inbox_connection_at(o, 0u);
  orm_transaction_t *t = NULL;
  orm_result_t *r = NULL;
  inbox_meta_t m = {0};
  char q[1024];
  int rc = inbox_begin(control, &t, error);
  if (rc == SALTS_OK) rc = inbox_preflight(o, control, t, c, error);
  if (rc == SALTS_OK) rc = inbox_read_meta(o, control, t, &m, error);
  if (rc == SALTS_OK && c->open_mode == TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE &&
      m.owner_state != INBOX_OWNER_CLOSED)
    rc = SALTS_EBUSY;
  if (rc == SALTS_OK && c->open_mode == TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER &&
      (m.owner_state != INBOX_OWNER_ACTIVE || m.generation != (int64_t)c->expected_generation))
    rc = SALTS_EBUSY;
  if (rc == SALTS_OK && c->open_mode == TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER &&
      (m.in_flight_claims > INT64_MAX - m.failed_records ||
       m.in_flight_claims > INT64_MAX - m.failed))
    rc = SALTS_ERANGE;
  if (rc == SALTS_OK && m.generation == INT64_MAX) rc = SALTS_ERANGE;
  uint64_t next = rc == SALTS_OK ? (uint64_t)m.generation + 1u : 0u;
  if (rc == SALTS_OK && c->open_mode == TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER) {
    orm_value_t v[] = {orm_i64(SALTS_EBUSY), orm_i64(TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN),
                       orm_i64((int64_t)c->expected_generation)};
    (void)snprintf(q, sizeof(q),
                   "UPDATE %s SET phase=2,failure_status=?1,failure_kind=?2 WHERE phase=1 AND "
                   "claim_generation=?3",
                   o->records_table);
    rc = inbox_exec(control->connection, t, q, v, 3, &r, error);
    uint64_t affected = 0u;
    if (rc == SALTS_OK) rc = inbox_orm_status(orm_result_affected_rows(r, &affected, error));
    if (rc == SALTS_OK && affected != (uint64_t)m.in_flight_claims) rc = SALTS_EPROTO;
    orm_result_destroy(r);
    r = NULL;
  }
  orm_value_t v[] = {orm_i64((int64_t)next), orm_i64((int64_t)m.generation)};
  (void)snprintf(
      q, sizeof(q),
      "UPDATE %s SET "
      "generation=?1,owner_state=1,failed_records=failed_records+in_flight_claims,failed=failed+in_"
      "flight_claims,in_flight_claims=0 WHERE singleton_id=1 AND generation=?2",
      o->meta_table);
  if (rc == SALTS_OK) rc = inbox_exec(control->connection, t, q, v, 2, &r, error);
  if (rc == SALTS_OK) rc = inbox_affected_one(r, error);
  orm_result_destroy(r);
  rc = inbox_finish(t, rc, error);
  if (rc != SALTS_OK) {
    inbox_owner_free(o);
    return rc;
  }
  o->generation = next;
  out->ops = &inbox_ops;
  out->ctx = o;
  return SALTS_OK;
}
