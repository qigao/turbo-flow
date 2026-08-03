#include "flowie_cluster_pgsql_internal.h"

#include "libpq-fe.h"
#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLUSTER_PGSQL_OID_INT2 21u
#define FLOWIE_CLUSTER_PGSQL_OID_INT4 23u
#define FLOWIE_CLUSTER_PGSQL_OID_INT8 20u
#define FLOWIE_CLUSTER_PGSQL_OID_TEXT 25u
#define FLOWIE_CLUSTER_PGSQL_OID_BYTEA 17u
#define FLOWIE_CLUSTER_FACT_DIGEST_DOMAIN "flowie.cluster.fact.command.v1"
#define FLOWIE_CLUSTER_FACT_CAPACITY_LOCK_SEED "1908145071"

struct flowie_cluster_pgsql_fact_store_s {
  PGconn *connection;
  tstr_t conninfo;
  tstr_t schema_name;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t node_id;
  tstr_t advertised_endpoint;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  uint32_t hash_version;
  uint32_t shard_count;
  uint64_t lease_ttl_ms;
  uint64_t renew_interval_ms;
  uint64_t retry_interval_ms;
  uint64_t worst_case_db_latency_ms;
  uint64_t safety_margin_ms;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_fact_records;
  size_t max_receipts;
  size_t max_dedupe_records;
  size_t max_outbox_records;
  uint64_t max_outbox_bytes;
  size_t max_event_payload_size;
  tstr_t ownership_sql;
  tstr_t capacity_lock_sql;
  tstr_t receipt_select_sql;
  tstr_t dedupe_select_sql;
  tstr_t shared_selection_select_sql;
  tstr_t dedupe_ack_count_sql;
  tstr_t revision_select_sql;
  tstr_t capacity_select_sql;
  tstr_t receipt_insert_sql;
  tstr_t dedupe_insert_sql;
  tstr_t shared_selection_insert_sql;
  tstr_t fact_insert_sql;
  tstr_t fact_update_sql;
  tstr_t fact_delete_sql;
  tstr_t fact_scan_sql;
  tstr_t fact_get_sql;
  tstr_t outbox_insert_sql;
  tstr_t outbox_next_sql;
  tstr_t outbox_settle_sql;
  tstr_t outbox_settle_confirm_sql;
};

static void flowie_cluster_pgsql_fact_config_view(flowie_cluster_pgsql_fact_store_t *store,
                                                  flowie_cluster_pgsql_config_t *coordinator,
                                                  flowie_cluster_pgsql_fact_config_t *config);
static int flowie_cluster_pgsql_fact_owner_validate(
    const flowie_cluster_pgsql_fact_store_t *store,
    const flowie_cluster_owner_token_t *current_owner);

static int flowie_cluster_pgsql_fact_sqlstate_status(const char *sqlstate) {
  if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0 ||
                   strcmp(sqlstate, "55P03") == 0))
    return TURBO_EBUSY;
  if (sqlstate && strcmp(sqlstate, "57014") == 0) return TURBO_ETIMEDOUT;
  return TURBO_EIO;
}

static int flowie_cluster_pgsql_fact_result_status(PGresult *result, ExecStatusType expected) {
  if (result && PQresultStatus(result) == expected) return TURBO_OK;
  return flowie_cluster_pgsql_fact_sqlstate_status(
      result ? PQresultErrorField(result, PG_DIAG_SQLSTATE) : NULL);
}

static int flowie_cluster_pgsql_fact_exec(PGconn *connection, const char *sql,
                                          ExecStatusType expected) {
  PGresult *result;
  int rc;
  if (!connection || !sql) return TURBO_EINVAL;
  result = PQexec(connection, sql);
  rc = flowie_cluster_pgsql_fact_result_status(result, expected);
  if (result) PQclear(result);
  return rc;
}

static PGconn *flowie_cluster_pgsql_fact_connect(const char *conninfo, uint64_t timeout_ms) {
  const char *keywords[3] = {"dbname", "connect_timeout", NULL};
  const char *values[3];
  char timeout_seconds[32];
  uint64_t seconds = timeout_ms / FLOWIE_CLUSTER_PGSQL_MS_PER_SECOND;
  int written;
  if (timeout_ms % FLOWIE_CLUSTER_PGSQL_MS_PER_SECOND != 0u) ++seconds;
  if (seconds == 0u) seconds = 1u;
  written = snprintf(timeout_seconds, sizeof(timeout_seconds), "%llu", (unsigned long long)seconds);
  if (written <= 0 || (size_t)written >= sizeof(timeout_seconds)) return NULL;
  values[0] = conninfo;
  values[1] = timeout_seconds;
  values[2] = NULL;
  return PQconnectdbParams(keywords, values, 1);
}

static int flowie_cluster_pgsql_fact_set_timeouts(PGconn *connection, uint64_t timeout_ms) {
  static const char sql[] = "SELECT pg_catalog.set_config('statement_timeout',$1,false),"
                            "pg_catalog.set_config('lock_timeout',$1,false)";
  char timeout[32];
  const char *values[1] = {timeout};
  PGresult *result;
  int written = snprintf(timeout, sizeof(timeout), "%llu", (unsigned long long)timeout_ms);
  int rc;
  if (written <= 0 || (size_t)written >= sizeof(timeout)) return TURBO_ERANGE;
  result = PQexecParams(connection, sql, 1, NULL, values, NULL, NULL, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 2)) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_rollback(flowie_cluster_pgsql_fact_store_t *store,
                                              int status) {
  if (store && store->connection)
    (void)flowie_cluster_pgsql_fact_exec(store->connection, "ROLLBACK", PGRES_COMMAND_OK);
  return status;
}

static int flowie_cluster_pgsql_fact_parse_u64(const char *text, size_t size, uint64_t *out) {
  char buffer[32];
  char *end = NULL;
  unsigned long long value;
  if (!text || size == 0u || size >= sizeof(buffer) || !out) return TURBO_EPROTO;
  memcpy(buffer, text, size);
  buffer[size] = '\0';
  errno = 0;
  value = strtoull(buffer, &end, 10);
  if (errno != 0 || end != buffer + size) return TURBO_EPROTO;
  *out = (uint64_t)value;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_fact_parse_size(const char *text, size_t size, size_t *out) {
  uint64_t value;
  int rc = flowie_cluster_pgsql_fact_parse_u64(text, size, &value);
  if (rc != TURBO_OK) return rc;
  if (value > SIZE_MAX) return TURBO_ERANGE;
  *out = (size_t)value;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_fact_nonzero_bytes(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  if (!bytes) return 0;
  for (size_t index = 0u; index < size; ++index)
    combined |= bytes[index];
  return combined != 0u;
}

int flowie_cluster_pgsql_fact_config_validate(const flowie_cluster_pgsql_fact_config_t *config) {
  int rc;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || !config->coordinator)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_config_validate(config->coordinator);
  if (rc != TURBO_OK) return rc;
  if (config->max_key_size == 0u || config->max_key_size > FLOWIE_CLUSTER_KEY_MAX ||
      config->max_value_size == 0u || config->max_value_size > INT_MAX ||
      config->max_batch_size == 0u ||
      config->max_batch_size > FLOWIE_CLUSTER_PGSQL_FACT_BATCH_MAX ||
      config->max_fact_records == 0u || config->max_fact_records > INT64_MAX ||
      config->max_receipts == 0u || config->max_receipts > INT64_MAX ||
      config->max_dedupe_records == 0u || config->max_dedupe_records > INT64_MAX ||
      config->max_outbox_records == 0u || config->max_outbox_records > INT64_MAX ||
      config->max_outbox_bytes == 0u || config->max_outbox_bytes > INT64_MAX ||
      config->max_event_payload_size == 0u || config->max_event_payload_size > INT_MAX)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int
flowie_cluster_pgsql_fact_record_equal(const flowie_cluster_pgsql_fact_mutation_t *left,
                                       const flowie_cluster_pgsql_fact_mutation_t *right) {
  return left->key_kind == right->key_kind && left->record.key_size == right->record.key_size &&
         memcmp(left->record.key, right->record.key, left->record.key_size) == 0;
}

int flowie_cluster_pgsql_fact_command_validate(const flowie_cluster_pgsql_fact_config_t *config,
                                               const flowie_cluster_pgsql_fact_command_t *command) {
  const flowie_cluster_pgsql_config_t *coordinator;
  flowie_cluster_owner_token_t expected = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  uint64_t event_bytes = 0u;
  int rc = flowie_cluster_pgsql_fact_config_validate(config);
  if (rc != TURBO_OK) return rc;
  coordinator = config->coordinator;
  if (!command || command->size < sizeof(*command) ||
      command->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      !flowie_cluster_pgsql_fact_nonzero_bytes(command->command_id, sizeof(command->command_id)) ||
      ((!command->mutations || command->mutation_count == 0u) && !command->dedupe) ||
      (command->mutation_count != 0u && !command->mutations) ||
      command->mutation_count > config->max_batch_size)
    return TURBO_EINVAL;
  if (command->owner.shard_id >= coordinator->shard_count) return TURBO_EINVAL;
  rc = flowie_cluster_owner_token_init(&expected, command->owner.shard_id,
                                       command->owner.owner_epoch, coordinator->node_id,
                                       strlen(coordinator->node_id), coordinator->boot_id);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_owner_token_require(&expected, &command->owner);
  if (rc != TURBO_OK) return rc;
  if (command->dedupe &&
      (command->dedupe->size < sizeof(*command->dedupe) ||
       command->dedupe->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
       !flowie_cluster_pgsql_fact_nonzero_bytes(command->dedupe->source_command_id,
                                                sizeof(command->dedupe->source_command_id)) ||
       command->dedupe->source_shard_id >= coordinator->shard_count ||
       command->dedupe->source_owner_epoch == 0u ||
       command->dedupe->source_fact_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
       command->dedupe->target_session_id > INT64_MAX ||
       (command->dedupe->shared_filter_size != 0u &&
        (!command->dedupe->shared_filter ||
         command->dedupe->shared_filter_size > config->max_key_size)) ||
       (command->dedupe->shared_filter_size == 0u && command->dedupe->shared_filter) ||
       !flowie_cluster_pgsql_fact_nonzero_bytes(command->dedupe->event_digest,
                                                sizeof(command->dedupe->event_digest))))
    return TURBO_EINVAL;
  for (size_t index = 0u; index < command->mutation_count; ++index) {
    const flowie_cluster_pgsql_fact_mutation_t *mutation = &command->mutations[index];
    const turbo_flow_record_mutation_t *record = &mutation->record;
    uint32_t shard_id = UINT32_MAX;
    if (mutation->size < sizeof(*mutation) ||
        (mutation->write_kind != FLOWIE_CLUSTER_PGSQL_FACT_AND_EVENT &&
         mutation->write_kind != FLOWIE_CLUSTER_PGSQL_EVENT_ONLY &&
         mutation->write_kind != FLOWIE_CLUSTER_PGSQL_FACT_ONLY) ||
        (mutation->key_kind != FLOWIE_CLUSTER_KEY_SESSION &&
         mutation->key_kind != FLOWIE_CLUSTER_KEY_RETAINED) ||
        !mutation->shard_key || mutation->shard_key_size == 0u ||
        mutation->shard_key_size > FLOWIE_CLUSTER_KEY_MAX || record->size < sizeof(*record) ||
        !record->key || record->key_size == 0u || record->key_size > config->max_key_size ||
        record->expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
        record->next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
        (!mutation->event_payload && mutation->event_payload_size != 0u) ||
        mutation->event_payload_size > config->max_event_payload_size)
      return TURBO_EINVAL;
    if (mutation->write_kind == FLOWIE_CLUSTER_PGSQL_FACT_ONLY) {
      if (mutation->event_type != 0u || mutation->event_payload ||
          mutation->event_payload_size != 0u)
        return TURBO_EINVAL;
    } else if (mutation->event_type == 0u) {
      return TURBO_EINVAL;
    }
    if (mutation->write_kind == FLOWIE_CLUSTER_PGSQL_EVENT_ONLY) {
      if (record->kind != TURBO_FLOW_RECORD_PUT ||
          record->expected_revision != TURBO_FLOW_RECORD_REVISION_ABSENT ||
          record->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || record->value ||
          record->value_size != 0u)
        return TURBO_EINVAL;
    } else if (record->kind == TURBO_FLOW_RECORD_PUT) {
      if ((!record->value && record->value_size != 0u) ||
          record->value_size > config->max_value_size ||
          record->next_revision <= record->expected_revision)
        return TURBO_EINVAL;
    } else if (record->kind == TURBO_FLOW_RECORD_DELETE) {
      if (record->expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT ||
          record->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || record->value ||
          record->value_size != 0u)
        return TURBO_EINVAL;
    } else {
      return TURBO_EINVAL;
    }
    if (mutation->write_kind != FLOWIE_CLUSTER_PGSQL_FACT_ONLY) {
      if (event_bytes > UINT64_MAX - mutation->event_payload_size) return TURBO_ERANGE;
      event_bytes += mutation->event_payload_size;
    }
    if (event_bytes > config->max_outbox_bytes) return TURBO_ENOSPC;
    rc = flowie_cluster_shard_for_key(
        coordinator->hash_version, mutation->key_kind, (const uint8_t *)coordinator->cluster_id,
        strlen(coordinator->cluster_id), (const uint8_t *)coordinator->listener_id,
        strlen(coordinator->listener_id), mutation->shard_key, mutation->shard_key_size,
        coordinator->shard_count, &shard_id);
    if (rc != TURBO_OK) return rc;
    if (shard_id != command->owner.shard_id) return TURBO_EBUSY;
    /* Batch is capped at 256; this keeps validation allocation-free and deterministic. */
    for (size_t previous = 0u; previous < index; ++previous)
      if (flowie_cluster_pgsql_fact_record_equal(mutation, &command->mutations[previous]))
        return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int flowie_cluster_pgsql_fact_scan_validate(const flowie_cluster_pgsql_fact_config_t *config,
                                            const flowie_cluster_pgsql_fact_scan_t *scan) {
  const flowie_cluster_pgsql_config_t *coordinator;
  flowie_cluster_owner_token_t expected = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  int rc = flowie_cluster_pgsql_fact_config_validate(config);
  if (rc != TURBO_OK) return rc;
  if (!scan || scan->size < sizeof(*scan) || scan->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      (scan->key_kind != FLOWIE_CLUSTER_KEY_SESSION &&
       scan->key_kind != FLOWIE_CLUSTER_KEY_RETAINED) ||
      scan->max_records == 0u || scan->max_records >= INT_MAX ||
      scan->max_records > config->max_fact_records)
    return TURBO_EINVAL;
  coordinator = config->coordinator;
  if (scan->owner.shard_id >= coordinator->shard_count) return TURBO_EINVAL;
  rc = flowie_cluster_owner_token_init(&expected, scan->owner.shard_id, scan->owner.owner_epoch,
                                       coordinator->node_id, strlen(coordinator->node_id),
                                       coordinator->boot_id);
  if (rc != TURBO_OK) return rc;
  return flowie_cluster_owner_token_require(&expected, &scan->owner);
}

static void flowie_cluster_pgsql_fact_digest_u64(crypto_blake2b_ctx *ctx, uint64_t value) {
  uint8_t encoded[8];
  for (size_t index = 0u; index < sizeof(encoded); ++index)
    encoded[sizeof(encoded) - index - 1u] = (uint8_t)(value >> (index * 8u));
  crypto_blake2b_update(ctx, encoded, sizeof(encoded));
}

static void flowie_cluster_pgsql_fact_digest_bytes(crypto_blake2b_ctx *ctx, const void *bytes,
                                                   size_t size) {
  flowie_cluster_pgsql_fact_digest_u64(ctx, size);
  if (size != 0u) crypto_blake2b_update(ctx, (const uint8_t *)bytes, size);
}

static void flowie_cluster_pgsql_fact_digest(const flowie_cluster_pgsql_fact_command_t *command,
                                             uint8_t out[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE]) {
  crypto_blake2b_ctx ctx;
  crypto_blake2b_init(&ctx, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE);
  crypto_blake2b_update(&ctx, (const uint8_t *)FLOWIE_CLUSTER_FACT_DIGEST_DOMAIN,
                        sizeof(FLOWIE_CLUSTER_FACT_DIGEST_DOMAIN) - 1u);
  flowie_cluster_pgsql_fact_digest_bytes(&ctx, command->command_id, sizeof(command->command_id));
  flowie_cluster_pgsql_fact_digest_u64(&ctx, command->owner.shard_id);
  flowie_cluster_pgsql_fact_digest_u64(&ctx, command->owner.owner_epoch);
  flowie_cluster_pgsql_fact_digest_bytes(&ctx, command->owner.node_id, command->owner.node_id_size);
  flowie_cluster_pgsql_fact_digest_bytes(&ctx, command->owner.boot_id,
                                         sizeof(command->owner.boot_id));
  flowie_cluster_pgsql_fact_digest_u64(&ctx, command->mutation_count);
  flowie_cluster_pgsql_fact_digest_u64(&ctx, command->dedupe ? 1u : 0u);
  if (command->dedupe) {
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, command->dedupe->source_command_id,
                                           sizeof(command->dedupe->source_command_id));
    flowie_cluster_pgsql_fact_digest_u64(&ctx, command->dedupe->event_index);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, command->dedupe->source_shard_id);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, command->dedupe->source_owner_epoch);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, command->dedupe->source_fact_revision);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, command->dedupe->target_session_id);
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, command->dedupe->event_digest,
                                           sizeof(command->dedupe->event_digest));
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, command->dedupe->shared_filter,
                                           command->dedupe->shared_filter_size);
  }
  for (size_t index = 0u; index < command->mutation_count; ++index) {
    const flowie_cluster_pgsql_fact_mutation_t *mutation = &command->mutations[index];
    const turbo_flow_record_mutation_t *record = &mutation->record;
    flowie_cluster_pgsql_fact_digest_u64(&ctx, mutation->write_kind);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, mutation->key_kind);
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, mutation->shard_key, mutation->shard_key_size);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, record->kind);
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, record->key, record->key_size);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, record->expected_revision);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, record->next_revision);
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, record->value, record->value_size);
    flowie_cluster_pgsql_fact_digest_u64(&ctx, mutation->event_type);
    flowie_cluster_pgsql_fact_digest_bytes(&ctx, mutation->event_payload,
                                           mutation->event_payload_size);
  }
  crypto_blake2b_final(&ctx, out);
}

static tstr_t flowie_cluster_pgsql_fact_sql(const char *format, const char *schema) {
  tstr_t sql = tstr_new();
  tstr_t next;
  if (!sql) return NULL;
  next = tstr_cat_fmt(sql, format, schema, schema, schema, schema, schema, schema, schema, schema);
  if (!next) {
    tstr_free(sql);
    return NULL;
  }
  return next;
}

static tstr_t flowie_cluster_pgsql_fact_schema_ddl(const char *schema) {
  tstr_t sql = tstr_new();
  tstr_t next;
  if (!sql) return NULL;
#define FLOWIE_CLUSTER_FACT_APPEND(...)                                                            \
  do {                                                                                             \
    next = tstr_cat_fmt(sql, __VA_ARGS__);                                                         \
    if (!next) {                                                                                   \
      tstr_free(sql);                                                                              \
      return NULL;                                                                                 \
    }                                                                                              \
    sql = next;                                                                                    \
  } while (0)
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_command_receipt("
      "cluster_id TEXT NOT NULL,listener_id TEXT NOT NULL,command_id BYTEA NOT NULL CHECK("
      "octet_length(command_id)=%u),command_digest BYTEA NOT NULL CHECK(octet_length("
      "command_digest)=%u),shard_id INTEGER NOT NULL CHECK(shard_id>=0),owner_epoch BIGINT NOT "
      "NULL CHECK(owner_epoch>0),mutation_count INTEGER NOT NULL CHECK(mutation_count>0),"
      "created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),PRIMARY KEY("
      "cluster_id,listener_id,command_id),FOREIGN KEY(cluster_id) REFERENCES %s.cluster_config("
      "cluster_id));",
      schema, FLOWIE_CLUSTER_COMMAND_ID_SIZE, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE, schema);
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_fact("
      "cluster_id TEXT NOT NULL,listener_id TEXT NOT NULL,record_kind SMALLINT NOT NULL CHECK("
      "record_kind IN (1,2)),record_key BYTEA NOT NULL,shard_id INTEGER NOT NULL "
      "CHECK(shard_id>=0),"
      "revision BIGINT NOT NULL CHECK(revision>0),owner_epoch BIGINT NOT NULL CHECK(owner_epoch>0),"
      "value BYTEA NOT NULL,PRIMARY KEY(cluster_id,listener_id,record_kind,record_key),FOREIGN KEY("
      "cluster_id) REFERENCES %s.cluster_config(cluster_id));",
      schema, schema);
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_event_dedupe("
      "cluster_id TEXT NOT NULL,listener_id TEXT NOT NULL,target_shard_id INTEGER NOT NULL CHECK("
      "target_shard_id>=0),target_session_id BIGINT NOT NULL CHECK(target_session_id>=0),"
      "source_command_id BYTEA NOT NULL CHECK(octet_length(source_command_id)=%u),event_index "
      "INTEGER NOT NULL CHECK(event_index>=0),source_shard_id INTEGER NOT NULL CHECK("
      "source_shard_id>=0),source_owner_epoch BIGINT NOT NULL CHECK(source_owner_epoch>0),"
      "source_fact_revision BIGINT NOT NULL CHECK(source_fact_revision>=0),event_digest BYTEA NOT "
      "NULL CHECK(octet_length(event_digest)=%u),created_at TIMESTAMPTZ NOT NULL DEFAULT "
      "pg_catalog.clock_timestamp(),PRIMARY KEY(cluster_id,listener_id,target_shard_id,"
      "target_session_id,source_command_id,event_index),FOREIGN KEY(cluster_id) REFERENCES "
      "%s.cluster_config(cluster_id));",
      schema, FLOWIE_CLUSTER_COMMAND_ID_SIZE, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE, schema);
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE INDEX IF NOT EXISTS cluster_event_dedupe_source_idx ON "
      "%s.cluster_event_dedupe(cluster_id,listener_id,source_command_id,event_index);",
      schema);
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_shared_selection("
      "cluster_id TEXT NOT NULL,listener_id TEXT NOT NULL,source_command_id BYTEA NOT NULL CHECK("
      "octet_length(source_command_id)=%u),event_index INTEGER NOT NULL CHECK(event_index>=0),"
      "shared_filter BYTEA NOT NULL,source_shard_id INTEGER NOT NULL CHECK(source_shard_id>=0),"
      "source_owner_epoch BIGINT NOT NULL CHECK(source_owner_epoch>0),source_fact_revision BIGINT "
      "NOT NULL CHECK(source_fact_revision>=0),event_digest BYTEA NOT NULL CHECK(octet_length("
      "event_digest)=%u),target_shard_id INTEGER NOT NULL CHECK(target_shard_id>=0),"
      "target_session_id BIGINT NOT NULL CHECK(target_session_id>0),created_at TIMESTAMPTZ NOT "
      "NULL DEFAULT pg_catalog.clock_timestamp(),PRIMARY KEY(cluster_id,listener_id,"
      "source_command_id,event_index,shared_filter),FOREIGN KEY(cluster_id) REFERENCES "
      "%s.cluster_config(cluster_id));",
      schema, FLOWIE_CLUSTER_COMMAND_ID_SIZE, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE, schema);
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_outbox("
      "cluster_id TEXT NOT NULL,listener_id TEXT NOT NULL,command_id BYTEA NOT NULL,event_index "
      "INTEGER NOT NULL CHECK(event_index>=0),shard_id INTEGER NOT NULL CHECK(shard_id>=0),"
      "owner_epoch BIGINT NOT NULL CHECK(owner_epoch>0),event_type BIGINT NOT NULL CHECK("
      "event_type>0),record_kind SMALLINT NOT NULL CHECK(record_kind IN (1,2)),record_key BYTEA "
      "NOT NULL,fact_revision BIGINT NOT NULL CHECK(fact_revision>=0),payload BYTEA NOT NULL,"
      "payload_size BIGINT NOT NULL CHECK(payload_size>=0),created_at TIMESTAMPTZ NOT NULL DEFAULT "
      "pg_catalog.clock_timestamp(),published_at TIMESTAMPTZ,attempt_count BIGINT NOT NULL DEFAULT "
      "0 CHECK(attempt_count>=0),PRIMARY "
      "KEY(cluster_id,listener_id,command_id,event_index),FOREIGN "
      "KEY(cluster_id,listener_id,command_id) REFERENCES %s.cluster_command_receipt(cluster_id,"
      "listener_id,command_id));",
      schema, schema);
  FLOWIE_CLUSTER_FACT_APPEND(
      "CREATE INDEX IF NOT EXISTS cluster_outbox_pending_idx ON %s.cluster_outbox("
      "cluster_id,listener_id,shard_id,created_at,command_id,event_index) WHERE published_at IS "
      "NULL;",
      schema);
#undef FLOWIE_CLUSTER_FACT_APPEND
  return sql;
}

static int flowie_cluster_pgsql_fact_schema_prepare(flowie_cluster_pgsql_fact_store_t *store,
                                                    int create_schema) {
  tstr_t sql = NULL;
  PGresult *result = NULL;
  int rc;
  if (create_schema) {
    sql = flowie_cluster_pgsql_fact_schema_ddl(store->schema_name);
    if (!sql) return TURBO_ENOMEM;
    rc = flowie_cluster_pgsql_fact_exec(store->connection, sql, PGRES_COMMAND_OK);
    tstr_free(sql);
    if (rc != TURBO_OK) return rc;
  }
  sql = flowie_cluster_pgsql_fact_sql(
      "SELECT r.command_id::bytea,r.command_digest::bytea,f.record_key::bytea,f.revision::text,"
      "o.payload::bytea,o.payload_size::text,d.source_command_id::bytea,d.event_digest::bytea,"
      "x.shared_filter::bytea "
      "FROM %s.cluster_command_receipt r CROSS JOIN %s.cluster_fact f CROSS JOIN "
      "%s.cluster_outbox o CROSS JOIN %s.cluster_event_dedupe d CROSS JOIN "
      "%s.cluster_shared_selection x LIMIT 0",
      store->schema_name);
  if (!sql) return TURBO_ENOMEM;
  result = PQexec(store->connection, sql);
  tstr_free(sql);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK &&
      (PQnfields(result) != 9 || PQftype(result, 0) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
       PQftype(result, 1) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
       PQftype(result, 2) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
       PQftype(result, 3) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
       PQftype(result, 4) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
       PQftype(result, 5) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
       PQftype(result, 6) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
       PQftype(result, 7) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
       PQftype(result, 8) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA))
    rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_sql_prepare(flowie_cluster_pgsql_fact_store_t *store) {
  const char *schema = store->schema_name;
#define FLOWIE_CLUSTER_FACT_SQL(field, format)                                                     \
  do {                                                                                             \
    store->field = flowie_cluster_pgsql_fact_sql(format, schema);                                  \
    if (!store->field) return TURBO_ENOMEM;                                                        \
  } while (0)
  FLOWIE_CLUSTER_FACT_SQL(
      ownership_sql,
      "SELECT 1 FROM %s.cluster_shard s JOIN %s.cluster_node n ON n.cluster_id=s.cluster_id AND "
      "n.node_id=s.owner_node_id WHERE s.cluster_id=$1 AND s.listener_id=$2 AND "
      "s.shard_id=$3::integer AND s.owner_node_id=$4 AND s.owner_boot_id=$5 AND "
      "s.owner_epoch=$6::bigint AND s.lease_until>pg_catalog.clock_timestamp() AND n.boot_id=$5 "
      "AND n.lease_until>pg_catalog.clock_timestamp() FOR UPDATE OF s,n");
  FLOWIE_CLUSTER_FACT_SQL(capacity_lock_sql,
                          "SELECT "
                          "pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended($1||chr(31)"
                          "||$2," FLOWIE_CLUSTER_FACT_CAPACITY_LOCK_SEED "))");
  FLOWIE_CLUSTER_FACT_SQL(
      receipt_select_sql,
      "SELECT command_digest,shard_id::text,owner_epoch::text,mutation_count::text FROM "
      "%s.cluster_command_receipt WHERE cluster_id=$1 AND listener_id=$2 AND command_id=$3");
  FLOWIE_CLUSTER_FACT_SQL(
      dedupe_select_sql,
      "SELECT source_shard_id::text,source_owner_epoch::text,source_fact_revision::text,"
      "event_digest::bytea FROM %s.cluster_event_dedupe WHERE cluster_id=$1 AND listener_id=$2 "
      "AND target_shard_id=$3::integer AND target_session_id=$4::bigint AND "
      "source_command_id=$5 AND event_index=$6::integer");
  FLOWIE_CLUSTER_FACT_SQL(
      shared_selection_select_sql,
      "SELECT source_shard_id::text,source_owner_epoch::text,source_fact_revision::text,"
      "event_digest::bytea,target_shard_id::text,target_session_id::text FROM "
      "%s.cluster_shared_selection WHERE cluster_id=$1 AND listener_id=$2 AND "
      "source_command_id=$3 AND event_index=$4::integer AND shared_filter=$5");
  FLOWIE_CLUSTER_FACT_SQL(
      dedupe_ack_count_sql,
      "SELECT count(*) FILTER (WHERE source_shard_id=$5::integer AND "
      "source_owner_epoch=$6::bigint AND source_fact_revision=$7::bigint AND "
      "event_digest=$8)::text,count(*)::text FROM %s.cluster_event_dedupe WHERE cluster_id=$1 "
      "AND listener_id=$2 AND target_session_id=0 AND source_command_id=$3 AND "
      "event_index=$4::integer");
  FLOWIE_CLUSTER_FACT_SQL(
      revision_select_sql,
      "SELECT revision::text FROM %s.cluster_fact WHERE cluster_id=$1 AND listener_id=$2 AND "
      "record_kind=$3::smallint AND record_key=$4 FOR UPDATE");
  FLOWIE_CLUSTER_FACT_SQL(
      capacity_select_sql,
      "SELECT (SELECT count(*) FROM %s.cluster_fact WHERE cluster_id=$1 AND listener_id=$2)::text,"
      "(SELECT count(*) FROM %s.cluster_command_receipt WHERE cluster_id=$1 AND "
      "listener_id=$2)::text,"
      "((SELECT count(*) FROM %s.cluster_event_dedupe WHERE cluster_id=$1 AND "
      "listener_id=$2)+(SELECT count(*) FROM %s.cluster_shared_selection WHERE cluster_id=$1 "
      "AND listener_id=$2))::text,"
      "(SELECT count(*) FROM %s.cluster_outbox WHERE cluster_id=$1 AND listener_id=$2 AND "
      "published_at IS NULL)::text,(SELECT COALESCE(sum(payload_size),0) FROM %s.cluster_outbox "
      "WHERE cluster_id=$1 AND listener_id=$2 AND published_at IS NULL)::text");
  FLOWIE_CLUSTER_FACT_SQL(
      receipt_insert_sql,
      "INSERT INTO %s.cluster_command_receipt(cluster_id,listener_id,command_id,command_digest,"
      "shard_id,owner_epoch,mutation_count) "
      "VALUES($1,$2,$3,$4,$5::integer,$6::bigint,$7::integer)");
  FLOWIE_CLUSTER_FACT_SQL(
      dedupe_insert_sql,
      "INSERT INTO %s.cluster_event_dedupe(cluster_id,listener_id,target_shard_id,"
      "target_session_id,source_command_id,event_index,source_shard_id,source_owner_epoch,"
      "source_fact_revision,event_digest) VALUES($1,$2,$3::integer,$4::bigint,$5,$6::integer,"
      "$7::integer,$8::bigint,$9::bigint,$10)");
  FLOWIE_CLUSTER_FACT_SQL(
      shared_selection_insert_sql,
      "INSERT INTO %s.cluster_shared_selection(cluster_id,listener_id,source_command_id,"
      "event_index,shared_filter,source_shard_id,source_owner_epoch,source_fact_revision,"
      "event_digest,target_shard_id,target_session_id) VALUES($1,$2,$3,$4::integer,$5,"
      "$6::integer,$7::bigint,$8::bigint,$9,$10::integer,$11::bigint)");
  FLOWIE_CLUSTER_FACT_SQL(
      fact_insert_sql,
      "INSERT INTO %s.cluster_fact(cluster_id,listener_id,record_kind,record_key,shard_id,revision,"
      "owner_epoch,value) VALUES($1,$2,$3::smallint,$4,$5::integer,$6::bigint,$7::bigint,$8)");
  FLOWIE_CLUSTER_FACT_SQL(
      fact_update_sql,
      "UPDATE %s.cluster_fact SET revision=$5::bigint,owner_epoch=$6::bigint,value=$7 WHERE "
      "cluster_id=$1 AND listener_id=$2 AND record_kind=$3::smallint AND record_key=$4 AND "
      "revision=$8::bigint");
  FLOWIE_CLUSTER_FACT_SQL(fact_delete_sql,
                          "DELETE FROM %s.cluster_fact WHERE cluster_id=$1 AND listener_id=$2 AND "
                          "record_kind=$3::smallint AND record_key=$4 AND revision=$5::bigint");
  FLOWIE_CLUSTER_FACT_SQL(
      fact_scan_sql,
      "WITH owned AS (SELECT 1 FROM %s.cluster_shard s JOIN %s.cluster_node n ON "
      "n.cluster_id=s.cluster_id AND n.node_id=s.owner_node_id WHERE s.cluster_id=$1 AND "
      "s.listener_id=$2 AND s.shard_id=$3::integer AND s.owner_node_id=$4 AND "
      "s.owner_boot_id=$5 AND s.owner_epoch=$6::bigint AND "
      "s.lease_until>pg_catalog.clock_timestamp() AND n.boot_id=$5 AND "
      "n.lease_until>pg_catalog.clock_timestamp()) SELECT f.record_key::bytea,"
      "f.revision::text,f.value::bytea,f.record_key_size::text,f.value_size::text FROM owned LEFT "
      "JOIN LATERAL (SELECT CASE WHEN octet_length(record_key) BETWEEN 1 AND $9::bigint THEN "
      "record_key END AS record_key,revision,CASE WHEN octet_length(value)<=$10::bigint THEN value "
      "END AS value,octet_length(record_key) AS record_key_size,octet_length(value) AS value_size "
      "FROM %s.cluster_fact WHERE cluster_id=$1 AND listener_id=$2 AND "
      "record_kind=$7::smallint AND shard_id=$3::integer ORDER BY record_key LIMIT $8::bigint) f "
      "ON true ORDER BY f.record_key");
  FLOWIE_CLUSTER_FACT_SQL(
      fact_get_sql,
      "WITH owned AS (SELECT 1 FROM %s.cluster_shard s JOIN %s.cluster_node n ON "
      "n.cluster_id=s.cluster_id AND n.node_id=s.owner_node_id WHERE s.cluster_id=$1 AND "
      "s.listener_id=$2 AND s.shard_id=$3::integer AND s.owner_node_id=$4 AND "
      "s.owner_boot_id=$5 AND s.owner_epoch=$6::bigint AND "
      "s.lease_until>pg_catalog.clock_timestamp() AND n.boot_id=$5 AND "
      "n.lease_until>pg_catalog.clock_timestamp()) SELECT f.shard_id::text,f.owner_epoch::text,"
      "f.revision::text,f.record_key::bytea,f.value::bytea,octet_length(f.record_key)::text,"
      "octet_length(f.value)::text FROM owned LEFT JOIN LATERAL (SELECT shard_id,owner_epoch,"
      "revision,record_key,value FROM %s.cluster_fact WHERE cluster_id=$1 AND listener_id=$2 "
      "AND record_kind=$7::smallint AND record_key=$8 LIMIT 1) f ON true");
  FLOWIE_CLUSTER_FACT_SQL(
      outbox_insert_sql,
      "INSERT INTO %s.cluster_outbox(cluster_id,listener_id,command_id,event_index,shard_id,"
      "owner_epoch,event_type,record_kind,record_key,fact_revision,payload,payload_size) VALUES("
      "$1,$2,$3,$4::integer,$5::integer,$6::bigint,$7::bigint,$8::smallint,$9,$10::bigint,$11,"
      "$12::bigint)");
  FLOWIE_CLUSTER_FACT_SQL(
      outbox_next_sql,
      "WITH picked AS (SELECT o.command_id,o.event_index,o.attempt_count FROM "
      "%s.cluster_outbox o WHERE o.cluster_id=$1 AND o.listener_id=$2 AND "
      "o.shard_id=$3::integer AND o.event_type=$4::bigint AND o.published_at IS NULL ORDER BY "
      "o.created_at,o.command_id,o.event_index LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE "
      "%s.cluster_outbox o SET attempt_count=CASE WHEN p.attempt_count=9223372036854775807 THEN "
      "p.attempt_count ELSE p.attempt_count+1 END FROM picked p WHERE o.cluster_id=$1 AND "
      "o.listener_id=$2 AND o.command_id=p.command_id AND o.event_index=p.event_index RETURNING "
      "o.command_id::bytea,o.event_index::text,o.shard_id::text,o.owner_epoch::text,"
      "o.event_type::text,o.record_kind::text,o.record_key::bytea,o.fact_revision::text,"
      "o.payload::bytea,o.payload_size::text,"
      "floor(extract(epoch from o.created_at))::bigint::text,p.attempt_count::text,"
      "o.attempt_count::text");
  FLOWIE_CLUSTER_FACT_SQL(
      outbox_settle_sql,
      "UPDATE %s.cluster_outbox SET published_at=pg_catalog.clock_timestamp() WHERE "
      "cluster_id=$1 AND listener_id=$2 AND command_id=$3 AND event_index=$4::integer AND "
      "shard_id=$5::integer AND owner_epoch=$6::bigint AND event_type=$7::bigint AND "
      "record_kind=$8::smallint AND record_key=$9 AND fact_revision=$10::bigint AND payload=$11 "
      "AND payload_size=$12::bigint AND published_at IS NULL RETURNING 1");
  FLOWIE_CLUSTER_FACT_SQL(
      outbox_settle_confirm_sql,
      "SELECT 1 FROM %s.cluster_outbox WHERE cluster_id=$1 AND listener_id=$2 AND command_id=$3 "
      "AND event_index=$4::integer AND shard_id=$5::integer AND owner_epoch=$6::bigint AND "
      "event_type=$7::bigint AND record_kind=$8::smallint AND record_key=$9 AND "
      "fact_revision=$10::bigint AND payload=$11 AND payload_size=$12::bigint AND published_at "
      "IS NOT NULL");
#undef FLOWIE_CLUSTER_FACT_SQL
  return TURBO_OK;
}

void flowie_cluster_pgsql_fact_store_destroy(flowie_cluster_pgsql_fact_store_t *store) {
  if (!store) return;
  if (store->connection) PQfinish(store->connection);
  tstr_freep(&store->conninfo);
  tstr_freep(&store->schema_name);
  tstr_freep(&store->cluster_id);
  tstr_freep(&store->listener_id);
  tstr_freep(&store->node_id);
  tstr_freep(&store->advertised_endpoint);
  tstr_freep(&store->ownership_sql);
  tstr_freep(&store->capacity_lock_sql);
  tstr_freep(&store->receipt_select_sql);
  tstr_freep(&store->dedupe_select_sql);
  tstr_freep(&store->shared_selection_select_sql);
  tstr_freep(&store->dedupe_ack_count_sql);
  tstr_freep(&store->revision_select_sql);
  tstr_freep(&store->capacity_select_sql);
  tstr_freep(&store->receipt_insert_sql);
  tstr_freep(&store->dedupe_insert_sql);
  tstr_freep(&store->shared_selection_insert_sql);
  tstr_freep(&store->fact_insert_sql);
  tstr_freep(&store->fact_update_sql);
  tstr_freep(&store->fact_delete_sql);
  tstr_freep(&store->fact_scan_sql);
  tstr_freep(&store->fact_get_sql);
  tstr_freep(&store->outbox_insert_sql);
  tstr_freep(&store->outbox_next_sql);
  tstr_freep(&store->outbox_settle_sql);
  tstr_freep(&store->outbox_settle_confirm_sql);
  crypto_wipe(store->boot_id, sizeof(store->boot_id));
  free(store);
}

int flowie_cluster_pgsql_fact_store_open(const flowie_cluster_pgsql_fact_config_t *config,
                                         flowie_cluster_pgsql_fact_store_t **out) {
  const flowie_cluster_pgsql_config_t *coordinator;
  flowie_cluster_pgsql_fact_store_t *store;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_pgsql_fact_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  coordinator = config->coordinator;
  store = (flowie_cluster_pgsql_fact_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->conninfo = tstr_dup(coordinator->conninfo);
  store->schema_name = tstr_dup(coordinator->schema_name);
  store->cluster_id = tstr_dup(coordinator->cluster_id);
  store->listener_id = tstr_dup(coordinator->listener_id);
  store->node_id = tstr_dup(coordinator->node_id);
  store->advertised_endpoint = tstr_dup(coordinator->advertised_endpoint);
  memcpy(store->boot_id, coordinator->boot_id, sizeof(store->boot_id));
  store->hash_version = coordinator->hash_version;
  store->shard_count = coordinator->shard_count;
  store->lease_ttl_ms = coordinator->lease_ttl_ms;
  store->renew_interval_ms = coordinator->renew_interval_ms;
  store->retry_interval_ms = coordinator->retry_interval_ms;
  store->worst_case_db_latency_ms = coordinator->worst_case_db_latency_ms;
  store->safety_margin_ms = coordinator->safety_margin_ms;
  store->max_key_size = config->max_key_size;
  store->max_value_size = config->max_value_size;
  store->max_batch_size = config->max_batch_size;
  store->max_fact_records = config->max_fact_records;
  store->max_receipts = config->max_receipts;
  store->max_dedupe_records = config->max_dedupe_records;
  store->max_outbox_records = config->max_outbox_records;
  store->max_outbox_bytes = config->max_outbox_bytes;
  store->max_event_payload_size = config->max_event_payload_size;
  if (!store->conninfo || !store->schema_name || !store->cluster_id || !store->listener_id ||
      !store->node_id || !store->advertised_endpoint) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  store->connection =
      flowie_cluster_pgsql_fact_connect(store->conninfo, store->worst_case_db_latency_ms);
  if (!store->connection || PQstatus(store->connection) != CONNECTION_OK) {
    rc = TURBO_EIO;
    goto fail;
  }
  rc = flowie_cluster_pgsql_fact_exec(
      store->connection, "SELECT pg_catalog.set_config('search_path','',false)", PGRES_TUPLES_OK);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_set_timeouts(store->connection, store->worst_case_db_latency_ms);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_schema_prepare(store, coordinator->create_schema);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_fact_sql_prepare(store);
  if (rc != TURBO_OK) goto fail;
  *out = store;
  return TURBO_OK;

fail:
  flowie_cluster_pgsql_fact_store_destroy(store);
  return rc;
}

int flowie_cluster_pgsql_fact_store_reopen(flowie_cluster_pgsql_fact_store_t **store) {
  flowie_cluster_pgsql_config_t coordinator = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  flowie_cluster_pgsql_fact_store_t *replacement = NULL;
  flowie_cluster_pgsql_fact_store_t *current;
  int rc;
  if (!store || !(current = *store)) return TURBO_EINVAL;
  flowie_cluster_pgsql_fact_config_view(current, &coordinator, &config);
  coordinator.create_schema = 0;
  rc = flowie_cluster_pgsql_fact_store_open(&config, &replacement);
  if (rc != TURBO_OK) return rc;
  *store = replacement;
  flowie_cluster_pgsql_fact_store_destroy(current);
  return TURBO_OK;
}

static int flowie_cluster_pgsql_fact_text_u64(char *buffer, size_t capacity, uint64_t value) {
  int written = snprintf(buffer, capacity, "%llu", (unsigned long long)value);
  return written <= 0 || (size_t)written >= capacity ? TURBO_ERANGE : TURBO_OK;
}

static void flowie_cluster_pgsql_fact_config_view(flowie_cluster_pgsql_fact_store_t *store,
                                                  flowie_cluster_pgsql_config_t *coordinator,
                                                  flowie_cluster_pgsql_fact_config_t *config) {
  *coordinator = (flowie_cluster_pgsql_config_t)FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  coordinator->conninfo = store->conninfo;
  coordinator->schema_name = store->schema_name;
  coordinator->cluster_id = store->cluster_id;
  coordinator->listener_id = store->listener_id;
  coordinator->node_id = store->node_id;
  coordinator->advertised_endpoint = store->advertised_endpoint;
  memcpy(coordinator->boot_id, store->boot_id, sizeof(coordinator->boot_id));
  coordinator->shard_count = store->shard_count;
  coordinator->hash_version = store->hash_version;
  coordinator->lease_ttl_ms = store->lease_ttl_ms;
  coordinator->renew_interval_ms = store->renew_interval_ms;
  coordinator->retry_interval_ms = store->retry_interval_ms;
  coordinator->worst_case_db_latency_ms = store->worst_case_db_latency_ms;
  coordinator->safety_margin_ms = store->safety_margin_ms;
  *config = (flowie_cluster_pgsql_fact_config_t)FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  config->coordinator = coordinator;
  config->max_key_size = store->max_key_size;
  config->max_value_size = store->max_value_size;
  config->max_batch_size = store->max_batch_size;
  config->max_fact_records = store->max_fact_records;
  config->max_receipts = store->max_receipts;
  config->max_dedupe_records = store->max_dedupe_records;
  config->max_outbox_records = store->max_outbox_records;
  config->max_outbox_bytes = store->max_outbox_bytes;
  config->max_event_payload_size = store->max_event_payload_size;
}

int flowie_cluster_pgsql_fact_scan(flowie_cluster_pgsql_fact_store_t *store,
                                   const flowie_cluster_pgsql_fact_scan_t *scan,
                                   turbo_flow_record_visit_fn visit, void *visit_ctx) {
  static const Oid types[10] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                                FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                                FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                                FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                                FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  flowie_cluster_pgsql_config_t coordinator = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  char shard[16];
  char epoch[32];
  char kind[8];
  char limit[32];
  char max_key_size[32];
  char max_value_size[32];
  const char *values[10];
  int lengths[10] = {0, 0, 0, 0, FLOWIE_CLUSTER_BOOT_ID_SIZE, 0, 0, 0, 0, 0};
  int formats[10] = {0, 0, 0, 0, 1, 0, 0, 0, 0, 0};
  PGresult *result = NULL;
  int rows;
  int rc;
  if (!store || !visit) return TURBO_EINVAL;
  flowie_cluster_pgsql_fact_config_view(store, &coordinator, &config);
  rc = flowie_cluster_pgsql_fact_scan_validate(&config, scan);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), scan->owner.shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(epoch, sizeof(epoch), scan->owner.owner_epoch);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_fact_text_u64(kind, sizeof(kind), scan->key_kind);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(limit, sizeof(limit), scan->max_records + 1u);
  if (rc == TURBO_OK)
    rc =
        flowie_cluster_pgsql_fact_text_u64(max_key_size, sizeof(max_key_size), store->max_key_size);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(max_value_size, sizeof(max_value_size),
                                            store->max_value_size);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = shard;
  values[3] = scan->owner.node_id;
  values[4] = (const char *)scan->owner.boot_id;
  values[5] = epoch;
  values[6] = kind;
  values[7] = limit;
  values[8] = max_key_size;
  values[9] = max_value_size;
  result =
      PQexecParams(store->connection, store->fact_scan_sql, 10, types, values, lengths, formats, 1);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc != TURBO_OK) goto done;
  rows = PQntuples(result);
  if (PQnfields(result) != 5 || PQftype(result, 0) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 1) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 2) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 3) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 4) != FLOWIE_CLUSTER_PGSQL_OID_TEXT) {
    rc = TURBO_EPROTO;
    goto done;
  }
  if (rows == 0) {
    rc = TURBO_EBUSY;
    goto done;
  }
  if (rows == 1 && PQgetisnull(result, 0, 0) && PQgetisnull(result, 0, 1) &&
      PQgetisnull(result, 0, 2) && PQgetisnull(result, 0, 3) && PQgetisnull(result, 0, 4))
    goto done;
  if ((size_t)rows > scan->max_records) {
    rc = TURBO_ENOSPC;
    goto done;
  }
  for (int row = 0; row < rows; ++row) {
    turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
    uint32_t record_shard = UINT32_MAX;
    size_t declared_key_size = 0u;
    size_t declared_value_size = 0u;
    if (PQgetisnull(result, row, 0) || PQgetisnull(result, row, 1) || PQgetisnull(result, row, 2) ||
        PQgetisnull(result, row, 3) || PQgetisnull(result, row, 4)) {
      rc = TURBO_EPROTO;
      break;
    }
    record.key = (const uint8_t *)PQgetvalue(result, row, 0);
    record.key_size = (size_t)PQgetlength(result, row, 0);
    record.value = (const uint8_t *)PQgetvalue(result, row, 2);
    record.value_size = (size_t)PQgetlength(result, row, 2);
    rc = flowie_cluster_pgsql_fact_parse_size(
        PQgetvalue(result, row, 3), (size_t)PQgetlength(result, row, 3), &declared_key_size);
    if (rc == TURBO_OK)
      rc = flowie_cluster_pgsql_fact_parse_size(
          PQgetvalue(result, row, 4), (size_t)PQgetlength(result, row, 4), &declared_value_size);
    if (rc != TURBO_OK || record.key_size == 0u || record.key_size > store->max_key_size ||
        record.value_size > store->max_value_size || declared_key_size != record.key_size ||
        declared_value_size != record.value_size) {
      rc = TURBO_EPROTO;
      break;
    }
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, row, 1),
                                             (size_t)PQgetlength(result, row, 1), &record.revision);
    if (rc != TURBO_OK || record.revision == 0u ||
        record.revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX) {
      rc = TURBO_EPROTO;
      break;
    }
    if (scan->key_kind == FLOWIE_CLUSTER_KEY_SESSION) {
      rc = flowie_cluster_shard_for_key(
          store->hash_version, scan->key_kind, (const uint8_t *)store->cluster_id,
          tstr_len(store->cluster_id), (const uint8_t *)store->listener_id,
          tstr_len(store->listener_id), record.key, record.key_size, store->shard_count,
          &record_shard);
      if (rc != TURBO_OK || record_shard != scan->owner.shard_id) {
        rc = TURBO_EPROTO;
        break;
      }
    }
    rc = visit(visit_ctx, &record);
    if (rc != TURBO_OK) break;
  }

done:
  if (result) PQclear(result);
  return rc;
}

void flowie_cluster_pgsql_fact_record_cleanup(flowie_cluster_pgsql_fact_record_t *record) {
  if (!record || record->size < sizeof(*record) ||
      record->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1)
    return;
  tstr_free(record->key);
  tstr_free(record->value);
  *record = (flowie_cluster_pgsql_fact_record_t)FLOWIE_CLUSTER_PGSQL_FACT_RECORD_INIT;
}

int flowie_cluster_pgsql_fact_get(flowie_cluster_pgsql_fact_store_t *store,
                                  const flowie_cluster_owner_token_t *current_owner,
                                  flowie_cluster_key_kind_t key_kind, const uint8_t *key,
                                  size_t key_size, flowie_cluster_pgsql_fact_record_t *out) {
  static const Oid types[8] = {
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_BYTEA};
  char shard[16];
  char epoch[32];
  char kind[8];
  const char *values[8];
  int lengths[8] = {0, 0, 0, 0, FLOWIE_CLUSTER_BOOT_ID_SIZE, 0, 0, 0};
  int formats[8] = {0, 0, 0, 0, 1, 0, 0, 1};
  PGresult *result = NULL;
  uint64_t parsed_shard = 0u;
  size_t declared_key_size = 0u;
  size_t declared_value_size = 0u;
  int rows;
  int rc;
  if (!store || !current_owner ||
      (key_kind != FLOWIE_CLUSTER_KEY_SESSION && key_kind != FLOWIE_CLUSTER_KEY_RETAINED) ||
      !key || key_size == 0u || key_size > store->max_key_size || key_size > INT_MAX || !out ||
      out->size < sizeof(*out) || out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || out->key ||
      out->value)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_owner_validate(store, current_owner);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), current_owner->shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(epoch, sizeof(epoch), current_owner->owner_epoch);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_fact_text_u64(kind, sizeof(kind), key_kind);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = shard;
  values[3] = current_owner->node_id;
  values[4] = (const char *)current_owner->boot_id;
  values[5] = epoch;
  values[6] = kind;
  values[7] = (const char *)key;
  lengths[7] = (int)key_size;
  result = PQexecParams(store->connection, store->fact_get_sql, 8, types, values, lengths, formats,
                        1);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc != TURBO_OK) goto done;
  rows = PQntuples(result);
  if (PQnfields(result) != 7 || rows < 0 || rows > 1 ||
      PQftype(result, 0) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 1) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 2) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 3) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 4) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 5) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 6) != FLOWIE_CLUSTER_PGSQL_OID_TEXT) {
    rc = TURBO_EPROTO;
    goto done;
  }
  if (rows == 0) {
    rc = TURBO_EBUSY;
    goto done;
  }
  if (PQgetisnull(result, 0, 0) && PQgetisnull(result, 0, 1) &&
      PQgetisnull(result, 0, 2) && PQgetisnull(result, 0, 3) &&
      PQgetisnull(result, 0, 4) && PQgetisnull(result, 0, 5) &&
      PQgetisnull(result, 0, 6)) {
    rc = TURBO_ENOENT;
    goto done;
  }
  for (int column = 0; column < 7; ++column) {
    if (PQgetisnull(result, 0, column)) {
      rc = TURBO_EPROTO;
      goto done;
    }
  }
  rc = flowie_cluster_pgsql_fact_parse_u64(
      PQgetvalue(result, 0, 0), (size_t)PQgetlength(result, 0, 0), &parsed_shard);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(
        PQgetvalue(result, 0, 1), (size_t)PQgetlength(result, 0, 1), &out->owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(
        PQgetvalue(result, 0, 2), (size_t)PQgetlength(result, 0, 2), &out->revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(
        PQgetvalue(result, 0, 5), (size_t)PQgetlength(result, 0, 5), &declared_key_size);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(
        PQgetvalue(result, 0, 6), (size_t)PQgetlength(result, 0, 6), &declared_value_size);
  if (rc != TURBO_OK || parsed_shard != current_owner->shard_id || out->owner_epoch == 0u ||
      out->revision == 0u || out->revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
      declared_key_size != (size_t)PQgetlength(result, 0, 3) ||
      declared_value_size != (size_t)PQgetlength(result, 0, 4) ||
      declared_key_size != key_size || declared_value_size > store->max_value_size ||
      memcmp(PQgetvalue(result, 0, 3), key, key_size) != 0) {
    rc = TURBO_EPROTO;
    goto done;
  }
  out->key = tstr_new_len(PQgetvalue(result, 0, 3), declared_key_size);
  out->value = tstr_new_len(PQgetvalue(result, 0, 4), declared_value_size);
  if (!out->key || !out->value) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  out->key_kind = key_kind;
  out->shard_id = (uint32_t)parsed_shard;
  rc = TURBO_OK;

done:
  if (result) PQclear(result);
  if (rc != TURBO_OK) flowie_cluster_pgsql_fact_record_cleanup(out);
  return rc;
}

static int flowie_cluster_pgsql_fact_receipt_check(
    flowie_cluster_pgsql_fact_store_t *store, const flowie_cluster_pgsql_fact_command_t *command,
    const uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE], int absent_status) {
  static const Oid types[3] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA};
  const char *values[3] = {store->cluster_id, store->listener_id,
                           (const char *)command->command_id};
  int lengths[3] = {0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE};
  int formats[3] = {0, 0, 1};
  PGresult *result = PQexecParams(store->connection, store->receipt_select_sql, 3, types, values,
                                  lengths, formats, 1);
  uint64_t shard = 0u;
  uint64_t epoch = 0u;
  uint64_t count = 0u;
  int rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = absent_status;
  if (rc == TURBO_OK &&
      (PQntuples(result) != 1 || PQnfields(result) != 4 || PQgetisnull(result, 0, 0) ||
       PQgetlength(result, 0, 0) != FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE ||
       PQgetisnull(result, 0, 1) || PQgetisnull(result, 0, 2) || PQgetisnull(result, 0, 3)))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 1),
                                             (size_t)PQgetlength(result, 0, 1), &shard);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 2),
                                             (size_t)PQgetlength(result, 0, 2), &epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 3),
                                             (size_t)PQgetlength(result, 0, 3), &count);
  if (rc == TURBO_OK &&
      (memcmp(PQgetvalue(result, 0, 0), digest, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE) != 0 ||
       shard != command->owner.shard_id || epoch != command->owner.owner_epoch ||
       count != command->mutation_count))
    rc = TURBO_EBUSY;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_dedupe_check(
    flowie_cluster_pgsql_fact_store_t *store, const flowie_cluster_pgsql_fact_command_t *command,
    int absent_status) {
  static const Oid types[6] = {
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  const flowie_cluster_pgsql_event_dedupe_t *dedupe;
  char target_shard[16], target_session[32], event_index[16];
  const char *values[6];
  int lengths[6] = {0, 0, 0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE, 0};
  int formats[6] = {0, 0, 0, 0, 1, 0};
  PGresult *result = NULL;
  uint64_t source_shard = 0u;
  uint64_t source_epoch = 0u;
  uint64_t source_revision = 0u;
  int rc;
  if (!store || !command || !(dedupe = command->dedupe)) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_text_u64(target_shard, sizeof(target_shard),
                                          command->owner.shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(target_session, sizeof(target_session),
                                            dedupe->target_session_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(event_index, sizeof(event_index),
                                            dedupe->event_index);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = target_shard;
  values[3] = target_session;
  values[4] = (const char *)dedupe->source_command_id;
  values[5] = event_index;
  result = PQexecParams(store->connection, store->dedupe_select_sql, 6, types, values, lengths,
                        formats, 1);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = absent_status;
  if (rc == TURBO_OK &&
      (PQntuples(result) != 1 || PQnfields(result) != 4 || PQgetisnull(result, 0, 0) ||
       PQgetisnull(result, 0, 1) || PQgetisnull(result, 0, 2) || PQgetisnull(result, 0, 3) ||
       PQgetlength(result, 0, 3) != FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 0),
                                             (size_t)PQgetlength(result, 0, 0), &source_shard);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 1),
                                             (size_t)PQgetlength(result, 0, 1), &source_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 2),
                                             (size_t)PQgetlength(result, 0, 2), &source_revision);
  if (rc == TURBO_OK &&
      (source_shard != dedupe->source_shard_id || source_epoch != dedupe->source_owner_epoch ||
       source_revision != dedupe->source_fact_revision ||
       memcmp(PQgetvalue(result, 0, 3), dedupe->event_digest,
              FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE) != 0))
    rc = TURBO_EBUSY;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_shared_selection_check(
    flowie_cluster_pgsql_fact_store_t *store, const flowie_cluster_pgsql_fact_command_t *command,
    int absent_status) {
  static const Oid types[5] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA};
  const flowie_cluster_pgsql_event_dedupe_t *dedupe;
  char event_index[16];
  const char *values[5];
  int lengths[5] = {0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE, 0, 0};
  int formats[5] = {0, 0, 1, 0, 1};
  PGresult *result = NULL;
  uint64_t source_shard = 0u, source_epoch = 0u, source_revision = 0u;
  uint64_t target_shard = 0u, target_session = 0u;
  int rc;
  if (!store || !command || !(dedupe = command->dedupe) || !dedupe->shared_filter ||
      dedupe->shared_filter_size == 0u)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_text_u64(event_index, sizeof(event_index), dedupe->event_index);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = (const char *)dedupe->source_command_id;
  values[3] = event_index;
  values[4] = (const char *)dedupe->shared_filter;
  lengths[4] = (int)dedupe->shared_filter_size;
  result = PQexecParams(store->connection, store->shared_selection_select_sql, 5, types, values,
                        lengths, formats, 1);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = absent_status;
  if (rc == TURBO_OK &&
      (PQntuples(result) != 1 || PQnfields(result) != 6 || PQgetisnull(result, 0, 0) ||
       PQgetisnull(result, 0, 1) || PQgetisnull(result, 0, 2) || PQgetisnull(result, 0, 3) ||
       PQgetisnull(result, 0, 4) || PQgetisnull(result, 0, 5) ||
       PQgetlength(result, 0, 3) != FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 0),
                                             (size_t)PQgetlength(result, 0, 0), &source_shard);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 1),
                                             (size_t)PQgetlength(result, 0, 1), &source_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 2),
                                             (size_t)PQgetlength(result, 0, 2), &source_revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 4),
                                             (size_t)PQgetlength(result, 0, 4), &target_shard);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 5),
                                             (size_t)PQgetlength(result, 0, 5), &target_session);
  if (rc == TURBO_OK &&
      (source_shard != dedupe->source_shard_id || source_epoch != dedupe->source_owner_epoch ||
       source_revision != dedupe->source_fact_revision ||
       memcmp(PQgetvalue(result, 0, 3), dedupe->event_digest,
              FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE) != 0))
    rc = TURBO_EBUSY;
  if (rc == TURBO_OK &&
      (target_shard != command->owner.shard_id || target_session != dedupe->target_session_id))
    rc = TURBO_EALREADY;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_lock_owner(flowie_cluster_pgsql_fact_store_t *store,
                                                const flowie_cluster_owner_token_t *owner) {
  static const Oid types[6] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  char shard[16];
  char epoch[32];
  const char *values[6] = {store->cluster_id, store->listener_id,           shard,
                           owner->node_id,    (const char *)owner->boot_id, epoch};
  int lengths[6] = {0, 0, 0, 0, FLOWIE_CLUSTER_BOOT_ID_SIZE, 0};
  int formats[6] = {0, 0, 0, 0, 1, 0};
  PGresult *result;
  int rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), owner->shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(epoch, sizeof(epoch), owner->owner_epoch);
  if (rc != TURBO_OK) return rc;
  result =
      PQexecParams(store->connection, store->ownership_sql, 6, types, values, lengths, formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) != 1)
    rc = PQntuples(result) == 0 ? TURBO_EBUSY : TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int
flowie_cluster_pgsql_fact_owner_validate(const flowie_cluster_pgsql_fact_store_t *store,
                                         const flowie_cluster_owner_token_t *current_owner) {
  flowie_cluster_owner_token_t expected = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  int rc;
  if (!store || !current_owner || current_owner->shard_id >= store->shard_count)
    return TURBO_EINVAL;
  rc = flowie_cluster_owner_token_init(&expected, current_owner->shard_id,
                                       current_owner->owner_epoch, store->node_id,
                                       tstr_len(store->node_id), store->boot_id);
  if (rc != TURBO_OK) return rc;
  return flowie_cluster_owner_token_require(&expected, current_owner);
}

void flowie_cluster_pgsql_outbox_event_cleanup(flowie_cluster_pgsql_outbox_event_t *event) {
  if (!event) return;
  tstr_free(event->record_key);
  tstr_free(event->payload);
  *event = (flowie_cluster_pgsql_outbox_event_t)FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
}

static int
flowie_cluster_pgsql_outbox_event_validate(const flowie_cluster_pgsql_fact_store_t *store,
                                           const flowie_cluster_pgsql_outbox_event_t *event) {
  if (!store || !event || event->size < sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      !flowie_cluster_pgsql_fact_nonzero_bytes(event->command_id, sizeof(event->command_id)) ||
      event->event_index > INT_MAX || event->shard_id >= store->shard_count ||
      event->event_owner_epoch == 0u || event->event_type == 0u ||
      (event->record_kind != FLOWIE_CLUSTER_KEY_SESSION &&
       event->record_kind != FLOWIE_CLUSTER_KEY_RETAINED) ||
      !event->record_key || tstr_len(event->record_key) == 0u ||
      tstr_len(event->record_key) > store->max_key_size || !event->payload ||
      tstr_len(event->payload) > store->max_event_payload_size ||
      event->created_at_epoch_seconds == 0u || event->attempt_count == 0u ||
      event->attempt_count > INT64_MAX)
    return TURBO_EINVAL;
  return TURBO_OK;
}

int flowie_cluster_pgsql_outbox_next(flowie_cluster_pgsql_fact_store_t *store,
                                     const flowie_cluster_owner_token_t *current_owner,
                                     uint64_t event_type,
                                     flowie_cluster_pgsql_outbox_event_t *out) {
  static const Oid types[4] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  flowie_cluster_pgsql_outbox_event_t event = FLOWIE_CLUSTER_PGSQL_OUTBOX_EVENT_INIT;
  char shard[16];
  char type[32];
  const char *values[4] = {store ? store->cluster_id : NULL, store ? store->listener_id : NULL,
                           shard, type};
  PGresult *result = NULL;
  uint64_t parsed_event_index = 0u;
  uint64_t parsed_shard_id = 0u;
  uint64_t parsed_record_kind = 0u;
  uint64_t previous_attempt = 0u;
  size_t declared_payload_size = 0u;
  int rc;
  if (!store || !current_owner || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || out->record_key || out->payload ||
      event_type == 0u || event_type > INT64_MAX)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_owner_validate(store, current_owner);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), current_owner->shard_id);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_fact_text_u64(type, sizeof(type), event_type);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_exec(store->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_lock_owner(store, current_owner);
  if (rc != TURBO_OK) return flowie_cluster_pgsql_fact_rollback(store, rc);
  result = PQexecParams(store->connection, store->outbox_next_sql, 4, types, values, NULL, NULL, 1);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc != TURBO_OK) goto fail;
  if (PQnfields(result) != 13 || PQftype(result, 0) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 1) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 2) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 3) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 4) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 5) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 6) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 7) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 8) != FLOWIE_CLUSTER_PGSQL_OID_BYTEA ||
      PQftype(result, 9) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 10) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 11) != FLOWIE_CLUSTER_PGSQL_OID_TEXT ||
      PQftype(result, 12) != FLOWIE_CLUSTER_PGSQL_OID_TEXT || PQntuples(result) > 1) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  if (PQntuples(result) == 0) {
    PQclear(result);
    result = NULL;
    rc = flowie_cluster_pgsql_fact_exec(store->connection, "COMMIT", PGRES_COMMAND_OK);
    return rc == TURBO_OK ? TURBO_ENOENT : rc;
  }
  for (int column = 0; column < 13; ++column)
    if (PQgetisnull(result, 0, column)) {
      rc = TURBO_EPROTO;
      goto fail;
    }
  if (PQgetlength(result, 0, 0) != FLOWIE_CLUSTER_COMMAND_ID_SIZE ||
      !flowie_cluster_pgsql_fact_nonzero_bytes((const uint8_t *)PQgetvalue(result, 0, 0),
                                               FLOWIE_CLUSTER_COMMAND_ID_SIZE)) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  memcpy(event.command_id, PQgetvalue(result, 0, 0), sizeof(event.command_id));
#define FLOWIE_CLUSTER_OUTBOX_PARSE(column, target)                                                \
  do {                                                                                             \
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, column),                        \
                                             (size_t)PQgetlength(result, 0, column), &(target));   \
    if (rc != TURBO_OK) goto fail;                                                                 \
  } while (0)
  FLOWIE_CLUSTER_OUTBOX_PARSE(1, parsed_event_index);
  FLOWIE_CLUSTER_OUTBOX_PARSE(2, parsed_shard_id);
  FLOWIE_CLUSTER_OUTBOX_PARSE(3, event.event_owner_epoch);
  FLOWIE_CLUSTER_OUTBOX_PARSE(4, event.event_type);
  FLOWIE_CLUSTER_OUTBOX_PARSE(5, parsed_record_kind);
  FLOWIE_CLUSTER_OUTBOX_PARSE(7, event.fact_revision);
  FLOWIE_CLUSTER_OUTBOX_PARSE(10, event.created_at_epoch_seconds);
  FLOWIE_CLUSTER_OUTBOX_PARSE(11, previous_attempt);
  FLOWIE_CLUSTER_OUTBOX_PARSE(12, event.attempt_count);
#undef FLOWIE_CLUSTER_OUTBOX_PARSE
  rc = flowie_cluster_pgsql_fact_parse_size(
      PQgetvalue(result, 0, 9), (size_t)PQgetlength(result, 0, 9), &declared_payload_size);
  if (rc != TURBO_OK || previous_attempt >= INT64_MAX || parsed_event_index > INT_MAX ||
      parsed_shard_id > UINT32_MAX ||
      (parsed_record_kind != FLOWIE_CLUSTER_KEY_SESSION &&
       parsed_record_kind != FLOWIE_CLUSTER_KEY_RETAINED) ||
      event.event_owner_epoch == 0u || event.event_owner_epoch > INT64_MAX ||
      event.event_type == 0u || event.event_type > INT64_MAX || event.fact_revision > INT64_MAX ||
      event.created_at_epoch_seconds == 0u ||
      event.created_at_epoch_seconds > INT64_MAX ||
      event.attempt_count > INT64_MAX || event.attempt_count != previous_attempt + 1u ||
      parsed_shard_id != current_owner->shard_id || event.event_type != event_type ||
      (size_t)PQgetlength(result, 0, 6) == 0u ||
      (size_t)PQgetlength(result, 0, 6) > store->max_key_size ||
      (size_t)PQgetlength(result, 0, 8) > store->max_event_payload_size ||
      declared_payload_size != (size_t)PQgetlength(result, 0, 8)) {
    rc = previous_attempt >= INT64_MAX ? TURBO_ERANGE : TURBO_EPROTO;
    goto fail;
  }
  event.event_index = (uint32_t)parsed_event_index;
  event.shard_id = (uint32_t)parsed_shard_id;
  event.record_kind = (flowie_cluster_key_kind_t)parsed_record_kind;
  event.record_key = tstr_new_len(PQgetvalue(result, 0, 6), (size_t)PQgetlength(result, 0, 6));
  event.payload = tstr_new_len(PQgetvalue(result, 0, 8), (size_t)PQgetlength(result, 0, 8));
  if (!event.record_key || !event.payload) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  PQclear(result);
  result = NULL;
  rc = flowie_cluster_pgsql_fact_exec(store->connection, "COMMIT", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) goto cleanup;
  *out = event;
  return TURBO_OK;

fail:
  if (result) PQclear(result);
  rc = flowie_cluster_pgsql_fact_rollback(store, rc);
cleanup:
  flowie_cluster_pgsql_outbox_event_cleanup(&event);
  return rc;
}

static int
flowie_cluster_pgsql_outbox_settle_query(flowie_cluster_pgsql_fact_store_t *store,
                                         const flowie_cluster_pgsql_outbox_event_t *event,
                                         const char *sql) {
  static const Oid types[12] = {
      FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_BYTEA,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_BYTEA,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  char index[16], shard[16], epoch[32], type[32], kind[8], revision[32], payload_size[32];
  const char *values[12] = {store->cluster_id,
                            store->listener_id,
                            (const char *)event->command_id,
                            index,
                            shard,
                            epoch,
                            type,
                            kind,
                            event->record_key,
                            revision,
                            event->payload,
                            payload_size};
  int lengths[12] = {0,
                     0,
                     FLOWIE_CLUSTER_COMMAND_ID_SIZE,
                     0,
                     0,
                     0,
                     0,
                     0,
                     (int)tstr_len(event->record_key),
                     0,
                     (int)tstr_len(event->payload),
                     0};
  int formats[12] = {0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0};
  PGresult *result;
  int rc = flowie_cluster_pgsql_fact_text_u64(index, sizeof(index), event->event_index);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), event->shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(epoch, sizeof(epoch), event->event_owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(type, sizeof(type), event->event_type);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(kind, sizeof(kind), event->record_kind);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(revision, sizeof(revision), event->fact_revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(payload_size, sizeof(payload_size),
                                            tstr_len(event->payload));
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(store->connection, sql, 12, types, values, lengths, formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQnfields(result) != 1 || PQntuples(result) > 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = PQntuples(result) == 1 ? TURBO_OK : TURBO_ENOENT;
  if (result) PQclear(result);
  return rc;
}

int flowie_cluster_pgsql_outbox_settle(flowie_cluster_pgsql_fact_store_t *store,
                                       const flowie_cluster_owner_token_t *current_owner,
                                       const flowie_cluster_pgsql_outbox_event_t *event) {
  int rc;
  if (!store || !current_owner) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_owner_validate(store, current_owner);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_outbox_event_validate(store, event);
  if (rc != TURBO_OK) return rc;
  if (event->shard_id != current_owner->shard_id) return TURBO_EBUSY;
  rc = flowie_cluster_pgsql_fact_exec(store->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_lock_owner(store, current_owner);
  if (rc != TURBO_OK) return flowie_cluster_pgsql_fact_rollback(store, rc);
  rc = flowie_cluster_pgsql_outbox_settle_query(store, event, store->outbox_settle_sql);
  if (rc == TURBO_OK) {
    rc = flowie_cluster_pgsql_fact_exec(store->connection, "COMMIT", PGRES_COMMAND_OK);
    return rc;
  }
  if (rc == TURBO_ENOENT) {
    rc = flowie_cluster_pgsql_outbox_settle_query(store, event, store->outbox_settle_confirm_sql);
    if (rc == TURBO_OK) rc = TURBO_EALREADY;
    else if (rc == TURBO_ENOENT) rc = TURBO_EBUSY;
  }
  return flowie_cluster_pgsql_fact_rollback(store, rc);
}

static int flowie_cluster_pgsql_fact_capacity_lock(flowie_cluster_pgsql_fact_store_t *store) {
  const char *values[2] = {store->cluster_id, store->listener_id};
  PGresult *result =
      PQexecParams(store->connection, store->capacity_lock_sql, 2, NULL, values, NULL, NULL, 0);
  int rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int
flowie_cluster_pgsql_fact_current_revision(flowie_cluster_pgsql_fact_store_t *store,
                                           const flowie_cluster_pgsql_fact_mutation_t *mutation,
                                           uint64_t *revision) {
  static const Oid types[4] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_INT2, FLOWIE_CLUSTER_PGSQL_OID_BYTEA};
  char kind[8];
  const char *values[4] = {store->cluster_id, store->listener_id, kind,
                           (const char *)mutation->record.key};
  int lengths[4] = {0, 0, 0, (int)mutation->record.key_size};
  int formats[4] = {0, 0, 0, 1};
  PGresult *result;
  int rc = flowie_cluster_pgsql_fact_text_u64(kind, sizeof(kind), mutation->key_kind);
  *revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(store->connection, store->revision_select_sql, 4, types, values, lengths,
                        formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) > 1) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && PQntuples(result) == 1)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 0),
                                             (size_t)PQgetlength(result, 0, 0), revision);
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_capacity(flowie_cluster_pgsql_fact_store_t *store,
                                              ptrdiff_t fact_delta, size_t added_receipts,
                                              size_t added_records,
                                              size_t added_dedupe_records,
                                              uint64_t added_bytes) {
  const char *values[2] = {store->cluster_id, store->listener_id};
  PGresult *result =
      PQexecParams(store->connection, store->capacity_select_sql, 2, NULL, values, NULL, NULL, 0);
  size_t facts = 0u;
  size_t receipts = 0u;
  size_t dedupe_records = 0u;
  size_t outbox_records = 0u;
  uint64_t outbox_bytes = 0u;
  int rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 5)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(PQgetvalue(result, 0, 0),
                                              (size_t)PQgetlength(result, 0, 0), &facts);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(PQgetvalue(result, 0, 1),
                                              (size_t)PQgetlength(result, 0, 1), &receipts);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(PQgetvalue(result, 0, 2),
                                              (size_t)PQgetlength(result, 0, 2), &dedupe_records);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(PQgetvalue(result, 0, 3),
                                              (size_t)PQgetlength(result, 0, 3), &outbox_records);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_u64(PQgetvalue(result, 0, 4),
                                             (size_t)PQgetlength(result, 0, 4), &outbox_bytes);
  if (result) PQclear(result);
  if (rc != TURBO_OK) return rc;
  if (facts > store->max_fact_records || receipts > store->max_receipts ||
      added_receipts > store->max_receipts - receipts ||
      dedupe_records > store->max_dedupe_records ||
      added_dedupe_records > store->max_dedupe_records - dedupe_records ||
      outbox_records > store->max_outbox_records ||
      added_records > store->max_outbox_records - outbox_records ||
      outbox_bytes > store->max_outbox_bytes ||
      added_bytes > store->max_outbox_bytes - outbox_bytes)
    return TURBO_ENOSPC;
  if (fact_delta < 0 && (size_t)(-fact_delta) > facts) return TURBO_EPROTO;
  if (fact_delta > 0 && (size_t)fact_delta > store->max_fact_records - facts) return TURBO_ENOSPC;
  return TURBO_OK;
}

static int
flowie_cluster_pgsql_fact_insert_receipt(flowie_cluster_pgsql_fact_store_t *store,
                                         const flowie_cluster_pgsql_fact_command_t *command,
                                         const uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE]) {
  static const Oid types[7] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_BYTEA,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  char shard[16], epoch[32], count[32];
  const char *values[7] = {store->cluster_id,
                           store->listener_id,
                           (const char *)command->command_id,
                           (const char *)digest,
                           shard,
                           epoch,
                           count};
  int lengths[7] = {0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE, 0,
                    0, 0};
  int formats[7] = {0, 0, 1, 1, 0, 0, 0};
  PGresult *result;
  int rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), command->owner.shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(epoch, sizeof(epoch), command->owner.owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(count, sizeof(count), command->mutation_count);
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(store->connection, store->receipt_insert_sql, 7, types, values, lengths,
                        formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK && strcmp(PQcmdTuples(result), "1") != 0) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_insert_dedupe(
    flowie_cluster_pgsql_fact_store_t *store,
    const flowie_cluster_pgsql_fact_command_t *command) {
  static const Oid types[10] = {
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_BYTEA};
  const flowie_cluster_pgsql_event_dedupe_t *dedupe;
  char target_shard[16], target_session[32], event_index[16];
  char source_shard[16], source_epoch[32], source_revision[32];
  const char *values[10];
  int lengths[10] = {0, 0, 0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE, 0, 0, 0, 0,
                     FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE};
  int formats[10] = {0, 0, 0, 0, 1, 0, 0, 0, 0, 1};
  PGresult *result;
  int rc;
  if (!store || !command || !(dedupe = command->dedupe)) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_text_u64(target_shard, sizeof(target_shard),
                                          command->owner.shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(target_session, sizeof(target_session),
                                            dedupe->target_session_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(event_index, sizeof(event_index),
                                            dedupe->event_index);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_shard, sizeof(source_shard),
                                            dedupe->source_shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_epoch, sizeof(source_epoch),
                                            dedupe->source_owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_revision, sizeof(source_revision),
                                            dedupe->source_fact_revision);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = target_shard;
  values[3] = target_session;
  values[4] = (const char *)dedupe->source_command_id;
  values[5] = event_index;
  values[6] = source_shard;
  values[7] = source_epoch;
  values[8] = source_revision;
  values[9] = (const char *)dedupe->event_digest;
  result = PQexecParams(store->connection, store->dedupe_insert_sql, 10, types, values, lengths,
                        formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK && strcmp(PQcmdTuples(result), "1") != 0) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_insert_shared_selection(
    flowie_cluster_pgsql_fact_store_t *store,
    const flowie_cluster_pgsql_fact_command_t *command) {
  static const Oid types[11] = {
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  const flowie_cluster_pgsql_event_dedupe_t *dedupe;
  char event_index[16], source_shard[16], source_epoch[32], source_revision[32];
  char target_shard[16], target_session[32];
  const char *values[11];
  int lengths[11] = {0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE, 0, 0, 0, 0, 0,
                     FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE, 0, 0};
  int formats[11] = {0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0};
  PGresult *result;
  int rc;
  if (!store || !command || !(dedupe = command->dedupe) || !dedupe->shared_filter ||
      dedupe->shared_filter_size == 0u)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_text_u64(event_index, sizeof(event_index), dedupe->event_index);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_shard, sizeof(source_shard),
                                            dedupe->source_shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_epoch, sizeof(source_epoch),
                                            dedupe->source_owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_revision, sizeof(source_revision),
                                            dedupe->source_fact_revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(target_shard, sizeof(target_shard),
                                            command->owner.shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(target_session, sizeof(target_session),
                                            dedupe->target_session_id);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = (const char *)dedupe->source_command_id;
  values[3] = event_index;
  values[4] = (const char *)dedupe->shared_filter;
  values[5] = source_shard;
  values[6] = source_epoch;
  values[7] = source_revision;
  values[8] = (const char *)dedupe->event_digest;
  values[9] = target_shard;
  values[10] = target_session;
  lengths[4] = (int)dedupe->shared_filter_size;
  result = PQexecParams(store->connection, store->shared_selection_insert_sql, 11, types, values,
                        lengths, formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK && strcmp(PQcmdTuples(result), "1") != 0) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_fact_apply(flowie_cluster_pgsql_fact_store_t *store,
                                           const flowie_cluster_pgsql_fact_command_t *command,
                                           const flowie_cluster_pgsql_fact_mutation_t *mutation,
                                           size_t event_index) {
  static const char empty = '\0';
  char kind[8], shard[16], next_revision[32], epoch[32], expected_revision[32];
  char index_text[32], event_type[32], payload_size[32];
  const turbo_flow_record_mutation_t *record = &mutation->record;
  const char *insert_values[8] = {store->cluster_id,
                                  store->listener_id,
                                  kind,
                                  (const char *)record->key,
                                  shard,
                                  next_revision,
                                  epoch,
                                  record->value_size ? (const char *)record->value : &empty};
  int insert_lengths[8] = {0, 0, 0, (int)record->key_size, 0, 0, 0, (int)record->value_size};
  int insert_formats[8] = {0, 0, 0, 1, 0, 0, 0, 1};
  const char *update_values[8] = {store->cluster_id,
                                  store->listener_id,
                                  kind,
                                  (const char *)record->key,
                                  next_revision,
                                  epoch,
                                  record->value_size ? (const char *)record->value : &empty,
                                  expected_revision};
  int update_lengths[8] = {0, 0, 0, (int)record->key_size, 0, 0, (int)record->value_size, 0};
  int update_formats[8] = {0, 0, 0, 1, 0, 0, 1, 0};
  const char *delete_values[5] = {store->cluster_id, store->listener_id, kind,
                                  (const char *)record->key, expected_revision};
  int delete_lengths[5] = {0, 0, 0, (int)record->key_size, 0};
  int delete_formats[5] = {0, 0, 0, 1, 0};
  const char *outbox_values[12] = {
      store->cluster_id,
      store->listener_id,
      (const char *)command->command_id,
      index_text,
      shard,
      epoch,
      event_type,
      kind,
      (const char *)record->key,
      next_revision,
      mutation->event_payload_size ? (const char *)mutation->event_payload : &empty,
      payload_size};
  int outbox_lengths[12] = {0,
                            0,
                            FLOWIE_CLUSTER_COMMAND_ID_SIZE,
                            0,
                            0,
                            0,
                            0,
                            0,
                            (int)record->key_size,
                            0,
                            (int)mutation->event_payload_size,
                            0};
  int outbox_formats[12] = {0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0};
  PGresult *result = NULL;
  int rc = flowie_cluster_pgsql_fact_text_u64(kind, sizeof(kind), mutation->key_kind);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(shard, sizeof(shard), command->owner.shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(next_revision, sizeof(next_revision),
                                            record->next_revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(epoch, sizeof(epoch), command->owner.owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(expected_revision, sizeof(expected_revision),
                                            record->expected_revision);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(index_text, sizeof(index_text), event_index);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(event_type, sizeof(event_type), mutation->event_type);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(payload_size, sizeof(payload_size),
                                            mutation->event_payload_size);
  if (rc != TURBO_OK) return rc;
  if (mutation->write_kind == FLOWIE_CLUSTER_PGSQL_EVENT_ONLY)
    rc = TURBO_OK;
  else if (record->kind == TURBO_FLOW_RECORD_DELETE)
    result = PQexecParams(store->connection, store->fact_delete_sql, 5, NULL, delete_values,
                          delete_lengths, delete_formats, 0);
  else if (record->expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT)
    result = PQexecParams(store->connection, store->fact_insert_sql, 8, NULL, insert_values,
                          insert_lengths, insert_formats, 0);
  else
    result = PQexecParams(store->connection, store->fact_update_sql, 8, NULL, update_values,
                          update_lengths, update_formats, 0);
  if (mutation->write_kind != FLOWIE_CLUSTER_PGSQL_EVENT_ONLY) {
    rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_COMMAND_OK);
    if (rc == TURBO_OK && strcmp(PQcmdTuples(result), "1") != 0) rc = TURBO_EBUSY;
    if (result) PQclear(result);
  }
  if (rc != TURBO_OK) return rc;
  if (mutation->write_kind == FLOWIE_CLUSTER_PGSQL_FACT_ONLY) return TURBO_OK;
  result = PQexecParams(store->connection, store->outbox_insert_sql, 12, NULL, outbox_values,
                        outbox_lengths, outbox_formats, 0);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK && strcmp(PQcmdTuples(result), "1") != 0) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

int flowie_cluster_pgsql_fact_commit(flowie_cluster_pgsql_fact_store_t *store,
                                     const flowie_cluster_pgsql_fact_command_t *command) {
  flowie_cluster_pgsql_config_t coordinator = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
  ptrdiff_t fact_delta = 0;
  size_t outbox_records = 0u;
  uint64_t event_bytes = 0u;
  int insert_shared_selection = 0;
  int rc;
  if (!store || !store->connection || PQstatus(store->connection) != CONNECTION_OK)
    return TURBO_EIO;
  flowie_cluster_pgsql_fact_config_view(store, &coordinator, &config);
  rc = flowie_cluster_pgsql_fact_command_validate(&config, command);
  if (rc != TURBO_OK) return rc;
  flowie_cluster_pgsql_fact_digest(command, digest);
  rc = flowie_cluster_pgsql_fact_exec(store->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_lock_owner(store, &command->owner);
  if (rc != TURBO_OK) goto rollback;
  rc = flowie_cluster_pgsql_fact_capacity_lock(store);
  if (rc != TURBO_OK) goto rollback;
  if (command->dedupe && command->dedupe->shared_filter_size != 0u) {
    rc = flowie_cluster_pgsql_fact_shared_selection_check(store, command, TURBO_ENOENT);
    if (rc == TURBO_OK) {
      rc = TURBO_EALREADY;
      goto rollback;
    }
    if (rc == TURBO_ENOENT)
      insert_shared_selection = 1;
    else
      goto rollback;
  }
  if (command->dedupe && command->dedupe->shared_filter_size == 0u) {
    rc = flowie_cluster_pgsql_fact_dedupe_check(store, command, TURBO_ENOENT);
    if (rc == TURBO_OK) {
      rc = TURBO_EALREADY;
      goto rollback;
    }
    if (rc != TURBO_ENOENT) goto rollback;
  }
  if (command->mutation_count != 0u) {
    rc = flowie_cluster_pgsql_fact_receipt_check(store, command, digest, TURBO_ENOENT);
    if (rc == TURBO_OK) {
      rc = TURBO_EALREADY;
      goto rollback;
    }
    if (rc != TURBO_ENOENT) goto rollback;
  }
  for (size_t index = 0u; index < command->mutation_count; ++index) {
    const flowie_cluster_pgsql_fact_mutation_t *mutation = &command->mutations[index];
    const turbo_flow_record_mutation_t *record = &mutation->record;
    uint64_t current_revision = 0u;
    if (mutation->write_kind != FLOWIE_CLUSTER_PGSQL_EVENT_ONLY) {
      rc = flowie_cluster_pgsql_fact_current_revision(store, mutation, &current_revision);
      if (rc != TURBO_OK) goto rollback;
      if (current_revision != record->expected_revision) {
        rc = TURBO_EBUSY;
        goto rollback;
      }
      if (record->kind == TURBO_FLOW_RECORD_PUT &&
          current_revision == TURBO_FLOW_RECORD_REVISION_ABSENT)
        ++fact_delta;
      else if (record->kind == TURBO_FLOW_RECORD_DELETE)
        --fact_delta;
    }
    if (mutation->write_kind != FLOWIE_CLUSTER_PGSQL_FACT_ONLY) {
      if (event_bytes > UINT64_MAX - mutation->event_payload_size) {
        rc = TURBO_ERANGE;
        goto rollback;
      }
      event_bytes += mutation->event_payload_size;
      outbox_records += 1u;
    }
  }
  rc = flowie_cluster_pgsql_fact_capacity(store, fact_delta,
                                          command->mutation_count != 0u ? 1u : 0u, outbox_records,
                                          command->dedupe ? 1u : 0u,
                                          event_bytes);
  if (rc != TURBO_OK) goto rollback;
  if (command->mutation_count != 0u) {
    rc = flowie_cluster_pgsql_fact_insert_receipt(store, command, digest);
    if (rc != TURBO_OK) goto rollback;
  }
  if (insert_shared_selection) {
    rc = flowie_cluster_pgsql_fact_insert_shared_selection(store, command);
    if (rc != TURBO_OK) goto rollback;
  } else if (command->dedupe) {
    rc = flowie_cluster_pgsql_fact_insert_dedupe(store, command);
    if (rc != TURBO_OK) goto rollback;
  }
  for (size_t index = 0u; index < command->mutation_count; ++index) {
    rc = flowie_cluster_pgsql_fact_apply(store, command, &command->mutations[index], index);
    if (rc != TURBO_OK) goto rollback;
  }
  rc = flowie_cluster_pgsql_fact_exec(store->connection, "COMMIT", PGRES_COMMAND_OK);
  crypto_wipe(digest, sizeof(digest));
  return rc;

rollback:
  crypto_wipe(digest, sizeof(digest));
  return flowie_cluster_pgsql_fact_rollback(store, rc);
}

int flowie_cluster_pgsql_fact_confirm(flowie_cluster_pgsql_fact_store_t *store,
                                      const flowie_cluster_pgsql_fact_command_t *command) {
  flowie_cluster_pgsql_config_t coordinator = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  uint8_t digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE];
  int rc;
  if (!store || !store->connection || PQstatus(store->connection) != CONNECTION_OK)
    return TURBO_EIO;
  flowie_cluster_pgsql_fact_config_view(store, &coordinator, &config);
  rc = flowie_cluster_pgsql_fact_command_validate(&config, command);
  if (rc != TURBO_OK) return rc;
  if (command->dedupe && command->dedupe->shared_filter_size != 0u) {
    return flowie_cluster_pgsql_fact_shared_selection_check(store, command, TURBO_ENOENT);
  }
  if (command->dedupe) {
    rc = flowie_cluster_pgsql_fact_dedupe_check(store, command, TURBO_ENOENT);
    if (rc == TURBO_OK || rc == TURBO_EBUSY) return rc;
    if (rc != TURBO_ENOENT) return rc;
  }
  flowie_cluster_pgsql_fact_digest(command, digest);
  rc = flowie_cluster_pgsql_fact_receipt_check(store, command, digest, TURBO_ENOENT);
  crypto_wipe(digest, sizeof(digest));
  return rc;
}

int flowie_cluster_pgsql_event_ack_count(
    flowie_cluster_pgsql_fact_store_t *store,
    const flowie_cluster_pgsql_event_dedupe_t *source_event, size_t *out_count) {
  static const Oid types[8] = {
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
      FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_BYTEA};
  char event_index[16], source_shard[16], source_epoch[32], source_revision[32];
  const char *values[8];
  int lengths[8] = {0, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE, 0, 0, 0, 0,
                    FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE};
  int formats[8] = {0, 0, 1, 0, 0, 0, 0, 1};
  PGresult *result = NULL;
  size_t exact_count = 0u;
  size_t total_count = 0u;
  int rc;
  if (out_count) *out_count = 0u;
  if (!store || !store->connection || PQstatus(store->connection) != CONNECTION_OK ||
      !source_event || source_event->size < sizeof(*source_event) ||
      source_event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      source_event->target_session_id != 0u ||
      source_event->source_shard_id >= store->shard_count ||
      source_event->source_owner_epoch == 0u ||
      source_event->source_fact_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
      !flowie_cluster_pgsql_fact_nonzero_bytes(source_event->source_command_id,
                                               sizeof(source_event->source_command_id)) ||
      !flowie_cluster_pgsql_fact_nonzero_bytes(source_event->event_digest,
                                               sizeof(source_event->event_digest)) ||
      !out_count)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_text_u64(event_index, sizeof(event_index),
                                          source_event->event_index);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_shard, sizeof(source_shard),
                                            source_event->source_shard_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_epoch, sizeof(source_epoch),
                                            source_event->source_owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_text_u64(source_revision, sizeof(source_revision),
                                            source_event->source_fact_revision);
  if (rc != TURBO_OK) return rc;
  values[0] = store->cluster_id;
  values[1] = store->listener_id;
  values[2] = (const char *)source_event->source_command_id;
  values[3] = event_index;
  values[4] = source_shard;
  values[5] = source_epoch;
  values[6] = source_revision;
  values[7] = (const char *)source_event->event_digest;
  result = PQexecParams(store->connection, store->dedupe_ack_count_sql, 8, types, values, lengths,
                        formats, 1);
  rc = flowie_cluster_pgsql_fact_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 2)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(PQgetvalue(result, 0, 0),
                                              (size_t)PQgetlength(result, 0, 0), &exact_count);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_fact_parse_size(PQgetvalue(result, 0, 1),
                                              (size_t)PQgetlength(result, 0, 1), &total_count);
  if (result) PQclear(result);
  if (rc != TURBO_OK) return rc;
  if (total_count > store->shard_count) return TURBO_EPROTO;
  if (exact_count != total_count) return TURBO_EBUSY;
  *out_count = exact_count;
  return TURBO_OK;
}
