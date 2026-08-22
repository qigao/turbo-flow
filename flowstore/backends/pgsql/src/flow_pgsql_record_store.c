#include "flow_pgsql_storage_internal.h"

#include "libpq-fe.h"
#include "turbo_error.h"
#include "turbo_flow_stl_adapter.h"
#include "turbo_str.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_PGSQL_RECORD_TABLE "turbo_flow_record_store"
#define FLOW_PGSQL_RECORD_LOCK_SEED "607599071"
#define FLOW_PGSQL_OID_INT8 20u
#define FLOW_PGSQL_OID_TEXT 25u
#define FLOW_PGSQL_OID_BYTEA 17u

typedef struct flow_pgsql_record_store_s {
  PGconn *connection;
  tstr conninfo;
  tstr namespace_name;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_records;
  turbo_hash_map_t mutation_keys;
  int mutation_keys_initialized;
} flow_pgsql_record_store_t;

static int flow_pgsql_record_result_status(PGresult *result, ExecStatusType expected) {
  const char *sqlstate;
  if (result && PQresultStatus(result) == expected) return TURBO_OK;
  sqlstate = result ? PQresultErrorField(result, PG_DIAG_SQLSTATE) : NULL;
  if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0 ||
                   strcmp(sqlstate, "55P03") == 0))
    return TURBO_EBUSY;
  return TURBO_EIO;
}

static int flow_pgsql_record_exec(PGconn *connection, const char *sql, ExecStatusType expected) {
  PGresult *result;
  int rc;
  if (!connection || !sql) return TURBO_EINVAL;
  result = PQexec(connection, sql);
  rc = flow_pgsql_record_result_status(result, expected);
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_record_rollback(flow_pgsql_record_store_t *store, int status) {
  if (store && store->connection)
    (void)flow_pgsql_record_exec(store->connection, "ROLLBACK", PGRES_COMMAND_OK);
  return status;
}

static int flow_pgsql_record_parse_u64(const char *text, size_t text_size, uint64_t *out) {
  char buffer[32];
  char *end = NULL;
  unsigned long long value;
  if (!text || text_size == 0u || text_size >= sizeof(buffer) || !out) return TURBO_EPROTO;
  memcpy(buffer, text, text_size);
  buffer[text_size] = '\0';
  errno = 0;
  value = strtoull(buffer, &end, 10);
  if (errno != 0 || end != buffer + text_size || value == 0u ||
      value > (unsigned long long)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EPROTO;
  *out = (uint64_t)value;
  return TURBO_OK;
}

static int flow_pgsql_record_parse_count(const char *text, size_t text_size, size_t *out) {
  char buffer[32];
  char *end = NULL;
  unsigned long long value;
  if (!text || text_size == 0u || text_size >= sizeof(buffer) || !out) return TURBO_EPROTO;
  memcpy(buffer, text, text_size);
  buffer[text_size] = '\0';
  errno = 0;
  value = strtoull(buffer, &end, 10);
  if (errno != 0 || end != buffer + text_size || value > (unsigned long long)SIZE_MAX)
    return TURBO_EPROTO;
  *out = (size_t)value;
  return TURBO_OK;
}

static size_t flow_pgsql_record_key_hash(const void *key, size_t key_size, void *ctx) {
  const vstr *view = (const vstr *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(view->data, view->len, NULL);
}

static bool flow_pgsql_record_key_equal(const void *left, const void *right, size_t key_size,
                                        void *ctx) {
  const vstr *a = (const vstr *)left;
  const vstr *b = (const vstr *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static int flow_pgsql_record_mutations_validate(flow_pgsql_record_store_t *store,
                                                const turbo_flow_record_mutation_t *mutations,
                                                size_t mutation_count) {
  static const uint8_t present = 1u;
  if (!store || !mutations || mutation_count == 0u || mutation_count > store->max_batch_size)
    return TURBO_EINVAL;
  turbo_hash_map_clear(&store->mutation_keys);
  for (size_t i = 0u; i < mutation_count; ++i) {
    const turbo_flow_record_mutation_t *mutation = &mutations[i];
    vstr key;
    int rc;
    if (mutation->size < sizeof(*mutation) || !mutation->key || mutation->key_size == 0u ||
        mutation->key_size > store->max_key_size ||
        mutation->expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
        mutation->next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
      return TURBO_EINVAL;
    if (mutation->kind == TURBO_FLOW_RECORD_PUT) {
      if ((!mutation->value && mutation->value_size != 0u) ||
          mutation->value_size > store->max_value_size ||
          mutation->next_revision <= mutation->expected_revision)
        return TURBO_EINVAL;
    } else if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
      if (mutation->expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT ||
          mutation->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || mutation->value ||
          mutation->value_size != 0u)
        return TURBO_EINVAL;
    } else {
      return TURBO_EINVAL;
    }
    key = vstr_from_buf((const char *)mutation->key, mutation->key_size);
    if (turbo_hash_map_contains(&store->mutation_keys, &key)) return TURBO_EINVAL;
    rc = turbo_hash_map_put(&store->mutation_keys, &key, &present);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_pgsql_record_schema_prepare(flow_pgsql_record_store_t *store, int create_table) {
  static const char ddl[] =
      "CREATE TABLE IF NOT EXISTS " FLOW_PGSQL_RECORD_TABLE " ("
      "namespace_name text NOT NULL,record_key bytea NOT NULL,revision bigint NOT NULL "
      "CHECK(revision>0),value bytea NOT NULL,PRIMARY KEY(namespace_name,record_key));";
  static const char validate[] =
      "SELECT namespace_name::text,record_key::bytea,revision::bigint,value::bytea "
      "FROM " FLOW_PGSQL_RECORD_TABLE " LIMIT 0";
  PGresult *result;
  int rc;
  if (!store || !store->connection) return TURBO_EINVAL;
  if (create_table) {
    rc = flow_pgsql_record_exec(store->connection, ddl, PGRES_COMMAND_OK);
    if (rc != TURBO_OK) return rc;
  }
  result = PQexec(store->connection, validate);
  rc = flow_pgsql_record_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK &&
      (PQnfields(result) != 4 || PQftype(result, 0) != FLOW_PGSQL_OID_TEXT ||
       PQftype(result, 1) != FLOW_PGSQL_OID_BYTEA || PQftype(result, 2) != FLOW_PGSQL_OID_INT8 ||
       PQftype(result, 3) != FLOW_PGSQL_OID_BYTEA))
    rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_record_store_scan(void *ctx, turbo_flow_record_visit_fn visit,
                                        void *visit_ctx) {
  static const char sql[] = "SELECT record_key,revision::text,value FROM " FLOW_PGSQL_RECORD_TABLE
                            " WHERE namespace_name=$1 ORDER BY record_key";
  flow_pgsql_record_store_t *store = (flow_pgsql_record_store_t *)ctx;
  const char *values[1];
  PGresult *result = NULL;
  int rc;
  if (!store || !store->connection || !visit) return TURBO_EINVAL;
  rc = flow_pgsql_record_exec(store->connection, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
                              PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  values[0] = store->namespace_name;
  result = PQexecParams(store->connection, sql, 1, NULL, values, NULL, NULL, 1);
  rc = flow_pgsql_record_result_status(result, PGRES_TUPLES_OK);
  if (rc != TURBO_OK) goto rollback;
  if (PQntuples(result) < 0 || (size_t)PQntuples(result) > store->max_records) {
    rc = TURBO_ENOSPC;
    goto rollback;
  }
  for (int row = 0; row < PQntuples(result); ++row) {
    turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
    int key_size = PQgetlength(result, row, 0);
    int revision_size = PQgetlength(result, row, 1);
    int value_size = PQgetlength(result, row, 2);
    if (PQgetisnull(result, row, 0) || PQgetisnull(result, row, 1) || PQgetisnull(result, row, 2) ||
        key_size <= 0 || (size_t)key_size > store->max_key_size || revision_size <= 0 ||
        value_size < 0 || (size_t)value_size > store->max_value_size) {
      rc = TURBO_EPROTO;
      goto rollback;
    }
    record.key = (const uint8_t *)PQgetvalue(result, row, 0);
    record.key_size = (size_t)key_size;
    record.value = (const uint8_t *)PQgetvalue(result, row, 2);
    record.value_size = (size_t)value_size;
    rc = flow_pgsql_record_parse_u64(PQgetvalue(result, row, 1), (size_t)revision_size,
                                     &record.revision);
    if (rc == TURBO_OK) rc = visit(visit_ctx, &record);
    if (rc != TURBO_OK) goto rollback;
  }
  PQclear(result);
  result = NULL;
  return flow_pgsql_record_exec(store->connection, "COMMIT", PGRES_COMMAND_OK);

rollback:
  if (result) PQclear(result);
  return flow_pgsql_record_rollback(store, rc);
}

static int flow_pgsql_record_current_revision(flow_pgsql_record_store_t *store,
                                              const turbo_flow_record_mutation_t *mutation,
                                              uint64_t *revision) {
  static const char sql[] = "SELECT revision::text FROM " FLOW_PGSQL_RECORD_TABLE
                            " WHERE namespace_name=$1 AND record_key=$2";
  static const Oid types[2] = {FLOW_PGSQL_OID_TEXT, FLOW_PGSQL_OID_BYTEA};
  const char *values[2] = {store->namespace_name, (const char *)mutation->key};
  int lengths[2] = {(int)tstr_len(store->namespace_name), (int)mutation->key_size};
  int formats[2] = {0, 1};
  PGresult *result;
  int rc;
  *revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  result = PQexecParams(store->connection, sql, 2, types, values, lengths, formats, 0);
  rc = flow_pgsql_record_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) > 1) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && PQntuples(result) == 1) {
    int size = PQgetlength(result, 0, 0);
    if (PQgetisnull(result, 0, 0) || size <= 0) rc = TURBO_EPROTO;
    else rc = flow_pgsql_record_parse_u64(PQgetvalue(result, 0, 0), (size_t)size, revision);
  }
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_record_count(flow_pgsql_record_store_t *store, size_t *count) {
  static const char sql[] =
      "SELECT count(*)::text FROM " FLOW_PGSQL_RECORD_TABLE " WHERE namespace_name=$1";
  const char *values[1] = {store->namespace_name};
  PGresult *result = PQexecParams(store->connection, sql, 1, NULL, values, NULL, NULL, 0);
  int rc = flow_pgsql_record_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQgetisnull(result, 0, 0))) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flow_pgsql_record_parse_count(PQgetvalue(result, 0, 0), (size_t)PQgetlength(result, 0, 0),
                                       count);
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_record_apply(flow_pgsql_record_store_t *store,
                                   const turbo_flow_record_mutation_t *mutation) {
  static const char put_sql[] = "INSERT INTO " FLOW_PGSQL_RECORD_TABLE
                                "(namespace_name,record_key,revision,value) VALUES($1,$2,$3,$4) "
                                "ON CONFLICT(namespace_name,record_key) DO UPDATE SET "
                                "revision=excluded.revision,value=excluded.value";
  static const char delete_sql[] = "DELETE FROM " FLOW_PGSQL_RECORD_TABLE
                                   " WHERE namespace_name=$1 AND record_key=$2 AND revision=$3";
  static const Oid put_types[4] = {FLOW_PGSQL_OID_TEXT, FLOW_PGSQL_OID_BYTEA, FLOW_PGSQL_OID_INT8,
                                   FLOW_PGSQL_OID_BYTEA};
  static const Oid delete_types[3] = {FLOW_PGSQL_OID_TEXT, FLOW_PGSQL_OID_BYTEA,
                                      FLOW_PGSQL_OID_INT8};
  static const char empty_value = '\0';
  char revision[32];
  const char *values[4];
  int lengths[4];
  int formats[4] = {0, 1, 0, 1};
  PGresult *result;
  int revision_size;
  int rc;
  uint64_t selected_revision = mutation->kind == TURBO_FLOW_RECORD_PUT
                                   ? mutation->next_revision
                                   : mutation->expected_revision;
  revision_size =
      snprintf(revision, sizeof(revision), "%llu", (unsigned long long)selected_revision);
  if (revision_size <= 0 || (size_t)revision_size >= sizeof(revision)) return TURBO_ERANGE;
  values[0] = store->namespace_name;
  values[1] = (const char *)mutation->key;
  values[2] = revision;
  values[3] = mutation->value_size ? (const char *)mutation->value : &empty_value;
  lengths[0] = (int)tstr_len(store->namespace_name);
  lengths[1] = (int)mutation->key_size;
  lengths[2] = revision_size;
  lengths[3] = (int)mutation->value_size;
  result = PQexecParams(store->connection,
                        mutation->kind == TURBO_FLOW_RECORD_PUT ? put_sql : delete_sql,
                        mutation->kind == TURBO_FLOW_RECORD_PUT ? 4 : 3,
                        mutation->kind == TURBO_FLOW_RECORD_PUT ? put_types : delete_types, values,
                        lengths, formats, 0);
  rc = flow_pgsql_record_result_status(result, PGRES_COMMAND_OK);
  if (rc == TURBO_OK && mutation->kind == TURBO_FLOW_RECORD_DELETE &&
      strcmp(PQcmdTuples(result), "1") != 0)
    rc = TURBO_EBUSY;
  if (result) PQclear(result);
  return rc;
}

static int flow_pgsql_record_store_commit(void *ctx, const turbo_flow_record_mutation_t *mutations,
                                          size_t mutation_count) {
  static const char lock_sql[] =
      "SELECT pg_advisory_xact_lock(hashtextextended($1," FLOW_PGSQL_RECORD_LOCK_SEED "))";
  flow_pgsql_record_store_t *store = (flow_pgsql_record_store_t *)ctx;
  const char *lock_values[1];
  PGresult *result = NULL;
  ptrdiff_t count_delta = 0;
  size_t record_count = 0u;
  int rc;
  if (!store || !store->connection) return TURBO_EINVAL;
  rc = flow_pgsql_record_mutations_validate(store, mutations, mutation_count);
  if (rc != TURBO_OK) return rc;
  rc = flow_pgsql_record_exec(store->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) goto done;
  lock_values[0] = store->namespace_name;
  result = PQexecParams(store->connection, lock_sql, 1, NULL, lock_values, NULL, NULL, 0);
  rc = flow_pgsql_record_result_status(result, PGRES_TUPLES_OK);
  if (result) PQclear(result);
  result = NULL;
  if (rc != TURBO_OK) goto rollback;
  for (size_t i = 0u; i < mutation_count; ++i) {
    uint64_t current_revision;
    rc = flow_pgsql_record_current_revision(store, &mutations[i], &current_revision);
    if (rc != TURBO_OK) goto rollback;
    if (current_revision != mutations[i].expected_revision) {
      rc = TURBO_EBUSY;
      goto rollback;
    }
    if (mutations[i].kind == TURBO_FLOW_RECORD_PUT &&
        current_revision == TURBO_FLOW_RECORD_REVISION_ABSENT)
      ++count_delta;
    else if (mutations[i].kind == TURBO_FLOW_RECORD_DELETE) --count_delta;
  }
  rc = flow_pgsql_record_count(store, &record_count);
  if (rc != TURBO_OK) goto rollback;
  if (record_count > store->max_records) {
    rc = TURBO_ENOSPC;
    goto rollback;
  }
  if (count_delta < 0 && (size_t)(-count_delta) > record_count) {
    rc = TURBO_EPROTO;
    goto rollback;
  }
  if (count_delta > 0 && (size_t)count_delta > store->max_records - record_count) {
    rc = TURBO_ENOSPC;
    goto rollback;
  }
  for (size_t i = 0u; i < mutation_count; ++i) {
    rc = flow_pgsql_record_apply(store, &mutations[i]);
    if (rc != TURBO_OK) goto rollback;
  }
  rc = flow_pgsql_record_exec(store->connection, "COMMIT", PGRES_COMMAND_OK);
  goto done;

rollback:
  rc = flow_pgsql_record_rollback(store, rc);
done:
  turbo_hash_map_clear(&store->mutation_keys);
  return rc;
}

int flow_pgsql_record_store_create(const turbo_flow_pgsql_record_store_config_t *config,
                                   turbo_flow_record_store_t *out) {
  flow_pgsql_record_store_t *store;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  int rc;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_PGSQL_RECORD_STORE_API_VERSION || !out ||
      out->size < sizeof(*out) || out->ctx || !config->conninfo || !config->conninfo[0] ||
      !config->namespace_name || !config->namespace_name[0] ||
      strlen(config->namespace_name) > TURBO_FLOW_PGSQL_RECORD_STORE_NAMESPACE_MAX ||
      config->max_key_size > TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_KEY_SIZE ||
      config->max_value_size > TURBO_FLOW_PGSQL_RECORD_STORE_MAX_VALUE_SIZE ||
      config->max_batch_size > UINT16_MAX || config->max_records == 0u ||
      config->max_records > TURBO_FLOW_PGSQL_RECORD_STORE_MAX_RECORDS ||
      (config->create_table != 0 && config->create_table != 1))
    return TURBO_EINVAL;
  max_key_size = config->max_key_size ? config->max_key_size
                                      : TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_KEY_SIZE;
  max_value_size = config->max_value_size ? config->max_value_size
                                          : TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE;
  max_batch_size = config->max_batch_size ? config->max_batch_size
                                          : TURBO_FLOW_PGSQL_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE;
  if (max_key_size == 0u || max_value_size == 0u || max_batch_size == 0u) return TURBO_EINVAL;
  store = (flow_pgsql_record_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->conninfo = tstr_dup(config->conninfo);
  store->namespace_name = tstr_dup(config->namespace_name);
  store->max_key_size = max_key_size;
  store->max_value_size = max_value_size;
  store->max_batch_size = max_batch_size;
  store->max_records = config->max_records;
  rc = turbo_hash_map_init(&store->mutation_keys, sizeof(vstr), sizeof(uint8_t),
                           flow_pgsql_record_key_hash, flow_pgsql_record_key_equal, NULL);
  if (rc == TURBO_OK) {
    store->mutation_keys_initialized = 1;
    rc = turbo_hash_map_reserve(&store->mutation_keys, max_batch_size);
  }
  if (!store->conninfo || !store->namespace_name || rc != TURBO_OK) goto fail;
  store->connection = PQconnectdb(store->conninfo);
  if (!store->connection || PQstatus(store->connection) != CONNECTION_OK) {
    rc = TURBO_EIO;
    goto fail;
  }
  rc = flow_pgsql_record_schema_prepare(store, config->create_table);
  if (rc != TURBO_OK) goto fail;
  out->api_version = TURBO_FLOW_RECORD_STORE_API_VERSION;
  out->capabilities = TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  out->max_key_size = max_key_size;
  out->max_value_size = max_value_size;
  out->max_batch_size = max_batch_size;
  out->max_records = config->max_records;
  out->ctx = store;
  out->scan = flow_pgsql_record_store_scan;
  out->commit = flow_pgsql_record_store_commit;
  return TURBO_OK;

fail:
  if (store->connection) PQfinish(store->connection);
  if (store->mutation_keys_initialized) turbo_hash_map_destroy(&store->mutation_keys);
  tstr_freep(&store->conninfo);
  tstr_freep(&store->namespace_name);
  free(store);
  return rc == TURBO_OK ? TURBO_ENOMEM : rc;
}

void flow_pgsql_record_store_destroy(turbo_flow_record_store_t *store) {
  flow_pgsql_record_store_t *pgsql_store;
  if (!store || store->size < sizeof(*store) || !store->ctx) return;
  pgsql_store = (flow_pgsql_record_store_t *)store->ctx;
  if (pgsql_store->connection) PQfinish(pgsql_store->connection);
  if (pgsql_store->mutation_keys_initialized) turbo_hash_map_destroy(&pgsql_store->mutation_keys);
  tstr_freep(&pgsql_store->conninfo);
  tstr_freep(&pgsql_store->namespace_name);
  free(pgsql_store);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
}
