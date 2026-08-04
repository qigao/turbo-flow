#include "flow_sqlite_storage_internal.h"

#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_str.h"

#include <limits.h>
#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_SQLITE_RECORD_TABLE "turbo_flow_record_store_v1"
#define FLOW_SQLITE_MAX_BATCH_SIZE UINT16_MAX

static const char FLOW_SQLITE_RECORD_FILE_PRAGMAS[] =
    "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;";

static const char FLOW_SQLITE_RECORD_MEMORY_PRAGMAS[] =
    "PRAGMA journal_mode=MEMORY;PRAGMA synchronous=OFF;";

static const char FLOW_SQLITE_RECORD_SCHEMA[] =
    "CREATE TABLE IF NOT EXISTS " FLOW_SQLITE_RECORD_TABLE "("
    "namespace_name TEXT NOT NULL,record_key BLOB NOT NULL,revision INTEGER NOT NULL "
    "CHECK(revision>0),value BLOB NOT NULL,PRIMARY KEY(namespace_name,record_key)) WITHOUT ROWID;";

typedef struct flow_sqlite_record_store_s {
  sqlite3 *database;
  tstr_t database_path;
  tstr_t namespace_name;
  size_t max_records;
  size_t max_bytes;
  size_t max_item_bytes;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  turbo_hash_map_t mutation_keys;
  int mutation_keys_initialized;
} flow_sqlite_record_store_t;

static int flow_sqlite_status(int status) {
  switch (status & 0xff) {
  case SQLITE_OK:
  case SQLITE_DONE:
  case SQLITE_ROW:
    return TURBO_OK;
  case SQLITE_BUSY:
  case SQLITE_LOCKED:
    return TURBO_EBUSY;
  case SQLITE_NOMEM:
    return TURBO_ENOMEM;
  case SQLITE_FULL:
    return TURBO_ENOSPC;
  case SQLITE_TOOBIG:
    return TURBO_EFBIG;
  case SQLITE_INTERRUPT:
    return TURBO_EINTR;
  case SQLITE_CONSTRAINT:
  case SQLITE_MISMATCH:
  case SQLITE_RANGE:
    return TURBO_EINVAL;
  default:
    return TURBO_EIO;
  }
}

static int flow_sqlite_exec(sqlite3 *database, const char *sql) {
  int status;
  if (!database || !sql) return TURBO_EINVAL;
  status = sqlite3_exec(database, sql, NULL, NULL, NULL);
  return status == SQLITE_OK ? TURBO_OK : flow_sqlite_status(status);
}

static int flow_sqlite_size_add(size_t left, size_t right, size_t *out) {
  if (!out || left > SIZE_MAX - right) return TURBO_ERANGE;
  *out = left + right;
  return TURBO_OK;
}

static size_t flow_sqlite_record_key_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *view = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(view->data, view->len, NULL);
}

static bool flow_sqlite_record_key_equal(const void *left, const void *right, size_t key_size,
                                         void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static int flow_sqlite_record_validate_mutations(flow_sqlite_record_store_t *store,
                                                 const turbo_flow_record_mutation_t *mutations,
                                                 size_t mutation_count) {
  static const uint8_t present = 1u;
  if (!store || !mutations || mutation_count == 0u || mutation_count > store->max_batch_size)
    return TURBO_EINVAL;
  turbo_hash_map_clear(&store->mutation_keys);
  for (size_t i = 0u; i < mutation_count; ++i) {
    const turbo_flow_record_mutation_t *mutation = &mutations[i];
    tstr_v key;
    size_t item_bytes = 0u;
    int rc;
    if (mutation->size < sizeof(*mutation) || !mutation->key || mutation->key_size == 0u ||
        mutation->key_size > store->max_key_size || mutation->key_size > INT_MAX ||
        mutation->expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
        mutation->next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
      return TURBO_EINVAL;
    if (mutation->kind == TURBO_FLOW_RECORD_PUT) {
      if ((!mutation->value && mutation->value_size != 0u) ||
          mutation->value_size > store->max_value_size || mutation->value_size > INT_MAX ||
          mutation->next_revision <= mutation->expected_revision)
        return TURBO_EINVAL;
      rc = flow_sqlite_size_add(mutation->key_size, mutation->value_size, &item_bytes);
      if (rc != TURBO_OK || item_bytes > store->max_item_bytes) return TURBO_EFBIG;
    } else if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
      if (mutation->expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT ||
          mutation->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || mutation->value ||
          mutation->value_size != 0u)
        return TURBO_EINVAL;
    } else {
      return TURBO_EINVAL;
    }
    key = tstr_v_from_buf((const char *)mutation->key, mutation->key_size);
    if (turbo_hash_map_contains(&store->mutation_keys, &key)) return TURBO_EINVAL;
    rc = turbo_hash_map_put(&store->mutation_keys, &key, &present);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_sqlite_record_stats(flow_sqlite_record_store_t *store, size_t *records,
                                    size_t *bytes) {
  static const char sql[] =
      "SELECT count(*),COALESCE(sum(length(record_key)),0),COALESCE(sum(length(value)),0) "
      "FROM " FLOW_SQLITE_RECORD_TABLE " WHERE namespace_name=?1";
  sqlite3_stmt *statement = NULL;
  sqlite3_int64 record_count;
  sqlite3_int64 key_bytes;
  sqlite3_int64 value_bytes;
  int status;
  int rc;
  if (records) *records = 0u;
  if (bytes) *bytes = 0u;
  if (!store || !store->database || !records || !bytes) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(store->database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK) {
    rc = flow_sqlite_status(status == SQLITE_OK ? sqlite3_errcode(store->database) : status);
    goto done;
  }
  status = sqlite3_step(statement);
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 2) != SQLITE_INTEGER) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flow_sqlite_status(status);
    goto done;
  }
  record_count = sqlite3_column_int64(statement, 0);
  key_bytes = sqlite3_column_int64(statement, 1);
  value_bytes = sqlite3_column_int64(statement, 2);
  if (record_count < 0 || key_bytes < 0 || value_bytes < 0 ||
      (uint64_t)record_count > (uint64_t)SIZE_MAX || (uint64_t)key_bytes > (uint64_t)SIZE_MAX ||
      (uint64_t)value_bytes > (uint64_t)SIZE_MAX ||
      (size_t)key_bytes > SIZE_MAX - (size_t)value_bytes) {
    rc = TURBO_EPROTO;
    goto done;
  }
  *records = (size_t)record_count;
  *bytes = (size_t)key_bytes + (size_t)value_bytes;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  return rc;
}

static int flow_sqlite_record_scan(void *ctx, turbo_flow_record_visit_fn visit, void *visit_ctx) {
  static const char sql[] = "SELECT record_key,revision,value FROM " FLOW_SQLITE_RECORD_TABLE
                            " WHERE namespace_name=?1 ORDER BY record_key";
  flow_sqlite_record_store_t *store = (flow_sqlite_record_store_t *)ctx;
  sqlite3_stmt *statement = NULL;
  size_t records = 0u;
  size_t bytes = 0u;
  int transaction = 0;
  int status;
  int rc;
  if (!store || !store->database || !visit) return TURBO_EINVAL;
  rc = flow_sqlite_exec(store->database, "BEGIN");
  if (rc != TURBO_OK) return rc;
  transaction = 1;
  status = sqlite3_prepare_v2(store->database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK) {
    rc = flow_sqlite_status(status == SQLITE_OK ? sqlite3_errcode(store->database) : status);
    goto rollback;
  }
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    turbo_flow_record_view_t view = TURBO_FLOW_RECORD_VIEW_INIT;
    int key_size;
    int value_size;
    size_t item_bytes;
    sqlite3_int64 revision;
    if (sqlite3_column_type(statement, 0) != SQLITE_BLOB ||
        sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 2) != SQLITE_BLOB) {
      rc = TURBO_EPROTO;
      goto rollback;
    }
    key_size = sqlite3_column_bytes(statement, 0);
    value_size = sqlite3_column_bytes(statement, 2);
    revision = sqlite3_column_int64(statement, 1);
    if (key_size <= 0 || value_size < 0 || revision <= 0 ||
        (size_t)key_size > store->max_key_size || (size_t)value_size > store->max_value_size) {
      rc = TURBO_EINVAL;
      goto rollback;
    }
    if (flow_sqlite_size_add((size_t)key_size, (size_t)value_size, &item_bytes) != TURBO_OK) {
      rc = TURBO_EFBIG;
      goto rollback;
    }
    if (item_bytes > store->max_item_bytes) {
      rc = TURBO_EFBIG;
      goto rollback;
    }
    if (records >= store->max_records || item_bytes > store->max_bytes ||
        bytes > store->max_bytes - item_bytes) {
      rc = TURBO_ENOSPC;
      goto rollback;
    }
    view.key = (const uint8_t *)sqlite3_column_blob(statement, 0);
    view.key_size = (size_t)key_size;
    view.revision = (uint64_t)revision;
    view.value = (const uint8_t *)sqlite3_column_blob(statement, 2);
    view.value_size = (size_t)value_size;
    if (!view.key || (!view.value && view.value_size != 0u)) {
      rc = TURBO_EPROTO;
      goto rollback;
    }
    ++records;
    bytes += item_bytes;
    rc = visit(visit_ctx, &view);
    if (rc != TURBO_OK) goto rollback;
  }
  if (status != SQLITE_DONE) {
    rc = flow_sqlite_status(status);
    goto rollback;
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  rc = flow_sqlite_exec(store->database, "COMMIT");
  if (rc == TURBO_OK) transaction = 0;
  goto done;

rollback:
  (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  transaction = 0;
done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction) (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  return rc;
}

static int flow_sqlite_record_current(flow_sqlite_record_store_t *store, sqlite3_stmt *statement,
                                      const turbo_flow_record_mutation_t *mutation,
                                      uint64_t *revision, size_t *value_size, int *found) {
  int status;
  if (revision) *revision = 0u;
  if (value_size) *value_size = 0u;
  if (found) *found = 0;
  if (!store || !statement || !mutation || !revision || !value_size || !found) return TURBO_EINVAL;
  (void)sqlite3_reset(statement);
  (void)sqlite3_clear_bindings(statement);
  if (sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_blob(statement, 2, mutation->key, (int)mutation->key_size, SQLITE_TRANSIENT) !=
          SQLITE_OK)
    return flow_sqlite_status(sqlite3_errcode(store->database));
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) return TURBO_OK;
  if (status != SQLITE_ROW) return flow_sqlite_status(status);
  if (sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
      sqlite3_column_int64(statement, 0) <= 0 || sqlite3_column_int64(statement, 1) < 0)
    return TURBO_EPROTO;
  *revision = (uint64_t)sqlite3_column_int64(statement, 0);
  *value_size = (size_t)sqlite3_column_int64(statement, 1);
  *found = 1;
  return sqlite3_step(statement) == SQLITE_DONE ? TURBO_OK : TURBO_EPROTO;
}

static int flow_sqlite_record_put(flow_sqlite_record_store_t *store, sqlite3_stmt *statement,
                                  const turbo_flow_record_mutation_t *mutation) {
  int value_status;
  (void)sqlite3_reset(statement);
  (void)sqlite3_clear_bindings(statement);
  value_status = mutation->value_size == 0u
                     ? sqlite3_bind_zeroblob(statement, 4, 0)
                     : sqlite3_bind_blob(statement, 4, mutation->value, (int)mutation->value_size,
                                         SQLITE_TRANSIENT);
  if (sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_blob(statement, 2, mutation->key, (int)mutation->key_size, SQLITE_TRANSIENT) !=
          SQLITE_OK ||
      sqlite3_bind_int64(statement, 3, (sqlite3_int64)mutation->next_revision) != SQLITE_OK ||
      value_status != SQLITE_OK || sqlite3_step(statement) != SQLITE_DONE)
    return flow_sqlite_status(sqlite3_errcode(store->database));
  return TURBO_OK;
}

static int flow_sqlite_record_delete(flow_sqlite_record_store_t *store, sqlite3_stmt *statement,
                                     const turbo_flow_record_mutation_t *mutation) {
  (void)sqlite3_reset(statement);
  (void)sqlite3_clear_bindings(statement);
  if (sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_blob(statement, 2, mutation->key, (int)mutation->key_size, SQLITE_TRANSIENT) !=
          SQLITE_OK ||
      sqlite3_step(statement) != SQLITE_DONE)
    return flow_sqlite_status(sqlite3_errcode(store->database));
  return sqlite3_changes(store->database) == 1 ? TURBO_OK : TURBO_EBUSY;
}

static int flow_sqlite_record_commit(void *ctx, const turbo_flow_record_mutation_t *mutations,
                                     size_t mutation_count) {
  static const char current_sql[] = "SELECT revision,length(value) FROM " FLOW_SQLITE_RECORD_TABLE
                                    " WHERE namespace_name=?1 AND record_key=?2";
  static const char put_sql[] =
      "INSERT INTO " FLOW_SQLITE_RECORD_TABLE
      "(namespace_name,record_key,revision,value) VALUES(?1,?2,?3,?4) "
      "ON CONFLICT(namespace_name,record_key) DO UPDATE SET revision=excluded.revision,"
      "value=excluded.value";
  static const char delete_sql[] =
      "DELETE FROM " FLOW_SQLITE_RECORD_TABLE " WHERE namespace_name=?1 AND record_key=?2";
  flow_sqlite_record_store_t *store = (flow_sqlite_record_store_t *)ctx;
  sqlite3_stmt *current = NULL;
  sqlite3_stmt *put = NULL;
  sqlite3_stmt *delete_record = NULL;
  size_t records;
  size_t bytes;
  int transaction = 0;
  int status;
  int rc;
  if (!store || !store->database) return TURBO_EINVAL;
  rc = flow_sqlite_record_validate_mutations(store, mutations, mutation_count);
  if (rc != TURBO_OK) goto done;
  rc = flow_sqlite_exec(store->database, "BEGIN IMMEDIATE");
  if (rc != TURBO_OK) goto done;
  transaction = 1;
  rc = flow_sqlite_record_stats(store, &records, &bytes);
  if (rc != TURBO_OK) goto rollback;
  if (records > store->max_records || bytes > store->max_bytes) {
    rc = TURBO_ENOSPC;
    goto rollback;
  }
  status = sqlite3_prepare_v2(store->database, current_sql, -1, &current, NULL);
  if (status == SQLITE_OK) status = sqlite3_prepare_v2(store->database, put_sql, -1, &put, NULL);
  if (status == SQLITE_OK)
    status = sqlite3_prepare_v2(store->database, delete_sql, -1, &delete_record, NULL);
  if (status != SQLITE_OK) {
    rc = flow_sqlite_status(status);
    goto rollback;
  }
  for (size_t i = 0u; i < mutation_count; ++i) {
    const turbo_flow_record_mutation_t *mutation = &mutations[i];
    uint64_t revision;
    size_t old_value_size;
    size_t item_bytes;
    size_t next_bytes;
    int found;
    rc = flow_sqlite_record_current(store, current, mutation, &revision, &old_value_size, &found);
    if (rc != TURBO_OK) goto rollback;
    if ((found ? revision : TURBO_FLOW_RECORD_REVISION_ABSENT) != mutation->expected_revision) {
      rc = TURBO_EBUSY;
      goto rollback;
    }
    if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
      if (!found || records == 0u || bytes < mutation->key_size + old_value_size) {
        rc = found ? TURBO_EPROTO : TURBO_EBUSY;
        goto rollback;
      }
      --records;
      bytes -= mutation->key_size + old_value_size;
      rc = flow_sqlite_record_delete(store, delete_record, mutation);
    } else {
      if (flow_sqlite_size_add(mutation->key_size, mutation->value_size, &item_bytes) != TURBO_OK ||
          item_bytes > store->max_item_bytes) {
        rc = TURBO_EFBIG;
        goto rollback;
      }
      if (found) {
        if (bytes < old_value_size ||
            flow_sqlite_size_add(bytes - old_value_size, mutation->value_size, &next_bytes) !=
                TURBO_OK) {
          rc = TURBO_EPROTO;
          goto rollback;
        }
      } else {
        if (records >= store->max_records ||
            flow_sqlite_size_add(bytes, item_bytes, &next_bytes) != TURBO_OK) {
          rc = TURBO_ENOSPC;
          goto rollback;
        }
        ++records;
      }
      if (next_bytes > store->max_bytes) {
        rc = TURBO_ENOSPC;
        goto rollback;
      }
      bytes = next_bytes;
      rc = flow_sqlite_record_put(store, put, mutation);
    }
    if (rc != TURBO_OK) goto rollback;
  }
  rc = flow_sqlite_exec(store->database, "COMMIT");
  if (rc == TURBO_OK) transaction = 0;
  goto done;

rollback:
  (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  transaction = 0;
done:
  if (current) (void)sqlite3_finalize(current);
  if (put) (void)sqlite3_finalize(put);
  if (delete_record) (void)sqlite3_finalize(delete_record);
  if (transaction) (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  if (store && store->mutation_keys_initialized) turbo_hash_map_clear(&store->mutation_keys);
  return rc;
}

static int flow_sqlite_record_config_validate(const turbo_flow_sqlite_record_store_config_t *config,
                                              turbo_flow_record_store_t *out) {
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_SQLITE_RECORD_STORE_CONFIG_VERSION || !out ||
      out->size < sizeof(*out) || out->ctx || !config->database_path || !config->database_path[0] ||
      !config->namespace_name || !config->namespace_name[0] ||
      strlen(config->namespace_name) > TURBO_FLOW_SQLITE_RECORD_STORE_NAMESPACE_MAX ||
      config->busy_timeout_ms < 0 || config->max_records == 0u || config->max_bytes == 0u ||
      config->max_item_bytes == 0u || config->max_item_bytes > config->max_bytes ||
      config->max_key_size == 0u || config->max_key_size > INT_MAX ||
      config->max_value_size == 0u || config->max_value_size > INT_MAX ||
      config->max_batch_size == 0u || config->max_batch_size > FLOW_SQLITE_MAX_BATCH_SIZE ||
      config->max_records > INT64_MAX || config->max_bytes > INT64_MAX ||
      config->max_item_bytes > INT64_MAX)
    return TURBO_EINVAL;
  return TURBO_OK;
}

int turbo_flow_sqlite_record_store_create(const turbo_flow_sqlite_record_store_config_t *config,
                                          turbo_flow_record_store_t *out) {
  flow_sqlite_record_store_t *store = NULL;
  size_t records;
  size_t bytes;
  int process_local;
  int status;
  int rc;
  rc = flow_sqlite_record_config_validate(config, out);
  if (rc != TURBO_OK) return rc;
  process_local = strcmp(config->database_path, ":memory:") == 0;
  store = (flow_sqlite_record_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->database_path = tstr_dup(config->database_path);
  store->namespace_name = tstr_dup(config->namespace_name);
  store->max_records = config->max_records;
  store->max_bytes = config->max_bytes;
  store->max_item_bytes = config->max_item_bytes;
  store->max_key_size = config->max_key_size;
  store->max_value_size = config->max_value_size;
  store->max_batch_size = config->max_batch_size;
  if (!store->database_path || !store->namespace_name) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = turbo_hash_map_init(&store->mutation_keys, sizeof(tstr_v), sizeof(uint8_t),
                           flow_sqlite_record_key_hash, flow_sqlite_record_key_equal, NULL);
  if (rc != TURBO_OK) goto fail;
  store->mutation_keys_initialized = 1;
  rc = turbo_hash_map_reserve(&store->mutation_keys, config->max_batch_size);
  if (rc != TURBO_OK) goto fail;
  status =
      sqlite3_open_v2(store->database_path, &store->database,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (status != SQLITE_OK) {
    rc = flow_sqlite_status(status);
    goto fail;
  }
  if (config->busy_timeout_ms > 0 &&
      sqlite3_busy_timeout(store->database, config->busy_timeout_ms) != SQLITE_OK) {
    rc = flow_sqlite_status(sqlite3_errcode(store->database));
    goto fail;
  }
  rc = flow_sqlite_exec(store->database, process_local ? FLOW_SQLITE_RECORD_MEMORY_PRAGMAS
                                                       : FLOW_SQLITE_RECORD_FILE_PRAGMAS);
  if (rc != TURBO_OK) goto fail;
  rc = flow_sqlite_exec(store->database, FLOW_SQLITE_RECORD_SCHEMA);
  if (rc != TURBO_OK) goto fail;
  rc = flow_sqlite_record_stats(store, &records, &bytes);
  if (rc != TURBO_OK) goto fail;
  if (records > store->max_records || bytes > store->max_bytes) {
    rc = TURBO_ENOSPC;
    goto fail;
  }
  out->api_version = TURBO_FLOW_RECORD_STORE_API_VERSION;
  out->capabilities = TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  if (!process_local) out->capabilities |= TURBO_FLOW_RECORD_STORE_DURABLE;
  out->max_key_size = store->max_key_size;
  out->max_value_size = store->max_value_size;
  out->max_batch_size = store->max_batch_size;
  out->max_records = store->max_records;
  out->ctx = store;
  out->scan = flow_sqlite_record_scan;
  out->commit = flow_sqlite_record_commit;
  return TURBO_OK;

fail:
  if (store) {
    if (store->database) (void)sqlite3_close_v2(store->database);
    if (store->mutation_keys_initialized) turbo_hash_map_destroy(&store->mutation_keys);
    tstr_freep(&store->database_path);
    tstr_freep(&store->namespace_name);
    free(store);
  }
  return rc;
}

int turbo_flow_sqlite_record_store_close(turbo_flow_record_store_t *store) {
  flow_sqlite_record_store_t *sqlite_store;
  int status;
  if (!store || store->size < sizeof(*store) || !store->ctx) return TURBO_EINVAL;
  sqlite_store = (flow_sqlite_record_store_t *)store->ctx;
  status = sqlite_store->database ? sqlite3_close_v2(sqlite_store->database) : SQLITE_OK;
  sqlite_store->database = NULL;
  if (sqlite_store->mutation_keys_initialized) turbo_hash_map_destroy(&sqlite_store->mutation_keys);
  tstr_freep(&sqlite_store->database_path);
  tstr_freep(&sqlite_store->namespace_name);
  free(sqlite_store);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  return status == SQLITE_OK ? TURBO_OK : flow_sqlite_status(status);
}
