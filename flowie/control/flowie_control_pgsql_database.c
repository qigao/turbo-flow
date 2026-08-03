#include "flowie_control_pgsql_database_internal.h"

#include "flowie_control_credential_internal.h"
#include "libpq-fe.h"
#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_PGSQL_MIGRATION_LOCK_SEED "194728351"
#define FLOWIE_CONTROL_PGSQL_SCHEMA_FINGERPRINT "flowie-control-schema-v1-20260727"
#define FLOWIE_CONTROL_PGSQL_STRINGIFY_VALUE(value) #value
#define FLOWIE_CONTROL_PGSQL_STRINGIFY(value) FLOWIE_CONTROL_PGSQL_STRINGIFY_VALUE(value)

struct flowie_control_pgsql_database_s {
  PGconn *connection;
  tstr_t conninfo;
  tstr_t schema_name;
  uint32_t schema_version;
};

typedef enum flowie_control_pgsql_pool_slot_state_e {
  FLOWIE_CONTROL_PGSQL_POOL_SLOT_AVAILABLE = 0,
  FLOWIE_CONTROL_PGSQL_POOL_SLOT_LEASED = 1,
  FLOWIE_CONTROL_PGSQL_POOL_SLOT_CLEANING = 2,
  FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD = 3
} flowie_control_pgsql_pool_slot_state_t;

typedef struct flowie_control_pgsql_pool_slot_s {
  flowie_control_pgsql_database_t *database;
  flowie_control_pgsql_pool_slot_state_t state;
  uint64_t generation;
} flowie_control_pgsql_pool_slot_t;

struct flowie_control_pgsql_pool_s {
  flowie_control_pgsql_pool_slot_t *slots;
  size_t capacity;
  size_t leased_count;
  size_t acquire_waiters;
  uint64_t acquisition_timeouts;
  uint64_t cleanup_failures;
  int acquire_timeout_ms;
  int closing;
  int reconnect_requested;
  int reconnect_in_progress;
  int reconnect_thread_started;
  tstr_t conninfo;
  tstr_t password;
  tstr_t schema_name;
  flowie_control_pgsql_database_config_t database_config;
  turbo_thread_t reconnect_thread;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
};

static int flowie_control_pgsql_schema_name_valid(const char *name) {
  size_t length;
  if (!name) return 0;
  length = strnlen(name, FLOWIE_CONTROL_PGSQL_SCHEMA_NAME_MAX + 1u);
  if (length == 0u || length > FLOWIE_CONTROL_PGSQL_SCHEMA_NAME_MAX ||
      !((name[0] >= 'a' && name[0] <= 'z') || name[0] == '_'))
    return 0;
  for (size_t index = 1u; index < length; ++index) {
    char byte = name[index];
    if (!((byte >= 'a' && byte <= 'z') || (byte >= '0' && byte <= '9') || byte == '_')) return 0;
  }
  return 1;
}

static int
flowie_control_pgsql_database_config_valid(const flowie_control_pgsql_database_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->version != FLOWIE_CONTROL_PGSQL_DATABASE_VERSION || !config->conninfo ||
      !config->conninfo[0] ||
      strnlen(config->conninfo, FLOWIE_CONTROL_PGSQL_CONNINFO_MAX + 1u) >
          FLOWIE_CONTROL_PGSQL_CONNINFO_MAX ||
      (config->password && strnlen(config->password, FLOWIE_CONTROL_PGSQL_CONNINFO_MAX + 1u) >
                               FLOWIE_CONTROL_PGSQL_CONNINFO_MAX) ||
      !flowie_control_pgsql_schema_name_valid(config->schema_name) ||
      config->connect_timeout_seconds <= 0 || config->connect_timeout_seconds > 60 ||
      config->statement_timeout_ms <= 0 ||
      config->statement_timeout_ms > FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS ||
      config->lock_timeout_ms <= 0 ||
      config->lock_timeout_ms > FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS ||
      (config->require_tls != 0 && config->require_tls != 1) ||
      (config->schema_mode != FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE &&
       config->schema_mode != FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE))
    return 0;
  return 1;
}

int flowie_control_pgsql_sqlstate_status(const char *sqlstate) {
  if (!sqlstate || strnlen(sqlstate, 6u) != 5u) return TURBO_EIO;
  if (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0 ||
      strcmp(sqlstate, "55P03") == 0)
    return TURBO_EBUSY;
  if (strcmp(sqlstate, "57014") == 0) return TURBO_ETIMEDOUT;
  if (strcmp(sqlstate, "23503") == 0) return TURBO_EBUSY;
  if (strcmp(sqlstate, "23505") == 0) return TURBO_EALREADY;
  if (strncmp(sqlstate, "23", 2u) == 0 || strncmp(sqlstate, "22", 2u) == 0) return TURBO_EINVAL;
  if (strncmp(sqlstate, "08", 2u) == 0 || strcmp(sqlstate, "57P01") == 0) return TURBO_EIO;
  return TURBO_EIO;
}

int flowie_control_pgsql_public_conninfo_validate(const char *conninfo) {
  static const char *const rejected[] = {"password", "passfile", "sslpassword", "service",
                                         "servicefile"};
  PQconninfoOption *options;
  char *message = NULL;
  int sslmode_valid = 0;
  int rc = TURBO_OK;
  if (!conninfo || !conninfo[0] ||
      strnlen(conninfo, FLOWIE_CONTROL_PGSQL_CONNINFO_MAX + 1u) > FLOWIE_CONTROL_PGSQL_CONNINFO_MAX)
    return TURBO_EINVAL;
  options = PQconninfoParse(conninfo, &message);
  if (!options) {
    if (message) PQfreemem(message);
    return TURBO_EINVAL;
  }
  for (PQconninfoOption *option = options; option->keyword; ++option) {
    if (strcmp(option->keyword, "sslmode") == 0 && option->val &&
        strcmp(option->val, "verify-full") == 0)
      sslmode_valid = 1;
    for (size_t index = 0u; index < sizeof(rejected) / sizeof(rejected[0]); ++index) {
      if (strcmp(option->keyword, rejected[index]) == 0 && option->val && option->val[0]) {
        rc = TURBO_EPERM;
        break;
      }
    }
    if (rc != TURBO_OK) break;
  }
  if (rc == TURBO_OK && !sslmode_valid) rc = TURBO_EPERM;
  PQconninfoFree(options);
  if (message) PQfreemem(message);
  return rc;
}

int flowie_control_pgsql_result_status(PGresult *result, int expected_status) {
  const char *sqlstate;
  if (result && PQresultStatus(result) == (ExecStatusType)expected_status) return TURBO_OK;
  sqlstate = result ? PQresultErrorField(result, PG_DIAG_SQLSTATE) : NULL;
  return flowie_control_pgsql_sqlstate_status(sqlstate);
}

static int flowie_control_pgsql_exec(PGconn *connection, const char *sql, ExecStatusType expected) {
  PGresult *result;
  int rc;
  if (!connection || !sql) return TURBO_EINVAL;
  result = PQexec(connection, sql);
  rc = flowie_control_pgsql_result_status(result, expected);
  if (result) PQclear(result);
  return rc;
}

static int flowie_control_pgsql_set_session(PGconn *connection, const char *key,
                                            const char *value) {
  static const char sql[] = "SELECT pg_catalog.set_config($1,$2,false)";
  const char *values[2] = {key, value};
  PGresult *result;
  int rc;
  result = PQexecParams(connection, sql, 2, NULL, values, NULL, NULL, 0);
  rc = flowie_control_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (result) PQclear(result);
  return rc;
}

static int flowie_control_pgsql_timeout_text(int timeout_ms, char output[32]) {
  int length = snprintf(output, 32u, "%dms", timeout_ms);
  return length > 0 && length < 32 ? TURBO_OK : TURBO_ERANGE;
}

static int flowie_control_pgsql_session_prepare(PGconn *connection, int statement_timeout_ms,
                                                int lock_timeout_ms) {
  char statement_timeout[32];
  char lock_timeout[32];
  int rc = flowie_control_pgsql_timeout_text(statement_timeout_ms, statement_timeout);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_timeout_text(lock_timeout_ms, lock_timeout);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_set_session(connection, "search_path", "");
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_set_session(connection, "statement_timeout", statement_timeout);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_set_session(connection, "lock_timeout", lock_timeout);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_set_session(connection, "idle_in_transaction_session_timeout",
                                          statement_timeout);
  return rc;
}

static tstr_t flowie_control_pgsql_schema_sql(const char *schema) {
  tstr_t sql = tstr_new();
  if (!sql) return NULL;
#define FLOWIE_CONTROL_PGSQL_APPEND(...)                                                           \
  do {                                                                                             \
    tstr_t next = tstr_cat_fmt(sql, __VA_ARGS__);                                                  \
    if (!next) {                                                                                   \
      tstr_free(sql);                                                                              \
      return NULL;                                                                                 \
    }                                                                                              \
    sql = next;                                                                                    \
  } while (0)
  FLOWIE_CONTROL_PGSQL_APPEND("CREATE SCHEMA IF NOT EXISTS %s;", schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.schema_version("
      "singleton SMALLINT PRIMARY KEY CHECK(singleton=1),"
      "version INTEGER NOT NULL CHECK(version>0),"
      "fingerprint TEXT NOT NULL,"
      "applied_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp());",
      schema);
  FLOWIE_CONTROL_PGSQL_APPEND("INSERT INTO %s.schema_version(singleton,version,fingerprint)"
                              " VALUES(1,%u,'" FLOWIE_CONTROL_PGSQL_SCHEMA_FINGERPRINT "')"
                              " ON CONFLICT(singleton) DO NOTHING;",
                              schema, FLOWIE_CONTROL_PGSQL_SCHEMA_VERSION);
  FLOWIE_CONTROL_PGSQL_APPEND("CREATE TABLE IF NOT EXISTS %s.meta("
                              "singleton SMALLINT PRIMARY KEY CHECK(singleton=1),"
                              "revision BIGINT NOT NULL CHECK(revision>=0));"
                              "INSERT INTO %s.meta(singleton,revision) VALUES(1,0)"
                              " ON CONFLICT(singleton) DO NOTHING;",
                              schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND("CREATE TABLE IF NOT EXISTS %s.root_group("
                              "root_group_id TEXT PRIMARY KEY);",
                              schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.user_account("
      "root_group_id TEXT NOT NULL,principal_id TEXT NOT NULL,principal_type TEXT NOT NULL,"
      "enabled BOOLEAN NOT NULL,revision BIGINT NOT NULL CHECK(revision>0),"
      "created_at BIGINT NOT NULL CHECK(created_at>0),updated_at BIGINT NOT NULL "
      "CHECK(updated_at>0),"
      "PRIMARY KEY(root_group_id,principal_id),"
      "FOREIGN KEY(root_group_id) REFERENCES %s.root_group(root_group_id));",
      schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.security_group("
      "root_group_id TEXT NOT NULL,group_id TEXT NOT NULL,parent_group_id TEXT,"
      "depth INTEGER NOT NULL CHECK(depth>=0 AND depth<=" FLOWIE_CONTROL_PGSQL_STRINGIFY(
          FLOWIE_CONTROL_GROUP_MAX_DEPTH) "),enabled BOOLEAN NOT NULL,"
                                          "revision BIGINT NOT NULL CHECK(revision>0),created_at "
                                          "BIGINT NOT NULL CHECK(created_at>0),"
                                          "updated_at BIGINT NOT NULL CHECK(updated_at>0),PRIMARY "
                                          "KEY(root_group_id,group_id),"
                                          "FOREIGN KEY(root_group_id) REFERENCES "
                                          "%s.root_group(root_group_id),"
                                          "FOREIGN KEY(root_group_id,parent_group_id) REFERENCES "
                                          "%s.security_group(root_group_id,group_id),"
                                          "CHECK((group_id=root_group_id AND parent_group_id IS "
                                          "NULL AND depth=0) OR"
                                          "(group_id<>root_group_id AND parent_group_id IS NOT "
                                          "NULL AND depth>0)));",
      schema, schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.security_role("
      "root_group_id TEXT NOT NULL,role_id TEXT NOT NULL,enabled BOOLEAN NOT NULL,"
      "revision BIGINT NOT NULL CHECK(revision>0),created_at BIGINT NOT NULL CHECK(created_at>0),"
      "updated_at BIGINT NOT NULL CHECK(updated_at>0),PRIMARY KEY(root_group_id,role_id),"
      "FOREIGN KEY(root_group_id) REFERENCES %s.root_group(root_group_id));",
      schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.credential("
      "root_group_id TEXT NOT NULL,principal_id TEXT NOT NULL,"
      "kdf_algorithm INTEGER NOT NULL CHECK(kdf_algorithm=" FLOWIE_CONTROL_PGSQL_STRINGIFY(
          FLOWIE_CONTROL_CREDENTIAL_KDF_ARGON2ID) "),"
                                                  "memory_blocks INTEGER NOT NULL "
                                                  "CHECK(memory_blocks>0),passes INTEGER NOT NULL "
                                                  "CHECK(passes>0),"
                                                  "lanes INTEGER NOT NULL CHECK(lanes>0),"
                                                  "salt BYTEA NOT NULL "
                                                  "CHECK(octet_length(salt)"
                                                  "=" FLOWIE_CONTROL_PGSQL_STRINGIFY(
                                                      FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE) "),"
                                                                                           "verifie"
                                                                                           "r "
                                                                                           "BYTEA "
                                                                                           "NOT "
                                                                                           "NULL "
                                                                                           "CHECK("
                                                                                           "octet_"
                                                                                           "length("
                                                                                           "verifie"
                                                                                           "r)"
                                                                                           "=" FLOWIE_CONTROL_PGSQL_STRINGIFY(
                                                                                               FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE) "),enabled BOOLEAN NOT NULL,"
                                                                                                                                        "revision BIGINT NOT NULL CHECK(revision>0),created_at BIGINT NOT NULL CHECK(created_at>0),"
                                                                                                                                        "updated_at BIGINT NOT NULL CHECK(updated_at>0),PRIMARY KEY(root_group_id,principal_id),"
                                                                                                                                        "FOREIGN KEY(root_group_id,principal_id) REFERENCES %s.user_account(root_group_id,principal_id));",
      schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.membership("
      "root_group_id TEXT NOT NULL,principal_id TEXT NOT NULL,group_id TEXT NOT NULL,"
      "revision BIGINT NOT NULL CHECK(revision>0),created_at BIGINT NOT NULL CHECK(created_at>0),"
      "PRIMARY KEY(root_group_id,principal_id,group_id),"
      "FOREIGN KEY(root_group_id,principal_id) REFERENCES "
      "%s.user_account(root_group_id,principal_id),"
      "FOREIGN KEY(root_group_id,group_id) REFERENCES %s.security_group(root_group_id,group_id),"
      "CHECK(group_id<>root_group_id));",
      schema, schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.user_role("
      "root_group_id TEXT NOT NULL,principal_id TEXT NOT NULL,role_id TEXT NOT NULL,"
      "revision BIGINT NOT NULL CHECK(revision>0),created_at BIGINT NOT NULL CHECK(created_at>0),"
      "PRIMARY KEY(root_group_id,principal_id,role_id),"
      "FOREIGN KEY(root_group_id,principal_id) REFERENCES "
      "%s.user_account(root_group_id,principal_id),"
      "FOREIGN KEY(root_group_id,role_id) REFERENCES %s.security_role(root_group_id,role_id));",
      schema, schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.audit("
      "request_id TEXT PRIMARY KEY,actor TEXT NOT NULL,operation TEXT NOT NULL,"
      "root_group_id TEXT NOT NULL,target_id TEXT NOT NULL,target_detail TEXT NOT NULL,"
      "result_revision BIGINT NOT NULL CHECK(result_revision>0),"
      "occurred_at BIGINT NOT NULL CHECK(occurred_at>0),"
      "FOREIGN KEY(root_group_id) REFERENCES %s.root_group(root_group_id));"
      "CREATE INDEX IF NOT EXISTS audit_root_revision_idx"
      " ON %s.audit(root_group_id,result_revision);",
      schema, schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.policy_draft("
      "root_group_id TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0 AND ordinal<4096),"
      "rule_line TEXT NOT NULL CHECK(length(rule_line)>0 AND length(rule_line)<=2047),"
      "revision BIGINT NOT NULL CHECK(revision>0),updated_at BIGINT NOT NULL CHECK(updated_at>0),"
      "PRIMARY KEY(root_group_id,ordinal),"
      "FOREIGN KEY(root_group_id) REFERENCES %s.root_group(root_group_id));",
      schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.acl_bundle("
      "namespace_name TEXT PRIMARY KEY,policy_version BIGINT NOT NULL CHECK(policy_version>0),"
      "expires_at BIGINT NOT NULL CHECK(expires_at>=0));"
      "CREATE TABLE IF NOT EXISTS %s.acl_rule("
      "namespace_name TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0),"
      "rule_line TEXT NOT NULL CHECK(length(rule_line)>0),PRIMARY KEY(namespace_name,ordinal),"
      "FOREIGN KEY(namespace_name) REFERENCES %s.acl_bundle(namespace_name) ON DELETE CASCADE);",
      schema, schema, schema);
  FLOWIE_CONTROL_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.policy_publish_result("
      "request_id TEXT PRIMARY KEY,policy_version BIGINT NOT NULL CHECK(policy_version>0),"
      "FOREIGN KEY(request_id) REFERENCES %s.audit(request_id));",
      schema, schema);
#undef FLOWIE_CONTROL_PGSQL_APPEND
  return sql;
}

static int flowie_control_pgsql_parse_version(PGresult *result, uint32_t *version_out) {
  char *end = NULL;
  unsigned long value;
  const char *text;
  if (!result || !version_out || PQresultStatus(result) != PGRES_TUPLES_OK ||
      PQntuples(result) != 1 || PQnfields(result) != 2 || PQgetisnull(result, 0, 0) ||
      PQgetisnull(result, 0, 1))
    return TURBO_EPROTO;
  if (strcmp(PQgetvalue(result, 0, 1), FLOWIE_CONTROL_PGSQL_SCHEMA_FINGERPRINT) != 0)
    return TURBO_EPROTO;
  text = PQgetvalue(result, 0, 0);
  errno = 0;
  value = strtoul(text, &end, 10);
  if (errno != 0 || !end || *end != '\0' || value == 0u || value > UINT32_MAX) return TURBO_EPROTO;
  *version_out = (uint32_t)value;
  return TURBO_OK;
}

static int flowie_control_pgsql_schema_version_read(PGconn *connection, const char *schema,
                                                    uint32_t *version_out) {
  tstr_t sql = tstr_new();
  tstr_t next;
  PGresult *result = NULL;
  int rc;
  if (!sql) return TURBO_ENOMEM;
  next = tstr_cat_fmt(
      sql, "SELECT version::text,fingerprint FROM %s.schema_version WHERE singleton=1", schema);
  if (!next) {
    tstr_free(sql);
    return TURBO_ENOMEM;
  }
  sql = next;
  result = PQexec(connection, sql);
  rc = flowie_control_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_version(result, version_out);
  if (result) PQclear(result);
  tstr_free(sql);
  return rc;
}

static int flowie_control_pgsql_schema_objects_validate(PGconn *connection,
                                                        const char *schema_name) {
  static const char sql[] =
      "SELECT count(*)::text FROM pg_catalog.pg_class c"
      " JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace"
      " WHERE n.nspname=$1 AND c.relkind IN('r','p') AND c.relname IN("
      "'schema_version','meta','root_group','user_account','security_group','security_role',"
      "'credential','membership','user_role','audit','policy_draft','acl_bundle','acl_rule',"
      "'policy_publish_result')";
  static const unsigned long expected_count = 14u;
  const char *values[1] = {schema_name};
  PGresult *result;
  char *end = NULL;
  unsigned long count;
  int rc;
  result = PQexecParams(connection, sql, 1, NULL, values, NULL, NULL, 0);
  rc = flowie_control_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK) {
    if (PQntuples(result) != 1 || PQnfields(result) != 1 || PQgetisnull(result, 0, 0))
      rc = TURBO_EPROTO;
    else {
      errno = 0;
      count = strtoul(PQgetvalue(result, 0, 0), &end, 10);
      if (errno != 0 || !end || *end != '\0' || count != expected_count) rc = TURBO_EPROTO;
    }
  }
  if (result) PQclear(result);
  return rc;
}

static int flowie_control_pgsql_schema_prepare(PGconn *connection, const char *schema,
                                               flowie_control_pgsql_schema_mode_t mode,
                                               uint32_t *version_out) {
  static const char lock_sql[] =
      "SELECT pg_catalog.pg_advisory_xact_lock("
      "pg_catalog.hashtextextended($1," FLOWIE_CONTROL_PGSQL_MIGRATION_LOCK_SEED "))";
  const char *lock_values[1] = {schema};
  tstr_t sql = NULL;
  PGresult *result = NULL;
  int transaction = 0;
  int rc;
  if (mode == FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE)
    return flowie_control_pgsql_schema_version_read(connection, schema, version_out);
  rc = flowie_control_pgsql_exec(connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  transaction = 1;
  result = PQexecParams(connection, lock_sql, 1, NULL, lock_values, NULL, NULL, 0);
  rc = flowie_control_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (result) PQclear(result);
  if (rc != TURBO_OK) goto done;
  sql = flowie_control_pgsql_schema_sql(schema);
  if (!sql) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flowie_control_pgsql_exec(connection, sql, PGRES_COMMAND_OK);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_schema_version_read(connection, schema, version_out);
  if (rc == TURBO_OK && *version_out != FLOWIE_CONTROL_PGSQL_SCHEMA_VERSION) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    rc = flowie_control_pgsql_exec(connection, "COMMIT", PGRES_COMMAND_OK);
    if (rc == TURBO_OK) transaction = 0;
  }
done:
  if (transaction) (void)flowie_control_pgsql_exec(connection, "ROLLBACK", PGRES_COMMAND_OK);
  tstr_free(sql);
  return rc;
}

int flowie_control_pgsql_database_open(const flowie_control_pgsql_database_config_t *config,
                                       flowie_control_pgsql_database_t **out) {
  flowie_control_pgsql_database_t *database;
  const char *keywords[] = {
      "dbname", "password", "connect_timeout", "application_name", "target_session_attrs", NULL};
  const char *values[] = {NULL, NULL, NULL, "flowie-control", "read-write", NULL};
  char connect_timeout[16];
  char *escaped_schema = NULL;
  int length;
  int rc;
  if (out) *out = NULL;
  if (!out || !flowie_control_pgsql_database_config_valid(config)) return TURBO_EINVAL;
  length =
      snprintf(connect_timeout, sizeof(connect_timeout), "%d", config->connect_timeout_seconds);
  if (length <= 0 || (size_t)length >= sizeof(connect_timeout)) return TURBO_ERANGE;
  database = (flowie_control_pgsql_database_t *)calloc(1u, sizeof(*database));
  if (!database) return TURBO_ENOMEM;
  database->conninfo = tstr_dup(config->conninfo);
  database->schema_name = tstr_dup(config->schema_name);
  if (!database->conninfo || !database->schema_name) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  values[0] = database->conninfo;
  values[1] = config->password;
  values[2] = connect_timeout;
  database->connection = PQconnectdbParams(keywords, values, 1);
  if (!database->connection || PQstatus(database->connection) != CONNECTION_OK) {
    rc = TURBO_EIO;
    goto fail;
  }
  if (config->require_tls) {
    PQconninfoOption *options = PQconninfo(database->connection);
    const char *sslmode = NULL;
    if (!options) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    for (PQconninfoOption *option = options; option->keyword; ++option) {
      if (strcmp(option->keyword, "sslmode") == 0) {
        sslmode = option->val;
        break;
      }
    }
    if (!sslmode || strcmp(sslmode, "verify-full") != 0 || !PQsslInUse(database->connection))
      rc = TURBO_EPERM;
    else rc = TURBO_OK;
    PQconninfoFree(options);
    if (rc != TURBO_OK) goto fail;
  }
  rc = flowie_control_pgsql_session_prepare(database->connection, config->statement_timeout_ms,
                                            config->lock_timeout_ms);
  if (rc != TURBO_OK) goto fail;
  escaped_schema = PQescapeIdentifier(database->connection, database->schema_name,
                                      tstr_len(database->schema_name));
  if (!escaped_schema) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = flowie_control_pgsql_schema_prepare(database->connection, escaped_schema,
                                           config->schema_mode, &database->schema_version);
  PQfreemem(escaped_schema);
  escaped_schema = NULL;
  if (rc != TURBO_OK) goto fail;
  if (database->schema_version != FLOWIE_CONTROL_PGSQL_SCHEMA_VERSION) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  rc = flowie_control_pgsql_schema_objects_validate(database->connection, database->schema_name);
  if (rc != TURBO_OK) goto fail;
  *out = database;
  return TURBO_OK;

fail:
  if (escaped_schema) PQfreemem(escaped_schema);
  flowie_control_pgsql_database_destroy(database);
  return rc;
}

void flowie_control_pgsql_database_destroy(flowie_control_pgsql_database_t *database) {
  if (!database) return;
  if (database->connection) PQfinish(database->connection);
  if (database->conninfo) crypto_wipe(database->conninfo, tstr_len(database->conninfo));
  tstr_freep(&database->conninfo);
  tstr_freep(&database->schema_name);
  free(database);
}

uint32_t
flowie_control_pgsql_database_schema_version(const flowie_control_pgsql_database_t *database) {
  return database ? database->schema_version : 0u;
}

static void flowie_control_pgsql_pool_lease_reset(flowie_control_pgsql_pool_lease_t *lease) {
  if (!lease) return;
  lease->owner = NULL;
  lease->connection = NULL;
  lease->slot = 0u;
  lease->generation = 0u;
}

static void flowie_control_pgsql_pool_storage_destroy(flowie_control_pgsql_pool_t *pool) {
  if (!pool) return;
  if (pool->reconnect_thread_started) {
    turbo_mutex_lock(&pool->mutex);
    pool->closing = 1;
    turbo_cond_broadcast(&pool->changed);
    turbo_mutex_unlock(&pool->mutex);
    (void)turbo_thread_join(&pool->reconnect_thread);
    turbo_thread_destroy(&pool->reconnect_thread);
  }
  if (pool->slots) {
    for (size_t index = 0u; index < pool->capacity; ++index)
      flowie_control_pgsql_database_destroy(pool->slots[index].database);
  }
  free(pool->slots);
  if (pool->conninfo) crypto_wipe(pool->conninfo, tstr_len(pool->conninfo));
  tstr_freep(&pool->conninfo);
  if (pool->password) crypto_wipe(pool->password, tstr_len(pool->password));
  tstr_freep(&pool->password);
  tstr_freep(&pool->schema_name);
  turbo_cond_destroy(&pool->changed);
  turbo_mutex_destroy(&pool->mutex);
  free(pool);
}

static int
flowie_control_pgsql_pool_config_valid(const flowie_control_pgsql_pool_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->version != FLOWIE_CONTROL_PGSQL_POOL_VERSION || config->capacity == 0u ||
      config->capacity > FLOWIE_CONTROL_PGSQL_POOL_CAPACITY_MAX ||
      config->acquire_timeout_ms <= 0 ||
      config->acquire_timeout_ms > FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS ||
      !flowie_control_pgsql_database_config_valid(&config->database))
    return 0;
  return 1;
}

static void flowie_control_pgsql_pool_reconnect_worker(void *ctx);

int flowie_control_pgsql_pool_create(const flowie_control_pgsql_pool_config_t *config,
                                     flowie_control_pgsql_pool_t **out) {
  flowie_control_pgsql_pool_t *pool;
  flowie_control_pgsql_database_config_t slot_config;
  int rc = TURBO_OK;
  if (out) *out = NULL;
  if (!out || !flowie_control_pgsql_pool_config_valid(config)) return TURBO_EINVAL;
  pool = (flowie_control_pgsql_pool_t *)calloc(1u, sizeof(*pool));
  if (!pool) return TURBO_ENOMEM;
  turbo_mutex_init(&pool->mutex);
  turbo_cond_init(&pool->changed);
  pool->capacity = config->capacity;
  pool->acquire_timeout_ms = config->acquire_timeout_ms;
  pool->conninfo = config->database.conninfo ? tstr_dup(config->database.conninfo) : NULL;
  pool->password = config->database.password ? tstr_dup(config->database.password) : NULL;
  pool->schema_name = config->database.schema_name ? tstr_dup(config->database.schema_name) : NULL;
  if (!pool->conninfo || (config->database.password && !pool->password) || !pool->schema_name) {
    rc = config->database.conninfo && config->database.schema_name ? TURBO_ENOMEM : TURBO_EINVAL;
    goto fail;
  }
  pool->slots = (flowie_control_pgsql_pool_slot_t *)calloc(pool->capacity, sizeof(*pool->slots));
  if (!pool->slots) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  pool->database_config = config->database;
  pool->database_config.conninfo = pool->conninfo;
  pool->database_config.password = pool->password;
  pool->database_config.schema_name = pool->schema_name;
  slot_config = pool->database_config;
  for (size_t index = 0u; index < pool->capacity; ++index) {
    if (index != 0u) slot_config.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE;
    rc = flowie_control_pgsql_database_open(&slot_config, &pool->slots[index].database);
    if (rc != TURBO_OK) goto fail;
    pool->slots[index].state = FLOWIE_CONTROL_PGSQL_POOL_SLOT_AVAILABLE;
  }
  rc = turbo_thread_create(&pool->reconnect_thread, flowie_control_pgsql_pool_reconnect_worker,
                           pool);
  if (rc != TURBO_OK) goto fail;
  pool->reconnect_thread_started = 1;
  *out = pool;
  return TURBO_OK;

fail:
  flowie_control_pgsql_pool_storage_destroy(pool);
  return rc;
}

static int flowie_control_pgsql_pool_wait(flowie_control_pgsql_pool_t *pool, uint64_t deadline_ms) {
  uint64_t now_ms = turbo_monotonic_ms();
  uint64_t remaining_ms;
  if (now_ms >= deadline_ms) return TURBO_ETIMEDOUT;
  remaining_ms = deadline_ms - now_ms;
  if (remaining_ms > (uint64_t)FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS)
    remaining_ms = FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS;
  return turbo_cond_timedwait(&pool->changed, &pool->mutex, remaining_ms * 1000000ULL) == 0
             ? TURBO_OK
             : TURBO_ETIMEDOUT;
}

static void flowie_control_pgsql_pool_reconnect_worker(void *ctx) {
  flowie_control_pgsql_pool_t *pool = (flowie_control_pgsql_pool_t *)ctx;
  for (;;) {
    flowie_control_pgsql_pool_slot_t *slot;
    flowie_control_pgsql_database_t *previous_database;
    flowie_control_pgsql_database_t *reopened_database = NULL;
    flowie_control_pgsql_database_config_t reopen_config;
    size_t slot_index;
    int rc;

    turbo_mutex_lock(&pool->mutex);
    while (!pool->closing && !pool->reconnect_requested)
      turbo_cond_wait(&pool->changed, &pool->mutex);
    if (pool->closing) {
      pool->reconnect_requested = 0;
      turbo_cond_broadcast(&pool->changed);
      turbo_mutex_unlock(&pool->mutex);
      return;
    }
    pool->reconnect_requested = 0;
    for (slot_index = 0u; slot_index < pool->capacity; ++slot_index) {
      if (pool->slots[slot_index].state == FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD) break;
    }
    if (slot_index == pool->capacity) {
      turbo_mutex_unlock(&pool->mutex);
      continue;
    }
    slot = &pool->slots[slot_index];
    previous_database = slot->database;
    slot->database = NULL;
    slot->state = FLOWIE_CONTROL_PGSQL_POOL_SLOT_CLEANING;
    pool->reconnect_in_progress = 1;
    reopen_config = pool->database_config;
    reopen_config.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE;
    turbo_mutex_unlock(&pool->mutex);

    flowie_control_pgsql_database_destroy(previous_database);
    rc = flowie_control_pgsql_database_open(&reopen_config, &reopened_database);

    turbo_mutex_lock(&pool->mutex);
    slot->database = reopened_database;
    slot->state = rc == TURBO_OK ? FLOWIE_CONTROL_PGSQL_POOL_SLOT_AVAILABLE
                                 : FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD;
    pool->reconnect_in_progress = 0;
    if (rc != TURBO_OK) {
      ++pool->cleanup_failures;
    } else if (!pool->closing) {
      for (slot_index = 0u; slot_index < pool->capacity; ++slot_index) {
        if (pool->slots[slot_index].state == FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD) {
          pool->reconnect_requested = 1;
          break;
        }
      }
    }
    turbo_cond_broadcast(&pool->changed);
    turbo_mutex_unlock(&pool->mutex);
  }
}

int flowie_control_pgsql_pool_acquire(flowie_control_pgsql_pool_t *pool,
                                      flowie_control_pgsql_pool_lease_t *lease) {
  uint64_t started_ms;
  uint64_t deadline_ms;
  int reconnect_attempted = 0;
  int rc = TURBO_OK;
  if (!pool || !lease) return TURBO_EINVAL;
  flowie_control_pgsql_pool_lease_reset(lease);
  started_ms = turbo_monotonic_ms();
  deadline_ms = started_ms + (uint64_t)pool->acquire_timeout_ms;
  if (deadline_ms < started_ms) deadline_ms = UINT64_MAX;
  turbo_mutex_lock(&pool->mutex);
  if (pool->closing) {
    turbo_mutex_unlock(&pool->mutex);
    return TURBO_ESHUTDOWN;
  }
  ++pool->acquire_waiters;
  for (;;) {
    size_t healthy_count = 0u;
    int has_dead_slot = 0;
    if (pool->closing) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    for (size_t index = 0u; index < pool->capacity; ++index) {
      flowie_control_pgsql_pool_slot_t *slot = &pool->slots[index];
      if (slot->state != FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD) {
        ++healthy_count;
      } else {
        has_dead_slot = 1;
      }
      if (slot->state != FLOWIE_CONTROL_PGSQL_POOL_SLOT_AVAILABLE) continue;
      slot->state = FLOWIE_CONTROL_PGSQL_POOL_SLOT_LEASED;
      ++slot->generation;
      if (slot->generation == 0u) ++slot->generation;
      ++pool->leased_count;
      lease->owner = pool;
      lease->connection = slot->database->connection;
      lease->slot = index;
      lease->generation = slot->generation;
      rc = TURBO_OK;
      goto done;
    }
    if (has_dead_slot && !reconnect_attempted) {
      if (!pool->reconnect_requested && !pool->reconnect_in_progress) {
        pool->reconnect_requested = 1;
        turbo_cond_broadcast(&pool->changed);
      }
      reconnect_attempted = 1;
    }
    if (healthy_count == 0u && !pool->reconnect_requested && !pool->reconnect_in_progress) {
      rc = TURBO_EIO;
      break;
    }
    rc = flowie_control_pgsql_pool_wait(pool, deadline_ms);
    if (rc != TURBO_OK && turbo_monotonic_ms() >= deadline_ms) break;
  }

done:
  --pool->acquire_waiters;
  if (rc == TURBO_ETIMEDOUT) ++pool->acquisition_timeouts;
  if (pool->closing) turbo_cond_broadcast(&pool->changed);
  turbo_mutex_unlock(&pool->mutex);
  return rc;
}

PGconn *flowie_control_pgsql_pool_lease_connection(const flowie_control_pgsql_pool_lease_t *lease) {
  return lease ? lease->connection : NULL;
}

int flowie_control_pgsql_pool_stats(flowie_control_pgsql_pool_t *pool,
                                    flowie_control_pgsql_pool_stats_t *out) {
  flowie_control_pgsql_pool_stats_t stats = FLOWIE_CONTROL_PGSQL_POOL_STATS_INIT;
  if (!pool || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  turbo_mutex_lock(&pool->mutex);
  stats.capacity = pool->capacity;
  stats.leased = pool->leased_count;
  stats.waiters = pool->acquire_waiters;
  stats.acquisition_timeouts = pool->acquisition_timeouts;
  stats.cleanup_failures = pool->cleanup_failures;
  stats.closing = pool->closing;
  for (size_t index = 0u; index < pool->capacity; ++index) {
    if (pool->slots[index].state != FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD) ++stats.healthy;
    if (pool->slots[index].state == FLOWIE_CONTROL_PGSQL_POOL_SLOT_AVAILABLE) ++stats.available;
  }
  turbo_mutex_unlock(&pool->mutex);
  *out = stats;
  return TURBO_OK;
}

const char *flowie_control_pgsql_pool_schema_name(const flowie_control_pgsql_pool_t *pool) {
  return pool ? pool->schema_name : NULL;
}

static int flowie_control_pgsql_pool_connection_clean(flowie_control_pgsql_pool_t *pool,
                                                      flowie_control_pgsql_pool_slot_t *slot) {
  PGTransactionStatusType transaction_status;
  int rc = TURBO_OK;
  if (!slot->database || !slot->database->connection) return TURBO_EIO;
  transaction_status = PQtransactionStatus(slot->database->connection);
  if (transaction_status == PQTRANS_INTRANS || transaction_status == PQTRANS_INERROR)
    rc = flowie_control_pgsql_exec(slot->database->connection, "ROLLBACK", PGRES_COMMAND_OK);
  else if (transaction_status != PQTRANS_IDLE) rc = TURBO_EIO;
  if (rc == TURBO_OK && PQstatus(slot->database->connection) == CONNECTION_OK &&
      PQtransactionStatus(slot->database->connection) == PQTRANS_IDLE)
    return TURBO_OK;
  flowie_control_pgsql_database_destroy(slot->database);
  slot->database = NULL;
  {
    flowie_control_pgsql_database_config_t reopen_config = pool->database_config;
    reopen_config.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE;
    rc = flowie_control_pgsql_database_open(&reopen_config, &slot->database);
  }
  return rc;
}

int flowie_control_pgsql_pool_release(flowie_control_pgsql_pool_lease_t *lease) {
  flowie_control_pgsql_pool_t *pool;
  flowie_control_pgsql_pool_slot_t *slot;
  int rc;
  if (!lease || !lease->owner || !lease->connection) return TURBO_EINVAL;
  pool = lease->owner;
  turbo_mutex_lock(&pool->mutex);
  if (lease->slot >= pool->capacity) {
    turbo_mutex_unlock(&pool->mutex);
    return TURBO_EINVAL;
  }
  slot = &pool->slots[lease->slot];
  if (slot->state != FLOWIE_CONTROL_PGSQL_POOL_SLOT_LEASED ||
      slot->generation != lease->generation || !slot->database ||
      slot->database->connection != lease->connection) {
    turbo_mutex_unlock(&pool->mutex);
    return TURBO_EINVAL;
  }
  slot->state = FLOWIE_CONTROL_PGSQL_POOL_SLOT_CLEANING;
  turbo_mutex_unlock(&pool->mutex);

  rc = flowie_control_pgsql_pool_connection_clean(pool, slot);

  turbo_mutex_lock(&pool->mutex);
  slot->state = rc == TURBO_OK ? FLOWIE_CONTROL_PGSQL_POOL_SLOT_AVAILABLE
                               : FLOWIE_CONTROL_PGSQL_POOL_SLOT_DEAD;
  if (rc != TURBO_OK) ++pool->cleanup_failures;
  --pool->leased_count;
  flowie_control_pgsql_pool_lease_reset(lease);
  turbo_cond_broadcast(&pool->changed);
  turbo_mutex_unlock(&pool->mutex);
  return rc;
}

int flowie_control_pgsql_pool_close(flowie_control_pgsql_pool_t *pool, int timeout_ms) {
  uint64_t started_ms;
  uint64_t deadline_ms;
  int rc = TURBO_OK;
  if (!pool || timeout_ms < 0 || timeout_ms > FLOWIE_CONTROL_PGSQL_TIMEOUT_MAX_MS)
    return TURBO_EINVAL;
  started_ms = turbo_monotonic_ms();
  deadline_ms = started_ms + (uint64_t)timeout_ms;
  if (deadline_ms < started_ms) deadline_ms = UINT64_MAX;
  turbo_mutex_lock(&pool->mutex);
  pool->closing = 1;
  turbo_cond_broadcast(&pool->changed);
  while (pool->leased_count != 0u || pool->acquire_waiters != 0u || pool->reconnect_in_progress) {
    if (timeout_ms == 0 || turbo_monotonic_ms() >= deadline_ms) {
      rc = TURBO_ETIMEDOUT;
      break;
    }
    rc = flowie_control_pgsql_pool_wait(pool, deadline_ms);
    if (rc != TURBO_OK && turbo_monotonic_ms() >= deadline_ms) break;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&pool->mutex);
  return rc;
}

int flowie_control_pgsql_pool_destroy(flowie_control_pgsql_pool_t *pool) {
  if (!pool) return TURBO_OK;
  turbo_mutex_lock(&pool->mutex);
  if (!pool->closing || pool->leased_count != 0u || pool->acquire_waiters != 0u ||
      pool->reconnect_in_progress) {
    turbo_mutex_unlock(&pool->mutex);
    return TURBO_EBUSY;
  }
  turbo_mutex_unlock(&pool->mutex);
  flowie_control_pgsql_pool_storage_destroy(pool);
  return TURBO_OK;
}
