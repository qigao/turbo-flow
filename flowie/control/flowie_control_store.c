#include "flowie_control_store_internal.h"

#include "flowie_control_credential_internal.h"
#include "flowie_control_repository_internal.h"
#include "flowie_control_validation_internal.h"

#include "turbo_error.h"
#include "turbo_str.h"

#include <limits.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWIE_CONTROL_OPERATION_MAX = 31, FLOWIE_CONTROL_BUSY_TIMEOUT_MAX_MS = 30000 };

#define FLOWIE_CONTROL_SQLITE_SCHEMA_VERSION 3
#define FLOWIE_CONTROL_SQLITE_SCHEMA_FINGERPRINT "flowie-control-acl-document-schema-v3-20260805"

#define FLOWIE_CONTROL_STRINGIFY_VALUE(value) #value
#define FLOWIE_CONTROL_STRINGIFY(value) FLOWIE_CONTROL_STRINGIFY_VALUE(value)

static const char FLOWIE_CONTROL_OPERATION_USER_CREATE[] = "user.create";
static const char FLOWIE_CONTROL_OPERATION_USER_DISABLE[] = "user.disable";
static const char FLOWIE_CONTROL_OPERATION_DOMAIN_CREATE[] = "domain.create";
static const char FLOWIE_CONTROL_OPERATION_GROUP_CREATE[] = "group.create";
static const char FLOWIE_CONTROL_OPERATION_GROUP_DELETE[] = "group.delete";
static const char FLOWIE_CONTROL_OPERATION_MEMBERSHIP_ADD[] = "membership.add";
static const char FLOWIE_CONTROL_OPERATION_MEMBERSHIP_REMOVE[] = "membership.remove";
static const char FLOWIE_CONTROL_OPERATION_ROLE_CREATE[] = "role.create";
static const char FLOWIE_CONTROL_OPERATION_ROLE_DISABLE[] = "role.disable";
static const char FLOWIE_CONTROL_OPERATION_USER_ROLE_ADD[] = "user_role.add";
static const char FLOWIE_CONTROL_OPERATION_USER_ROLE_REMOVE[] = "user_role.remove";
static const char FLOWIE_CONTROL_OPERATION_CREDENTIAL_GENERATE[] = "credential.generate";
static const char FLOWIE_CONTROL_OPERATION_CREDENTIAL_ROTATE[] = "credential.rotate";
static const char FLOWIE_CONTROL_OPERATION_CREDENTIAL_REVOKE[] = "credential.revoke";
static const char FLOWIE_CONTROL_OPERATION_POLICY_RULE_PUT[] = "policy.rule.put";
static const char FLOWIE_CONTROL_OPERATION_POLICY_RULE_DELETE[] = "policy.rule.delete";
static const char FLOWIE_CONTROL_OPERATION_POLICY_PUBLISH[] = "policy.publish";
static const char FLOWIE_CONTROL_TARGET_DOMAIN[] = "domain";
static const char FLOWIE_CONTROL_TARGET_GROUP[] = "group";
static const char FLOWIE_CONTROL_TARGET_ROLE[] = "role";
static const char FLOWIE_CONTROL_TARGET_CREDENTIAL[] = "credential";
static const char FLOWIE_CONTROL_DETAIL_ARGON2ID[] = "argon2id";
static const char FLOWIE_CONTROL_TARGET_POLICY_RULE[] = "policy_rule";
static const char FLOWIE_CONTROL_POLICY_SCHEMA[] =
    "CREATE TABLE IF NOT EXISTS flowie_control_policy_draft("
    "domain_id TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0 AND ordinal<4096),"
    "rule_line TEXT NOT NULL CHECK(length(rule_line)>0 AND length(rule_line)<=16383),"
    "revision INTEGER NOT NULL CHECK(revision>0),updated_at INTEGER NOT NULL CHECK(updated_at>0),"
    "PRIMARY KEY(domain_id,ordinal),"
    "FOREIGN KEY(domain_id) REFERENCES flowie_control_domain(domain_id)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS turbo_flow_acl_bundle_v3("
    "namespace_name TEXT PRIMARY KEY,policy_version INTEGER NOT NULL CHECK(policy_version>0),"
    "expires_at INTEGER NOT NULL CHECK(expires_at>=0));"
    "CREATE TABLE IF NOT EXISTS turbo_flow_acl_rule_v3("
    "namespace_name TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0),"
    "rule_line TEXT NOT NULL CHECK(length(rule_line)>0),"
    "PRIMARY KEY(namespace_name,ordinal),"
    "FOREIGN KEY(namespace_name) REFERENCES turbo_flow_acl_bundle_v3(namespace_name)"
    " ON DELETE CASCADE) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS flowie_control_policy_publish_result("
    "request_id TEXT PRIMARY KEY,policy_version INTEGER NOT NULL CHECK(policy_version>0),"
    "FOREIGN KEY(request_id) REFERENCES flowie_control_audit(request_id)) WITHOUT ROWID;";
static const char
    FLOWIE_CONTROL_SCHEMA
        [] = "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;"
             "CREATE TABLE IF NOT EXISTS flowie_control_schema_version("
             "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
             "version INTEGER NOT NULL CHECK(version>0),fingerprint TEXT NOT NULL);"
             "INSERT OR IGNORE INTO flowie_control_schema_version(singleton,version,fingerprint) "
             "VALUES(1," FLOWIE_CONTROL_STRINGIFY(FLOWIE_CONTROL_SQLITE_SCHEMA_VERSION) ","
             "'" FLOWIE_CONTROL_SQLITE_SCHEMA_FINGERPRINT "');"
             "CREATE TABLE IF NOT EXISTS flowie_control_meta("
             "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
             "revision INTEGER NOT NULL CHECK(revision>=0));"
             "INSERT OR IGNORE INTO flowie_control_meta(singleton,revision) VALUES(1,0);"
             "CREATE TABLE IF NOT EXISTS flowie_control_domain("
             "domain_id TEXT PRIMARY KEY) WITHOUT ROWID;"
             "CREATE TABLE IF NOT EXISTS flowie_control_user("
             "domain_id TEXT NOT NULL,principal_id TEXT NOT NULL,principal_type TEXT NOT NULL,"
             "enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),"
             "revision INTEGER NOT NULL CHECK(revision>0),"
             "created_at INTEGER NOT NULL CHECK(created_at>0),"
             "updated_at INTEGER NOT NULL CHECK(updated_at>0),"
             "PRIMARY KEY(domain_id,principal_id),"
             "FOREIGN KEY(domain_id) REFERENCES flowie_control_domain(domain_id)) WITHOUT ROWID;"
             "CREATE TABLE IF NOT EXISTS flowie_control_credential("
             "domain_id TEXT NOT NULL,principal_id TEXT NOT NULL,"
             "kdf_algorithm INTEGER NOT NULL CHECK(kdf_algorithm=" FLOWIE_CONTROL_STRINGIFY(
                 FLOWIE_CONTROL_CREDENTIAL_KDF_ARGON2ID) "),"
                                                         "memory_blocks INTEGER NOT NULL "
                                                         "CHECK(memory_blocks>0),"
                                                         "passes INTEGER NOT NULL CHECK(passes>0),"
                                                         "lanes INTEGER NOT NULL CHECK(lanes>0),"
                                                         "salt BLOB NOT NULL "
                                                         "CHECK(length(salt)"
                                                         "=" FLOWIE_CONTROL_STRINGIFY(FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE) "),"
                                                                                                                           "verifier BLOB NOT NULL CHECK(length(verifier)=" FLOWIE_CONTROL_STRINGIFY(
                                                                                                                               FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE) "),"
                                                                                                                                                                        "enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),"
                                                                                                                                                                        "revision INTEGER NOT NULL CHECK(revision>0),"
                                                                                                                                                                        "created_at INTEGER NOT NULL CHECK(created_at>0),"
                                                                                                                                                                        "updated_at INTEGER NOT NULL CHECK(updated_at>0),"
                                                                                                                                                                        "PRIMARY KEY(domain_id,principal_id),"
                                                                                                                                                                        "FOREIGN KEY(domain_id,principal_id) REFERENCES "
                                                                                                                                                                        "flowie_control_user(domain_id,principal_id)) WITHOUT ROWID;"
                                                                                                                                                                        "CREATE TABLE IF NOT EXISTS flowie_control_group("
                                                                                                                                                                        "domain_id TEXT NOT NULL,group_id TEXT NOT NULL,parent_group_id TEXT,"
                                                                                                                                                                        "depth INTEGER NOT NULL CHECK(depth>=0 AND depth<=" FLOWIE_CONTROL_STRINGIFY(FLOWIE_CONTROL_GROUP_MAX_DEPTH) "),"
                                                                                                                                                                                                                                                                                     "enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),"
                                                                                                                                                                                                                                                                                     "revision INTEGER NOT NULL CHECK(revision>0),"
                                                                                                                                                                                                                                                                                     "created_at INTEGER NOT NULL CHECK(created_at>0),"
                                                                                                                                                                                                                                                                                     "updated_at INTEGER NOT NULL CHECK(updated_at>0),"
                                                                                                                                                                                                                                                                                     "PRIMARY KEY(domain_id,group_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id) REFERENCES flowie_control_domain(domain_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id,parent_group_id) REFERENCES "
                                                                                                                                                                                                                                                                                     "flowie_control_group(domain_id,group_id),"
                                                                                                                                                                                                                                                                                     "CHECK(group_id<>domain_id),"
                                                                                                                                                                                                                                                                                     "CHECK((parent_group_id IS NULL AND depth=0) OR "
                                                                                                                                                                                                                                                                                     "(parent_group_id IS NOT NULL AND depth>0))) WITHOUT ROWID;"
                                                                                                                                                                                                                                                                                     "CREATE TABLE IF NOT EXISTS flowie_control_role("
                                                                                                                                                                                                                                                                                     "domain_id TEXT NOT NULL,role_id TEXT NOT NULL,"
                                                                                                                                                                                                                                                                                     "enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),"
                                                                                                                                                                                                                                                                                     "revision INTEGER NOT NULL CHECK(revision>0),"
                                                                                                                                                                                                                                                                                     "created_at INTEGER NOT NULL CHECK(created_at>0),"
                                                                                                                                                                                                                                                                                     "updated_at INTEGER NOT NULL CHECK(updated_at>0),"
                                                                                                                                                                                                                                                                                     "PRIMARY KEY(domain_id,role_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id) REFERENCES flowie_control_domain(domain_id)) WITHOUT ROWID;"
                                                                                                                                                                                                                                                                                     "CREATE TABLE IF NOT EXISTS flowie_control_membership("
                                                                                                                                                                                                                                                                                     "domain_id TEXT NOT NULL,principal_id TEXT NOT "
                                                                                                                                                                                                                                                                                     "NULL,group_id TEXT NOT NULL,"
                                                                                                                                                                                                                                                                                     "revision INTEGER NOT NULL CHECK(revision>0),created_at "
                                                                                                                                                                                                                                                                                     "INTEGER NOT NULL CHECK(created_at>0),"
                                                                                                                                                                                                                                                                                     "PRIMARY KEY(domain_id,principal_id,group_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id,principal_id) REFERENCES "
                                                                                                                                                                                                                                                                                     "flowie_control_user(domain_id,principal_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id,group_id) REFERENCES "
                                                                                                                                                                                                                                                                                     "flowie_control_group(domain_id,group_id)) WITHOUT ROWID;"
                                                                                                                                                                                                                                                                                     "CREATE TABLE IF NOT EXISTS flowie_control_user_role("
                                                                                                                                                                                                                                                                                     "domain_id TEXT NOT NULL,principal_id TEXT NOT NULL,"
                                                                                                                                                                                                                                                                                     "role_id TEXT NOT NULL,revision INTEGER NOT NULL "
                                                                                                                                                                                                                                                                                     "CHECK(revision>0),created_at INTEGER NOT NULL "
                                                                                                                                                                                                                                                                                     "CHECK(created_at>0),"
                                                                                                                                                                                                                                                                                     "PRIMARY KEY(domain_id,principal_id,role_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id,principal_id) REFERENCES "
                                                                                                                                                                                                                                                                                     "flowie_control_user(domain_id,principal_id),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id,role_id) REFERENCES "
                                                                                                                                                                                                                                                                                     "flowie_control_role(domain_id,role_id)) WITHOUT ROWID;"
                                                                                                                                                                                                                                                                                     "CREATE TABLE IF NOT EXISTS flowie_control_audit("
                                                                                                                                                                                                                                                                                     "request_id TEXT PRIMARY KEY,actor TEXT NOT NULL,operation "
                                                                                                                                                                                                                                                                                     "TEXT NOT NULL,"
                                                                                                                                                                                                                                                                                     "domain_id TEXT NOT NULL,target_id TEXT NOT "
                                                                                                                                                                                                                                                                                     "NULL,target_detail TEXT NOT NULL,"
                                                                                                                                                                                                                                                                                     "result_revision INTEGER NOT NULL CHECK(result_revision>0),"
                                                                                                                                                                                                                                                                                     "occurred_at INTEGER NOT NULL CHECK(occurred_at>0),"
                                                                                                                                                                                                                                                                                     "FOREIGN KEY(domain_id) REFERENCES flowie_control_domain(domain_id));";

struct flowie_control_store_s {
  tstr_t database_path;
  int busy_timeout_ms;
  flowie_control_repository_t repository;
};

typedef struct flowie_control_credential_record_s {
  flowie_control_credential_kdf_params_t params;
  uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE];
  uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE];
  uint64_t user_revision;
  uint64_t credential_revision;
  int user_enabled;
  int credential_exists;
  int credential_enabled;
} flowie_control_credential_record_t;

typedef struct flowie_control_policy_bundle_owner_s {
  turbo_flow_security_rule_t *rules;
} flowie_control_policy_bundle_owner_t;

static int flowie_control_sqlite_status(int status) {
  int primary = status & 0xff;
  if (primary == SQLITE_BUSY || primary == SQLITE_LOCKED) return TURBO_EBUSY;
  if (primary == SQLITE_NOMEM) return TURBO_ENOMEM;
  if (primary == SQLITE_CONSTRAINT || primary == SQLITE_MISMATCH || primary == SQLITE_RANGE)
    return TURBO_EINVAL;
  return TURBO_EIO;
}

static int flowie_control_schema_preflight(sqlite3 *database) {
  static const char sql[] =
      "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' "
      "AND name='flowie_control_schema_version'),"
      "EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' "
      "AND name GLOB 'flowie_control_*' AND name<>'flowie_control_schema_version')";
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (!database) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  status = sqlite3_step(statement);
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  rc = !sqlite3_column_int(statement, 0) && sqlite3_column_int(statement, 1) ? TURBO_EPROTO
                                                                            : TURBO_OK;
done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_schema_validate(sqlite3 *database) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (!database) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database,
      "SELECT version,fingerprint FROM flowie_control_schema_version WHERE singleton=1", -1,
      &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  status = sqlite3_step(statement);
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_TEXT ||
      sqlite3_column_int(statement, 0) != FLOWIE_CONTROL_SQLITE_SCHEMA_VERSION ||
      strcmp((const char *)sqlite3_column_text(statement, 1),
             FLOWIE_CONTROL_SQLITE_SCHEMA_FINGERPRINT) != 0 ||
      sqlite3_step(statement) != SQLITE_DONE) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = TURBO_OK;
done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_open_database(const flowie_control_store_t *store, sqlite3 **out) {
  sqlite3 *database = NULL;
  int status;
  if (out) *out = NULL;
  if (!store || !store->database_path || !out) return TURBO_EINVAL;
  status =
      sqlite3_open_v2(store->database_path, &database,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (status == SQLITE_OK) status = sqlite3_extended_result_codes(database, 1);
  if (status == SQLITE_OK && store->busy_timeout_ms > 0)
    status = sqlite3_busy_timeout(database, store->busy_timeout_ms);
  if (status == SQLITE_OK)
    status = sqlite3_exec(database, "PRAGMA foreign_keys=ON", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    if (database) (void)sqlite3_close(database);
    return flowie_control_sqlite_status(status);
  }
  *out = database;
  return TURBO_OK;
}

static int flowie_control_bind_text(sqlite3_stmt *statement, int index, const char *value) {
  int status = sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT);
  return status == SQLITE_OK ? TURBO_OK : flowie_control_sqlite_status(status);
}

static int flowie_control_bind_blob(sqlite3_stmt *statement, int index, const void *value,
                                    size_t size) {
  int status;
  if (!statement || (!value && size != 0u) || size > (size_t)INT_MAX) return TURBO_EINVAL;
  status = sqlite3_bind_blob(statement, index, value, (int)size, SQLITE_TRANSIENT);
  return status == SQLITE_OK ? TURBO_OK : flowie_control_sqlite_status(status);
}

static int flowie_control_copy_column(sqlite3_stmt *statement, int column, char *out,
                                      size_t capacity) {
  const unsigned char *text;
  int length;
  if (!statement || !out || capacity == 0u || sqlite3_column_type(statement, column) != SQLITE_TEXT)
    return TURBO_EPROTO;
  text = sqlite3_column_text(statement, column);
  length = sqlite3_column_bytes(statement, column);
  if (!text || length <= 0 || (size_t)length >= capacity || memchr(text, '\0', (size_t)length))
    return TURBO_EPROTO;
  memcpy(out, text, (size_t)length);
  out[length] = '\0';
  return TURBO_OK;
}

static int flowie_control_read_revision(sqlite3 *database, uint64_t *revision_out) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc = TURBO_EIO;
  if (!database || !revision_out) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database, "SELECT revision FROM flowie_control_meta WHERE singleton=1", -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  status = sqlite3_step(statement);
  if (status == SQLITE_ROW && sqlite3_column_type(statement, 0) == SQLITE_INTEGER &&
      sqlite3_column_int64(statement, 0) >= 0) {
    *revision_out = (uint64_t)sqlite3_column_int64(statement, 0);
    rc = TURBO_OK;
  } else {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
  }
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_advance_revision(sqlite3 *database, uint64_t current,
                                           uint64_t *next_out) {
  sqlite3_stmt *statement = NULL;
  uint64_t next;
  int status;
  int rc;
  if (!database || !next_out || current >= (uint64_t)INT64_MAX) return TURBO_ERANGE;
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database, "UPDATE flowie_control_meta SET revision=?1 WHERE singleton=1 AND revision=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  status = sqlite3_bind_int64(statement, 1, (sqlite3_int64)next);
  if (status == SQLITE_OK) status = sqlite3_bind_int64(statement, 2, (sqlite3_int64)current);
  if (status == SQLITE_OK) status = sqlite3_step(statement);
  rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
           ? TURBO_OK
           : (status == SQLITE_DONE ? TURBO_EBUSY : flowie_control_sqlite_status(status));
  (void)sqlite3_finalize(statement);
  if (rc == TURBO_OK) *next_out = next;
  return rc;
}

static int flowie_control_replay(sqlite3 *database, const char *request_id, const char *actor,
                                 const char *operation, const char *domain_id,
                                 const char *target_id, const char *target_detail,
                                 flowie_control_command_result_t *result, int *found_out) {
  sqlite3_stmt *statement = NULL;
  const unsigned char *stored_actor;
  const unsigned char *stored_operation;
  const unsigned char *stored_root;
  const unsigned char *stored_target;
  const unsigned char *stored_detail;
  int status;
  int rc = TURBO_EIO;
  if (!database || !request_id || !actor || !operation || !domain_id || !target_id || !result ||
      !found_out)
    return TURBO_EINVAL;
  *found_out = 0;
  status = sqlite3_prepare_v2(
      database,
      "SELECT actor,operation,domain_id,target_id,target_detail,result_revision "
      "FROM flowie_control_audit WHERE request_id=?1",
      -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, request_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_OK;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_TEXT ||
      sqlite3_column_type(statement, 1) != SQLITE_TEXT ||
      sqlite3_column_type(statement, 2) != SQLITE_TEXT ||
      sqlite3_column_type(statement, 3) != SQLITE_TEXT ||
      sqlite3_column_type(statement, 4) != SQLITE_TEXT ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      sqlite3_column_int64(statement, 5) <= 0) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  stored_actor = sqlite3_column_text(statement, 0);
  stored_operation = sqlite3_column_text(statement, 1);
  stored_root = sqlite3_column_text(statement, 2);
  stored_target = sqlite3_column_text(statement, 3);
  stored_detail = sqlite3_column_text(statement, 4);
  if (!stored_actor || !stored_operation || !stored_root || !stored_target || !stored_detail ||
      strcmp((const char *)stored_actor, actor) != 0 ||
      strcmp((const char *)stored_operation, operation) != 0 ||
      strcmp((const char *)stored_root, domain_id) != 0 ||
      strcmp((const char *)stored_target, target_id) != 0 ||
      (target_detail && strcmp((const char *)stored_detail, target_detail) != 0)) {
    rc = TURBO_EBUSY;
    goto done;
  }
  result->revision = (uint64_t)sqlite3_column_int64(statement, 5);
  result->replayed = 1;
  *found_out = 1;
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_insert_audit(sqlite3 *database, const char *request_id, const char *actor,
                                       const char *operation, const char *domain_id,
                                       const char *target_id, const char *target_detail,
                                       uint64_t revision, uint64_t occurred_at) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_audit(request_id,actor,operation,domain_id,target_id,"
      "target_detail,result_revision,occurred_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, request_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, actor);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, operation);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 4, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 5, target_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 6, target_detail);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 7, (sqlite3_int64)revision) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 8, (sqlite3_int64)occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);
  }
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_command_common_valid(const char *domain_id, const char *target_id,
                                               const char *actor, const char *request_id,
                                               uint64_t expected_revision, uint64_t occurred_at) {
  return flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) &&
         flowie_control_text_valid(target_id, TURBO_FLOW_SECURITY_ID_MAX) &&
         flowie_control_text_valid(actor, FLOWIE_CONTROL_ACTOR_MAX) &&
         flowie_control_text_valid(request_id, FLOWIE_CONTROL_REQUEST_ID_MAX) &&
         expected_revision <= (uint64_t)INT64_MAX && occurred_at > 0u &&
         occurred_at <= (uint64_t)INT64_MAX;
}

static int flowie_control_domain_exists(sqlite3 *database, const char *domain_id) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (!database || !domain_id) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database, "SELECT 1 FROM flowie_control_domain WHERE domain_id=?1", -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE)
    rc = TURBO_ENOENT;
  else if (status != SQLITE_ROW || sqlite3_step(statement) != SQLITE_DONE)
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
  else
    rc = TURBO_OK;
done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_group_lookup(sqlite3 *database, const char *domain_id,
                                       const char *group_id, uint32_t *depth_out,
                                       int *enabled_out) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (depth_out) *depth_out = 0u;
  if (enabled_out) *enabled_out = 0;
  if (!database || !domain_id || !group_id || !depth_out || !enabled_out) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database,
      "SELECT depth,enabled FROM flowie_control_group WHERE domain_id=?1 AND group_id=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, group_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
      sqlite3_column_int64(statement, 0) < 0 ||
      sqlite3_column_int64(statement, 0) > FLOWIE_CONTROL_GROUP_MAX_DEPTH ||
      (sqlite3_column_int(statement, 1) != 0 && sqlite3_column_int(statement, 1) != 1)) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  *depth_out = (uint32_t)sqlite3_column_int(statement, 0);
  *enabled_out = sqlite3_column_int(statement, 1);
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_group_references(sqlite3 *database, const char *domain_id,
                                           const char *group_id, int *active_child_out,
                                           int *direct_membership_out) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (active_child_out) *active_child_out = 0;
  if (direct_membership_out) *direct_membership_out = 0;
  if (!database || !domain_id || !group_id || !active_child_out || !direct_membership_out)
    return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database,
      "SELECT EXISTS(SELECT 1 FROM flowie_control_group WHERE domain_id=?1 AND "
      "parent_group_id=?2),EXISTS(SELECT 1 FROM flowie_control_membership WHERE "
      "domain_id=?1 AND group_id=?2)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, group_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
      (sqlite3_column_int(statement, 0) != 0 && sqlite3_column_int(statement, 0) != 1) ||
      (sqlite3_column_int(statement, 1) != 0 && sqlite3_column_int(statement, 1) != 1)) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  *active_child_out = sqlite3_column_int(statement, 0);
  *direct_membership_out = sqlite3_column_int(statement, 1);
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_policy_subject_referenced(sqlite3 *database, const char *domain_id,
                                                    turbo_flow_security_subject_kind_t subject_kind,
                                                    const char *subject, int *referenced_out) {
  static const char sql[] =
      "SELECT 0,rule_line FROM flowie_control_policy_draft WHERE domain_id=?1 "
      "UNION ALL SELECT 1,rule_line FROM turbo_flow_acl_rule_v3 WHERE namespace_name=?1";
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (referenced_out) *referenced_out = 0;
  if (!database || !domain_id || !subject || !referenced_out ||
      (subject_kind != TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL &&
       subject_kind != TURBO_FLOW_SECURITY_SUBJECT_ROLE &&
       subject_kind != TURBO_FLOW_SECURITY_SUBJECT_GROUP))
    return TURBO_EINVAL;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char *line;
    int line_size;
    int published;
    if (sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 1) != SQLITE_TEXT) {
      rc = TURBO_EPROTO;
      goto done;
    }
    published = sqlite3_column_int(statement, 0);
    line = sqlite3_column_text(statement, 1);
    line_size = sqlite3_column_bytes(statement, 1);
    if (!line || line_size <= 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (published) {
      turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
      if ((size_t)line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX ||
          turbo_flow_security_rule_parse_line((const char *)line, (size_t)line_size, &rule) !=
              TURBO_OK ||
          strcmp(rule.domain_id, domain_id) != 0) {
        rc = TURBO_EPROTO;
        goto done;
      }
      if (rule.subject_kind == subject_kind && strcmp(rule.subject, subject) == 0) {
        *referenced_out = 1;
        rc = TURBO_OK;
        goto done;
      }
      if (subject_kind == TURBO_FLOW_SECURITY_SUBJECT_GROUP &&
          rule.resource_type == TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC &&
          rule.match_kind == TURBO_FLOW_SECURITY_MATCH_ADAPTER) {
        size_t domain_size = strlen(domain_id);
        const char *cursor = rule.pattern;
        const char *devices;
        if (strncmp(cursor, domain_id, domain_size) == 0 &&
            strncmp(cursor + domain_size, "/groups/", sizeof("/groups/") - 1u) == 0) {
          cursor += domain_size + sizeof("/groups/") - 1u;
          devices = strstr(cursor, "/devices/");
          while (devices && cursor < devices) {
            const char *slash = strchr(cursor, '/');
            const char *end = slash && slash < devices ? slash : devices;
            size_t length = (size_t)(end - cursor);
            if (strlen(subject) == length && memcmp(cursor, subject, length) == 0) {
              *referenced_out = 1;
              rc = TURBO_OK;
              goto done;
            }
            cursor = end < devices ? end + 1u : devices;
          }
        }
      }
    } else {
      flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
      if (flowie_control_acl_parse((const char *)line, (size_t)line_size, &document) != TURBO_OK) {
        rc = TURBO_EPROTO;
        goto done;
      }
      if (subject_kind == TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL &&
          strcmp(document.subject, subject) == 0) {
        *referenced_out = 1;
        rc = TURBO_OK;
        goto done;
      }
      if (subject_kind == TURBO_FLOW_SECURITY_SUBJECT_GROUP) {
        for (size_t entry_index = 0u; entry_index < document.entry_count; ++entry_index) {
          const flowie_control_acl_entry_t *entry = &document.entries[entry_index];
          for (size_t group_index = 0u; group_index < entry->group_count; ++group_index) {
            size_t length = entry->group_lengths[group_index];
            const char *group = entry->topic + entry->group_offsets[group_index];
            if (strlen(subject) == length && memcmp(group, subject, length) == 0) {
              *referenced_out = 1;
              rc = TURBO_OK;
              goto done;
            }
          }
        }
      }
    }
  }
  rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_user_enabled(sqlite3 *database, const char *domain_id,
                                       const char *principal_id, int *enabled_out) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (enabled_out) *enabled_out = 0;
  if (!database || !domain_id || !principal_id || !enabled_out) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database,
      "SELECT enabled FROM flowie_control_user WHERE domain_id=?1 AND principal_id=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      (sqlite3_column_int(statement, 0) != 0 && sqlite3_column_int(statement, 0) != 1)) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  *enabled_out = sqlite3_column_int(statement, 0);
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_credential_record_read(sqlite3 *database, const char *domain_id,
                                                 const char *principal_id,
                                                 flowie_control_credential_record_t *out) {
  flowie_control_credential_record_t record = {0};
  sqlite3_stmt *statement = NULL;
  const void *salt;
  const void *verifier;
  int status;
  int rc;
  if (!database || !domain_id || !principal_id || !out) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database,
      "SELECT u.enabled,u.revision,c.kdf_algorithm,c.memory_blocks,c.passes,c.lanes,c.salt,"
      "c.verifier,c.enabled,c.revision FROM flowie_control_user u LEFT JOIN "
      "flowie_control_credential c ON c.domain_id=u.domain_id AND "
      "c.principal_id=u.principal_id WHERE u.domain_id=?1 AND u.principal_id=?2",
      -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
      (sqlite3_column_int(statement, 0) != 0 && sqlite3_column_int(statement, 0) != 1) ||
      sqlite3_column_int64(statement, 1) <= 0) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  record.user_enabled = sqlite3_column_int(statement, 0);
  record.user_revision = (uint64_t)sqlite3_column_int64(statement, 1);
  if (sqlite3_column_type(statement, 2) == SQLITE_NULL) {
    for (int column = 3; column <= 9; ++column) {
      if (sqlite3_column_type(statement, column) != SQLITE_NULL) {
        rc = TURBO_EPROTO;
        goto done;
      }
    }
    *out = record;
    rc = TURBO_OK;
    goto done;
  }
  if (sqlite3_column_type(statement, 2) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 6) != SQLITE_BLOB ||
      sqlite3_column_bytes(statement, 6) != FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE ||
      sqlite3_column_type(statement, 7) != SQLITE_BLOB ||
      sqlite3_column_bytes(statement, 7) != FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE ||
      sqlite3_column_type(statement, 8) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 9) != SQLITE_INTEGER ||
      sqlite3_column_int64(statement, 2) < 0 || sqlite3_column_int64(statement, 2) > UINT32_MAX ||
      sqlite3_column_int64(statement, 3) < 0 || sqlite3_column_int64(statement, 3) > UINT32_MAX ||
      sqlite3_column_int64(statement, 4) < 0 || sqlite3_column_int64(statement, 4) > UINT32_MAX ||
      sqlite3_column_int64(statement, 5) < 0 || sqlite3_column_int64(statement, 5) > UINT32_MAX ||
      (sqlite3_column_int(statement, 8) != 0 && sqlite3_column_int(statement, 8) != 1) ||
      sqlite3_column_int64(statement, 9) <= 0) {
    rc = TURBO_EPROTO;
    goto done;
  }
  record.params.algorithm = (uint32_t)sqlite3_column_int64(statement, 2);
  record.params.memory_blocks = (uint32_t)sqlite3_column_int64(statement, 3);
  record.params.passes = (uint32_t)sqlite3_column_int64(statement, 4);
  record.params.lanes = (uint32_t)sqlite3_column_int64(statement, 5);
  if (!flowie_control_credential_params_valid(&record.params)) {
    rc = TURBO_EPROTO;
    goto done;
  }
  salt = sqlite3_column_blob(statement, 6);
  verifier = sqlite3_column_blob(statement, 7);
  if (!salt || !verifier) {
    rc = TURBO_EPROTO;
    goto done;
  }
  memcpy(record.salt, salt, sizeof(record.salt));
  memcpy(record.verifier, verifier, sizeof(record.verifier));
  record.credential_enabled = sqlite3_column_int(statement, 8);
  record.credential_revision = (uint64_t)sqlite3_column_int64(statement, 9);
  record.credential_exists = 1;
  *out = record;
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  if (rc != TURBO_OK) flowie_control_credential_wipe(&record, sizeof(record));
  return rc;
}

static int flowie_control_role_enabled(sqlite3 *database, const char *domain_id,
                                       const char *role_id, int *enabled_out) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (enabled_out) *enabled_out = 0;
  if (!database || !domain_id || !role_id || !enabled_out) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database, "SELECT enabled FROM flowie_control_role WHERE domain_id=?1 AND role_id=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, role_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      (sqlite3_column_int(statement, 0) != 0 && sqlite3_column_int(statement, 0) != 1)) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  *enabled_out = sqlite3_column_int(statement, 0);
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_effective_roles_database(sqlite3 *database, const char *domain_id,
                                                   const char *principal_id,
                                                   flowie_control_effective_roles_view_t *out) {
  static const char sql[] =
      "SELECT r.role_id FROM flowie_control_user_role ur "
      "JOIN flowie_control_role r ON r.domain_id=ur.domain_id AND r.role_id=ur.role_id "
      "WHERE ur.domain_id=?1 AND ur.principal_id=?2 AND r.enabled=1 ORDER BY r.role_id";
  flowie_control_effective_roles_view_t view = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (!database || !domain_id || !principal_id || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    if (view.role_count >= TURBO_FLOW_SECURITY_MAX_ROLES) {
      rc = TURBO_ENOSPC;
      goto done;
    }
    rc = flowie_control_copy_column(statement, 0, view.roles[view.role_count],
                                    sizeof(view.roles[view.role_count]));
    if (rc != TURBO_OK) goto done;
    ++view.role_count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  *out = view;
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_effective_groups_database(sqlite3 *database, const char *domain_id,
                                                    const char *principal_id,
                                                    flowie_control_effective_groups_view_t *out) {
  static const char sql[] =
      "WITH RECURSIVE effective(group_id,parent_group_id,depth) AS ("
      "SELECT g.group_id,g.parent_group_id,g.depth FROM flowie_control_membership m "
      "JOIN flowie_control_group g ON g.domain_id=m.domain_id AND g.group_id=m.group_id "
      "WHERE m.domain_id=?1 AND m.principal_id=?2 AND g.enabled=1 "
      "UNION SELECT p.group_id,p.parent_group_id,p.depth FROM effective e "
      "JOIN flowie_control_group p ON p.domain_id=?1 AND p.group_id=e.parent_group_id "
      "WHERE p.enabled=1) SELECT group_id FROM effective GROUP BY group_id "
      "ORDER BY MIN(depth),group_id";
  flowie_control_effective_groups_view_t view = FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (!database || !domain_id || !principal_id || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    if (view.group_count >= TURBO_FLOW_SECURITY_MAX_GROUPS) {
      rc = TURBO_ENOSPC;
      goto done;
    }
    rc = flowie_control_copy_column(statement, 0, view.groups[view.group_count],
                                    sizeof(view.groups[view.group_count]));
    if (rc != TURBO_OK) goto done;
    ++view.group_count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  *out = view;
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_policy_target(uint32_t ordinal, char output[32]) {
  int written = snprintf(output, 32u, "%u", ordinal);
  return written > 0 && written < 32 ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_control_policy_publish_detail(uint64_t expires_at, char output[64]) {
  int written = snprintf(output, 64u, "expires_at=%llu", (unsigned long long)expires_at);
  return written > 0 && written < 64 ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_control_acl_group_path_validate(sqlite3 *database, const char *domain_id,
                                                  const flowie_control_acl_entry_t *entry) {
  static const char sql[] =
      "SELECT parent_group_id,depth,enabled FROM flowie_control_group "
      "WHERE domain_id=?1 AND group_id=?2";
  sqlite3_stmt *statement = NULL;
  char previous[TURBO_FLOW_SECURITY_ID_MAX + 1u] = {0};
  int status;
  int rc;
  if (!database || !domain_id || !entry || entry->group_count == 0u ||
      entry->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS)
    return TURBO_EINVAL;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  for (size_t index = 0u; index < entry->group_count; ++index) {
    char current[TURBO_FLOW_SECURITY_ID_MAX + 1u];
    size_t topic_size = strlen(entry->topic);
    size_t offset = entry->group_offsets[index];
    size_t length = entry->group_lengths[index];
    if (length == 0u || length > TURBO_FLOW_SECURITY_ID_MAX || offset > topic_size ||
        length > topic_size - offset) {
      rc = TURBO_EPROTO;
      goto done;
    }
    memcpy(current, entry->topic + offset, length);
    current[length] = '\0';
    (void)sqlite3_reset(statement);
    (void)sqlite3_clear_bindings(statement);
    rc = flowie_control_bind_text(statement, 1, domain_id);
    if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, current);
    if (rc != TURBO_OK) goto done;
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
      rc = TURBO_ENOENT;
      goto done;
    }
    if (status != SQLITE_ROW || sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 2) != SQLITE_INTEGER ||
        sqlite3_column_int(statement, 1) != (int)index || sqlite3_column_int(statement, 2) != 1 ||
        (index == 0u && sqlite3_column_type(statement, 0) != SQLITE_NULL) ||
        (index != 0u &&
         (sqlite3_column_type(statement, 0) != SQLITE_TEXT ||
          strcmp((const char *)sqlite3_column_text(statement, 0), previous) != 0))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    memcpy(previous, current, length + 1u);
  }
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

static int flowie_control_policy_document_validate(
    sqlite3 *database, const char *domain_id, const char *document_text, size_t document_size,
    flowie_control_acl_document_t *document_out, size_t *rule_count_out,
    size_t *deny_rule_count_out) {
  flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
  size_t rule_count = 1u;
  size_t deny_count = 0u;
  int enabled = 0;
  int rc;
  if (rule_count_out) *rule_count_out = 0u;
  if (deny_rule_count_out) *deny_rule_count_out = 0u;
  if (!database || !rule_count_out || !deny_rule_count_out) return TURBO_EINVAL;
  rc = flowie_control_acl_document_syntax_validate(domain_id, document_text, document_size,
                                                   &document);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_domain_exists(database, domain_id);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_user_enabled(database, domain_id, document.subject, &enabled);
  if (rc != TURBO_OK) return rc;
  if (!enabled) return TURBO_EPERM;
  if (document.connection_effect == TURBO_FLOW_SECURITY_DENY) deny_count = 1u;
  for (size_t index = 0u; index < document.entry_count; ++index) {
    const flowie_control_acl_entry_t *entry = &document.entries[index];
    rc = flowie_control_acl_group_path_validate(database, domain_id, entry);
    if (rc != TURBO_OK) return rc;
    if (entry->alternative_count == 0u ||
        rule_count > TURBO_FLOW_SECURITY_MAX_RULES - entry->alternative_count)
      return TURBO_ENOSPC;
    rule_count += entry->alternative_count;
    if (entry->effect == TURBO_FLOW_SECURITY_DENY)
      deny_count += entry->alternative_count;
  }
  if (document_out) *document_out = document;
  *rule_count_out = rule_count;
  *deny_rule_count_out = deny_count;
  return TURBO_OK;
}

static int flowie_control_policy_validate_database(sqlite3 *database, const char *domain_id,
                                                   flowie_control_policy_validation_t *out) {
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  sqlite3_stmt *statement = NULL;
  char *subjects = NULL;
  size_t document_count = 0u;
  int status;
  int rc;
  if (!database || !domain_id || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flowie_control_read_revision(database, &validation.store_revision);
  if (rc != TURBO_OK) return rc;
  subjects = (char *)calloc(TURBO_FLOW_SECURITY_MAX_RULES,
                            TURBO_FLOW_SECURITY_ID_MAX + 1u);
  if (!subjects) return TURBO_ENOMEM;
  status = sqlite3_prepare_v2(
      database,
      "SELECT rule_line FROM flowie_control_policy_draft WHERE domain_id=?1 ORDER BY ordinal",
      -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char *line;
    int line_size;
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
    size_t expanded = 0u;
    size_t denied = 0u;
    if (document_count >= TURBO_FLOW_SECURITY_MAX_RULES ||
        sqlite3_column_type(statement, 0) != SQLITE_TEXT) {
      rc = document_count >= TURBO_FLOW_SECURITY_MAX_RULES ? TURBO_ENOSPC : TURBO_EPROTO;
      goto done;
    }
    line = sqlite3_column_text(statement, 0);
    line_size = sqlite3_column_bytes(statement, 0);
    if (!line || line_size <= 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    rc = flowie_control_policy_document_validate(database, domain_id, (const char *)line,
                                                 (size_t)line_size, &document, &expanded, &denied);
    if (rc != TURBO_OK) goto done;
    for (size_t prior = 0u; prior < document_count; ++prior) {
      if (strcmp(subjects + prior * (TURBO_FLOW_SECURITY_ID_MAX + 1u), document.subject) == 0) {
        rc = TURBO_EALREADY;
        goto done;
      }
    }
    memcpy(subjects + document_count * (TURBO_FLOW_SECURITY_ID_MAX + 1u), document.subject,
           strlen(document.subject) + 1u);
    ++document_count;
    if (expanded > TURBO_FLOW_SECURITY_MAX_RULES - validation.rule_count) {
      rc = TURBO_ENOSPC;
      goto done;
    }
    validation.rule_count += expanded;
    validation.deny_rule_count += denied;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  if (validation.rule_count == 0u) {
    rc = TURBO_ENOENT;
    goto done;
  }
  *out = validation;
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  free(subjects);
  return rc;
}

static int flowie_control_policy_subject_unique(sqlite3 *database, const char *domain_id,
                                                uint32_t ordinal, const char *subject) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc;
  if (!database || !domain_id || !subject) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(
      database,
      "SELECT rule_line FROM flowie_control_policy_draft WHERE domain_id=?1 AND ordinal<>?2",
      -1, &statement, NULL);
  if (status != SQLITE_OK) return flowie_control_sqlite_status(status);
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 2, ordinal) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char *text;
    int text_size;
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
    if (sqlite3_column_type(statement, 0) != SQLITE_TEXT ||
        !(text = sqlite3_column_text(statement, 0)) ||
        (text_size = sqlite3_column_bytes(statement, 0)) <= 0 ||
        flowie_control_acl_parse((const char *)text, (size_t)text_size, &document) != TURBO_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (strcmp(document.subject, subject) == 0) {
      rc = TURBO_EALREADY;
      goto done;
    }
  }
  rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);

done:
  (void)sqlite3_finalize(statement);
  return rc;
}

int flowie_control_store_open(const flowie_control_store_config_t *config,
                              flowie_control_store_t **out) {
  flowie_control_store_t *store;
  sqlite3 *database = NULL;
  int status;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || !config->database_path ||
      !config->database_path[0] || config->busy_timeout_ms < 0 ||
      config->busy_timeout_ms > FLOWIE_CONTROL_BUSY_TIMEOUT_MAX_MS)
    return TURBO_EINVAL;
  store = (flowie_control_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->database_path = tstr_dup(config->database_path);
  store->busy_timeout_ms = config->busy_timeout_ms;
  if (!store->database_path) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_schema_preflight(database);
  if (rc != TURBO_OK) goto fail;
  status = sqlite3_exec(database, FLOWIE_CONTROL_SCHEMA, NULL, NULL, NULL);
  if (status == SQLITE_OK)
    status = sqlite3_exec(database, FLOWIE_CONTROL_POLICY_SCHEMA, NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto fail;
  }
  rc = flowie_control_schema_validate(database);
  if (rc != TURBO_OK) goto fail;
  (void)sqlite3_close(database);
  database = NULL;
  store->repository = (flowie_control_repository_t)FLOWIE_CONTROL_REPOSITORY_INIT;
  rc = flowie_control_repository_bind_sqlite(store, &store->repository);
  if (rc != TURBO_OK) goto fail;
  *out = store;
  return TURBO_OK;

fail:
  if (database) (void)sqlite3_close(database);
  flowie_control_store_destroy(store);
  return rc;
}

void flowie_control_store_destroy(flowie_control_store_t *store) {
  if (!store) return;
  tstr_freep(&store->database_path);
  store->repository = (flowie_control_repository_t)FLOWIE_CONTROL_REPOSITORY_INIT;
  free(store);
}

const flowie_control_repository_t *flowie_control_store_repository(flowie_control_store_t *store) {
  if (!store || flowie_control_repository_validate(&store->repository) != TURBO_OK) return NULL;
  return &store->repository;
}

int flowie_control_store_domain_create(
    flowie_control_store_t *store, const flowie_control_domain_create_command_t *command,
    flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->domain_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_DOMAIN_CREATE, command->domain_id,
                             command->domain_id, FLOWIE_CONTROL_TARGET_DOMAIN, result,
                             &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database, "INSERT INTO flowie_control_domain(domain_id) VALUES(?1)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE
             ? TURBO_OK
             : ((status & 0xff) == SQLITE_CONSTRAINT ? TURBO_EALREADY
                                                     : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_DOMAIN_CREATE,
                                   command->domain_id, command->domain_id,
                                   FLOWIE_CONTROL_TARGET_DOMAIN, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_group_create(flowie_control_store_t *store,
                                      const flowie_control_group_create_command_t *command,
                                      flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint32_t parent_depth = 0u;
  int parent_enabled = 0;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->group_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      (command->parent_group_id &&
       !flowie_control_text_valid(command->parent_group_id, TURBO_FLOW_SECURITY_ID_MAX)) ||
      strcmp(command->group_id, command->domain_id) == 0 ||
      (command->parent_group_id && strcmp(command->group_id, command->parent_group_id) == 0))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_GROUP_CREATE, command->domain_id,
                             command->group_id,
                             command->parent_group_id ? command->parent_group_id
                                                      : FLOWIE_CONTROL_TARGET_DOMAIN,
                             result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  if (command->parent_group_id) {
    rc = flowie_control_group_lookup(database, command->domain_id, command->parent_group_id,
                                     &parent_depth, &parent_enabled);
    if (rc != TURBO_OK) goto done;
    if (!parent_enabled) {
      rc = TURBO_EPERM;
      goto done;
    }
    if (parent_depth >= FLOWIE_CONTROL_GROUP_MAX_DEPTH) {
      rc = TURBO_ENOSPC;
      goto done;
    }
  } else {
    rc = flowie_control_domain_exists(database, command->domain_id);
    if (rc != TURBO_OK) goto done;
  }
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_group(domain_id,group_id,parent_group_id,depth,enabled,"
      "revision,created_at,updated_at) VALUES(?1,?2,?3,?4,1,?5,?6,?6)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->group_id);
  if (rc == TURBO_OK && command->parent_group_id)
    rc = flowie_control_bind_text(statement, 3, command->parent_group_id);
  else if (rc == TURBO_OK && sqlite3_bind_null(statement, 3) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int(statement, 4,
                       command->parent_group_id ? (int)(parent_depth + 1u) : 0) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 5, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 6, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE
             ? TURBO_OK
             : ((status & 0xff) == SQLITE_CONSTRAINT ? TURBO_EALREADY
                                                     : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_GROUP_CREATE, command->domain_id,
                                   command->group_id,
                                   command->parent_group_id ? command->parent_group_id
                                                            : FLOWIE_CONTROL_TARGET_DOMAIN,
                                   next,
                                   command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_group_delete(flowie_control_store_t *store,
                                      const flowie_control_group_delete_command_t *command,
                                      flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint32_t group_depth = 0u;
  int group_enabled = 0;
  int child = 0;
  int direct_membership = 0;
  int policy_reference = 0;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->group_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      strcmp(command->group_id, command->domain_id) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_GROUP_DELETE, command->domain_id,
                             command->group_id, FLOWIE_CONTROL_TARGET_GROUP, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_group_lookup(database, command->domain_id, command->group_id,
                                   &group_depth, &group_enabled);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_group_references(database, command->domain_id, command->group_id,
                                       &child, &direct_membership);
  if (rc != TURBO_OK) goto done;
  if (child || direct_membership) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_policy_subject_referenced(database, command->domain_id,
                                                TURBO_FLOW_SECURITY_SUBJECT_GROUP,
                                                command->group_id, &policy_reference);
  if (rc != TURBO_OK) goto done;
  if (policy_reference) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "DELETE FROM flowie_control_group WHERE domain_id=?1 AND group_id=?2",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->group_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_EBUSY : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_GROUP_DELETE, command->domain_id,
                                   command->group_id, FLOWIE_CONTROL_TARGET_GROUP, next,
                                   command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_user_create(flowie_control_store_t *store,
                                     const flowie_control_user_create_command_t *command,
                                     flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->principal_type, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_USER_CREATE, command->domain_id,
                             command->principal_id, command->principal_type, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_domain_exists(database, command->domain_id);
  if (rc != TURBO_OK) goto done;
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_user(domain_id,principal_id,principal_type,enabled,revision,"
      "created_at,updated_at) VALUES(?1,?2,?3,1,?4,?5,?5)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, command->principal_type);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 4, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 5, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE
             ? TURBO_OK
             : ((status & 0xff) == SQLITE_CONSTRAINT ? TURBO_EALREADY
                                                     : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_USER_CREATE, command->domain_id,
                                   command->principal_id, command->principal_type, next,
                                   command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_user_disable(flowie_control_store_t *store,
                                      const flowie_control_user_disable_command_t *command,
                                      flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  char principal_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u] = {0};
  uint64_t current = 0u;
  uint64_t next = 0u;
  int policy_reference = 0;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_USER_DISABLE, command->domain_id,
                             command->principal_id, NULL, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  status = sqlite3_prepare_v2(
      database,
      "SELECT principal_type,enabled FROM flowie_control_user WHERE domain_id=?1 AND "
      "principal_id=?2",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 1) != SQLITE_INTEGER) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_copy_column(statement, 0, principal_type, sizeof(principal_type));
  if (rc != TURBO_OK) goto done;
  if (sqlite3_column_int(statement, 1) != 1) {
    rc = TURBO_EALREADY;
    goto done;
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  rc = flowie_control_policy_subject_referenced(database, command->domain_id,
                                                TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL,
                                                command->principal_id, &policy_reference);
  if (rc != TURBO_OK) goto done;
  if (policy_reference) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "UPDATE flowie_control_user SET enabled=0,revision=?1,updated_at=?2 WHERE domain_id=?3 "
      "AND principal_id=?4 AND enabled=1",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  if (sqlite3_bind_int64(statement, 1, (sqlite3_int64)next) != SQLITE_OK ||
      sqlite3_bind_int64(statement, 2, (sqlite3_int64)command->occurred_at) != SQLITE_OK) {
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
    goto done;
  }
  rc = flowie_control_bind_text(statement, 3, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 4, command->principal_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_EBUSY : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(
      database, command->request_id, command->actor, FLOWIE_CONTROL_OPERATION_USER_DISABLE,
      command->domain_id, command->principal_id, principal_type, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_user_get(flowie_control_store_t *store, const char *domain_id,
                                  const char *principal_id, flowie_control_user_view_t *out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_user_view_t view = FLOWIE_CONTROL_USER_VIEW_INIT;
  int status;
  int rc;
  if (out && out->size >= sizeof(*out)) *out = view;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(
      database,
      "SELECT domain_id,principal_id,principal_type,enabled,revision,created_at,updated_at "
      "FROM flowie_control_user WHERE domain_id=?1 AND principal_id=?2",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 6) != SQLITE_INTEGER ||
      sqlite3_column_int64(statement, 4) <= 0 || sqlite3_column_int64(statement, 5) <= 0 ||
      sqlite3_column_int64(statement, 6) <= 0) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_copy_column(statement, 0, view.domain_id, sizeof(view.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 1, view.principal_id, sizeof(view.principal_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 2, view.principal_type, sizeof(view.principal_type));
  if (rc != TURBO_OK) goto done;
  view.enabled = sqlite3_column_int(statement, 3);
  if (view.enabled != 0 && view.enabled != 1) {
    rc = TURBO_EPROTO;
    goto done;
  }
  view.revision = (uint64_t)sqlite3_column_int64(statement, 4);
  view.created_at = (uint64_t)sqlite3_column_int64(statement, 5);
  view.updated_at = (uint64_t)sqlite3_column_int64(statement, 6);
  *out = view;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  return rc;
}

void flowie_control_generated_credential_wipe(flowie_control_generated_credential_t *credential) {
  if (!credential || credential->size < sizeof(*credential)) return;
  flowie_control_credential_wipe(credential->token, sizeof(credential->token));
  credential->token_size = 0u;
}

static int flowie_control_store_credential_issue(
    flowie_control_store_t *store, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result, const char *operation, int require_existing) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_credential_record_t record = {0};
  flowie_control_credential_kdf_params_t params;
  flowie_control_command_result_t replay = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  char token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY] = {0};
  uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE] = {0};
  uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE] = {0};
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result)) {
    flowie_control_generated_credential_wipe(result);
    *result = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  }
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || !operation ||
      ((!command->initial_secret && command->initial_secret_size != 0u) ||
       (command->initial_secret &&
        (command->initial_secret_size == 0u ||
         command->initial_secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX))) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  flowie_control_credential_default_params(&params);
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_replay(database, command->request_id, command->actor, operation,
                             command->domain_id, command->principal_id,
                             FLOWIE_CONTROL_DETAIL_ARGON2ID, &replay, &found);
  if (rc != TURBO_OK) goto done;
  if (found) {
    rc = TURBO_EALREADY;
    goto done;
  }
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_credential_record_read(database, command->domain_id,
                                             command->principal_id, &record);
  if (rc != TURBO_OK) goto done;
  if (!record.user_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (require_existing && !record.credential_exists) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (!require_existing && record.credential_exists) {
    rc = TURBO_EALREADY;
    goto done;
  }
  flowie_control_credential_wipe(&record, sizeof(record));
  (void)sqlite3_close(database);
  database = NULL;

  if (command->initial_secret)
    rc = flowie_control_credential_hash(command->initial_secret, command->initial_secret_size, salt,
                                        verifier, &params);
  else rc = flowie_control_credential_generate(token, salt, verifier, &params);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  replay = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  found = 0;
  rc = flowie_control_replay(database, command->request_id, command->actor, operation,
                             command->domain_id, command->principal_id,
                             FLOWIE_CONTROL_DETAIL_ARGON2ID, &replay, &found);
  if (rc != TURBO_OK) goto done;
  if (found) {
    rc = TURBO_EALREADY;
    goto done;
  }
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_credential_record_read(database, command->domain_id,
                                             command->principal_id, &record);
  if (rc != TURBO_OK) goto done;
  if (!record.user_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (require_existing && !record.credential_exists) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (!require_existing && record.credential_exists) {
    rc = TURBO_EALREADY;
    goto done;
  }
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      require_existing
          ? "UPDATE flowie_control_credential SET kdf_algorithm=?3,memory_blocks=?4,passes=?5,"
            "lanes=?6,salt=?7,verifier=?8,enabled=1,revision=?9,updated_at=?10 WHERE "
            "domain_id=?1 AND principal_id=?2"
          : "INSERT INTO flowie_control_credential(domain_id,principal_id,kdf_algorithm,"
            "memory_blocks,passes,lanes,salt,verifier,enabled,revision,created_at,updated_at) "
            "VALUES(?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?10,?10)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 3, params.algorithm) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 4, params.memory_blocks) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 5, params.passes) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 6, params.lanes) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) rc = flowie_control_bind_blob(statement, 7, salt, sizeof(salt));
  if (rc == TURBO_OK) rc = flowie_control_bind_blob(statement, 8, verifier, sizeof(verifier));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 9, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 10, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_EBUSY : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor, operation,
                                   command->domain_id, command->principal_id,
                                   FLOWIE_CONTROL_DETAIL_ARGON2ID, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  result->revision = next;
  if (!command->initial_secret) {
    memcpy(result->token, token, sizeof(result->token));
    result->token_size = FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE;
  }
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started && database) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  if (database) (void)sqlite3_close(database);
  flowie_control_credential_wipe(&record, sizeof(record));
  flowie_control_credential_wipe(token, sizeof(token));
  flowie_control_credential_wipe(salt, sizeof(salt));
  flowie_control_credential_wipe(verifier, sizeof(verifier));
  if (rc != TURBO_OK && result && result->size >= sizeof(*result))
    *result = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  return rc;
}

int flowie_control_store_credential_generate(
    flowie_control_store_t *store, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result) {
  return flowie_control_store_credential_issue(store, command, result,
                                               FLOWIE_CONTROL_OPERATION_CREDENTIAL_GENERATE, 0);
}

int flowie_control_store_credential_rotate(flowie_control_store_t *store,
                                           const flowie_control_credential_issue_command_t *command,
                                           flowie_control_generated_credential_t *result) {
  return flowie_control_store_credential_issue(store, command, result,
                                               FLOWIE_CONTROL_OPERATION_CREDENTIAL_ROTATE, 1);
}

int flowie_control_store_credential_revoke(
    flowie_control_store_t *store, const flowie_control_credential_revoke_command_t *command,
    flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_credential_record_t record = {0};
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_CREDENTIAL_REVOKE, command->domain_id,
                             command->principal_id, FLOWIE_CONTROL_TARGET_CREDENTIAL, result,
                             &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_credential_record_read(database, command->domain_id,
                                             command->principal_id, &record);
  if (rc != TURBO_OK) goto done;
  if (!record.credential_exists) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (!record.credential_enabled) {
    rc = TURBO_EALREADY;
    goto done;
  }
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "UPDATE flowie_control_credential SET enabled=0,revision=?1,updated_at=?2 WHERE "
      "domain_id=?3 AND principal_id=?4 AND enabled=1",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  if (sqlite3_bind_int64(statement, 1, (sqlite3_int64)next) != SQLITE_OK ||
      sqlite3_bind_int64(statement, 2, (sqlite3_int64)command->occurred_at) != SQLITE_OK) {
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
    goto done;
  }
  rc = flowie_control_bind_text(statement, 3, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 4, command->principal_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_EBUSY : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_CREDENTIAL_REVOKE,
                                   command->domain_id, command->principal_id,
                                   FLOWIE_CONTROL_TARGET_CREDENTIAL, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  flowie_control_credential_wipe(&record, sizeof(record));
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_credential_verify(flowie_control_store_t *store, const char *domain_id,
                                           const char *principal_id, const void *secret,
                                           size_t secret_size,
                                           flowie_control_credential_verify_result_t *result) {
  flowie_control_credential_record_t record = {0};
  flowie_control_credential_record_t fresh = {0};
  flowie_control_credential_kdf_params_t dummy_params;
  uint8_t dummy_salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE] = {0};
  uint8_t dummy_verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE] = {0};
  sqlite3 *database = NULL;
  int user_exists = 1;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !secret ||
      secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !result ||
      result->size < sizeof(*result))
    return TURBO_EINVAL;
  flowie_control_credential_default_params(&dummy_params);
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_credential_record_read(database, domain_id, principal_id, &record);
  (void)sqlite3_close(database);
  database = NULL;
  if (rc == TURBO_ENOENT) {
    user_exists = 0;
    rc = TURBO_OK;
  }
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_credential_verify(secret, secret_size,
                                        record.credential_exists ? record.salt : dummy_salt,
                                        record.credential_exists ? record.verifier : dummy_verifier,
                                        record.credential_exists ? &record.params : &dummy_params);
  if (rc != TURBO_OK) goto done;
  if (!user_exists || !record.user_enabled || !record.credential_exists ||
      !record.credential_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_credential_record_read(database, domain_id, principal_id, &fresh);
  if (rc != TURBO_OK) goto done;
  if (!fresh.user_enabled || !fresh.credential_exists || !fresh.credential_enabled ||
      fresh.user_revision != record.user_revision ||
      fresh.credential_revision != record.credential_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  result->user_revision = fresh.user_revision;
  result->credential_revision = fresh.credential_revision;
  rc = TURBO_OK;

done:
  if (database) (void)sqlite3_close(database);
  flowie_control_credential_wipe(&record, sizeof(record));
  flowie_control_credential_wipe(&fresh, sizeof(fresh));
  flowie_control_credential_wipe(dummy_salt, sizeof(dummy_salt));
  flowie_control_credential_wipe(dummy_verifier, sizeof(dummy_verifier));
  if (rc != TURBO_OK && result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  return rc;
}

int flowie_control_store_credential_resolve(
    flowie_control_store_t *store, const char *principal_id, const void *secret,
    size_t secret_size, flowie_control_credential_resolution_t *result) {
  static const char sql[] =
      "SELECT u.domain_id FROM flowie_control_user u "
      "JOIN flowie_control_credential c ON c.domain_id=u.domain_id "
      "AND c.principal_id=u.principal_id "
      "WHERE u.principal_id=?1 AND u.enabled=1 AND c.enabled=1 "
      "ORDER BY u.domain_id LIMIT 2";
  flowie_control_credential_resolution_t resolved = FLOWIE_CONTROL_CREDENTIAL_RESOLUTION_INIT;
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  size_t match_count = 0u;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result)) *result = resolved;
  if (!store || !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !secret ||
      secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !result ||
      result->size < sizeof(*result))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status == SQLITE_OK)
    status = sqlite3_bind_text(statement, 1, principal_id, -1, SQLITE_TRANSIENT);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char *domain;
    int domain_size;
    if (sqlite3_column_type(statement, 0) != SQLITE_TEXT ||
        !(domain = sqlite3_column_text(statement, 0)) ||
        (domain_size = sqlite3_column_bytes(statement, 0)) <= 0 ||
        (size_t)domain_size > TURBO_FLOW_SECURITY_ID_MAX) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (match_count == 0u) {
      memcpy(resolved.domain_id, domain, (size_t)domain_size);
      resolved.domain_id[domain_size] = '\0';
    }
    ++match_count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  (void)sqlite3_close(database);
  database = NULL;
  if (match_count != 1u) {
    flowie_control_credential_verify_result_t dummy = FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    (void)flowie_control_store_credential_verify(store, "__unresolved__", principal_id, secret,
                                                 secret_size, &dummy);
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_store_credential_verify(store, resolved.domain_id, principal_id, secret,
                                               secret_size, &resolved.verified);
  if (rc == TURBO_OK) *result = resolved;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (database) (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_credential_resolution_t)
      FLOWIE_CONTROL_CREDENTIAL_RESOLUTION_INIT;
  return rc;
}

int flowie_control_store_credential_state(flowie_control_store_t *store, const char *domain_id,
                                          const char *principal_id,
                                          flowie_control_credential_verify_result_t *result) {
  flowie_control_credential_record_t record = {0};
  sqlite3 *database = NULL;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !result ||
      result->size < sizeof(*result))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_credential_record_read(database, domain_id, principal_id, &record);
  if (rc == TURBO_ENOENT) rc = TURBO_EPERM;
  if (rc != TURBO_OK) goto done;
  if (!record.user_enabled || !record.credential_exists || !record.credential_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  result->user_revision = record.user_revision;
  result->credential_revision = record.credential_revision;
  rc = TURBO_OK;

done:
  if (database) (void)sqlite3_close(database);
  flowie_control_credential_wipe(&record, sizeof(record));
  if (rc != TURBO_OK && result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  return rc;
}

int flowie_control_store_current_revision(flowie_control_store_t *store, uint64_t *revision_out) {
  sqlite3 *database = NULL;
  int rc;
  if (revision_out) *revision_out = 0u;
  if (!store || !revision_out) return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc == TURBO_OK) rc = flowie_control_read_revision(database, revision_out);
  if (database) (void)sqlite3_close(database);
  if (rc != TURBO_OK) *revision_out = 0u;
  return rc;
}

int flowie_control_store_principal_snapshot(
    flowie_control_store_t *store, const char *domain_id, const char *principal_id,
    const flowie_control_credential_verify_result_t *expected,
    flowie_control_principal_snapshot_t *out) {
  static const char sql[] =
      "SELECT u.domain_id,u.principal_id,u.principal_type,u.enabled,u.revision,"
      "c.enabled,c.revision FROM flowie_control_user u LEFT JOIN flowie_control_credential c "
      "ON c.domain_id=u.domain_id AND c.principal_id=u.principal_id "
      "WHERE u.domain_id=?1 AND u.principal_id=?2";
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  sqlite3_int64 user_revision;
  sqlite3_int64 credential_revision;
  int transaction_started = 0;
  int status;
  int rc;

  if (out && out->size >= sizeof(*out)) *out = snapshot;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !expected ||
      expected->size < sizeof(*expected) || expected->user_revision == 0u ||
      expected->credential_revision == 0u || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;

  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "BEGIN", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 6) != SQLITE_INTEGER ||
      (sqlite3_column_int(statement, 3) != 0 && sqlite3_column_int(statement, 3) != 1) ||
      (sqlite3_column_int(statement, 5) != 0 && sqlite3_column_int(statement, 5) != 1)) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  user_revision = sqlite3_column_int64(statement, 4);
  credential_revision = sqlite3_column_int64(statement, 6);
  if (!sqlite3_column_int(statement, 3) || !sqlite3_column_int(statement, 5) ||
      user_revision <= 0 || credential_revision <= 0 ||
      (uint64_t)user_revision != expected->user_revision ||
      (uint64_t)credential_revision != expected->credential_revision) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_copy_column(statement, 0, snapshot.domain_id,
                                  sizeof(snapshot.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 1, snapshot.principal_id,
                                    sizeof(snapshot.principal_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 2, snapshot.principal_type,
                                    sizeof(snapshot.principal_type));
  if (rc != TURBO_OK) goto done;
  snapshot.user_revision = (uint64_t)user_revision;
  snapshot.credential_revision = (uint64_t)credential_revision;
  (void)sqlite3_finalize(statement);
  statement = NULL;

  rc = flowie_control_effective_groups_database(database, domain_id, principal_id,
                                                &snapshot.effective_groups);
  if (rc == TURBO_OK)
    rc = flowie_control_effective_roles_database(database, domain_id, principal_id,
                                                 &snapshot.effective_roles);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  *out = snapshot;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started && database) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  if (database) (void)sqlite3_close(database);
  if (rc != TURBO_OK && out && out->size >= sizeof(*out))
    *out = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  return rc;
}

int flowie_control_store_external_principal_snapshot(flowie_control_store_t *store,
                                                     const char *domain_id,
                                                     const char *principal_id,
                                                     uint64_t assertion_revision,
                                                     flowie_control_principal_snapshot_t *out) {
  static const char sql[] = "SELECT domain_id,principal_id,principal_type,enabled,revision "
                            "FROM flowie_control_user WHERE domain_id=?1 AND principal_id=?2";
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  sqlite3_int64 user_revision;
  int transaction_started = 0;
  int status;
  int rc;

  if (out && out->size >= sizeof(*out)) *out = snapshot;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      assertion_revision == 0u || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;

  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "BEGIN", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, principal_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      (sqlite3_column_int(statement, 3) != 0 && sqlite3_column_int(statement, 3) != 1)) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  user_revision = sqlite3_column_int64(statement, 4);
  if (!sqlite3_column_int(statement, 3) || user_revision <= 0) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_copy_column(statement, 0, snapshot.domain_id,
                                  sizeof(snapshot.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 1, snapshot.principal_id,
                                    sizeof(snapshot.principal_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 2, snapshot.principal_type,
                                    sizeof(snapshot.principal_type));
  if (rc != TURBO_OK) goto done;
  snapshot.user_revision = (uint64_t)user_revision;
  snapshot.credential_revision = assertion_revision;
  (void)sqlite3_finalize(statement);
  statement = NULL;

  rc = flowie_control_effective_groups_database(database, domain_id, principal_id,
                                                &snapshot.effective_groups);
  if (rc == TURBO_OK)
    rc = flowie_control_effective_roles_database(database, domain_id, principal_id,
                                                 &snapshot.effective_roles);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  *out = snapshot;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started && database) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  if (database) (void)sqlite3_close(database);
  if (rc != TURBO_OK && out && out->size >= sizeof(*out))
    *out = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  return rc;
}

int flowie_control_store_membership_add(flowie_control_store_t *store,
                                        const flowie_control_membership_add_command_t *command,
                                        flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_effective_groups_view_t effective = FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint32_t group_depth = 0u;
  int group_enabled = 0;
  int user_enabled = 0;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->group_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      strcmp(command->group_id, command->domain_id) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_MEMBERSHIP_ADD, command->domain_id,
                             command->principal_id, command->group_id, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_user_enabled(database, command->domain_id, command->principal_id,
                                   &user_enabled);
  if (rc != TURBO_OK) goto done;
  if (!user_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_group_lookup(database, command->domain_id, command->group_id,
                                   &group_depth, &group_enabled);
  if (rc != TURBO_OK) goto done;
  if (!group_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_membership(domain_id,principal_id,group_id,revision,"
      "created_at) VALUES(?1,?2,?3,?4,?5)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, command->group_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 4, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 5, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE
             ? TURBO_OK
             : ((status & 0xff) == SQLITE_CONSTRAINT ? TURBO_EALREADY
                                                     : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_effective_groups_database(database, command->domain_id,
                                                command->principal_id, &effective);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(
      database, command->request_id, command->actor, FLOWIE_CONTROL_OPERATION_MEMBERSHIP_ADD,
      command->domain_id, command->principal_id, command->group_id, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_membership_remove(
    flowie_control_store_t *store, const flowie_control_membership_remove_command_t *command,
    flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->group_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      strcmp(command->group_id, command->domain_id) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_MEMBERSHIP_REMOVE, command->domain_id,
                             command->principal_id, command->group_id, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  status = sqlite3_prepare_v2(
      database,
      "DELETE FROM flowie_control_membership WHERE domain_id=?1 AND principal_id=?2 AND "
      "group_id=?3",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, command->group_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_ENOENT : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(
      database, command->request_id, command->actor, FLOWIE_CONTROL_OPERATION_MEMBERSHIP_REMOVE,
      command->domain_id, command->principal_id, command->group_id, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_effective_groups(flowie_control_store_t *store, const char *domain_id,
                                          const char *principal_id,
                                          flowie_control_effective_groups_view_t *out) {
  sqlite3 *database = NULL;
  flowie_control_effective_groups_view_t view = FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  int enabled = 0;
  int rc;
  if (out && out->size >= sizeof(*out)) *out = view;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_user_enabled(database, domain_id, principal_id, &enabled);
  if (rc == TURBO_OK && !enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK)
    rc = flowie_control_effective_groups_database(database, domain_id, principal_id, &view);
  (void)sqlite3_close(database);
  if (rc == TURBO_OK) *out = view;
  return rc;
}

int flowie_control_store_role_create(flowie_control_store_t *store,
                                     const flowie_control_role_create_command_t *command,
                                     flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->role_id, command->actor,
                                           command->request_id, command->expected_revision,
                                           command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_ROLE_CREATE, command->domain_id,
                             command->role_id, FLOWIE_CONTROL_TARGET_ROLE, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_domain_exists(database, command->domain_id);
  if (rc != TURBO_OK) goto done;
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_role(domain_id,role_id,enabled,revision,created_at,"
      "updated_at) VALUES(?1,?2,1,?3,?4,?4)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->role_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 3, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 4, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE
             ? TURBO_OK
             : ((status & 0xff) == SQLITE_CONSTRAINT ? TURBO_EALREADY
                                                     : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_ROLE_CREATE, command->domain_id,
                                   command->role_id, FLOWIE_CONTROL_TARGET_ROLE, next,
                                   command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_role_disable(flowie_control_store_t *store,
                                      const flowie_control_role_disable_command_t *command,
                                      flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int role_enabled = 0;
  int policy_reference = 0;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->role_id, command->actor,
                                           command->request_id, command->expected_revision,
                                           command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_ROLE_DISABLE, command->domain_id,
                             command->role_id, FLOWIE_CONTROL_TARGET_ROLE, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_role_enabled(database, command->domain_id, command->role_id,
                                   &role_enabled);
  if (rc != TURBO_OK) goto done;
  if (!role_enabled) {
    rc = TURBO_EALREADY;
    goto done;
  }
  rc = flowie_control_policy_subject_referenced(database, command->domain_id,
                                                TURBO_FLOW_SECURITY_SUBJECT_ROLE, command->role_id,
                                                &policy_reference);
  if (rc != TURBO_OK) goto done;
  if (policy_reference) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "UPDATE flowie_control_role SET enabled=0,revision=?1,updated_at=?2 WHERE domain_id=?3 "
      "AND role_id=?4 AND enabled=1",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  if (sqlite3_bind_int64(statement, 1, (sqlite3_int64)next) != SQLITE_OK ||
      sqlite3_bind_int64(statement, 2, (sqlite3_int64)command->occurred_at) != SQLITE_OK) {
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
    goto done;
  }
  rc = flowie_control_bind_text(statement, 3, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 4, command->role_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_EBUSY : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_ROLE_DISABLE, command->domain_id,
                                   command->role_id, FLOWIE_CONTROL_TARGET_ROLE, next,
                                   command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_user_role_add(flowie_control_store_t *store,
                                       const flowie_control_user_role_add_command_t *command,
                                       flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_effective_roles_view_t effective = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int user_enabled = 0;
  int role_enabled = 0;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_USER_ROLE_ADD, command->domain_id,
                             command->principal_id, command->role_id, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_user_enabled(database, command->domain_id, command->principal_id,
                                   &user_enabled);
  if (rc != TURBO_OK) goto done;
  if (!user_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_role_enabled(database, command->domain_id, command->role_id,
                                   &role_enabled);
  if (rc != TURBO_OK) goto done;
  if (!role_enabled) {
    rc = TURBO_EPERM;
    goto done;
  }
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO "
      "flowie_control_user_role(domain_id,principal_id,role_id,revision,created_at) "
      "VALUES(?1,?2,?3,?4,?5)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, command->role_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 4, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 5, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE
             ? TURBO_OK
             : ((status & 0xff) == SQLITE_CONSTRAINT ? TURBO_EALREADY
                                                     : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_effective_roles_database(database, command->domain_id,
                                               command->principal_id, &effective);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(
      database, command->request_id, command->actor, FLOWIE_CONTROL_OPERATION_USER_ROLE_ADD,
      command->domain_id, command->principal_id, command->role_id, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_user_role_remove(flowie_control_store_t *store,
                                          const flowie_control_user_role_remove_command_t *command,
                                          flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->principal_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_USER_ROLE_REMOVE, command->domain_id,
                             command->principal_id, command->role_id, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  status = sqlite3_prepare_v2(
      database,
      "DELETE FROM flowie_control_user_role WHERE domain_id=?1 AND principal_id=?2 AND "
      "role_id=?3",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, command->principal_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, command->role_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_ENOENT : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(
      database, command->request_id, command->actor, FLOWIE_CONTROL_OPERATION_USER_ROLE_REMOVE,
      command->domain_id, command->principal_id, command->role_id, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_effective_roles(flowie_control_store_t *store, const char *domain_id,
                                         const char *principal_id,
                                         flowie_control_effective_roles_view_t *out) {
  sqlite3 *database = NULL;
  flowie_control_effective_roles_view_t view = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  int enabled = 0;
  int rc;
  if (out && out->size >= sizeof(*out)) *out = view;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_user_enabled(database, domain_id, principal_id, &enabled);
  if (rc == TURBO_OK && !enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK)
    rc = flowie_control_effective_roles_database(database, domain_id, principal_id, &view);
  (void)sqlite3_close(database);
  if (rc == TURBO_OK) *out = view;
  return rc;
}

int flowie_control_store_policy_rule_put(flowie_control_store_t *store,
                                         const flowie_control_policy_rule_put_command_t *command,
                                         flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
  size_t expanded_rule_count = 0u;
  size_t deny_rule_count = 0u;
  char target[32];
  size_t line_size;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || command->ordinal >= TURBO_FLOW_SECURITY_MAX_RULES ||
      !flowie_control_command_common_valid(command->domain_id, command->domain_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      !command->rule_line ||
      (line_size = strnlen(command->rule_line, FLOWIE_CONTROL_ACL_DOCUMENT_MAX + 1u)) == 0u ||
      line_size > FLOWIE_CONTROL_ACL_DOCUMENT_MAX ||
      flowie_control_policy_target(command->ordinal, target) != TURBO_OK)
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_POLICY_RULE_PUT, command->domain_id,
                             target, command->rule_line, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_policy_document_validate(
      database, command->domain_id, command->rule_line, line_size, &document,
      &expanded_rule_count, &deny_rule_count);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_policy_subject_unique(database, command->domain_id, command->ordinal,
                                            document.subject);
  if (rc != TURBO_OK) goto done;
  if (current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next = current + 1u;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_policy_draft(domain_id,ordinal,rule_line,revision,updated_at)"
      " VALUES(?1,?2,?3,?4,?5) ON CONFLICT(domain_id,ordinal) DO UPDATE SET "
      "rule_line=excluded.rule_line,revision=excluded.revision,updated_at=excluded.updated_at",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 2, command->ordinal) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 3, command->rule_line);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 4, (sqlite3_int64)next) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 5, (sqlite3_int64)command->occurred_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_POLICY_RULE_PUT, command->domain_id,
                                   target, command->rule_line, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_policy_rule_delete(
    flowie_control_store_t *store, const flowie_control_policy_rule_delete_command_t *command,
    flowie_control_command_result_t *result) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  char target[32];
  uint64_t current = 0u;
  uint64_t next = 0u;
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || command->ordinal >= TURBO_FLOW_SECURITY_MAX_RULES ||
      !flowie_control_command_common_valid(command->domain_id, command->domain_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      flowie_control_policy_target(command->ordinal, target) != TURBO_OK)
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_POLICY_RULE_DELETE, command->domain_id,
                             target, FLOWIE_CONTROL_TARGET_POLICY_RULE, result, &found);
  if (rc != TURBO_OK) goto done;
  if (found) goto commit;
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  status = sqlite3_prepare_v2(
      database, "DELETE FROM flowie_control_policy_draft WHERE domain_id=?1 AND ordinal=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 2, command->ordinal) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE && sqlite3_changes(database) == 1
             ? TURBO_OK
             : (status == SQLITE_DONE ? TURBO_ENOENT : flowie_control_sqlite_status(status));
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(database, command->request_id, command->actor,
                                   FLOWIE_CONTROL_OPERATION_POLICY_RULE_DELETE,
                                   command->domain_id, target,
                                   FLOWIE_CONTROL_TARGET_POLICY_RULE, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_store_policy_validate(flowie_control_store_t *store, const char *domain_id,
                                         flowie_control_policy_validation_t *out) {
  sqlite3 *database = NULL;
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  int transaction_started = 0;
  int status;
  int rc;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_policy_validation_t)FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_policy_validate_database(database, domain_id, &validation);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  *out = validation;
  rc = TURBO_OK;

done:
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  return rc;
}

int flowie_control_store_policy_rule_list(flowie_control_store_t *store, const char *domain_id,
                                          uint32_t after_ordinal, int has_after,
                                          flowie_control_policy_rule_view_t *items,
                                          size_t item_capacity, size_t *count_out,
                                          int *has_more_out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  size_t count = 0u;
  int status;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !items ||
      item_capacity == 0u || item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out ||
      !has_more_out || (has_after != 0 && has_after != 1))
    return TURBO_EINVAL;
  for (size_t i = 0u; i < item_capacity; ++i) {
    if (items[i].size < sizeof(items[i])) return TURBO_EINVAL;
    items[i] = (flowie_control_policy_rule_view_t)FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
  }
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(
      database,
      "SELECT ordinal,rule_line,revision,updated_at FROM flowie_control_policy_draft "
      "WHERE domain_id=?1 AND (?2=0 OR ordinal>?3) ORDER BY ordinal LIMIT ?4",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK && sqlite3_bind_int(statement, 2, has_after) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 3, after_ordinal) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 4, (sqlite3_int64)(item_capacity + 1u)) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    sqlite3_int64 ordinal;
    sqlite3_int64 revision;
    sqlite3_int64 updated_at;
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    if (sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 1) != SQLITE_TEXT ||
        sqlite3_column_type(statement, 2) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
        (ordinal = sqlite3_column_int64(statement, 0)) < 0 || ordinal > UINT32_MAX ||
        (revision = sqlite3_column_int64(statement, 2)) <= 0 ||
        (updated_at = sqlite3_column_int64(statement, 3)) <= 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    items[count].ordinal = (uint32_t)ordinal;
    rc = flowie_control_copy_column(statement, 1, items[count].rule_line,
                                    sizeof(items[count].rule_line));
    if (rc != TURBO_OK) goto done;
    items[count].revision = (uint64_t)revision;
    items[count].updated_at = (uint64_t)updated_at;
    ++count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  *count_out = count;
  rc = TURBO_OK;

done:
  (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

int flowie_control_store_policy_status(flowie_control_store_t *store, const char *domain_id,
                                       flowie_control_policy_status_t *out) {
  flowie_control_policy_status_t view = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  sqlite3_int64 draft_count;
  sqlite3_int64 published_count;
  int status;
  int rc;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_policy_status_t)FLOWIE_CONTROL_POLICY_STATUS_INIT;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_read_revision(database, &view.store_revision);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "SELECT (SELECT COUNT(*) FROM flowie_control_policy_draft WHERE domain_id=?1),"
      "b.policy_version,b.expires_at,(SELECT COUNT(*) FROM turbo_flow_acl_rule_v3 "
      "WHERE namespace_name=?1) FROM turbo_flow_acl_bundle_v3 b WHERE b.namespace_name=?1",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    (void)sqlite3_finalize(statement);
    statement = NULL;
    status = sqlite3_prepare_v2(
        database, "SELECT COUNT(*) FROM flowie_control_policy_draft WHERE domain_id=?1", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
      rc = flowie_control_sqlite_status(status);
      goto done;
    }
    rc = flowie_control_bind_text(statement, 1, domain_id);
    if (rc != TURBO_OK) goto done;
    status = sqlite3_step(statement);
    if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        (draft_count = sqlite3_column_int64(statement, 0)) < 0 ||
        (uint64_t)draft_count > SIZE_MAX) {
      rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
      goto done;
    }
    view.draft_rule_count = (size_t)draft_count;
  } else {
    if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 2) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
        (draft_count = sqlite3_column_int64(statement, 0)) < 0 ||
        sqlite3_column_int64(statement, 1) <= 0 || sqlite3_column_int64(statement, 2) < 0 ||
        (published_count = sqlite3_column_int64(statement, 3)) < 0 ||
        (uint64_t)draft_count > SIZE_MAX || (uint64_t)published_count > SIZE_MAX) {
      rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
      goto done;
    }
    view.draft_rule_count = (size_t)draft_count;
    view.policy_version = (uint64_t)sqlite3_column_int64(statement, 1);
    view.expires_at = (uint64_t)sqlite3_column_int64(statement, 2);
    view.published_rule_count = (size_t)published_count;
  }
  *out = view;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  return rc;
}

void flowie_control_store_policy_bundle_release(turbo_flow_security_policy_bundle_t *bundle) {
  flowie_control_policy_bundle_owner_t *owner;
  if (!bundle) return;
  owner = (flowie_control_policy_bundle_owner_t *)bundle->provider_bundle;
  if (owner) {
    free(owner->rules);
    free(owner);
  }
  *bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
}

int flowie_control_store_policy_bundle_load(flowie_control_store_t *store,
                                            const char *domain_id, uint64_t required_version,
                                            turbo_flow_security_policy_bundle_t *bundle_out) {
  flowie_control_policy_bundle_owner_t *owner = NULL;
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  sqlite3_int64 rule_count_value;
  size_t expected_ordinal = 0u;
  size_t rule_count = 0u;
  uint64_t policy_version = 0u;
  uint64_t expires_at = 0u;
  int transaction_started = 0;
  int status;
  int rc;
  if (bundle_out && bundle_out->size >= sizeof(*bundle_out))
    *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      required_version > (uint64_t)INT64_MAX || !bundle_out ||
      bundle_out->size < sizeof(*bundle_out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  status = sqlite3_prepare_v2(
      database,
      "SELECT b.policy_version,b.expires_at,"
      "(SELECT COUNT(*) FROM turbo_flow_acl_rule_v3 r WHERE r.namespace_name=b.namespace_name) "
      "FROM turbo_flow_acl_bundle_v3 b WHERE b.namespace_name=?1 "
      "AND (?2=0 OR b.policy_version=?2)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 2, (sqlite3_int64)required_version) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 2) != SQLITE_INTEGER ||
      sqlite3_column_int64(statement, 0) <= 0 || sqlite3_column_int64(statement, 1) < 0 ||
      (rule_count_value = sqlite3_column_int64(statement, 2)) <= 0 ||
      rule_count_value > (sqlite3_int64)TURBO_FLOW_SECURITY_MAX_RULES) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  policy_version = (uint64_t)sqlite3_column_int64(statement, 0);
  expires_at = (uint64_t)sqlite3_column_int64(statement, 1);
  rule_count = (size_t)rule_count_value;
  (void)sqlite3_finalize(statement);
  statement = NULL;

  owner = (flowie_control_policy_bundle_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  owner->rules = (turbo_flow_security_rule_t *)calloc(rule_count, sizeof(*owner->rules));
  if (!owner->rules) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  status = sqlite3_prepare_v2(database,
                              "SELECT ordinal,rule_line FROM turbo_flow_acl_rule_v3 "
                              "WHERE namespace_name=?1 ORDER BY ordinal",
                              -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char *line;
    int line_size;
    sqlite3_int64 ordinal;
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    if (expected_ordinal >= rule_count || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 1) != SQLITE_TEXT) {
      rc = TURBO_EPROTO;
      goto done;
    }
    ordinal = sqlite3_column_int64(statement, 0);
    line = sqlite3_column_text(statement, 1);
    line_size = sqlite3_column_bytes(statement, 1);
    if (ordinal < 0 || (uint64_t)ordinal != (uint64_t)expected_ordinal || !line || line_size <= 0 ||
        (size_t)line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX ||
        memchr(line, '\0', (size_t)line_size) ||
        turbo_flow_security_rule_parse_line((const char *)line, (size_t)line_size, &rule) !=
            TURBO_OK ||
        strcmp(rule.domain_id, domain_id) != 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    owner->rules[expected_ordinal++] = rule;
  }
  if (status != SQLITE_DONE || expected_ordinal != rule_count) {
    rc = status == SQLITE_DONE ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  bundle_out->policy_version = policy_version;
  bundle_out->expires_at = expires_at;
  bundle_out->rules = owner->rules;
  bundle_out->rule_count = rule_count;
  bundle_out->provider_bundle = owner;
  owner = NULL;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  if (database) (void)sqlite3_close(database);
  if (owner) {
    free(owner->rules);
    free(owner);
  }
  if (rc != TURBO_OK)
    *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  return rc;
}

int flowie_control_store_policy_publish(flowie_control_store_t *store,
                                        const flowie_control_policy_publish_command_t *command,
                                        flowie_control_policy_publish_result_t *result) {
  flowie_control_command_result_t replay = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  sqlite3_stmt *draft = NULL;
  sqlite3_stmt *insert_rule = NULL;
  turbo_flow_security_rule_t *compiled_rules = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint64_t current_policy = 0u;
  uint64_t next_policy = 0u;
  size_t ordinal = 0u;
  char publish_detail[64];
  int transaction_started = 0;
  int found = 0;
  int status;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_policy_publish_result_t)FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  if (!store || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_command_common_valid(command->domain_id, command->domain_id,
                                           command->actor, command->request_id,
                                           command->expected_revision, command->occurred_at) ||
      command->expires_at > (uint64_t)INT64_MAX ||
      (command->expires_at != 0u && command->expires_at <= command->occurred_at) ||
      flowie_control_policy_publish_detail(command->expires_at, publish_detail) != TURBO_OK)
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 1;
  rc = flowie_control_replay(database, command->request_id, command->actor,
                             FLOWIE_CONTROL_OPERATION_POLICY_PUBLISH, command->domain_id,
                             command->domain_id, publish_detail, &replay, &found);
  if (rc != TURBO_OK) goto done;
  if (found) {
    status = sqlite3_prepare_v2(
        database,
        "SELECT policy_version FROM flowie_control_policy_publish_result WHERE request_id=?1", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
      rc = flowie_control_sqlite_status(status);
      goto done;
    }
    rc = flowie_control_bind_text(statement, 1, command->request_id);
    if (rc != TURBO_OK) goto done;
    status = sqlite3_step(statement);
    if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        sqlite3_column_int64(statement, 0) <= 0) {
      rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
      goto done;
    }
    result->revision = replay.revision;
    result->policy_version = (uint64_t)sqlite3_column_int64(statement, 0);
    result->replayed = 1;
    (void)sqlite3_finalize(statement);
    statement = NULL;
    goto commit;
  }
  rc = flowie_control_read_revision(database, &current);
  if (rc != TURBO_OK) goto done;
  if (command->expected_revision != 0u && current != command->expected_revision) {
    rc = TURBO_EBUSY;
    goto done;
  }
  rc = flowie_control_policy_validate_database(database, command->domain_id, &validation);
  if (rc != TURBO_OK) goto done;
  compiled_rules = (turbo_flow_security_rule_t *)calloc(validation.rule_count,
                                                        sizeof(*compiled_rules));
  if (!compiled_rules) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  status = sqlite3_prepare_v2(
      database, "SELECT policy_version FROM turbo_flow_acl_bundle_v3 WHERE namespace_name=?1", -1,
      &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_ROW) {
    if (sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
        sqlite3_column_int64(statement, 0) <= 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    current_policy = (uint64_t)sqlite3_column_int64(statement, 0);
  } else if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (current_policy >= (uint64_t)INT64_MAX || current >= (uint64_t)INT64_MAX) {
    rc = TURBO_ERANGE;
    goto done;
  }
  next_policy = current_policy + 1u;
  status = sqlite3_prepare_v2(
      database, "DELETE FROM turbo_flow_acl_rule_v3 WHERE namespace_name=?1", -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO turbo_flow_acl_bundle_v3(namespace_name,policy_version,expires_at) "
      "VALUES(?1,?2,?3) ON CONFLICT(namespace_name) DO UPDATE SET "
      "policy_version=excluded.policy_version,expires_at=excluded.expires_at",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->domain_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 2, (sqlite3_int64)next_policy) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 3, (sqlite3_int64)command->expires_at) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "SELECT rule_line FROM flowie_control_policy_draft WHERE domain_id=?1 ORDER BY ordinal",
      -1, &draft, NULL);
  if (status == SQLITE_OK)
    status = sqlite3_prepare_v2(
        database,
        "INSERT INTO turbo_flow_acl_rule_v3(namespace_name,ordinal,rule_line) VALUES(?1,?2,?3)", -1,
        &insert_rule, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(draft, 1, command->domain_id);
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(draft)) == SQLITE_ROW) {
    const unsigned char *line;
    int line_size;
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
    size_t compiled_count = 0u;
    if (ordinal >= validation.rule_count || sqlite3_column_type(draft, 0) != SQLITE_TEXT) {
      rc = TURBO_EPROTO;
      goto done;
    }
    line = sqlite3_column_text(draft, 0);
    line_size = sqlite3_column_bytes(draft, 0);
    if (!line || line_size <= 0 ||
        flowie_control_acl_document_syntax_validate(command->domain_id, (const char *)line,
                                                    (size_t)line_size, &document) != TURBO_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    rc = flowie_control_acl_compile(&document, command->domain_id, compiled_rules + ordinal,
                                    validation.rule_count - ordinal, &compiled_count);
    if (rc != TURBO_OK || compiled_count == 0u) {
      rc = rc != TURBO_OK ? rc : TURBO_EPROTO;
      goto done;
    }
    for (size_t index = 0u; index < compiled_count; ++index) {
      char canonical[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u];
      size_t canonical_size = 0u;
      rc = turbo_flow_security_rule_format_line(&compiled_rules[ordinal + index], canonical,
                                                sizeof(canonical), &canonical_size);
      if (rc != TURBO_OK) goto done;
      (void)sqlite3_reset(insert_rule);
      (void)sqlite3_clear_bindings(insert_rule);
      rc = flowie_control_bind_text(insert_rule, 1, command->domain_id);
      if (rc == TURBO_OK &&
          sqlite3_bind_int64(insert_rule, 2, (sqlite3_int64)(ordinal + index)) != SQLITE_OK)
        rc = flowie_control_sqlite_status(sqlite3_errcode(database));
      if (rc == TURBO_OK &&
          sqlite3_bind_text(insert_rule, 3, canonical, (int)canonical_size, SQLITE_TRANSIENT) !=
              SQLITE_OK)
        rc = flowie_control_sqlite_status(sqlite3_errcode(database));
      if (rc == TURBO_OK) {
        int insert_status = sqlite3_step(insert_rule);
        rc = insert_status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(insert_status);
      }
      if (rc != TURBO_OK) goto done;
    }
    ordinal += compiled_count;
  }
  if (status != SQLITE_DONE || ordinal != validation.rule_count) {
    rc = status == SQLITE_DONE ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  (void)sqlite3_finalize(draft);
  draft = NULL;
  (void)sqlite3_finalize(insert_rule);
  insert_rule = NULL;
  rc = flowie_control_advance_revision(database, current, &next);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_insert_audit(
      database, command->request_id, command->actor, FLOWIE_CONTROL_OPERATION_POLICY_PUBLISH,
      command->domain_id, command->domain_id, publish_detail, next, command->occurred_at);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO flowie_control_policy_publish_result(request_id,policy_version) VALUES(?1,?2)",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, command->request_id);
  if (rc == TURBO_OK && sqlite3_bind_int64(statement, 2, (sqlite3_int64)next_policy) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK) {
    status = sqlite3_step(statement);
    rc = status == SQLITE_DONE ? TURBO_OK : flowie_control_sqlite_status(status);
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (rc != TURBO_OK) goto done;
  result->revision = next;
  result->policy_version = next_policy;
  result->replayed = 0;

commit:
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  transaction_started = 0;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  if (draft) (void)sqlite3_finalize(draft);
  if (insert_rule) (void)sqlite3_finalize(insert_rule);
  free(compiled_rules);
  if (transaction_started) (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK)
    *result = (flowie_control_policy_publish_result_t)FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  return rc;
}

typedef int (*flowie_control_page_row_fn)(sqlite3_stmt *statement, void *item);

int flowie_control_store_domain_get(flowie_control_store_t *store, const char *domain_id,
                                        flowie_control_domain_view_t *out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  flowie_control_domain_view_t view = FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  int status;
  int rc;
  if (out && out->size >= sizeof(*out)) *out = view;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(
      database, "SELECT domain_id FROM flowie_control_domain WHERE domain_id=?1",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc != TURBO_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_copy_column(statement, 0, view.domain_id,
                                  sizeof(view.domain_id));
  if (rc == TURBO_OK && sqlite3_step(statement) != SQLITE_DONE) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) *out = view;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  return rc;
}

int flowie_control_store_domain_list(flowie_control_store_t *store,
                                         const char *after_domain_id,
                                         flowie_control_domain_view_t *items,
                                         size_t item_capacity, size_t *count_out,
                                         int *has_more_out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  size_t count = 0u;
  int status;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!store ||
      (after_domain_id &&
       !flowie_control_text_valid(after_domain_id, TURBO_FLOW_SECURITY_ID_MAX)) ||
      !items || item_capacity == 0u || item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out ||
      !has_more_out)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (items[index].size < sizeof(items[index])) return TURBO_EINVAL;
    items[index] = (flowie_control_domain_view_t)FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  }
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(
      database,
      "SELECT domain_id FROM flowie_control_domain "
      "WHERE (?1='' OR domain_id>?1) ORDER BY domain_id LIMIT ?2",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1,
                                after_domain_id ? after_domain_id : "");
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 2, (sqlite3_int64)(item_capacity + 1u)) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    rc = flowie_control_copy_column(statement, 0, items[count].domain_id,
                                    sizeof(items[count].domain_id));
    if (rc != TURBO_OK) goto done;
    ++count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  *count_out = count;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

static int flowie_control_page_arguments_valid(flowie_control_store_t *store,
                                               const char *domain_id, const char *after_id,
                                               const void *items, size_t item_size,
                                               size_t item_capacity, size_t *count_out,
                                               int *has_more_out) {
  const uint8_t *cursor = (const uint8_t *)items;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      (after_id && !flowie_control_text_valid(after_id, TURBO_FLOW_SECURITY_ID_MAX)) || !items ||
      item_size < sizeof(size_t) || item_capacity == 0u ||
      item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out || !has_more_out)
    return 0;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (*(const size_t *)(cursor + index * item_size) < item_size) return 0;
  }
  return 1;
}

static int flowie_control_text_page(flowie_control_store_t *store, const char *domain_id,
                                    const char *after_id, const char *sql, void *items,
                                    size_t item_size, size_t item_capacity,
                                    flowie_control_page_row_fn decode, size_t *count_out,
                                    int *has_more_out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  uint8_t *cursor = (uint8_t *)items;
  size_t count = 0u;
  int status;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!sql || !decode ||
      !flowie_control_page_arguments_valid(store, domain_id, after_id, items, item_size,
                                           item_capacity, count_out, has_more_out))
    return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(database, sql, -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_bind_text(statement, 2, after_id ? after_id : "");
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 3, (sqlite3_int64)(item_capacity + 1u)) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    rc = decode(statement, cursor + count * item_size);
    if (rc != TURBO_OK) goto done;
    ++count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  *count_out = count;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

static int flowie_control_user_page_row(sqlite3_stmt *statement, void *item) {
  flowie_control_user_view_t *view = (flowie_control_user_view_t *)item;
  sqlite3_int64 revision;
  sqlite3_int64 created_at;
  sqlite3_int64 updated_at;
  int enabled;
  int rc;
  if (sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 6) != SQLITE_INTEGER ||
      (enabled = sqlite3_column_int(statement, 3)) < 0 || enabled > 1 ||
      (revision = sqlite3_column_int64(statement, 4)) <= 0 ||
      (created_at = sqlite3_column_int64(statement, 5)) <= 0 ||
      (updated_at = sqlite3_column_int64(statement, 6)) <= 0)
    return TURBO_EPROTO;
  *view = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
  rc = flowie_control_copy_column(statement, 0, view->domain_id, sizeof(view->domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 1, view->principal_id, sizeof(view->principal_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 2, view->principal_type,
                                    sizeof(view->principal_type));
  if (rc != TURBO_OK) return rc;
  view->enabled = enabled;
  view->revision = (uint64_t)revision;
  view->created_at = (uint64_t)created_at;
  view->updated_at = (uint64_t)updated_at;
  return TURBO_OK;
}

static int flowie_control_group_page_row(sqlite3_stmt *statement, void *item) {
  flowie_control_group_view_t *view = (flowie_control_group_view_t *)item;
  sqlite3_int64 depth;
  sqlite3_int64 revision;
  sqlite3_int64 created_at;
  sqlite3_int64 updated_at;
  int enabled;
  int rc;
  if (sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 6) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 7) != SQLITE_INTEGER ||
      (depth = sqlite3_column_int64(statement, 3)) < 0 || depth > FLOWIE_CONTROL_GROUP_MAX_DEPTH ||
      (enabled = sqlite3_column_int(statement, 4)) < 0 || enabled > 1 ||
      (revision = sqlite3_column_int64(statement, 5)) <= 0 ||
      (created_at = sqlite3_column_int64(statement, 6)) <= 0 ||
      (updated_at = sqlite3_column_int64(statement, 7)) <= 0)
    return TURBO_EPROTO;
  *view = (flowie_control_group_view_t)FLOWIE_CONTROL_GROUP_VIEW_INIT;
  rc = flowie_control_copy_column(statement, 0, view->domain_id, sizeof(view->domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 1, view->group_id, sizeof(view->group_id));
  if (rc == TURBO_OK && sqlite3_column_type(statement, 2) != SQLITE_NULL)
    rc = flowie_control_copy_column(statement, 2, view->parent_group_id,
                                    sizeof(view->parent_group_id));
  if (rc != TURBO_OK) return rc;
  view->depth = (uint32_t)depth;
  view->enabled = enabled;
  view->revision = (uint64_t)revision;
  view->created_at = (uint64_t)created_at;
  view->updated_at = (uint64_t)updated_at;
  return TURBO_OK;
}

static int flowie_control_role_page_row(sqlite3_stmt *statement, void *item) {
  flowie_control_role_view_t *view = (flowie_control_role_view_t *)item;
  sqlite3_int64 revision;
  sqlite3_int64 created_at;
  sqlite3_int64 updated_at;
  int enabled;
  int rc;
  if (sqlite3_column_type(statement, 2) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 3) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 4) != SQLITE_INTEGER ||
      sqlite3_column_type(statement, 5) != SQLITE_INTEGER ||
      (enabled = sqlite3_column_int(statement, 2)) < 0 || enabled > 1 ||
      (revision = sqlite3_column_int64(statement, 3)) <= 0 ||
      (created_at = sqlite3_column_int64(statement, 4)) <= 0 ||
      (updated_at = sqlite3_column_int64(statement, 5)) <= 0)
    return TURBO_EPROTO;
  *view = (flowie_control_role_view_t)FLOWIE_CONTROL_ROLE_VIEW_INIT;
  rc = flowie_control_copy_column(statement, 0, view->domain_id, sizeof(view->domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_copy_column(statement, 1, view->role_id, sizeof(view->role_id));
  if (rc != TURBO_OK) return rc;
  view->enabled = enabled;
  view->revision = (uint64_t)revision;
  view->created_at = (uint64_t)created_at;
  view->updated_at = (uint64_t)updated_at;
  return TURBO_OK;
}

int flowie_control_store_user_list(flowie_control_store_t *store, const char *domain_id,
                                   const char *after_principal_id,
                                   flowie_control_user_view_t *items, size_t item_capacity,
                                   size_t *count_out, int *has_more_out) {
  static const char sql[] =
      "SELECT domain_id,principal_id,principal_type,enabled,revision,created_at,updated_at "
      "FROM flowie_control_user WHERE domain_id=?1 AND (?2='' OR principal_id>?2) "
      "ORDER BY principal_id LIMIT ?3";
  return flowie_control_text_page(store, domain_id, after_principal_id, sql, items,
                                  sizeof(*items), item_capacity, flowie_control_user_page_row,
                                  count_out, has_more_out);
}

int flowie_control_store_group_list(flowie_control_store_t *store, const char *domain_id,
                                    const char *after_group_id, flowie_control_group_view_t *items,
                                    size_t item_capacity, size_t *count_out, int *has_more_out) {
  static const char sql[] =
      "SELECT domain_id,group_id,parent_group_id,depth,enabled,revision,created_at,updated_at "
      "FROM flowie_control_group WHERE domain_id=?1 AND (?2='' OR group_id>?2) "
      "ORDER BY group_id LIMIT ?3";
  return flowie_control_text_page(store, domain_id, after_group_id, sql, items, sizeof(*items),
                                  item_capacity, flowie_control_group_page_row, count_out,
                                  has_more_out);
}

int flowie_control_store_role_list(flowie_control_store_t *store, const char *domain_id,
                                   const char *after_role_id, flowie_control_role_view_t *items,
                                   size_t item_capacity, size_t *count_out, int *has_more_out) {
  static const char sql[] =
      "SELECT domain_id,role_id,enabled,revision,created_at,updated_at FROM "
      "flowie_control_role "
      "WHERE domain_id=?1 AND (?2='' OR role_id>?2) ORDER BY role_id LIMIT ?3";
  return flowie_control_text_page(store, domain_id, after_role_id, sql, items, sizeof(*items),
                                  item_capacity, flowie_control_role_page_row, count_out,
                                  has_more_out);
}

int flowie_control_store_audit_list(flowie_control_store_t *store, const char *domain_id,
                                    uint64_t after_revision, flowie_control_audit_view_t *items,
                                    size_t item_capacity, size_t *count_out, int *has_more_out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  size_t count = 0u;
  int status;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!store || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !items ||
      item_capacity == 0u || item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out ||
      !has_more_out || after_revision > (uint64_t)INT64_MAX)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (items[index].size < sizeof(items[index])) return TURBO_EINVAL;
    items[index] = (flowie_control_audit_view_t)FLOWIE_CONTROL_AUDIT_VIEW_INIT;
  }
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(
      database,
      "SELECT request_id,actor,operation,domain_id,target_id,target_detail,result_revision,"
      "occurred_at FROM flowie_control_audit WHERE domain_id=?1 AND result_revision>?2 "
      "ORDER BY result_revision LIMIT ?3",
      -1, &statement, NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  rc = flowie_control_bind_text(statement, 1, domain_id);
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 2, (sqlite3_int64)after_revision) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc == TURBO_OK &&
      sqlite3_bind_int64(statement, 3, (sqlite3_int64)(item_capacity + 1u)) != SQLITE_OK)
    rc = flowie_control_sqlite_status(sqlite3_errcode(database));
  if (rc != TURBO_OK) goto done;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    flowie_control_audit_view_t *view;
    sqlite3_int64 revision;
    sqlite3_int64 occurred_at;
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    if (sqlite3_column_type(statement, 6) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 7) != SQLITE_INTEGER ||
        (revision = sqlite3_column_int64(statement, 6)) <= 0 ||
        (occurred_at = sqlite3_column_int64(statement, 7)) <= 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    view = &items[count];
    rc = flowie_control_copy_column(statement, 0, view->request_id, sizeof(view->request_id));
    if (rc == TURBO_OK)
      rc = flowie_control_copy_column(statement, 1, view->actor, sizeof(view->actor));
    if (rc == TURBO_OK)
      rc = flowie_control_copy_column(statement, 2, view->operation, sizeof(view->operation));
    if (rc == TURBO_OK)
      rc = flowie_control_copy_column(statement, 3, view->domain_id,
                                      sizeof(view->domain_id));
    if (rc == TURBO_OK)
      rc = flowie_control_copy_column(statement, 4, view->target_id, sizeof(view->target_id));
    if (rc == TURBO_OK)
      rc = flowie_control_copy_column(statement, 5, view->target_detail,
                                      sizeof(view->target_detail));
    if (rc != TURBO_OK) goto done;
    view->revision = (uint64_t)revision;
    view->occurred_at = (uint64_t)occurred_at;
    ++count;
  }
  if (status != SQLITE_DONE) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  *count_out = count;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  if (rc != TURBO_OK) {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

int flowie_control_store_revision(flowie_control_store_t *store, uint64_t *revision_out) {
  sqlite3 *database = NULL;
  int rc;
  if (revision_out) *revision_out = 0u;
  if (!store || !revision_out) return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_read_revision(database, revision_out);
  (void)sqlite3_close(database);
  return rc;
}

int flowie_control_store_audit_count(flowie_control_store_t *store, size_t *count_out) {
  sqlite3 *database = NULL;
  sqlite3_stmt *statement = NULL;
  sqlite3_int64 count;
  int status;
  int rc;
  if (count_out) *count_out = 0u;
  if (!store || !count_out) return TURBO_EINVAL;
  rc = flowie_control_open_database(store, &database);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(database, "SELECT COUNT(*) FROM flowie_control_audit", -1, &statement,
                              NULL);
  if (status != SQLITE_OK) {
    rc = flowie_control_sqlite_status(status);
    goto done;
  }
  status = sqlite3_step(statement);
  if (status != SQLITE_ROW || sqlite3_column_type(statement, 0) != SQLITE_INTEGER ||
      (count = sqlite3_column_int64(statement, 0)) < 0 || (uint64_t)count > SIZE_MAX) {
    rc = status == SQLITE_ROW ? TURBO_EPROTO : flowie_control_sqlite_status(status);
    goto done;
  }
  *count_out = (size_t)count;
  rc = TURBO_OK;

done:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_close(database);
  return rc;
}
