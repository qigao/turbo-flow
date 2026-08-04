#include "flowie_control_pgsql_command_internal.h"

#include "flowie_control_credential_internal.h"
#include "flowie_control_validation_internal.h"

#include "libpq-fe.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char FLOWIE_CONTROL_PGSQL_OPERATION_DOMAIN_CREATE[] = "domain.create";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_USER_CREATE[] = "user.create";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_USER_DISABLE[] = "user.disable";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_GENERATE[] = "credential.generate";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_ROTATE[] = "credential.rotate";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_REVOKE[] = "credential.revoke";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_GROUP_CREATE[] = "group.create";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_GROUP_DELETE[] = "group.delete";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_MEMBERSHIP_ADD[] = "membership.add";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_MEMBERSHIP_REMOVE[] = "membership.remove";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_ROLE_CREATE[] = "role.create";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_ROLE_DISABLE[] = "role.disable";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_USER_ROLE_ADD[] = "user_role.add";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_USER_ROLE_REMOVE[] = "user_role.remove";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_RULE_PUT[] = "policy.rule.put";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_RULE_DELETE[] = "policy.rule.delete";
static const char FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_PUBLISH[] = "policy.publish";
static const char FLOWIE_CONTROL_PGSQL_TARGET_DOMAIN[] = "domain";
static const char FLOWIE_CONTROL_PGSQL_TARGET_CREDENTIAL[] = "credential";
static const char FLOWIE_CONTROL_PGSQL_TARGET_GROUP[] = "group";
static const char FLOWIE_CONTROL_PGSQL_TARGET_ROLE[] = "role";
static const char FLOWIE_CONTROL_PGSQL_TARGET_POLICY_RULE[] = "policy_rule";
static const char FLOWIE_CONTROL_PGSQL_DETAIL_ARGON2ID[] = "argon2id";

typedef enum flowie_control_pgsql_command_sql_e {
  FLOWIE_CONTROL_PGSQL_COMMAND_REPLAY = 0,
  FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_READ,
  FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_LOCK,
  FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_ADVANCE,
  FLOWIE_CONTROL_PGSQL_COMMAND_AUDIT_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_DOMAIN_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_DOMAIN_LOOKUP,
  FLOWIE_CONTROL_PGSQL_COMMAND_USER_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_USER_LOCK,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_SUBJECT_LINES,
  FLOWIE_CONTROL_PGSQL_COMMAND_USER_DISABLE,
  FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_STATE,
  FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_ROTATE,
  FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_REVOKE,
  FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_LOOKUP,
  FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_REFERENCES,
  FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_DELETE,
  FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_EFFECTIVE_COUNT,
  FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_DELETE,
  FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_LOOKUP,
  FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_DISABLE,
  FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_EFFECTIVE_COUNT,
  FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_DELETE,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_UPSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_DELETE,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_LINES,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_BUNDLE_VERSION,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_RULES_DELETE,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_BUNDLE_UPSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_RULES_COPY,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_PUBLISH_RESULT_LOOKUP,
  FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_PUBLISH_RESULT_INSERT,
  FLOWIE_CONTROL_PGSQL_COMMAND_SQL_COUNT
} flowie_control_pgsql_command_sql_t;

typedef struct flowie_control_pgsql_credential_state_s {
  int user_enabled;
  int credential_exists;
  int credential_enabled;
} flowie_control_pgsql_credential_state_t;

typedef struct flowie_control_pgsql_commit_evidence_s {
  const char *request_id;
  const char *actor;
  const char *operation;
  const char *domain_id;
  const char *target_id;
  const char *target_detail;
  uint64_t revision;
  int present;
} flowie_control_pgsql_commit_evidence_t;

typedef struct flowie_control_pgsql_command_session_s {
  flowie_control_pgsql_command_t *view;
  flowie_control_pgsql_pool_lease_t lease;
  PGconn *connection;
  flowie_control_pgsql_commit_evidence_t evidence;
  int transaction;
} flowie_control_pgsql_command_session_t;

struct flowie_control_pgsql_command_s {
  flowie_control_pgsql_pool_t *pool;
  tstr_t sql[FLOWIE_CONTROL_PGSQL_COMMAND_SQL_COUNT];
};

static int flowie_control_pgsql_command_sql_set(flowie_control_pgsql_command_t *command,
                                                flowie_control_pgsql_command_sql_t index,
                                                const char *format, const char *schema) {
  tstr_t sql;
  tstr_t next;
  if (!command || index >= FLOWIE_CONTROL_PGSQL_COMMAND_SQL_COUNT || !format || !schema)
    return TURBO_EINVAL;
  sql = tstr_new();
  if (!sql) return TURBO_ENOMEM;
  next = tstr_cat_fmt(sql, format, schema, schema, schema, schema, schema, schema, schema, schema);
  if (!next) {
    tstr_free(sql);
    return TURBO_ENOMEM;
  }
  command->sql[index] = next;
  return TURBO_OK;
}

int flowie_control_pgsql_command_create(flowie_control_pgsql_pool_t *pool,
                                        flowie_control_pgsql_command_t **out) {
  flowie_control_pgsql_command_t *command;
  const char *schema;
  int rc;
  if (out) *out = NULL;
  if (!pool || !out || !(schema = flowie_control_pgsql_pool_schema_name(pool))) return TURBO_EINVAL;
  command = (flowie_control_pgsql_command_t *)calloc(1u, sizeof(*command));
  if (!command) return TURBO_ENOMEM;
  command->pool = pool;
  rc = flowie_control_pgsql_command_sql_set(
      command, FLOWIE_CONTROL_PGSQL_COMMAND_REPLAY,
      "SELECT actor,operation,domain_id,target_id,target_detail,result_revision::text "
      "FROM %s.audit WHERE request_id=$1",
      schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_READ,
        "SELECT revision::text FROM %s.meta WHERE singleton=1", schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_LOCK,
        "SELECT revision::text FROM %s.meta WHERE singleton=1 FOR UPDATE", schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_ADVANCE,
        "UPDATE %s.meta SET revision=$1::bigint "
        "WHERE singleton=1 AND revision=$2::bigint RETURNING revision::text",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_AUDIT_INSERT,
        "INSERT INTO %s.audit(request_id,actor,operation,domain_id,target_id,target_detail,"
        "result_revision,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7::bigint,$8::bigint) "
        "RETURNING request_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_DOMAIN_INSERT,
        "INSERT INTO %s.domain(domain_id) SELECT $1 "
        "WHERE $2::bigint>0 AND $3::bigint>0 RETURNING domain_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_DOMAIN_LOOKUP,
        "SELECT '0','1' FROM %s.domain WHERE domain_id=$1 FOR SHARE",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_USER_INSERT,
        "INSERT INTO %s.user_account(domain_id,principal_id,principal_type,enabled,revision,"
        "created_at,updated_at) VALUES($1,$2,$3,true,$4::bigint,$5::bigint,$5::bigint) "
        "RETURNING principal_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_USER_LOCK,
        "SELECT principal_type,CASE WHEN enabled THEN '1' ELSE '0' END "
        "FROM %s.user_account WHERE domain_id=$1 AND principal_id=$2 FOR UPDATE",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_SUBJECT_LINES,
        "SELECT rule_line FROM %s.policy_draft WHERE domain_id=$1 "
        "UNION ALL SELECT rule_line FROM %s.acl_rule WHERE namespace_name=$1",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_USER_DISABLE,
        "UPDATE %s.user_account SET enabled=false,revision=$1::bigint,updated_at=$2::bigint "
        "WHERE domain_id=$3 AND principal_id=$4 AND enabled RETURNING principal_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_STATE,
        "SELECT CASE WHEN u.enabled THEN '1' ELSE '0' END,"
        "CASE WHEN c.principal_id IS NULL THEN '0' ELSE '1' END,"
        "CASE WHEN c.enabled IS NULL THEN NULL WHEN c.enabled THEN '1' ELSE '0' END "
        "FROM %s.user_account u LEFT JOIN %s.credential c "
        "ON c.domain_id=u.domain_id AND c.principal_id=u.principal_id "
        "WHERE u.domain_id=$1 AND u.principal_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_INSERT,
        "INSERT INTO %s.credential(domain_id,principal_id,kdf_algorithm,memory_blocks,passes,"
        "lanes,salt,verifier,enabled,revision,created_at,updated_at) "
        "VALUES($1,$2,$3::integer,$4::integer,$5::integer,$6::integer,"
        "pg_catalog.decode($7::text,'hex'),pg_catalog.decode($8::text,'hex'),true,$9::bigint,"
        "$10::bigint,$10::bigint) RETURNING principal_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_ROTATE,
        "UPDATE %s.credential SET kdf_algorithm=$3::integer,memory_blocks=$4::integer,"
        "passes=$5::integer,lanes=$6::integer,salt=pg_catalog.decode($7::text,'hex'),"
        "verifier=pg_catalog.decode($8::text,'hex'),enabled=true,revision=$9::bigint,"
        "updated_at=$10::bigint WHERE domain_id=$1 AND principal_id=$2 "
        "RETURNING principal_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_REVOKE,
        "UPDATE %s.credential SET enabled=false,revision=$1::bigint,updated_at=$2::bigint "
        "WHERE domain_id=$3 AND principal_id=$4 AND enabled RETURNING principal_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_LOOKUP,
        "SELECT depth::text,CASE WHEN enabled THEN '1' ELSE '0' END "
        "FROM %s.security_group WHERE domain_id=$1 AND group_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_INSERT,
        "INSERT INTO %s.security_group(domain_id,group_id,parent_group_id,depth,enabled,"
        "revision,created_at,updated_at) VALUES($1,$2,$3,$4::integer,true,$5::bigint,$6::bigint,"
        "$6::bigint) RETURNING group_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_REFERENCES,
        "SELECT CASE WHEN EXISTS(SELECT 1 FROM %s.security_group WHERE domain_id=$1 "
        "AND parent_group_id=$2) THEN '1' ELSE '0' END,"
        "CASE WHEN EXISTS(SELECT 1 FROM %s.membership WHERE domain_id=$1 AND group_id=$2) "
        "THEN '1' ELSE '0' END",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_DELETE,
        "DELETE FROM %s.security_group WHERE domain_id=$1 AND group_id=$2 RETURNING group_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_EFFECTIVE_COUNT,
        "WITH RECURSIVE effective(group_id,parent_group_id) AS ("
        "SELECT g.group_id,g.parent_group_id FROM %s.membership m "
        "JOIN %s.security_group g ON g.domain_id=m.domain_id AND g.group_id=m.group_id "
        "WHERE m.domain_id=$1 AND m.principal_id=$2 AND g.enabled "
        "UNION SELECT group_id,parent_group_id FROM %s.security_group WHERE domain_id=$1 "
        "AND group_id=$3 AND enabled "
        "UNION SELECT p.group_id,p.parent_group_id FROM effective e "
        "JOIN %s.security_group p ON p.domain_id=$1 AND p.group_id=e.parent_group_id "
        "WHERE p.enabled) SELECT count(*)::text FROM effective",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_INSERT,
        "INSERT INTO %s.membership(domain_id,principal_id,group_id,revision,created_at) "
        "VALUES($1,$2,$3,$4::bigint,$5::bigint) RETURNING group_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_DELETE,
        "DELETE FROM %s.membership WHERE domain_id=$1 AND principal_id=$2 AND group_id=$3 "
        "RETURNING group_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_LOOKUP,
        "SELECT CASE WHEN enabled THEN '1' ELSE '0' END FROM %s.security_role "
        "WHERE domain_id=$1 AND role_id=$2 FOR SHARE",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_INSERT,
        "INSERT INTO "
        "%s.security_role(domain_id,role_id,enabled,revision,created_at,updated_at) "
        "VALUES($1,$2,true,$3::bigint,$4::bigint,$4::bigint) RETURNING role_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_DISABLE,
        "UPDATE %s.security_role SET enabled=false,revision=$1::bigint,updated_at=$2::bigint "
        "WHERE domain_id=$3 AND role_id=$4 AND enabled RETURNING role_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_INSERT,
        "INSERT INTO %s.user_role(domain_id,principal_id,role_id,revision,created_at) "
        "VALUES($1,$2,$3,$4::bigint,$5::bigint) RETURNING role_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_EFFECTIVE_COUNT,
        "SELECT count(*)::text FROM %s.user_role ur JOIN %s.security_role r "
        "ON r.domain_id=ur.domain_id AND r.role_id=ur.role_id "
        "WHERE ur.domain_id=$1 AND ur.principal_id=$2 AND r.enabled",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_DELETE,
        "DELETE FROM %s.user_role WHERE domain_id=$1 AND principal_id=$2 AND role_id=$3 "
        "RETURNING role_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_UPSERT,
        "INSERT INTO %s.policy_draft(domain_id,ordinal,rule_line,revision,updated_at) "
        "VALUES($1,$2::integer,$3,$4::bigint,$5::bigint) "
        "ON CONFLICT(domain_id,ordinal) DO UPDATE SET rule_line=excluded.rule_line,"
        "revision=excluded.revision,updated_at=excluded.updated_at RETURNING ordinal::text",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_DELETE,
        "DELETE FROM %s.policy_draft WHERE domain_id=$1 AND ordinal=$2::integer "
        "RETURNING ordinal::text",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_LINES,
        "SELECT rule_line FROM %s.policy_draft WHERE domain_id=$1 ORDER BY ordinal", schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_BUNDLE_VERSION,
        "SELECT policy_version::text FROM %s.acl_bundle WHERE namespace_name=$1 FOR UPDATE",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_RULES_DELETE,
        "DELETE FROM %s.acl_rule WHERE namespace_name=$1 RETURNING ordinal::text", schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_BUNDLE_UPSERT,
        "INSERT INTO %s.acl_bundle(namespace_name,policy_version,expires_at) "
        "VALUES($1,$2::bigint,$3::bigint) ON CONFLICT(namespace_name) DO UPDATE SET "
        "policy_version=excluded.policy_version,expires_at=excluded.expires_at "
        "RETURNING policy_version::text",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_RULES_COPY,
        "INSERT INTO %s.acl_rule(namespace_name,ordinal,rule_line) "
        "SELECT $1,(pg_catalog.row_number() OVER (ORDER BY ordinal)-1)::integer,rule_line "
        "FROM %s.policy_draft WHERE domain_id=$1 RETURNING ordinal::text",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_PUBLISH_RESULT_LOOKUP,
        "SELECT policy_version::text FROM %s.policy_publish_result WHERE request_id=$1", schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_sql_set(
        command, FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_PUBLISH_RESULT_INSERT,
        "INSERT INTO %s.policy_publish_result(request_id,policy_version) "
        "VALUES($1,$2::bigint) RETURNING policy_version::text",
        schema);
  if (rc != TURBO_OK) {
    flowie_control_pgsql_command_destroy(command);
    return rc;
  }
  *out = command;
  return TURBO_OK;
}

void flowie_control_pgsql_command_destroy(flowie_control_pgsql_command_t *command) {
  if (!command) return;
  for (size_t index = 0u; index < FLOWIE_CONTROL_PGSQL_COMMAND_SQL_COUNT; ++index)
    tstr_freep(&command->sql[index]);
  free(command);
}

static int
flowie_control_pgsql_command_session_open(flowie_control_pgsql_command_t *view,
                                          flowie_control_pgsql_command_session_t *session) {
  int rc;
  if (!view || !session) return TURBO_EINVAL;
  memset(session, 0, sizeof(*session));
  session->view = view;
  session->lease = (flowie_control_pgsql_pool_lease_t)FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
  rc = flowie_control_pgsql_pool_acquire(view->pool, &session->lease);
  if (rc != TURBO_OK) return rc;
  session->connection = flowie_control_pgsql_pool_lease_connection(&session->lease);
  if (session->connection) return TURBO_OK;
  (void)flowie_control_pgsql_pool_release(&session->lease);
  return TURBO_EIO;
}

static int flowie_control_pgsql_command_exec(flowie_control_pgsql_command_session_t *session,
                                             const char *sql, int parameter_count,
                                             const char *const *values, PGresult **result_out) {
  PGresult *result;
  int rc;
  if (result_out) *result_out = NULL;
  if (!session || !session->connection || !sql || parameter_count < 0 || !result_out)
    return TURBO_EINVAL;
  result = PQexecParams(session->connection, sql, parameter_count, NULL, values, NULL, NULL, 0);
  rc = flowie_control_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc != TURBO_OK) {
    if (result) PQclear(result);
    return rc;
  }
  *result_out = result;
  return TURBO_OK;
}

static int flowie_control_pgsql_command_simple(flowie_control_pgsql_command_session_t *session,
                                               const char *sql) {
  PGresult *result;
  int rc;
  if (!session || !session->connection || !sql) return TURBO_EINVAL;
  result = PQexec(session->connection, sql);
  rc = flowie_control_pgsql_result_status(result, PGRES_COMMAND_OK);
  if (result) PQclear(result);
  return rc;
}

static int
flowie_control_pgsql_command_transaction_begin(flowie_control_pgsql_command_session_t *session) {
  int rc = flowie_control_pgsql_command_simple(
      session, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE");
  if (rc == TURBO_OK) session->transaction = 1;
  return rc;
}

static void flowie_control_pgsql_command_evidence_set(
    flowie_control_pgsql_command_session_t *session, const char *request_id, const char *actor,
    const char *operation, const char *domain_id, const char *target_id,
    const char *target_detail, uint64_t revision) {
  if (!session) return;
  session->evidence.request_id = request_id;
  session->evidence.actor = actor;
  session->evidence.operation = operation;
  session->evidence.domain_id = domain_id;
  session->evidence.target_id = target_id;
  session->evidence.target_detail = target_detail;
  session->evidence.revision = revision;
  session->evidence.present = 1;
}

static int
flowie_control_pgsql_command_session_close(flowie_control_pgsql_command_session_t *session,
                                           int operation_status) {
  int transaction_status = TURBO_OK;
  int release_status;
  int had_transaction;
  if (!session || !session->connection) return operation_status;
  had_transaction = session->transaction;
  if (had_transaction) {
    transaction_status = flowie_control_pgsql_command_simple(
        session, operation_status == TURBO_OK ? "COMMIT" : "ROLLBACK");
    if (transaction_status == TURBO_OK) session->transaction = 0;
  }
  release_status = flowie_control_pgsql_pool_release(&session->lease);
  session->connection = NULL;
  if (operation_status != TURBO_OK) return operation_status;
  if (!had_transaction) return release_status;
  if (transaction_status == TURBO_OK) return TURBO_OK;
  if (session->evidence.present) {
    int committed = 0;
    int confirm_status = flowie_control_pgsql_command_commit_confirm(
        session->view, session->evidence.request_id, session->evidence.actor,
        session->evidence.operation, session->evidence.domain_id, session->evidence.target_id,
        session->evidence.target_detail, session->evidence.revision, &committed);
    if (confirm_status != TURBO_OK) return confirm_status;
    if (committed) return TURBO_OK;
  }
  return transaction_status;
}

static int flowie_control_pgsql_command_common_valid(const char *domain_id,
                                                     const char *target_id, const char *actor,
                                                     const char *request_id,
                                                     uint64_t expected_revision,
                                                     uint64_t occurred_at) {
  return flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) &&
         flowie_control_text_valid(target_id, TURBO_FLOW_SECURITY_ID_MAX) &&
         flowie_control_text_valid(actor, FLOWIE_CONTROL_ACTOR_MAX) &&
         flowie_control_text_valid(request_id, FLOWIE_CONTROL_REQUEST_ID_MAX) &&
         expected_revision <= (uint64_t)INT64_MAX && occurred_at > 0u &&
         occurred_at <= (uint64_t)INT64_MAX;
}

static int flowie_control_pgsql_result_text(PGresult *result, int row, int column,
                                            const char **text_out, size_t *length_out) {
  int length;
  const char *text;
  if (text_out) *text_out = NULL;
  if (length_out) *length_out = 0u;
  if (!result || row < 0 || row >= PQntuples(result) || column < 0 || column >= PQnfields(result) ||
      PQgetisnull(result, row, column) || !text_out || !length_out)
    return TURBO_EPROTO;
  length = PQgetlength(result, row, column);
  text = PQgetvalue(result, row, column);
  if (length < 0 || !text || memchr(text, '\0', (size_t)length)) return TURBO_EPROTO;
  *text_out = text;
  *length_out = (size_t)length;
  return TURBO_OK;
}

static int flowie_control_pgsql_text_equals(PGresult *result, int row, int column,
                                            const char *expected) {
  const char *text;
  size_t length;
  size_t expected_length;
  int rc;
  if (!expected) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK) return rc;
  expected_length = strlen(expected);
  return length == expected_length && memcmp(text, expected, length) == 0 ? TURBO_OK : TURBO_EBUSY;
}

static int flowie_control_pgsql_copy_text(PGresult *result, int row, int column, char *output,
                                          size_t capacity) {
  const char *text;
  size_t length;
  int rc;
  if (!output || capacity == 0u) return TURBO_EINVAL;
  output[0] = '\0';
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK) return rc;
  if (length >= capacity) return TURBO_EPROTO;
  memcpy(output, text, length);
  output[length] = '\0';
  return TURBO_OK;
}

static int flowie_control_pgsql_command_result_bool(PGresult *result, int row, int column,
                                                    int *value_out);

static int flowie_control_pgsql_parse_u64(PGresult *result, int row, int column,
                                          uint64_t *value_out) {
  const char *text;
  size_t length;
  char buffer[32];
  char *end = NULL;
  unsigned long long value;
  int rc;
  if (value_out) *value_out = 0u;
  if (!value_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK) return rc;
  if (length == 0u || length >= sizeof(buffer)) return TURBO_EPROTO;
  memcpy(buffer, text, length);
  buffer[length] = '\0';
  errno = 0;
  value = strtoull(buffer, &end, 10);
  if (errno != 0 || !end || *end != '\0') return TURBO_EPROTO;
  *value_out = (uint64_t)value;
  return TURBO_OK;
}

static int flowie_control_pgsql_u64_text(uint64_t value, char output[32]) {
  int length = snprintf(output, 32u, "%llu", (unsigned long long)value);
  return length > 0 && length < 32 ? TURBO_OK : TURBO_ERANGE;
}

static int flowie_control_pgsql_command_replay(flowie_control_pgsql_command_t *view,
                                               flowie_control_pgsql_command_session_t *session,
                                               const char *request_id, const char *actor,
                                               const char *operation, const char *domain_id,
                                               const char *target_id, const char *target_detail,
                                               flowie_control_command_result_t *result,
                                               int *found_out) {
  const char *values[1] = {request_id};
  PGresult *replay = NULL;
  int rc;
  if (found_out) *found_out = 0;
  if (!view || !session || !request_id || !actor || !operation || !domain_id || !target_id ||
      !result || !found_out)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_REPLAY], 1,
                                         values, &replay);
  if (rc != TURBO_OK) return rc;
  if (PQntuples(replay) == 0) {
    rc = PQnfields(replay) == 6 ? TURBO_OK : TURBO_EPROTO;
    goto done;
  }
  if (PQntuples(replay) != 1 || PQnfields(replay) != 6) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_control_pgsql_text_equals(replay, 0, 0, actor);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_text_equals(replay, 0, 1, operation);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_text_equals(replay, 0, 2, domain_id);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_text_equals(replay, 0, 3, target_id);
  if (rc == TURBO_OK && target_detail)
    rc = flowie_control_pgsql_text_equals(replay, 0, 4, target_detail);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(replay, 0, 5, &result->revision);
  if (rc == TURBO_OK && result->revision == 0u) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    result->replayed = 1;
    *found_out = 1;
    flowie_control_pgsql_command_evidence_set(session, request_id, actor, operation, domain_id,
                                              target_id, target_detail, result->revision);
  }

done:
  PQclear(replay);
  return rc;
}

int flowie_control_pgsql_command_commit_confirm(flowie_control_pgsql_command_t *view,
                                                const char *request_id, const char *actor,
                                                const char *operation, const char *domain_id,
                                                const char *target_id, const char *target_detail,
                                                uint64_t revision, int *committed_out) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_command_result_t replay = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  int found = 0;
  int rc;
  if (committed_out) *committed_out = 0;
  if (!view || !request_id || !actor || !operation || !domain_id || !target_id ||
      !target_detail || revision == 0u || !committed_out)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc =
      flowie_control_pgsql_command_replay(view, &session, request_id, actor, operation,
                                          domain_id, target_id, target_detail, &replay, &found);
  if (rc == TURBO_OK && found && replay.revision != revision) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) *committed_out = found;
  (void)flowie_control_pgsql_pool_release(&session.lease);
  session.connection = NULL;
  return rc;
}

static int
flowie_control_pgsql_command_revision_lock(flowie_control_pgsql_command_t *view,
                                           flowie_control_pgsql_command_session_t *session,
                                           uint64_t expected_revision, uint64_t *current_out) {
  PGresult *revision = NULL;
  int rc;
  if (current_out) *current_out = 0u;
  if (!view || !session || !current_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_LOCK], 0, NULL, &revision);
  if (rc == TURBO_OK && (PQntuples(revision) != 1 || PQnfields(revision) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(revision, 0, 0, current_out);
  if (revision) PQclear(revision);
  if (rc == TURBO_OK && expected_revision != 0u && *current_out != expected_revision)
    return TURBO_EBUSY;
  return rc;
}

static int
flowie_control_pgsql_command_revision_read(flowie_control_pgsql_command_t *view,
                                           flowie_control_pgsql_command_session_t *session,
                                           uint64_t expected_revision) {
  PGresult *revision = NULL;
  uint64_t current = 0u;
  int rc;
  if (!view || !session) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_READ], 0, NULL, &revision);
  if (rc == TURBO_OK && (PQntuples(revision) != 1 || PQnfields(revision) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(revision, 0, 0, &current);
  if (revision) PQclear(revision);
  if (rc == TURBO_OK && expected_revision != 0u && current != expected_revision)
    return TURBO_EBUSY;
  return rc;
}

static int
flowie_control_pgsql_command_revision_advance(flowie_control_pgsql_command_t *view,
                                              flowie_control_pgsql_command_session_t *session,
                                              uint64_t current, uint64_t *next_out) {
  char current_text[32];
  char next_text[32];
  const char *values[2];
  PGresult *revision = NULL;
  uint64_t returned = 0u;
  int rc;
  if (next_out) *next_out = 0u;
  if (!view || !session || !next_out) return TURBO_EINVAL;
  if (current >= (uint64_t)INT64_MAX) return TURBO_ERANGE;
  *next_out = current + 1u;
  rc = flowie_control_pgsql_u64_text(current, current_text);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(*next_out, next_text);
  values[0] = next_text;
  values[1] = current_text;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_exec(
        session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_REVISION_ADVANCE], 2, values, &revision);
  if (rc == TURBO_OK && (PQntuples(revision) != 1 || PQnfields(revision) != 1)) rc = TURBO_EBUSY;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(revision, 0, 0, &returned);
  if (rc == TURBO_OK && returned != *next_out) rc = TURBO_EPROTO;
  if (revision) PQclear(revision);
  if (rc != TURBO_OK) *next_out = 0u;
  return rc;
}

static int flowie_control_pgsql_command_audit_insert(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *request_id, const char *actor, const char *operation, const char *domain_id,
    const char *target_id, const char *target_detail, uint64_t revision, uint64_t occurred_at) {
  char revision_text[32];
  char occurred_at_text[32];
  const char *values[8];
  PGresult *audit = NULL;
  int rc = flowie_control_pgsql_u64_text(revision, revision_text);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(occurred_at, occurred_at_text);
  values[0] = request_id;
  values[1] = actor;
  values[2] = operation;
  values[3] = domain_id;
  values[4] = target_id;
  values[5] = target_detail;
  values[6] = revision_text;
  values[7] = occurred_at_text;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_exec(
        session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_AUDIT_INSERT], 8, values, &audit);
  if (rc == TURBO_OK && (PQntuples(audit) != 1 || PQnfields(audit) != 1)) rc = TURBO_EPROTO;
  if (audit) PQclear(audit);
  if (rc == TURBO_OK)
    flowie_control_pgsql_command_evidence_set(session, request_id, actor, operation, domain_id,
                                              target_id, target_detail, revision);
  return rc;
}

static int flowie_control_pgsql_command_root_lookup(flowie_control_pgsql_command_t *view,
                                                    flowie_control_pgsql_command_session_t *session,
                                                    const char *domain_id) {
  const char *values[1] = {domain_id};
  PGresult *group = NULL;
  uint64_t depth = 0u;
  int rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_DOMAIN_LOOKUP], 1, values, &group);
  if (rc != TURBO_OK) return rc;
  if (PQntuples(group) == 0) {
    rc = PQnfields(group) == 2 ? TURBO_ENOENT : TURBO_EPROTO;
    goto done;
  }
  if (PQntuples(group) != 1 || PQnfields(group) != 2) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_control_pgsql_parse_u64(group, 0, 0, &depth);
  if (rc == TURBO_OK && (depth != 0u || strcmp(PQgetvalue(group, 0, 1), "1") != 0))
    rc = TURBO_EPERM;

done:
  PQclear(group);
  return rc;
}

static int flowie_control_pgsql_command_group_lookup(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, const char *group_id, uint64_t *depth_out, int *enabled_out) {
  const char *values[2] = {domain_id, group_id};
  PGresult *group = NULL;
  int rc;
  if (depth_out) *depth_out = 0u;
  if (enabled_out) *enabled_out = 0;
  if (!view || !session || !domain_id || !group_id || !depth_out || !enabled_out)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_LOOKUP], 2, values, &group);
  if (rc == TURBO_OK && PQntuples(group) == 0)
    rc = PQnfields(group) == 2 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && (PQntuples(group) != 1 || PQnfields(group) != 2)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(group, 0, 0, depth_out);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_command_result_bool(group, 0, 1, enabled_out);
  if (group) PQclear(group);
  return rc;
}

static int flowie_control_pgsql_command_group_references(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, const char *group_id, int *referenced_out) {
  const char *values[2] = {domain_id, group_id};
  PGresult *result = NULL;
  int active_child = 0;
  int direct_membership = 0;
  int rc;
  if (referenced_out) *referenced_out = 0;
  if (!view || !session || !domain_id || !group_id || !referenced_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_REFERENCES], 2, values, &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 2)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_command_result_bool(result, 0, 0, &active_child);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_result_bool(result, 0, 1, &direct_membership);
  if (result) PQclear(result);
  if (rc == TURBO_OK) *referenced_out = active_child || direct_membership;
  return rc;
}

static int flowie_control_pgsql_command_role_lookup(flowie_control_pgsql_command_t *view,
                                                    flowie_control_pgsql_command_session_t *session,
                                                    const char *domain_id, const char *role_id,
                                                    int *enabled_out) {
  const char *values[2] = {domain_id, role_id};
  PGresult *role = NULL;
  int rc;
  if (enabled_out) *enabled_out = 0;
  if (!view || !session || !domain_id || !role_id || !enabled_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_LOOKUP], 2, values, &role);
  if (rc == TURBO_OK && PQntuples(role) == 0)
    rc = PQnfields(role) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && (PQntuples(role) != 1 || PQnfields(role) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_command_result_bool(role, 0, 0, enabled_out);
  if (role) PQclear(role);
  return rc;
}

static int flowie_control_pgsql_command_user_role_capacity(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, const char *principal_id) {
  const char *values[2] = {domain_id, principal_id};
  PGresult *result = NULL;
  uint64_t count = 0u;
  int rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_EFFECTIVE_COUNT], 2, values,
      &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(result, 0, 0, &count);
  if (result) PQclear(result);
  if (rc == TURBO_OK && count > TURBO_FLOW_SECURITY_MAX_ROLES) return TURBO_ENOSPC;
  return rc;
}

static int flowie_control_pgsql_command_membership_capacity(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, const char *principal_id, const char *group_id) {
  const char *values[3] = {domain_id, principal_id, group_id};
  PGresult *result = NULL;
  uint64_t count = 0u;
  int rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_EFFECTIVE_COUNT], 3, values,
      &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(result, 0, 0, &count);
  if (result) PQclear(result);
  if (rc == TURBO_OK && count > TURBO_FLOW_SECURITY_MAX_GROUPS) return TURBO_ENOSPC;
  if (rc == TURBO_OK && count == 0u) return TURBO_EPROTO;
  return rc;
}

static int flowie_control_pgsql_command_policy_subject_referenced(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, turbo_flow_security_subject_kind_t subject_kind, const char *subject,
    int *referenced_out) {
  const char *values[1] = {domain_id};
  PGresult *lines = NULL;
  int rc;
  if (referenced_out) *referenced_out = 0;
  if (!view || !session || !domain_id || !subject || !referenced_out ||
      (subject_kind != TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL &&
       subject_kind != TURBO_FLOW_SECURITY_SUBJECT_ROLE &&
       subject_kind != TURBO_FLOW_SECURITY_SUBJECT_GROUP))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_SUBJECT_LINES], 1, values, &lines);
  if (rc != TURBO_OK) return rc;
  if (PQnfields(lines) != 1) {
    rc = TURBO_EPROTO;
    goto done;
  }
  for (int row = 0; row < PQntuples(lines); ++row) {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    const char *line;
    size_t line_size;
    rc = flowie_control_pgsql_result_text(lines, row, 0, &line, &line_size);
    if (rc != TURBO_OK || line_size == 0u || line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX ||
        turbo_flow_security_rule_parse_line(line, line_size, &rule) != TURBO_OK ||
        strcmp(rule.domain_id, domain_id) != 0) {
      rc = TURBO_EPROTO;
      goto done;
    }
    if (rule.subject_kind == subject_kind && strcmp(rule.subject, subject) == 0) {
      *referenced_out = 1;
      break;
    }
  }
  rc = TURBO_OK;

done:
  PQclear(lines);
  return rc;
}

static int flowie_control_pgsql_command_result_bool(PGresult *result, int row, int column,
                                                    int *value_out) {
  const char *text;
  size_t length;
  int rc;
  if (value_out) *value_out = 0;
  if (!value_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK) return rc;
  if (length != 1u || (text[0] != '0' && text[0] != '1')) return TURBO_EPROTO;
  *value_out = text[0] == '1';
  return TURBO_OK;
}

static int
flowie_control_pgsql_command_credential_state(flowie_control_pgsql_command_t *view,
                                              flowie_control_pgsql_command_session_t *session,
                                              const char *domain_id, const char *principal_id,
                                              flowie_control_pgsql_credential_state_t *state_out) {
  const char *values[2] = {domain_id, principal_id};
  flowie_control_pgsql_credential_state_t state = {0};
  PGresult *result = NULL;
  int rc;
  if (!view || !session || !domain_id || !principal_id || !state_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_STATE], 2, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0)
    rc = PQnfields(result) == 3 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 3)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_result_bool(result, 0, 0, &state.user_enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_result_bool(result, 0, 1, &state.credential_exists);
  if (rc == TURBO_OK && state.credential_exists)
    rc = flowie_control_pgsql_command_result_bool(result, 0, 2, &state.credential_enabled);
  if (rc == TURBO_OK && !state.credential_exists && !PQgetisnull(result, 0, 2)) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  if (rc == TURBO_OK) *state_out = state;
  return rc;
}

static int flowie_control_pgsql_command_policy_target(uint32_t ordinal, char output[32]) {
  int written = snprintf(output, 32u, "%u", ordinal);
  return written > 0 && written < 32 ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_control_pgsql_command_policy_publish_detail(uint64_t expires_at,
                                                              char output[64]) {
  int written = snprintf(output, 64u, "expires_at=%llu", (unsigned long long)expires_at);
  return written > 0 && written < 64 ? TURBO_OK : TURBO_EINVAL;
}

static int flowie_control_pgsql_command_policy_rule_validate(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, const char *rule_line, size_t rule_line_size,
    turbo_flow_security_rule_t *rule_out) {
  flowie_control_pgsql_credential_state_t user = {0};
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  uint64_t depth = 0u;
  int enabled = 0;
  int rc;
  if (!view || !session) return TURBO_EINVAL;
  rc = flowie_control_policy_rule_syntax_validate(domain_id, rule_line, rule_line_size, &rule);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_root_lookup(view, session, domain_id);
  if (rc != TURBO_OK) return rc;
  switch (rule.subject_kind) {
  case TURBO_FLOW_SECURITY_SUBJECT_ANY:
    break;
  case TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL:
    rc = flowie_control_pgsql_command_credential_state(view, session, domain_id, rule.subject,
                                                       &user);
    if (rc != TURBO_OK) return rc;
    if (!user.user_enabled) return TURBO_EPERM;
    break;
  case TURBO_FLOW_SECURITY_SUBJECT_ROLE:
    rc = flowie_control_pgsql_command_role_lookup(view, session, domain_id, rule.subject,
                                                  &enabled);
    if (rc != TURBO_OK) return rc;
    if (!enabled) return TURBO_EPERM;
    break;
  case TURBO_FLOW_SECURITY_SUBJECT_GROUP:
    rc = flowie_control_pgsql_command_group_lookup(view, session, domain_id, rule.subject,
                                                   &depth, &enabled);
    if (rc != TURBO_OK) return rc;
    if (!enabled) return TURBO_EPERM;
    break;
  default:
    return TURBO_EPROTO;
  }
  if (rule_out) *rule_out = rule;
  return TURBO_OK;
}

static int flowie_control_pgsql_command_policy_validate(
    flowie_control_pgsql_command_t *view, flowie_control_pgsql_command_session_t *session,
    const char *domain_id, uint64_t store_revision, flowie_control_policy_validation_t *out) {
  const char *values[1] = {domain_id};
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  PGresult *lines = NULL;
  int rc;
  if (!view || !session || !domain_id || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_LINES], 1, values, &lines);
  if (rc != TURBO_OK) return rc;
  if (PQnfields(lines) != 1) {
    rc = TURBO_EPROTO;
    goto done;
  }
  validation.store_revision = store_revision;
  for (int row = 0; row < PQntuples(lines); ++row) {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    const char *line;
    size_t line_size;
    if (validation.rule_count >= TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = TURBO_ENOSPC;
      goto done;
    }
    rc = flowie_control_pgsql_result_text(lines, row, 0, &line, &line_size);
    if (rc != TURBO_OK) goto done;
    rc = flowie_control_pgsql_command_policy_rule_validate(view, session, domain_id, line,
                                                           line_size, &rule);
    if (rc != TURBO_OK) goto done;
    ++validation.rule_count;
    if (rule.effect == TURBO_FLOW_SECURITY_DENY) ++validation.deny_rule_count;
  }
  if (validation.rule_count == 0u) {
    rc = TURBO_ENOENT;
    goto done;
  }
  *out = validation;
  rc = TURBO_OK;

done:
  PQclear(lines);
  return rc;
}

static int
flowie_control_pgsql_command_policy_version(flowie_control_pgsql_command_t *view,
                                            flowie_control_pgsql_command_session_t *session,
                                            const char *domain_id, uint64_t *version_out) {
  const char *values[1] = {domain_id};
  PGresult *result = NULL;
  int rc;
  if (version_out) *version_out = 0u;
  if (!view || !session || !domain_id || !version_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_BUNDLE_VERSION], 1, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0)
    rc = PQnfields(result) == 1 ? TURBO_OK : TURBO_EPROTO;
  else if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  else if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(result, 0, 0, version_out);
  if (result) PQclear(result);
  return rc;
}

static int
flowie_control_pgsql_command_publish_replay_version(flowie_control_pgsql_command_t *view,
                                                    flowie_control_pgsql_command_session_t *session,
                                                    const char *request_id, uint64_t *version_out) {
  const char *values[1] = {request_id};
  PGresult *result = NULL;
  int rc;
  if (version_out) *version_out = 0u;
  if (!view || !session || !request_id || !version_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_exec(
      session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_PUBLISH_RESULT_LOOKUP], 1, values,
      &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_parse_u64(result, 0, 0, version_out);
  if (rc == TURBO_OK && *version_out == 0u) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_control_pgsql_hex_encode(const uint8_t *input, size_t input_size, char *output,
                                           size_t capacity) {
  static const char digits[] = "0123456789abcdef";
  if (!input || input_size == 0u || input_size > (SIZE_MAX - 1u) / 2u || !output ||
      capacity < input_size * 2u + 1u)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < input_size; ++index) {
    output[index * 2u] = digits[input[index] >> 4u];
    output[index * 2u + 1u] = digits[input[index] & 0x0fu];
  }
  output[input_size * 2u] = '\0';
  return TURBO_OK;
}

int flowie_control_pgsql_command_domain_create(
    flowie_control_pgsql_command_t *view, const flowie_control_domain_create_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char next_text[32];
  char occurred_at_text[32];
  const char *values[3];
  PGresult *inserted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(command->domain_id, command->domain_id,
                                                 command->actor, command->request_id,
                                                 command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_DOMAIN_CREATE, command->domain_id,
        command->domain_id, FLOWIE_CONTROL_PGSQL_TARGET_DOMAIN, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = next_text;
  values[2] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_DOMAIN_INSERT], 3, values, &inserted);
  if (rc == TURBO_OK && !found && (PQntuples(inserted) != 1 || PQnfields(inserted) != 1))
    rc = TURBO_EPROTO;
  if (inserted) PQclear(inserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_DOMAIN_CREATE, command->domain_id,
        command->domain_id, FLOWIE_CONTROL_PGSQL_TARGET_DOMAIN, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

static int flowie_control_pgsql_command_credential_issue_preflight(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_issue_command_t *command,
    const char *operation, int require_existing) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_pgsql_credential_state_t state = {0};
  flowie_control_command_result_t replay = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  int found = 0;
  int rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                           operation, command->domain_id, command->principal_id,
                                           FLOWIE_CONTROL_PGSQL_DETAIL_ARGON2ID, &replay, &found);
  if (rc == TURBO_OK && found) rc = TURBO_EALREADY;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_revision_read(view, &session, command->expected_revision);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_credential_state(view, &session, command->domain_id,
                                                       command->principal_id, &state);
  if (rc == TURBO_OK && !state.user_enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && require_existing && !state.credential_exists) rc = TURBO_ENOENT;
  if (rc == TURBO_OK && !require_existing && state.credential_exists) rc = TURBO_EALREADY;
  return flowie_control_pgsql_command_session_close(&session, rc);
}

static int flowie_control_pgsql_command_credential_issue(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result, const char *operation, int require_existing) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_pgsql_credential_state_t state = {0};
  flowie_control_credential_kdf_params_t params;
  flowie_control_command_result_t replay = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  char token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY] = {0};
  uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE] = {0};
  uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE] = {0};
  char algorithm_text[32];
  char memory_blocks_text[32];
  char passes_text[32];
  char lanes_text[32];
  char salt_hex[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE * 2u + 1u];
  char verifier_hex[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE * 2u + 1u];
  char next_text[32];
  char occurred_at_text[32];
  const char *values[10];
  PGresult *updated = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result)) {
    flowie_control_generated_credential_wipe(result);
    *result = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  }
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || !operation ||
      ((!command->initial_secret && command->initial_secret_size != 0u) ||
       (command->initial_secret &&
        (command->initial_secret_size == 0u ||
         command->initial_secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX))) ||
      !flowie_control_pgsql_command_common_valid(command->domain_id, command->principal_id,
                                                 command->actor, command->request_id,
                                                 command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_credential_issue_preflight(view, command, operation,
                                                               require_existing);
  flowie_control_credential_default_params(&params);
  if (rc == TURBO_OK && command->initial_secret)
    rc = flowie_control_credential_hash(command->initial_secret, command->initial_secret_size, salt,
                                        verifier, &params);
  else if (rc == TURBO_OK) rc = flowie_control_credential_generate(token, salt, verifier, &params);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(
        view, &session, command->request_id, command->actor, operation, command->domain_id,
        command->principal_id, FLOWIE_CONTROL_PGSQL_DETAIL_ARGON2ID, &replay, &found);
  if (rc == TURBO_OK && found) rc = TURBO_EALREADY;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_credential_state(view, &session, command->domain_id,
                                                       command->principal_id, &state);
  if (rc == TURBO_OK && !state.user_enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && require_existing && !state.credential_exists) rc = TURBO_ENOENT;
  if (rc == TURBO_OK && !require_existing && state.credential_exists) rc = TURBO_EALREADY;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(params.algorithm, algorithm_text);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(params.memory_blocks, memory_blocks_text);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(params.passes, passes_text);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(params.lanes, lanes_text);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_hex_encode(salt, sizeof(salt), salt_hex, sizeof(salt_hex));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_hex_encode(verifier, sizeof(verifier), verifier_hex,
                                         sizeof(verifier_hex));
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = command->principal_id;
  values[2] = algorithm_text;
  values[3] = memory_blocks_text;
  values[4] = passes_text;
  values[5] = lanes_text;
  values[6] = salt_hex;
  values[7] = verifier_hex;
  values[8] = next_text;
  values[9] = occurred_at_text;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_exec(
        &session,
        view->sql[require_existing ? FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_ROTATE
                                   : FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_INSERT],
        10, values, &updated);
  if (rc == TURBO_OK && (PQntuples(updated) != 1 || PQnfields(updated) != 1)) rc = TURBO_EBUSY;
  if (updated) PQclear(updated);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor, operation, command->domain_id,
        command->principal_id, FLOWIE_CONTROL_PGSQL_DETAIL_ARGON2ID, next, command->occurred_at);
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc == TURBO_OK) {
    result->revision = next;
    if (!command->initial_secret) {
      memcpy(result->token, token, sizeof(result->token));
      result->token_size = FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE;
    }
  }

done:
  flowie_control_credential_wipe(token, sizeof(token));
  flowie_control_credential_wipe(salt, sizeof(salt));
  flowie_control_credential_wipe(verifier, sizeof(verifier));
  flowie_control_credential_wipe(salt_hex, sizeof(salt_hex));
  flowie_control_credential_wipe(verifier_hex, sizeof(verifier_hex));
  if (rc != TURBO_OK && result && result->size >= sizeof(*result))
    *result = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  return rc;
}

int flowie_control_pgsql_command_credential_generate(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result) {
  return flowie_control_pgsql_command_credential_issue(
      view, command, result, FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_GENERATE, 0);
}

int flowie_control_pgsql_command_credential_rotate(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result) {
  return flowie_control_pgsql_command_credential_issue(
      view, command, result, FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_ROTATE, 1);
}

int flowie_control_pgsql_command_credential_revoke(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_revoke_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_pgsql_credential_state_t state = {0};
  char next_text[32];
  char occurred_at_text[32];
  const char *values[4];
  PGresult *updated = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(command->domain_id, command->principal_id,
                                                 command->actor, command->request_id,
                                                 command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_REVOKE, command->domain_id,
        command->principal_id, FLOWIE_CONTROL_PGSQL_TARGET_CREDENTIAL, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_credential_state(view, &session, command->domain_id,
                                                       command->principal_id, &state);
  if (rc == TURBO_OK && !found && !state.credential_exists) rc = TURBO_ENOENT;
  if (rc == TURBO_OK && !found && !state.credential_enabled) rc = TURBO_EALREADY;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = next_text;
  values[1] = occurred_at_text;
  values[2] = command->domain_id;
  values[3] = command->principal_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_CREDENTIAL_REVOKE], 4, values, &updated);
  if (rc == TURBO_OK && !found && (PQntuples(updated) != 1 || PQnfields(updated) != 1))
    rc = TURBO_EBUSY;
  if (updated) PQclear(updated);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_CREDENTIAL_REVOKE, command->domain_id,
        command->principal_id, FLOWIE_CONTROL_PGSQL_TARGET_CREDENTIAL, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_user_create(flowie_control_pgsql_command_t *view,
                                             const flowie_control_user_create_command_t *command,
                                             flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char next_text[32];
  char occurred_at_text[32];
  const char *values[5];
  PGresult *inserted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->principal_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->principal_type, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_USER_CREATE,
                                             command->domain_id, command->principal_id,
                                             command->principal_type, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_root_lookup(view, &session, command->domain_id);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = command->principal_id;
  values[2] = command->principal_type;
  values[3] = next_text;
  values[4] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_USER_INSERT], 5, values, &inserted);
  if (rc == TURBO_OK && !found && (PQntuples(inserted) != 1 || PQnfields(inserted) != 1))
    rc = TURBO_EPROTO;
  if (inserted) PQclear(inserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_USER_CREATE, command->domain_id, command->principal_id,
        command->principal_type, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_user_disable(flowie_control_pgsql_command_t *view,
                                              const flowie_control_user_disable_command_t *command,
                                              flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char principal_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u] = {0};
  char next_text[32];
  char occurred_at_text[32];
  const char *lookup_values[2];
  const char *update_values[4];
  PGresult *user = NULL;
  PGresult *updated = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int policy_reference = 0;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(command->domain_id, command->principal_id,
                                                 command->actor, command->request_id,
                                                 command->expected_revision, command->occurred_at))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_USER_DISABLE,
                                             command->domain_id, command->principal_id, NULL,
                                             result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  lookup_values[0] = command->domain_id;
  lookup_values[1] = command->principal_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_USER_LOCK], 2, lookup_values, &user);
  if (rc == TURBO_OK && !found && PQntuples(user) == 0)
    rc = PQnfields(user) == 2 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && !found && (PQntuples(user) != 1 || PQnfields(user) != 2)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_copy_text(user, 0, 0, principal_type, sizeof(principal_type));
  if (rc == TURBO_OK && !found && strcmp(PQgetvalue(user, 0, 1), "1") != 0) rc = TURBO_EALREADY;
  if (user) PQclear(user);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_policy_subject_referenced(
        view, &session, command->domain_id, TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL,
        command->principal_id, &policy_reference);
  if (rc == TURBO_OK && !found && policy_reference) rc = TURBO_EBUSY;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  update_values[0] = next_text;
  update_values[1] = occurred_at_text;
  update_values[2] = command->domain_id;
  update_values[3] = command->principal_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_USER_DISABLE], 4, update_values, &updated);
  if (rc == TURBO_OK && !found && (PQntuples(updated) != 1 || PQnfields(updated) != 1))
    rc = TURBO_EBUSY;
  if (updated) PQclear(updated);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_USER_DISABLE, command->domain_id, command->principal_id,
        principal_type, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_group_create(flowie_control_pgsql_command_t *view,
                                              const flowie_control_group_create_command_t *command,
                                              flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char depth_text[32];
  char next_text[32];
  char occurred_at_text[32];
  const char *values[6];
  PGresult *inserted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint64_t parent_depth = 0u;
  int parent_enabled = 0;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->group_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      (command->parent_group_id &&
       !flowie_control_text_valid(command->parent_group_id, TURBO_FLOW_SECURITY_ID_MAX)) ||
      strcmp(command->group_id, command->domain_id) == 0 ||
      (command->parent_group_id && strcmp(command->group_id, command->parent_group_id) == 0))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_GROUP_CREATE,
                                             command->domain_id, command->group_id,
                                             command->parent_group_id
                                                 ? command->parent_group_id
                                                 : FLOWIE_CONTROL_PGSQL_TARGET_DOMAIN,
                                             result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found && command->parent_group_id)
    rc = flowie_control_pgsql_command_group_lookup(view, &session, command->domain_id,
                                                   command->parent_group_id, &parent_depth,
                                                   &parent_enabled);
  if (rc == TURBO_OK && !found && command->parent_group_id && !parent_enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && !found && command->parent_group_id &&
      parent_depth >= FLOWIE_CONTROL_GROUP_MAX_DEPTH)
    rc = TURBO_ENOSPC;
  if (rc == TURBO_OK && !found && !command->parent_group_id)
    rc = flowie_control_pgsql_command_root_lookup(view, &session, command->domain_id);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->parent_group_id ? parent_depth + 1u : 0u,
                                       depth_text);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = command->group_id;
  values[2] = command->parent_group_id;
  values[3] = depth_text;
  values[4] = next_text;
  values[5] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_INSERT], 6, values, &inserted);
  if (rc == TURBO_OK && !found && (PQntuples(inserted) != 1 || PQnfields(inserted) != 1))
    rc = TURBO_EPROTO;
  if (inserted) PQclear(inserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_GROUP_CREATE, command->domain_id, command->group_id,
        command->parent_group_id ? command->parent_group_id : FLOWIE_CONTROL_PGSQL_TARGET_DOMAIN,
        next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_group_delete(
    flowie_control_pgsql_command_t *view, const flowie_control_group_delete_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  const char *values[2];
  PGresult *deleted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint64_t depth = 0u;
  int enabled = 0;
  int referenced = 0;
  int policy_reference = 0;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->group_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      strcmp(command->group_id, command->domain_id) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_GROUP_DELETE,
                                             command->domain_id, command->group_id,
                                             FLOWIE_CONTROL_PGSQL_TARGET_GROUP, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_group_lookup(view, &session, command->domain_id,
                                                   command->group_id, &depth, &enabled);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_group_references(view, &session, command->domain_id,
                                                       command->group_id, &referenced);
  if (rc == TURBO_OK && !found && referenced) rc = TURBO_EBUSY;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_policy_subject_referenced(
        view, &session, command->domain_id, TURBO_FLOW_SECURITY_SUBJECT_GROUP,
        command->group_id, &policy_reference);
  if (rc == TURBO_OK && !found && policy_reference) rc = TURBO_EBUSY;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  values[0] = command->domain_id;
  values[1] = command->group_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_GROUP_DELETE], 2, values, &deleted);
  if (rc == TURBO_OK && !found && (PQntuples(deleted) != 1 || PQnfields(deleted) != 1))
    rc = TURBO_EBUSY;
  if (deleted) PQclear(deleted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_GROUP_DELETE, command->domain_id, command->group_id,
        FLOWIE_CONTROL_PGSQL_TARGET_GROUP, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_membership_add(
    flowie_control_pgsql_command_t *view, const flowie_control_membership_add_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_pgsql_credential_state_t user = {0};
  char next_text[32];
  char occurred_at_text[32];
  const char *values[5];
  PGresult *inserted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint64_t depth = 0u;
  int enabled = 0;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->principal_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->group_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      strcmp(command->group_id, command->domain_id) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_MEMBERSHIP_ADD,
                                             command->domain_id, command->principal_id,
                                             command->group_id, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_credential_state(view, &session, command->domain_id,
                                                       command->principal_id, &user);
  if (rc == TURBO_OK && !found && !user.user_enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_group_lookup(view, &session, command->domain_id,
                                                   command->group_id, &depth, &enabled);
  if (rc == TURBO_OK && !found && !enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_membership_capacity(view, &session, command->domain_id,
                                                          command->principal_id, command->group_id);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = command->principal_id;
  values[2] = command->group_id;
  values[3] = next_text;
  values[4] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_INSERT], 5, values, &inserted);
  if (rc == TURBO_OK && !found && (PQntuples(inserted) != 1 || PQnfields(inserted) != 1))
    rc = TURBO_EPROTO;
  if (inserted) PQclear(inserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_MEMBERSHIP_ADD, command->domain_id,
        command->principal_id, command->group_id, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_membership_remove(
    flowie_control_pgsql_command_t *view, const flowie_control_membership_remove_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  const char *values[3];
  PGresult *removed = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->principal_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->group_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      strcmp(command->group_id, command->domain_id) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_MEMBERSHIP_REMOVE,
                                             command->domain_id, command->principal_id,
                                             command->group_id, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  values[0] = command->domain_id;
  values[1] = command->principal_id;
  values[2] = command->group_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_MEMBERSHIP_DELETE], 3, values, &removed);
  if (rc == TURBO_OK && !found && PQntuples(removed) == 0)
    rc = PQnfields(removed) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && !found && (PQntuples(removed) != 1 || PQnfields(removed) != 1))
    rc = TURBO_EPROTO;
  if (removed) PQclear(removed);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_MEMBERSHIP_REMOVE, command->domain_id,
        command->principal_id, command->group_id, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_role_create(flowie_control_pgsql_command_t *view,
                                             const flowie_control_role_create_command_t *command,
                                             flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char next_text[32];
  char occurred_at_text[32];
  const char *values[4];
  PGresult *inserted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->role_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_ROLE_CREATE,
                                             command->domain_id, command->role_id,
                                             FLOWIE_CONTROL_PGSQL_TARGET_ROLE, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_root_lookup(view, &session, command->domain_id);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = command->role_id;
  values[2] = next_text;
  values[3] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_INSERT], 4, values, &inserted);
  if (rc == TURBO_OK && !found && (PQntuples(inserted) != 1 || PQnfields(inserted) != 1))
    rc = TURBO_EPROTO;
  if (inserted) PQclear(inserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_ROLE_CREATE, command->domain_id, command->role_id,
        FLOWIE_CONTROL_PGSQL_TARGET_ROLE, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_role_disable(flowie_control_pgsql_command_t *view,
                                              const flowie_control_role_disable_command_t *command,
                                              flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char next_text[32];
  char occurred_at_text[32];
  const char *values[4];
  PGresult *updated = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int enabled = 0;
  int policy_reference = 0;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->role_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_ROLE_DISABLE,
                                             command->domain_id, command->role_id,
                                             FLOWIE_CONTROL_PGSQL_TARGET_ROLE, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_role_lookup(view, &session, command->domain_id,
                                                  command->role_id, &enabled);
  if (rc == TURBO_OK && !found && !enabled) rc = TURBO_EALREADY;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_policy_subject_referenced(
        view, &session, command->domain_id, TURBO_FLOW_SECURITY_SUBJECT_ROLE, command->role_id,
        &policy_reference);
  if (rc == TURBO_OK && !found && policy_reference) rc = TURBO_EBUSY;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = next_text;
  values[1] = occurred_at_text;
  values[2] = command->domain_id;
  values[3] = command->role_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_ROLE_DISABLE], 4, values, &updated);
  if (rc == TURBO_OK && !found && (PQntuples(updated) != 1 || PQnfields(updated) != 1))
    rc = TURBO_EBUSY;
  if (updated) PQclear(updated);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_ROLE_DISABLE, command->domain_id, command->role_id,
        FLOWIE_CONTROL_PGSQL_TARGET_ROLE, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_user_role_add(
    flowie_control_pgsql_command_t *view, const flowie_control_user_role_add_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_pgsql_credential_state_t user = {0};
  char next_text[32];
  char occurred_at_text[32];
  const char *values[5];
  PGresult *inserted = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int role_enabled = 0;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->principal_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_USER_ROLE_ADD,
                                             command->domain_id, command->principal_id,
                                             command->role_id, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_credential_state(view, &session, command->domain_id,
                                                       command->principal_id, &user);
  if (rc == TURBO_OK && !found && !user.user_enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_role_lookup(view, &session, command->domain_id,
                                                  command->role_id, &role_enabled);
  if (rc == TURBO_OK && !found && !role_enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = command->principal_id;
  values[2] = command->role_id;
  values[3] = next_text;
  values[4] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_INSERT], 5, values, &inserted);
  if (rc == TURBO_OK && !found && (PQntuples(inserted) != 1 || PQnfields(inserted) != 1))
    rc = TURBO_EPROTO;
  if (inserted) PQclear(inserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_user_role_capacity(view, &session, command->domain_id,
                                                         command->principal_id);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_USER_ROLE_ADD, command->domain_id, command->principal_id,
        command->role_id, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_user_role_remove(
    flowie_control_pgsql_command_t *view, const flowie_control_user_role_remove_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  const char *values[3];
  PGresult *removed = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->principal_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !flowie_control_text_valid(command->role_id, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_USER_ROLE_REMOVE,
                                             command->domain_id, command->principal_id,
                                             command->role_id, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  values[0] = command->domain_id;
  values[1] = command->principal_id;
  values[2] = command->role_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_USER_ROLE_DELETE], 3, values, &removed);
  if (rc == TURBO_OK && !found && PQntuples(removed) == 0)
    rc = PQnfields(removed) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && !found && (PQntuples(removed) != 1 || PQnfields(removed) != 1))
    rc = TURBO_EPROTO;
  if (removed) PQclear(removed);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_USER_ROLE_REMOVE, command->domain_id,
        command->principal_id, command->role_id, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_policy_rule_put(
    flowie_control_pgsql_command_t *view, const flowie_control_policy_rule_put_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
  char target[32];
  char next_text[32];
  char occurred_at_text[32];
  const char *values[5];
  PGresult *upserted = NULL;
  size_t line_size;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || command->ordinal >= TURBO_FLOW_SECURITY_MAX_RULES ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->domain_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      !command->rule_line ||
      (line_size = strnlen(command->rule_line, TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u)) == 0u ||
      line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX ||
      flowie_control_pgsql_command_policy_target(command->ordinal, target) != TURBO_OK)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_RULE_PUT,
                                             command->domain_id, target, command->rule_line,
                                             result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_policy_rule_validate(view, &session, command->domain_id,
                                                           command->rule_line, line_size, &rule);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next, next_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->occurred_at, occurred_at_text);
  values[0] = command->domain_id;
  values[1] = target;
  values[2] = command->rule_line;
  values[3] = next_text;
  values[4] = occurred_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_UPSERT], 5, values,
        &upserted);
  if (rc == TURBO_OK && !found && (PQntuples(upserted) != 1 || PQnfields(upserted) != 1))
    rc = TURBO_EPROTO;
  if (upserted) PQclear(upserted);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_RULE_PUT, command->domain_id, target,
        command->rule_line, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_policy_rule_delete(
    flowie_control_pgsql_command_t *view,
    const flowie_control_policy_rule_delete_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  char target[32];
  const char *values[2];
  PGresult *removed = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || command->ordinal >= TURBO_FLOW_SECURITY_MAX_RULES ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->domain_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      flowie_control_pgsql_command_policy_target(command->ordinal, target) != TURBO_OK)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_RULE_DELETE, command->domain_id, target,
        FLOWIE_CONTROL_PGSQL_TARGET_POLICY_RULE, result, &found);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  values[0] = command->domain_id;
  values[1] = target;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_DRAFT_DELETE], 2, values, &removed);
  if (rc == TURBO_OK && !found && PQntuples(removed) == 0)
    rc = PQnfields(removed) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  if (rc == TURBO_OK && !found && (PQntuples(removed) != 1 || PQnfields(removed) != 1))
    rc = TURBO_EPROTO;
  if (removed) PQclear(removed);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_RULE_DELETE, command->domain_id, target,
        FLOWIE_CONTROL_PGSQL_TARGET_POLICY_RULE, next, command->occurred_at);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK) *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_command_policy_publish(
    flowie_control_pgsql_command_t *view, const flowie_control_policy_publish_command_t *command,
    flowie_control_policy_publish_result_t *result) {
  flowie_control_pgsql_command_session_t session;
  flowie_control_command_result_t replay = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  char publish_detail[64];
  char policy_text[32];
  char expires_at_text[32];
  const char *root_values[1];
  const char *bundle_values[3];
  const char *publish_values[2];
  PGresult *deleted = NULL;
  PGresult *bundle = NULL;
  PGresult *rules = NULL;
  PGresult *published = NULL;
  uint64_t current = 0u;
  uint64_t next = 0u;
  uint64_t current_policy = 0u;
  uint64_t next_policy = 0u;
  uint64_t returned_policy = 0u;
  int found = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_policy_publish_result_t)FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  if (!view || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) ||
      !flowie_control_pgsql_command_common_valid(
          command->domain_id, command->domain_id, command->actor, command->request_id,
          command->expected_revision, command->occurred_at) ||
      command->expires_at > (uint64_t)INT64_MAX ||
      (command->expires_at != 0u && command->expires_at <= command->occurred_at) ||
      flowie_control_pgsql_command_policy_publish_detail(command->expires_at, publish_detail) !=
          TURBO_OK)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_command_session_open(view, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_command_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_command_replay(view, &session, command->request_id, command->actor,
                                             FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_PUBLISH,
                                             command->domain_id, command->domain_id,
                                             publish_detail, &replay, &found);
  if (rc == TURBO_OK && found)
    rc = flowie_control_pgsql_command_publish_replay_version(view, &session, command->request_id,
                                                             &returned_policy);
  if (rc == TURBO_OK && found) {
    result->revision = replay.revision;
    result->policy_version = returned_policy;
    result->replayed = 1;
  }
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_lock(view, &session, command->expected_revision,
                                                    &current);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_policy_validate(view, &session, command->domain_id,
                                                      current, &validation);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_policy_version(view, &session, command->domain_id,
                                                     &current_policy);
  if (rc == TURBO_OK && !found && current_policy >= (uint64_t)INT64_MAX) rc = TURBO_ERANGE;
  if (rc == TURBO_OK && !found) next_policy = current_policy + 1u;
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_u64_text(next_policy, policy_text);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_u64_text(command->expires_at, expires_at_text);
  root_values[0] = command->domain_id;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_RULES_DELETE], 1, root_values,
        &deleted);
  if (rc == TURBO_OK && !found && PQnfields(deleted) != 1) rc = TURBO_EPROTO;
  if (deleted) PQclear(deleted);
  bundle_values[0] = command->domain_id;
  bundle_values[1] = policy_text;
  bundle_values[2] = expires_at_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_BUNDLE_UPSERT], 3, bundle_values,
        &bundle);
  if (rc == TURBO_OK && !found && (PQntuples(bundle) != 1 || PQnfields(bundle) != 1))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK && !found) rc = flowie_control_pgsql_parse_u64(bundle, 0, 0, &returned_policy);
  if (rc == TURBO_OK && !found && returned_policy != next_policy) rc = TURBO_EPROTO;
  if (bundle) PQclear(bundle);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_RULES_COPY], 1, root_values,
        &rules);
  if (rc == TURBO_OK && !found &&
      (PQnfields(rules) != 1 || (size_t)PQntuples(rules) != validation.rule_count))
    rc = TURBO_EPROTO;
  if (rules) PQclear(rules);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_revision_advance(view, &session, current, &next);
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_audit_insert(
        view, &session, command->request_id, command->actor,
        FLOWIE_CONTROL_PGSQL_OPERATION_POLICY_PUBLISH, command->domain_id,
        command->domain_id, publish_detail, next, command->occurred_at);
  publish_values[0] = command->request_id;
  publish_values[1] = policy_text;
  if (rc == TURBO_OK && !found)
    rc = flowie_control_pgsql_command_exec(
        &session, view->sql[FLOWIE_CONTROL_PGSQL_COMMAND_POLICY_PUBLISH_RESULT_INSERT], 2,
        publish_values, &published);
  if (rc == TURBO_OK && !found && (PQntuples(published) != 1 || PQnfields(published) != 1))
    rc = TURBO_EPROTO;
  if (published) PQclear(published);
  if (rc == TURBO_OK && !found) {
    result->revision = next;
    result->policy_version = next_policy;
    result->replayed = 0;
  }
  rc = flowie_control_pgsql_command_session_close(&session, rc);
  if (rc != TURBO_OK)
    *result = (flowie_control_policy_publish_result_t)FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  return rc;
}
