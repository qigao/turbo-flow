#include "flowie_control_pgsql_query_internal.h"

#include "flowie_control_credential_internal.h"
#include "flowie_control_validation_internal.h"

#include "libpq-fe.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum flowie_control_pgsql_query_sql_e {
  FLOWIE_CONTROL_PGSQL_QUERY_CREDENTIAL = 0,
  FLOWIE_CONTROL_PGSQL_QUERY_REVISION,
  FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_GET,
  FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_LIST,
  FLOWIE_CONTROL_PGSQL_QUERY_USER_GET,
  FLOWIE_CONTROL_PGSQL_QUERY_USER_LIST,
  FLOWIE_CONTROL_PGSQL_QUERY_USER_ENABLED,
  FLOWIE_CONTROL_PGSQL_QUERY_LOCAL_PRINCIPAL,
  FLOWIE_CONTROL_PGSQL_QUERY_EXTERNAL_PRINCIPAL,
  FLOWIE_CONTROL_PGSQL_QUERY_GROUPS,
  FLOWIE_CONTROL_PGSQL_QUERY_ROLES,
  FLOWIE_CONTROL_PGSQL_QUERY_GROUP_ENABLED,
  FLOWIE_CONTROL_PGSQL_QUERY_ACL_GROUP,
  FLOWIE_CONTROL_PGSQL_QUERY_ROLE_ENABLED,
  FLOWIE_CONTROL_PGSQL_QUERY_GROUP_LIST,
  FLOWIE_CONTROL_PGSQL_QUERY_ROLE_LIST,
  FLOWIE_CONTROL_PGSQL_QUERY_POLICY_DRAFT_LINES,
  FLOWIE_CONTROL_PGSQL_QUERY_POLICY_RULE_LIST,
  FLOWIE_CONTROL_PGSQL_QUERY_POLICY_STATUS,
  FLOWIE_CONTROL_PGSQL_QUERY_BUNDLE_META,
  FLOWIE_CONTROL_PGSQL_QUERY_BUNDLE_RULES,
  FLOWIE_CONTROL_PGSQL_QUERY_AUDIT_LIST,
  FLOWIE_CONTROL_PGSQL_QUERY_AUDIT_COUNT,
  FLOWIE_CONTROL_PGSQL_QUERY_SQL_COUNT
} flowie_control_pgsql_query_sql_t;

typedef struct flowie_control_pgsql_query_session_s {
  flowie_control_pgsql_pool_lease_t lease;
  PGconn *connection;
  int transaction;
} flowie_control_pgsql_query_session_t;

typedef struct flowie_control_pgsql_credential_record_s {
  flowie_control_credential_kdf_params_t params;
  uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE];
  uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE];
  uint64_t user_revision;
  uint64_t credential_revision;
  int user_enabled;
  int credential_exists;
  int credential_enabled;
} flowie_control_pgsql_credential_record_t;

typedef struct flowie_control_pgsql_bundle_owner_s {
  turbo_flow_security_rule_t *rules;
} flowie_control_pgsql_bundle_owner_t;

struct flowie_control_pgsql_query_s {
  flowie_control_pgsql_pool_t *pool;
  tstr_t sql[FLOWIE_CONTROL_PGSQL_QUERY_SQL_COUNT];
};

static int flowie_control_pgsql_query_sql_set(flowie_control_pgsql_query_t *query,
                                              flowie_control_pgsql_query_sql_t index,
                                              const char *format, const char *schema) {
  tstr_t sql;
  tstr_t next;
  if (!query || index >= FLOWIE_CONTROL_PGSQL_QUERY_SQL_COUNT || !format || !schema)
    return TURBO_EINVAL;
  sql = tstr_new();
  if (!sql) return TURBO_ENOMEM;
  next = tstr_cat_fmt(sql, format, schema, schema, schema, schema, schema, schema, schema, schema);
  if (!next) {
    tstr_free(sql);
    return TURBO_ENOMEM;
  }
  query->sql[index] = next;
  return TURBO_OK;
}

int flowie_control_pgsql_query_create(flowie_control_pgsql_pool_t *pool,
                                      flowie_control_pgsql_query_t **out) {
  flowie_control_pgsql_query_t *query;
  const char *schema;
  int rc;
  if (out) *out = NULL;
  if (!pool || !out || !(schema = flowie_control_pgsql_pool_schema_name(pool))) return TURBO_EINVAL;
  query = (flowie_control_pgsql_query_t *)calloc(1u, sizeof(*query));
  if (!query) return TURBO_ENOMEM;
  query->pool = pool;
  rc = flowie_control_pgsql_query_sql_set(
      query, FLOWIE_CONTROL_PGSQL_QUERY_CREDENTIAL,
      "SELECT CASE WHEN u.enabled THEN '1' ELSE '0' END,u.revision::text,"
      "c.kdf_algorithm::text,c.memory_blocks::text,c.passes::text,c.lanes::text,"
      "pg_catalog.encode(c.salt,'hex'),pg_catalog.encode(c.verifier,'hex'),"
      "CASE WHEN c.enabled IS NULL THEN NULL WHEN c.enabled THEN '1' ELSE '0' END,"
      "c.revision::text "
      "FROM %s.user_account u LEFT JOIN %s.credential c "
      "ON c.domain_id=u.domain_id AND c.principal_id=u.principal_id "
      "WHERE u.domain_id=$1 AND u.principal_id=$2",
      schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(query, FLOWIE_CONTROL_PGSQL_QUERY_REVISION,
                                            "SELECT revision::text FROM %s.meta WHERE singleton=1",
                                            schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_GET,
        "SELECT domain_id FROM %s.domain WHERE domain_id=$1",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_LIST,
        "SELECT domain_id FROM %s.domain WHERE ($1='' OR domain_id>$1) "
        "ORDER BY domain_id LIMIT $2::bigint",
        schema);
  if (rc == TURBO_OK)
    rc =
        flowie_control_pgsql_query_sql_set(query, FLOWIE_CONTROL_PGSQL_QUERY_USER_GET,
                                           "SELECT domain_id,principal_id,principal_type,"
                                           "CASE WHEN enabled THEN '1' ELSE '0' END,revision::text,"
                                           "created_at::text,updated_at::text FROM %s.user_account "
                                           "WHERE domain_id=$1 AND principal_id=$2",
                                           schema);
  if (rc == TURBO_OK)
    rc =
        flowie_control_pgsql_query_sql_set(query, FLOWIE_CONTROL_PGSQL_QUERY_USER_LIST,
                                           "SELECT domain_id,principal_id,principal_type,"
                                           "CASE WHEN enabled THEN '1' ELSE '0' END,revision::text,"
                                           "created_at::text,updated_at::text FROM %s.user_account "
                                           "WHERE domain_id=$1 AND ($2='' OR principal_id>$2) "
                                           "ORDER BY principal_id LIMIT $3::bigint",
                                           schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_USER_ENABLED,
        "SELECT CASE WHEN enabled THEN '1' ELSE '0' END FROM %s.user_account "
        "WHERE domain_id=$1 AND principal_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_LOCAL_PRINCIPAL,
        "SELECT u.domain_id,u.principal_id,u.principal_type,"
        "CASE WHEN u.enabled THEN '1' ELSE '0' END,u.revision::text,"
        "CASE WHEN c.enabled IS NULL THEN NULL WHEN c.enabled THEN '1' ELSE '0' END,"
        "c.revision::text "
        "FROM %s.user_account u LEFT JOIN %s.credential c "
        "ON c.domain_id=u.domain_id AND c.principal_id=u.principal_id "
        "WHERE u.domain_id=$1 AND u.principal_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_EXTERNAL_PRINCIPAL,
        "SELECT domain_id,principal_id,principal_type,"
        "CASE WHEN enabled THEN '1' ELSE '0' END,revision::text "
        "FROM %s.user_account WHERE domain_id=$1 AND principal_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_GROUPS,
        "WITH RECURSIVE effective(group_id,parent_group_id,depth) AS ("
        "SELECT g.group_id,g.parent_group_id,g.depth FROM %s.membership m "
        "JOIN %s.security_group g ON g.domain_id=m.domain_id "
        "AND g.group_id=m.group_id WHERE m.domain_id=$1 AND m.principal_id=$2 AND g.enabled "
        "UNION SELECT p.group_id,p.parent_group_id,p.depth FROM effective e "
        "JOIN %s.security_group p ON p.domain_id=$1 AND p.group_id=e.parent_group_id "
        "WHERE p.enabled) SELECT group_id FROM effective GROUP BY group_id "
        "ORDER BY MIN(depth),group_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_ROLES,
        "SELECT r.role_id FROM %s.user_role ur JOIN %s.security_role r "
        "ON r.domain_id=ur.domain_id AND r.role_id=ur.role_id "
        "WHERE ur.domain_id=$1 AND ur.principal_id=$2 AND r.enabled ORDER BY r.role_id",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_GROUP_ENABLED,
        "SELECT CASE WHEN enabled THEN '1' ELSE '0' END FROM %s.security_group "
        "WHERE domain_id=$1 AND group_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_ACL_GROUP,
        "SELECT parent_group_id,depth::text,CASE WHEN enabled THEN '1' ELSE '0' END "
        "FROM %s.security_group WHERE domain_id=$1 AND group_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_ROLE_ENABLED,
        "SELECT CASE WHEN enabled THEN '1' ELSE '0' END FROM %s.security_role "
        "WHERE domain_id=$1 AND role_id=$2",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_GROUP_LIST,
        "SELECT domain_id,group_id,parent_group_id,depth::text,"
        "CASE WHEN enabled THEN '1' ELSE '0' END,revision::text,"
        "created_at::text,updated_at::text FROM %s.security_group "
        "WHERE domain_id=$1 AND ($2='' OR group_id>$2) "
        "ORDER BY group_id LIMIT $3::bigint",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_ROLE_LIST,
        "SELECT domain_id,role_id,CASE WHEN enabled THEN '1' ELSE '0' END,"
        "revision::text,created_at::text,updated_at::text FROM %s.security_role "
        "WHERE domain_id=$1 AND ($2='' OR role_id>$2) "
        "ORDER BY role_id LIMIT $3::bigint",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_POLICY_DRAFT_LINES,
        "SELECT rule_line FROM %s.policy_draft WHERE domain_id=$1 ORDER BY ordinal", schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_POLICY_RULE_LIST,
        "SELECT d.ordinal::text,d.rule_line,d.revision::text,d.updated_at::text "
        "FROM %s.policy_draft d WHERE d.domain_id=$1 AND "
        "($2='0' OR d.ordinal>$3::bigint) ORDER BY d.ordinal LIMIT $4::bigint",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_POLICY_STATUS,
        "SELECT m.revision::text,"
        "(SELECT COUNT(*)::text FROM %s.policy_draft d WHERE d.domain_id=$1),"
        "COALESCE(b.policy_version,0)::text,COALESCE(b.expires_at,0)::text,"
        "(SELECT COUNT(*)::text FROM %s.acl_rule r WHERE r.namespace_name=$1) "
        "FROM %s.meta m LEFT JOIN %s.acl_bundle b ON b.namespace_name=$1 WHERE m.singleton=1",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_BUNDLE_META,
        "SELECT b.policy_version::text,b.expires_at::text,"
        "(SELECT COUNT(*)::text FROM %s.acl_rule r "
        "WHERE r.namespace_name=b.namespace_name) FROM %s.acl_bundle b "
        "WHERE b.namespace_name=$1 AND ($2='0' OR b.policy_version=$2::bigint)",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_BUNDLE_RULES,
        "SELECT r.ordinal::text,r.rule_line FROM %s.acl_rule r "
        "WHERE r.namespace_name=$1 ORDER BY r.ordinal",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(
        query, FLOWIE_CONTROL_PGSQL_QUERY_AUDIT_LIST,
        "SELECT request_id,actor,operation,domain_id,target_id,target_detail,"
        "result_revision::text,occurred_at::text FROM %s.audit "
        "WHERE domain_id=$1 AND result_revision>$2::bigint "
        "ORDER BY result_revision LIMIT $3::bigint",
        schema);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_sql_set(query, FLOWIE_CONTROL_PGSQL_QUERY_AUDIT_COUNT,
                                            "SELECT COUNT(*)::text FROM %s.audit", schema);
  if (rc != TURBO_OK) {
    flowie_control_pgsql_query_destroy(query);
    return rc;
  }
  *out = query;
  return TURBO_OK;
}

void flowie_control_pgsql_query_destroy(flowie_control_pgsql_query_t *query) {
  if (!query) return;
  for (size_t index = 0u; index < FLOWIE_CONTROL_PGSQL_QUERY_SQL_COUNT; ++index)
    tstr_freep(&query->sql[index]);
  free(query);
}

static int flowie_control_pgsql_query_session_open(flowie_control_pgsql_query_t *query,
                                                   flowie_control_pgsql_query_session_t *session) {
  int rc;
  if (!query || !session) return TURBO_EINVAL;
  memset(session, 0, sizeof(*session));
  session->lease = (flowie_control_pgsql_pool_lease_t)FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
  rc = flowie_control_pgsql_pool_acquire(query->pool, &session->lease);
  if (rc != TURBO_OK) return rc;
  session->connection = flowie_control_pgsql_pool_lease_connection(&session->lease);
  if (session->connection) return TURBO_OK;
  (void)flowie_control_pgsql_pool_release(&session->lease);
  return TURBO_EIO;
}

static int flowie_control_pgsql_query_exec(flowie_control_pgsql_query_session_t *session,
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

static int flowie_control_pgsql_query_command(flowie_control_pgsql_query_session_t *session,
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
flowie_control_pgsql_query_transaction_begin(flowie_control_pgsql_query_session_t *session) {
  int rc = flowie_control_pgsql_query_command(
      session, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
  if (rc == TURBO_OK) session->transaction = 1;
  return rc;
}

static int flowie_control_pgsql_query_session_close(flowie_control_pgsql_query_session_t *session,
                                                    int commit, int operation_status) {
  int transaction_status = TURBO_OK;
  int release_status;
  if (!session || !session->connection) return operation_status;
  if (session->transaction) {
    transaction_status =
        flowie_control_pgsql_query_command(session, commit ? "COMMIT" : "ROLLBACK");
    if (transaction_status == TURBO_OK) session->transaction = 0;
  }
  release_status = flowie_control_pgsql_pool_release(&session->lease);
  session->connection = NULL;
  if (operation_status != TURBO_OK) return operation_status;
  if (transaction_status != TURBO_OK) return transaction_status;
  return release_status;
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

static int flowie_control_pgsql_result_copy(PGresult *result, int row, int column, char *out,
                                            size_t capacity) {
  const char *text;
  size_t length;
  int rc;
  if (!out || capacity == 0u) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK || length == 0u || length >= capacity) return TURBO_EPROTO;
  memcpy(out, text, length);
  out[length] = '\0';
  return TURBO_OK;
}

static int flowie_control_pgsql_result_uint64(PGresult *result, int row, int column,
                                              uint64_t minimum, uint64_t maximum,
                                              uint64_t *value_out) {
  const char *text;
  uint64_t value = 0u;
  size_t length;
  int rc;
  if (value_out) *value_out = 0u;
  if (!value_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK || length == 0u) return TURBO_EPROTO;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char byte = (unsigned char)text[index];
    const uint64_t digit = byte >= '0' && byte <= '9' ? (uint64_t)(byte - '0') : UINT64_MAX;
    if (digit > 9u || value > (maximum - digit) / 10u) return TURBO_EPROTO;
    value = value * 10u + digit;
  }
  if (value < minimum) return TURBO_EPROTO;
  *value_out = value;
  return TURBO_OK;
}

static int flowie_control_pgsql_result_uint32(PGresult *result, int row, int column,
                                              uint32_t minimum, uint32_t maximum,
                                              uint32_t *value_out) {
  uint64_t value = 0u;
  int rc;
  if (value_out) *value_out = 0u;
  if (!value_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_uint64(result, row, column, minimum, maximum, &value);
  if (rc == TURBO_OK) *value_out = (uint32_t)value;
  return rc;
}

static int flowie_control_pgsql_result_bool(PGresult *result, int row, int column, int *value_out) {
  const char *text;
  size_t length;
  int rc;
  if (value_out) *value_out = 0;
  if (!value_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK || length != 1u || (text[0] != '0' && text[0] != '1')) return TURBO_EPROTO;
  *value_out = text[0] == '1';
  return TURBO_OK;
}

typedef int (*flowie_control_pgsql_query_page_row_fn)(PGresult *result, int row, void *item);

static int flowie_control_pgsql_query_user_row(PGresult *result, int row, void *item) {
  flowie_control_user_view_t view = FLOWIE_CONTROL_USER_VIEW_INIT;
  int rc;
  if (!result || PQnfields(result) != 7 || !item) return TURBO_EPROTO;
  rc = flowie_control_pgsql_result_copy(result, row, 0, view.domain_id,
                                        sizeof(view.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, row, 1, view.principal_id,
                                          sizeof(view.principal_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, row, 2, view.principal_type,
                                          sizeof(view.principal_type));
  if (rc == TURBO_OK) rc = flowie_control_pgsql_result_bool(result, row, 3, &view.enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 4, 1u, INT64_MAX, &view.revision);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 5, 1u, INT64_MAX, &view.created_at);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 6, 1u, INT64_MAX, &view.updated_at);
  if (rc == TURBO_OK) *(flowie_control_user_view_t *)item = view;
  return rc;
}

static int flowie_control_pgsql_query_group_row(PGresult *result, int row, void *item) {
  flowie_control_group_view_t view = FLOWIE_CONTROL_GROUP_VIEW_INIT;
  int rc;
  if (!result || PQnfields(result) != 8 || !item) return TURBO_EPROTO;
  rc = flowie_control_pgsql_result_copy(result, row, 0, view.domain_id,
                                        sizeof(view.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, row, 1, view.group_id, sizeof(view.group_id));
  if (rc == TURBO_OK && !PQgetisnull(result, row, 2))
    rc = flowie_control_pgsql_result_copy(result, row, 2, view.parent_group_id,
                                          sizeof(view.parent_group_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint32(result, row, 3, 0u, FLOWIE_CONTROL_GROUP_MAX_DEPTH,
                                            &view.depth);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_result_bool(result, row, 4, &view.enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 5, 1u, INT64_MAX, &view.revision);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 6, 1u, INT64_MAX, &view.created_at);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 7, 1u, INT64_MAX, &view.updated_at);
  if (rc == TURBO_OK) *(flowie_control_group_view_t *)item = view;
  return rc;
}

static int flowie_control_pgsql_query_role_row(PGresult *result, int row, void *item) {
  flowie_control_role_view_t view = FLOWIE_CONTROL_ROLE_VIEW_INIT;
  int rc;
  if (!result || PQnfields(result) != 6 || !item) return TURBO_EPROTO;
  rc = flowie_control_pgsql_result_copy(result, row, 0, view.domain_id,
                                        sizeof(view.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, row, 1, view.role_id, sizeof(view.role_id));
  if (rc == TURBO_OK) rc = flowie_control_pgsql_result_bool(result, row, 2, &view.enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 3, 1u, INT64_MAX, &view.revision);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 4, 1u, INT64_MAX, &view.created_at);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, row, 5, 1u, INT64_MAX, &view.updated_at);
  if (rc == TURBO_OK) *(flowie_control_role_view_t *)item = view;
  return rc;
}

static int flowie_control_pgsql_query_domain_row(PGresult *result, int row, void *item) {
  flowie_control_domain_view_t view = FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  int rc;
  if (!result || PQnfields(result) != 1 || !item) return TURBO_EPROTO;
  rc = flowie_control_pgsql_result_copy(result, row, 0, view.domain_id,
                                        sizeof(view.domain_id));
  if (rc == TURBO_OK) *(flowie_control_domain_view_t *)item = view;
  return rc;
}

static int flowie_control_pgsql_query_text_page(flowie_control_pgsql_query_t *query,
                                                flowie_control_pgsql_query_sql_t sql_index,
                                                const char *domain_id, const char *after_id,
                                                void *items, size_t item_size, size_t item_capacity,
                                                flowie_control_pgsql_query_page_row_fn decode,
                                                size_t *count_out, int *has_more_out) {
  flowie_control_pgsql_query_session_t session;
  char limit[32];
  const char *values[3] = {domain_id, after_id ? after_id : "", limit};
  uint8_t *cursor = (uint8_t *)items;
  PGresult *result = NULL;
  size_t count = 0u;
  int written;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!query || sql_index >= FLOWIE_CONTROL_PGSQL_QUERY_SQL_COUNT ||
      !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      (after_id && !flowie_control_text_valid(after_id, TURBO_FLOW_SECURITY_ID_MAX)) || !items ||
      item_size < sizeof(size_t) || item_capacity == 0u ||
      item_capacity > FLOWIE_CONTROL_PAGE_MAX || !decode || !count_out || !has_more_out)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (*(const size_t *)(cursor + index * item_size) < item_size) return TURBO_EINVAL;
  }
  written = snprintf(limit, sizeof(limit), "%llu", (unsigned long long)(item_capacity + 1u));
  if (written <= 0 || (size_t)written >= sizeof(limit)) return TURBO_ERANGE;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(&session, query->sql[sql_index], 3, values, &result);
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    rc = decode(result, row, cursor + count * item_size);
    if (rc == TURBO_OK) ++count;
  }
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc == TURBO_OK) *count_out = count;
  else {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

static int flowie_control_pgsql_query_enabled(flowie_control_pgsql_query_t *query,
                                              flowie_control_pgsql_query_session_t *session,
                                              flowie_control_pgsql_query_sql_t sql_index,
                                              const char *domain_id, const char *id,
                                              int *enabled_out) {
  const char *values[2] = {domain_id, id};
  PGresult *result = NULL;
  int enabled = 0;
  int rc;
  if (enabled_out) *enabled_out = 0;
  if (!query || !session || !domain_id || !id || !enabled_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_exec(session, query->sql[sql_index], 2, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0)
    rc = PQnfields(result) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  else if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_result_bool(result, 0, 0, &enabled);
  if (result) PQclear(result);
  if (rc == TURBO_OK) *enabled_out = enabled;
  return rc;
}

static int flowie_control_pgsql_query_domain_exists(flowie_control_pgsql_query_t *query,
                                                    flowie_control_pgsql_query_session_t *session,
                                                    const char *domain_id) {
  const char *values[1] = {domain_id};
  PGresult *result = NULL;
  int rc;
  if (!query || !session || !domain_id) return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_exec(
      session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_GET], 1, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0)
    rc = PQnfields(result) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  else if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1))
    rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_control_pgsql_hex_decode(PGresult *result, int row, int column, uint8_t *out,
                                           size_t output_size) {
  const char *text;
  size_t length;
  int rc;
  if (!out || output_size == 0u || output_size > SIZE_MAX / 2u) return TURBO_EINVAL;
  rc = flowie_control_pgsql_result_text(result, row, column, &text, &length);
  if (rc != TURBO_OK || length != output_size * 2u) return TURBO_EPROTO;
  for (size_t index = 0u; index < output_size; ++index) {
    unsigned char high = (unsigned char)text[index * 2u];
    unsigned char low = (unsigned char)text[index * 2u + 1u];
    high = high >= '0' && high <= '9'   ? high - '0'
           : high >= 'a' && high <= 'f' ? high - 'a' + 10u
                                        : 0xffu;
    low = low >= '0' && low <= '9' ? low - '0' : low >= 'a' && low <= 'f' ? low - 'a' + 10u : 0xffu;
    if (high > 15u || low > 15u) return TURBO_EPROTO;
    out[index] = (uint8_t)((high << 4u) | low);
  }
  return TURBO_OK;
}

int flowie_control_pgsql_query_user_get(flowie_control_pgsql_query_t *query,
                                        const char *domain_id, const char *principal_id,
                                        flowie_control_user_view_t *out) {
  const char *values[2] = {domain_id, principal_id};
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  int rc;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(&session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_USER_GET], 2,
                                       values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0)
    rc = PQnfields(result) == 7 ? TURBO_ENOENT : TURBO_EPROTO;
  else if (rc == TURBO_OK && PQntuples(result) != 1) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_query_user_row(result, 0, out);
  if (result) PQclear(result);
  return flowie_control_pgsql_query_session_close(&session, 0, rc);
}

int flowie_control_pgsql_query_domain_get(flowie_control_pgsql_query_t *query,
                                              const char *domain_id,
                                              flowie_control_domain_view_t *out) {
  const char *values[1] = {domain_id};
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  int rc;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_domain_view_t)FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(
      &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_GET], 1, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0)
    rc = PQnfields(result) == 1 ? TURBO_ENOENT : TURBO_EPROTO;
  else if (rc == TURBO_OK && PQntuples(result) != 1)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_query_domain_row(result, 0, out);
  if (result) PQclear(result);
  return flowie_control_pgsql_query_session_close(&session, 0, rc);
}

int flowie_control_pgsql_query_domain_list(flowie_control_pgsql_query_t *query,
                                               const char *after_domain_id,
                                               flowie_control_domain_view_t *items,
                                               size_t item_capacity, size_t *count_out,
                                               int *has_more_out) {
  flowie_control_pgsql_query_session_t session;
  char limit[32];
  const char *values[2] = {after_domain_id ? after_domain_id : "", limit};
  PGresult *result = NULL;
  size_t count = 0u;
  int written;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!query ||
      (after_domain_id &&
       !flowie_control_text_valid(after_domain_id, TURBO_FLOW_SECURITY_ID_MAX)) ||
      !items || item_capacity == 0u || item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out ||
      !has_more_out)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (items[index].size < sizeof(items[index])) return TURBO_EINVAL;
    items[index] = (flowie_control_domain_view_t)FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  }
  written = snprintf(limit, sizeof(limit), "%llu",
                     (unsigned long long)(item_capacity + 1u));
  if (written <= 0 || (size_t)written >= sizeof(limit)) return TURBO_ERANGE;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(
      &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_DOMAIN_LIST], 2, values, &result);
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    rc = flowie_control_pgsql_query_domain_row(result, row, &items[count]);
    if (rc == TURBO_OK) ++count;
  }
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc == TURBO_OK)
    *count_out = count;
  else {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

int flowie_control_pgsql_query_user_list(flowie_control_pgsql_query_t *query,
                                         const char *domain_id, const char *after_principal_id,
                                         flowie_control_user_view_t *items, size_t item_capacity,
                                         size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_text_page(
      query, FLOWIE_CONTROL_PGSQL_QUERY_USER_LIST, domain_id, after_principal_id, items,
      sizeof(*items), item_capacity, flowie_control_pgsql_query_user_row, count_out, has_more_out);
}

static int
flowie_control_pgsql_credential_record_decode(PGresult *result,
                                              flowie_control_pgsql_credential_record_t *out) {
  flowie_control_pgsql_credential_record_t record = {0};
  int rc;
  if (!result || !out || PQntuples(result) != 1 || PQnfields(result) != 10) return TURBO_EPROTO;
  rc = flowie_control_pgsql_result_bool(result, 0, 0, &record.user_enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 1, 1u, INT64_MAX, &record.user_revision);
  if (rc != TURBO_OK) goto done;
  if (PQgetisnull(result, 0, 2)) {
    for (int column = 3; column < 10; ++column) {
      if (!PQgetisnull(result, 0, column)) {
        rc = TURBO_EPROTO;
        goto done;
      }
    }
    *out = record;
    return TURBO_OK;
  }
  rc = flowie_control_pgsql_result_uint32(result, 0, 2, 0u, UINT32_MAX, &record.params.algorithm);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint32(result, 0, 3, 0u, UINT32_MAX,
                                            &record.params.memory_blocks);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint32(result, 0, 4, 0u, UINT32_MAX, &record.params.passes);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint32(result, 0, 5, 0u, UINT32_MAX, &record.params.lanes);
  if (rc == TURBO_OK && !flowie_control_credential_params_valid(&record.params)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_hex_decode(result, 0, 6, record.salt, sizeof(record.salt));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_hex_decode(result, 0, 7, record.verifier, sizeof(record.verifier));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_bool(result, 0, 8, &record.credential_enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 9, 1u, INT64_MAX,
                                            &record.credential_revision);
  if (rc == TURBO_OK) {
    record.credential_exists = 1;
    *out = record;
    return TURBO_OK;
  }

done:
  flowie_control_credential_wipe(&record, sizeof(record));
  return rc;
}

static int
flowie_control_pgsql_credential_record_read(flowie_control_pgsql_query_t *query,
                                            const char *domain_id, const char *principal_id,
                                            flowie_control_pgsql_credential_record_t *out) {
  const char *values[2] = {domain_id, principal_id};
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  int rc;
  if (!query || !domain_id || !principal_id || !out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(&session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_CREDENTIAL],
                                       2, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = TURBO_ENOENT;
  else if (rc == TURBO_OK) rc = flowie_control_pgsql_credential_record_decode(result, out);
  if (result) PQclear(result);
  return flowie_control_pgsql_query_session_close(&session, 0, rc);
}

int flowie_control_pgsql_query_credential_verify(
    flowie_control_pgsql_query_t *query, const char *domain_id, const char *principal_id,
    const void *secret, size_t secret_size, flowie_control_credential_verify_result_t *result) {
  flowie_control_pgsql_credential_record_t record = {0};
  flowie_control_pgsql_credential_record_t fresh = {0};
  flowie_control_credential_kdf_params_t dummy_params;
  uint8_t dummy_salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE] = {0};
  uint8_t dummy_verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE] = {0};
  int user_exists = 1;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !secret ||
      secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !result ||
      result->size < sizeof(*result))
    return TURBO_EINVAL;
  flowie_control_credential_default_params(&dummy_params);
  rc = flowie_control_pgsql_credential_record_read(query, domain_id, principal_id, &record);
  if (rc == TURBO_ENOENT) {
    user_exists = 0;
    rc = TURBO_OK;
  }
  if (rc == TURBO_OK)
    rc = flowie_control_credential_verify(
        secret, secret_size, record.credential_exists ? record.salt : dummy_salt,
        record.credential_exists ? record.verifier : dummy_verifier,
        record.credential_exists ? &record.params : &dummy_params);
  if (rc == TURBO_OK && (!user_exists || !record.user_enabled || !record.credential_exists ||
                         !record.credential_enabled))
    rc = TURBO_EPERM;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_credential_record_read(query, domain_id, principal_id, &fresh);
  if (rc == TURBO_OK && (!fresh.user_enabled || !fresh.credential_exists ||
                         !fresh.credential_enabled || fresh.user_revision != record.user_revision ||
                         fresh.credential_revision != record.credential_revision))
    rc = TURBO_EBUSY;
  if (rc == TURBO_OK) {
    result->user_revision = fresh.user_revision;
    result->credential_revision = fresh.credential_revision;
  }
  flowie_control_credential_wipe(&record, sizeof(record));
  flowie_control_credential_wipe(&fresh, sizeof(fresh));
  flowie_control_credential_wipe(dummy_salt, sizeof(dummy_salt));
  flowie_control_credential_wipe(dummy_verifier, sizeof(dummy_verifier));
  if (rc != TURBO_OK)
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_query_credential_state(flowie_control_pgsql_query_t *query,
                                                const char *domain_id, const char *principal_id,
                                                flowie_control_credential_verify_result_t *result) {
  flowie_control_pgsql_credential_record_t record = {0};
  int rc;
  if (result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !result ||
      result->size < sizeof(*result))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_credential_record_read(query, domain_id, principal_id, &record);
  if (rc == TURBO_ENOENT) rc = TURBO_EPERM;
  if (rc == TURBO_OK &&
      (!record.user_enabled || !record.credential_exists || !record.credential_enabled))
    rc = TURBO_EPERM;
  if (rc == TURBO_OK) {
    result->user_revision = record.user_revision;
    result->credential_revision = record.credential_revision;
  }
  flowie_control_credential_wipe(&record, sizeof(record));
  if (rc != TURBO_OK)
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  return rc;
}

int flowie_control_pgsql_query_current_revision(flowie_control_pgsql_query_t *query,
                                                uint64_t *revision_out) {
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  int rc;
  if (revision_out) *revision_out = 0u;
  if (!query || !revision_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(&session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_REVISION], 0,
                                       NULL, &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 0, 0u, INT64_MAX, revision_out);
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc != TURBO_OK) *revision_out = 0u;
  return rc;
}

static int flowie_control_pgsql_query_effective_groups_read(
    flowie_control_pgsql_query_t *query, flowie_control_pgsql_query_session_t *session,
    const char *domain_id, const char *principal_id,
    flowie_control_effective_groups_view_t *out) {
  const char *values[2] = {domain_id, principal_id};
  flowie_control_effective_groups_view_t view = FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  PGresult *result = NULL;
  int rc = flowie_control_pgsql_query_exec(session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_GROUPS],
                                           2, values, &result);
  if (rc == TURBO_OK && PQnfields(result) != 1) rc = TURBO_EPROTO;
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    if (view.group_count >= TURBO_FLOW_SECURITY_MAX_GROUPS) rc = TURBO_ENOSPC;
    else {
      rc = flowie_control_pgsql_result_copy(result, row, 0, view.groups[view.group_count],
                                            sizeof(view.groups[view.group_count]));
      if (rc == TURBO_OK) ++view.group_count;
    }
  }
  if (result) PQclear(result);
  if (rc == TURBO_OK) *out = view;
  return rc;
}

static int
flowie_control_pgsql_query_effective_roles_read(flowie_control_pgsql_query_t *query,
                                                flowie_control_pgsql_query_session_t *session,
                                                const char *domain_id, const char *principal_id,
                                                flowie_control_effective_roles_view_t *out) {
  const char *values[2] = {domain_id, principal_id};
  flowie_control_effective_roles_view_t view = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  PGresult *result = NULL;
  int rc = flowie_control_pgsql_query_exec(session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_ROLES], 2,
                                           values, &result);
  if (rc == TURBO_OK && PQnfields(result) != 1) rc = TURBO_EPROTO;
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    if (view.role_count >= TURBO_FLOW_SECURITY_MAX_ROLES) rc = TURBO_ENOSPC;
    else {
      rc = flowie_control_pgsql_result_copy(result, row, 0, view.roles[view.role_count],
                                            sizeof(view.roles[view.role_count]));
      if (rc == TURBO_OK) ++view.role_count;
    }
  }
  if (result) PQclear(result);
  if (rc == TURBO_OK) *out = view;
  return rc;
}

static int flowie_control_pgsql_query_principal(
    flowie_control_pgsql_query_t *query, const char *domain_id, const char *principal_id,
    const flowie_control_credential_verify_result_t *expected, uint64_t assertion_revision,
    flowie_control_principal_snapshot_t *out) {
  const int local = expected != NULL;
  const char *values[2] = {domain_id, principal_id};
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  uint64_t user_revision = 0u;
  uint64_t credential_revision = 0u;
  int user_enabled = 0;
  int credential_enabled = 0;
  int rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_exec(
        &session,
        query->sql[local ? FLOWIE_CONTROL_PGSQL_QUERY_LOCAL_PRINCIPAL
                         : FLOWIE_CONTROL_PGSQL_QUERY_EXTERNAL_PRINCIPAL],
        2, values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = TURBO_EPERM;
  else if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != (local ? 7 : 5)))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_result_bool(result, 0, 3, &user_enabled);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 4, 1u, INT64_MAX, &user_revision);
  if (rc == TURBO_OK && local && (PQgetisnull(result, 0, 5) || PQgetisnull(result, 0, 6)))
    rc = TURBO_EPERM;
  if (rc == TURBO_OK && local)
    rc = flowie_control_pgsql_result_bool(result, 0, 5, &credential_enabled);
  if (rc == TURBO_OK && local)
    rc = flowie_control_pgsql_result_uint64(result, 0, 6, 1u, INT64_MAX, &credential_revision);
  if (rc == TURBO_OK &&
      (!user_enabled ||
       (local && (!credential_enabled || user_revision != expected->user_revision ||
                  credential_revision != expected->credential_revision))))
    rc = TURBO_EPERM;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, 0, 0, snapshot.domain_id,
                                          sizeof(snapshot.domain_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, 0, 1, snapshot.principal_id,
                                          sizeof(snapshot.principal_id));
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_copy(result, 0, 2, snapshot.principal_type,
                                          sizeof(snapshot.principal_type));
  if (result) {
    PQclear(result);
    result = NULL;
  }
  if (rc == TURBO_OK) {
    snapshot.user_revision = user_revision;
    snapshot.credential_revision = local ? credential_revision : assertion_revision;
    rc = flowie_control_pgsql_query_effective_groups_read(query, &session, domain_id,
                                                          principal_id, &snapshot.effective_groups);
  }
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_effective_roles_read(query, &session, domain_id,
                                                         principal_id, &snapshot.effective_roles);
  rc = flowie_control_pgsql_query_session_close(&session, rc == TURBO_OK, rc);
  if (rc == TURBO_OK) *out = snapshot;
  return rc;
}

int flowie_control_pgsql_query_principal_snapshot(
    flowie_control_pgsql_query_t *query, const char *domain_id, const char *principal_id,
    const flowie_control_credential_verify_result_t *expected,
    flowie_control_principal_snapshot_t *out) {
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !expected ||
      expected->size < sizeof(*expected) || expected->user_revision == 0u ||
      expected->credential_revision == 0u || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  return flowie_control_pgsql_query_principal(query, domain_id, principal_id, expected, 0u,
                                              out);
}

int flowie_control_pgsql_query_external_principal_snapshot(
    flowie_control_pgsql_query_t *query, const char *domain_id, const char *principal_id,
    uint64_t assertion_revision, flowie_control_principal_snapshot_t *out) {
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      assertion_revision == 0u || !out || out->size < sizeof(*out))
    return TURBO_EINVAL;
  return flowie_control_pgsql_query_principal(query, domain_id, principal_id, NULL,
                                              assertion_revision, out);
}

static int flowie_control_pgsql_query_effective(flowie_control_pgsql_query_t *query,
                                                const char *domain_id, const char *principal_id,
                                                void *out, int groups) {
  flowie_control_pgsql_query_session_t session;
  int enabled = 0;
  int rc;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX) || !out)
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc =
        flowie_control_pgsql_query_enabled(query, &session, FLOWIE_CONTROL_PGSQL_QUERY_USER_ENABLED,
                                           domain_id, principal_id, &enabled);
  if (rc == TURBO_OK && !enabled) rc = TURBO_EPERM;
  if (rc == TURBO_OK && groups)
    rc = flowie_control_pgsql_query_effective_groups_read(
        query, &session, domain_id, principal_id,
        (flowie_control_effective_groups_view_t *)out);
  if (rc == TURBO_OK && !groups)
    rc = flowie_control_pgsql_query_effective_roles_read(
        query, &session, domain_id, principal_id, (flowie_control_effective_roles_view_t *)out);
  return flowie_control_pgsql_query_session_close(&session, rc == TURBO_OK, rc);
}

int flowie_control_pgsql_query_effective_groups(flowie_control_pgsql_query_t *query,
                                                const char *domain_id, const char *principal_id,
                                                flowie_control_effective_groups_view_t *out) {
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_effective_groups_view_t)FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  return flowie_control_pgsql_query_effective(query, domain_id, principal_id, out, 1);
}

int flowie_control_pgsql_query_group_list(flowie_control_pgsql_query_t *query,
                                          const char *domain_id, const char *after_group_id,
                                          flowie_control_group_view_t *items, size_t item_capacity,
                                          size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_text_page(
      query, FLOWIE_CONTROL_PGSQL_QUERY_GROUP_LIST, domain_id, after_group_id, items,
      sizeof(*items), item_capacity, flowie_control_pgsql_query_group_row, count_out, has_more_out);
}

int flowie_control_pgsql_query_effective_roles(flowie_control_pgsql_query_t *query,
                                               const char *domain_id, const char *principal_id,
                                               flowie_control_effective_roles_view_t *out) {
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_effective_roles_view_t)FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  return flowie_control_pgsql_query_effective(query, domain_id, principal_id, out, 0);
}

int flowie_control_pgsql_query_role_list(flowie_control_pgsql_query_t *query,
                                         const char *domain_id, const char *after_role_id,
                                         flowie_control_role_view_t *items, size_t item_capacity,
                                         size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_text_page(
      query, FLOWIE_CONTROL_PGSQL_QUERY_ROLE_LIST, domain_id, after_role_id, items,
      sizeof(*items), item_capacity, flowie_control_pgsql_query_role_row, count_out, has_more_out);
}

static int flowie_control_pgsql_query_acl_group_path_validate(
    flowie_control_pgsql_query_t *query, flowie_control_pgsql_query_session_t *session,
    const char *domain_id, const flowie_control_acl_entry_t *entry) {
  char previous[TURBO_FLOW_SECURITY_ID_MAX + 1u] = {0};
  int rc = TURBO_OK;
  if (!query || !session || !domain_id || !entry || entry->group_count == 0u ||
      entry->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS)
    return TURBO_EINVAL;
  for (size_t index = 0u; rc == TURBO_OK && index < entry->group_count; ++index) {
    char current[TURBO_FLOW_SECURITY_ID_MAX + 1u];
    const char *values[2] = {domain_id, current};
    const char *parent = NULL;
    size_t parent_size = 0u;
    size_t topic_size = strlen(entry->topic);
    size_t offset = entry->group_offsets[index];
    size_t length = entry->group_lengths[index];
    uint64_t depth = 0u;
    int enabled = 0;
    PGresult *result = NULL;
    if (length == 0u || length > TURBO_FLOW_SECURITY_ID_MAX || offset > topic_size ||
        length > topic_size - offset)
      return TURBO_EPROTO;
    memcpy(current, entry->topic + offset, length);
    current[length] = '\0';
    rc = flowie_control_pgsql_query_exec(
        session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_ACL_GROUP], 2, values, &result);
    if (rc == TURBO_OK && PQntuples(result) == 0) rc = TURBO_ENOENT;
    if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 3)) rc = TURBO_EPROTO;
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_uint64(result, 0, 1, 0u,
                                               FLOWIE_CONTROL_GROUP_MAX_DEPTH, &depth);
    if (rc == TURBO_OK) rc = flowie_control_pgsql_result_bool(result, 0, 2, &enabled);
    if (rc == TURBO_OK && !enabled) rc = TURBO_EPERM;
    if (rc == TURBO_OK && depth != index) rc = TURBO_EPROTO;
    if (rc == TURBO_OK && index == 0u && !PQgetisnull(result, 0, 0)) rc = TURBO_EPROTO;
    if (rc == TURBO_OK && index != 0u) {
      if (PQgetisnull(result, 0, 0))
        rc = TURBO_EPROTO;
      else
        rc = flowie_control_pgsql_result_text(result, 0, 0, &parent, &parent_size);
      if (rc == TURBO_OK &&
          (strlen(previous) != parent_size || memcmp(previous, parent, parent_size) != 0))
        rc = TURBO_EPROTO;
    }
    if (result) PQclear(result);
    if (rc == TURBO_OK) memcpy(previous, current, length + 1u);
  }
  return rc;
}

int flowie_control_pgsql_query_policy_validate(flowie_control_pgsql_query_t *query,
                                               const char *domain_id,
                                               flowie_control_policy_validation_t *out) {
  const char *values[1] = {domain_id};
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  char *subjects = NULL;
  size_t document_count = 0u;
  int rc;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_policy_validation_t)FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_exec(&session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_REVISION],
                                         0, NULL, &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc =
        flowie_control_pgsql_result_uint64(result, 0, 0, 0u, INT64_MAX, &validation.store_revision);
  if (result) {
    PQclear(result);
    result = NULL;
  }
  if (rc == TURBO_OK) rc = flowie_control_pgsql_query_domain_exists(query, &session, domain_id);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_exec(
        &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_POLICY_DRAFT_LINES], 1, values, &result);
  if (rc == TURBO_OK && PQnfields(result) != 1) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    subjects = (char *)calloc(TURBO_FLOW_SECURITY_MAX_RULES,
                              TURBO_FLOW_SECURITY_ID_MAX + 1u);
    if (!subjects) rc = TURBO_ENOMEM;
  }
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
    const char *line = NULL;
    size_t line_size = 0u;
    size_t expanded = 1u;
    size_t denied = 0u;
    int enabled = 0;
    if (document_count >= TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = TURBO_ENOSPC;
      break;
    }
    rc = flowie_control_pgsql_result_text(result, row, 0, &line, &line_size);
    if (rc == TURBO_OK)
      rc = flowie_control_acl_document_syntax_validate(domain_id, line, line_size, &document);
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_query_enabled(query, &session,
                                              FLOWIE_CONTROL_PGSQL_QUERY_USER_ENABLED,
                                              domain_id, document.subject, &enabled);
    if (rc == TURBO_OK && !enabled) rc = TURBO_EPERM;
    if (rc == TURBO_OK) {
      for (size_t prior = 0u; prior < document_count; ++prior)
        if (strcmp(subjects + prior * (TURBO_FLOW_SECURITY_ID_MAX + 1u), document.subject) == 0)
          rc = TURBO_EALREADY;
    }
    if (rc == TURBO_OK) {
      memcpy(subjects + document_count * (TURBO_FLOW_SECURITY_ID_MAX + 1u), document.subject,
             strlen(document.subject) + 1u);
      ++document_count;
      if (document.connection_effect == TURBO_FLOW_SECURITY_DENY) denied = 1u;
      for (size_t index = 0u; rc == TURBO_OK && index < document.entry_count; ++index) {
        const flowie_control_acl_entry_t *entry = &document.entries[index];
        rc = flowie_control_pgsql_query_acl_group_path_validate(query, &session, domain_id, entry);
        if (rc == TURBO_OK &&
            (entry->alternative_count == 0u ||
             expanded > TURBO_FLOW_SECURITY_MAX_RULES - entry->alternative_count))
          rc = TURBO_ENOSPC;
        if (rc == TURBO_OK) {
          expanded += entry->alternative_count;
          if (entry->effect == TURBO_FLOW_SECURITY_DENY) denied += entry->alternative_count;
        }
      }
    }
    if (rc == TURBO_OK) {
      if (expanded > TURBO_FLOW_SECURITY_MAX_RULES - validation.rule_count)
        rc = TURBO_ENOSPC;
      else {
        validation.rule_count += expanded;
        validation.deny_rule_count += denied;
      }
    }
  }
  if (rc == TURBO_OK && validation.rule_count == 0u) rc = TURBO_ENOENT;
  if (result) PQclear(result);
  free(subjects);
  rc = flowie_control_pgsql_query_session_close(&session, rc == TURBO_OK, rc);
  if (rc == TURBO_OK) *out = validation;
  return rc;
}

int flowie_control_pgsql_query_policy_rule_list(flowie_control_pgsql_query_t *query,
                                                const char *domain_id, uint32_t after_ordinal,
                                                int has_after,
                                                flowie_control_policy_rule_view_t *items,
                                                size_t item_capacity, size_t *count_out,
                                                int *has_more_out) {
  flowie_control_pgsql_query_session_t session;
  char after[32];
  char limit[32];
  const char *values[4] = {domain_id, has_after ? "1" : "0", after, limit};
  PGresult *result = NULL;
  size_t count = 0u;
  int written;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      (has_after != 0 && has_after != 1) || !items || item_capacity == 0u ||
      item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out || !has_more_out)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (items[index].size < sizeof(items[index])) return TURBO_EINVAL;
    items[index] = (flowie_control_policy_rule_view_t)FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
  }
  written = snprintf(after, sizeof(after), "%u", after_ordinal);
  if (written <= 0 || (size_t)written >= sizeof(after)) return TURBO_ERANGE;
  written = snprintf(limit, sizeof(limit), "%llu", (unsigned long long)(item_capacity + 1u));
  if (written <= 0 || (size_t)written >= sizeof(limit)) return TURBO_ERANGE;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(
      &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_POLICY_RULE_LIST], 4, values, &result);
  if (rc == TURBO_OK && PQnfields(result) != 4) rc = TURBO_EPROTO;
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    flowie_control_policy_rule_view_t *view;
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    view = &items[count];
    rc = flowie_control_pgsql_result_uint32(result, row, 0, 0u, TURBO_FLOW_SECURITY_MAX_RULES - 1u,
                                            &view->ordinal);
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_copy(result, row, 1, view->rule_line,
                                            sizeof(view->rule_line));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_uint64(result, row, 2, 1u, INT64_MAX, &view->revision);
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_uint64(result, row, 3, 1u, INT64_MAX, &view->updated_at);
    if (rc == TURBO_OK) ++count;
  }
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc == TURBO_OK) *count_out = count;
  else {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

int flowie_control_pgsql_query_policy_status(flowie_control_pgsql_query_t *query,
                                             const char *domain_id,
                                             flowie_control_policy_status_t *out) {
  const char *values[1] = {domain_id};
  flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  uint64_t draft_count = 0u;
  uint64_t published_count = 0u;
  int rc;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_policy_status_t)FLOWIE_CONTROL_POLICY_STATUS_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !out ||
      out->size < sizeof(*out))
    return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(
      &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_POLICY_STATUS], 1, values, &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 5)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 0, 0u, INT64_MAX, &status.store_revision);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 1, 0u, SIZE_MAX, &draft_count);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 2, 0u, INT64_MAX, &status.policy_version);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 3, 0u, INT64_MAX, &status.expires_at);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 4, 0u, SIZE_MAX, &published_count);
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc == TURBO_OK) {
    status.draft_rule_count = (size_t)draft_count;
    status.published_rule_count = (size_t)published_count;
    *out = status;
  }
  return rc;
}

void flowie_control_pgsql_query_policy_bundle_release(turbo_flow_security_policy_bundle_t *bundle) {
  flowie_control_pgsql_bundle_owner_t *owner;
  if (!bundle) return;
  owner = (flowie_control_pgsql_bundle_owner_t *)bundle->provider_bundle;
  if (owner) {
    free(owner->rules);
    free(owner);
  }
  *bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
}

int flowie_control_pgsql_query_policy_bundle_load(flowie_control_pgsql_query_t *query,
                                                  const char *domain_id,
                                                  uint64_t required_version,
                                                  turbo_flow_security_policy_bundle_t *bundle_out) {
  flowie_control_pgsql_bundle_owner_t *owner = NULL;
  flowie_control_pgsql_query_session_t session;
  turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  char version[32];
  const char *meta_values[2] = {domain_id, version};
  const char *rule_values[1] = {domain_id};
  PGresult *result = NULL;
  uint64_t rule_count_value = 0u;
  size_t expected_ordinal = 0u;
  int written;
  int rc;
  if (bundle_out && bundle_out->size >= sizeof(*bundle_out))
    *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      required_version > (uint64_t)INT64_MAX || !bundle_out ||
      bundle_out->size < sizeof(*bundle_out))
    return TURBO_EINVAL;
  written = snprintf(version, sizeof(version), "%llu", (unsigned long long)required_version);
  if (written <= 0 || (size_t)written >= sizeof(version)) return TURBO_ERANGE;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_transaction_begin(&session);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_exec(
        &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_BUNDLE_META], 2, meta_values, &result);
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = TURBO_ENOENT;
  else if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 3)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 0, 1u, INT64_MAX, &bundle.policy_version);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 1, 0u, INT64_MAX, &bundle.expires_at);
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_result_uint64(result, 0, 2, 1u, TURBO_FLOW_SECURITY_MAX_RULES,
                                            &rule_count_value);
  if (result) {
    PQclear(result);
    result = NULL;
  }
  if (rc == TURBO_OK) {
    owner = (flowie_control_pgsql_bundle_owner_t *)calloc(1u, sizeof(*owner));
    if (!owner) rc = TURBO_ENOMEM;
    else {
      owner->rules =
          (turbo_flow_security_rule_t *)calloc((size_t)rule_count_value, sizeof(*owner->rules));
      if (!owner->rules) rc = TURBO_ENOMEM;
    }
  }
  if (rc == TURBO_OK)
    rc = flowie_control_pgsql_query_exec(
        &session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_BUNDLE_RULES], 1, rule_values, &result);
  if (rc == TURBO_OK && PQnfields(result) != 2) rc = TURBO_EPROTO;
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    const char *line = NULL;
    size_t line_size = 0u;
    uint64_t ordinal = 0u;
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    if (expected_ordinal >= (size_t)rule_count_value) rc = TURBO_EPROTO;
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_uint64(result, row, 0, 0u, INT64_MAX, &ordinal);
    if (rc == TURBO_OK) rc = flowie_control_pgsql_result_text(result, row, 1, &line, &line_size);
    if (rc == TURBO_OK &&
        (ordinal != expected_ordinal || line_size == 0u ||
         line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX ||
         turbo_flow_security_rule_parse_line(line, line_size, &rule) != TURBO_OK ||
         strcmp(rule.domain_id, domain_id) != 0))
      rc = TURBO_EPROTO;
    if (rc == TURBO_OK) owner->rules[expected_ordinal++] = rule;
  }
  if (rc == TURBO_OK && expected_ordinal != (size_t)rule_count_value) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, rc == TURBO_OK, rc);
  if (rc == TURBO_OK) {
    bundle.rules = owner->rules;
    bundle.rule_count = (size_t)rule_count_value;
    bundle.provider_bundle = owner;
    *bundle_out = bundle;
    owner = NULL;
  }
  if (owner) {
    free(owner->rules);
    free(owner);
  }
  return rc;
}

int flowie_control_pgsql_query_audit_list(flowie_control_pgsql_query_t *query,
                                          const char *domain_id, uint64_t after_revision,
                                          flowie_control_audit_view_t *items, size_t item_capacity,
                                          size_t *count_out, int *has_more_out) {
  flowie_control_pgsql_query_session_t session;
  char after[32];
  char limit[32];
  const char *values[3] = {domain_id, after, limit};
  PGresult *result = NULL;
  size_t count = 0u;
  int written;
  int rc;
  if (count_out) *count_out = 0u;
  if (has_more_out) *has_more_out = 0;
  if (!query || !flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      after_revision > (uint64_t)INT64_MAX || !items || item_capacity == 0u ||
      item_capacity > FLOWIE_CONTROL_PAGE_MAX || !count_out || !has_more_out)
    return TURBO_EINVAL;
  for (size_t index = 0u; index < item_capacity; ++index) {
    if (items[index].size < sizeof(items[index])) return TURBO_EINVAL;
    items[index] = (flowie_control_audit_view_t)FLOWIE_CONTROL_AUDIT_VIEW_INIT;
  }
  written = snprintf(after, sizeof(after), "%llu", (unsigned long long)after_revision);
  if (written <= 0 || (size_t)written >= sizeof(after)) return TURBO_ERANGE;
  written = snprintf(limit, sizeof(limit), "%llu", (unsigned long long)(item_capacity + 1u));
  if (written <= 0 || (size_t)written >= sizeof(limit)) return TURBO_ERANGE;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(&session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_AUDIT_LIST],
                                       3, values, &result);
  if (rc == TURBO_OK && PQnfields(result) != 8) rc = TURBO_EPROTO;
  for (int row = 0; rc == TURBO_OK && row < PQntuples(result); ++row) {
    flowie_control_audit_view_t *view;
    if (count == item_capacity) {
      *has_more_out = 1;
      continue;
    }
    view = &items[count];
    rc = flowie_control_pgsql_result_copy(result, row, 0, view->request_id,
                                          sizeof(view->request_id));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_copy(result, row, 1, view->actor, sizeof(view->actor));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_copy(result, row, 2, view->operation,
                                            sizeof(view->operation));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_copy(result, row, 3, view->domain_id,
                                            sizeof(view->domain_id));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_copy(result, row, 4, view->target_id,
                                            sizeof(view->target_id));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_copy(result, row, 5, view->target_detail,
                                            sizeof(view->target_detail));
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_uint64(result, row, 6, 1u, INT64_MAX, &view->revision);
    if (rc == TURBO_OK)
      rc = flowie_control_pgsql_result_uint64(result, row, 7, 1u, INT64_MAX, &view->occurred_at);
    if (rc == TURBO_OK) ++count;
  }
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc == TURBO_OK) *count_out = count;
  else {
    *count_out = 0u;
    *has_more_out = 0;
  }
  return rc;
}

int flowie_control_pgsql_query_audit_count(flowie_control_pgsql_query_t *query, size_t *count_out) {
  flowie_control_pgsql_query_session_t session;
  PGresult *result = NULL;
  uint64_t count = 0u;
  int rc;
  if (count_out) *count_out = 0u;
  if (!query || !count_out) return TURBO_EINVAL;
  rc = flowie_control_pgsql_query_session_open(query, &session);
  if (rc != TURBO_OK) return rc;
  rc = flowie_control_pgsql_query_exec(&session, query->sql[FLOWIE_CONTROL_PGSQL_QUERY_AUDIT_COUNT],
                                       0, NULL, &result);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 1)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_control_pgsql_result_uint64(result, 0, 0, 0u, SIZE_MAX, &count);
  if (result) PQclear(result);
  rc = flowie_control_pgsql_query_session_close(&session, 0, rc);
  if (rc == TURBO_OK) *count_out = (size_t)count;
  return rc;
}
