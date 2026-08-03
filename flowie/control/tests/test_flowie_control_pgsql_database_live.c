#include "flowie_control_auth_repository_contract.h"
#include "flowie_control_bootstrap_internal.h"
#include "flowie_control_credential_internal.h"
#include "flowie_control_management_repository_contract.h"
#include "flowie_control_pgsql_command_internal.h"
#include "flowie_control_pgsql_database_internal.h"
#include "flowie_control_pgsql_query_internal.h"
#include "flowie_control_pgsql_repository_internal.h"
#include "flowie_control_repository_contract.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include "libpq-fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FLOWIE_CONTROL_PGSQL_TEST_SEED_SQL_CAPACITY = 8192 };

static void hex_encode(const uint8_t *input, size_t input_size, char *output,
                       size_t output_capacity) {
  static const char digits[] = "0123456789abcdef";
  check_not_null(input);
  check_not_null(output);
  check_true(input_size <= (output_capacity - 1u) / 2u);
  for (size_t index = 0u; index < input_size; ++index) {
    output[index * 2u] = digits[input[index] >> 4u];
    output[index * 2u + 1u] = digits[input[index] & 0x0fu];
  }
  output[input_size * 2u] = '\0';
}

static void seed_query_contract(PGconn *connection, const char *schema_name,
                                const flowie_control_credential_kdf_params_t *params,
                                const uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
                                const uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE]) {
  static const char deny_rule[] = "deny|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/private/#";
  static const char allow_rule[] =
      "allow|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/events/#";
  char salt_hex[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE * 2u + 1u];
  char verifier_hex[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE * 2u + 1u];
  char sql[FLOWIE_CONTROL_PGSQL_TEST_SEED_SQL_CAPACITY];
  int written;
  PGresult *result;

  hex_encode(salt, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE, salt_hex, sizeof(salt_hex));
  hex_encode(verifier, FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE, verifier_hex, sizeof(verifier_hex));
  written = snprintf(
      sql, sizeof(sql),
      "BEGIN;"
      "INSERT INTO %s.root_group(root_group_id) VALUES('root-a');"
      "INSERT INTO %s.security_group(root_group_id,group_id,parent_group_id,depth,enabled,"
      "revision,created_at,updated_at) VALUES"
      "('root-a','root-a',NULL,0,true,1,1000,1000),"
      "('root-a','operators','root-a',1,true,4,1003,1003);"
      "INSERT INTO %s.user_account(root_group_id,principal_id,principal_type,enabled,revision,"
      "created_at,updated_at) VALUES('root-a','device-7','device',true,2,1001,1001);"
      "INSERT INTO %s.credential(root_group_id,principal_id,kdf_algorithm,memory_blocks,passes,"
      "lanes,salt,verifier,enabled,revision,created_at,updated_at) VALUES"
      "('root-a','device-7',%u,%u,%u,%u,pg_catalog.decode('%s','hex'),"
      "pg_catalog.decode('%s','hex'),true,3,1002,1002);"
      "INSERT INTO %s.security_role(root_group_id,role_id,enabled,revision,created_at,updated_at)"
      " VALUES('root-a','reader',true,5,1004,1004);"
      "INSERT INTO %s.membership(root_group_id,principal_id,group_id,revision,created_at)"
      " VALUES('root-a','device-7','operators',6,1005);"
      "INSERT INTO %s.user_role(root_group_id,principal_id,role_id,revision,created_at)"
      " VALUES('root-a','device-7','reader',7,1006);"
      "INSERT INTO %s.acl_bundle(namespace_name,policy_version,expires_at)"
      " VALUES('root-a',1,20000);"
      "INSERT INTO %s.acl_rule(namespace_name,ordinal,rule_line) VALUES"
      "('root-a',0,'%s'),('root-a',1,'%s');"
      "INSERT INTO %s.audit(request_id,actor,operation,root_group_id,target_id,target_detail,"
      "result_revision,occurred_at) VALUES"
      "('request-user','admin-1','user.create','root-a','device-7','device',2,1001),"
      "('request-role','admin-1','role.create','root-a','reader','role',5,1004);"
      "UPDATE %s.meta SET revision=7 WHERE singleton=1;"
      "COMMIT;",
      schema_name, schema_name, schema_name, schema_name, params->algorithm, params->memory_blocks,
      params->passes, params->lanes, salt_hex, verifier_hex, schema_name, schema_name, schema_name,
      schema_name, schema_name, deny_rule, allow_rule, schema_name, schema_name);
  check_true(written > 0);
  check_true((size_t)written < sizeof(sql));
  result = PQexec(connection, sql);
  check_not_null(result);
  check_int_eq(PQresultStatus(result), PGRES_COMMAND_OK);
  PQclear(result);
}

static void drop_test_schema(const char *conninfo, const char *schema_name) {
  const char *keywords[] = {"dbname", "connect_timeout", "application_name", NULL};
  const char *values[] = {conninfo, "5", "flowie-control-test-cleanup", NULL};
  char sql[160];
  PGconn *connection = PQconnectdbParams(keywords, values, 1);
  PGresult *result;
  check_not_null(connection);
  check_int_eq(PQstatus(connection), CONNECTION_OK);
  check_true(snprintf(sql, sizeof(sql), "DROP SCHEMA \"%s\" CASCADE", schema_name) > 0);
  result = PQexec(connection, sql);
  check_not_null(result);
  check_int_eq(PQresultStatus(result), PGRES_COMMAND_OK);
  PQclear(result);
  PQfinish(connection);
}

static void terminate_test_backend(const char *conninfo, int backend_pid) {
  const char *keywords[] = {"dbname", "connect_timeout", "application_name", NULL};
  const char *values[] = {conninfo, "5", "flowie-control-test-terminator", NULL};
  char sql[96];
  PGconn *connection = PQconnectdbParams(keywords, values, 1);
  PGresult *result;
  check_not_null(connection);
  check_int_eq(PQstatus(connection), CONNECTION_OK);
  check_true(snprintf(sql, sizeof(sql), "SELECT pg_catalog.pg_terminate_backend(%d)",
                      backend_pid) > 0);
  result = PQexec(connection, sql);
  check_not_null(result);
  check_int_eq(PQresultStatus(result), PGRES_TUPLES_OK);
  check_str_eq(PQgetvalue(result, 0, 0), "t");
  PQclear(result);
  PQfinish(connection);
}

spec("Flowie control PostgreSQL database live") {
  it("bootstraps one administrator through the PostgreSQL repository contract") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    static const char password[] = "postgres-bootstrap-password";
    char schema_name[64];
    flowie_control_pgsql_pool_config_t pool_config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_repository_provider_t *provider = NULL;
    const flowie_control_repository_t *repository;
    flowie_control_config_bootstrap_t bootstrap = {0};
    flowie_control_credential_verify_result_t credential =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_bootstrap_%llu",
                   (unsigned long long)turbo_hrtime());
    pool_config.database.conninfo = conninfo;
    pool_config.database.schema_name = schema_name;
    pool_config.database.require_tls = strstr(conninfo, "sslmode=verify-full") != NULL;
    pool_config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    pool_config.capacity = 2u;
    bootstrap.enabled = 1;
    (void)snprintf(bootstrap.root_group_id, sizeof(bootstrap.root_group_id), "%s", "root-a");
    (void)snprintf(bootstrap.principal_id, sizeof(bootstrap.principal_id), "%s", "admin-a");
    (void)snprintf(bootstrap.principal_type, sizeof(bootstrap.principal_type), "%s", "human");
    (void)snprintf(bootstrap.password_ref, sizeof(bootstrap.password_ref), "%s",
                   "env://FLOWIE_BOOTSTRAP_PASSWORD");

    check_int_eq(flowie_control_pgsql_repository_create(&pool_config, &provider), TURBO_OK);
    repository = flowie_control_pgsql_repository_view(provider);
    check_not_null(repository);
    check_int_eq(flowie_control_bootstrap_apply(repository, &bootstrap, password,
                                                sizeof(password) - 1u, 1000u),
                 TURBO_OK);
    check_int_eq(flowie_control_bootstrap_apply(repository, &bootstrap, password,
                                                sizeof(password) - 1u, 2000u),
                 TURBO_OK);
    check_int_eq(repository->auth->credential_verify(repository->ctx, "root-a", "admin-a", password,
                                                     sizeof(password) - 1u, &credential),
                 TURBO_OK);
    check_int_eq(flowie_control_pgsql_repository_destroy(provider, 5000), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("serializes migration and validates the resulting schema version") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_database_config_t config = FLOWIE_CONTROL_PGSQL_DATABASE_CONFIG_INIT;
    flowie_control_pgsql_database_t *database = NULL;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_%llu",
                   (unsigned long long)turbo_hrtime());
    config.conninfo = conninfo;
    config.schema_name = schema_name;
    config.require_tls = 0;
    config.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    check_int_eq(flowie_control_pgsql_database_open(&config, &database), TURBO_OK);
    check_uint_eq(flowie_control_pgsql_database_schema_version(database),
                  FLOWIE_CONTROL_PGSQL_SCHEMA_VERSION);
    flowie_control_pgsql_database_destroy(database);
    database = NULL;

    config.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_VALIDATE;
    check_int_eq(flowie_control_pgsql_database_open(&config, &database), TURBO_OK);
    check_uint_eq(flowie_control_pgsql_database_schema_version(database),
                  FLOWIE_CONTROL_PGSQL_SCHEMA_VERSION);
    flowie_control_pgsql_database_destroy(database);
    drop_test_schema(conninfo, schema_name);
  }

  it("bounds leases, rolls back abandoned transactions, and closes quiescently") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_pool_lease_t first = FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
    flowie_control_pgsql_pool_lease_t second = FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
    flowie_control_pgsql_pool_stats_t stats = FLOWIE_CONTROL_PGSQL_POOL_STATS_INIT;
    PGresult *result;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_pool_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 1u;
    config.acquire_timeout_ms = 20;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_not_null(pool);

    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &first), TURBO_OK);
    check_not_null(flowie_control_pgsql_pool_lease_connection(&first));
    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &second), TURBO_ETIMEDOUT);
    check_null(flowie_control_pgsql_pool_lease_connection(&second));
    check_int_eq(flowie_control_pgsql_pool_stats(pool, &stats), TURBO_OK);
    check_uint_eq(stats.capacity, 1u);
    check_uint_eq(stats.healthy, 1u);
    check_uint_eq(stats.available, 0u);
    check_uint_eq(stats.leased, 1u);
    check_uint_eq(stats.acquisition_timeouts, 1u);

    result = PQexec(flowie_control_pgsql_pool_lease_connection(&first), "BEGIN");
    check_not_null(result);
    check_int_eq(PQresultStatus(result), PGRES_COMMAND_OK);
    PQclear(result);
    check_int_eq(flowie_control_pgsql_pool_release(&first), TURBO_OK);
    check_null(flowie_control_pgsql_pool_lease_connection(&first));

    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &second), TURBO_OK);
    check_int_eq(PQtransactionStatus(flowie_control_pgsql_pool_lease_connection(&second)),
                 PQTRANS_IDLE);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 1), TURBO_ETIMEDOUT);
    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &first), TURBO_ESHUTDOWN);
    check_int_eq(flowie_control_pgsql_pool_release(&second), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 20), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);

    drop_test_schema(conninfo, schema_name);
  }

  it("reopens a dead pool slot asynchronously after PostgreSQL becomes healthy") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_pool_lease_t lease = FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
    flowie_control_pgsql_pool_stats_t stats = FLOWIE_CONTROL_PGSQL_POOL_STATS_INIT;
    flowie_control_pgsql_database_t *restored = NULL;
    PGconn *connection;
    PGresult *result;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_reconnect_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = strstr(conninfo, "sslmode=verify-full") != NULL;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 1u;
    config.acquire_timeout_ms = 5000;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &lease), TURBO_OK);
    connection = flowie_control_pgsql_pool_lease_connection(&lease);
    check_not_null(connection);

    terminate_test_backend(conninfo, PQbackendPID(connection));
    result = PQexec(connection, "SELECT 1");
    check_not_null(result);
    check_int_eq(PQresultStatus(result), PGRES_FATAL_ERROR);
    PQclear(result);
    drop_test_schema(conninfo, schema_name);

    check_int_ne(flowie_control_pgsql_pool_release(&lease), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_stats(pool, &stats), TURBO_OK);
    check_uint_eq(stats.healthy, 0u);
    check_uint_eq(stats.available, 0u);
    check_uint_eq(stats.cleanup_failures, 1u);

    check_int_eq(flowie_control_pgsql_database_open(&config.database, &restored), TURBO_OK);
    flowie_control_pgsql_database_destroy(restored);
    restored = NULL;
    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &lease), TURBO_OK);
    check_not_null(flowie_control_pgsql_pool_lease_connection(&lease));
    check_int_eq(flowie_control_pgsql_pool_stats(pool, &stats), TURBO_OK);
    check_uint_eq(stats.healthy, 1u);
    check_uint_eq(stats.leased, 1u);
    check_int_eq(flowie_control_pgsql_pool_release(&lease), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_stats(pool, &stats), TURBO_OK);
    check_uint_eq(stats.available, 1u);

    check_int_eq(flowie_control_pgsql_pool_close(pool, 5000), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("reads credential generations, consistent principals, and immutable ACL bundles") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_pool_lease_t seed = FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_credential_kdf_params_t params;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    flowie_control_principal_snapshot_t principal = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    flowie_control_audit_view_t audit[1] = {FLOWIE_CONTROL_AUDIT_VIEW_INIT};
    uint8_t secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE] = {0};
    uint8_t wrong_secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE] = {0};
    uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE] = {0};
    uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE] = {0};
    uint64_t revision = 0u;
    size_t audit_count = 0u;
    size_t page_count = 0u;
    int has_more = 0;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_query_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    flowie_control_credential_default_params(&params);
    check_int_eq(flowie_control_credential_generate(secret, salt, verifier, &params), TURBO_OK);
    memcpy(wrong_secret, secret, sizeof(wrong_secret));
    wrong_secret[0] ^= 0xffu;

    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &seed), TURBO_OK);
    seed_query_contract(flowie_control_pgsql_pool_lease_connection(&seed), schema_name, &params,
                        salt, verifier);
    check_int_eq(flowie_control_pgsql_pool_release(&seed), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 7u);
    check_int_eq(
        flowie_control_pgsql_query_credential_state(query, "root-a", "device-7", &verified),
        TURBO_OK);
    check_uint_eq(verified.user_revision, 2u);
    check_uint_eq(verified.credential_revision, 3u);
    check_int_eq(flowie_control_pgsql_query_credential_verify(
                     query, "root-a", "device-7", wrong_secret, sizeof(wrong_secret), &verified),
                 TURBO_EPERM);
    check_int_eq(flowie_control_pgsql_query_credential_verify(query, "root-a", "device-7", secret,
                                                              sizeof(secret), &verified),
                 TURBO_OK);

    check_int_eq(flowie_control_pgsql_query_principal_snapshot(query, "root-a", "device-7",
                                                               &verified, &principal),
                 TURBO_OK);
    check_str_eq(principal.principal_type, "device");
    check_uint_eq(principal.effective_groups.group_count, 2u);
    check_str_eq(principal.effective_groups.groups[0], "root-a");
    check_str_eq(principal.effective_groups.groups[1], "operators");
    check_uint_eq(principal.effective_roles.role_count, 1u);
    check_str_eq(principal.effective_roles.roles[0], "reader");

    principal = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    check_int_eq(flowie_control_pgsql_query_external_principal_snapshot(query, "root-a", "device-7",
                                                                        99u, &principal),
                 TURBO_OK);
    check_uint_eq(principal.credential_revision, 99u);

    check_int_eq(flowie_control_pgsql_query_policy_bundle_load(query, "root-a", 0u, &bundle),
                 TURBO_OK);
    check_uint_eq(bundle.policy_version, 1u);
    check_uint_eq(bundle.expires_at, 20000u);
    check_uint_eq(bundle.rule_count, 2u);
    check_str_eq(bundle.rules[0].pattern, "root-a/private/#");
    check_str_eq(bundle.rules[1].pattern, "root-a/events/#");
    flowie_control_pgsql_query_policy_bundle_release(&bundle);
    check_int_eq(flowie_control_pgsql_query_policy_bundle_load(query, "root-a", 1u, &bundle),
                 TURBO_OK);
    flowie_control_pgsql_query_policy_bundle_release(&bundle);
    check_int_eq(flowie_control_pgsql_query_policy_bundle_load(query, "root-a", 2u, &bundle),
                 TURBO_ENOENT);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, 2u);
    check_int_eq(flowie_control_pgsql_query_audit_list(query, "root-a", 0u, audit, 1u, &page_count,
                                                       &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_true(has_more);
    check_str_eq(audit[0].request_id, "request-user");
    check_uint_eq(audit[0].revision, 2u);

    flowie_control_credential_wipe(secret, sizeof(secret));
    flowie_control_credential_wipe(wrong_secret, sizeof(wrong_secret));
    flowie_control_credential_wipe(salt, sizeof(salt));
    flowie_control_credential_wipe(verifier, sizeof(verifier));
    flowie_control_pgsql_query_destroy(query);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("commits root and user commands with replay, revision, ACL, and audit invariants") {
    static const char referenced_rule[] =
        "allow|principal|device-7|root-a|publish|mqtt_topic|adapter|root-a/events/#";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    char policy_sql[1024];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_pool_lease_t seed = FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t stale = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_user_disable_command_t disable = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
    flowie_control_command_result_t command_result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_principal_snapshot_t principal = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    uint64_t revision = 0u;
    size_t audit_count = 0u;
    PGresult *result;
    int written;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_command_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 1u);
    check_false(command_result.replayed);
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 1u);
    check_true(command_result.replayed);
    root.actor = "different-admin";
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &command_result),
                 TURBO_EBUSY);
    root.actor = "admin-1";

    user.root_group_id = "root-a";
    user.principal_id = "device-7";
    user.principal_type = "device";
    user.actor = "admin-1";
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 2u);
    check_false(command_result.replayed);
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &command_result),
                 TURBO_OK);
    check_true(command_result.replayed);

    stale = user;
    stale.principal_id = "device-stale";
    stale.request_id = "request-user-stale";
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &stale, &command_result),
                 TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 2u);

    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &seed), TURBO_OK);
    written = snprintf(policy_sql, sizeof(policy_sql),
                       "INSERT INTO %s.policy_draft(root_group_id,ordinal,rule_line,revision,"
                       "updated_at) VALUES('root-a',0,'%s',2,1002)",
                       schema_name, referenced_rule);
    check_true(written > 0);
    check_true((size_t)written < sizeof(policy_sql));
    result = PQexec(flowie_control_pgsql_pool_lease_connection(&seed), policy_sql);
    check_not_null(result);
    check_int_eq(PQresultStatus(result), PGRES_COMMAND_OK);
    PQclear(result);
    check_int_eq(flowie_control_pgsql_pool_release(&seed), TURBO_OK);

    disable.root_group_id = "root-a";
    disable.principal_id = "device-7";
    disable.actor = "admin-1";
    disable.request_id = "request-disable";
    disable.expected_revision = 2u;
    disable.occurred_at = 1003u;
    check_int_eq(flowie_control_pgsql_command_user_disable(commands, &disable, &command_result),
                 TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 2u);

    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &seed), TURBO_OK);
    written = snprintf(policy_sql, sizeof(policy_sql),
                       "DELETE FROM %s.policy_draft WHERE root_group_id='root-a'", schema_name);
    check_true(written > 0);
    check_true((size_t)written < sizeof(policy_sql));
    result = PQexec(flowie_control_pgsql_pool_lease_connection(&seed), policy_sql);
    check_not_null(result);
    check_int_eq(PQresultStatus(result), PGRES_COMMAND_OK);
    PQclear(result);
    check_int_eq(flowie_control_pgsql_pool_release(&seed), TURBO_OK);

    check_int_eq(flowie_control_pgsql_command_user_disable(commands, &disable, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 3u);
    check_false(command_result.replayed);
    check_int_eq(flowie_control_pgsql_command_user_disable(commands, &disable, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 3u);
    check_true(command_result.replayed);
    check_int_eq(flowie_control_pgsql_query_external_principal_snapshot(query, "root-a", "device-7",
                                                                        1u, &principal),
                 TURBO_EPERM);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 3u);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, 3u);

    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("confirms an uncertain commit only from an exact durable audit identity") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    int committed = 0;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_commit_confirm_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 1u);

    check_int_eq(flowie_control_pgsql_command_commit_confirm(
                     commands, "request-root", "admin-1", "root_group.create", "root-a", "root-a",
                     "root_group", 1u, &committed),
                 TURBO_OK);
    check_true(committed);

    committed = 1;
    check_int_eq(flowie_control_pgsql_command_commit_confirm(
                     commands, "request-missing", "admin-1", "root_group.create", "root-a",
                     "root-a", "root_group", 1u, &committed),
                 TURBO_OK);
    check_false(committed);

    committed = 1;
    check_int_eq(flowie_control_pgsql_command_commit_confirm(
                     commands, "request-root", "different-admin", "root_group.create", "root-a",
                     "root-a", "root_group", 1u, &committed),
                 TURBO_EBUSY);
    check_false(committed);

    committed = 1;
    check_int_eq(flowie_control_pgsql_command_commit_confirm(
                     commands, "request-root", "admin-1", "root_group.create", "root-a", "root-a",
                     "root_group", 2u, &committed),
                 TURBO_EPROTO);
    check_false(committed);

    check_int_eq(flowie_control_pgsql_command_commit_confirm(commands, "request-root", "admin-1",
                                                             "root_group.create", "root-a",
                                                             "root-a", NULL, 1u, &committed),
                 TURBO_EINVAL);

    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("commits credential generate, rotate, revoke, and reactivation semantics") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_user_disable_command_t disable = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
    flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
    flowie_control_credential_revoke_command_t revoke =
        FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_generated_credential_t rotated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    flowie_control_command_result_t command_result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    uint8_t first_secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE] = {0};
    uint8_t active_secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE] = {0};
    uint64_t revision = 0u;
    size_t audit_count = 0u;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_credential_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &command_result),
                 TURBO_OK);

    user.root_group_id = "root-a";
    user.principal_id = "device-7";
    user.principal_type = "device";
    user.actor = "admin-1";
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &command_result),
                 TURBO_OK);

    issue.root_group_id = "root-a";
    issue.principal_id = "device-7";
    issue.actor = "admin-1";
    issue.request_id = "request-credential-generate";
    issue.expected_revision = 2u;
    issue.occurred_at = 1002u;
    check_int_eq(flowie_control_pgsql_command_credential_generate(commands, &issue, &generated),
                 TURBO_OK);
    check_uint_eq(generated.revision, 3u);
    check_uint_eq(generated.secret_size, sizeof(generated.secret));
    memcpy(first_secret, generated.secret, sizeof(first_secret));
    check_int_eq(flowie_control_pgsql_query_credential_verify(
                     query, "root-a", "device-7", first_secret, sizeof(first_secret), &verified),
                 TURBO_OK);
    check_uint_eq(verified.credential_revision, 3u);

    check_int_eq(flowie_control_pgsql_command_credential_generate(commands, &issue, &generated),
                 TURBO_EALREADY);
    check_uint_eq(generated.secret_size, 0u);
    issue.actor = "different-admin";
    check_int_eq(flowie_control_pgsql_command_credential_generate(commands, &issue, &generated),
                 TURBO_EBUSY);
    issue.actor = "admin-1";
    issue.request_id = "request-credential-generate-existing";
    issue.expected_revision = 3u;
    check_int_eq(flowie_control_pgsql_command_credential_generate(commands, &issue, &generated),
                 TURBO_EALREADY);

    issue.request_id = "request-credential-rotate";
    issue.expected_revision = 3u;
    issue.occurred_at = 1003u;
    check_int_eq(flowie_control_pgsql_command_credential_rotate(commands, &issue, &rotated),
                 TURBO_OK);
    check_uint_eq(rotated.revision, 4u);
    check_uint_eq(rotated.secret_size, sizeof(rotated.secret));
    check_mem_ne(rotated.secret, first_secret, sizeof(first_secret));
    memcpy(active_secret, rotated.secret, sizeof(active_secret));
    check_int_eq(flowie_control_pgsql_query_credential_verify(
                     query, "root-a", "device-7", first_secret, sizeof(first_secret), &verified),
                 TURBO_EPERM);
    check_int_eq(flowie_control_pgsql_query_credential_verify(
                     query, "root-a", "device-7", active_secret, sizeof(active_secret), &verified),
                 TURBO_OK);
    issue.request_id = "request-credential-rotate-stale";
    issue.expected_revision = 3u;
    check_int_eq(flowie_control_pgsql_command_credential_rotate(commands, &issue, &rotated),
                 TURBO_EBUSY);
    check_uint_eq(rotated.secret_size, 0u);

    revoke.root_group_id = "root-a";
    revoke.principal_id = "device-7";
    revoke.actor = "admin-1";
    revoke.request_id = "request-credential-revoke";
    revoke.expected_revision = 4u;
    revoke.occurred_at = 1004u;
    check_int_eq(flowie_control_pgsql_command_credential_revoke(commands, &revoke, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 5u);
    check_false(command_result.replayed);
    check_int_eq(flowie_control_pgsql_command_credential_revoke(commands, &revoke, &command_result),
                 TURBO_OK);
    check_uint_eq(command_result.revision, 5u);
    check_true(command_result.replayed);
    check_int_eq(
        flowie_control_pgsql_query_credential_state(query, "root-a", "device-7", &verified),
        TURBO_EPERM);
    revoke.request_id = "request-credential-revoke-disabled";
    revoke.expected_revision = 5u;
    check_int_eq(flowie_control_pgsql_command_credential_revoke(commands, &revoke, &command_result),
                 TURBO_EALREADY);

    issue.request_id = "request-credential-reactivate";
    issue.expected_revision = 5u;
    issue.occurred_at = 1005u;
    rotated = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    check_int_eq(flowie_control_pgsql_command_credential_rotate(commands, &issue, &rotated),
                 TURBO_OK);
    check_uint_eq(rotated.revision, 6u);
    check_int_eq(flowie_control_pgsql_query_credential_verify(
                     query, "root-a", "device-7", rotated.secret, rotated.secret_size, &verified),
                 TURBO_OK);

    disable.root_group_id = "root-a";
    disable.principal_id = "device-7";
    disable.actor = "admin-1";
    disable.request_id = "request-user-disable";
    disable.expected_revision = 6u;
    disable.occurred_at = 1006u;
    check_int_eq(flowie_control_pgsql_command_user_disable(commands, &disable, &command_result),
                 TURBO_OK);
    issue.request_id = "request-disabled-user-rotate";
    issue.expected_revision = 7u;
    issue.occurred_at = 1007u;
    check_int_eq(flowie_control_pgsql_command_credential_rotate(commands, &issue, &generated),
                 TURBO_EPERM);
    check_uint_eq(generated.secret_size, 0u);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 7u);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, 7u);

    flowie_control_generated_credential_wipe(&generated);
    flowie_control_generated_credential_wipe(&rotated);
    flowie_control_credential_wipe(first_secret, sizeof(first_secret));
    flowie_control_credential_wipe(active_secret, sizeof(active_secret));
    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("commits hierarchical groups and direct membership as one revision stream") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    flowie_control_group_disable_command_t disable = FLOWIE_CONTROL_GROUP_DISABLE_COMMAND_INIT;
    flowie_control_membership_add_command_t add = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
    flowie_control_membership_remove_command_t remove =
        FLOWIE_CONTROL_MEMBERSHIP_REMOVE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_principal_snapshot_t principal = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    uint64_t revision = 0u;
    size_t audit_count = 0u;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_group_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &result),
                 TURBO_OK);
    user.root_group_id = "root-a";
    user.principal_id = "device-7";
    user.principal_type = "device";
    user.actor = "admin-1";
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &result), TURBO_OK);

    group.root_group_id = "root-a";
    group.group_id = "engineering";
    group.parent_group_id = "root-a";
    group.actor = "admin-1";
    group.request_id = "request-engineering";
    group.expected_revision = 2u;
    group.occurred_at = 1002u;
    check_int_eq(flowie_control_pgsql_command_group_create(commands, &group, &result), TURBO_OK);
    check_uint_eq(result.revision, 3u);
    check_int_eq(flowie_control_pgsql_command_group_create(commands, &group, &result), TURBO_OK);
    check_true(result.replayed);
    group.group_id = "backend";
    group.parent_group_id = "engineering";
    group.request_id = "request-backend";
    group.expected_revision = 3u;
    group.occurred_at = 1003u;
    check_int_eq(flowie_control_pgsql_command_group_create(commands, &group, &result), TURBO_OK);

    add.root_group_id = "root-a";
    add.principal_id = "device-7";
    add.group_id = "backend";
    add.actor = "admin-1";
    add.request_id = "request-member";
    add.expected_revision = 4u;
    add.occurred_at = 1004u;
    check_int_eq(flowie_control_pgsql_command_membership_add(commands, &add, &result), TURBO_OK);
    check_uint_eq(result.revision, 5u);
    check_int_eq(flowie_control_pgsql_query_external_principal_snapshot(query, "root-a", "device-7",
                                                                        99u, &principal),
                 TURBO_OK);
    check_uint_eq(principal.effective_groups.group_count, 3u);
    check_str_eq(principal.effective_groups.groups[0], "root-a");
    check_str_eq(principal.effective_groups.groups[1], "engineering");
    check_str_eq(principal.effective_groups.groups[2], "backend");

    disable.root_group_id = "root-a";
    disable.group_id = "engineering";
    disable.actor = "admin-1";
    disable.request_id = "request-disable-engineering";
    disable.expected_revision = 5u;
    disable.occurred_at = 1005u;
    check_int_eq(flowie_control_pgsql_command_group_disable(commands, &disable, &result),
                 TURBO_EBUSY);
    disable.group_id = "backend";
    disable.request_id = "request-disable-backend";
    check_int_eq(flowie_control_pgsql_command_group_disable(commands, &disable, &result),
                 TURBO_EBUSY);

    remove.root_group_id = "root-a";
    remove.principal_id = "device-7";
    remove.group_id = "backend";
    remove.actor = "admin-1";
    remove.request_id = "request-member-remove";
    remove.expected_revision = 5u;
    remove.occurred_at = 1006u;
    check_int_eq(flowie_control_pgsql_command_membership_remove(commands, &remove, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 6u);
    check_int_eq(flowie_control_pgsql_command_membership_remove(commands, &remove, &result),
                 TURBO_OK);
    check_true(result.replayed);
    disable.expected_revision = 6u;
    check_int_eq(flowie_control_pgsql_command_group_disable(commands, &disable, &result), TURBO_OK);
    check_uint_eq(result.revision, 7u);
    check_int_eq(flowie_control_pgsql_command_group_disable(commands, &disable, &result), TURBO_OK);
    check_true(result.replayed);
    disable.group_id = "engineering";
    disable.request_id = "request-disable-engineering";
    disable.expected_revision = 7u;
    disable.occurred_at = 1007u;
    check_int_eq(flowie_control_pgsql_command_group_disable(commands, &disable, &result), TURBO_OK);
    check_uint_eq(result.revision, 8u);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 8u);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, 8u);

    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("rejects membership when its effective group closure exceeds the security ABI") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    char group_id[64];
    char request_id[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    flowie_control_membership_add_command_t add = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    uint64_t revision = 0u;
    uint64_t actual_revision = 0u;
    size_t audit_count = 0u;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_group_capacity_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &result),
                 TURBO_OK);
    user.root_group_id = "root-a";
    user.principal_id = "device-7";
    user.principal_type = "device";
    user.actor = "admin-1";
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &result), TURBO_OK);
    revision = 2u;

    group.root_group_id = "root-a";
    group.parent_group_id = "root-a";
    group.actor = "admin-1";
    for (uint32_t index = 0u; index < TURBO_FLOW_SECURITY_MAX_GROUPS; ++index) {
      (void)snprintf(group_id, sizeof(group_id), "branch-%u", index);
      (void)snprintf(request_id, sizeof(request_id), "request-group-%u", index);
      group.group_id = group_id;
      group.request_id = request_id;
      group.expected_revision = revision;
      group.occurred_at = 2000u + revision;
      check_int_eq(flowie_control_pgsql_command_group_create(commands, &group, &result), TURBO_OK);
      ++revision;
    }

    add.root_group_id = "root-a";
    add.principal_id = "device-7";
    add.actor = "admin-1";
    for (uint32_t index = 0u; index + 1u < TURBO_FLOW_SECURITY_MAX_GROUPS; ++index) {
      (void)snprintf(group_id, sizeof(group_id), "branch-%u", index);
      (void)snprintf(request_id, sizeof(request_id), "request-member-%u", index);
      add.group_id = group_id;
      add.request_id = request_id;
      add.expected_revision = revision;
      add.occurred_at = 3000u + revision;
      check_int_eq(flowie_control_pgsql_command_membership_add(commands, &add, &result), TURBO_OK);
      ++revision;
    }
    (void)snprintf(group_id, sizeof(group_id), "branch-%u", TURBO_FLOW_SECURITY_MAX_GROUPS - 1u);
    add.group_id = group_id;
    add.request_id = "request-member-overflow";
    add.expected_revision = revision;
    add.occurred_at = 4000u + revision;
    check_int_eq(flowie_control_pgsql_command_membership_add(commands, &add, &result),
                 TURBO_ENOSPC);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &actual_revision), TURBO_OK);
    check_uint_eq(actual_revision, revision);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, revision);

    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("commits role assignment, tombstone, replay, and ACL reference semantics") {
    static const char role_rule[] =
        "allow|role|reader|root-a|publish|mqtt_topic|adapter|root-a/events/#";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    char policy_sql[1024];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_pool_lease_t seed = FLOWIE_CONTROL_PGSQL_POOL_LEASE_INIT;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    flowie_control_role_disable_command_t disable = FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT;
    flowie_control_user_role_add_command_t add = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
    flowie_control_user_role_remove_command_t remove = FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_principal_snapshot_t principal = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    flowie_control_user_view_t users[1] = {FLOWIE_CONTROL_USER_VIEW_INIT};
    flowie_control_group_view_t groups[1] = {FLOWIE_CONTROL_GROUP_VIEW_INIT};
    flowie_control_role_view_t roles[1] = {FLOWIE_CONTROL_ROLE_VIEW_INIT};
    flowie_control_effective_groups_view_t effective_groups =
        FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
    flowie_control_effective_roles_view_t effective_roles =
        FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
    PGresult *seed_result = NULL;
    uint64_t revision = 0u;
    size_t audit_count = 0u;
    size_t page_count = 0u;
    int has_more = 0;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_role_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &result),
                 TURBO_OK);
    user.root_group_id = "root-a";
    user.principal_id = "device-7";
    user.principal_type = "device";
    user.actor = "admin-1";
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &result), TURBO_OK);

    role.root_group_id = "root-a";
    role.role_id = "writer";
    role.actor = "admin-1";
    role.request_id = "request-role-writer";
    role.expected_revision = 2u;
    role.occurred_at = 1002u;
    check_int_eq(flowie_control_pgsql_command_role_create(commands, &role, &result), TURBO_OK);
    check_uint_eq(result.revision, 3u);
    check_int_eq(flowie_control_pgsql_command_role_create(commands, &role, &result), TURBO_OK);
    check_true(result.replayed);
    role.role_id = "reader";
    role.request_id = "request-role-reader";
    role.expected_revision = 3u;
    role.occurred_at = 1003u;
    check_int_eq(flowie_control_pgsql_command_role_create(commands, &role, &result), TURBO_OK);

    add.root_group_id = "root-a";
    add.principal_id = "device-7";
    add.role_id = "writer";
    add.actor = "admin-1";
    add.request_id = "request-user-role-writer";
    add.expected_revision = 4u;
    add.occurred_at = 1004u;
    check_int_eq(flowie_control_pgsql_command_user_role_add(commands, &add, &result), TURBO_OK);
    add.role_id = "reader";
    add.request_id = "request-user-role-reader";
    add.expected_revision = 5u;
    add.occurred_at = 1005u;
    check_int_eq(flowie_control_pgsql_command_user_role_add(commands, &add, &result), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_external_principal_snapshot(query, "root-a", "device-7",
                                                                        99u, &principal),
                 TURBO_OK);
    check_uint_eq(principal.effective_roles.role_count, 2u);
    check_str_eq(principal.effective_roles.roles[0], "reader");
    check_str_eq(principal.effective_roles.roles[1], "writer");
    check_int_eq(flowie_control_pgsql_query_user_get(query, "root-a", "device-7", &users[0]),
                 TURBO_OK);
    check_str_eq(users[0].principal_type, "device");
    users[0] = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
    check_int_eq(flowie_control_pgsql_query_user_list(query, "root-a", NULL, users, 1u, &page_count,
                                                      &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_false(has_more);
    check_str_eq(users[0].principal_id, "device-7");
    check_int_eq(flowie_control_pgsql_query_group_list(query, "root-a", NULL, groups, 1u,
                                                       &page_count, &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_false(has_more);
    check_str_eq(groups[0].group_id, "root-a");
    check_uint_eq(groups[0].depth, 0u);
    check_int_eq(
        flowie_control_pgsql_query_effective_groups(query, "root-a", "device-7", &effective_groups),
        TURBO_OK);
    check_uint_eq(effective_groups.group_count, 1u);
    check_str_eq(effective_groups.groups[0], "root-a");
    check_int_eq(
        flowie_control_pgsql_query_effective_roles(query, "root-a", "device-7", &effective_roles),
        TURBO_OK);
    check_uint_eq(effective_roles.role_count, 2u);
    check_str_eq(effective_roles.roles[0], "reader");
    check_str_eq(effective_roles.roles[1], "writer");
    check_int_eq(flowie_control_pgsql_query_role_list(query, "root-a", NULL, roles, 1u, &page_count,
                                                      &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_true(has_more);
    check_str_eq(roles[0].role_id, "reader");
    roles[0] = (flowie_control_role_view_t)FLOWIE_CONTROL_ROLE_VIEW_INIT;
    check_int_eq(flowie_control_pgsql_query_role_list(query, "root-a", "reader", roles, 1u,
                                                      &page_count, &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_false(has_more);
    check_str_eq(roles[0].role_id, "writer");
    add.expected_revision = 0u;
    check_int_eq(flowie_control_pgsql_command_user_role_add(commands, &add, &result), TURBO_OK);
    check_true(result.replayed);
    check_uint_eq(result.revision, 6u);

    disable.root_group_id = "root-a";
    disable.role_id = "writer";
    disable.actor = "admin-1";
    disable.request_id = "request-disable-writer";
    disable.expected_revision = 6u;
    disable.occurred_at = 1006u;
    check_int_eq(flowie_control_pgsql_command_role_disable(commands, &disable, &result), TURBO_OK);
    check_uint_eq(result.revision, 7u);
    principal = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    check_int_eq(flowie_control_pgsql_query_external_principal_snapshot(query, "root-a", "device-7",
                                                                        100u, &principal),
                 TURBO_OK);
    check_uint_eq(principal.effective_roles.role_count, 1u);
    check_str_eq(principal.effective_roles.roles[0], "reader");
    disable.expected_revision = 0u;
    check_int_eq(flowie_control_pgsql_command_role_disable(commands, &disable, &result), TURBO_OK);
    check_true(result.replayed);
    add.request_id = "request-add-disabled-writer";
    add.role_id = "writer";
    add.expected_revision = 7u;
    add.occurred_at = 1007u;
    check_int_eq(flowie_control_pgsql_command_user_role_add(commands, &add, &result), TURBO_EPERM);

    remove.root_group_id = "root-a";
    remove.principal_id = "device-7";
    remove.role_id = "writer";
    remove.actor = "admin-1";
    remove.request_id = "request-remove-writer";
    remove.expected_revision = 7u;
    remove.occurred_at = 1008u;
    check_int_eq(flowie_control_pgsql_command_user_role_remove(commands, &remove, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 8u);
    remove.expected_revision = 0u;
    check_int_eq(flowie_control_pgsql_command_user_role_remove(commands, &remove, &result),
                 TURBO_OK);
    check_true(result.replayed);

    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &seed), TURBO_OK);
    check_true(snprintf(policy_sql, sizeof(policy_sql),
                        "INSERT INTO %s.policy_draft(root_group_id,ordinal,rule_line,revision,"
                        "updated_at) VALUES('root-a',10,'%s',8,1010)",
                        schema_name, role_rule) > 0);
    seed_result = PQexec(flowie_control_pgsql_pool_lease_connection(&seed), policy_sql);
    check_not_null(seed_result);
    check_int_eq(PQresultStatus(seed_result), PGRES_COMMAND_OK);
    PQclear(seed_result);
    seed_result = NULL;
    check_int_eq(flowie_control_pgsql_pool_release(&seed), TURBO_OK);

    disable.role_id = "reader";
    disable.request_id = "request-disable-reader-referenced";
    disable.expected_revision = 8u;
    disable.occurred_at = 1011u;
    check_int_eq(flowie_control_pgsql_command_role_disable(commands, &disable, &result),
                 TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_pool_acquire(pool, &seed), TURBO_OK);
    check_true(snprintf(policy_sql, sizeof(policy_sql),
                        "DELETE FROM %s.policy_draft WHERE root_group_id='root-a' AND ordinal=10",
                        schema_name) > 0);
    seed_result = PQexec(flowie_control_pgsql_pool_lease_connection(&seed), policy_sql);
    check_not_null(seed_result);
    check_int_eq(PQresultStatus(seed_result), PGRES_COMMAND_OK);
    PQclear(seed_result);
    seed_result = NULL;
    check_int_eq(flowie_control_pgsql_pool_release(&seed), TURBO_OK);
    disable.request_id = "request-disable-reader";
    check_int_eq(flowie_control_pgsql_command_role_disable(commands, &disable, &result), TURBO_OK);
    check_uint_eq(result.revision, 9u);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 9u);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, 9u);

    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("rolls back a role assignment beyond the security ABI capacity") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    char role_id[64];
    char request_id[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    flowie_control_user_role_add_command_t add = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_principal_snapshot_t principal = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
    uint64_t revision = 0u;
    uint64_t actual_revision = 0u;
    size_t audit_count = 0u;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_role_capacity_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &result),
                 TURBO_OK);
    user.root_group_id = "root-a";
    user.principal_id = "device-7";
    user.principal_type = "device";
    user.actor = "admin-1";
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_user_create(commands, &user, &result), TURBO_OK);
    revision = 2u;

    role.root_group_id = "root-a";
    role.actor = "admin-1";
    for (uint32_t index = 0u; index <= TURBO_FLOW_SECURITY_MAX_ROLES; ++index) {
      (void)snprintf(role_id, sizeof(role_id), "role-%u", index);
      (void)snprintf(request_id, sizeof(request_id), "request-role-%u", index);
      role.role_id = role_id;
      role.request_id = request_id;
      role.expected_revision = revision;
      role.occurred_at = 2000u + revision;
      check_int_eq(flowie_control_pgsql_command_role_create(commands, &role, &result), TURBO_OK);
      ++revision;
    }

    add.root_group_id = "root-a";
    add.principal_id = "device-7";
    add.actor = "admin-1";
    for (uint32_t index = 0u; index < TURBO_FLOW_SECURITY_MAX_ROLES; ++index) {
      (void)snprintf(role_id, sizeof(role_id), "role-%u", index);
      (void)snprintf(request_id, sizeof(request_id), "request-user-role-%u", index);
      add.role_id = role_id;
      add.request_id = request_id;
      add.expected_revision = revision;
      add.occurred_at = 3000u + revision;
      check_int_eq(flowie_control_pgsql_command_user_role_add(commands, &add, &result), TURBO_OK);
      ++revision;
    }
    (void)snprintf(role_id, sizeof(role_id), "role-%u", TURBO_FLOW_SECURITY_MAX_ROLES);
    add.role_id = role_id;
    add.request_id = "request-user-role-overflow";
    add.expected_revision = revision;
    add.occurred_at = 4000u + revision;
    check_int_eq(flowie_control_pgsql_command_user_role_add(commands, &add, &result), TURBO_ENOSPC);
    check_int_eq(flowie_control_pgsql_query_external_principal_snapshot(query, "root-a", "device-7",
                                                                        99u, &principal),
                 TURBO_OK);
    check_uint_eq(principal.effective_roles.role_count, TURBO_FLOW_SECURITY_MAX_ROLES);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &actual_revision), TURBO_OK);
    check_uint_eq(actual_revision, revision);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, revision);

    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("publishes validated ACL drafts as atomic versioned bundles") {
    static const char role_rule[] =
        "allow|role|reader|root-a|publish|mqtt_topic|adapter|root-a/events/#";
    static const char deny_rule[] = "deny|any|*|root-a|publish|mqtt_topic|adapter|root-a/private/#";
    static const char replacement_rule[] =
        "allow|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/public/#";
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;
    flowie_control_pgsql_command_t *commands = NULL;
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
    flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    flowie_control_role_disable_command_t disable = FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT;
    flowie_control_policy_rule_put_command_t put = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
    flowie_control_policy_rule_delete_command_t remove =
        FLOWIE_CONTROL_POLICY_RULE_DELETE_COMMAND_INIT;
    flowie_control_policy_publish_command_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_policy_publish_result_t published = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
    flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
    flowie_control_policy_rule_view_t rules[1] = {FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT};
    flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
    turbo_flow_security_policy_bundle_t first = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    turbo_flow_security_policy_bundle_t second = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
    uint64_t revision = 0u;
    size_t audit_count = 0u;
    size_t page_count = 0u;
    int has_more = 0;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_policy_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_OK);
    check_int_eq(flowie_control_pgsql_command_create(pool, &commands), TURBO_OK);
    check_int_eq(flowie_control_pgsql_query_create(pool, &query), TURBO_OK);

    root.root_group_id = "root-a";
    root.actor = "admin-1";
    root.request_id = "request-root";
    root.expected_revision = 0u;
    root.occurred_at = 1000u;
    check_int_eq(flowie_control_pgsql_command_root_group_create(commands, &root, &result),
                 TURBO_OK);
    role.root_group_id = "root-a";
    role.role_id = "reader";
    role.actor = "admin-1";
    role.request_id = "request-role";
    role.expected_revision = 1u;
    role.occurred_at = 1001u;
    check_int_eq(flowie_control_pgsql_command_role_create(commands, &role, &result), TURBO_OK);

    put.root_group_id = "root-a";
    put.ordinal = 10u;
    put.rule_line = role_rule;
    put.actor = "admin-1";
    put.request_id = "request-put-role";
    put.expected_revision = 2u;
    put.occurred_at = 1002u;
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(commands, &put, &result), TURBO_OK);
    check_uint_eq(result.revision, 3u);
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(commands, &put, &result), TURBO_OK);
    check_true(result.replayed);
    put.ordinal = 20u;
    put.rule_line = deny_rule;
    put.request_id = "request-put-deny";
    put.expected_revision = 3u;
    put.occurred_at = 1003u;
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(commands, &put, &result), TURBO_OK);
    put.ordinal = 30u;
    put.rule_line = "allow|role|missing|root-a|connect|generic|exact|client";
    put.request_id = "request-put-missing-role";
    put.expected_revision = 4u;
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(commands, &put, &result),
                 TURBO_ENOENT);
    put.rule_line = "allow|any|*|root-a|subscribe|mqtt_topic|adapter|root-a/#/tail";
    put.request_id = "request-put-bad-filter";
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(commands, &put, &result),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_pgsql_query_policy_validate(query, "root-a", &validation),
                 TURBO_OK);
    check_uint_eq(validation.store_revision, 4u);
    check_uint_eq(validation.rule_count, 2u);
    check_uint_eq(validation.deny_rule_count, 1u);
    check_int_eq(flowie_control_pgsql_query_policy_rule_list(query, "root-a", 0u, 0, rules, 1u,
                                                             &page_count, &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_true(has_more);
    check_uint_eq(rules[0].ordinal, 10u);
    check_str_eq(rules[0].rule_line, role_rule);
    rules[0] = (flowie_control_policy_rule_view_t)FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
    check_int_eq(flowie_control_pgsql_query_policy_rule_list(query, "root-a", 10u, 1, rules, 1u,
                                                             &page_count, &has_more),
                 TURBO_OK);
    check_uint_eq(page_count, 1u);
    check_false(has_more);
    check_uint_eq(rules[0].ordinal, 20u);
    check_int_eq(flowie_control_pgsql_query_policy_status(query, "root-a", &status), TURBO_OK);
    check_uint_eq(status.store_revision, 4u);
    check_uint_eq(status.policy_version, 0u);
    check_uint_eq(status.draft_rule_count, 2u);
    check_uint_eq(status.published_rule_count, 0u);

    publish.root_group_id = "root-a";
    publish.actor = "admin-1";
    publish.request_id = "request-publish-first";
    publish.expected_revision = 4u;
    publish.occurred_at = 2000u;
    publish.expires_at = 20000u;
    check_int_eq(flowie_control_pgsql_command_policy_publish(commands, &publish, &published),
                 TURBO_OK);
    check_uint_eq(published.revision, 5u);
    check_uint_eq(published.policy_version, 1u);
    check_false(published.replayed);
    check_int_eq(flowie_control_pgsql_query_policy_status(query, "root-a", &status), TURBO_OK);
    check_uint_eq(status.store_revision, 5u);
    check_uint_eq(status.policy_version, 1u);
    check_uint_eq(status.expires_at, 20000u);
    check_uint_eq(status.draft_rule_count, 2u);
    check_uint_eq(status.published_rule_count, 2u);
    publish.expected_revision = 0u;
    check_int_eq(flowie_control_pgsql_command_policy_publish(commands, &publish, &published),
                 TURBO_OK);
    check_true(published.replayed);
    check_uint_eq(published.revision, 5u);
    check_uint_eq(published.policy_version, 1u);
    publish.expires_at = 21000u;
    check_int_eq(flowie_control_pgsql_command_policy_publish(commands, &publish, &published),
                 TURBO_EBUSY);
    publish.expires_at = 20000u;
    check_int_eq(flowie_control_pgsql_query_policy_bundle_load(query, "root-a", 1u, &first),
                 TURBO_OK);
    check_uint_eq(first.rule_count, 2u);
    check_str_eq(first.rules[0].subject, "reader");
    check_str_eq(first.rules[1].pattern, "root-a/private/#");

    remove.root_group_id = "root-a";
    remove.ordinal = 10u;
    remove.actor = "admin-1";
    remove.request_id = "request-delete-role-rule";
    remove.expected_revision = 5u;
    remove.occurred_at = 2001u;
    check_int_eq(flowie_control_pgsql_command_policy_rule_delete(commands, &remove, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 6u);
    remove.expected_revision = 0u;
    check_int_eq(flowie_control_pgsql_command_policy_rule_delete(commands, &remove, &result),
                 TURBO_OK);
    check_true(result.replayed);

    disable.root_group_id = "root-a";
    disable.role_id = "reader";
    disable.actor = "admin-1";
    disable.request_id = "request-disable-published-role";
    disable.expected_revision = 6u;
    disable.occurred_at = 2002u;
    check_int_eq(flowie_control_pgsql_command_role_disable(commands, &disable, &result),
                 TURBO_EBUSY);
    put.ordinal = 10u;
    put.rule_line = replacement_rule;
    put.request_id = "request-put-replacement";
    put.expected_revision = 6u;
    put.occurred_at = 2003u;
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(commands, &put, &result), TURBO_OK);
    publish.request_id = "request-publish-second";
    publish.expected_revision = 7u;
    publish.occurred_at = 2004u;
    publish.expires_at = 22000u;
    check_int_eq(flowie_control_pgsql_command_policy_publish(commands, &publish, &published),
                 TURBO_OK);
    check_uint_eq(published.revision, 8u);
    check_uint_eq(published.policy_version, 2u);
    check_int_eq(flowie_control_pgsql_query_policy_bundle_load(query, "root-a", 2u, &second),
                 TURBO_OK);
    check_uint_eq(second.rule_count, 2u);
    check_str_eq(second.rules[0].pattern, "root-a/public/#");
    check_str_eq(first.rules[0].subject, "reader");
    check_str_eq(first.rules[1].pattern, "root-a/private/#");

    disable.request_id = "request-disable-role";
    disable.expected_revision = 8u;
    disable.occurred_at = 2005u;
    check_int_eq(flowie_control_pgsql_command_role_disable(commands, &disable, &result), TURBO_OK);
    check_uint_eq(result.revision, 9u);
    check_int_eq(flowie_control_pgsql_query_current_revision(query, &revision), TURBO_OK);
    check_uint_eq(revision, 9u);
    check_int_eq(flowie_control_pgsql_query_audit_count(query, &audit_count), TURBO_OK);
    check_uint_eq(audit_count, 9u);

    flowie_control_pgsql_query_policy_bundle_release(&first);
    flowie_control_pgsql_query_policy_bundle_release(&second);
    flowie_control_pgsql_query_destroy(query);
    flowie_control_pgsql_command_destroy(commands);
    check_int_eq(flowie_control_pgsql_pool_close(pool, 100), TURBO_OK);
    check_int_eq(flowie_control_pgsql_pool_destroy(pool), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("binds all PostgreSQL operations through the repository contract") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_repository_provider_t *provider = NULL;
    const flowie_control_repository_t *repository;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_repository_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_repository_create(&config, &provider), TURBO_OK);
    repository = flowie_control_pgsql_repository_view(provider);
    check_not_null(repository);
    flowie_control_repository_basic_contract_run(repository);

    check_int_eq(flowie_control_pgsql_repository_destroy(provider, 100), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("serves local Auth and ACL generation through the PostgreSQL Repository") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_repository_provider_t *provider = NULL;
    const flowie_control_repository_t *repository;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_auth_repository_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_repository_create(&config, &provider), TURBO_OK);
    repository = flowie_control_pgsql_repository_view(provider);
    check_not_null(repository);
    flowie_control_auth_repository_contract_run(repository);
    check_int_eq(flowie_control_pgsql_repository_destroy(provider, 100), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }

  it("serves account and ACL management through the PostgreSQL Repository") {
    const char *conninfo = getenv("TURBO_FLOW_PGSQL_TEST_CONNINFO");
    char schema_name[64];
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_repository_provider_t *provider = NULL;
    const flowie_control_repository_t *repository;

    check_not_null(conninfo);
    check_true(conninfo[0] != '\0');
    (void)snprintf(schema_name, sizeof(schema_name), "flowie_control_management_%llu",
                   (unsigned long long)turbo_hrtime());
    config.database.conninfo = conninfo;
    config.database.schema_name = schema_name;
    config.database.require_tls = 0;
    config.database.schema_mode = FLOWIE_CONTROL_PGSQL_SCHEMA_MIGRATE;
    config.capacity = 2u;
    check_int_eq(flowie_control_pgsql_repository_create(&config, &provider), TURBO_OK);
    repository = flowie_control_pgsql_repository_view(provider);
    check_not_null(repository);
    flowie_control_management_repository_contract_run(repository);
    check_int_eq(flowie_control_pgsql_repository_destroy(provider, 100), TURBO_OK);
    drop_test_schema(conninfo, schema_name);
  }
}
