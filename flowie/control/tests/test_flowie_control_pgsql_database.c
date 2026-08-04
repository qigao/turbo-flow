#include "flowie_control_pgsql_command_internal.h"
#include "flowie_control_pgsql_database_internal.h"
#include "flowie_control_pgsql_query_internal.h"
#include "flowie_control_pgsql_repository_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

spec("Flowie control PostgreSQL database") {
  it("rejects unsafe or unbounded startup configuration before connecting") {
    flowie_control_pgsql_database_config_t config = FLOWIE_CONTROL_PGSQL_DATABASE_CONFIG_INIT;
    flowie_control_pgsql_database_t *database = NULL;

    check_int_eq(flowie_control_pgsql_database_open(&config, &database), TURBO_EINVAL);
    check_null(database);

    config.conninfo = "host=127.0.0.1 port=1 connect_timeout=1";
    config.schema_name = "unsafe-name";
    check_int_eq(flowie_control_pgsql_database_open(&config, &database), TURBO_EINVAL);
    check_null(database);

    config.schema_name = "flowie_control";
    config.statement_timeout_ms = 0;
    check_int_eq(flowie_control_pgsql_database_open(&config, &database), TURBO_EINVAL);
    check_null(database);
  }

  it("maps retryable, integrity, and connection SQLSTATE classes") {
    check_int_eq(flowie_control_pgsql_sqlstate_status("40001"), TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_sqlstate_status("40P01"), TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_sqlstate_status("55P03"), TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_sqlstate_status("57014"), TURBO_ETIMEDOUT);
    check_int_eq(flowie_control_pgsql_sqlstate_status("23503"), TURBO_EBUSY);
    check_int_eq(flowie_control_pgsql_sqlstate_status("23505"), TURBO_EALREADY);
    check_int_eq(flowie_control_pgsql_sqlstate_status("08006"), TURBO_EIO);
    check_int_eq(flowie_control_pgsql_sqlstate_status(NULL), TURBO_EIO);
  }

  it("accepts only non-secret verify-full public conninfo") {
    check_int_eq(flowie_control_pgsql_public_conninfo_validate(
                     "host=db.internal dbname=flowie user=flowie sslmode=verify-full"),
                 TURBO_OK);
    check_int_eq(flowie_control_pgsql_public_conninfo_validate(
                     "host=db.internal dbname=flowie user=flowie password=secret "
                     "sslmode=verify-full"),
                 TURBO_EPERM);
    check_int_eq(flowie_control_pgsql_public_conninfo_validate(
                     "postgresql://flowie:secret@db.internal/flowie?sslmode=verify-full"),
                 TURBO_EPERM);
    check_int_eq(
        flowie_control_pgsql_public_conninfo_validate("service=production sslmode=verify-full"),
        TURBO_EPERM);
    check_int_eq(
        flowie_control_pgsql_public_conninfo_validate("host=db.internal dbname=flowie user=flowie"),
        TURBO_EPERM);
    check_int_eq(flowie_control_pgsql_public_conninfo_validate("not a conninfo"), TURBO_EINVAL);
  }

  it("rejects invalid bounded pool configuration before connecting") {
    flowie_control_pgsql_pool_config_t config = FLOWIE_CONTROL_PGSQL_POOL_CONFIG_INIT;
    flowie_control_pgsql_pool_t *pool = NULL;

    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_EINVAL);
    check_null(pool);

    config.database.conninfo = "host=127.0.0.1 port=1 connect_timeout=1";
    config.capacity = 0u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_EINVAL);
    check_null(pool);

    config.capacity = FLOWIE_CONTROL_PGSQL_POOL_CAPACITY_MAX + 1u;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_EINVAL);
    check_null(pool);

    config.capacity = 1u;
    config.acquire_timeout_ms = 0;
    check_int_eq(flowie_control_pgsql_pool_create(&config, &pool), TURBO_EINVAL);
    check_null(pool);
  }

  it("rejects a query view without its borrowed pool") {
    flowie_control_pgsql_query_t *query = NULL;
    flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
    flowie_control_group_view_t group = FLOWIE_CONTROL_GROUP_VIEW_INIT;
    flowie_control_role_view_t role = FLOWIE_CONTROL_ROLE_VIEW_INIT;
    flowie_control_effective_groups_view_t groups = FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
    flowie_control_effective_roles_view_t roles = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
    flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
    flowie_control_policy_rule_view_t rule = FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT;
    flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
    size_t count = 0u;
    int has_more = 0;

    check_int_eq(flowie_control_pgsql_query_create(NULL, &query), TURBO_EINVAL);
    check_null(query);
    check_int_eq(flowie_control_pgsql_query_user_get(NULL, "root-a", "device-7", &user),
                 TURBO_EINVAL);
    check_int_eq(
        flowie_control_pgsql_query_user_list(NULL, "root-a", NULL, &user, 1u, &count, &has_more),
        TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_query_effective_groups(NULL, "root-a", "device-7", &groups),
                 TURBO_EINVAL);
    check_int_eq(
        flowie_control_pgsql_query_group_list(NULL, "root-a", NULL, &group, 1u, &count, &has_more),
        TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_query_effective_roles(NULL, "root-a", "device-7", &roles),
                 TURBO_EINVAL);
    check_int_eq(
        flowie_control_pgsql_query_role_list(NULL, "root-a", NULL, &role, 1u, &count, &has_more),
        TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_query_policy_validate(NULL, "root-a", &validation),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_query_policy_rule_list(NULL, "root-a", 0u, 0, &rule, 1u,
                                                             &count, &has_more),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_query_policy_status(NULL, "root-a", &status), TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_repository_create(NULL, NULL), TURBO_EINVAL);
    check_null(flowie_control_pgsql_repository_view(NULL));
    check_int_eq(flowie_control_pgsql_repository_destroy(NULL, 0), TURBO_OK);
  }

  it("rejects command creation and credential operations without their borrowed view") {
    flowie_control_pgsql_command_t *view = NULL;
    flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
    flowie_control_credential_revoke_command_t revoke =
        FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
    flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    flowie_control_group_delete_command_t group_delete = FLOWIE_CONTROL_GROUP_DELETE_COMMAND_INIT;
    flowie_control_membership_add_command_t membership_add =
        FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
    flowie_control_membership_remove_command_t membership_remove =
        FLOWIE_CONTROL_MEMBERSHIP_REMOVE_COMMAND_INIT;
    flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    flowie_control_role_disable_command_t role_disable = FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT;
    flowie_control_user_role_add_command_t user_role_add =
        FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
    flowie_control_user_role_remove_command_t user_role_remove =
        FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT;
    flowie_control_policy_rule_put_command_t policy_put =
        FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
    flowie_control_policy_rule_delete_command_t policy_delete =
        FLOWIE_CONTROL_POLICY_RULE_DELETE_COMMAND_INIT;
    flowie_control_policy_publish_command_t policy_publish =
        FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
    flowie_control_policy_publish_result_t published = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

    check_int_eq(flowie_control_pgsql_command_create(NULL, &view), TURBO_EINVAL);
    check_null(view);
    check_int_eq(flowie_control_pgsql_command_credential_generate(NULL, &issue, &generated),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_credential_rotate(NULL, &issue, &generated),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_credential_revoke(NULL, &revoke, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_group_create(NULL, &group, &result), TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_group_delete(NULL, &group_delete, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_membership_add(NULL, &membership_add, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_membership_remove(NULL, &membership_remove, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_role_create(NULL, &role, &result), TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_role_disable(NULL, &role_disable, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_user_role_add(NULL, &user_role_add, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_user_role_remove(NULL, &user_role_remove, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_policy_rule_put(NULL, &policy_put, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_policy_rule_delete(NULL, &policy_delete, &result),
                 TURBO_EINVAL);
    check_int_eq(flowie_control_pgsql_command_policy_publish(NULL, &policy_publish, &published),
                 TURBO_EINVAL);
  }
}
