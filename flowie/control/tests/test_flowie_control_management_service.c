#include "flowie_control_management_repository_contract.h"
#include "flowie_control_management_service_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

static flowie_control_management_service_t *management_open(char **path_out,
                                                            flowie_control_store_t **store_out) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_management_service_config_t service_config =
      FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_management_service_t *service = NULL;

  *path_out = tt_make_temp_file("flowie-management", ".sqlite3");
  check_not_null(*path_out);
  store_config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&store_config, store_out), TURBO_OK);
  root.domain_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "request-root";
  root.expected_revision = 0u;
  root.occurred_at = 1000u;
  check_int_eq(flowie_control_store_domain_create(*store_out, &root, &result), TURBO_OK);
  service_config.repository = flowie_control_store_repository(*store_out);
  check_int_eq(flowie_control_management_service_create(&service_config, &service), TURBO_OK);
  return service;
}

static void management_close(flowie_control_management_service_t *service,
                             flowie_control_store_t *store, char *path) {
  flowie_control_management_service_destroy(service);
  flowie_control_store_destroy(store);
  check_int_eq(tt_remove_file(path), 0);
  free(path);
}

spec("Flowie ACL management service") {
  it("uses the provider-neutral Repository for account and ACL management") {
    flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
    flowie_control_store_t *store = NULL;
    char *path = tt_make_temp_file("flowie-management-contract", ".sqlite3");

    check_not_null(path);
    store_config.database_path = path;
    check_int_eq(flowie_control_store_open(&store_config, &store), TURBO_OK);
    flowie_control_management_repository_contract_run(flowie_control_store_repository(store));
    flowie_control_store_destroy(store);
    check_int_eq(tt_remove_file(path), 0);
    free(path);
  }

  it("enforces method permissions and root-bound command identity") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = management_open(&path, &store);
    flowie_control_management_caller_t viewer = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_caller_t user_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_caller_t security_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_status_t status = FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

    viewer.domain_id = "root-a";
    viewer.actor = "viewer-1";
    viewer.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    user_admin.domain_id = "root-a";
    user_admin.actor = "user-admin-1";
    user_admin.permissions =
        FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    security_admin.domain_id = "root-a";
    security_admin.actor = "security-admin-1";
    security_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

    check_int_eq(flowie_control_management_system_status(service, &viewer, &status), TURBO_OK);
    check_uint_eq(status.store_revision, 1u);

    user.domain_id = "root-a";
    user.principal_id = "device-1";
    user.principal_type = "device";
    user.actor = "user-admin-1";
    user.request_id = "request-user-1";
    user.expected_revision = 1u;
    user.occurred_at = 2000u;
    check_int_eq(flowie_control_management_user_create(service, &viewer, &user, &result),
                 TURBO_EPERM);
    check_int_eq(flowie_control_management_user_create(service, &user_admin, &user, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 2u);

    user.principal_id = "device-2";
    user.domain_id = "root-b";
    user.request_id = "request-cross-root";
    user.expected_revision = 2u;
    check_int_eq(flowie_control_management_user_create(service, &user_admin, &user, &result),
                 TURBO_EPERM);

    role.domain_id = "root-a";
    role.role_id = "operator";
    role.actor = "user-admin-1";
    role.request_id = "request-role-user-admin";
    role.expected_revision = 2u;
    role.occurred_at = 3000u;
    check_int_eq(flowie_control_management_role_create(service, &user_admin, &role, &result),
                 TURBO_EPERM);
    role.actor = "security-admin-1";
    role.request_id = "request-role-security-admin";
    check_int_eq(flowie_control_management_role_create(service, &security_admin, &role, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 3u);

    management_close(service, store, path);
  }

  it("returns bounded root-scoped keyset pages and security audit records") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = management_open(&path, &store);
    flowie_control_management_caller_t admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_user_view_t users[1] = {FLOWIE_CONTROL_USER_VIEW_INIT};
    flowie_control_audit_view_t audits[2] = {FLOWIE_CONTROL_AUDIT_VIEW_INIT,
                                             FLOWIE_CONTROL_AUDIT_VIEW_INIT};
    size_t count = 0u;
    int has_more = 0;

    admin.domain_id = "root-a";
    admin.actor = "security-admin-1";
    admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    user.domain_id = "root-a";
    user.principal_type = "device";
    user.actor = admin.actor;
    user.expected_revision = 1u;
    user.occurred_at = 2000u;
    user.principal_id = "device-a";
    user.request_id = "request-user-a";
    check_int_eq(flowie_control_management_user_create(service, &admin, &user, &result), TURBO_OK);
    user.expected_revision = 2u;
    user.occurred_at = 2001u;
    user.principal_id = "device-b";
    user.request_id = "request-user-b";
    check_int_eq(flowie_control_management_user_create(service, &admin, &user, &result), TURBO_OK);

    check_int_eq(
        flowie_control_management_user_list(service, &admin, NULL, users, 1u, &count, &has_more),
        TURBO_OK);
    check_size_eq(count, 1u);
    check_true(has_more);
    check_str_eq(users[0].principal_id, "device-a");
    users[0] = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
    check_int_eq(flowie_control_management_user_list(service, &admin, "device-a", users, 1u, &count,
                                                     &has_more),
                 TURBO_OK);
    check_size_eq(count, 1u);
    check_false(has_more);
    check_str_eq(users[0].principal_id, "device-b");

    check_int_eq(
        flowie_control_management_audit_list(service, &admin, 0u, audits, 2u, &count, &has_more),
        TURBO_OK);
    check_size_eq(count, 2u);
    check_true(has_more);
    check_uint_eq(audits[0].revision, 1u);
    check_uint_eq(audits[1].revision, 2u);
    check_str_eq(audits[1].operation, "user.create");

    management_close(service, store, path);
  }

  it("restricts credential lifecycle commands to root-bound security administrators") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = management_open(&path, &store);
    flowie_control_management_caller_t user_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_caller_t security_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
    flowie_control_credential_revoke_command_t revoke =
        FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

    user_admin.domain_id = "root-a";
    user_admin.actor = "user-admin-1";
    user_admin.permissions =
        FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    security_admin.domain_id = "root-a";
    security_admin.actor = "security-admin-1";
    security_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

    user.domain_id = security_admin.domain_id;
    user.principal_id = "device-1";
    user.principal_type = "device";
    user.actor = security_admin.actor;
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 2000u;
    check_int_eq(flowie_control_management_user_create(service, &security_admin, &user, &result),
                 TURBO_OK);

    issue.domain_id = user_admin.domain_id;
    issue.principal_id = user.principal_id;
    issue.actor = user_admin.actor;
    issue.request_id = "request-credential-denied";
    issue.expected_revision = 2u;
    issue.occurred_at = 3000u;
    check_int_eq(
        flowie_control_management_credential_generate(service, &user_admin, &issue, &generated),
        TURBO_EPERM);

    issue.actor = security_admin.actor;
    issue.request_id = "request-credential-generate";
    check_int_eq(
        flowie_control_management_credential_generate(service, &security_admin, &issue, &generated),
        TURBO_OK);
    check_uint_eq(generated.revision, 3u);
    check_size_eq(generated.token_size, FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE);
    check_str_starts_with(generated.token, FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX);
    flowie_control_generated_credential_wipe(&generated);

    issue.domain_id = "root-b";
    issue.request_id = "request-credential-cross-root";
    issue.expected_revision = 3u;
    check_int_eq(
        flowie_control_management_credential_rotate(service, &security_admin, &issue, &generated),
        TURBO_EPERM);
    issue.domain_id = security_admin.domain_id;
    issue.request_id = "request-credential-rotate";
    check_int_eq(
        flowie_control_management_credential_rotate(service, &security_admin, &issue, &generated),
        TURBO_OK);
    check_uint_eq(generated.revision, 4u);
    check_size_eq(generated.token_size, FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE);
    check_str_starts_with(generated.token, FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX);
    flowie_control_generated_credential_wipe(&generated);

    revoke.domain_id = security_admin.domain_id;
    revoke.principal_id = user.principal_id;
    revoke.actor = security_admin.actor;
    revoke.request_id = "request-credential-revoke";
    revoke.expected_revision = 4u;
    revoke.occurred_at = 4000u;
    check_int_eq(
        flowie_control_management_credential_revoke(service, &security_admin, &revoke, &result),
        TURBO_OK);
    check_uint_eq(result.revision, 5u);

    flowie_control_generated_credential_wipe(&generated);
    management_close(service, store, path);
  }

  it("lets only the system administrator set human passwords across domains") {
    static const char initial_password[] = "Root-B-Initial-Password-2026";
    static const char replacement_password[] = "Root-B-Replaced-Password-2026";
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = management_open(&path, &store);
    flowie_control_management_caller_t system_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_caller_t root_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_password_set_command_t password = FLOWIE_CONTROL_PASSWORD_SET_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;

    system_admin.domain_id = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN;
    system_admin.actor = "admin";
    system_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN;
    root_admin.domain_id = "root-a";
    root_admin.actor = "root-admin";
    root_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

    root.domain_id = "root-b";
    root.actor = system_admin.actor;
    root.request_id = "root-b-create";
    root.expected_revision = 1u;
    root.occurred_at = 2000u;
    check_int_eq(flowie_control_management_domain_create(service, &system_admin, &root, &result),
                 TURBO_OK);
    user.domain_id = "root-b";
    user.principal_id = "admin-b";
    user.principal_type = "human";
    user.actor = system_admin.actor;
    user.request_id = "admin-b-create";
    user.expected_revision = 2u;
    user.occurred_at = 2001u;
    check_int_eq(flowie_control_management_user_create(service, &system_admin, &user, &result),
                 TURBO_OK);

    password.domain_id = "root-b";
    password.principal_id = "admin-b";
    password.new_password = initial_password;
    password.new_password_size = sizeof(initial_password) - 1u;
    password.mode = FLOWIE_CONTROL_PASSWORD_CREATE;
    password.actor = system_admin.actor;
    password.request_id = "admin-b-password-create";
    password.expected_revision = 3u;
    password.occurred_at = 2002u;
    check_int_eq(flowie_control_management_password_set(service, &root_admin, &password, &result),
                 TURBO_EPERM);
    check_int_eq(flowie_control_management_password_set(service, &system_admin, &password, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 4u);
    check_int_eq(flowie_control_store_credential_verify(
                     store, "root-b", "admin-b", initial_password, sizeof(initial_password) - 1u,
                     &verified),
                 TURBO_OK);
    check_uint_eq(verified.credential_revision, 4u);

    password.new_password = replacement_password;
    password.new_password_size = sizeof(replacement_password) - 1u;
    password.mode = FLOWIE_CONTROL_PASSWORD_REPLACE;
    password.request_id = "admin-b-password-replace";
    password.expected_revision = 4u;
    password.occurred_at = 2003u;
    check_int_eq(flowie_control_management_password_set(service, &system_admin, &password, &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 5u);
    verified = (flowie_control_credential_verify_result_t)
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    check_int_eq(flowie_control_store_credential_verify(
                     store, "root-b", "admin-b", replacement_password,
                     sizeof(replacement_password) - 1u, &verified),
                 TURBO_OK);
    check_uint_eq(verified.credential_revision, 5u);

    management_close(service, store, path);
  }

  it("lets only the system administrator select another existing domain") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    flowie_control_management_service_t *service = management_open(&path, &store);
    flowie_control_management_caller_t system_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_caller_t root_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_management_caller_t scoped = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
    flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_user_view_t users[2] = {
        FLOWIE_CONTROL_USER_VIEW_INIT, FLOWIE_CONTROL_USER_VIEW_INIT};
    flowie_control_domain_view_t roots[3] = {
        FLOWIE_CONTROL_DOMAIN_VIEW_INIT, FLOWIE_CONTROL_DOMAIN_VIEW_INIT,
        FLOWIE_CONTROL_DOMAIN_VIEW_INIT};
    size_t count = 0u;
    int has_more = 0;

    system_admin.domain_id = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN;
    system_admin.actor = "admin";
    system_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN;
    root_admin.domain_id = "root-a";
    root_admin.actor = "admin-a";
    root_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

    root.domain_id = FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN;
    root.actor = "bootstrap";
    root.request_id = "request-system-root";
    root.expected_revision = 1u;
    root.occurred_at = 2000u;
    check_int_eq(flowie_control_store_domain_create(store, &root, &result), TURBO_OK);
    root.domain_id = "root-b";
    root.actor = system_admin.actor;
    root.request_id = "request-root-b";
    root.expected_revision = 2u;
    root.occurred_at = 2001u;
    check_int_eq(flowie_control_management_domain_create(service, &system_admin, &root, &result),
                 TURBO_OK);
    user.domain_id = "root-b";
    user.principal_id = "admin-b";
    user.principal_type = "human";
    user.actor = system_admin.actor;
    user.request_id = "request-admin-b";
    user.expected_revision = 3u;
    user.occurred_at = 2002u;
    check_int_eq(flowie_control_management_user_create(service, &system_admin, &user, &result),
                 TURBO_OK);

    check_int_eq(flowie_control_management_scope_caller(service, &system_admin, "root-b", &scoped),
                 TURBO_OK);
    check_str_eq(scoped.domain_id, "root-b");
    check_int_eq(flowie_control_management_user_list(service, &scoped, NULL, users, 2u, &count,
                                                     &has_more),
                 TURBO_OK);
    check_size_eq(count, 1u);
    check_str_eq(users[0].principal_id, "admin-b");
    check_false(has_more);
    check_int_eq(flowie_control_management_scope_caller(service, &root_admin, "root-b", &scoped),
                 TURBO_EPERM);
    check_int_eq(
        flowie_control_management_scope_caller(service, &system_admin, "missing", &scoped),
        TURBO_ENOENT);

    check_int_eq(flowie_control_management_domain_list(
                     service, &system_admin, NULL, roots, 3u, &count, &has_more),
                 TURBO_OK);
    check_size_eq(count, 3u);
    check_str_eq(roots[0].domain_id, "root-a");
    check_str_eq(roots[1].domain_id, "root-b");
    check_str_eq(roots[2].domain_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN);
    check_false(has_more);
    check_int_eq(flowie_control_management_domain_list(
                     service, &root_admin, NULL, roots, 3u, &count, &has_more),
                 TURBO_EPERM);

    management_close(service, store, path);
  }
}
