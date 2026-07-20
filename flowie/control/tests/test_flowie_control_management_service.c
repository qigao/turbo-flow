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
  flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_management_service_t *service = NULL;

  *path_out = tt_make_temp_file("flowie-management", ".sqlite3");
  check_not_null(*path_out);
  store_config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&store_config, store_out), TURBO_OK);
  root.root_group_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "request-root";
  root.expected_revision = 0u;
  root.occurred_at = 1000u;
  check_int_eq(flowie_control_store_root_group_create(*store_out, &root, &result), TURBO_OK);
  service_config.store = *store_out;
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

    viewer.root_group_id = "root-a";
    viewer.actor = "viewer-1";
    viewer.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    user_admin.root_group_id = "root-a";
    user_admin.actor = "user-admin-1";
    user_admin.permissions =
        FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    security_admin.root_group_id = "root-a";
    security_admin.actor = "security-admin-1";
    security_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

    check_int_eq(flowie_control_management_system_status(service, &viewer, &status), TURBO_OK);
    check_uint_eq(status.store_revision, 1u);

    user.root_group_id = "root-a";
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
    user.root_group_id = "root-b";
    user.request_id = "request-cross-root";
    user.expected_revision = 2u;
    check_int_eq(flowie_control_management_user_create(service, &user_admin, &user, &result),
                 TURBO_EPERM);

    role.root_group_id = "root-a";
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

    admin.root_group_id = "root-a";
    admin.actor = "security-admin-1";
    admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    user.root_group_id = "root-a";
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
    flowie_control_credential_issue_command_t issue =
        FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
    flowie_control_credential_revoke_command_t revoke =
        FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

    user_admin.root_group_id = "root-a";
    user_admin.actor = "user-admin-1";
    user_admin.permissions =
        FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    security_admin.root_group_id = "root-a";
    security_admin.actor = "security-admin-1";
    security_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

    user.root_group_id = security_admin.root_group_id;
    user.principal_id = "device-1";
    user.principal_type = "device";
    user.actor = security_admin.actor;
    user.request_id = "request-user";
    user.expected_revision = 1u;
    user.occurred_at = 2000u;
    check_int_eq(flowie_control_management_user_create(service, &security_admin, &user, &result),
                 TURBO_OK);

    issue.root_group_id = user_admin.root_group_id;
    issue.principal_id = user.principal_id;
    issue.actor = user_admin.actor;
    issue.request_id = "request-credential-denied";
    issue.expected_revision = 2u;
    issue.occurred_at = 3000u;
    check_int_eq(flowie_control_management_credential_generate(service, &user_admin, &issue,
                                                               &generated),
                 TURBO_EPERM);

    issue.actor = security_admin.actor;
    issue.request_id = "request-credential-generate";
    check_int_eq(flowie_control_management_credential_generate(service, &security_admin, &issue,
                                                               &generated),
                 TURBO_OK);
    check_uint_eq(generated.revision, 3u);
    check_size_eq(generated.secret_size, FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE);
    flowie_control_generated_credential_wipe(&generated);

    issue.root_group_id = "root-b";
    issue.request_id = "request-credential-cross-root";
    issue.expected_revision = 3u;
    check_int_eq(flowie_control_management_credential_rotate(service, &security_admin, &issue,
                                                             &generated),
                 TURBO_EPERM);
    issue.root_group_id = security_admin.root_group_id;
    issue.request_id = "request-credential-rotate";
    check_int_eq(flowie_control_management_credential_rotate(service, &security_admin, &issue,
                                                             &generated),
                 TURBO_OK);
    check_uint_eq(generated.revision, 4u);
    check_size_eq(generated.secret_size, FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE);
    flowie_control_generated_credential_wipe(&generated);

    revoke.root_group_id = security_admin.root_group_id;
    revoke.principal_id = user.principal_id;
    revoke.actor = security_admin.actor;
    revoke.request_id = "request-credential-revoke";
    revoke.expected_revision = 4u;
    revoke.occurred_at = 4000u;
    check_int_eq(flowie_control_management_credential_revoke(service, &security_admin, &revoke,
                                                             &result),
                 TURBO_OK);
    check_uint_eq(result.revision, 5u);

    flowie_control_generated_credential_wipe(&generated);
    management_close(service, store, path);
  }
}
