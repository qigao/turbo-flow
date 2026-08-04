#include "flowie_control_bootstrap_internal.h"
#include "flowie_control_runtime_internal.h"
#include "flowie_control_store_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BOOTSTRAP_PASSWORD FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD
#define CHANGED_PASSWORD "changed-bootstrap-password"

static int bootstrap_open(char **path_out, flowie_control_store_t **store_out) {
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  if (path_out) *path_out = NULL;
  if (store_out) *store_out = NULL;
  if (!path_out || !store_out) return TURBO_EINVAL;
  *path_out = tt_make_temp_file("flowie-control-bootstrap", ".sqlite3");
  if (!*path_out) return TURBO_ENOMEM;
  store_config.database_path = *path_out;
  return flowie_control_store_open(&store_config, store_out);
}

static void bootstrap_close(char *path, flowie_control_store_t *store) {
  flowie_control_store_destroy(store);
  if (path) {
    (void)tt_remove_file(path);
    free(path);
  }
}

static flowie_control_config_bootstrap_t bootstrap_config(void) {
  flowie_control_config_bootstrap_t config = {0};
  (void)snprintf(config.domain_id, sizeof(config.domain_id), "%s",
                 FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN);
  (void)snprintf(config.principal_id, sizeof(config.principal_id), "%s", "admin");
  (void)snprintf(config.principal_type, sizeof(config.principal_type), "%s", "human");
  return config;
}

spec("Flowie controller bootstrap") {
  it("creates and verifies one initial security administrator") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    const flowie_control_repository_t *repository;
    flowie_control_config_bootstrap_t config = bootstrap_config();
    flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
    flowie_control_effective_roles_view_t roles = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
    flowie_control_credential_verify_result_t credential =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    flowie_control_management_service_config_t service_config =
        FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
    flowie_control_management_service_t *service = NULL;
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    flowie_control_password_change_command_t change = FLOWIE_CONTROL_PASSWORD_CHANGE_COMMAND_INIT;
    flowie_control_command_result_t changed = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    int has_password_change_required = 0;
    int has_system_admin = 0;
    size_t audit_count = 0u;

    check_int_eq(bootstrap_open(&path, &store), TURBO_OK);
    repository = flowie_control_store_repository(store);
    check_not_null(repository);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, BOOTSTRAP_PASSWORD,
                                                strlen(BOOTSTRAP_PASSWORD), 1000u),
                 TURBO_OK);
    check_int_eq(repository->user->get(repository->ctx, "system", "admin", &user), TURBO_OK);
    check_true(user.enabled);
    check_str_eq(user.principal_type, "human");
    check_int_eq(repository->auth->credential_verify(repository->ctx, "system", "admin",
                                                     BOOTSTRAP_PASSWORD, strlen(BOOTSTRAP_PASSWORD),
                                                     &credential),
                 TURBO_OK);
    check_int_eq(repository->role->effective(repository->ctx, "system", "admin", &roles),
                 TURBO_OK);
    check_uint_eq(roles.role_count, 2u);
    for (uint32_t index = 0u; index < roles.role_count; ++index) {
      if (strcmp(roles.roles[index], FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN) == 0)
        has_system_admin = 1;
      if (strcmp(roles.roles[index],
                 FLOWIE_CONTROL_MANAGEMENT_ROLE_PASSWORD_CHANGE_REQUIRED) == 0)
        has_password_change_required = 1;
    }
    check_true(has_system_admin);
    check_true(has_password_change_required);
    check_int_eq(repository->audit->count(repository->ctx, &audit_count), TURBO_OK);
    check_size_eq(audit_count, 7u);
    service_config.repository = repository;
    check_int_eq(flowie_control_management_service_create(&service_config, &service), TURBO_OK);
    check_int_eq(flowie_control_management_identity_resolve_principal(repository, "system", "admin",
                                                                      &caller),
                 TURBO_OK);
    check_uint_eq(caller.permissions, FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE);
    change.new_password = CHANGED_PASSWORD;
    change.new_password_size = strlen(CHANGED_PASSWORD);
    change.request_id = "bootstrap-first-password-change";
    change.expected_revision = 7u;
    change.occurred_at = 2000u;
    check_int_eq(flowie_control_management_password_change(service, &caller, &change, &changed),
                 TURBO_OK);
    check_uint_eq(changed.revision, 9u);
    check_int_eq(repository->auth->credential_verify(repository->ctx, "system", "admin",
                                                     BOOTSTRAP_PASSWORD,
                                                     strlen(BOOTSTRAP_PASSWORD), &credential),
                 TURBO_EPERM);
    check_int_eq(repository->auth->credential_verify(repository->ctx, "system", "admin",
                                                     CHANGED_PASSWORD, strlen(CHANGED_PASSWORD),
                                                     &credential),
                 TURBO_OK);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, BOOTSTRAP_PASSWORD,
                                                strlen(BOOTSTRAP_PASSWORD), 3000u),
                 TURBO_OK);
    check_int_eq(repository->auth->credential_verify(repository->ctx, "system", "admin",
                                                     BOOTSTRAP_PASSWORD,
                                                     strlen(BOOTSTRAP_PASSWORD), &credential),
                 TURBO_EPERM);
    check_int_eq(repository->auth->credential_verify(repository->ctx, "system", "admin",
                                                     CHANGED_PASSWORD, strlen(CHANGED_PASSWORD),
                                                     &credential),
                 TURBO_OK);
    roles = (flowie_control_effective_roles_view_t)FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
    check_int_eq(repository->role->effective(repository->ctx, "system", "admin", &roles),
                 TURBO_OK);
    check_uint_eq(roles.role_count, 1u);
    check_str_eq(roles.roles[0], FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN);
    caller = (flowie_control_management_caller_t)FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
    check_int_eq(flowie_control_management_identity_resolve_principal(repository, "system", "admin",
                                                                      &caller),
                 TURBO_OK);
    check_uint_eq(caller.permissions, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN);
    flowie_control_management_service_destroy(service);
    bootstrap_close(path, store);
  }

  it("replays an interrupted-safe bootstrap without changing repository state") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    const flowie_control_repository_t *repository;
    flowie_control_config_bootstrap_t config = bootstrap_config();
    size_t audit_count = 0u;

    check_int_eq(bootstrap_open(&path, &store), TURBO_OK);
    repository = flowie_control_store_repository(store);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, BOOTSTRAP_PASSWORD,
                                                strlen(BOOTSTRAP_PASSWORD), 1000u),
                 TURBO_OK);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, BOOTSTRAP_PASSWORD,
                                                strlen(BOOTSTRAP_PASSWORD), 2000u),
                 TURBO_OK);
    check_int_eq(repository->audit->count(repository->ctx, &audit_count), TURBO_OK);
    check_size_eq(audit_count, 7u);
    bootstrap_close(path, store);
  }

  it("treats the initial password as a one-time initialization secret") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    const flowie_control_repository_t *repository;
    flowie_control_config_bootstrap_t config = bootstrap_config();

    check_int_eq(bootstrap_open(&path, &store), TURBO_OK);
    repository = flowie_control_store_repository(store);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, BOOTSTRAP_PASSWORD,
                                                strlen(BOOTSTRAP_PASSWORD), 1000u),
                 TURBO_OK);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, "different-password-strong",
                                                strlen("different-password-strong"), 2000u),
                 TURBO_OK);
    bootstrap_close(path, store);
  }

  it("rejects an unrelated non-empty repository") {
    char *path = NULL;
    flowie_control_store_t *store = NULL;
    const flowie_control_repository_t *repository;
    flowie_control_config_bootstrap_t config = bootstrap_config();
    flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

    check_int_eq(bootstrap_open(&path, &store), TURBO_OK);
    repository = flowie_control_store_repository(store);
    root.domain_id = "unrelated";
    root.actor = "test";
    root.request_id = "unrelated-root";
    root.expected_revision = 0u;
    root.occurred_at = 500u;
    check_int_eq(repository->user->domain_create(repository->ctx, &root, &result), TURBO_OK);
    check_int_eq(flowie_control_bootstrap_apply(repository, &config, BOOTSTRAP_PASSWORD,
                                                strlen(BOOTSTRAP_PASSWORD), 1000u),
                 TURBO_EBUSY);
    bootstrap_close(path, store);
  }
}
