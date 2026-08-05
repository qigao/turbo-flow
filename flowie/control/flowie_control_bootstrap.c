#include "flowie_control_bootstrap_internal.h"

#include "flowie_control_credential_internal.h"
#include "flowie_control_runtime_internal.h"
#include "turbo_error.h"

#include <string.h>

#define FLOWIE_CONTROL_BOOTSTRAP_ACTOR "flowie-bootstrap"
#define FLOWIE_CONTROL_BOOTSTRAP_ROOT_REQUEST "flowie-bootstrap-v1-root"
#define FLOWIE_CONTROL_BOOTSTRAP_USER_REQUEST "flowie-bootstrap-v1-user"
#define FLOWIE_CONTROL_BOOTSTRAP_CREDENTIAL_REQUEST "flowie-bootstrap-v1-credential"
#define FLOWIE_CONTROL_BOOTSTRAP_ROLE_REQUEST "flowie-bootstrap-v1-role"
#define FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ROLE_REQUEST "flowie-bootstrap-v1-password-role"
#define FLOWIE_CONTROL_BOOTSTRAP_ASSIGNMENT_REQUEST "flowie-bootstrap-v1-role-assignment"
#define FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ASSIGNMENT_REQUEST                                      \
  "flowie-bootstrap-v1-password-assignment"

typedef enum flowie_control_bootstrap_revision_e {
  FLOWIE_CONTROL_BOOTSTRAP_EMPTY_REVISION = 0u,
  FLOWIE_CONTROL_BOOTSTRAP_ROOT_REVISION = 1u,
  FLOWIE_CONTROL_BOOTSTRAP_USER_REVISION = 2u,
  FLOWIE_CONTROL_BOOTSTRAP_CREDENTIAL_REVISION = 3u,
  FLOWIE_CONTROL_BOOTSTRAP_ROLE_REVISION = 4u,
  FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ROLE_REVISION = 5u,
  FLOWIE_CONTROL_BOOTSTRAP_ASSIGNMENT_REVISION = 6u,
  FLOWIE_CONTROL_BOOTSTRAP_COMPLETE_REVISION = 7u
} flowie_control_bootstrap_revision_t;

static int flowie_control_bootstrap_result(int rc, const flowie_control_command_result_t *result,
                                           uint64_t expected_revision) {
  if (rc != TURBO_OK) return rc;
  return result && result->revision == expected_revision ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_control_bootstrap_verify(const flowie_control_repository_t *repository,
                                           const flowie_control_config_bootstrap_t *config) {
  flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
  flowie_control_effective_roles_view_t roles = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  int has_system_admin = 0;
  int rc =
      repository->user->get(repository->ctx, config->domain_id, config->principal_id, &user);
  if (rc == TURBO_OK && (!user.enabled || strcmp(user.principal_type, config->principal_type) != 0))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = repository->role->effective(repository->ctx, config->domain_id, config->principal_id,
                                     &roles);
  if (rc == TURBO_OK) {
    for (uint32_t index = 0u; index < roles.role_count; ++index) {
      if (strcmp(roles.roles[index], FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN) == 0)
        has_system_admin = 1;
    }
    if (!has_system_admin) rc = TURBO_EPERM;
  }
  return rc;
}

int flowie_control_bootstrap_apply(const flowie_control_repository_t *repository,
                                   const flowie_control_config_bootstrap_t *config,
                                   const void *password, size_t password_size,
                                   uint64_t occurred_at) {
  flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_credential_issue_command_t credential =
      FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_role_create_command_t password_role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_user_role_add_command_t password_assignment =
      FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  flowie_control_domain_view_t existing_domain = FLOWIE_CONTROL_DOMAIN_VIEW_INIT;
  uint64_t current_revision = 0u;
  int rc;
  if (flowie_control_repository_validate(repository) != TURBO_OK || !config ||
      !config->domain_id[0] || !config->principal_id[0] || !config->principal_type[0] ||
      !password || password_size < FLOWIE_CONTROL_CONFIG_BOOTSTRAP_PASSWORD_MIN ||
      password_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || occurred_at == 0u)
    return TURBO_EINVAL;

  rc = repository->auth->current_revision(repository->ctx, &current_revision);
  if (rc != TURBO_OK) goto done;
  if (current_revision != FLOWIE_CONTROL_BOOTSTRAP_EMPTY_REVISION) {
    rc = repository->user->domain_get(repository->ctx, config->domain_id, &existing_domain);
    if (rc == TURBO_ENOENT) rc = TURBO_EBUSY;
    if (rc != TURBO_OK) goto done;
  }

  root.domain_id = config->domain_id;
  root.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  root.request_id = FLOWIE_CONTROL_BOOTSTRAP_ROOT_REQUEST;
  root.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_EMPTY_REVISION;
  root.occurred_at = occurred_at;
  rc = repository->user->domain_create(repository->ctx, &root, &result);
  if (rc == TURBO_EALREADY) rc = TURBO_EBUSY;
  rc = flowie_control_bootstrap_result(rc, &result, FLOWIE_CONTROL_BOOTSTRAP_ROOT_REVISION);
  if (rc != TURBO_OK) goto done;

  user.domain_id = config->domain_id;
  user.principal_id = config->principal_id;
  user.principal_type = config->principal_type;
  user.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  user.request_id = FLOWIE_CONTROL_BOOTSTRAP_USER_REQUEST;
  user.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_ROOT_REVISION;
  user.occurred_at = occurred_at;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  rc = repository->user->create(repository->ctx, &user, &result);
  rc = flowie_control_bootstrap_result(rc, &result, FLOWIE_CONTROL_BOOTSTRAP_USER_REVISION);
  if (rc != TURBO_OK) goto done;

  credential.domain_id = config->domain_id;
  credential.principal_id = config->principal_id;
  credential.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  credential.request_id = FLOWIE_CONTROL_BOOTSTRAP_CREDENTIAL_REQUEST;
  credential.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_USER_REVISION;
  credential.occurred_at = occurred_at;
  credential.initial_secret = password;
  credential.initial_secret_size = password_size;
  rc = repository->credential->generate(repository->ctx, &credential, &generated);
  if (rc == TURBO_OK && (generated.revision != FLOWIE_CONTROL_BOOTSTRAP_CREDENTIAL_REVISION ||
                         generated.token_size != 0u))
    rc = TURBO_EPROTO;
  if (rc == TURBO_EALREADY) rc = TURBO_OK;
  if (rc != TURBO_OK) goto done;

  role.domain_id = config->domain_id;
  role.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN;
  role.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  role.request_id = FLOWIE_CONTROL_BOOTSTRAP_ROLE_REQUEST;
  role.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_CREDENTIAL_REVISION;
  role.occurred_at = occurred_at;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  rc = repository->role->create(repository->ctx, &role, &result);
  rc = flowie_control_bootstrap_result(rc, &result, FLOWIE_CONTROL_BOOTSTRAP_ROLE_REVISION);
  if (rc != TURBO_OK) goto done;

  password_role.domain_id = config->domain_id;
  password_role.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_PASSWORD_CHANGE_REQUIRED;
  password_role.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  password_role.request_id = FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ROLE_REQUEST;
  password_role.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_ROLE_REVISION;
  password_role.occurred_at = occurred_at;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  rc = repository->role->create(repository->ctx, &password_role, &result);
  rc = flowie_control_bootstrap_result(rc, &result,
                                       FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ROLE_REVISION);
  if (rc != TURBO_OK) goto done;

  assignment.domain_id = config->domain_id;
  assignment.principal_id = config->principal_id;
  assignment.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN;
  assignment.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  assignment.request_id = FLOWIE_CONTROL_BOOTSTRAP_ASSIGNMENT_REQUEST;
  assignment.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ROLE_REVISION;
  assignment.occurred_at = occurred_at;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  rc = repository->role->assignment_add(repository->ctx, &assignment, &result);
  rc = flowie_control_bootstrap_result(rc, &result, FLOWIE_CONTROL_BOOTSTRAP_ASSIGNMENT_REVISION);
  if (rc != TURBO_OK) goto done;

  password_assignment.domain_id = config->domain_id;
  password_assignment.principal_id = config->principal_id;
  password_assignment.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_PASSWORD_CHANGE_REQUIRED;
  password_assignment.actor = FLOWIE_CONTROL_BOOTSTRAP_ACTOR;
  password_assignment.request_id = FLOWIE_CONTROL_BOOTSTRAP_PASSWORD_ASSIGNMENT_REQUEST;
  password_assignment.expected_revision = FLOWIE_CONTROL_BOOTSTRAP_ASSIGNMENT_REVISION;
  password_assignment.occurred_at = occurred_at;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  rc = repository->role->assignment_add(repository->ctx, &password_assignment, &result);
  rc = flowie_control_bootstrap_result(rc, &result, FLOWIE_CONTROL_BOOTSTRAP_COMPLETE_REVISION);
  if (rc == TURBO_OK) rc = flowie_control_bootstrap_verify(repository, config);

done:
  flowie_control_generated_credential_wipe(&generated);
  return rc;
}
