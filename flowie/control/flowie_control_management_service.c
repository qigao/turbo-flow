#include "flowie_control_management_service_internal.h"

#include "flowie_control_validation_internal.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct flowie_control_management_service_s {
  flowie_control_repository_t repository;
};

static uint32_t
flowie_control_management_permissions(const flowie_control_effective_roles_view_t *roles) {
  uint32_t permissions = 0u;
  int password_change_required = 0;
  if (!roles || roles->size < sizeof(*roles)) return 0u;
  for (uint32_t index = 0u; index < roles->role_count; ++index) {
    const char *role = roles->roles[index];
    if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_PASSWORD_CHANGE_REQUIRED) == 0)
      password_change_required = 1;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_VIEWER;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_POLICY_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
    else if (strcmp(role, FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN) == 0)
      permissions |= FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN;
  }
  return password_change_required ? FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE : permissions;
}

int flowie_control_management_identity_resolve_principal(
    const flowie_control_repository_t *repository, const char *root_group_id,
    const char *principal_id, flowie_control_management_caller_t *caller_out) {
  flowie_control_user_view_t user = FLOWIE_CONTROL_USER_VIEW_INIT;
  flowie_control_effective_roles_view_t roles = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  int rc;
  if (caller_out && caller_out->size >= sizeof(*caller_out)) *caller_out = caller;
  if (flowie_control_repository_validate(repository) != TURBO_OK || !root_group_id ||
      !principal_id || !caller_out || caller_out->size < sizeof(*caller_out))
    return TURBO_EINVAL;
  rc = repository->user->get(repository->ctx, root_group_id, principal_id, &user);
  if (rc != TURBO_OK || !user.enabled) return TURBO_EPERM;
  rc = repository->role->effective(repository->ctx, root_group_id, principal_id, &roles);
  if (rc != TURBO_OK) return rc == TURBO_EINVAL ? rc : TURBO_EPERM;
  caller.permissions = flowie_control_management_permissions(&roles);
  if (strcmp(root_group_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ROOT_GROUP) != 0)
    caller.permissions &= ~FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN;
  if (caller.permissions == 0u) return TURBO_EPERM;
  caller.root_group_id = root_group_id;
  caller.actor = principal_id;
  *caller_out = caller;
  return TURBO_OK;
}

static int flowie_control_management_caller_valid(const flowie_control_management_caller_t *caller,
                                                  uint32_t required_permission) {
  uint32_t known = FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN |
                   FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN |
                   FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN |
                   FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN |
                   FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE;
  size_t root_size;
  size_t actor_size;
  if (!caller || caller->size < sizeof(*caller) || !caller->root_group_id || !caller->actor ||
      (caller->permissions & ~known) != 0u)
    return 0;
  root_size = strnlen(caller->root_group_id, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  actor_size = strnlen(caller->actor, FLOWIE_CONTROL_ACTOR_MAX + 1u);
  if (root_size == 0u || root_size > TURBO_FLOW_SECURITY_ID_MAX || actor_size == 0u ||
      actor_size > FLOWIE_CONTROL_ACTOR_MAX)
    return 0;
  return (caller->permissions & (FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN |
                                 FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN)) != 0u ||
         (caller->permissions & required_permission) == required_permission;
}

int flowie_control_management_authorize(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        uint32_t required_permission) {
  if (!service || flowie_control_repository_validate(&service->repository) != TURBO_OK ||
      !flowie_control_management_caller_valid(caller, required_permission))
    return TURBO_EPERM;
  return TURBO_OK;
}

int flowie_control_management_scope_caller(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller, const char *target_root_group_id,
    flowie_control_management_caller_t *scoped_out) {
  flowie_control_root_group_view_t root = FLOWIE_CONTROL_ROOT_GROUP_VIEW_INIT;
  flowie_control_management_caller_t scoped = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  int rc;
  if (scoped_out && scoped_out->size >= sizeof(*scoped_out)) *scoped_out = scoped;
  if (!target_root_group_id ||
      !flowie_control_text_valid(target_root_group_id, TURBO_FLOW_SECURITY_ID_MAX) || !scoped_out ||
      scoped_out->size < sizeof(*scoped_out))
    return TURBO_EINVAL;
  rc = flowie_control_management_authorize(service, caller, 0u);
  if (rc != TURBO_OK) return rc;
  if (strcmp(target_root_group_id, caller->root_group_id) != 0 &&
      (((caller->permissions & FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN) == 0u) ||
       strcmp(caller->root_group_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ROOT_GROUP) != 0))
    return TURBO_EPERM;
  rc = service->repository.user->root_group_get(service->repository.ctx, target_root_group_id,
                                                 &root);
  if (rc != TURBO_OK) return rc;
  scoped = *caller;
  scoped.root_group_id = target_root_group_id;
  *scoped_out = scoped;
  return TURBO_OK;
}

static int flowie_control_management_read(flowie_control_management_service_t *service,
                                          const flowie_control_management_caller_t *caller,
                                          uint32_t permission) {
  return flowie_control_management_authorize(service, caller, permission);
}

static int flowie_control_management_write(flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller,
                                           uint32_t permission, const char *root_group_id,
                                           const char *actor) {
  int rc = flowie_control_management_read(service, caller, permission);
  if (rc != TURBO_OK) return rc;
  if (!root_group_id || !actor || strcmp(actor, caller->actor) != 0 ||
      (strcmp(root_group_id, caller->root_group_id) != 0 &&
       ((caller->permissions & FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN) == 0u ||
        strcmp(caller->root_group_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ROOT_GROUP) != 0)))
    return TURBO_EPERM;
  return TURBO_OK;
}

int flowie_control_management_root_group_create(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_root_group_create_command_t *command,
    flowie_control_command_result_t *result) {
  int rc;
  if (!command || command->size < sizeof(*command) || !command->root_group_id || !command->actor ||
      strcmp(command->root_group_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ROOT_GROUP) == 0)
    return TURBO_EINVAL;
  rc = flowie_control_management_authorize(service, caller,
                                           FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN);
  if (rc != TURBO_OK ||
      strcmp(caller->root_group_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ROOT_GROUP) != 0 ||
      strcmp(command->actor, caller->actor) != 0)
    return TURBO_EPERM;
  return service->repository.user->root_group_create(service->repository.ctx, command, result);
}

int flowie_control_management_root_group_list(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller, const char *after_root_group_id,
    flowie_control_root_group_view_t *items, size_t capacity, size_t *count_out,
    int *has_more_out) {
  int rc = flowie_control_management_authorize(service, caller,
                                               FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN);
  if (rc != TURBO_OK) return rc;
  if (strcmp(caller->root_group_id, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ROOT_GROUP) != 0)
    return TURBO_EPERM;
  return service->repository.user->root_group_list(
      service->repository.ctx, after_root_group_id, items, capacity, count_out, has_more_out);
}

int flowie_control_management_password_change(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_password_change_command_t *command,
    flowie_control_command_result_t *result) {
  static const char credential_suffix[] = ":credential";
  static const char complete_suffix[] = ":complete";
  flowie_control_credential_issue_command_t credential =
      FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_user_role_remove_command_t complete = FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT;
  flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  flowie_control_credential_verify_result_t verified =
      FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  char credential_request[FLOWIE_CONTROL_REQUEST_ID_MAX + 1u];
  char complete_request[FLOWIE_CONTROL_REQUEST_ID_MAX + 1u];
  uint64_t current = 0u;
  size_t request_size;
  int credential_replayed = 0;
  int password_change_required;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!service || !caller || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || !command->new_password ||
      command->new_password_size < FLOWIE_CONTROL_HUMAN_PASSWORD_MIN_SIZE ||
      command->new_password_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !command->request_id ||
      command->occurred_at == 0u)
    return TURBO_EINVAL;
  request_size = strnlen(command->request_id, FLOWIE_CONTROL_REQUEST_ID_MAX + 1u);
  if (request_size == 0u ||
      request_size + sizeof(credential_suffix) - 1u > FLOWIE_CONTROL_REQUEST_ID_MAX ||
      request_size + sizeof(complete_suffix) - 1u > FLOWIE_CONTROL_REQUEST_ID_MAX)
    return TURBO_EINVAL;
  rc = flowie_control_management_authorize(service, caller,
                                           FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE);
  if (rc != TURBO_OK) return rc;
  password_change_required = caller->permissions == FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE;
  (void)snprintf(credential_request, sizeof(credential_request), "%s%s", command->request_id,
                 credential_suffix);
  (void)snprintf(complete_request, sizeof(complete_request), "%s%s", command->request_id,
                 complete_suffix);
  credential.root_group_id = caller->root_group_id;
  credential.principal_id = caller->actor;
  credential.actor = caller->actor;
  credential.request_id = credential_request;
  credential.expected_revision = command->expected_revision;
  credential.occurred_at = command->occurred_at;
  credential.initial_secret = command->new_password;
  credential.initial_secret_size = command->new_password_size;
  rc = service->repository.credential->rotate(service->repository.ctx, &credential, &generated);
  if (rc == TURBO_EALREADY) {
    credential_replayed = 1;
    rc = service->repository.auth->credential_verify(
        service->repository.ctx, caller->root_group_id, caller->actor, command->new_password,
        command->new_password_size, &verified);
  }
  if (rc != TURBO_OK) goto done;
  if (!password_change_required) {
    if (credential_replayed)
      rc = service->repository.audit->revision(service->repository.ctx, &result->revision);
    else
      result->revision = generated.revision;
    result->replayed = credential_replayed;
    goto done;
  }
  rc = service->repository.audit->revision(service->repository.ctx, &current);
  if (rc != TURBO_OK) goto done;
  complete.root_group_id = caller->root_group_id;
  complete.principal_id = caller->actor;
  complete.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_PASSWORD_CHANGE_REQUIRED;
  complete.actor = caller->actor;
  complete.request_id = complete_request;
  complete.expected_revision = current;
  complete.occurred_at = command->occurred_at;
  rc = service->repository.role->assignment_remove(service->repository.ctx, &complete, result);

done:
  flowie_control_generated_credential_wipe(&generated);
  return rc;
}

int flowie_control_management_password_set(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_password_set_command_t *command,
    flowie_control_command_result_t *result) {
  flowie_control_credential_issue_command_t credential =
      FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  if (!service || !caller || !command || command->size < sizeof(*command) || !result ||
      result->size < sizeof(*result) || !command->root_group_id || !command->principal_id ||
      !command->new_password || command->new_password_size < FLOWIE_CONTROL_HUMAN_PASSWORD_MIN_SIZE ||
      command->new_password_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX ||
      (command->mode != FLOWIE_CONTROL_PASSWORD_CREATE &&
       command->mode != FLOWIE_CONTROL_PASSWORD_REPLACE) ||
      !command->actor || !command->request_id || command->occurred_at == 0u)
    return TURBO_EINVAL;
  rc = flowie_control_management_write(service, caller, FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                       command->root_group_id, command->actor);
  if (rc != TURBO_OK) return rc;
  credential.root_group_id = command->root_group_id;
  credential.principal_id = command->principal_id;
  credential.actor = command->actor;
  credential.request_id = command->request_id;
  credential.expected_revision = command->expected_revision;
  credential.occurred_at = command->occurred_at;
  credential.initial_secret = command->new_password;
  credential.initial_secret_size = command->new_password_size;
  rc = command->mode == FLOWIE_CONTROL_PASSWORD_CREATE
           ? service->repository.credential->generate(service->repository.ctx, &credential,
                                                      &generated)
           : service->repository.credential->rotate(service->repository.ctx, &credential,
                                                    &generated);
  if (rc == TURBO_OK) {
    result->revision = generated.revision;
    result->replayed = 0;
  }
  flowie_control_generated_credential_wipe(&generated);
  return rc;
}

int flowie_control_management_current_revision(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    uint64_t *revision_out) {
  int rc;
  if (revision_out) *revision_out = 0u;
  if (!revision_out) return TURBO_EINVAL;
  rc = flowie_control_management_authorize(service, caller,
                                           FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE);
  return rc == TURBO_OK
             ? service->repository.audit->revision(service->repository.ctx, revision_out)
             : rc;
}

int flowie_control_management_service_create(
    const flowie_control_management_service_config_t *config,
    flowie_control_management_service_t **out) {
  flowie_control_management_service_t *service;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      flowie_control_repository_validate(config->repository) != TURBO_OK || !out)
    return TURBO_EINVAL;
  service = (flowie_control_management_service_t *)calloc(1u, sizeof(*service));
  if (!service) return TURBO_ENOMEM;
  service->repository = *config->repository;
  *out = service;
  return TURBO_OK;
}

void flowie_control_management_service_destroy(flowie_control_management_service_t *service) {
  if (!service) return;
  memset(service, 0, sizeof(*service));
  free(service);
}

int flowie_control_management_system_status(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            flowie_control_management_status_t *out) {
  flowie_control_management_status_t status = FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT;
  int rc;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  *out = status;
  rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  if (rc == TURBO_OK)
    rc = service->repository.audit->revision(service->repository.ctx, &status.store_revision);
  if (rc == TURBO_OK)
    rc = service->repository.policy->status(service->repository.ctx, caller->root_group_id,
                                            &status.policy);
  if (rc == TURBO_OK) *out = status;
  return rc;
}

int flowie_control_management_user_get(flowie_control_management_service_t *service,
                                       const flowie_control_management_caller_t *caller,
                                       const char *principal_id, flowie_control_user_view_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.user->get(service->repository.ctx,
                                                        caller->root_group_id, principal_id, out)
                        : rc;
}

int flowie_control_management_user_list(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        const char *after_principal_id,
                                        flowie_control_user_view_t *items, size_t capacity,
                                        size_t *count_out, int *has_more_out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.user->list(service->repository.ctx,
                                                         caller->root_group_id, after_principal_id,
                                                         items, capacity, count_out, has_more_out)
                        : rc;
}

#define FLOWIE_CONTROL_MANAGEMENT_WRITE(name, permission, command_type, capability, operation)     \
  int name(flowie_control_management_service_t *service,                                           \
           const flowie_control_management_caller_t *caller, const command_type *command,          \
           flowie_control_command_result_t *result) {                                              \
    int rc;                                                                                        \
    if (!command || command->size < sizeof(*command)) return TURBO_EINVAL;                         \
    rc = flowie_control_management_write(service, caller, permission, command->root_group_id,      \
                                         command->actor);                                          \
    return rc == TURBO_OK ? service->repository.capability->operation(service->repository.ctx,     \
                                                                      command, result)             \
                          : rc;                                                                    \
  }

FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_create,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_user_create_command_t, user, create)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_disable,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_user_disable_command_t, user, disable)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_credential_revoke,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_credential_revoke_command_t, credential, revoke)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_group_create,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_group_create_command_t, group, create)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_group_disable,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_group_disable_command_t, group, disable)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_membership_add,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_membership_add_command_t, group, membership_add)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_membership_remove,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_membership_remove_command_t, group,
                                membership_remove)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_role_create,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_role_create_command_t, role, create)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_role_disable,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_role_disable_command_t, role, disable)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_role_add,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_user_role_add_command_t, role, assignment_add)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_role_remove,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_user_role_remove_command_t, role, assignment_remove)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_policy_rule_put,
                                FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN,
                                flowie_control_policy_rule_put_command_t, policy, rule_put)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_policy_rule_delete,
                                FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN,
                                flowie_control_policy_rule_delete_command_t, policy, rule_delete)

#undef FLOWIE_CONTROL_MANAGEMENT_WRITE

static int flowie_control_management_credential_issue(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result, int rotate) {
  int rc;
  if (!command || command->size < sizeof(*command)) return TURBO_EINVAL;
  rc = flowie_control_management_write(service, caller, FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                       command->root_group_id, command->actor);
  if (rc != TURBO_OK) return rc;
  return rotate
             ? service->repository.credential->rotate(service->repository.ctx, command, result)
             : service->repository.credential->generate(service->repository.ctx, command, result);
}

int flowie_control_management_credential_generate(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result) {
  return flowie_control_management_credential_issue(service, caller, command, result, 0);
}

int flowie_control_management_credential_rotate(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result) {
  return flowie_control_management_credential_issue(service, caller, command, result, 1);
}

int flowie_control_management_group_list(flowie_control_management_service_t *service,
                                         const flowie_control_management_caller_t *caller,
                                         const char *after_group_id,
                                         flowie_control_group_view_t *items, size_t capacity,
                                         size_t *count_out, int *has_more_out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.group->list(service->repository.ctx,
                                                          caller->root_group_id, after_group_id,
                                                          items, capacity, count_out, has_more_out)
                        : rc;
}

int flowie_control_management_effective_groups(flowie_control_management_service_t *service,
                                               const flowie_control_management_caller_t *caller,
                                               const char *principal_id,
                                               flowie_control_effective_groups_view_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.group->effective(
                              service->repository.ctx, caller->root_group_id, principal_id, out)
                        : rc;
}

int flowie_control_management_role_list(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        const char *after_role_id,
                                        flowie_control_role_view_t *items, size_t capacity,
                                        size_t *count_out, int *has_more_out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.role->list(service->repository.ctx,
                                                         caller->root_group_id, after_role_id,
                                                         items, capacity, count_out, has_more_out)
                        : rc;
}

int flowie_control_management_effective_roles(flowie_control_management_service_t *service,
                                              const flowie_control_management_caller_t *caller,
                                              const char *principal_id,
                                              flowie_control_effective_roles_view_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.role->effective(
                              service->repository.ctx, caller->root_group_id, principal_id, out)
                        : rc;
}

int flowie_control_management_policy_rule_list(flowie_control_management_service_t *service,
                                               const flowie_control_management_caller_t *caller,
                                               uint32_t after_ordinal, int has_after,
                                               flowie_control_policy_rule_view_t *items,
                                               size_t capacity, size_t *count_out,
                                               int *has_more_out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK
             ? service->repository.policy->rule_list(service->repository.ctx, caller->root_group_id,
                                                     after_ordinal, has_after, items, capacity,
                                                     count_out, has_more_out)
             : rc;
}

int flowie_control_management_policy_validate(flowie_control_management_service_t *service,
                                              const flowie_control_management_caller_t *caller,
                                              flowie_control_policy_validation_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.policy->validate(service->repository.ctx,
                                                               caller->root_group_id, out)
                        : rc;
}

int flowie_control_management_policy_status(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            flowie_control_policy_status_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? service->repository.policy->status(service->repository.ctx,
                                                             caller->root_group_id, out)
                        : rc;
}

int flowie_control_management_policy_publish(flowie_control_management_service_t *service,
                                             const flowie_control_management_caller_t *caller,
                                             const flowie_control_policy_publish_command_t *command,
                                             flowie_control_policy_publish_result_t *result) {
  int rc;
  if (!command || command->size < sizeof(*command)) return TURBO_EINVAL;
  rc = flowie_control_management_write(service, caller, FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN,
                                       command->root_group_id, command->actor);
  return rc == TURBO_OK
             ? service->repository.policy->publish(service->repository.ctx, command, result)
             : rc;
}

int flowie_control_management_audit_list(flowie_control_management_service_t *service,
                                         const flowie_control_management_caller_t *caller,
                                         uint64_t after_revision,
                                         flowie_control_audit_view_t *items, size_t capacity,
                                         size_t *count_out, int *has_more_out) {
  int rc =
      flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN);
  return rc == TURBO_OK ? service->repository.audit->list(service->repository.ctx,
                                                          caller->root_group_id, after_revision,
                                                          items, capacity, count_out, has_more_out)
                        : rc;
}
