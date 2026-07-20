#include "flowie_control_management_service_internal.h"

#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

struct flowie_control_management_service_s {
  flowie_control_store_t *store;
};

static int flowie_control_management_caller_valid(const flowie_control_management_caller_t *caller,
                                                  uint32_t required_permission) {
  uint32_t known = FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN |
                   FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN |
                   FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;
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
  return (caller->permissions & FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN) != 0u ||
         (caller->permissions & required_permission) == required_permission;
}

static int flowie_control_management_read(flowie_control_management_service_t *service,
                                          const flowie_control_management_caller_t *caller,
                                          uint32_t permission) {
  if (!service || !service->store || !flowie_control_management_caller_valid(caller, permission))
    return TURBO_EPERM;
  return TURBO_OK;
}

static int flowie_control_management_write(flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller,
                                           uint32_t permission, const char *root_group_id,
                                           const char *actor) {
  int rc = flowie_control_management_read(service, caller, permission);
  if (rc != TURBO_OK) return rc;
  if (!root_group_id || !actor || strcmp(root_group_id, caller->root_group_id) != 0 ||
      strcmp(actor, caller->actor) != 0)
    return TURBO_EPERM;
  return TURBO_OK;
}

int flowie_control_management_service_create(
    const flowie_control_management_service_config_t *config,
    flowie_control_management_service_t **out) {
  flowie_control_management_service_t *service;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !config->store || !out) return TURBO_EINVAL;
  service = (flowie_control_management_service_t *)calloc(1u, sizeof(*service));
  if (!service) return TURBO_ENOMEM;
  service->store = config->store;
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
  if (rc == TURBO_OK) rc = flowie_control_store_revision(service->store, &status.store_revision);
  if (rc == TURBO_OK)
    rc = flowie_control_store_policy_status(service->store, caller->root_group_id, &status.policy);
  if (rc == TURBO_OK) *out = status;
  return rc;
}

int flowie_control_management_user_get(flowie_control_management_service_t *service,
                                       const flowie_control_management_caller_t *caller,
                                       const char *principal_id, flowie_control_user_view_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? flowie_control_store_user_get(service->store, caller->root_group_id,
                                                        principal_id, out)
                        : rc;
}

int flowie_control_management_user_list(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        const char *after_principal_id,
                                        flowie_control_user_view_t *items, size_t capacity,
                                        size_t *count_out, int *has_more_out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? flowie_control_store_user_list(service->store, caller->root_group_id,
                                                         after_principal_id, items, capacity,
                                                         count_out, has_more_out)
                        : rc;
}

#define FLOWIE_CONTROL_MANAGEMENT_WRITE(name, permission, command_type, store_fn)                  \
  int name(flowie_control_management_service_t *service,                                           \
           const flowie_control_management_caller_t *caller, const command_type *command,          \
           flowie_control_command_result_t *result) {                                              \
    int rc;                                                                                        \
    if (!command || command->size < sizeof(*command)) return TURBO_EINVAL;                         \
    rc = flowie_control_management_write(service, caller, permission, command->root_group_id,      \
                                         command->actor);                                          \
    return rc == TURBO_OK ? store_fn(service->store, command, result) : rc;                        \
  }

FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_create,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_user_create_command_t,
                                flowie_control_store_user_create)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_disable,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_user_disable_command_t,
                                flowie_control_store_user_disable)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_credential_revoke,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_credential_revoke_command_t,
                                flowie_control_store_credential_revoke)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_group_create,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_group_create_command_t,
                                flowie_control_store_group_create)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_group_disable,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_group_disable_command_t,
                                flowie_control_store_group_disable)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_membership_add,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_membership_add_command_t,
                                flowie_control_store_membership_add)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_membership_remove,
                                FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN,
                                flowie_control_membership_remove_command_t,
                                flowie_control_store_membership_remove)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_role_create,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_role_create_command_t,
                                flowie_control_store_role_create)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_role_disable,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_role_disable_command_t,
                                flowie_control_store_role_disable)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_role_add,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_user_role_add_command_t,
                                flowie_control_store_user_role_add)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_user_role_remove,
                                FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                flowie_control_user_role_remove_command_t,
                                flowie_control_store_user_role_remove)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_policy_rule_put,
                                FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN,
                                flowie_control_policy_rule_put_command_t,
                                flowie_control_store_policy_rule_put)
FLOWIE_CONTROL_MANAGEMENT_WRITE(flowie_control_management_policy_rule_delete,
                                FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN,
                                flowie_control_policy_rule_delete_command_t,
                                flowie_control_store_policy_rule_delete)

#undef FLOWIE_CONTROL_MANAGEMENT_WRITE

static int flowie_control_management_credential_issue(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result, int rotate) {
  int rc;
  if (!command || command->size < sizeof(*command)) return TURBO_EINVAL;
  rc = flowie_control_management_write(service, caller,
                                       FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN,
                                       command->root_group_id, command->actor);
  if (rc != TURBO_OK) return rc;
  return rotate ? flowie_control_store_credential_rotate(service->store, command, result)
                : flowie_control_store_credential_generate(service->store, command, result);
}

int flowie_control_management_credential_generate(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result) {
  return flowie_control_management_credential_issue(service, caller, command, result, 0);
}

int flowie_control_management_credential_rotate(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
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
  return rc == TURBO_OK ? flowie_control_store_group_list(service->store, caller->root_group_id,
                                                          after_group_id, items, capacity,
                                                          count_out, has_more_out)
                        : rc;
}

int flowie_control_management_effective_groups(flowie_control_management_service_t *service,
                                               const flowie_control_management_caller_t *caller,
                                               const char *principal_id,
                                               flowie_control_effective_groups_view_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? flowie_control_store_effective_groups(
                              service->store, caller->root_group_id, principal_id, out)
                        : rc;
}

int flowie_control_management_role_list(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        const char *after_role_id,
                                        flowie_control_role_view_t *items, size_t capacity,
                                        size_t *count_out, int *has_more_out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK
             ? flowie_control_store_role_list(service->store, caller->root_group_id, after_role_id,
                                              items, capacity, count_out, has_more_out)
             : rc;
}

int flowie_control_management_effective_roles(flowie_control_management_service_t *service,
                                              const flowie_control_management_caller_t *caller,
                                              const char *principal_id,
                                              flowie_control_effective_roles_view_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK ? flowie_control_store_effective_roles(
                              service->store, caller->root_group_id, principal_id, out)
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
             ? flowie_control_store_policy_rule_list(service->store, caller->root_group_id,
                                                     after_ordinal, has_after, items, capacity,
                                                     count_out, has_more_out)
             : rc;
}

int flowie_control_management_policy_validate(flowie_control_management_service_t *service,
                                              const flowie_control_management_caller_t *caller,
                                              flowie_control_policy_validation_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK
             ? flowie_control_store_policy_validate(service->store, caller->root_group_id, out)
             : rc;
}

int flowie_control_management_policy_status(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            flowie_control_policy_status_t *out) {
  int rc = flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_VIEWER);
  return rc == TURBO_OK
             ? flowie_control_store_policy_status(service->store, caller->root_group_id, out)
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
  return rc == TURBO_OK ? flowie_control_store_policy_publish(service->store, command, result) : rc;
}

int flowie_control_management_audit_list(flowie_control_management_service_t *service,
                                         const flowie_control_management_caller_t *caller,
                                         uint64_t after_revision,
                                         flowie_control_audit_view_t *items, size_t capacity,
                                         size_t *count_out, int *has_more_out) {
  int rc =
      flowie_control_management_read(service, caller, FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN);
  return rc == TURBO_OK ? flowie_control_store_audit_list(service->store, caller->root_group_id,
                                                          after_revision, items, capacity,
                                                          count_out, has_more_out)
                        : rc;
}
