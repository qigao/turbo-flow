#ifndef FLOWIE_CONTROL_MANAGEMENT_SERVICE_INTERNAL_H
#define FLOWIE_CONTROL_MANAGEMENT_SERVICE_INTERNAL_H

#include "flowie_control_identity_internal.h"
#include "flowie_control_repository_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_management_service_s flowie_control_management_service_t;

#define FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER "viewer"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN "user_admin"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_POLICY_ADMIN "policy_admin"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_SECURITY_ADMIN "security_admin"
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_SYSTEM_ADMIN FLOWIE_CONTROL_SYSTEM_ADMIN_ROLE
#define FLOWIE_CONTROL_MANAGEMENT_ROLE_PASSWORD_CHANGE_REQUIRED                                   \
  FLOWIE_CONTROL_PASSWORD_CHANGE_REQUIRED_ROLE
#define FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN FLOWIE_CONTROL_SYSTEM_DOMAIN

typedef enum flowie_control_management_permission_e {
  FLOWIE_CONTROL_MANAGEMENT_VIEWER = 1u << 0,
  FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN = 1u << 1,
  FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN = 1u << 2,
  FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN = 1u << 3,
  FLOWIE_CONTROL_MANAGEMENT_SYSTEM_ADMIN = 1u << 4,
  FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE = 1u << 5
} flowie_control_management_permission_t;

typedef struct flowie_control_management_caller_s {
  size_t size;
  const char *domain_id;
  const char *actor;
  uint32_t permissions;
} flowie_control_management_caller_t;

#define FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT                                                      \
  {sizeof(flowie_control_management_caller_t), NULL, NULL, 0u}

typedef struct flowie_control_management_service_config_s {
  size_t size;
  const flowie_control_repository_t *repository;
} flowie_control_management_service_config_t;

#define FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT                                              \
  {sizeof(flowie_control_management_service_config_t), NULL}

typedef struct flowie_control_management_status_s {
  size_t size;
  uint64_t store_revision;
  flowie_control_policy_status_t policy;
} flowie_control_management_status_t;

typedef struct flowie_control_password_change_command_s {
  size_t size;
  const void *new_password;
  size_t new_password_size;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_password_change_command_t;

#define FLOWIE_CONTROL_PASSWORD_CHANGE_COMMAND_INIT                                               \
  {sizeof(flowie_control_password_change_command_t), NULL, 0u, NULL, 0u, 0u}

typedef enum flowie_control_password_set_mode_e {
  FLOWIE_CONTROL_PASSWORD_CREATE = 0,
  FLOWIE_CONTROL_PASSWORD_REPLACE = 1
} flowie_control_password_set_mode_t;

typedef struct flowie_control_password_set_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const void *new_password;
  size_t new_password_size;
  flowie_control_password_set_mode_t mode;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_password_set_command_t;

#define FLOWIE_CONTROL_PASSWORD_SET_COMMAND_INIT                                                  \
  {sizeof(flowie_control_password_set_command_t), NULL, NULL, NULL, 0u,                           \
   FLOWIE_CONTROL_PASSWORD_CREATE, NULL, NULL, 0u, 0u}

#define FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT                                                      \
  {sizeof(flowie_control_management_status_t), 0u, FLOWIE_CONTROL_POLICY_STATUS_INIT}

int flowie_control_management_service_create(
    const flowie_control_management_service_config_t *config,
    flowie_control_management_service_t **out);
void flowie_control_management_service_destroy(flowie_control_management_service_t *service);

/** Validate one resolved caller against a read permission without touching repository state. */
int flowie_control_management_authorize(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        uint32_t required_permission);

/**
 * Resolve an authorized target Domain into a request-local caller view.
 *
 * The returned caller borrows `target_domain_id`. Same-Root access is unchanged; cross-Root
 * access requires a system_admin whose authenticated Domain is `system`.
 */
int flowie_control_management_scope_caller(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller, const char *target_domain_id,
    flowie_control_management_caller_t *scoped_out);

/** Resolve one enabled local principal and its current effective roles into a borrowed caller. */
int flowie_control_management_identity_resolve_principal(
    const flowie_control_repository_t *repository, const char *domain_id,
    const char *principal_id, flowie_control_management_caller_t *caller_out);

int flowie_control_management_system_status(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            flowie_control_management_status_t *out);
int flowie_control_management_domain_create(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_domain_create_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_domain_list(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller, const char *after_domain_id,
    flowie_control_domain_view_t *items, size_t capacity, size_t *count_out,
    int *has_more_out);
int flowie_control_management_password_change(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_password_change_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_password_set(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_password_set_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_current_revision(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    uint64_t *revision_out);
int flowie_control_management_user_get(flowie_control_management_service_t *service,
                                       const flowie_control_management_caller_t *caller,
                                       const char *principal_id, flowie_control_user_view_t *out);
int flowie_control_management_user_list(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        const char *after_principal_id,
                                        flowie_control_user_view_t *items, size_t capacity,
                                        size_t *count_out, int *has_more_out);
int flowie_control_management_user_create(flowie_control_management_service_t *service,
                                          const flowie_control_management_caller_t *caller,
                                          const flowie_control_user_create_command_t *command,
                                          flowie_control_command_result_t *result);
int flowie_control_management_user_disable(flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller,
                                           const flowie_control_user_disable_command_t *command,
                                           flowie_control_command_result_t *result);
int flowie_control_management_credential_generate(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);
int flowie_control_management_credential_rotate(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);
int flowie_control_management_credential_revoke(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_credential_revoke_command_t *command,
    flowie_control_command_result_t *result);

int flowie_control_management_group_list(flowie_control_management_service_t *service,
                                         const flowie_control_management_caller_t *caller,
                                         const char *after_group_id,
                                         flowie_control_group_view_t *items, size_t capacity,
                                         size_t *count_out, int *has_more_out);
int flowie_control_management_group_create(flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller,
                                           const flowie_control_group_create_command_t *command,
                                           flowie_control_command_result_t *result);
int flowie_control_management_group_delete(flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller,
                                           const flowie_control_group_delete_command_t *command,
                                           flowie_control_command_result_t *result);
int flowie_control_management_membership_add(flowie_control_management_service_t *service,
                                             const flowie_control_management_caller_t *caller,
                                             const flowie_control_membership_add_command_t *command,
                                             flowie_control_command_result_t *result);
int flowie_control_management_membership_remove(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_membership_remove_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_effective_groups(flowie_control_management_service_t *service,
                                               const flowie_control_management_caller_t *caller,
                                               const char *principal_id,
                                               flowie_control_effective_groups_view_t *out);

int flowie_control_management_role_list(flowie_control_management_service_t *service,
                                        const flowie_control_management_caller_t *caller,
                                        const char *after_role_id,
                                        flowie_control_role_view_t *items, size_t capacity,
                                        size_t *count_out, int *has_more_out);
int flowie_control_management_role_create(flowie_control_management_service_t *service,
                                          const flowie_control_management_caller_t *caller,
                                          const flowie_control_role_create_command_t *command,
                                          flowie_control_command_result_t *result);
int flowie_control_management_role_disable(flowie_control_management_service_t *service,
                                           const flowie_control_management_caller_t *caller,
                                           const flowie_control_role_disable_command_t *command,
                                           flowie_control_command_result_t *result);
int flowie_control_management_user_role_add(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            const flowie_control_user_role_add_command_t *command,
                                            flowie_control_command_result_t *result);
int flowie_control_management_user_role_remove(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_user_role_remove_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_effective_roles(flowie_control_management_service_t *service,
                                              const flowie_control_management_caller_t *caller,
                                              const char *principal_id,
                                              flowie_control_effective_roles_view_t *out);

int flowie_control_management_policy_rule_put(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_policy_rule_put_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_policy_rule_delete(
    flowie_control_management_service_t *service, const flowie_control_management_caller_t *caller,
    const flowie_control_policy_rule_delete_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_management_policy_rule_list(flowie_control_management_service_t *service,
                                               const flowie_control_management_caller_t *caller,
                                               uint32_t after_ordinal, int has_after,
                                               flowie_control_policy_rule_view_t *items,
                                               size_t capacity, size_t *count_out,
                                               int *has_more_out);
int flowie_control_management_policy_validate(flowie_control_management_service_t *service,
                                              const flowie_control_management_caller_t *caller,
                                              flowie_control_policy_validation_t *out);
int flowie_control_management_policy_status(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            flowie_control_policy_status_t *out);
int flowie_control_management_policy_publish(flowie_control_management_service_t *service,
                                             const flowie_control_management_caller_t *caller,
                                             const flowie_control_policy_publish_command_t *command,
                                             flowie_control_policy_publish_result_t *result);

int flowie_control_management_audit_list(flowie_control_management_service_t *service,
                                         const flowie_control_management_caller_t *caller,
                                         uint64_t after_revision,
                                         flowie_control_audit_view_t *items, size_t capacity,
                                         size_t *count_out, int *has_more_out);

#ifdef __cplusplus
}
#endif

#endif
