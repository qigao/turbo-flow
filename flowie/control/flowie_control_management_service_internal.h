#ifndef FLOWIE_CONTROL_MANAGEMENT_SERVICE_INTERNAL_H
#define FLOWIE_CONTROL_MANAGEMENT_SERVICE_INTERNAL_H

#include "flowie_control_store_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_management_service_s flowie_control_management_service_t;

typedef enum flowie_control_management_permission_e {
  FLOWIE_CONTROL_MANAGEMENT_VIEWER = 1u << 0,
  FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN = 1u << 1,
  FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN = 1u << 2,
  FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN = 1u << 3
} flowie_control_management_permission_t;

typedef struct flowie_control_management_caller_s {
  size_t size;
  const char *root_group_id;
  const char *actor;
  uint32_t permissions;
} flowie_control_management_caller_t;

#define FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT                                                      \
  {sizeof(flowie_control_management_caller_t), NULL, NULL, 0u}

typedef struct flowie_control_management_service_config_s {
  size_t size;
  flowie_control_store_t *store;
} flowie_control_management_service_config_t;

#define FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT                                              \
  {sizeof(flowie_control_management_service_config_t), NULL}

typedef struct flowie_control_management_status_s {
  size_t size;
  uint64_t store_revision;
  flowie_control_policy_status_t policy;
} flowie_control_management_status_t;

#define FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT                                                      \
  {sizeof(flowie_control_management_status_t), 0u, FLOWIE_CONTROL_POLICY_STATUS_INIT}

int flowie_control_management_service_create(
    const flowie_control_management_service_config_t *config,
    flowie_control_management_service_t **out);
void flowie_control_management_service_destroy(flowie_control_management_service_t *service);

int flowie_control_management_system_status(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            flowie_control_management_status_t *out);
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
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);
int flowie_control_management_credential_rotate(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
    const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);
int flowie_control_management_credential_revoke(
    flowie_control_management_service_t *service,
    const flowie_control_management_caller_t *caller,
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
int flowie_control_management_group_disable(flowie_control_management_service_t *service,
                                            const flowie_control_management_caller_t *caller,
                                            const flowie_control_group_disable_command_t *command,
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
