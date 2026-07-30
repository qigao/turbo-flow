#ifndef FLOWIE_CONTROL_REPOSITORY_INTERNAL_H
#define FLOWIE_CONTROL_REPOSITORY_INTERNAL_H

#include "flowie_control_store_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_REPOSITORY_VERSION 3u

typedef enum flowie_control_repository_capability_e {
  FLOWIE_CONTROL_REPOSITORY_DURABLE = 1u << 0,
  FLOWIE_CONTROL_REPOSITORY_ATOMIC_COMMANDS = 1u << 1,
  FLOWIE_CONTROL_REPOSITORY_CONSISTENT_AUTH_SNAPSHOT = 1u << 2,
  FLOWIE_CONTROL_REPOSITORY_KEYSET_PAGINATION = 1u << 3,
  FLOWIE_CONTROL_REPOSITORY_EXTERNAL_IDENTITY_SNAPSHOT = 1u << 4
} flowie_control_repository_capability_t;

#define FLOWIE_CONTROL_REPOSITORY_REQUIRED_CAPABILITIES                                            \
  (FLOWIE_CONTROL_REPOSITORY_DURABLE | FLOWIE_CONTROL_REPOSITORY_ATOMIC_COMMANDS |                 \
   FLOWIE_CONTROL_REPOSITORY_CONSISTENT_AUTH_SNAPSHOT |                                            \
   FLOWIE_CONTROL_REPOSITORY_KEYSET_PAGINATION |                                                   \
   FLOWIE_CONTROL_REPOSITORY_EXTERNAL_IDENTITY_SNAPSHOT)

typedef struct flowie_control_repository_user_ops_s {
  int (*root_group_create)(void *ctx, const flowie_control_root_group_create_command_t *command,
                           flowie_control_command_result_t *result);
  int (*root_group_get)(void *ctx, const char *root_group_id,
                        flowie_control_root_group_view_t *out);
  int (*root_group_list)(void *ctx, const char *after_root_group_id,
                         flowie_control_root_group_view_t *items, size_t item_capacity,
                         size_t *count_out, int *has_more_out);
  int (*create)(void *ctx, const flowie_control_user_create_command_t *command,
                flowie_control_command_result_t *result);
  int (*disable)(void *ctx, const flowie_control_user_disable_command_t *command,
                 flowie_control_command_result_t *result);
  int (*get)(void *ctx, const char *root_group_id, const char *principal_id,
             flowie_control_user_view_t *out);
  int (*list)(void *ctx, const char *root_group_id, const char *after_principal_id,
              flowie_control_user_view_t *items, size_t item_capacity, size_t *count_out,
              int *has_more_out);
} flowie_control_repository_user_ops_t;

typedef struct flowie_control_repository_auth_ops_s {
  int (*credential_verify)(void *ctx, const char *root_group_id, const char *principal_id,
                           const void *secret, size_t secret_size,
                           flowie_control_credential_verify_result_t *result);
  int (*credential_state)(void *ctx, const char *root_group_id, const char *principal_id,
                          flowie_control_credential_verify_result_t *result);
  int (*current_revision)(void *ctx, uint64_t *revision_out);
  int (*principal_snapshot)(void *ctx, const char *root_group_id, const char *principal_id,
                            const flowie_control_credential_verify_result_t *expected,
                            flowie_control_principal_snapshot_t *out);
  int (*external_principal_snapshot)(void *ctx, const char *root_group_id, const char *principal_id,
                                     uint64_t assertion_revision,
                                     flowie_control_principal_snapshot_t *out);
} flowie_control_repository_auth_ops_t;

typedef struct flowie_control_repository_credential_ops_s {
  int (*generate)(void *ctx, const flowie_control_credential_issue_command_t *command,
                  flowie_control_generated_credential_t *result);
  int (*rotate)(void *ctx, const flowie_control_credential_issue_command_t *command,
                flowie_control_generated_credential_t *result);
  int (*revoke)(void *ctx, const flowie_control_credential_revoke_command_t *command,
                flowie_control_command_result_t *result);
} flowie_control_repository_credential_ops_t;

typedef struct flowie_control_repository_group_ops_s {
  int (*create)(void *ctx, const flowie_control_group_create_command_t *command,
                flowie_control_command_result_t *result);
  int (*disable)(void *ctx, const flowie_control_group_disable_command_t *command,
                 flowie_control_command_result_t *result);
  int (*membership_add)(void *ctx, const flowie_control_membership_add_command_t *command,
                        flowie_control_command_result_t *result);
  int (*membership_remove)(void *ctx, const flowie_control_membership_remove_command_t *command,
                           flowie_control_command_result_t *result);
  int (*effective)(void *ctx, const char *root_group_id, const char *principal_id,
                   flowie_control_effective_groups_view_t *out);
  int (*list)(void *ctx, const char *root_group_id, const char *after_group_id,
              flowie_control_group_view_t *items, size_t item_capacity, size_t *count_out,
              int *has_more_out);
} flowie_control_repository_group_ops_t;

typedef struct flowie_control_repository_role_ops_s {
  int (*create)(void *ctx, const flowie_control_role_create_command_t *command,
                flowie_control_command_result_t *result);
  int (*disable)(void *ctx, const flowie_control_role_disable_command_t *command,
                 flowie_control_command_result_t *result);
  int (*assignment_add)(void *ctx, const flowie_control_user_role_add_command_t *command,
                        flowie_control_command_result_t *result);
  int (*assignment_remove)(void *ctx, const flowie_control_user_role_remove_command_t *command,
                           flowie_control_command_result_t *result);
  int (*effective)(void *ctx, const char *root_group_id, const char *principal_id,
                   flowie_control_effective_roles_view_t *out);
  int (*list)(void *ctx, const char *root_group_id, const char *after_role_id,
              flowie_control_role_view_t *items, size_t item_capacity, size_t *count_out,
              int *has_more_out);
} flowie_control_repository_role_ops_t;

typedef struct flowie_control_repository_policy_ops_s {
  int (*rule_put)(void *ctx, const flowie_control_policy_rule_put_command_t *command,
                  flowie_control_command_result_t *result);
  int (*rule_delete)(void *ctx, const flowie_control_policy_rule_delete_command_t *command,
                     flowie_control_command_result_t *result);
  int (*validate)(void *ctx, const char *root_group_id, flowie_control_policy_validation_t *out);
  int (*publish)(void *ctx, const flowie_control_policy_publish_command_t *command,
                 flowie_control_policy_publish_result_t *result);
  int (*rule_list)(void *ctx, const char *root_group_id, uint32_t after_ordinal, int has_after,
                   flowie_control_policy_rule_view_t *items, size_t item_capacity,
                   size_t *count_out, int *has_more_out);
  int (*status)(void *ctx, const char *root_group_id, flowie_control_policy_status_t *out);
  int (*bundle_load)(void *ctx, const char *root_group_id, uint64_t required_version,
                     turbo_flow_security_policy_bundle_t *bundle_out);
  void (*bundle_release)(void *ctx, turbo_flow_security_policy_bundle_t *bundle);
} flowie_control_repository_policy_ops_t;

typedef struct flowie_control_repository_audit_ops_s {
  int (*revision)(void *ctx, uint64_t *revision_out);
  int (*list)(void *ctx, const char *root_group_id, uint64_t after_revision,
              flowie_control_audit_view_t *items, size_t item_capacity, size_t *count_out,
              int *has_more_out);
  int (*count)(void *ctx, size_t *count_out);
} flowie_control_repository_audit_ops_t;

/**
 * Internal control-plane persistence port.
 *
 * The repository borrows `ctx` and every operation table. Its owner must keep them alive until
 * all auth and management services are destroyed. Providers must support concurrent calls or
 * serialize them internally. A failed command must not expose partial domain or audit state.
 */
struct flowie_control_repository_s {
  size_t size;
  uint32_t version;
  uint32_t capabilities;
  void *ctx;
  const flowie_control_repository_user_ops_t *user;
  const flowie_control_repository_auth_ops_t *auth;
  const flowie_control_repository_credential_ops_t *credential;
  const flowie_control_repository_group_ops_t *group;
  const flowie_control_repository_role_ops_t *role;
  const flowie_control_repository_policy_ops_t *policy;
  const flowie_control_repository_audit_ops_t *audit;
};

#define FLOWIE_CONTROL_REPOSITORY_INIT                                                             \
  {sizeof(flowie_control_repository_t),                                                            \
   FLOWIE_CONTROL_REPOSITORY_VERSION,                                                              \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

int flowie_control_repository_validate(const flowie_control_repository_t *repository);

/** Bind the existing SQLite fact store as a borrowed repository implementation. */
int flowie_control_repository_bind_sqlite(flowie_control_store_t *store,
                                          flowie_control_repository_t *repository);

#ifdef __cplusplus
}
#endif

#endif
