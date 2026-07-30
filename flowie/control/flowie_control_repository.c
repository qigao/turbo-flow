#include "flowie_control_repository_internal.h"

#include "turbo_error.h"

static int sqlite_root_group_create(void *ctx,
                                    const flowie_control_root_group_create_command_t *command,
                                    flowie_control_command_result_t *result) {
  return flowie_control_store_root_group_create((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_root_group_get(void *ctx, const char *root_group_id,
                                 flowie_control_root_group_view_t *out) {
  return flowie_control_store_root_group_get((flowie_control_store_t *)ctx, root_group_id, out);
}

static int sqlite_root_group_list(void *ctx, const char *after_root_group_id,
                                  flowie_control_root_group_view_t *items, size_t item_capacity,
                                  size_t *count_out, int *has_more_out) {
  return flowie_control_store_root_group_list((flowie_control_store_t *)ctx, after_root_group_id,
                                              items, item_capacity, count_out, has_more_out);
}

static int sqlite_user_create(void *ctx, const flowie_control_user_create_command_t *command,
                              flowie_control_command_result_t *result) {
  return flowie_control_store_user_create((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_user_disable(void *ctx, const flowie_control_user_disable_command_t *command,
                               flowie_control_command_result_t *result) {
  return flowie_control_store_user_disable((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_user_get(void *ctx, const char *root_group_id, const char *principal_id,
                           flowie_control_user_view_t *out) {
  return flowie_control_store_user_get((flowie_control_store_t *)ctx, root_group_id, principal_id,
                                       out);
}

static int sqlite_user_list(void *ctx, const char *root_group_id, const char *after_principal_id,
                            flowie_control_user_view_t *items, size_t item_capacity,
                            size_t *count_out, int *has_more_out) {
  return flowie_control_store_user_list((flowie_control_store_t *)ctx, root_group_id,
                                        after_principal_id, items, item_capacity, count_out,
                                        has_more_out);
}

static int sqlite_credential_verify(void *ctx, const char *root_group_id, const char *principal_id,
                                    const void *secret, size_t secret_size,
                                    flowie_control_credential_verify_result_t *result) {
  return flowie_control_store_credential_verify((flowie_control_store_t *)ctx, root_group_id,
                                                principal_id, secret, secret_size, result);
}

static int sqlite_credential_state(void *ctx, const char *root_group_id, const char *principal_id,
                                   flowie_control_credential_verify_result_t *result) {
  return flowie_control_store_credential_state((flowie_control_store_t *)ctx, root_group_id,
                                               principal_id, result);
}

static int sqlite_current_revision(void *ctx, uint64_t *revision_out) {
  return flowie_control_store_current_revision((flowie_control_store_t *)ctx, revision_out);
}

static int sqlite_principal_snapshot(void *ctx, const char *root_group_id, const char *principal_id,
                                     const flowie_control_credential_verify_result_t *expected,
                                     flowie_control_principal_snapshot_t *out) {
  return flowie_control_store_principal_snapshot((flowie_control_store_t *)ctx, root_group_id,
                                                 principal_id, expected, out);
}

static int sqlite_external_principal_snapshot(void *ctx, const char *root_group_id,
                                              const char *principal_id, uint64_t assertion_revision,
                                              flowie_control_principal_snapshot_t *out) {
  return flowie_control_store_external_principal_snapshot(
      (flowie_control_store_t *)ctx, root_group_id, principal_id, assertion_revision, out);
}

static int sqlite_credential_generate(void *ctx,
                                      const flowie_control_credential_issue_command_t *command,
                                      flowie_control_generated_credential_t *result) {
  return flowie_control_store_credential_generate((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_credential_rotate(void *ctx,
                                    const flowie_control_credential_issue_command_t *command,
                                    flowie_control_generated_credential_t *result) {
  return flowie_control_store_credential_rotate((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_credential_revoke(void *ctx,
                                    const flowie_control_credential_revoke_command_t *command,
                                    flowie_control_command_result_t *result) {
  return flowie_control_store_credential_revoke((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_group_create(void *ctx, const flowie_control_group_create_command_t *command,
                               flowie_control_command_result_t *result) {
  return flowie_control_store_group_create((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_group_disable(void *ctx, const flowie_control_group_disable_command_t *command,
                                flowie_control_command_result_t *result) {
  return flowie_control_store_group_disable((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_membership_add(void *ctx, const flowie_control_membership_add_command_t *command,
                                 flowie_control_command_result_t *result) {
  return flowie_control_store_membership_add((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_membership_remove(void *ctx,
                                    const flowie_control_membership_remove_command_t *command,
                                    flowie_control_command_result_t *result) {
  return flowie_control_store_membership_remove((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_effective_groups(void *ctx, const char *root_group_id, const char *principal_id,
                                   flowie_control_effective_groups_view_t *out) {
  return flowie_control_store_effective_groups((flowie_control_store_t *)ctx, root_group_id,
                                               principal_id, out);
}

static int sqlite_group_list(void *ctx, const char *root_group_id, const char *after_group_id,
                             flowie_control_group_view_t *items, size_t item_capacity,
                             size_t *count_out, int *has_more_out) {
  return flowie_control_store_group_list((flowie_control_store_t *)ctx, root_group_id,
                                         after_group_id, items, item_capacity, count_out,
                                         has_more_out);
}

static int sqlite_role_create(void *ctx, const flowie_control_role_create_command_t *command,
                              flowie_control_command_result_t *result) {
  return flowie_control_store_role_create((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_role_disable(void *ctx, const flowie_control_role_disable_command_t *command,
                               flowie_control_command_result_t *result) {
  return flowie_control_store_role_disable((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_role_assignment_add(void *ctx,
                                      const flowie_control_user_role_add_command_t *command,
                                      flowie_control_command_result_t *result) {
  return flowie_control_store_user_role_add((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_role_assignment_remove(void *ctx,
                                         const flowie_control_user_role_remove_command_t *command,
                                         flowie_control_command_result_t *result) {
  return flowie_control_store_user_role_remove((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_effective_roles(void *ctx, const char *root_group_id, const char *principal_id,
                                  flowie_control_effective_roles_view_t *out) {
  return flowie_control_store_effective_roles((flowie_control_store_t *)ctx, root_group_id,
                                              principal_id, out);
}

static int sqlite_role_list(void *ctx, const char *root_group_id, const char *after_role_id,
                            flowie_control_role_view_t *items, size_t item_capacity,
                            size_t *count_out, int *has_more_out) {
  return flowie_control_store_role_list((flowie_control_store_t *)ctx, root_group_id, after_role_id,
                                        items, item_capacity, count_out, has_more_out);
}

static int sqlite_policy_rule_put(void *ctx,
                                  const flowie_control_policy_rule_put_command_t *command,
                                  flowie_control_command_result_t *result) {
  return flowie_control_store_policy_rule_put((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_policy_rule_delete(void *ctx,
                                     const flowie_control_policy_rule_delete_command_t *command,
                                     flowie_control_command_result_t *result) {
  return flowie_control_store_policy_rule_delete((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_policy_validate(void *ctx, const char *root_group_id,
                                  flowie_control_policy_validation_t *out) {
  return flowie_control_store_policy_validate((flowie_control_store_t *)ctx, root_group_id, out);
}

static int sqlite_policy_publish(void *ctx, const flowie_control_policy_publish_command_t *command,
                                 flowie_control_policy_publish_result_t *result) {
  return flowie_control_store_policy_publish((flowie_control_store_t *)ctx, command, result);
}

static int sqlite_policy_rule_list(void *ctx, const char *root_group_id, uint32_t after_ordinal,
                                   int has_after, flowie_control_policy_rule_view_t *items,
                                   size_t item_capacity, size_t *count_out, int *has_more_out) {
  return flowie_control_store_policy_rule_list((flowie_control_store_t *)ctx, root_group_id,
                                               after_ordinal, has_after, items, item_capacity,
                                               count_out, has_more_out);
}

static int sqlite_policy_status(void *ctx, const char *root_group_id,
                                flowie_control_policy_status_t *out) {
  return flowie_control_store_policy_status((flowie_control_store_t *)ctx, root_group_id, out);
}

static int sqlite_policy_bundle_load(void *ctx, const char *root_group_id,
                                     uint64_t required_version,
                                     turbo_flow_security_policy_bundle_t *bundle_out) {
  return flowie_control_store_policy_bundle_load((flowie_control_store_t *)ctx, root_group_id,
                                                 required_version, bundle_out);
}

static void sqlite_policy_bundle_release(void *ctx,
                                         turbo_flow_security_policy_bundle_t *bundle) {
  (void)ctx;
  flowie_control_store_policy_bundle_release(bundle);
}

static int sqlite_audit_revision(void *ctx, uint64_t *revision_out) {
  return flowie_control_store_revision((flowie_control_store_t *)ctx, revision_out);
}

static int sqlite_audit_list(void *ctx, const char *root_group_id, uint64_t after_revision,
                             flowie_control_audit_view_t *items, size_t item_capacity,
                             size_t *count_out, int *has_more_out) {
  return flowie_control_store_audit_list((flowie_control_store_t *)ctx, root_group_id,
                                         after_revision, items, item_capacity, count_out,
                                         has_more_out);
}

static int sqlite_audit_count(void *ctx, size_t *count_out) {
  return flowie_control_store_audit_count((flowie_control_store_t *)ctx, count_out);
}

static const flowie_control_repository_user_ops_t SQLITE_USER_OPS = {
    sqlite_root_group_create, sqlite_root_group_get, sqlite_root_group_list,
    sqlite_user_create,       sqlite_user_disable,  sqlite_user_get,
    sqlite_user_list};
static const flowie_control_repository_auth_ops_t SQLITE_AUTH_OPS = {
    sqlite_credential_verify, sqlite_credential_state, sqlite_current_revision,
    sqlite_principal_snapshot, sqlite_external_principal_snapshot};
static const flowie_control_repository_credential_ops_t SQLITE_CREDENTIAL_OPS = {
    sqlite_credential_generate, sqlite_credential_rotate, sqlite_credential_revoke};
static const flowie_control_repository_group_ops_t SQLITE_GROUP_OPS = {
    sqlite_group_create,      sqlite_group_disable,    sqlite_membership_add,
    sqlite_membership_remove, sqlite_effective_groups, sqlite_group_list};
static const flowie_control_repository_role_ops_t SQLITE_ROLE_OPS = {
    sqlite_role_create,         sqlite_role_disable,
    sqlite_role_assignment_add, sqlite_role_assignment_remove,
    sqlite_effective_roles,     sqlite_role_list};
static const flowie_control_repository_policy_ops_t SQLITE_POLICY_OPS = {
    sqlite_policy_rule_put, sqlite_policy_rule_delete, sqlite_policy_validate,
    sqlite_policy_publish,  sqlite_policy_rule_list,   sqlite_policy_status,
    sqlite_policy_bundle_load, sqlite_policy_bundle_release};
static const flowie_control_repository_audit_ops_t SQLITE_AUDIT_OPS = {
    sqlite_audit_revision, sqlite_audit_list, sqlite_audit_count};

int flowie_control_repository_validate(const flowie_control_repository_t *repository) {
  if (!repository || repository->size < sizeof(*repository) ||
      repository->version != FLOWIE_CONTROL_REPOSITORY_VERSION ||
      (repository->capabilities & FLOWIE_CONTROL_REPOSITORY_REQUIRED_CAPABILITIES) !=
          FLOWIE_CONTROL_REPOSITORY_REQUIRED_CAPABILITIES ||
      !repository->ctx || !repository->user || !repository->auth || !repository->credential ||
      !repository->group || !repository->role || !repository->policy || !repository->audit)
    return TURBO_EINVAL;
  if (!repository->user->root_group_create || !repository->user->root_group_get ||
      !repository->user->root_group_list || !repository->user->create ||
      !repository->user->disable || !repository->user->get || !repository->user->list ||
      !repository->auth->credential_verify || !repository->auth->credential_state ||
      !repository->auth->current_revision || !repository->auth->principal_snapshot ||
      !repository->auth->external_principal_snapshot || !repository->credential->generate ||
      !repository->credential->rotate || !repository->credential->revoke ||
      !repository->group->create || !repository->group->disable ||
      !repository->group->membership_add || !repository->group->membership_remove ||
      !repository->group->effective || !repository->group->list || !repository->role->create ||
      !repository->role->disable || !repository->role->assignment_add ||
      !repository->role->assignment_remove || !repository->role->effective ||
      !repository->role->list || !repository->policy->rule_put ||
      !repository->policy->rule_delete || !repository->policy->validate ||
      !repository->policy->publish || !repository->policy->rule_list ||
      !repository->policy->status || !repository->policy->bundle_load ||
      !repository->policy->bundle_release || !repository->audit->revision ||
      !repository->audit->list || !repository->audit->count)
    return TURBO_EINVAL;
  return TURBO_OK;
}

int flowie_control_repository_bind_sqlite(flowie_control_store_t *store,
                                          flowie_control_repository_t *repository) {
  flowie_control_repository_t bound = FLOWIE_CONTROL_REPOSITORY_INIT;
  if (!store || !repository || repository->size < sizeof(*repository)) return TURBO_EINVAL;
  bound.capabilities = FLOWIE_CONTROL_REPOSITORY_REQUIRED_CAPABILITIES;
  bound.ctx = store;
  bound.user = &SQLITE_USER_OPS;
  bound.auth = &SQLITE_AUTH_OPS;
  bound.credential = &SQLITE_CREDENTIAL_OPS;
  bound.group = &SQLITE_GROUP_OPS;
  bound.role = &SQLITE_ROLE_OPS;
  bound.policy = &SQLITE_POLICY_OPS;
  bound.audit = &SQLITE_AUDIT_OPS;
  *repository = bound;
  return TURBO_OK;
}
