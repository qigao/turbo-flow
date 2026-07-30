#include "flowie_control_pgsql_repository_internal.h"

#include "flowie_control_pgsql_command_internal.h"
#include "flowie_control_pgsql_query_internal.h"

#include "turbo_error.h"

#include <stdlib.h>

struct flowie_control_pgsql_repository_provider_s {
  flowie_control_pgsql_pool_t *pool;
  flowie_control_pgsql_command_t *command;
  flowie_control_pgsql_query_t *query;
  flowie_control_repository_t repository;
};

static flowie_control_pgsql_repository_provider_t *flowie_control_pgsql_provider(void *ctx) {
  return (flowie_control_pgsql_repository_provider_t *)ctx;
}

static int pgsql_root_group_create(void *ctx,
                                   const flowie_control_root_group_create_command_t *command,
                                   flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_root_group_create(flowie_control_pgsql_provider(ctx)->command,
                                                        command, result);
}

static int pgsql_root_group_get(void *ctx, const char *root_group_id,
                                flowie_control_root_group_view_t *out) {
  return flowie_control_pgsql_query_root_group_get(flowie_control_pgsql_provider(ctx)->query,
                                                   root_group_id, out);
}

static int pgsql_root_group_list(void *ctx, const char *after_root_group_id,
                                 flowie_control_root_group_view_t *items, size_t item_capacity,
                                 size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_root_group_list(
      flowie_control_pgsql_provider(ctx)->query, after_root_group_id, items, item_capacity,
      count_out, has_more_out);
}

static int pgsql_user_create(void *ctx, const flowie_control_user_create_command_t *command,
                             flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_user_create(flowie_control_pgsql_provider(ctx)->command,
                                                  command, result);
}

static int pgsql_user_disable(void *ctx, const flowie_control_user_disable_command_t *command,
                              flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_user_disable(flowie_control_pgsql_provider(ctx)->command,
                                                   command, result);
}

static int pgsql_user_get(void *ctx, const char *root_group_id, const char *principal_id,
                          flowie_control_user_view_t *out) {
  return flowie_control_pgsql_query_user_get(flowie_control_pgsql_provider(ctx)->query,
                                             root_group_id, principal_id, out);
}

static int pgsql_user_list(void *ctx, const char *root_group_id, const char *after_principal_id,
                           flowie_control_user_view_t *items, size_t item_capacity,
                           size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_user_list(flowie_control_pgsql_provider(ctx)->query,
                                              root_group_id, after_principal_id, items,
                                              item_capacity, count_out, has_more_out);
}

static int pgsql_credential_verify(void *ctx, const char *root_group_id, const char *principal_id,
                                   const void *secret, size_t secret_size,
                                   flowie_control_credential_verify_result_t *result) {
  return flowie_control_pgsql_query_credential_verify(flowie_control_pgsql_provider(ctx)->query,
                                                      root_group_id, principal_id, secret,
                                                      secret_size, result);
}

static int pgsql_credential_state(void *ctx, const char *root_group_id, const char *principal_id,
                                  flowie_control_credential_verify_result_t *result) {
  return flowie_control_pgsql_query_credential_state(flowie_control_pgsql_provider(ctx)->query,
                                                     root_group_id, principal_id, result);
}

static int pgsql_current_revision(void *ctx, uint64_t *revision_out) {
  return flowie_control_pgsql_query_current_revision(flowie_control_pgsql_provider(ctx)->query,
                                                     revision_out);
}

static int pgsql_principal_snapshot(void *ctx, const char *root_group_id, const char *principal_id,
                                    const flowie_control_credential_verify_result_t *expected,
                                    flowie_control_principal_snapshot_t *out) {
  return flowie_control_pgsql_query_principal_snapshot(flowie_control_pgsql_provider(ctx)->query,
                                                       root_group_id, principal_id, expected, out);
}

static int pgsql_external_principal_snapshot(void *ctx, const char *root_group_id,
                                             const char *principal_id, uint64_t assertion_revision,
                                             flowie_control_principal_snapshot_t *out) {
  return flowie_control_pgsql_query_external_principal_snapshot(
      flowie_control_pgsql_provider(ctx)->query, root_group_id, principal_id, assertion_revision,
      out);
}

static int pgsql_credential_generate(void *ctx,
                                     const flowie_control_credential_issue_command_t *command,
                                     flowie_control_generated_credential_t *result) {
  return flowie_control_pgsql_command_credential_generate(
      flowie_control_pgsql_provider(ctx)->command, command, result);
}

static int pgsql_credential_rotate(void *ctx,
                                   const flowie_control_credential_issue_command_t *command,
                                   flowie_control_generated_credential_t *result) {
  return flowie_control_pgsql_command_credential_rotate(flowie_control_pgsql_provider(ctx)->command,
                                                        command, result);
}

static int pgsql_credential_revoke(void *ctx,
                                   const flowie_control_credential_revoke_command_t *command,
                                   flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_credential_revoke(flowie_control_pgsql_provider(ctx)->command,
                                                        command, result);
}

static int pgsql_group_create(void *ctx, const flowie_control_group_create_command_t *command,
                              flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_group_create(flowie_control_pgsql_provider(ctx)->command,
                                                   command, result);
}

static int pgsql_group_disable(void *ctx, const flowie_control_group_disable_command_t *command,
                               flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_group_disable(flowie_control_pgsql_provider(ctx)->command,
                                                    command, result);
}

static int pgsql_membership_add(void *ctx, const flowie_control_membership_add_command_t *command,
                                flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_membership_add(flowie_control_pgsql_provider(ctx)->command,
                                                     command, result);
}

static int pgsql_membership_remove(void *ctx,
                                   const flowie_control_membership_remove_command_t *command,
                                   flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_membership_remove(flowie_control_pgsql_provider(ctx)->command,
                                                        command, result);
}

static int pgsql_effective_groups(void *ctx, const char *root_group_id, const char *principal_id,
                                  flowie_control_effective_groups_view_t *out) {
  return flowie_control_pgsql_query_effective_groups(flowie_control_pgsql_provider(ctx)->query,
                                                     root_group_id, principal_id, out);
}

static int pgsql_group_list(void *ctx, const char *root_group_id, const char *after_group_id,
                            flowie_control_group_view_t *items, size_t item_capacity,
                            size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_group_list(flowie_control_pgsql_provider(ctx)->query,
                                               root_group_id, after_group_id, items, item_capacity,
                                               count_out, has_more_out);
}

static int pgsql_role_create(void *ctx, const flowie_control_role_create_command_t *command,
                             flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_role_create(flowie_control_pgsql_provider(ctx)->command,
                                                  command, result);
}

static int pgsql_role_disable(void *ctx, const flowie_control_role_disable_command_t *command,
                              flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_role_disable(flowie_control_pgsql_provider(ctx)->command,
                                                   command, result);
}

static int pgsql_role_assignment_add(void *ctx,
                                     const flowie_control_user_role_add_command_t *command,
                                     flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_user_role_add(flowie_control_pgsql_provider(ctx)->command,
                                                    command, result);
}

static int pgsql_role_assignment_remove(void *ctx,
                                        const flowie_control_user_role_remove_command_t *command,
                                        flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_user_role_remove(flowie_control_pgsql_provider(ctx)->command,
                                                       command, result);
}

static int pgsql_effective_roles(void *ctx, const char *root_group_id, const char *principal_id,
                                 flowie_control_effective_roles_view_t *out) {
  return flowie_control_pgsql_query_effective_roles(flowie_control_pgsql_provider(ctx)->query,
                                                    root_group_id, principal_id, out);
}

static int pgsql_role_list(void *ctx, const char *root_group_id, const char *after_role_id,
                           flowie_control_role_view_t *items, size_t item_capacity,
                           size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_role_list(flowie_control_pgsql_provider(ctx)->query,
                                              root_group_id, after_role_id, items, item_capacity,
                                              count_out, has_more_out);
}

static int pgsql_policy_rule_put(void *ctx, const flowie_control_policy_rule_put_command_t *command,
                                 flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_policy_rule_put(flowie_control_pgsql_provider(ctx)->command,
                                                      command, result);
}

static int pgsql_policy_rule_delete(void *ctx,
                                    const flowie_control_policy_rule_delete_command_t *command,
                                    flowie_control_command_result_t *result) {
  return flowie_control_pgsql_command_policy_rule_delete(
      flowie_control_pgsql_provider(ctx)->command, command, result);
}

static int pgsql_policy_validate(void *ctx, const char *root_group_id,
                                 flowie_control_policy_validation_t *out) {
  return flowie_control_pgsql_query_policy_validate(flowie_control_pgsql_provider(ctx)->query,
                                                    root_group_id, out);
}

static int pgsql_policy_publish(void *ctx, const flowie_control_policy_publish_command_t *command,
                                flowie_control_policy_publish_result_t *result) {
  return flowie_control_pgsql_command_policy_publish(flowie_control_pgsql_provider(ctx)->command,
                                                     command, result);
}

static int pgsql_policy_rule_list(void *ctx, const char *root_group_id, uint32_t after_ordinal,
                                  int has_after, flowie_control_policy_rule_view_t *items,
                                  size_t item_capacity, size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_policy_rule_list(flowie_control_pgsql_provider(ctx)->query,
                                                     root_group_id, after_ordinal, has_after, items,
                                                     item_capacity, count_out, has_more_out);
}

static int pgsql_policy_status(void *ctx, const char *root_group_id,
                               flowie_control_policy_status_t *out) {
  return flowie_control_pgsql_query_policy_status(flowie_control_pgsql_provider(ctx)->query,
                                                  root_group_id, out);
}

static int pgsql_policy_bundle_load(void *ctx, const char *root_group_id, uint64_t required_version,
                                    turbo_flow_security_policy_bundle_t *bundle_out) {
  return flowie_control_pgsql_query_policy_bundle_load(flowie_control_pgsql_provider(ctx)->query,
                                                       root_group_id, required_version, bundle_out);
}

static void pgsql_policy_bundle_release(void *ctx, turbo_flow_security_policy_bundle_t *bundle) {
  (void)ctx;
  flowie_control_pgsql_query_policy_bundle_release(bundle);
}

static int pgsql_audit_list(void *ctx, const char *root_group_id, uint64_t after_revision,
                            flowie_control_audit_view_t *items, size_t item_capacity,
                            size_t *count_out, int *has_more_out) {
  return flowie_control_pgsql_query_audit_list(flowie_control_pgsql_provider(ctx)->query,
                                               root_group_id, after_revision, items, item_capacity,
                                               count_out, has_more_out);
}

static int pgsql_audit_count(void *ctx, size_t *count_out) {
  return flowie_control_pgsql_query_audit_count(flowie_control_pgsql_provider(ctx)->query,
                                                count_out);
}

static const flowie_control_repository_user_ops_t PGSQL_USER_OPS = {
    pgsql_root_group_create, pgsql_root_group_get, pgsql_root_group_list,
    pgsql_user_create,       pgsql_user_disable,  pgsql_user_get,
    pgsql_user_list};
static const flowie_control_repository_auth_ops_t PGSQL_AUTH_OPS = {
    pgsql_credential_verify, pgsql_credential_state, pgsql_current_revision,
    pgsql_principal_snapshot, pgsql_external_principal_snapshot};
static const flowie_control_repository_credential_ops_t PGSQL_CREDENTIAL_OPS = {
    pgsql_credential_generate, pgsql_credential_rotate, pgsql_credential_revoke};
static const flowie_control_repository_group_ops_t PGSQL_GROUP_OPS = {
    pgsql_group_create,      pgsql_group_disable,    pgsql_membership_add,
    pgsql_membership_remove, pgsql_effective_groups, pgsql_group_list};
static const flowie_control_repository_role_ops_t PGSQL_ROLE_OPS = {
    pgsql_role_create,         pgsql_role_disable,
    pgsql_role_assignment_add, pgsql_role_assignment_remove,
    pgsql_effective_roles,     pgsql_role_list};
static const flowie_control_repository_policy_ops_t PGSQL_POLICY_OPS = {
    pgsql_policy_rule_put,    pgsql_policy_rule_delete,   pgsql_policy_validate,
    pgsql_policy_publish,     pgsql_policy_rule_list,     pgsql_policy_status,
    pgsql_policy_bundle_load, pgsql_policy_bundle_release};
static const flowie_control_repository_audit_ops_t PGSQL_AUDIT_OPS = {
    pgsql_current_revision, pgsql_audit_list, pgsql_audit_count};

int flowie_control_pgsql_repository_create(const flowie_control_pgsql_pool_config_t *config,
                                           flowie_control_pgsql_repository_provider_t **out) {
  flowie_control_pgsql_repository_provider_t *provider;
  flowie_control_repository_t repository = FLOWIE_CONTROL_REPOSITORY_INIT;
  int rc;
  if (out) *out = NULL;
  if (!config || !out) return TURBO_EINVAL;
  provider = (flowie_control_pgsql_repository_provider_t *)calloc(1u, sizeof(*provider));
  if (!provider) return TURBO_ENOMEM;
  rc = flowie_control_pgsql_pool_create(config, &provider->pool);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_command_create(provider->pool, &provider->command);
  if (rc == TURBO_OK) rc = flowie_control_pgsql_query_create(provider->pool, &provider->query);
  if (rc == TURBO_OK) {
    repository.capabilities = FLOWIE_CONTROL_REPOSITORY_REQUIRED_CAPABILITIES;
    repository.ctx = provider;
    repository.user = &PGSQL_USER_OPS;
    repository.auth = &PGSQL_AUTH_OPS;
    repository.credential = &PGSQL_CREDENTIAL_OPS;
    repository.group = &PGSQL_GROUP_OPS;
    repository.role = &PGSQL_ROLE_OPS;
    repository.policy = &PGSQL_POLICY_OPS;
    repository.audit = &PGSQL_AUDIT_OPS;
    rc = flowie_control_repository_validate(&repository);
  }
  if (rc == TURBO_OK) {
    provider->repository = repository;
    *out = provider;
    return TURBO_OK;
  }
  flowie_control_pgsql_query_destroy(provider->query);
  flowie_control_pgsql_command_destroy(provider->command);
  if (provider->pool) {
    (void)flowie_control_pgsql_pool_close(provider->pool, 0);
    (void)flowie_control_pgsql_pool_destroy(provider->pool);
  }
  free(provider);
  return rc;
}

const flowie_control_repository_t *
flowie_control_pgsql_repository_view(const flowie_control_pgsql_repository_provider_t *provider) {
  return provider ? &provider->repository : NULL;
}

int flowie_control_pgsql_repository_destroy(flowie_control_pgsql_repository_provider_t *provider,
                                            int timeout_ms) {
  int rc;
  if (!provider) return TURBO_OK;
  rc = flowie_control_pgsql_pool_close(provider->pool, timeout_ms);
  if (rc != TURBO_OK) return rc;
  flowie_control_pgsql_query_destroy(provider->query);
  flowie_control_pgsql_command_destroy(provider->command);
  rc = flowie_control_pgsql_pool_destroy(provider->pool);
  if (rc != TURBO_OK) return rc;
  free(provider);
  return TURBO_OK;
}
