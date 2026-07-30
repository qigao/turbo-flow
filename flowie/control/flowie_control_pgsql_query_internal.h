#ifndef FLOWIE_CONTROL_PGSQL_QUERY_INTERNAL_H
#define FLOWIE_CONTROL_PGSQL_QUERY_INTERNAL_H

#include "flowie_control_pgsql_database_internal.h"
#include "flowie_control_store_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_pgsql_query_s flowie_control_pgsql_query_t;

/**
 * Create the PostgreSQL read/query view used by the future Repository provider.
 *
 * The query object borrows `pool`; callers must destroy it before closing or destroying the pool.
 * Its prebuilt SQL is immutable, so concurrent calls are supported by independent pool leases.
 */
int flowie_control_pgsql_query_create(flowie_control_pgsql_pool_t *pool,
                                      flowie_control_pgsql_query_t **out);
void flowie_control_pgsql_query_destroy(flowie_control_pgsql_query_t *query);

int flowie_control_pgsql_query_root_group_get(flowie_control_pgsql_query_t *query,
                                              const char *root_group_id,
                                              flowie_control_root_group_view_t *out);
int flowie_control_pgsql_query_root_group_list(flowie_control_pgsql_query_t *query,
                                               const char *after_root_group_id,
                                               flowie_control_root_group_view_t *items,
                                               size_t item_capacity, size_t *count_out,
                                               int *has_more_out);
int flowie_control_pgsql_query_user_get(flowie_control_pgsql_query_t *query,
                                        const char *root_group_id, const char *principal_id,
                                        flowie_control_user_view_t *out);
int flowie_control_pgsql_query_user_list(flowie_control_pgsql_query_t *query,
                                         const char *root_group_id, const char *after_principal_id,
                                         flowie_control_user_view_t *items, size_t item_capacity,
                                         size_t *count_out, int *has_more_out);

int flowie_control_pgsql_query_credential_verify(flowie_control_pgsql_query_t *query,
                                                 const char *root_group_id,
                                                 const char *principal_id, const void *secret,
                                                 size_t secret_size,
                                                 flowie_control_credential_verify_result_t *result);
int flowie_control_pgsql_query_credential_state(flowie_control_pgsql_query_t *query,
                                                const char *root_group_id, const char *principal_id,
                                                flowie_control_credential_verify_result_t *result);
int flowie_control_pgsql_query_current_revision(flowie_control_pgsql_query_t *query,
                                                uint64_t *revision_out);
int flowie_control_pgsql_query_principal_snapshot(
    flowie_control_pgsql_query_t *query, const char *root_group_id, const char *principal_id,
    const flowie_control_credential_verify_result_t *expected,
    flowie_control_principal_snapshot_t *out);
int flowie_control_pgsql_query_external_principal_snapshot(
    flowie_control_pgsql_query_t *query, const char *root_group_id, const char *principal_id,
    uint64_t assertion_revision, flowie_control_principal_snapshot_t *out);

int flowie_control_pgsql_query_effective_groups(flowie_control_pgsql_query_t *query,
                                                const char *root_group_id, const char *principal_id,
                                                flowie_control_effective_groups_view_t *out);
int flowie_control_pgsql_query_group_list(flowie_control_pgsql_query_t *query,
                                          const char *root_group_id, const char *after_group_id,
                                          flowie_control_group_view_t *items, size_t item_capacity,
                                          size_t *count_out, int *has_more_out);
int flowie_control_pgsql_query_effective_roles(flowie_control_pgsql_query_t *query,
                                               const char *root_group_id, const char *principal_id,
                                               flowie_control_effective_roles_view_t *out);
int flowie_control_pgsql_query_role_list(flowie_control_pgsql_query_t *query,
                                         const char *root_group_id, const char *after_role_id,
                                         flowie_control_role_view_t *items, size_t item_capacity,
                                         size_t *count_out, int *has_more_out);

int flowie_control_pgsql_query_policy_validate(flowie_control_pgsql_query_t *query,
                                               const char *root_group_id,
                                               flowie_control_policy_validation_t *out);
int flowie_control_pgsql_query_policy_rule_list(flowie_control_pgsql_query_t *query,
                                                const char *root_group_id, uint32_t after_ordinal,
                                                int has_after,
                                                flowie_control_policy_rule_view_t *items,
                                                size_t item_capacity, size_t *count_out,
                                                int *has_more_out);
int flowie_control_pgsql_query_policy_status(flowie_control_pgsql_query_t *query,
                                             const char *root_group_id,
                                             flowie_control_policy_status_t *out);
int flowie_control_pgsql_query_policy_bundle_load(flowie_control_pgsql_query_t *query,
                                                  const char *root_group_id,
                                                  uint64_t required_version,
                                                  turbo_flow_security_policy_bundle_t *bundle_out);
void flowie_control_pgsql_query_policy_bundle_release(turbo_flow_security_policy_bundle_t *bundle);

int flowie_control_pgsql_query_audit_list(flowie_control_pgsql_query_t *query,
                                          const char *root_group_id, uint64_t after_revision,
                                          flowie_control_audit_view_t *items, size_t item_capacity,
                                          size_t *count_out, int *has_more_out);
int flowie_control_pgsql_query_audit_count(flowie_control_pgsql_query_t *query, size_t *count_out);

#ifdef __cplusplus
}
#endif

#endif
