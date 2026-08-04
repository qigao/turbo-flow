#ifndef FLOWIE_CONTROL_PGSQL_COMMAND_INTERNAL_H
#define FLOWIE_CONTROL_PGSQL_COMMAND_INTERNAL_H

#include "flowie_control_pgsql_database_internal.h"
#include "flowie_control_store_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flowie_control_pgsql_command_s flowie_control_pgsql_command_t;

/**
 * Create the PostgreSQL transactional command view.
 *
 * The command object borrows `pool`; callers must destroy it before closing or destroying the
 * pool. Each invocation uses an exclusive lease and one SERIALIZABLE transaction. PostgreSQL
 * serialization/deadlock failures are returned as TURBO_EBUSY so the service boundary can retry
 * the same request id without hiding revision conflicts.
 */
int flowie_control_pgsql_command_create(flowie_control_pgsql_pool_t *pool,
                                        flowie_control_pgsql_command_t **out);
void flowie_control_pgsql_command_destroy(flowie_control_pgsql_command_t *command);

/**
 * Confirm a command commit from its durable audit identity on a fresh pool lease.
 *
 * `committed_out` is zero when no audit row exists. A reused request id whose command identity
 * differs returns TURBO_EBUSY; a matching identity with a different revision returns TURBO_EPROTO.
 * This is the recovery read used when the COMMIT response is unavailable.
 */
int flowie_control_pgsql_command_commit_confirm(
    flowie_control_pgsql_command_t *view, const char *request_id, const char *actor,
    const char *operation, const char *domain_id, const char *target_id,
    const char *target_detail, uint64_t revision, int *committed_out);

int flowie_control_pgsql_command_domain_create(
    flowie_control_pgsql_command_t *view, const flowie_control_domain_create_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_user_create(flowie_control_pgsql_command_t *view,
                                             const flowie_control_user_create_command_t *command,
                                             flowie_control_command_result_t *result);
int flowie_control_pgsql_command_user_disable(flowie_control_pgsql_command_t *view,
                                              const flowie_control_user_disable_command_t *command,
                                              flowie_control_command_result_t *result);
/**
 * Create the first credential and return its random secret exactly once.
 *
 * Replaying the same request returns TURBO_EALREADY because the committed secret is not stored and
 * cannot be returned again.
 */
int flowie_control_pgsql_command_credential_generate(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);
/**
 * Replace an existing credential and return its new random secret exactly once.
 *
 * A revoked credential remains an existing credential and may be reactivated by rotate. Replaying
 * the same request returns TURBO_EALREADY.
 */
int flowie_control_pgsql_command_credential_rotate(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);
/** Revoke one enabled credential; an identical request-id replay returns the committed revision. */
int flowie_control_pgsql_command_credential_revoke(
    flowie_control_pgsql_command_t *view, const flowie_control_credential_revoke_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_group_create(flowie_control_pgsql_command_t *view,
                                              const flowie_control_group_create_command_t *command,
                                              flowie_control_command_result_t *result);
int flowie_control_pgsql_command_group_delete(
    flowie_control_pgsql_command_t *view, const flowie_control_group_delete_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_membership_add(
    flowie_control_pgsql_command_t *view, const flowie_control_membership_add_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_membership_remove(
    flowie_control_pgsql_command_t *view, const flowie_control_membership_remove_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_role_create(flowie_control_pgsql_command_t *view,
                                             const flowie_control_role_create_command_t *command,
                                             flowie_control_command_result_t *result);
int flowie_control_pgsql_command_role_disable(flowie_control_pgsql_command_t *view,
                                              const flowie_control_role_disable_command_t *command,
                                              flowie_control_command_result_t *result);
int flowie_control_pgsql_command_user_role_add(
    flowie_control_pgsql_command_t *view, const flowie_control_user_role_add_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_user_role_remove(
    flowie_control_pgsql_command_t *view, const flowie_control_user_role_remove_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_policy_rule_put(
    flowie_control_pgsql_command_t *view, const flowie_control_policy_rule_put_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_policy_rule_delete(
    flowie_control_pgsql_command_t *view,
    const flowie_control_policy_rule_delete_command_t *command,
    flowie_control_command_result_t *result);
int flowie_control_pgsql_command_policy_publish(
    flowie_control_pgsql_command_t *view, const flowie_control_policy_publish_command_t *command,
    flowie_control_policy_publish_result_t *result);

#ifdef __cplusplus
}
#endif

#endif
