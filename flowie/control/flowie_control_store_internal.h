#ifndef FLOWIE_CONTROL_STORE_INTERNAL_H
#define FLOWIE_CONTROL_STORE_INTERNAL_H

#include "turbo_flow_security.h"
#include "flowie_control_acl_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_REQUEST_ID_MAX 255u
#define FLOWIE_CONTROL_ACTOR_MAX 255u
#define FLOWIE_CONTROL_GROUP_MAX_DEPTH 15
#define FLOWIE_CONTROL_CREDENTIAL_ENTROPY_SIZE 32u
#define FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX "flw_mqtt_v1_"
#define FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX_SIZE                                               \
  (sizeof(FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX) - 1u)
#define FLOWIE_CONTROL_CREDENTIAL_TOKEN_PAYLOAD_SIZE                                              \
  ((FLOWIE_CONTROL_CREDENTIAL_ENTROPY_SIZE * 8u + 5u) / 6u)
#define FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE                                                      \
  (FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX_SIZE + FLOWIE_CONTROL_CREDENTIAL_TOKEN_PAYLOAD_SIZE)
#define FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY (FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE + 1u)
#define FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX 4096u
#define FLOWIE_CONTROL_PAGE_MAX 100u
#define FLOWIE_CONTROL_OPERATION_NAME_MAX 31u

typedef struct flowie_control_store_s flowie_control_store_t;
typedef struct flowie_control_repository_s flowie_control_repository_t;

typedef struct flowie_control_store_config_s {
  size_t size;
  const char *database_path;
  int busy_timeout_ms;
} flowie_control_store_config_t;

#define FLOWIE_CONTROL_STORE_CONFIG_INIT {sizeof(flowie_control_store_config_t), NULL, 1000}

typedef struct flowie_control_user_create_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *principal_type;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_user_create_command_t;

#define FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT                                                    \
  {sizeof(flowie_control_user_create_command_t), NULL, NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_user_disable_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_user_disable_command_t;

#define FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT                                                   \
  {sizeof(flowie_control_user_disable_command_t), NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_command_result_s {
  size_t size;
  uint64_t revision;
  int replayed;
} flowie_control_command_result_t;

#define FLOWIE_CONTROL_COMMAND_RESULT_INIT {sizeof(flowie_control_command_result_t), 0u, 0}

typedef struct flowie_control_user_view_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint64_t revision;
  uint64_t created_at;
  uint64_t updated_at;
  int enabled;
} flowie_control_user_view_t;

#define FLOWIE_CONTROL_USER_VIEW_INIT {sizeof(flowie_control_user_view_t)}

typedef struct flowie_control_credential_issue_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
  /** Optional borrowed bootstrap-only secret. Providers hash it and never return its plaintext. */
  const void *initial_secret;
  size_t initial_secret_size;
} flowie_control_credential_issue_command_t;

#define FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT                                               \
  {sizeof(flowie_control_credential_issue_command_t), NULL, NULL, NULL, NULL, 0u, 0u, NULL, 0u}

typedef struct flowie_control_generated_credential_s {
  size_t size;
  uint64_t revision;
  size_t token_size;
  char token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY];
} flowie_control_generated_credential_t;

#define FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT                                                   \
  {sizeof(flowie_control_generated_credential_t), 0u, 0u, {0}}

typedef struct flowie_control_credential_verify_result_s {
  size_t size;
  uint64_t user_revision;
  uint64_t credential_revision;
} flowie_control_credential_verify_result_t;

#define FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT                                               \
  {sizeof(flowie_control_credential_verify_result_t), 0u, 0u}

typedef struct flowie_control_credential_resolution_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  flowie_control_credential_verify_result_t verified;
} flowie_control_credential_resolution_t;

#define FLOWIE_CONTROL_CREDENTIAL_RESOLUTION_INIT                                                  \
  {sizeof(flowie_control_credential_resolution_t), {0},                                            \
   FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT}

typedef struct flowie_control_credential_revoke_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_credential_revoke_command_t;

#define FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT                                              \
  {sizeof(flowie_control_credential_revoke_command_t), NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_domain_create_command_s {
  size_t size;
  const char *domain_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_domain_create_command_t;

#define FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT                                              \
  {sizeof(flowie_control_domain_create_command_t), NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_domain_view_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
} flowie_control_domain_view_t;

#define FLOWIE_CONTROL_DOMAIN_VIEW_INIT {sizeof(flowie_control_domain_view_t), {0}}

typedef struct flowie_control_group_create_command_s {
  size_t size;
  const char *domain_id;
  const char *group_id;
  const char *parent_group_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_group_create_command_t;

#define FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT                                                   \
  {sizeof(flowie_control_group_create_command_t), NULL, NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_group_delete_command_s {
  size_t size;
  const char *domain_id;
  const char *group_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_group_delete_command_t;

#define FLOWIE_CONTROL_GROUP_DELETE_COMMAND_INIT                                                   \
  {sizeof(flowie_control_group_delete_command_t), NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_membership_add_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *group_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_membership_add_command_t;

#define FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT                                                 \
  {sizeof(flowie_control_membership_add_command_t), NULL, NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_membership_remove_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *group_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_membership_remove_command_t;

#define FLOWIE_CONTROL_MEMBERSHIP_REMOVE_COMMAND_INIT                                              \
  {sizeof(flowie_control_membership_remove_command_t), NULL, NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_effective_groups_view_s {
  size_t size;
  uint32_t group_count;
  char groups[TURBO_FLOW_SECURITY_MAX_GROUPS][TURBO_FLOW_SECURITY_ID_MAX + 1u];
} flowie_control_effective_groups_view_t;

#define FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT                                                  \
  {sizeof(flowie_control_effective_groups_view_t), 0u}

typedef struct flowie_control_role_create_command_s {
  size_t size;
  const char *domain_id;
  const char *role_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_role_create_command_t;

#define FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT                                                    \
  {sizeof(flowie_control_role_create_command_t), NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_role_disable_command_s {
  size_t size;
  const char *domain_id;
  const char *role_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_role_disable_command_t;

#define FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT                                                   \
  {sizeof(flowie_control_role_disable_command_t), NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_user_role_add_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *role_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_user_role_add_command_t;

#define FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT                                                  \
  {sizeof(flowie_control_user_role_add_command_t), NULL, NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_user_role_remove_command_s {
  size_t size;
  const char *domain_id;
  const char *principal_id;
  const char *role_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_user_role_remove_command_t;

#define FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT                                               \
  {sizeof(flowie_control_user_role_remove_command_t), NULL, NULL, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_effective_roles_view_s {
  size_t size;
  uint32_t role_count;
  char roles[TURBO_FLOW_SECURITY_MAX_ROLES][TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
} flowie_control_effective_roles_view_t;

#define FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT {sizeof(flowie_control_effective_roles_view_t), 0u}

typedef struct flowie_control_principal_snapshot_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint64_t user_revision;
  uint64_t credential_revision;
  flowie_control_effective_groups_view_t effective_groups;
  flowie_control_effective_roles_view_t effective_roles;
} flowie_control_principal_snapshot_t;

#define FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT                                                     \
  {sizeof(flowie_control_principal_snapshot_t),                                                    \
   "",                                                                                             \
   "",                                                                                             \
   "",                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT,                                                      \
   FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT}

typedef struct flowie_control_policy_rule_put_command_s {
  size_t size;
  const char *domain_id;
  uint32_t ordinal;
  const char *rule_line;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_policy_rule_put_command_t;

#define FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT                                                \
  {sizeof(flowie_control_policy_rule_put_command_t), NULL, 0u, NULL, NULL, NULL, 0u, 0u}

typedef struct flowie_control_policy_rule_delete_command_s {
  size_t size;
  const char *domain_id;
  uint32_t ordinal;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
} flowie_control_policy_rule_delete_command_t;

#define FLOWIE_CONTROL_POLICY_RULE_DELETE_COMMAND_INIT                                             \
  {sizeof(flowie_control_policy_rule_delete_command_t), NULL, 0u, NULL, NULL, 0u, 0u}

typedef struct flowie_control_policy_publish_command_s {
  size_t size;
  const char *domain_id;
  const char *actor;
  const char *request_id;
  uint64_t expected_revision;
  uint64_t occurred_at;
  uint64_t expires_at;
} flowie_control_policy_publish_command_t;

#define FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT                                                 \
  {sizeof(flowie_control_policy_publish_command_t), NULL, NULL, NULL, 0u, 0u, 0u}

typedef struct flowie_control_policy_validation_s {
  size_t size;
  uint64_t store_revision;
  size_t rule_count;
  size_t deny_rule_count;
} flowie_control_policy_validation_t;

#define FLOWIE_CONTROL_POLICY_VALIDATION_INIT                                                      \
  {sizeof(flowie_control_policy_validation_t), 0u, 0u, 0u}

typedef struct flowie_control_policy_publish_result_s {
  size_t size;
  uint64_t revision;
  uint64_t policy_version;
  int replayed;
} flowie_control_policy_publish_result_t;

#define FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT                                                  \
  {sizeof(flowie_control_policy_publish_result_t), 0u, 0u, 0}

typedef struct flowie_control_policy_status_s {
  size_t size;
  uint64_t store_revision;
  uint64_t policy_version;
  uint64_t expires_at;
  size_t draft_rule_count;
  size_t published_rule_count;
} flowie_control_policy_status_t;

#define FLOWIE_CONTROL_POLICY_STATUS_INIT                                                          \
  {sizeof(flowie_control_policy_status_t), 0u, 0u, 0u, 0u, 0u}

typedef struct flowie_control_policy_rule_view_s {
  size_t size;
  uint32_t ordinal;
  char rule_line[FLOWIE_CONTROL_ACL_DOCUMENT_MAX + 1u];
  uint64_t revision;
  uint64_t updated_at;
} flowie_control_policy_rule_view_t;

#define FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT                                                       \
  {sizeof(flowie_control_policy_rule_view_t), 0u, "", 0u, 0u}

typedef struct flowie_control_group_view_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char parent_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  uint32_t depth;
  uint64_t revision;
  uint64_t created_at;
  uint64_t updated_at;
  int enabled;
} flowie_control_group_view_t;

#define FLOWIE_CONTROL_GROUP_VIEW_INIT {sizeof(flowie_control_group_view_t)}

typedef struct flowie_control_role_view_s {
  size_t size;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char role_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  uint64_t revision;
  uint64_t created_at;
  uint64_t updated_at;
  int enabled;
} flowie_control_role_view_t;

#define FLOWIE_CONTROL_ROLE_VIEW_INIT {sizeof(flowie_control_role_view_t)}

typedef struct flowie_control_audit_view_s {
  size_t size;
  char request_id[FLOWIE_CONTROL_REQUEST_ID_MAX + 1u];
  char actor[FLOWIE_CONTROL_ACTOR_MAX + 1u];
  char operation[FLOWIE_CONTROL_OPERATION_NAME_MAX + 1u];
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char target_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char target_detail[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u];
  uint64_t revision;
  uint64_t occurred_at;
} flowie_control_audit_view_t;

#define FLOWIE_CONTROL_AUDIT_VIEW_INIT {sizeof(flowie_control_audit_view_t)}

/** Internal control-plane store. It owns only copied configuration, not caller strings. */
int flowie_control_store_open(const flowie_control_store_config_t *config,
                              flowie_control_store_t **out);
void flowie_control_store_destroy(flowie_control_store_t *store);

/**
 * Return the borrowed repository adapter owned by this SQLite store.
 *
 * The returned interface becomes invalid when `store` is destroyed.
 */
const flowie_control_repository_t *flowie_control_store_repository(flowie_control_store_t *store);

/** One transaction: validate revision, create user, advance revision, append audit, commit. */
int flowie_control_store_user_create(flowie_control_store_t *store,
                                     const flowie_control_user_create_command_t *command,
                                     flowie_control_command_result_t *result);

/** One transaction: validate revision, disable user, advance revision, append audit, commit. */
int flowie_control_store_user_disable(flowie_control_store_t *store,
                                      const flowie_control_user_disable_command_t *command,
                                      flowie_control_command_result_t *result);

/** Read-only snapshot copied into caller-owned storage. */
int flowie_control_store_user_get(flowie_control_store_t *store, const char *domain_id,
                                  const char *principal_id, flowie_control_user_view_t *out);

/** Create the first active credential and return its random secret exactly once. */
int flowie_control_store_credential_generate(
    flowie_control_store_t *store, const flowie_control_credential_issue_command_t *command,
    flowie_control_generated_credential_t *result);

/** Replace an existing verifier, re-enable it, and return a new random secret exactly once. */
int flowie_control_store_credential_rotate(flowie_control_store_t *store,
                                           const flowie_control_credential_issue_command_t *command,
                                           flowie_control_generated_credential_t *result);

/** Revoke one credential without deleting its audit or revision history. */
int flowie_control_store_credential_revoke(
    flowie_control_store_t *store, const flowie_control_credential_revoke_command_t *command,
    flowie_control_command_result_t *result);

/** Verify a bounded binary secret and return revisions suitable for cache invalidation. */
int flowie_control_store_credential_verify(flowie_control_store_t *store, const char *domain_id,
                                           const char *principal_id, const void *secret,
                                           size_t secret_size,
                                           flowie_control_credential_verify_result_t *result);

/** Resolve one globally unique enabled principal ID and verify its credential. */
int flowie_control_store_credential_resolve(
    flowie_control_store_t *store, const char *principal_id, const void *secret,
    size_t secret_size, flowie_control_credential_resolution_t *result);

/** Read active user and credential revisions without evaluating the credential KDF. */
int flowie_control_store_credential_state(flowie_control_store_t *store, const char *domain_id,
                                          const char *principal_id,
                                          flowie_control_credential_verify_result_t *result);

/** Read the global control-plane revision used to invalidate derived auth snapshots. */
int flowie_control_store_current_revision(flowie_control_store_t *store, uint64_t *revision_out);

/**
 * Read one revision-checked authentication snapshot in a single SQLite read transaction.
 *
 * The expected revisions must come from a successful credential verification. Any disable,
 * rotation, revoke, role change, or membership change committed before this transaction is
 * reflected atomically; credential/user revision changes fail closed.
 */
int flowie_control_store_principal_snapshot(
    flowie_control_store_t *store, const char *domain_id, const char *principal_id,
    const flowie_control_credential_verify_result_t *expected,
    flowie_control_principal_snapshot_t *out);

/**
 * Read one local authorization snapshot for an externally authenticated identity.
 *
 * The user must exist and be enabled, but no local credential is required. assertion_revision is
 * copied into credential_revision as the opaque external authentication generation; callers must
 * not use this snapshot in the local credential cache.
 */
int flowie_control_store_external_principal_snapshot(flowie_control_store_t *store,
                                                     const char *domain_id,
                                                     const char *principal_id,
                                                     uint64_t assertion_revision,
                                                     flowie_control_principal_snapshot_t *out);

/** Erase the caller-owned one-time secret. Safe to call on an initialized empty result. */
void flowie_control_generated_credential_wipe(flowie_control_generated_credential_t *credential);

/** Create the immutable root of one security tree. */
int flowie_control_store_domain_create(
    flowie_control_store_t *store, const flowie_control_domain_create_command_t *command,
    flowie_control_command_result_t *result);

/** Create one child under an existing node; tree depth is bounded and nodes have one parent. */
int flowie_control_store_group_create(flowie_control_store_t *store,
                                      const flowie_control_group_create_command_t *command,
                                      flowie_control_command_result_t *result);

/** Tombstone a non-root Group after all active child and direct membership references are gone. */
int flowie_control_store_group_delete(flowie_control_store_t *store,
                                      const flowie_control_group_delete_command_t *command,
                                      flowie_control_command_result_t *result);

/** Add direct membership and reject an effective ancestor closure larger than the security ABI. */
int flowie_control_store_membership_add(flowie_control_store_t *store,
                                        const flowie_control_membership_add_command_t *command,
                                        flowie_control_command_result_t *result);

/** Remove one direct group assignment, including assignments owned by disabled users. */
int flowie_control_store_membership_remove(
    flowie_control_store_t *store, const flowie_control_membership_remove_command_t *command,
    flowie_control_command_result_t *result);

/** Return root, direct groups, and all ancestors as a bounded caller-owned snapshot. */
int flowie_control_store_effective_groups(flowie_control_store_t *store, const char *domain_id,
                                          const char *principal_id,
                                          flowie_control_effective_groups_view_t *out);

/** Create one Domain-scoped role. */
int flowie_control_store_role_create(flowie_control_store_t *store,
                                     const flowie_control_role_create_command_t *command,
                                     flowie_control_command_result_t *result);

/** Tombstone one role; existing assignments remain referentially valid but become ineffective. */
int flowie_control_store_role_disable(flowie_control_store_t *store,
                                      const flowie_control_role_disable_command_t *command,
                                      flowie_control_command_result_t *result);

/** Add a direct user-role assignment and reject a role set larger than the security ABI. */
int flowie_control_store_user_role_add(flowie_control_store_t *store,
                                       const flowie_control_user_role_add_command_t *command,
                                       flowie_control_command_result_t *result);

/** Remove one direct assignment, including assignments to disabled users or roles. */
int flowie_control_store_user_role_remove(flowie_control_store_t *store,
                                          const flowie_control_user_role_remove_command_t *command,
                                          flowie_control_command_result_t *result);

/** Return enabled direct roles as a bounded caller-owned snapshot. */
int flowie_control_store_effective_roles(flowie_control_store_t *store, const char *domain_id,
                                         const char *principal_id,
                                         flowie_control_effective_roles_view_t *out);

/** Insert or replace one canonical draft rule under an explicit stable ordinal. */
int flowie_control_store_policy_rule_put(flowie_control_store_t *store,
                                         const flowie_control_policy_rule_put_command_t *command,
                                         flowie_control_command_result_t *result);

/** Remove one draft rule without renumbering the remaining draft. */
int flowie_control_store_policy_rule_delete(
    flowie_control_store_t *store, const flowie_control_policy_rule_delete_command_t *command,
    flowie_control_command_result_t *result);

/** Validate the complete current draft without modifying state. */
int flowie_control_store_policy_validate(flowie_control_store_t *store, const char *domain_id,
                                         flowie_control_policy_validation_t *out);

/** Publish one validated immutable v3 bundle and advance policy_version atomically. */
int flowie_control_store_policy_publish(flowie_control_store_t *store,
                                        const flowie_control_policy_publish_command_t *command,
                                        flowie_control_policy_publish_result_t *result);

/** Return bounded draft rows ordered by ordinal; after_ordinal is exclusive. */
int flowie_control_store_policy_rule_list(flowie_control_store_t *store, const char *domain_id,
                                          uint32_t after_ordinal, int has_after,
                                          flowie_control_policy_rule_view_t *items,
                                          size_t item_capacity, size_t *count_out,
                                          int *has_more_out);

/** Return current draft and published bundle metadata. */
int flowie_control_store_policy_status(flowie_control_store_t *store, const char *domain_id,
                                       flowie_control_policy_status_t *out);

/**
 * Load one immutable published ACL generation. required_version zero selects the current
 * generation; a positive value requires an exact match. The returned rules are owned by the
 * bundle and remain valid until flowie_control_store_policy_bundle_release().
 */
int flowie_control_store_policy_bundle_load(flowie_control_store_t *store,
                                            const char *domain_id, uint64_t required_version,
                                            turbo_flow_security_policy_bundle_t *bundle_out);
void flowie_control_store_policy_bundle_release(turbo_flow_security_policy_bundle_t *bundle);

/** Root-scoped keyset pages. Cursor values are exclusive and all results are caller-owned. */
int flowie_control_store_domain_get(flowie_control_store_t *store, const char *domain_id,
                                        flowie_control_domain_view_t *out);
int flowie_control_store_domain_list(flowie_control_store_t *store,
                                         const char *after_domain_id,
                                         flowie_control_domain_view_t *items,
                                         size_t item_capacity, size_t *count_out,
                                         int *has_more_out);
int flowie_control_store_user_list(flowie_control_store_t *store, const char *domain_id,
                                   const char *after_principal_id,
                                   flowie_control_user_view_t *items, size_t item_capacity,
                                   size_t *count_out, int *has_more_out);
int flowie_control_store_group_list(flowie_control_store_t *store, const char *domain_id,
                                    const char *after_group_id, flowie_control_group_view_t *items,
                                    size_t item_capacity, size_t *count_out, int *has_more_out);
int flowie_control_store_role_list(flowie_control_store_t *store, const char *domain_id,
                                   const char *after_role_id, flowie_control_role_view_t *items,
                                   size_t item_capacity, size_t *count_out, int *has_more_out);
int flowie_control_store_audit_list(flowie_control_store_t *store, const char *domain_id,
                                    uint64_t after_revision, flowie_control_audit_view_t *items,
                                    size_t item_capacity, size_t *count_out, int *has_more_out);

int flowie_control_store_revision(flowie_control_store_t *store, uint64_t *revision_out);
int flowie_control_store_audit_count(flowie_control_store_t *store, size_t *count_out);

#ifdef __cplusplus
}
#endif

#endif
