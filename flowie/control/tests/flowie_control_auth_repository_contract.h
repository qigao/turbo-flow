#ifndef FLOWIE_CONTROL_AUTH_REPOSITORY_CONTRACT_H
#define FLOWIE_CONTROL_AUTH_REPOSITORY_CONTRACT_H

#include "flowie_control_auth_service_internal.h"
#include "flowie_control_credential_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

#define FLOWIE_CONTROL_AUTH_CONTRACT_CERT                                                          \
  "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

static uint64_t flowie_control_auth_contract_clock(void *ctx) {
  return ctx ? *(const uint64_t *)ctx : 0u;
}

static int flowie_control_auth_contract_policy_version(void *ctx, const char *root_group_id,
                                                       uint64_t *policy_version_out) {
  const flowie_control_repository_t *repository = (const flowie_control_repository_t *)ctx;
  flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  int rc;
  if (policy_version_out) *policy_version_out = 0u;
  if (flowie_control_repository_validate(repository) != TURBO_OK || !root_group_id ||
      !policy_version_out)
    return TURBO_EINVAL;
  rc = repository->policy->status(repository->ctx, root_group_id, &status);
  if (rc == TURBO_OK) *policy_version_out = status.policy_version;
  return rc;
}

static int
flowie_control_auth_contract_group_present(const turbo_flow_security_principal_t *principal,
                                           const char *group) {
  if (!principal || !group) return 0;
  for (uint32_t index = 0u; index < principal->group_count; ++index) {
    if (strcmp(principal->groups[index], group) == 0) return 1;
  }
  return 0;
}

/**
 * Exercise the production local Auth path and its ACL generation dependency through one provider.
 *
 * The caller supplies a new, empty Repository and retains ownership of its provider.
 */
static void
flowie_control_auth_repository_contract_run(const flowie_control_repository_t *repository) {
  static const char policy_rule[] = "allow|role|publisher|root-a|connect|generic|exact|client";
  flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
  flowie_control_membership_add_command_t membership = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_credential_revoke_command_t revoke = FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
  flowie_control_policy_rule_put_command_t rule = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
  flowie_control_policy_publish_command_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
  flowie_control_generated_credential_t credential = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_policy_publish_result_t published = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  flowie_control_auth_service_config_t config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_auth_service_t *service = NULL;
  flowie_control_verified_caller_t caller = {sizeof(flowie_control_verified_caller_t), "listener-a",
                                             "broker-a", "root-a",
                                             FLOWIE_CONTROL_AUTH_CONTRACT_CERT, 1};
  flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  uint64_t now_seconds = 10000u;
  size_t audit_count = 0u;
  int cache_hit = -1;

  check_not_null(repository);
  check_int_eq(flowie_control_repository_validate(repository), TURBO_OK);

  root.root_group_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "auth-contract-root";
  root.occurred_at = 1000u;
  check_int_eq(repository->user->root_group_create(repository->ctx, &root, &result), TURBO_OK);

  user.root_group_id = "root-a";
  user.principal_id = "device-a";
  user.principal_type = "device";
  user.actor = "bootstrap";
  user.request_id = "auth-contract-user";
  user.expected_revision = 1u;
  user.occurred_at = 1001u;
  check_int_eq(repository->user->create(repository->ctx, &user, &result), TURBO_OK);

  group.root_group_id = "root-a";
  group.group_id = "engineering";
  group.parent_group_id = "root-a";
  group.actor = "bootstrap";
  group.request_id = "auth-contract-group";
  group.expected_revision = 2u;
  group.occurred_at = 1002u;
  check_int_eq(repository->group->create(repository->ctx, &group, &result), TURBO_OK);

  membership.root_group_id = "root-a";
  membership.principal_id = "device-a";
  membership.group_id = "engineering";
  membership.actor = "bootstrap";
  membership.request_id = "auth-contract-membership";
  membership.expected_revision = 3u;
  membership.occurred_at = 1003u;
  check_int_eq(repository->group->membership_add(repository->ctx, &membership, &result), TURBO_OK);

  role.root_group_id = "root-a";
  role.role_id = "publisher";
  role.actor = "bootstrap";
  role.request_id = "auth-contract-role";
  role.expected_revision = 4u;
  role.occurred_at = 1004u;
  check_int_eq(repository->role->create(repository->ctx, &role, &result), TURBO_OK);

  assignment.root_group_id = "root-a";
  assignment.principal_id = "device-a";
  assignment.role_id = "publisher";
  assignment.actor = "bootstrap";
  assignment.request_id = "auth-contract-assignment";
  assignment.expected_revision = 5u;
  assignment.occurred_at = 1005u;
  check_int_eq(repository->role->assignment_add(repository->ctx, &assignment, &result), TURBO_OK);

  issue.root_group_id = "root-a";
  issue.principal_id = "device-a";
  issue.actor = "bootstrap";
  issue.request_id = "auth-contract-credential";
  issue.expected_revision = 6u;
  issue.occurred_at = 1006u;
  check_int_eq(repository->credential->generate(repository->ctx, &issue, &credential), TURBO_OK);

  rule.root_group_id = "root-a";
  rule.ordinal = 10u;
  rule.rule_line = policy_rule;
  rule.actor = "bootstrap";
  rule.request_id = "auth-contract-rule";
  rule.expected_revision = 7u;
  rule.occurred_at = 1007u;
  check_int_eq(repository->policy->rule_put(repository->ctx, &rule, &result), TURBO_OK);

  publish.root_group_id = "root-a";
  publish.actor = "bootstrap";
  publish.request_id = "auth-contract-publish";
  publish.expected_revision = 8u;
  publish.occurred_at = 1008u;
  publish.expires_at = 20000u;
  check_int_eq(repository->policy->publish(repository->ctx, &publish, &published), TURBO_OK);
  check_uint_eq(published.policy_version, 1u);

  config.repository = repository;
  config.policy_version.ctx = (void *)repository;
  config.policy_version.current = flowie_control_auth_contract_policy_version;
  config.clock_seconds = flowie_control_auth_contract_clock;
  config.clock_ctx = &now_seconds;
  check_int_eq(flowie_control_auth_service_create(&config, &service), TURBO_OK);
  check_not_null(service);

  request.caller = &caller;
  request.identity = "device-a";
  request.method = "password";
  request.secret = credential.secret;
  request.secret_size = credential.secret_size;
  request.protocol = "mqtt";
  request.remote_address = "192.0.2.10:1883";
  check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
               TURBO_OK);
  check_false(cache_hit);
  check_str_eq(principal.root_group_id, "root-a");
  check_str_eq(principal.principal_id, "device-a");
  check_str_eq(principal.auth_method, "password");
  check_uint_eq(principal.policy_version, 1u);
  check_uint_eq(principal.expires_at, 10300u);
  check_true(flowie_control_auth_contract_group_present(&principal, "root-a"));
  check_true(flowie_control_auth_contract_group_present(&principal, "engineering"));
  check_uint_eq(principal.role_count, 1u);
  check_str_eq(principal.roles[0], "publisher");

  check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, &cache_hit),
               TURBO_OK);
  check_true(cache_hit);

  revoke.root_group_id = "root-a";
  revoke.principal_id = "device-a";
  revoke.actor = "bootstrap";
  revoke.request_id = "auth-contract-revoke";
  revoke.expected_revision = 9u;
  revoke.occurred_at = 1009u;
  check_int_eq(repository->credential->revoke(repository->ctx, &revoke, &result), TURBO_OK);
  check_int_eq(flowie_control_auth_service_authenticate(service, &request, &principal, NULL),
               TURBO_EPERM);
  check_str_eq(principal.principal_id, "");
  check_int_eq(repository->audit->count(repository->ctx, &audit_count), TURBO_OK);
  check_size_eq(audit_count, 10u);

  flowie_control_auth_service_destroy(service);
  flowie_control_generated_credential_wipe(&credential);
}

#endif
