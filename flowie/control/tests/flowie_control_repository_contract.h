#ifndef FLOWIE_CONTROL_REPOSITORY_CONTRACT_H
#define FLOWIE_CONTROL_REPOSITORY_CONTRACT_H

#include "flowie_control_repository_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static int flowie_control_contract_contains(const char *items, size_t item_size, uint32_t count,
                                            const char *expected) {
  for (uint32_t index = 0u; index < count; ++index) {
    if (strcmp(items + index * item_size, expected) == 0) return 1;
  }
  return 0;
}

/**
 * Provider-neutral transactional behavior required from every control Repository.
 *
 * The caller owns a new, empty provider and remains responsible for destroying it after this
 * function returns.
 */
static void
flowie_control_repository_basic_contract_run(const flowie_control_repository_t *repository) {
  static const char policy_rule[] =
      "user device-a allow {\n"
      "  read topic root-a/groups/operators/devices/%u/event\n"
      "}";
  flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
  flowie_control_membership_add_command_t membership = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_policy_rule_put_command_t rule = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
  flowie_control_policy_publish_command_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
  flowie_control_generated_credential_t credential = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  flowie_control_credential_verify_result_t verified = FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_user_view_t view = FLOWIE_CONTROL_USER_VIEW_INIT;
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  flowie_control_policy_publish_result_t published = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  flowie_control_policy_status_t status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  size_t audit_count = 0u;
  uint64_t revision = 0u;

  check_not_null(repository);
  check_int_eq(flowie_control_repository_validate(repository), TURBO_OK);

  root.domain_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "contract-root";
  root.occurred_at = 1000u;
  check_int_eq(repository->user->domain_create(repository->ctx, &root, &result), TURBO_OK);

  user.domain_id = "root-a";
  user.principal_id = "device-a";
  user.principal_type = "device";
  user.actor = "bootstrap";
  user.request_id = "contract-user";
  user.expected_revision = 1u;
  user.occurred_at = 1001u;
  check_int_eq(repository->user->create(repository->ctx, &user, &result), TURBO_OK);
  check_int_eq(repository->user->get(repository->ctx, "root-a", "device-a", &view), TURBO_OK);
  check_true(view.enabled);

  user.principal_id = "stale-device";
  user.request_id = "contract-stale-user";
  user.expected_revision = 1u;
  user.occurred_at = 1002u;
  check_int_eq(repository->user->create(repository->ctx, &user, &result), TURBO_EBUSY);
  check_int_eq(repository->user->get(repository->ctx, "root-a", "stale-device", &view),
               TURBO_ENOENT);

  group.domain_id = "root-a";
  group.group_id = "operators";
  group.parent_group_id = NULL;
  group.actor = "bootstrap";
  group.request_id = "contract-group";
  group.expected_revision = 2u;
  group.occurred_at = 1003u;
  check_int_eq(repository->group->create(repository->ctx, &group, &result), TURBO_OK);

  membership.domain_id = "root-a";
  membership.principal_id = "device-a";
  membership.group_id = "operators";
  membership.actor = "bootstrap";
  membership.request_id = "contract-membership";
  membership.expected_revision = 3u;
  membership.occurred_at = 1004u;
  check_int_eq(repository->group->membership_add(repository->ctx, &membership, &result), TURBO_OK);

  role.domain_id = "root-a";
  role.role_id = "reader";
  role.actor = "bootstrap";
  role.request_id = "contract-role";
  role.expected_revision = 4u;
  role.occurred_at = 1005u;
  check_int_eq(repository->role->create(repository->ctx, &role, &result), TURBO_OK);

  assignment.domain_id = "root-a";
  assignment.principal_id = "device-a";
  assignment.role_id = "reader";
  assignment.actor = "bootstrap";
  assignment.request_id = "contract-assignment";
  assignment.expected_revision = 5u;
  assignment.occurred_at = 1006u;
  check_int_eq(repository->role->assignment_add(repository->ctx, &assignment, &result), TURBO_OK);

  check_int_eq(repository->auth->external_principal_snapshot(repository->ctx, "root-a", "device-a",
                                                             77u, &snapshot),
               TURBO_OK);
  check_uint_eq(snapshot.user_revision, 2u);
  check_uint_eq(snapshot.credential_revision, 77u);
  check_true(flowie_control_contract_contains(snapshot.effective_groups.groups[0],
                                              sizeof(snapshot.effective_groups.groups[0]),
                                              snapshot.effective_groups.group_count, "operators"));
  check_true(flowie_control_contract_contains(snapshot.effective_roles.roles[0],
                                              sizeof(snapshot.effective_roles.roles[0]),
                                              snapshot.effective_roles.role_count, "reader"));

  issue.domain_id = "root-a";
  issue.principal_id = "device-a";
  issue.actor = "bootstrap";
  issue.request_id = "contract-credential";
  issue.expected_revision = 6u;
  issue.occurred_at = 1007u;
  check_int_eq(repository->credential->generate(repository->ctx, &issue, &credential), TURBO_OK);
  check_int_eq(repository->auth->credential_verify(repository->ctx, "root-a", "device-a",
                                                   credential.token, credential.token_size,
                                                   &verified),
               TURBO_OK);
  snapshot = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  check_int_eq(repository->auth->principal_snapshot(repository->ctx, "root-a", "device-a",
                                                    &verified, &snapshot),
               TURBO_OK);
  check_true(flowie_control_contract_contains(snapshot.effective_groups.groups[0],
                                              sizeof(snapshot.effective_groups.groups[0]),
                                              snapshot.effective_groups.group_count, "operators"));
  check_true(flowie_control_contract_contains(snapshot.effective_roles.roles[0],
                                              sizeof(snapshot.effective_roles.roles[0]),
                                              snapshot.effective_roles.role_count, "reader"));

  rule.domain_id = "root-a";
  rule.ordinal = 10u;
  rule.rule_line = policy_rule;
  rule.actor = "bootstrap";
  rule.request_id = "contract-rule";
  rule.expected_revision = 7u;
  rule.occurred_at = 1008u;
  check_int_eq(repository->policy->rule_put(repository->ctx, &rule, &result), TURBO_OK);
  check_int_eq(repository->policy->validate(repository->ctx, "root-a", &validation), TURBO_OK);
  check_uint_eq(validation.store_revision, 8u);
  check_size_eq(validation.rule_count, 2u);
  check_size_eq(validation.deny_rule_count, 0u);

  publish.domain_id = "root-a";
  publish.actor = "bootstrap";
  publish.request_id = "contract-publish";
  publish.expected_revision = 8u;
  publish.occurred_at = 1009u;
  publish.expires_at = 20000u;
  check_int_eq(repository->policy->publish(repository->ctx, &publish, &published), TURBO_OK);
  check_uint_eq(published.revision, 9u);
  check_uint_eq(published.policy_version, 1u);
  check_false(published.replayed);
  check_int_eq(repository->policy->status(repository->ctx, "root-a", &status), TURBO_OK);
  check_uint_eq(status.store_revision, 9u);
  check_uint_eq(status.policy_version, 1u);
  check_size_eq(status.draft_rule_count, 1u);
  check_size_eq(status.published_rule_count, 2u);

  check_int_eq(repository->policy->bundle_load(repository->ctx, "root-a", 1u, &bundle), TURBO_OK);
  check_uint_eq(bundle.policy_version, 1u);
  check_uint_eq(bundle.expires_at, 20000u);
  check_size_eq(bundle.rule_count, 2u);
  check_str_eq(bundle.rules[0].pattern, "");
  check_str_eq(bundle.rules[1].pattern, "root-a/groups/operators/devices/%u/event");
  repository->policy->bundle_release(repository->ctx, &bundle);
  bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  check_int_eq(repository->policy->bundle_load(repository->ctx, "root-a", 2u, &bundle),
               TURBO_ENOENT);

  publish.expected_revision = 0u;
  published = (flowie_control_policy_publish_result_t)FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  check_int_eq(repository->policy->publish(repository->ctx, &publish, &published), TURBO_OK);
  check_true(published.replayed);
  check_uint_eq(published.revision, 9u);
  check_uint_eq(published.policy_version, 1u);

  check_int_eq(repository->audit->revision(repository->ctx, &revision), TURBO_OK);
  check_uint_eq(revision, 9u);
  check_int_eq(repository->audit->count(repository->ctx, &audit_count), TURBO_OK);
  check_size_eq(audit_count, 9u);
  flowie_control_generated_credential_wipe(&credential);
}

#endif
