#ifndef FLOWIE_CONTROL_MANAGEMENT_REPOSITORY_CONTRACT_H
#define FLOWIE_CONTROL_MANAGEMENT_REPOSITORY_CONTRACT_H

#include "flowie_control_management_service_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

static int flowie_control_management_contract_contains(const char *items, size_t item_size,
                                                       uint32_t count, const char *expected) {
  if (!items || item_size == 0u || !expected) return 0;
  for (uint32_t index = 0u; index < count; ++index) {
    if (strcmp(items + index * item_size, expected) == 0) return 1;
  }
  return 0;
}

/**
 * Exercise the production management service through one new, empty Repository provider.
 *
 * Domain bootstrap intentionally uses the Repository directly because bootstrap is not an
 * authenticated management operation. Every subsequent account, Group, Role and ACL operation
 * crosses the same service boundary used by JSON-RPC.
 */
static void
flowie_control_management_repository_contract_run(const flowie_control_repository_t *repository) {
  static const char policy_rule[] =
      "allow|role|operator|root-a|subscribe|mqtt_topic|adapter|root-a/events/#";
  flowie_control_domain_create_command_t root = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_management_service_config_t config = FLOWIE_CONTROL_MANAGEMENT_SERVICE_CONFIG_INIT;
  flowie_control_management_service_t *service = NULL;
  flowie_control_management_caller_t viewer = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_management_caller_t user_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_management_caller_t policy_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_management_caller_t security_admin = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_group_create_command_t group = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
  flowie_control_membership_add_command_t membership = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_policy_rule_put_command_t rule = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
  flowie_control_policy_publish_command_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_policy_publish_result_t published = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
  flowie_control_management_status_t management_status = FLOWIE_CONTROL_MANAGEMENT_STATUS_INIT;
  flowie_control_user_view_t user_view = FLOWIE_CONTROL_USER_VIEW_INIT;
  flowie_control_user_view_t user_page[1] = {FLOWIE_CONTROL_USER_VIEW_INIT};
  flowie_control_group_view_t group_page[4] = {
      FLOWIE_CONTROL_GROUP_VIEW_INIT, FLOWIE_CONTROL_GROUP_VIEW_INIT,
      FLOWIE_CONTROL_GROUP_VIEW_INIT, FLOWIE_CONTROL_GROUP_VIEW_INIT};
  flowie_control_role_view_t role_page[2] = {FLOWIE_CONTROL_ROLE_VIEW_INIT,
                                             FLOWIE_CONTROL_ROLE_VIEW_INIT};
  flowie_control_effective_groups_view_t effective_groups =
      FLOWIE_CONTROL_EFFECTIVE_GROUPS_VIEW_INIT;
  flowie_control_effective_roles_view_t effective_roles = FLOWIE_CONTROL_EFFECTIVE_ROLES_VIEW_INIT;
  flowie_control_policy_rule_view_t rule_page[2] = {FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT,
                                                    FLOWIE_CONTROL_POLICY_RULE_VIEW_INIT};
  flowie_control_policy_validation_t validation = FLOWIE_CONTROL_POLICY_VALIDATION_INIT;
  flowie_control_policy_status_t policy_status = FLOWIE_CONTROL_POLICY_STATUS_INIT;
  flowie_control_audit_view_t audit_page[16] = {
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT,
      FLOWIE_CONTROL_AUDIT_VIEW_INIT, FLOWIE_CONTROL_AUDIT_VIEW_INIT};
  size_t count = 0u;
  int has_more = 0;

  check_not_null(repository);
  check_int_eq(flowie_control_repository_validate(repository), TURBO_OK);

  root.domain_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "management-contract-root";
  root.occurred_at = 1000u;
  check_int_eq(repository->user->domain_create(repository->ctx, &root, &result), TURBO_OK);

  config.repository = repository;
  check_int_eq(flowie_control_management_service_create(&config, &service), TURBO_OK);
  check_not_null(service);

  viewer.domain_id = "root-a";
  viewer.actor = "viewer-a";
  viewer.permissions = FLOWIE_CONTROL_MANAGEMENT_VIEWER;
  user_admin.domain_id = "root-a";
  user_admin.actor = "user-admin-a";
  user_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN;
  policy_admin.domain_id = "root-a";
  policy_admin.actor = "policy-admin-a";
  policy_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_POLICY_ADMIN;
  security_admin.domain_id = "root-a";
  security_admin.actor = "security-admin-a";
  security_admin.permissions = FLOWIE_CONTROL_MANAGEMENT_SECURITY_ADMIN;

  user.domain_id = "root-a";
  user.principal_id = "device-a";
  user.principal_type = "device";
  user.actor = viewer.actor;
  user.request_id = "management-contract-user-denied";
  user.expected_revision = 1u;
  user.occurred_at = 1001u;
  check_int_eq(flowie_control_management_user_create(service, &viewer, &user, &result),
               TURBO_EPERM);

  user.actor = user_admin.actor;
  user.request_id = "management-contract-user-a";
  check_int_eq(flowie_control_management_user_create(service, &user_admin, &user, &result),
               TURBO_OK);
  check_uint_eq(result.revision, 2u);
  user.principal_id = "device-b";
  user.request_id = "management-contract-user-b";
  user.expected_revision = 2u;
  user.occurred_at = 1002u;
  check_int_eq(flowie_control_management_user_create(service, &user_admin, &user, &result),
               TURBO_OK);
  check_uint_eq(result.revision, 3u);

  check_int_eq(flowie_control_management_user_get(service, &viewer, "device-a", &user_view),
               TURBO_OK);
  check_str_eq(user_view.principal_id, "device-a");
  check_true(user_view.enabled);
  check_int_eq(
      flowie_control_management_user_list(service, &viewer, NULL, user_page, 1u, &count, &has_more),
      TURBO_OK);
  check_size_eq(count, 1u);
  check_true(has_more);
  check_str_eq(user_page[0].principal_id, "device-a");
  user_page[0] = (flowie_control_user_view_t)FLOWIE_CONTROL_USER_VIEW_INIT;
  check_int_eq(flowie_control_management_user_list(service, &viewer, "device-a", user_page, 1u,
                                                   &count, &has_more),
               TURBO_OK);
  check_size_eq(count, 1u);
  check_false(has_more);
  check_str_eq(user_page[0].principal_id, "device-b");

  group.domain_id = "root-a";
  group.group_id = "operators";
  group.parent_group_id = NULL;
  group.actor = user_admin.actor;
  group.request_id = "management-contract-group";
  group.expected_revision = 3u;
  group.occurred_at = 1003u;
  check_int_eq(flowie_control_management_group_create(service, &user_admin, &group, &result),
               TURBO_OK);
  check_uint_eq(result.revision, 4u);
  check_int_eq(flowie_control_management_group_list(service, &viewer, NULL, group_page, 4u, &count,
                                                    &has_more),
               TURBO_OK);
  check_size_eq(count, 1u);
  check_false(has_more);

  membership.domain_id = "root-a";
  membership.principal_id = "device-a";
  membership.group_id = "operators";
  membership.actor = user_admin.actor;
  membership.request_id = "management-contract-membership";
  membership.expected_revision = 4u;
  membership.occurred_at = 1004u;
  check_int_eq(flowie_control_management_membership_add(service, &user_admin, &membership, &result),
               TURBO_OK);
  check_uint_eq(result.revision, 5u);
  check_int_eq(
      flowie_control_management_effective_groups(service, &viewer, "device-a", &effective_groups),
      TURBO_OK);
  check_true(flowie_control_management_contract_contains(
      effective_groups.groups[0], sizeof(effective_groups.groups[0]), effective_groups.group_count,
      "operators"));

  role.domain_id = "root-a";
  role.role_id = "operator";
  role.actor = security_admin.actor;
  role.request_id = "management-contract-role";
  role.expected_revision = 5u;
  role.occurred_at = 1005u;
  check_int_eq(flowie_control_management_role_create(service, &security_admin, &role, &result),
               TURBO_OK);
  check_uint_eq(result.revision, 6u);
  check_int_eq(
      flowie_control_management_role_list(service, &viewer, NULL, role_page, 2u, &count, &has_more),
      TURBO_OK);
  check_size_eq(count, 1u);
  check_false(has_more);
  check_str_eq(role_page[0].role_id, "operator");

  assignment.domain_id = "root-a";
  assignment.principal_id = "device-a";
  assignment.role_id = "operator";
  assignment.actor = security_admin.actor;
  assignment.request_id = "management-contract-assignment";
  assignment.expected_revision = 6u;
  assignment.occurred_at = 1006u;
  check_int_eq(
      flowie_control_management_user_role_add(service, &security_admin, &assignment, &result),
      TURBO_OK);
  check_uint_eq(result.revision, 7u);
  check_int_eq(
      flowie_control_management_effective_roles(service, &viewer, "device-a", &effective_roles),
      TURBO_OK);
  check_uint_eq(effective_roles.role_count, 1u);
  check_str_eq(effective_roles.roles[0], "operator");

  rule.domain_id = "root-a";
  rule.ordinal = 10u;
  rule.rule_line = policy_rule;
  rule.actor = policy_admin.actor;
  rule.request_id = "management-contract-rule";
  rule.expected_revision = 7u;
  rule.occurred_at = 1007u;
  check_int_eq(flowie_control_management_policy_rule_put(service, &viewer, &rule, &result),
               TURBO_EPERM);
  check_int_eq(flowie_control_management_policy_rule_put(service, &policy_admin, &rule, &result),
               TURBO_OK);
  check_uint_eq(result.revision, 8u);
  check_int_eq(flowie_control_management_policy_rule_list(service, &viewer, 0u, 0, rule_page, 2u,
                                                          &count, &has_more),
               TURBO_OK);
  check_size_eq(count, 1u);
  check_false(has_more);
  check_uint_eq(rule_page[0].ordinal, 10u);
  check_str_eq(rule_page[0].rule_line, policy_rule);
  check_int_eq(flowie_control_management_policy_validate(service, &viewer, &validation), TURBO_OK);
  check_uint_eq(validation.store_revision, 8u);
  check_size_eq(validation.rule_count, 1u);

  publish.domain_id = "root-a";
  publish.actor = policy_admin.actor;
  publish.request_id = "management-contract-publish";
  publish.expected_revision = 8u;
  publish.occurred_at = 1008u;
  publish.expires_at = 20000u;
  check_int_eq(
      flowie_control_management_policy_publish(service, &policy_admin, &publish, &published),
      TURBO_OK);
  check_uint_eq(published.revision, 9u);
  check_uint_eq(published.policy_version, 1u);
  check_int_eq(flowie_control_management_policy_status(service, &viewer, &policy_status), TURBO_OK);
  check_uint_eq(policy_status.store_revision, 9u);
  check_uint_eq(policy_status.policy_version, 1u);
  check_size_eq(policy_status.published_rule_count, 1u);
  check_int_eq(flowie_control_management_system_status(service, &viewer, &management_status),
               TURBO_OK);
  check_uint_eq(management_status.store_revision, 9u);
  check_uint_eq(management_status.policy.policy_version, 1u);

  check_int_eq(flowie_control_management_audit_list(service, &viewer, 0u, audit_page, 16u, &count,
                                                    &has_more),
               TURBO_EPERM);
  check_int_eq(flowie_control_management_audit_list(service, &security_admin, 0u, audit_page, 3u,
                                                    &count, &has_more),
               TURBO_OK);
  check_size_eq(count, 3u);
  check_true(has_more);
  check_uint_eq(audit_page[0].revision, 1u);
  check_uint_eq(audit_page[2].revision, 3u);
  check_int_eq(flowie_control_management_audit_list(service, &security_admin, 3u, audit_page, 16u,
                                                    &count, &has_more),
               TURBO_OK);
  check_size_eq(count, 6u);
  check_false(has_more);
  check_uint_eq(audit_page[5].revision, 9u);
  check_str_eq(audit_page[5].operation, "policy.publish");

  flowie_control_management_service_destroy(service);
}

#endif
