#include "flowie_control_credential_internal.h"
#include "flowie_control_service_credential_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct service_credential_fixture_s {
  char *database_path;
  flowie_control_store_t *store;
  flowie_control_service_credential_resolver_t *resolver;
  flowie_control_generated_credential_t credential;
  uint64_t revision;
} service_credential_fixture_t;

static void service_credential_add_role(service_credential_fixture_t *fixture,
                                        const char *role_id, const char *request_suffix) {
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  char role_request[64];
  char assignment_request[64];

  check_int_gt(snprintf(role_request, sizeof(role_request), "service-role-%s", request_suffix), 0);
  check_int_gt(snprintf(assignment_request, sizeof(assignment_request),
                        "service-assignment-%s", request_suffix),
               0);
  role.domain_id = "root-a";
  role.role_id = role_id;
  role.actor = "bootstrap";
  role.request_id = role_request;
  role.expected_revision = fixture->revision;
  role.occurred_at = 2000u + fixture->revision;
  check_int_eq(flowie_control_store_role_create(fixture->store, &role, &result), TURBO_OK);
  fixture->revision = result.revision;

  assignment.domain_id = "root-a";
  assignment.principal_id = "broker-a";
  assignment.role_id = role_id;
  assignment.actor = "bootstrap";
  assignment.request_id = assignment_request;
  assignment.expected_revision = fixture->revision;
  assignment.occurred_at = 3000u + fixture->revision;
  check_int_eq(flowie_control_store_user_role_add(fixture->store, &assignment, &result), TURBO_OK);
  fixture->revision = result.revision;
}

static service_credential_fixture_t service_credential_fixture_open(uint32_t permissions) {
  service_credential_fixture_t fixture = {0};
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_service_credential_config_t resolver_config =
      FLOWIE_CONTROL_SERVICE_CREDENTIAL_CONFIG_INIT;
  flowie_control_domain_create_command_t domain = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_credential_issue_command_t issue = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;

  fixture.credential =
      (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  fixture.database_path = tt_make_temp_file("flowie-service-credential", ".sqlite3");
  check_not_null(fixture.database_path);
  store_config.database_path = fixture.database_path;
  check_int_eq(flowie_control_store_open(&store_config, &fixture.store), TURBO_OK);
  check_not_null(fixture.store);

  domain.domain_id = "root-a";
  domain.actor = "bootstrap";
  domain.request_id = "service-domain-root-a";
  domain.occurred_at = 1000u;
  check_int_eq(flowie_control_store_domain_create(fixture.store, &domain, &result), TURBO_OK);
  fixture.revision = result.revision;

  user.domain_id = "root-a";
  user.principal_id = "broker-a";
  user.principal_type = "service";
  user.actor = "bootstrap";
  user.request_id = "service-user-broker-a";
  user.expected_revision = fixture.revision;
  user.occurred_at = 1001u;
  check_int_eq(flowie_control_store_user_create(fixture.store, &user, &result), TURBO_OK);
  fixture.revision = result.revision;

  issue.domain_id = "root-a";
  issue.principal_id = "broker-a";
  issue.actor = "bootstrap";
  issue.request_id = "service-credential-broker-a";
  issue.expected_revision = fixture.revision;
  issue.occurred_at = 1002u;
  check_int_eq(flowie_control_store_credential_generate(fixture.store, &issue,
                                                        &fixture.credential),
               TURBO_OK);
  fixture.revision = fixture.credential.revision;

  if ((permissions & FLOWIE_CONTROL_SERVICE_AUTHENTICATE) != 0u)
    service_credential_add_role(&fixture, FLOWIE_CONTROL_SERVICE_ROLE_AUTH_CLIENT, "auth");
  if ((permissions & FLOWIE_CONTROL_SERVICE_ACL_CHECK) != 0u)
    service_credential_add_role(&fixture, FLOWIE_CONTROL_SERVICE_ROLE_ACL_CLIENT, "acl");

  resolver_config.listener_id = "flowie-control-auth";
  resolver_config.repository = flowie_control_store_repository(fixture.store);
  check_int_eq(flowie_control_service_credential_resolver_create(&resolver_config,
                                                                 &fixture.resolver),
               TURBO_OK);
  check_not_null(fixture.resolver);
  return fixture;
}

static void service_credential_fixture_close(service_credential_fixture_t *fixture) {
  flowie_control_service_credential_resolver_destroy(fixture->resolver);
  flowie_control_generated_credential_wipe(&fixture->credential);
  flowie_control_store_destroy(fixture->store);
  check_int_eq(tt_remove_file(fixture->database_path), 0);
  free(fixture->database_path);
  memset(fixture, 0, sizeof(*fixture));
}

static int service_credential_resolve(service_credential_fixture_t *fixture,
                                      const char *service_domain, const char *service_id,
                                      const char *token, size_t token_size,
                                      uint32_t required_permission,
                                      flowie_control_verified_caller_t *caller) {
  return flowie_control_service_credential_resolve(
      fixture->resolver, service_domain, service_id, (const uint8_t *)token, token_size,
      required_permission, caller);
}

spec("Flowie repository-backed service credentials") {
  it("resolves one service with its Domain and endpoint roles") {
    service_credential_fixture_t fixture = service_credential_fixture_open(
        FLOWIE_CONTROL_SERVICE_AUTHENTICATE | FLOWIE_CONTROL_SERVICE_ACL_CHECK);
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    check_int_eq(service_credential_resolve(
                     &fixture, "root-a", "broker-a", fixture.credential.token,
                     fixture.credential.token_size,
                     FLOWIE_CONTROL_SERVICE_AUTHENTICATE | FLOWIE_CONTROL_SERVICE_ACL_CHECK,
                     &caller),
                 TURBO_OK);
    check_str_eq(caller.listener_id, "flowie-control-auth");
    check_str_eq(caller.service_id, "broker-a");
    check_str_eq(caller.domain_id, "root-a");
    check_true(caller.authenticated);
    check_bits(caller.permissions, FLOWIE_CONTROL_SERVICE_AUTHENTICATE |
                                       FLOWIE_CONTROL_SERVICE_ACL_CHECK);

    service_credential_fixture_close(&fixture);
  }

  it("rejects an incorrect token or public service selector") {
    service_credential_fixture_t fixture =
        service_credential_fixture_open(FLOWIE_CONTROL_SERVICE_AUTHENTICATE);
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    check_int_eq(service_credential_resolve(&fixture, "root-a", "broker-a", "wrong-token",
                                            sizeof("wrong-token") - 1u,
                                            FLOWIE_CONTROL_SERVICE_AUTHENTICATE, &caller),
                 TURBO_EPERM);
    check_false(caller.authenticated);
    check_int_eq(service_credential_resolve(
                     &fixture, "root-b", "broker-a", fixture.credential.token,
                     fixture.credential.token_size, FLOWIE_CONTROL_SERVICE_AUTHENTICATE, &caller),
                 TURBO_EPERM);
    check_int_eq(service_credential_resolve(
                     &fixture, "root-a", "broker-b", fixture.credential.token,
                     fixture.credential.token_size, FLOWIE_CONTROL_SERVICE_AUTHENTICATE, &caller),
                 TURBO_EPERM);

    service_credential_fixture_close(&fixture);
  }

  it("requires the role assigned to the requested endpoint") {
    service_credential_fixture_t fixture =
        service_credential_fixture_open(FLOWIE_CONTROL_SERVICE_AUTHENTICATE);
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    check_int_eq(service_credential_resolve(
                     &fixture, "root-a", "broker-a", fixture.credential.token,
                     fixture.credential.token_size, FLOWIE_CONTROL_SERVICE_AUTHENTICATE, &caller),
                 TURBO_OK);
    caller = (flowie_control_verified_caller_t)FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
    check_int_eq(service_credential_resolve(
                     &fixture, "root-a", "broker-a", fixture.credential.token,
                     fixture.credential.token_size, FLOWIE_CONTROL_SERVICE_ACL_CHECK, &caller),
                 TURBO_EPERM);
    check_false(caller.authenticated);

    service_credential_fixture_close(&fixture);
  }

  it("rejects a revoked service credential") {
    service_credential_fixture_t fixture =
        service_credential_fixture_open(FLOWIE_CONTROL_SERVICE_ACL_CHECK);
    flowie_control_credential_revoke_command_t revoke =
        FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
    flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
    flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;

    revoke.domain_id = "root-a";
    revoke.principal_id = "broker-a";
    revoke.actor = "bootstrap";
    revoke.request_id = "service-credential-revoke";
    revoke.expected_revision = fixture.revision;
    revoke.occurred_at = 4000u;
    check_int_eq(flowie_control_store_credential_revoke(fixture.store, &revoke, &result), TURBO_OK);
    check_int_eq(service_credential_resolve(
                     &fixture, "root-a", "broker-a", fixture.credential.token,
                     fixture.credential.token_size, FLOWIE_CONTROL_SERVICE_ACL_CHECK, &caller),
                 TURBO_EPERM);
    check_false(caller.authenticated);

    service_credential_fixture_close(&fixture);
  }
}
