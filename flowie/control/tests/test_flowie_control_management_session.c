#include "flowie_control_management_session_internal.h"
#include "flowie_control_store_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define SESSION_TEST_PASSWORD_A "management-password-a"
#define SESSION_TEST_PASSWORD_B "management-password-b"

typedef struct management_session_fixture_s {
  char *database_path;
  flowie_control_store_t *store;
  flowie_control_auth_service_t *auth_service;
  flowie_control_management_session_store_t *sessions;
} management_session_fixture_t;

static uint64_t management_session_clock(void *ctx) {
  (void)ctx;
  return 10000u;
}

static int management_session_policy_version(void *ctx, const char *root_group_id,
                                             uint64_t *version_out) {
  (void)ctx;
  if (version_out) *version_out = 0u;
  if (!root_group_id || !version_out || strcmp(root_group_id, "root-a") != 0)
    return TURBO_EINVAL;
  *version_out = 1u;
  return TURBO_OK;
}

static management_session_fixture_t management_session_fixture_open(
    size_t capacity, size_t max_sessions_per_principal) {
  management_session_fixture_t fixture = {0};
  flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_auth_service_config_t auth_config = FLOWIE_CONTROL_AUTH_SERVICE_CONFIG_INIT;
  flowie_control_management_session_config_t session_config =
      FLOWIE_CONTROL_MANAGEMENT_SESSION_CONFIG_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  flowie_control_root_group_create_command_t root = FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_user_create_command_t user = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_credential_issue_command_t credential =
      FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  flowie_control_role_create_command_t role = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_user_role_add_command_t assignment = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  uint64_t revision = 0u;

  fixture.database_path = tt_make_temp_file("flowie-management-session", ".sqlite3");
  check_not_null(fixture.database_path);
  store_config.database_path = fixture.database_path;
  check_int_eq(flowie_control_store_open(&store_config, &fixture.store), TURBO_OK);

  root.root_group_id = "root-a";
  root.actor = "bootstrap";
  root.request_id = "session-root";
  root.expected_revision = revision;
  root.occurred_at = 1u;
  check_int_eq(flowie_control_store_root_group_create(fixture.store, &root, &result), TURBO_OK);
  revision = result.revision;

  user.root_group_id = "root-a";
  user.principal_id = "admin-a";
  user.principal_type = "operator";
  user.actor = "bootstrap";
  user.request_id = "session-user-a";
  user.expected_revision = revision;
  user.occurred_at = 2u;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  check_int_eq(flowie_control_store_user_create(fixture.store, &user, &result), TURBO_OK);
  revision = result.revision;

  credential.root_group_id = "root-a";
  credential.principal_id = "admin-a";
  credential.actor = "bootstrap";
  credential.request_id = "session-credential-a";
  credential.expected_revision = revision;
  credential.occurred_at = 3u;
  credential.initial_secret = SESSION_TEST_PASSWORD_A;
  credential.initial_secret_size = sizeof(SESSION_TEST_PASSWORD_A) - 1u;
  check_int_eq(flowie_control_store_credential_generate(fixture.store, &credential, &generated),
               TURBO_OK);
  revision = generated.revision;
  flowie_control_generated_credential_wipe(&generated);

  user.principal_id = "admin-b";
  user.request_id = "session-user-b";
  user.expected_revision = revision;
  user.occurred_at = 4u;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  check_int_eq(flowie_control_store_user_create(fixture.store, &user, &result), TURBO_OK);
  revision = result.revision;

  credential.principal_id = "admin-b";
  credential.request_id = "session-credential-b";
  credential.expected_revision = revision;
  credential.occurred_at = 5u;
  credential.initial_secret = SESSION_TEST_PASSWORD_B;
  credential.initial_secret_size = sizeof(SESSION_TEST_PASSWORD_B) - 1u;
  generated = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
  check_int_eq(flowie_control_store_credential_generate(fixture.store, &credential, &generated),
               TURBO_OK);
  revision = generated.revision;
  flowie_control_generated_credential_wipe(&generated);

  role.root_group_id = "root-a";
  role.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER;
  role.actor = "bootstrap";
  role.request_id = "session-role-viewer";
  role.expected_revision = revision;
  role.occurred_at = 6u;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  check_int_eq(flowie_control_store_role_create(fixture.store, &role, &result), TURBO_OK);
  revision = result.revision;

  assignment.root_group_id = "root-a";
  assignment.principal_id = "admin-a";
  assignment.role_id = FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER;
  assignment.actor = "bootstrap";
  assignment.request_id = "session-assignment-a";
  assignment.expected_revision = revision;
  assignment.occurred_at = 7u;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  check_int_eq(flowie_control_store_user_role_add(fixture.store, &assignment, &result), TURBO_OK);
  revision = result.revision;

  assignment.principal_id = "admin-b";
  assignment.request_id = "session-assignment-b";
  assignment.expected_revision = revision;
  assignment.occurred_at = 8u;
  result = (flowie_control_command_result_t)FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  check_int_eq(flowie_control_store_user_role_add(fixture.store, &assignment, &result), TURBO_OK);

  auth_config.repository = flowie_control_store_repository(fixture.store);
  auth_config.policy_version.current = management_session_policy_version;
  auth_config.clock_seconds = management_session_clock;
  check_int_eq(flowie_control_auth_service_create(&auth_config, &fixture.auth_service), TURBO_OK);

  session_config.repository = flowie_control_store_repository(fixture.store);
  session_config.auth_service = fixture.auth_service;
  session_config.capacity = capacity;
  session_config.max_sessions_per_principal = max_sessions_per_principal;
  session_config.ttl_seconds = 3600u;
  session_config.clock = management_session_clock;
  check_int_eq(flowie_control_management_session_store_create(&session_config, &fixture.sessions),
               TURBO_OK);
  return fixture;
}

static void management_session_fixture_close(management_session_fixture_t *fixture) {
  flowie_control_management_session_store_destroy(fixture->sessions);
  flowie_control_auth_service_destroy(fixture->auth_service);
  flowie_control_store_destroy(fixture->store);
  check_int_eq(tt_remove_file(fixture->database_path), 0);
  free(fixture->database_path);
  memset(fixture, 0, sizeof(*fixture));
}

static int management_session_login(flowie_control_management_session_store_t *sessions,
                                    const char *principal, const char *password,
                                    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE +
                                                   1u]) {
  return flowie_control_management_session_login(
      sessions, "root-a", principal, (const uint8_t *)password, strlen(password),
      "127.0.0.1:443", token_out);
}

spec("Flowie management sessions") {
  it("revokes only the principal's oldest issued session at its concurrency limit") {
    management_session_fixture_t fixture = management_session_fixture_open(32u, 5u);
    char admin_a_tokens[6u][FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {{0}};
    char admin_b_token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
    flowie_control_management_session_identity_t identity =
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;

    for (size_t index = 0u; index < 5u; ++index)
      check_int_eq(management_session_login(fixture.sessions, "admin-a", SESSION_TEST_PASSWORD_A,
                                            admin_a_tokens[index]),
                   TURBO_OK);

    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_a_tokens[0],
                                                            &identity),
                 TURBO_OK);
    check_int_eq(management_session_login(fixture.sessions, "admin-b", SESSION_TEST_PASSWORD_B,
                                          admin_b_token),
                 TURBO_OK);
    check_int_eq(management_session_login(fixture.sessions, "admin-a", SESSION_TEST_PASSWORD_A,
                                          admin_a_tokens[5]),
                 TURBO_OK);

    identity = (flowie_control_management_session_identity_t)
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_a_tokens[0],
                                                            &identity),
                 TURBO_EPERM);
    for (size_t index = 1u; index < 6u; ++index) {
      identity = (flowie_control_management_session_identity_t)
          FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
      check_int_eq(flowie_control_management_session_resolve(fixture.sessions,
                                                              admin_a_tokens[index], &identity),
                   TURBO_OK);
      check_str_eq(identity.principal_id, "admin-a");
    }
    identity = (flowie_control_management_session_identity_t)
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_b_token,
                                                            &identity),
                 TURBO_OK);
    check_str_eq(identity.principal_id, "admin-b");

    management_session_fixture_close(&fixture);
  }

  it("keeps global capacity as an independent LRU limit") {
    management_session_fixture_t fixture = management_session_fixture_open(2u, 5u);
    char admin_a_first[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
    char admin_a_second[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
    char admin_b_token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
    flowie_control_management_session_identity_t identity =
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;

    check_int_eq(management_session_login(fixture.sessions, "admin-a", SESSION_TEST_PASSWORD_A,
                                          admin_a_first),
                 TURBO_OK);
    check_int_eq(management_session_login(fixture.sessions, "admin-b", SESSION_TEST_PASSWORD_B,
                                          admin_b_token),
                 TURBO_OK);
    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_a_first,
                                                            &identity),
                 TURBO_OK);
    check_int_eq(management_session_login(fixture.sessions, "admin-a", SESSION_TEST_PASSWORD_A,
                                          admin_a_second),
                 TURBO_OK);

    identity = (flowie_control_management_session_identity_t)
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_b_token,
                                                            &identity),
                 TURBO_EPERM);
    identity = (flowie_control_management_session_identity_t)
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_a_first,
                                                            &identity),
                 TURBO_OK);
    identity = (flowie_control_management_session_identity_t)
        FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
    check_int_eq(flowie_control_management_session_resolve(fixture.sessions, admin_a_second,
                                                            &identity),
                 TURBO_OK);

    management_session_fixture_close(&fixture);
  }
}
