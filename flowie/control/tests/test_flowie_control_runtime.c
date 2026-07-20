#include "flowie_control_runtime_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

#define TEST_FINGERPRINT                                                                          \
  "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

typedef struct control_runtime_fixture_s {
  char *path;
  flowie_control_store_t *store;
} control_runtime_fixture_t;

static int runtime_root_create(flowie_control_store_t *store, uint64_t revision) {
  flowie_control_root_group_create_command_t command =
      FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.actor = "bootstrap";
  command.request_id = "root-create";
  command.expected_revision = revision;
  command.occurred_at = 1000u;
  return flowie_control_store_root_group_create(store, &command, &result);
}

static int runtime_user_create(flowie_control_store_t *store, uint64_t revision) {
  flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "admin-a";
  command.principal_type = "operator";
  command.actor = "bootstrap";
  command.request_id = "user-create";
  command.expected_revision = revision;
  command.occurred_at = 1001u;
  return flowie_control_store_user_create(store, &command, &result);
}

static int runtime_role_create(flowie_control_store_t *store, const char *role,
                               const char *request_id, uint64_t revision) {
  flowie_control_role_create_command_t command = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.role_id = role;
  command.actor = "bootstrap";
  command.request_id = request_id;
  command.expected_revision = revision;
  command.occurred_at = 1000u + revision;
  return flowie_control_store_role_create(store, &command, &result);
}

static int runtime_role_add(flowie_control_store_t *store, const char *role,
                            const char *request_id, uint64_t revision) {
  flowie_control_user_role_add_command_t command = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = "admin-a";
  command.role_id = role;
  command.actor = "bootstrap";
  command.request_id = request_id;
  command.expected_revision = revision;
  command.occurred_at = 1000u + revision;
  return flowie_control_store_user_role_add(store, &command, &result);
}

static control_runtime_fixture_t runtime_fixture_open(void) {
  control_runtime_fixture_t fixture = {0};
  flowie_control_store_config_t config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  fixture.path = tt_make_temp_file("flowie-control-runtime", ".sqlite3");
  check_not_null(fixture.path);
  config.database_path = fixture.path;
  check_int_eq(flowie_control_store_open(&config, &fixture.store), TURBO_OK);
  check_int_eq(runtime_root_create(fixture.store, 0u), TURBO_OK);
  check_int_eq(runtime_user_create(fixture.store, 1u), TURBO_OK);
  return fixture;
}

static void runtime_fixture_close(control_runtime_fixture_t *fixture) {
  flowie_control_store_destroy(fixture->store);
  check_int_eq(tt_remove_file(fixture->path), 0);
  free(fixture->path);
  memset(fixture, 0, sizeof(*fixture));
}

spec("Flowie controller runtime") {
  it("maps only configured certificate identities to current reserved roles") {
    control_runtime_fixture_t fixture = runtime_fixture_open();
    flowie_control_config_admin_binding_t binding = {TEST_FINGERPRINT, "root-a", "admin-a"};
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;

    check_int_eq(flowie_control_management_identity_resolve(
                     fixture.store, &binding, 1u, TEST_FINGERPRINT, &caller),
                 TURBO_EPERM);
    check_int_eq(runtime_role_create(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER,
                                     "role-viewer", 2u),
                 TURBO_OK);
    check_int_eq(runtime_role_add(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER,
                                  "assign-viewer", 3u),
                 TURBO_OK);
    check_int_eq(runtime_role_create(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN,
                                     "role-user-admin", 4u),
                 TURBO_OK);
    check_int_eq(runtime_role_add(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN,
                                  "assign-user-admin", 5u),
                 TURBO_OK);
    check_int_eq(flowie_control_management_identity_resolve(
                     fixture.store, &binding, 1u, TEST_FINGERPRINT, &caller),
                 TURBO_OK);
    check_str_eq(caller.root_group_id, "root-a");
    check_str_eq(caller.actor, "admin-a");
    check_uint_eq(caller.permissions,
                  FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN);
    runtime_fixture_close(&fixture);
  }

  it("rejects fingerprints that are not explicitly bound") {
    control_runtime_fixture_t fixture = runtime_fixture_open();
    flowie_control_config_admin_binding_t binding = {TEST_FINGERPRINT, "root-a", "admin-a"};
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;

    check_int_eq(flowie_control_management_identity_resolve(
                     fixture.store, &binding, 1u,
                     "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                     &caller),
                 TURBO_EPERM);
    check_null(caller.root_group_id);
    check_null(caller.actor);
    runtime_fixture_close(&fixture);
  }

  it("fails closed when TLS identity files cannot be validated") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    memcpy(config.management.rpc_path, "/v1/management/rpc",
           sizeof("/v1/management/rpc"));
    memcpy(config.listener.tls.cert_file, "missing-control-cert.pem",
           sizeof("missing-control-cert.pem"));
    memcpy(config.listener.tls.key_file, "missing-control-key.pem",
           sizeof("missing-control-key.pem"));
    memcpy(config.listener.tls.client_ca_file, "missing-control-ca.pem",
           sizeof("missing-control-ca.pem"));

    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EIO);
  }

  it("rejects RPC routes that collide with fixed Dashboard routes") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    memcpy(config.management.rpc_path, "/v1/management/dashboard",
           sizeof("/v1/management/dashboard"));

    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EINVAL);
  }
}
