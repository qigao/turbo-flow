#include "flowie_control_runtime_internal.h"

#include "flowie_test_socket.h"
#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_FINGERPRINT "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

typedef struct control_runtime_fixture_s {
  char *path;
  flowie_control_store_t *store;
} control_runtime_fixture_t;

static int runtime_test_set_env(const char *name, const char *value) {
#ifdef _WIN32
  return _putenv_s(name, value ? value : "");
#else
  return value ? setenv(name, value, 1) : unsetenv(name);
#endif
}

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

static int runtime_role_add(flowie_control_store_t *store, const char *role, const char *request_id,
                            uint64_t revision) {
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
  it("resolves a logged-in principal to current reserved roles") {
    control_runtime_fixture_t fixture = runtime_fixture_open();
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;

    check_int_eq(flowie_control_management_identity_resolve_principal(
                     flowie_control_store_repository(fixture.store), "root-a", "admin-a", &caller),
                 TURBO_EPERM);
    check_int_eq(runtime_role_create(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER,
                                     "role-viewer", 2u),
                 TURBO_OK);
    check_int_eq(
        runtime_role_add(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER, "assign-viewer", 3u),
        TURBO_OK);
    check_int_eq(runtime_role_create(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN,
                                     "role-user-admin", 4u),
                 TURBO_OK);
    check_int_eq(runtime_role_add(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_USER_ADMIN,
                                  "assign-user-admin", 5u),
                 TURBO_OK);
    check_int_eq(flowie_control_management_identity_resolve_principal(
                     flowie_control_store_repository(fixture.store), "root-a", "admin-a", &caller),
                 TURBO_OK);
    check_str_eq(caller.root_group_id, "root-a");
    check_str_eq(caller.actor, "admin-a");
    check_uint_eq(caller.permissions,
                  FLOWIE_CONTROL_MANAGEMENT_VIEWER | FLOWIE_CONTROL_MANAGEMENT_USER_ADMIN);
    runtime_fixture_close(&fixture);
  }

  it("rejects principals that do not exist in the presented Root Group") {
    control_runtime_fixture_t fixture = runtime_fixture_open();
    flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;

    check_int_eq(flowie_control_management_identity_resolve_principal(
                     flowie_control_store_repository(fixture.store), "root-a", "missing-admin",
                     &caller),
                 TURBO_EPERM);
    check_null(caller.root_group_id);
    check_null(caller.actor);
    runtime_fixture_close(&fixture);
  }

  it("fails closed when TLS identity files cannot be validated") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    memcpy(config.management.rpc_path, "/v1/management/rpc", sizeof("/v1/management/rpc"));
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

  it("rejects incomplete local auth configuration") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    memcpy(config.management.rpc_path, "/v1/management/rpc", sizeof("/v1/management/rpc"));
    config.auth.enabled = 1;

    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EINVAL);
  }

  it("rejects invalid local executor bounds before TLS startup") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    memcpy(config.management.rpc_path, "/v1/management/rpc", sizeof("/v1/management/rpc"));
    config.auth.enabled = 1;
    config.auth.local_executor.workers = 0u;

    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EINVAL);
  }

  it("rejects simultaneous local executor and external HTTPS modes") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    memcpy(config.management.rpc_path, "/v1/management/rpc", sizeof("/v1/management/rpc"));
    config.auth.enabled = 1;
    config.auth.local_executor.configured = 1;
    config.auth.external_https.enabled = 1;

    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EINVAL);
  }

#if defined(FLOWIE_CONTROL_HAS_PGSQL)
  it("validates PostgreSQL credentials before TLS or database startup") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v1/management/rpc");
    config.store_provider = FLOWIE_CONTROL_CONFIG_STORE_POSTGRESQL;
    (void)snprintf(config.postgresql.conninfo, sizeof(config.postgresql.conninfo), "%s",
                   "host=db.internal dbname=flowie user=flowie password=literal "
                   "sslmode=verify-full");
    (void)snprintf(config.postgresql.password_ref, sizeof(config.postgresql.password_ref), "%s",
                   "env://FLOWIE_RUNTIME_PG_PASSWORD");
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_PG_PASSWORD", "secret"), 0);
    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EPERM);

    (void)snprintf(config.postgresql.conninfo, sizeof(config.postgresql.conninfo), "%s",
                   "host=db.internal dbname=flowie user=flowie sslmode=verify-full");
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_PG_PASSWORD", NULL), 0);
    check_int_eq(flowie_control_runtime_validate(&config), TURBO_ENOENT);

    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_PG_PASSWORD", "secret"), 0);
    (void)snprintf(config.listener.tls.cert_file, sizeof(config.listener.tls.cert_file), "%s",
                   "missing-control-cert.pem");
    (void)snprintf(config.listener.tls.key_file, sizeof(config.listener.tls.key_file), "%s",
                   "missing-control-key.pem");
    (void)snprintf(config.listener.tls.client_ca_file, sizeof(config.listener.tls.client_ca_file),
                   "%s", "missing-control-ca.pem");
    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EIO);
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_PG_PASSWORD", NULL), 0);
  }
#else
  it("rejects PostgreSQL selection when the provider is not built") {
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v1/management/rpc");
    config.store_provider = FLOWIE_CONTROL_CONFIG_STORE_POSTGRESQL;
    check_int_eq(flowie_control_runtime_validate(&config), TURBO_ENOTSUP);
  }
#endif

  it("validates the configured external HTTPS identity and secrets before startup") {
    char cert_file[512] = {0};
    char key_file[512] = {0};
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;

    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_EXTERNAL_TOKEN", "service-token"), 0);
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v1/management/rpc");
    (void)snprintf(config.listener.tls.cert_file, sizeof(config.listener.tls.cert_file), "%s",
                   cert_file);
    (void)snprintf(config.listener.tls.key_file, sizeof(config.listener.tls.key_file), "%s",
                   key_file);
    (void)snprintf(config.listener.tls.client_ca_file, sizeof(config.listener.tls.client_ca_file),
                   "%s", cert_file);
    config.auth.external_https.enabled = 1;
    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EINVAL);
    config.auth.enabled = 1;
    (void)snprintf(config.auth.method, sizeof(config.auth.method), "%s", "bearer");
    (void)snprintf(config.auth.external_https.url, sizeof(config.auth.external_https.url), "%s",
                   "https://localhost/v1/assert");
    (void)snprintf(config.auth.external_https.service_token_ref,
                   sizeof(config.auth.external_https.service_token_ref), "%s",
                   "env://FLOWIE_RUNTIME_EXTERNAL_TOKEN");
    (void)snprintf(config.auth.external_https.trusted_issuer,
                   sizeof(config.auth.external_https.trusted_issuer), "%s",
                   "https://identity.example");
    (void)snprintf(config.auth.external_https.subject_type,
                   sizeof(config.auth.external_https.subject_type), "%s", "device");
    (void)snprintf(config.auth.external_https.tls.ca_file,
                   sizeof(config.auth.external_https.tls.ca_file), "%s", cert_file);
    (void)snprintf(config.auth.external_https.tls.client_cert_file,
                   sizeof(config.auth.external_https.tls.client_cert_file), "%s", cert_file);
    (void)snprintf(config.auth.external_https.tls.client_key_file,
                   sizeof(config.auth.external_https.tls.client_key_file), "%s", key_file);

    check_int_eq(flowie_control_runtime_validate(&config), TURBO_OK);
    (void)snprintf(config.auth.external_https.tls.ca_file,
                   sizeof(config.auth.external_https.tls.ca_file), "%s", "missing-external-ca.pem");
    check_int_eq(flowie_control_runtime_validate(&config), TURBO_EIO);

    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_EXTERNAL_TOKEN", NULL), 0);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }

  it("owns the Dashboard and external HTTPS provider for the complete runtime lifecycle") {
    char cert_file[512] = {0};
    char key_file[512] = {0};
    control_runtime_fixture_t fixture = runtime_fixture_open();
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    flowie_control_runtime_t *runtime = NULL;

    check_int_eq(runtime_role_create(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER,
                                     "runtime-viewer", 2u),
                 TURBO_OK);
    check_int_eq(runtime_role_add(fixture.store, FLOWIE_CONTROL_MANAGEMENT_ROLE_VIEWER,
                                  "runtime-viewer-add", 3u),
                 TURBO_OK);
    flowie_control_store_destroy(fixture.store);
    fixture.store = NULL;
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_AUTH_TOKEN", "inbound-token"), 0);
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_EXTERNAL_TOKEN", "outbound-token"), 0);
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v1/management/rpc");
    config.dashboard_enabled = 1;
    (void)snprintf(config.listener.tls.cert_file, sizeof(config.listener.tls.cert_file), "%s",
                   cert_file);
    (void)snprintf(config.listener.tls.key_file, sizeof(config.listener.tls.key_file), "%s",
                   key_file);
    (void)snprintf(config.sqlite_path, sizeof(config.sqlite_path), "%s", fixture.path);
    config.auth.enabled = 1;
    (void)snprintf(config.auth.listener_id, sizeof(config.auth.listener_id), "%s",
                   "flowie-control-auth");
    (void)snprintf(config.auth.method, sizeof(config.auth.method), "%s", "bearer");
    config.auth.service_binding_count = 1u;
    (void)snprintf(config.auth.service_bindings[0].service_id,
                   sizeof(config.auth.service_bindings[0].service_id), "%s", "broker-main");
    (void)snprintf(config.auth.service_bindings[0].token_ref,
                   sizeof(config.auth.service_bindings[0].token_ref), "%s",
                   "env://FLOWIE_RUNTIME_AUTH_TOKEN");
    (void)snprintf(config.auth.service_bindings[0].root_group_id,
                   sizeof(config.auth.service_bindings[0].root_group_id), "%s", "root-a");
    config.auth.external_https.enabled = 1;
    (void)snprintf(config.auth.external_https.url, sizeof(config.auth.external_https.url), "%s",
                   "https://localhost/v1/assert");
    (void)snprintf(config.auth.external_https.service_token_ref,
                   sizeof(config.auth.external_https.service_token_ref), "%s",
                   "env://FLOWIE_RUNTIME_EXTERNAL_TOKEN");
    (void)snprintf(config.auth.external_https.trusted_issuer,
                   sizeof(config.auth.external_https.trusted_issuer), "%s",
                   "https://identity.example");
    (void)snprintf(config.auth.external_https.subject_type,
                   sizeof(config.auth.external_https.subject_type), "%s", "device");
    (void)snprintf(config.auth.external_https.tls.ca_file,
                   sizeof(config.auth.external_https.tls.ca_file), "%s", cert_file);
    (void)snprintf(config.auth.external_https.tls.client_cert_file,
                   sizeof(config.auth.external_https.tls.client_cert_file), "%s", cert_file);
    (void)snprintf(config.auth.external_https.tls.client_key_file,
                   sizeof(config.auth.external_https.tls.client_key_file), "%s", key_file);

    check_int_eq(flowie_control_runtime_create(&config, &runtime), TURBO_OK);
    check_not_null(runtime);
    check_int_eq(flowie_control_runtime_destroy(runtime), TURBO_OK);

    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_EXTERNAL_TOKEN", NULL), 0);
    check_int_eq(runtime_test_set_env("FLOWIE_RUNTIME_AUTH_TOKEN", NULL), 0);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    runtime_fixture_close(&fixture);
  }

  it("starts and stops an owned HTTPS listener without process signal handlers") {
    char cert_file[512] = {0};
    char key_file[512] = {0};
    control_runtime_fixture_t fixture = runtime_fixture_open();
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    flowie_control_runtime_t *runtime = NULL;

    flowie_control_store_destroy(fixture.store);
    fixture.store = NULL;
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    (void)snprintf(config.listener.host, sizeof(config.listener.host), "%s", "127.0.0.1");
    config.listener.port = flowie_test_port();
    check_true(config.listener.port != 0u);
    (void)snprintf(config.listener.tls.cert_file, sizeof(config.listener.tls.cert_file), "%s",
                   cert_file);
    (void)snprintf(config.listener.tls.key_file, sizeof(config.listener.tls.key_file), "%s",
                   key_file);
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v1/management/rpc");
    (void)snprintf(config.sqlite_path, sizeof(config.sqlite_path), "%s", fixture.path);

    check_int_eq(flowie_control_runtime_create(&config, &runtime), TURBO_OK);
    check_not_null(runtime);
    check_int_eq(flowie_control_runtime_start(runtime), TURBO_OK);
    check_int_eq(flowie_control_runtime_start(runtime), TURBO_EINVAL);
    check_int_eq(flowie_control_runtime_stop(runtime), TURBO_OK);
    check_int_eq(flowie_control_runtime_stop(runtime), TURBO_OK);
    check_int_eq(flowie_control_runtime_destroy(runtime), TURBO_OK);

    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    runtime_fixture_close(&fixture);
  }
}
