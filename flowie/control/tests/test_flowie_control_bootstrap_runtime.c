#include "flowie_control_runtime_internal.h"

#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>

static int bootstrap_runtime_set_env(const char *name, const char *value) {
#ifdef _WIN32
  return _putenv_s(name, value ? value : "");
#else
  return value ? setenv(name, value, 1) : unsetenv(name);
#endif
}

spec("Flowie controller bootstrap runtime") {
  it("bootstraps before creating login sessions and clears the password environment") {
    char cert_file[512] = {0};
    char key_file[512] = {0};
    char *database_path = tt_make_temp_file("flowie-control-runtime-bootstrap", ".sqlite3");
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    flowie_control_runtime_t *runtime = NULL;
    const char *remaining;

    check_not_null(database_path);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    check_int_eq(bootstrap_runtime_set_env("FLOWIE_RUNTIME_BOOTSTRAP_PASSWORD",
                                           "runtime-bootstrap-password"),
                 0);
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v1/management/rpc");
    config.dashboard_enabled = 0;
    (void)snprintf(config.listener.tls.cert_file, sizeof(config.listener.tls.cert_file), "%s",
                   cert_file);
    (void)snprintf(config.listener.tls.key_file, sizeof(config.listener.tls.key_file), "%s",
                   key_file);
    (void)snprintf(config.sqlite_path, sizeof(config.sqlite_path), "%s", database_path);
    config.bootstrap.enabled = 1;
    (void)snprintf(config.bootstrap.root_group_id, sizeof(config.bootstrap.root_group_id), "%s",
                   "root-a");
    (void)snprintf(config.bootstrap.principal_id, sizeof(config.bootstrap.principal_id), "%s",
                   "admin-a");
    (void)snprintf(config.bootstrap.principal_type, sizeof(config.bootstrap.principal_type), "%s",
                   "human");
    (void)snprintf(config.bootstrap.password_ref, sizeof(config.bootstrap.password_ref), "%s",
                   "env://FLOWIE_RUNTIME_BOOTSTRAP_PASSWORD");

    check_int_eq(flowie_control_runtime_create(&config, &runtime), TURBO_OK);
    check_not_null(runtime);
    remaining = getenv("FLOWIE_RUNTIME_BOOTSTRAP_PASSWORD");
    check_true(!remaining || !remaining[0]);
    check_int_eq(flowie_control_runtime_destroy(runtime), TURBO_OK);
    check_int_eq(tt_remove_file(database_path), 0);
    free(database_path);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }
}
