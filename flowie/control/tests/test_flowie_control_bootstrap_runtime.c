#include "flowie_control_runtime_internal.h"
#include "flowie_control_store_internal.h"

#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>

spec("Flowie controller bootstrap runtime") {
  it("initializes an empty store with the fixed system administrator credential") {
    char cert_file[512] = {0};
    char key_file[512] = {0};
    char *database_path = tt_make_temp_file("flowie-control-runtime-bootstrap", ".sqlite3");
    flowie_control_config_t config = FLOWIE_CONTROL_CONFIG_INIT;
    flowie_control_runtime_t *runtime = NULL;
    flowie_control_store_config_t store_config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
    flowie_control_store_t *store = NULL;
    flowie_control_credential_verify_result_t credential =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;

    check_not_null(database_path);
    check_int_eq(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)), 0);
    (void)snprintf(config.management.rpc_path, sizeof(config.management.rpc_path), "%s",
                   "/v2/control/rpc");
    config.dashboard_enabled = 0;
    (void)snprintf(config.listener.tls.cert_file, sizeof(config.listener.tls.cert_file), "%s",
                   cert_file);
    (void)snprintf(config.listener.tls.key_file, sizeof(config.listener.tls.key_file), "%s",
                   key_file);
    (void)snprintf(config.sqlite_path, sizeof(config.sqlite_path), "%s", database_path);

    check_int_eq(flowie_control_runtime_create(&config, &runtime), TURBO_OK);
    check_not_null(runtime);
    check_int_eq(flowie_control_runtime_destroy(runtime), TURBO_OK);
    store_config.database_path = database_path;
    check_int_eq(flowie_control_store_open(&store_config, &store), TURBO_OK);
    check_int_eq(flowie_control_store_repository(store)->auth->credential_verify(
                     flowie_control_store_repository(store)->ctx, "system", "admin",
                     FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD,
                     sizeof(FLOWIE_CONTROL_SYSTEM_ADMIN_INITIAL_PASSWORD) - 1u, &credential),
                 TURBO_OK);
    flowie_control_store_destroy(store);
    check_int_eq(tt_remove_file(database_path), 0);
    free(database_path);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }
}
