#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_operation.h"
#include <cmeta/type_select.h>
#include <tinytest.h>

static turbo_flow_plugin_host_config_t schema_config(size_t capacity) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  config.module_capacity = 8u;
  config.schema_capacity = capacity;
  return config;
}

spec("plugin schema catalog") {
  it("loads one semantic CMeta schema and leases its DLL through the snapshot") {
    turbo_flow_plugin_host_config_t config = schema_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_schema_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.schema_count, (size_t)1);
    check_equal(catalog.schemas[0].schema_version, 1u);
    check_true(catalog.schemas[0].data->storage_type != CMETA_TYPEOF(int));
    check_true(cmeta_type_equal(catalog.schemas[0].data->storage_type, CMETA_TYPEOF(int)));
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_EBUSY);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rolls back duplicate invalid and swallowed registration failures") {
    const char *paths[] = {FLOW_SCHEMA_DUPLICATE, FLOW_SCHEMA_BAD_VERSION,
                           FLOW_SCHEMA_BAD_DESCRIPTOR, FLOW_SCHEMA_SWALLOW};
    const int statuses[] = {SALTS_EALREADY, SALTS_EINVAL, SALTS_EINVAL, SALTS_ENOSPC};
    for (size_t i = 0u; i < 4u; ++i) {
      turbo_flow_plugin_host_config_t config = schema_config(i == 3u ? 0u : 2u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, paths[i], &error), statuses[i]);
      check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0);
      check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_GOOD, &error),
                  i == 3u ? SALTS_ENOSPC : SALTS_OK);
      check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
    }
  }

  it("rejects schema capability from a pre-1.4 root ABI") {
    turbo_flow_plugin_host_config_t config = schema_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_OLD_ABI, &error), SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_API);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("treats a complete 1.3 host configuration as zero schema capacity") {
    turbo_flow_plugin_host_config_t config = schema_config(99u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    config.size = TURBO_FLOW_PLUGIN_HOST_CONFIG_V1_3_SIZE;
    config.abi_minor = 3u;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_GOOD, &error), SALTS_ENOSPC);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rejects future schema wrappers and malformed catalog outputs") {
    turbo_flow_plugin_host_config_t config = schema_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_schema_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_BAD_ABI, &error), SALTS_EINVAL);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    catalog.size = 0u;
    check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &catalog), SALTS_EINVAL);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("accepts one stable identity at distinct schema versions") {
    turbo_flow_plugin_host_config_t config = schema_config(2u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_schema_catalog_v1_t catalog = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_VERSION_TWO, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.schema_count, (size_t)2);
    check_equal(catalog.schemas[0].schema_version, 1u);
    check_equal(catalog.schemas[1].schema_version, 2u);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rejects illegal and overlong schema stable identities") {
    const char *paths[] = {FLOW_SCHEMA_BAD_ID, FLOW_SCHEMA_LONG_ID};
    for (size_t i = 0u; i < 2u; ++i) {
      turbo_flow_plugin_host_config_t config = schema_config(1u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, paths[i], &error), SALTS_EINVAL);
      check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0);
      check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
    }
  }

  it("rejects a pre-1.4 schema wrapper from a 1.4 plugin") {
    turbo_flow_plugin_host_config_t config = schema_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_OLD_WRAPPER, &error), SALTS_EINVAL);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("keeps snapshots immutable across later schema registration") {
    turbo_flow_plugin_host_config_t config = schema_config(2u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_schema_catalog_v1_t old_catalog = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
    turbo_flow_plugin_schema_catalog_v1_t new_catalog = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *old_snapshot = NULL, *new_snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &old_snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_SCHEMA_SECOND, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &new_snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(old_snapshot, &old_catalog), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(new_snapshot, &new_catalog), SALTS_OK);
    check_equal(old_catalog.schema_count, (size_t)1);
    check_equal(new_catalog.schema_count, (size_t)2);
    turbo_flow_plugin_catalog_snapshot_destroy(new_snapshot);
    turbo_flow_plugin_catalog_snapshot_destroy(old_snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }
}
