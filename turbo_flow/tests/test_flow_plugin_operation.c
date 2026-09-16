#include "operation_schema_fixture.h"
#include "tinytest.h"
#include "turbo_flow_plugin.h"
static void catalog_observe(void *ctx, turbo_flow_plugin_lifecycle_event_t event, const char *id,
                            int status) {
  (void)id;
  (void)status;
  if (event == TURBO_FLOW_PLUGIN_LIFECYCLE_COMMIT) ++*(unsigned *)ctx;
}

spec("ABI3 operation catalog") {
  it("reuses Graph semantic matching across independent metadata translation units") {
    turbo_flow_data_schema_t left = {sizeof(left),
                                     TURBO_FLOW_DOMAIN_DATA,
                                     TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                     "cmeta.int.data",
                                     "Integer",
                                     "int",
                                     7u,
                                     1u,
                                     NULL};
    turbo_flow_data_schema_t right = left;
    /* Salts' scalar symbol may be shared by local TUs; the runtime test also checks DLL separation.
     */
    check_equal(turbo_flow_data_schema_match(&left, &cmeta_data_int, &right,
                                             operation_schema_fixture_int()),
                SALTS_OK);
    ++right.schema_version;
    check_equal(turbo_flow_data_schema_match(&left, &cmeta_data_int, &right,
                                             operation_schema_fixture_int()),
                SALTS_EPROTO);
    left.schema_name = "operation.Record.data";
    left.type_name = "Record";
    left.projection_type = "operation_record";
    right = left;
    check(&operation_record_data != operation_schema_fixture_record());
    check_equal(turbo_flow_data_schema_match(&left, &operation_record_data, &right,
                                             operation_schema_fixture_record()),
                SALTS_OK);
    check_equal(turbo_flow_data_schema_match(&left, &operation_record_data, &right,
                                             &operation_record_offset_data),
                SALTS_EPROTO);
  }
  it("rejects malformed ABI, shape, profile and permissions without any committed entries") {
    const char *paths[] = {
        FLOW_OPERATION_BAD_SHAPE,      FLOW_OPERATION_BAD_ABI,        FLOW_OPERATION_SHORT,
        FLOW_OPERATION_LONG,           FLOW_OPERATION_BAD_VTABLE,     FLOW_OPERATION_NO_GUARANTEE,
        FLOW_OPERATION_BAD_EFFECT,     FLOW_OPERATION_BAD_PERMISSION, FLOW_OPERATION_DUP_PERMISSION,
        FLOW_OPERATION_PERMISSION,     FLOW_OPERATION_BAD_LIMIT,      FLOW_OPERATION_MISSING_OUTPUT,
        FLOW_OPERATION_MEMORY_OVERFLOW};
    const int statuses[] = {SALTS_EINVAL,   SALTS_EINVAL,  SALTS_EINVAL,  SALTS_EINVAL,
                            SALTS_EINVAL,   SALTS_ENOTSUP, SALTS_ENOTSUP, SALTS_EINVAL,
                            SALTS_EALREADY, SALTS_ENOTSUP, SALTS_EINVAL,  SALTS_EPROTO,
                            SALTS_EINVAL};
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); ++i) {
      turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
      turbo_flow_plugin_operation_catalog_v3_t catalog;
      turbo_flow_plugin_schema_catalog_v1_t schemas = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
      turbo_flow_product_provider_registry_t products = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
      unsigned commits = 0;
      hc.lifecycle_observer = catalog_observe;
      hc.lifecycle_observer_ctx = &commits;
      check_equal(turbo_flow_plugin_host_create(&hc, &host, &error), SALTS_OK);
      int rc = turbo_flow_plugin_host_load(host, paths[i], &error);
      info("catalog rejection %zu: %d", i, rc);
      check_equal(rc, statuses[i]);
      check_equal(commits, 0u);
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
      turbo_flow_plugin_operation_catalog_v3_init(&catalog);
      check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog),
                  SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &schemas), SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_product_registry(snapshot, &products),
                  SALTS_OK);
      check_equal(catalog.count, (size_t)0);
      check_equal(schemas.schema_count, (size_t)0);
      check_equal(products.adapter_provider_count, (size_t)0);
      turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
      check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_OK);
    }
  }
  it("rolls back adapter, schema and operation tails even when the DLL swallows the first add "
     "error") {
    for (int exhausted = 0; exhausted < 3; ++exhausted) {
      turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
      turbo_flow_plugin_operation_catalog_v3_t catalog;
      turbo_flow_plugin_schema_catalog_v1_t schemas = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
      turbo_flow_product_provider_registry_t products = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
      unsigned commits = 0;
      hc.lifecycle_observer = catalog_observe;
      hc.lifecycle_observer_ctx = &commits;
      if (exhausted == 0) hc.adapter_provider_capacity = 0;
      if (exhausted == 1) hc.schema_capacity = 0;
      if (exhausted == 2) hc.operation_capacity = 0;
      check_equal(turbo_flow_plugin_host_create(&hc, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, FLOW_OPERATION_SWALLOW, &error), SALTS_ENOSPC);
      check_equal(commits, 0u);
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
      turbo_flow_plugin_operation_catalog_v3_init(&catalog);
      check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog),
                  SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &schemas), SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_product_registry(snapshot, &products),
                  SALTS_OK);
      check_equal(catalog.count, (size_t)0);
      check_equal(schemas.schema_count, (size_t)0);
      check_equal(products.adapter_provider_count, (size_t)0);
      turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
      check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_OK);
    }
  }
  it("does not borrow another module's registered schema and preserves its committed catalog") {
    turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_operation_catalog_v3_t catalog;
    unsigned commits = 0;
    hc.lifecycle_observer = catalog_observe;
    hc.lifecycle_observer_ctx = &commits;
    check_equal(turbo_flow_plugin_host_create(&hc, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_OPERATION_OK, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_OPERATION_NO_SCHEMA, &error), SALTS_EPROTO);
    check_equal(commits, 1u);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    turbo_flow_plugin_operation_catalog_v3_init(&catalog);
    check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.count, (size_t)1);
    check_equal(catalog.entries[0].plugin_id, "fixture.operation");
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_OK);
  }
  it("registers versioned operations transactionally through the canonical DLL entry") {
    const char *paths[] = {FLOW_OPERATION_OK,           FLOW_OPERATION_OK,
                           FLOW_OPERATION_TWO_VERSIONS, FLOW_OPERATION_TWO_VERSIONS,
                           FLOW_OPERATION_DUPLICATE,    FLOW_OPERATION_DUPLICATE_CHANGED,
                           FLOW_OPERATION_NO_SCHEMA,    FLOW_OPERATION_SWALLOW};
    const size_t capacities[] = {0, 1, 2, 1, 2, 2, 2, 2};
    const int expected[] = {SALTS_ENOSPC,   SALTS_OK,       SALTS_OK,     SALTS_ENOSPC,
                            SALTS_EALREADY, SALTS_EALREADY, SALTS_EPROTO, SALTS_EALREADY};
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); ++i) {
      turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
      turbo_flow_plugin_operation_catalog_v3_t operations;
      turbo_flow_plugin_schema_catalog_v1_t schemas = TURBO_FLOW_PLUGIN_SCHEMA_CATALOG_V1_INIT;
      config.operation_capacity = capacities[i];
      turbo_flow_plugin_operation_catalog_v3_init(&operations);
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, paths[i], &error), expected[i]);
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &operations),
                  SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_schema_catalog(snapshot, &schemas), SALTS_OK);
      check_equal(operations.count, expected[i] == SALTS_OK ? capacities[i] : (size_t)0);
      check_equal(schemas.schema_count, expected[i] == SALTS_OK ? (size_t)1 : (size_t)0);
      turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
      check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_OK);
    }
  }
  it("queries an empty bounded host without publishing partial catalog outputs") {
    turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_operation_catalog_v3_t catalog;
    config.operation_capacity = 0;
    turbo_flow_plugin_operation_catalog_v3_init(&catalog);
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.count, (size_t)0);
    catalog.size++;
    check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog),
                SALTS_EINVAL);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_OK);
  }
}
