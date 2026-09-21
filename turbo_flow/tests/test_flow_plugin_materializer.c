#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_materializer.h"

#include <stddef.h>
#include <string.h>
#include <tinytest.h>

static turbo_flow_plugin_host_config_t materializer_config(size_t capacity) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  config.module_capacity = 8u;
  config.materializer_capacity = capacity;
  return config;
}

static void materializer_observe(void *ctx, turbo_flow_plugin_lifecycle_event_t event,
                                 const char *plugin_id, int status) {
  (void)event;
  (void)plugin_id;
  (void)status;
  ++*(size_t *)ctx;
}

spec("plugin materializer catalog") {
  it("loads one ABI3.1 materializer, leases its DLL, and invokes the borrowed callback") {
    turbo_flow_plugin_host_config_t config = materializer_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_materializer_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    turbo_flow_plugin_materializer_input_v1_t input =
        TURBO_FLOW_PLUGIN_MATERIALIZER_INPUT_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    int encoded = 42;
    int native = 0;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_materializer_count(host), (size_t)1);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog),
                SALTS_OK);
    check_equal(catalog.count, (size_t)1);
    check_equal(catalog.entries[0].plugin_id, "fixture.materializer.good");
    check_equal(catalog.entries[0].materializer.schema.schema_name,
                "fixture.materializer.int");
    check_equal(catalog.entries[0].materializer.schema.schema_version, 1u);
    check_equal(catalog.entries[0].materializer.schema.encoding,
                TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(catalog.entries[0].materializer.native_bytes, sizeof(int));

    input.data = &encoded;
    input.data_size = sizeof(encoded);
    check_equal(catalog.entries[0].materializer.materialize(
                    catalog.entries[0].materializer.ctx, &input, &native, sizeof(native)),
                SALTS_OK);
    check_equal(native, encoded);

    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_EBUSY);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rolls back duplicate malformed missing-callback and swallowed failures") {
    const char *paths[] = {
        FLOW_MATERIALIZER_DUPLICATE,
        FLOW_MATERIALIZER_BAD_SCHEMA_VERSION,
        FLOW_MATERIALIZER_BAD_NATIVE_SIZE,
        FLOW_MATERIALIZER_BAD_MAX_ENCODED,
        FLOW_MATERIALIZER_MISSING_CALLBACK,
        FLOW_MATERIALIZER_SWALLOW};
    const int statuses[] = {
        SALTS_EALREADY, SALTS_EINVAL, SALTS_EINVAL,
        SALTS_EINVAL, SALTS_EINVAL, SALTS_ENOSPC};

    for (size_t i = 0u; i < sizeof(paths) / sizeof(paths[0]); ++i) {
      turbo_flow_plugin_host_config_t config =
          materializer_config(i + 1u == sizeof(paths) / sizeof(paths[0]) ? 0u : 2u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, paths[i], &error), statuses[i]);
      check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
      check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0);
      check_equal(turbo_flow_plugin_host_materializer_count(host), (size_t)0);
      check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
    }
  }

  it("rejects a future materializer wrapper from an ABI3.1 plugin") {
    turbo_flow_plugin_host_config_t config = materializer_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_BAD_ABI, &error),
                SALTS_EINVAL);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0);
    check_equal(turbo_flow_plugin_host_materializer_count(host), (size_t)0);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rejects an ABI3.0 root before load or registration") {
    size_t lifecycle_calls = 0u;
    turbo_flow_plugin_host_config_t config = materializer_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    config.lifecycle_observer = materializer_observe;
    config.lifecycle_observer_ctx = &lifecycle_calls;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_OLD_ABI, &error),
                SALTS_EINVAL);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_API);
    check_equal(lifecycle_calls, (size_t)0);
    check_equal(turbo_flow_plugin_host_module_count(host), (size_t)0);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rejects short host configuration instead of zero extending materializer capacity") {
    turbo_flow_plugin_host_config_t config = materializer_config(99u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    config.size = offsetof(turbo_flow_plugin_host_config_t, materializer_capacity);
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_EINVAL);
    check_null(host);
  }

  it("rejects malformed materializer catalog outputs") {
    turbo_flow_plugin_host_config_t config = materializer_config(1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_materializer_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);

    catalog.size = 0u;
    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog),
                SALTS_EINVAL);
    catalog = (turbo_flow_plugin_materializer_catalog_v1_t)
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    catalog.size = sizeof(catalog) + 1u;
    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog),
                SALTS_EINVAL);
    catalog = (turbo_flow_plugin_materializer_catalog_v1_t)
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    catalog.abi_minor = TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR + 1u;
    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(snapshot, &catalog),
                SALTS_EINVAL);

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("rejects the same schema version and encoding from a second plugin atomically") {
    turbo_flow_plugin_host_config_t config = materializer_config(2u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_SECOND, &error),
                SALTS_EALREADY);
    check_equal(turbo_flow_plugin_host_module_count(host), (size_t)1);
    check_equal(turbo_flow_plugin_host_materializer_count(host), (size_t)1);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }

  it("keeps snapshots immutable across later materializer registration") {
    turbo_flow_plugin_host_config_t config = materializer_config(2u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_materializer_catalog_v1_t old_catalog =
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    turbo_flow_plugin_materializer_catalog_v1_t new_catalog =
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *old_snapshot = NULL;
    turbo_flow_plugin_catalog_snapshot_t *new_snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_GOOD, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &old_snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_MATERIALIZER_VERSION_TWO, &error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &new_snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(old_snapshot, &old_catalog),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(new_snapshot, &new_catalog),
                SALTS_OK);
    check_equal(old_catalog.count, (size_t)1);
    check_equal(new_catalog.count, (size_t)2);
    check_equal(new_catalog.entries[1].materializer.schema.schema_version, 2u);

    turbo_flow_plugin_catalog_snapshot_destroy(new_snapshot);
    turbo_flow_plugin_catalog_snapshot_destroy(old_snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &error), SALTS_OK);
  }
}
