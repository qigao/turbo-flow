#include "tinytest.h"
#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_plugin_protocol.h"

#include <stdio.h>
#include <string.h>

#ifndef FLOW_PLUGIN_FIXTURE_GOOD_ONE
  #error FLOW_PLUGIN_FIXTURE_GOOD_ONE is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_GOOD_TWO
  #error FLOW_PLUGIN_FIXTURE_GOOD_TWO is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_DUPLICATE_ID
  #error FLOW_PLUGIN_FIXTURE_DUPLICATE_ID is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_DUPLICATE_KIND
  #error FLOW_PLUGIN_FIXTURE_DUPLICATE_KIND is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_DUPLICATE_RESOURCE_KIND
  #error FLOW_PLUGIN_FIXTURE_DUPLICATE_RESOURCE_KIND is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_REGISTER_FAIL
  #error FLOW_PLUGIN_FIXTURE_REGISTER_FAIL is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_DUPLICATE_STAGED
  #error FLOW_PLUGIN_FIXTURE_DUPLICATE_STAGED is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_LOAD_FAIL
  #error FLOW_PLUGIN_FIXTURE_LOAD_FAIL is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_BAD_ABI
  #error FLOW_PLUGIN_FIXTURE_BAD_ABI is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_BAD_SIZE
  #error FLOW_PLUGIN_FIXTURE_BAD_SIZE is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_BAD_ID
  #error FLOW_PLUGIN_FIXTURE_BAD_ID is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_NON_ASCII_ID
  #error FLOW_PLUGIN_FIXTURE_NON_ASCII_ID is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_BAD_VERSION
  #error FLOW_PLUGIN_FIXTURE_BAD_VERSION is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_MISSING_CALLBACK
  #error FLOW_PLUGIN_FIXTURE_MISSING_CALLBACK is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_QUIESCE_ONCE
  #error FLOW_PLUGIN_FIXTURE_QUIESCE_ONCE is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_SHUTDOWN_ONCE
  #error FLOW_PLUGIN_FIXTURE_SHUTDOWN_ONCE is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_CAPABILITY_MISMATCH
  #error FLOW_PLUGIN_FIXTURE_CAPABILITY_MISMATCH is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_EXTERNAL_POLL_WITHOUT_TRANSACTIONAL
  #error FLOW_PLUGIN_FIXTURE_EXTERNAL_POLL_WITHOUT_TRANSACTIONAL is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER
  #error FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER_ABI
  #error FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER_ABI is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_MISSING_SYMBOL
  #error FLOW_PLUGIN_FIXTURE_MISSING_SYMBOL is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_TRANSACTIONAL
  #error FLOW_PLUGIN_FIXTURE_TRANSACTIONAL is required
#endif
#ifndef FLOW_PLUGIN_FIXTURE_TRANSACTIONAL_DUPLICATE_KIND
  #error FLOW_PLUGIN_FIXTURE_TRANSACTIONAL_DUPLICATE_KIND is required
#endif

#define FLOW_PLUGIN_TEST_MAX_EVENTS 64u

typedef struct flow_plugin_test_event_s {
  turbo_flow_plugin_lifecycle_event_t event;
  int status;
  char plugin_id[TURBO_FLOW_PLUGIN_ID_MAX + 1u];
} flow_plugin_test_event_t;

typedef struct flow_plugin_test_probe_s {
  flow_plugin_test_event_t events[FLOW_PLUGIN_TEST_MAX_EVENTS];
  size_t count;
} flow_plugin_test_probe_t;

static void flow_plugin_test_observe(void *ctx, turbo_flow_plugin_lifecycle_event_t event,
                                     const char *plugin_id, int status) {
  flow_plugin_test_probe_t *probe = (flow_plugin_test_probe_t *)ctx;
  flow_plugin_test_event_t *record;
  size_t length;
  if (!probe || probe->count >= FLOW_PLUGIN_TEST_MAX_EVENTS) return;
  record = &probe->events[probe->count++];
  record->event = event;
  record->status = status;
  if (!plugin_id) return;
  length = strlen(plugin_id);
  if (length > TURBO_FLOW_PLUGIN_ID_MAX) length = TURBO_FLOW_PLUGIN_ID_MAX;
  memcpy(record->plugin_id, plugin_id, length);
  record->plugin_id[length] = '\0';
}

static turbo_flow_plugin_host_config_t flow_plugin_test_config(flow_plugin_test_probe_t *probe,
                                                               size_t modules, size_t adapters,
                                                               size_t resources) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  config.module_capacity = modules;
  config.adapter_provider_capacity = adapters;
  config.resource_provider_capacity = resources;
  config.lifecycle_observer = flow_plugin_test_observe;
  config.lifecycle_observer_ctx = probe;
  return config;
}

static void flow_plugin_test_check_event(const flow_plugin_test_probe_t *probe, size_t index,
                                         turbo_flow_plugin_lifecycle_event_t event,
                                         const char *plugin_id, int status) {
  check_true(index < probe->count);
  check_equal(probe->events[index].event, event);
  check_equal(probe->events[index].plugin_id, plugin_id);
  check_equal(probe->events[index].status, status);
}

spec("unified PluginHost") {
  it("creates a host from configured DLLs and verifies identity before plugin load") {
    char yaml[2048];
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t host_config = flow_plugin_test_config(&probe, 2u, 2u, 2u);
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    int size = snprintf(yaml, sizeof(yaml),
                        "version: 1\nplugins:\n"
                        "  - {id: fixture.one, version: 1.0.0, path: '%s'}\n"
                        "  - {id: fixture.two, version: 1.0.0, path: '%s'}\n"
                        "adapters: {}\n",
                        FLOW_PLUGIN_FIXTURE_GOOD_ONE, FLOW_PLUGIN_FIXTURE_GOOD_TWO);

    check_true(size > 0 && (size_t)size < sizeof(yaml));
    check_equal(turbo_flow_config_resolve_yaml(yaml, (size_t)size, &resolved, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_create_configured(&host_config, resolved, 1000u, &host,
                                                         &plugin_error),
                SALTS_OK);
    check_not_null(host);
    check_equal(turbo_flow_plugin_host_module_count(host), 2u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
    turbo_flow_resolved_config_destroy(resolved);

    size = snprintf(yaml, sizeof(yaml),
                    "version: 1\nplugins:\n"
                    "  - {id: fixture.wrong, version: 1.0.0, path: '%s'}\n"
                    "adapters: {}\n",
                    FLOW_PLUGIN_FIXTURE_GOOD_ONE);
    check_true(size > 0 && (size_t)size < sizeof(yaml));
    resolved = NULL;
    host = NULL;
    memset(&probe, 0, sizeof(probe));
    config_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    plugin_error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(yaml, (size_t)size, &resolved, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_create_configured(&host_config, resolved, 1000u, &host,
                                                         &plugin_error),
                SALTS_EPROTO);
    check_null(host);
    check_equal(plugin_error.stage, TURBO_FLOW_PLUGIN_STAGE_IDENTITY);
    check_equal(plugin_error.plugin_id, "fixture.wrong");
    check_equal(probe.count, 0u);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("rolls back earlier configured DLLs when a later DLL cannot load") {
    char yaml[2048];
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t host_config = flow_plugin_test_config(&probe, 2u, 2u, 2u);
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    int size = snprintf(yaml, sizeof(yaml),
                        "version: 1\nplugins:\n"
                        "  - {id: fixture.one, version: 1.0.0, path: '%s'}\n"
                        "  - {id: fixture.missing, version: 1.0.0, "
                        "path: 'C:/turbo-flow-test/missing-plugin.dll'}\n"
                        "adapters: {}\n",
                        FLOW_PLUGIN_FIXTURE_GOOD_ONE);

    check_true(size > 0 && (size_t)size < sizeof(yaml));
    check_equal(turbo_flow_config_resolve_yaml(yaml, (size_t)size, &resolved, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_create_configured(&host_config, resolved, 1000u, &host,
                                                         &plugin_error),
                SALTS_ENOENT);
    check_null(host);
    check_equal(plugin_error.stage, TURBO_FLOW_PLUGIN_STAGE_OPEN);
    check_equal(probe.count, 7u);
    flow_plugin_test_check_event(&probe, 3u, TURBO_FLOW_PLUGIN_LIFECYCLE_QUIESCE, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 4u, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 5u, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 6u, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD, "fixture.one",
                                 SALTS_OK);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("returns a retryable host when configured-load rollback itself fails") {
    char yaml[2048];
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t host_config = flow_plugin_test_config(&probe, 2u, 2u, 2u);
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_host_t *host = NULL;
    int size = snprintf(yaml, sizeof(yaml),
                        "version: 1\nplugins:\n"
                        "  - {id: fixture.quiesce-once, version: 1.0.0, path: '%s'}\n"
                        "  - {id: fixture.missing, version: 1.0.0, "
                        "path: 'C:/turbo-flow-test/missing-plugin.dll'}\n"
                        "adapters: {}\n",
                        FLOW_PLUGIN_FIXTURE_QUIESCE_ONCE);

    check_true(size > 0 && (size_t)size < sizeof(yaml));
    check_equal(turbo_flow_config_resolve_yaml(yaml, (size_t)size, &resolved, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_create_configured(&host_config, resolved, 1000u, &host,
                                                         &plugin_error),
                SALTS_EIO);
    check_not_null(host);
    check_equal(plugin_error.stage, TURBO_FLOW_PLUGIN_STAGE_QUIESCE);
    plugin_error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("rejects external progress capability without a transactional Product provider") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 2u, 1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(
                    host, FLOW_PLUGIN_FIXTURE_EXTERNAL_POLL_WITHOUT_TRANSACTIONAL, &error),
                SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_API);
    check_equal(turbo_flow_plugin_host_module_count(host), 0u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("atomically snapshots transactional adapter and resource providers") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 0u, 0u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    config.transactional_adapter_provider_capacity = 1u;
    config.transactional_resource_provider_capacity = 1u;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_TRANSACTIONAL, &error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_host_transactional_resource_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    check_equal(catalog.adapter_provider_count, 1u);
    check_equal(catalog.resource_provider_count, 1u);
    check_equal(catalog.adapter_providers[0].kind, "fixture.transactional.adapter");
    check_equal(catalog.resource_providers[0].kind, "fixture.transactional.resource");

    {
      const uint64_t canary = UINT64_C(0xA42C199E7735D108);
      struct physical_catalog_s {
        size_t size;
        uint32_t abi_major;
        uint32_t abi_minor;
        uint64_t canary;
      } physical = {sizeof(turbo_flow_plugin_transactional_product_catalog_v1_t), 2u, 0u,
                    UINT64_C(0xA42C199E7735D108)};
      check_equal(turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(
                      snapshot,
                      (turbo_flow_plugin_transactional_product_catalog_v1_t *)(void *)&physical),
                  SALTS_EINVAL);
      check_equal(physical.canary, canary);
    }
    catalog = (turbo_flow_plugin_transactional_product_catalog_v1_t)
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    catalog.size = sizeof(catalog) + 1u;
    check_equal(turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
                SALTS_EINVAL);
    catalog = (turbo_flow_plugin_transactional_product_catalog_v1_t)
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    catalog.abi_minor = 1u;
    check_equal(turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
                SALTS_EINVAL);

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("rolls back transactional providers on bounded capacity exhaustion") {
    const size_t adapter_capacities[] = {0u, 1u};
    const size_t resource_capacities[] = {1u, 0u};
    for (size_t i = 0u; i < 2u; ++i) {
      flow_plugin_test_probe_t probe = {0};
      turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 0u, 0u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      config.transactional_adapter_provider_capacity = adapter_capacities[i];
      config.transactional_resource_provider_capacity = resource_capacities[i];

      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_TRANSACTIONAL, &error),
                  SALTS_ENOSPC);
      check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
      check_equal(turbo_flow_plugin_host_module_count(host), 0u);
      check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 0u);
      check_equal(turbo_flow_plugin_host_transactional_resource_provider_count(host), 0u);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
    }
  }

  it("rejects one kind shared by legacy and transactional catalogs") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 2u, 1u, 1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    config.transactional_adapter_provider_capacity = 1u;
    config.transactional_resource_provider_capacity = 1u;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error), SALTS_OK);
    check_equal(
        turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_TRANSACTIONAL_DUPLICATE_KIND, &error),
        SALTS_EALREADY);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 0u);
    check_equal(turbo_flow_plugin_host_transactional_resource_provider_count(host), 0u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("rejects zero module capacity before allocating a host") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 0u, 1u, 1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_EINVAL);
    check_null(host);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT);
    check_equal(probe.count, 0u);

    config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
    config.abi_major = 99u;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_EINVAL);
    check_null(host);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT);

    config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
    config.abi_minor = 1u;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_EINVAL);
    check_null(host);

    config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
    config.size = sizeof(config) + 1u;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_EINVAL);
    check_null(host);
  }

  it("atomically loads providers and holds their module through a catalog snapshot") {
    static const char yaml[] = "version: 1\n"
                               "adapters:\n"
                               "  input:\n"
                               "    kind: fixture.adapter.one\n"
                               "    config: {}\n";
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_product_provider_registry_t registry = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_host_t *host = NULL;

    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_adapter_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_host_resource_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_product_registry(snapshot, &registry), SALTS_OK);
    check_equal(registry.adapter_provider_count, 1u);
    check_equal(registry.resource_provider_count, 1u);
    check_equal(registry.adapter_providers[0].kind, "fixture.adapter.one");
    check_equal(registry.resource_providers[0].kind, "fixture.resource.one");
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_product_preflight(resolved, &registry, &config_error), SALTS_OK);

    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_EBUSY);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_LEASE);
    check_equal(probe.count, 3u);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    turbo_flow_resolved_config_destroy(resolved);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
    check_equal(probe.count, 7u);
    flow_plugin_test_check_event(&probe, 0u, TURBO_FLOW_PLUGIN_LIFECYCLE_LOAD, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 1u, TURBO_FLOW_PLUGIN_LIFECYCLE_REGISTER, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 2u, TURBO_FLOW_PLUGIN_LIFECYCLE_COMMIT, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 3u, TURBO_FLOW_PLUGIN_LIFECYCLE_QUIESCE, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 4u, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 5u, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 6u, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD, "fixture.one",
                                 SALTS_OK);
  }

  it("rejects duplicate identities and kinds without changing committed counts") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 3u, 3u, 3u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    size_t events_before;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error), SALTS_OK);

    events_before = probe.count;
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_DUPLICATE_ID, &error),
                SALTS_EALREADY);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_IDENTITY);
    check_equal(probe.count, events_before);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);

    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_DUPLICATE_KIND, &error),
                SALTS_EALREADY);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_adapter_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_host_resource_provider_count(host), 1u);

    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(
        turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_DUPLICATE_RESOURCE_KIND, &error),
        SALTS_EALREADY);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_adapter_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_host_resource_provider_count(host), 1u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("rolls back staged providers on callback failure and duplicate staged registration") {
    const char *modules[] = {
        FLOW_PLUGIN_FIXTURE_REGISTER_FAIL, FLOW_PLUGIN_FIXTURE_DUPLICATE_STAGED,
        FLOW_PLUGIN_FIXTURE_CAPABILITY_MISMATCH, FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER,
        FLOW_PLUGIN_FIXTURE_INVALID_PROVIDER_ABI};
    const int expected[] = {SALTS_EIO, SALTS_EALREADY, SALTS_EPROTO, SALTS_EINVAL, SALTS_EINVAL};
    for (size_t i = 0u; i < sizeof(modules) / sizeof(modules[0]); ++i) {
      flow_plugin_test_probe_t probe = {0};
      turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 2u, 1u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, modules[i], &error), expected[i]);
      check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
      check_equal(turbo_flow_plugin_host_module_count(host), 0u);
      check_equal(turbo_flow_plugin_host_adapter_provider_count(host), 0u);
      check_equal(turbo_flow_plugin_host_resource_provider_count(host), 0u);
      check_equal(probe.count, 6u);
      check_equal(probe.events[2].event, TURBO_FLOW_PLUGIN_LIFECYCLE_ROLLBACK);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
    }
  }

  it("fails explicitly for missing files, symbols, incompatible ABI, and load callbacks") {
    char oversized_path[TURBO_FLOW_PLUGIN_PATH_MAX + 2u];
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);

    memset(oversized_path, 'a', sizeof(oversized_path));
    oversized_path[sizeof(oversized_path) - 1u] = '\0';
    check_equal(turbo_flow_plugin_host_load(host, oversized_path, &error), SALTS_EINVAL);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_ARGUMENT);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;

    check_equal(turbo_flow_plugin_host_load(host, "missing-plugin-file.dll", &error), SALTS_ENOENT);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_OPEN);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_MISSING_SYMBOL, &error),
                SALTS_ENOENT);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_SYMBOL);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_BAD_ABI, &error),
                SALTS_EINVAL);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_API);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_BAD_SIZE, &error),
                SALTS_EINVAL);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_API);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_MISSING_CALLBACK, &error),
                SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_API);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_BAD_ID, &error),
                SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_IDENTITY);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_NON_ASCII_ID, &error),
                SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_IDENTITY);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_BAD_VERSION, &error),
                SALTS_EPROTO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_IDENTITY);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_LOAD_FAIL, &error),
                SALTS_EIO);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_LOAD);
    check_equal(turbo_flow_plugin_host_module_count(host), 0u);
    check_equal(turbo_flow_plugin_host_adapter_provider_count(host), 0u);
    check_equal(turbo_flow_plugin_host_resource_provider_count(host), 0u);
    check_equal(probe.count, 4u);
    flow_plugin_test_check_event(&probe, 0u, TURBO_FLOW_PLUGIN_LIFECYCLE_LOAD, "fixture.load-fail",
                                 SALTS_EIO);
    flow_plugin_test_check_event(&probe, 1u, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN,
                                 "fixture.load-fail", SALTS_OK);
    flow_plugin_test_check_event(&probe, 2u, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY,
                                 "fixture.load-fail", SALTS_OK);
    flow_plugin_test_check_event(&probe, 3u, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD,
                                 "fixture.load-fail", SALTS_OK);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("retains retryable state when quiesce or shutdown fails") {
    const char *modules[] = {FLOW_PLUGIN_FIXTURE_QUIESCE_ONCE, FLOW_PLUGIN_FIXTURE_SHUTDOWN_ONCE};
    const turbo_flow_plugin_error_stage_t stages[] = {TURBO_FLOW_PLUGIN_STAGE_QUIESCE,
                                                      TURBO_FLOW_PLUGIN_STAGE_SHUTDOWN};
    for (size_t i = 0u; i < sizeof(modules) / sizeof(modules[0]); ++i) {
      flow_plugin_test_probe_t probe = {0};
      turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_load(host, modules[i], &error), SALTS_OK);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_EIO);
      check_equal(error.stage, stages[i]);
      error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
      check_equal(probe.count, 8u);
    }
  }

  it("enforces module and provider capacity at N plus one") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 1u, 1u, 1u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error), SALTS_OK);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_TWO, &error),
                SALTS_ENOSPC);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_CAPACITY);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);

    memset(&probe, 0, sizeof(probe));
    config = flow_plugin_test_config(&probe, 1u, 0u, 1u);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error),
                SALTS_ENOSPC);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), 0u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);

    memset(&probe, 0, sizeof(probe));
    config = flow_plugin_test_config(&probe, 1u, 1u, 0u);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error),
                SALTS_ENOSPC);
    check_equal(error.stage, TURBO_FLOW_PLUGIN_STAGE_REGISTRATION);
    check_equal(turbo_flow_plugin_host_module_count(host), 0u);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("shuts down and unloads multiple modules in reverse load order") {
    flow_plugin_test_probe_t probe = {0};
    turbo_flow_plugin_host_config_t config = flow_plugin_test_config(&probe, 2u, 2u, 2u);
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_ONE, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_PLUGIN_FIXTURE_GOOD_TWO, &error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
    check_equal(probe.count, 14u);
    flow_plugin_test_check_event(&probe, 6u, TURBO_FLOW_PLUGIN_LIFECYCLE_QUIESCE, "fixture.two",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 7u, TURBO_FLOW_PLUGIN_LIFECYCLE_QUIESCE, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 8u, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN, "fixture.two",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 9u, TURBO_FLOW_PLUGIN_LIFECYCLE_SHUTDOWN, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 10u, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY, "fixture.two",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 11u, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD, "fixture.two",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 12u, TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY, "fixture.one",
                                 SALTS_OK);
    flow_plugin_test_check_event(&probe, 13u, TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD, "fixture.one",
                                 SALTS_OK);
  }
}
