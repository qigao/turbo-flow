#include "tinytest.h"
#include "turbo_flow_plugin.h"

spec("caller-owned ABI3 result domain") {
  it("reserves bounded owner slots and releases its independent snapshot pin") {
    const size_t capacities[] = {0, 1, 1024, 1025};
    const int expected[] = {SALTS_EINVAL, SALTS_OK, SALTS_OK, SALTS_EINVAL};
    for (size_t i = 0; i < 4; ++i) {
      turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host = NULL;
      turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
      turbo_flow_plugin_result_domain_t *domain = NULL;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error), SALTS_OK);
      check_equal(turbo_flow_plugin_result_domain_create(snapshot, capacities[i], &domain, &error),
                  expected[i]);
      turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
      if (expected[i] == SALTS_OK) {
        check_equal(turbo_flow_plugin_result_domain_snapshot(domain, &state), SALTS_OK);
        check_equal(state.capacity, capacities[i]);
        check_equal(state.owner_count, (size_t)0);
        check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY);
        check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_EBUSY);
        check_equal(turbo_flow_plugin_result_domain_destroy(domain, &error), SALTS_OK);
      } else check_null(domain);
      check_equal(turbo_flow_plugin_host_destroy(host, 0, &error), SALTS_OK);
    }
  }
}
