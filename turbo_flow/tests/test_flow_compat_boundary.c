#include "tinytest.h"
#include "turbo_flow_config.h"

spec("flow_compat_boundary") {
  it("keeps configuration symbols on the historical Flow library") {
    static const char yaml[] = "version: 1\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 TURBO_OK);
    check_equal(turbo_flow_resolved_config_runtime_ingress(config, &ingress), TURBO_OK);
    check_equal(ingress.workers, TURBO_FLOW_ASYNC_INGRESS_DEFAULT_WORKERS);
    turbo_flow_resolved_config_destroy(config);
  }
}
