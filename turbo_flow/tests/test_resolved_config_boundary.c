#include "tinytest.h"
#include "turbo_flow_resolved_config.h"

#include <string.h>

spec("resolved_config_boundary") {
  it("resolves configuration without the Graph or Product API") {
    static const char yaml[] = "version: 1\nadapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t json_len = 0u;
    const char *json;

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    json = turbo_flow_resolved_config_json(config, &json_len);
    check_not_null(json);
    check_true(json_len > 0u);
    check_not_null(strstr(json, "\"adapters\":{}"));
    turbo_flow_resolved_config_destroy(config);
  }
}
