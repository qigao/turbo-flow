#include "turbo_flow_plugin_operation.h"
static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR == 3u &&
              TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR == 1u, "plugin ABI changed");
extern "C" int flow_plugin_projection_cpp_probe(void) {
  turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  turbo_flow_projection_owner_t *owner = nullptr;
  return turbo_flow_plugin_projection_owner_create(nullptr, &config, &owner);
}
