#include "turbo_flow_plugin_materializer.h"

static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR == 3u);
static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR == 2u);

int turbo_flow_plugin_materializer_header_cpp_probe() {
  turbo_flow_plugin_materializer_v1_t materializer =
      TURBO_FLOW_PLUGIN_MATERIALIZER_V1_INIT;
  turbo_flow_plugin_materializer_input_v1_t input =
      TURBO_FLOW_PLUGIN_MATERIALIZER_INPUT_V1_INIT;
  turbo_flow_plugin_materializer_catalog_v1_t catalog =
      TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
  return materializer.size == sizeof(materializer) &&
                 input.size == sizeof(input) &&
                 catalog.size == sizeof(catalog)
             ? 0
             : 1;
}
