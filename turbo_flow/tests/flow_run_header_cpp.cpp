#include "turbo_flow.h"

extern "C" int flow_run_cpp_header_probe(void) {
  turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
  return config.version == TURBO_FLOW_RUN_API_VERSION &&
                 result.version == TURBO_FLOW_RUN_API_VERSION &&
                 turbo_flow_message_type() != nullptr
             ? 0
             : 1;
}
