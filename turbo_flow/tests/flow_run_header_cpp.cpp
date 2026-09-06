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
extern "C" int flow_async_terminal_cpp_header_probe(void) {
  turbo_flow_async_terminal_claim_t claim = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  turbo_flow_async_terminal_adapter_ops_t ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  return claim.version == TURBO_FLOW_ASYNC_TERMINAL_API_VERSION &&
                 ops.version == TURBO_FLOW_ASYNC_TERMINAL_API_VERSION
             ? 0
             : 1;
}
