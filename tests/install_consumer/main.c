#include <turbo_flow.h>

int main(void) {
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || run_config.version != TURBO_FLOW_RUN_API_VERSION ||
      run_result.version != TURBO_FLOW_RUN_API_VERSION || !turbo_flow_message_type())
    return 1;
  turbo_flow_destroy(flow);
  return 0;
}
