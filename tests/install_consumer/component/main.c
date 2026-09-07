#if defined(TURBO_FLOW_TEST_CONFIG_COMPONENT)
#include <turbo_flow_resolved_config.h>

int main(void) {
  turbo_flow_resolved_config_t *config = NULL;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  int rc = turbo_flow_config_resolve_yaml(NULL, 0u, &config, &error);
  int unexpected_success = rc == SALTS_OK || config != NULL;
  if (config) turbo_flow_resolved_config_destroy(config);
  return unexpected_success;
}
#else
#include <turbo_flow.h>

int main(void) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return 1;
  turbo_flow_destroy(flow);
  return 0;
}
#endif
