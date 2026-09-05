#include <turbo_flow.h>

int main(void) {
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return 1;
  turbo_flow_destroy(flow);
  return 0;
}
