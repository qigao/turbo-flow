#include <turbo_flow.h>
#include <turbo_flow_chttp.h>
#include <salts/plugin.h>

static_assert(CMETA_PLUGIN_ABI_VERSION == 5u,
              "installed Salts 2.x must export canonical CMeta Plugin ABI 5");

int main() {
  turbo_flow_t *flow = turbo_flow_create();
  if (flow == nullptr) return 1;
  turbo_flow_destroy(flow);
  return 0;
}
