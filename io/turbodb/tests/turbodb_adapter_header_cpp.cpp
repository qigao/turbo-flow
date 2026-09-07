#include <turbo_flow_turbodb.h>

extern "C" int turbodb_adapter_header_cpp_probe(void) {
  return TURBO_FLOW_TURBODB_API_VERSION == 1u ? 0 : 1;
}
