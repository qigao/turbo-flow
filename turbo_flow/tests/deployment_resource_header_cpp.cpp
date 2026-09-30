#include "turbo_flow_deployment_resource.h"

#include <type_traits>

static_assert(
    std::is_standard_layout_v<turbo_flow_deployment_resource_requirement_v1_t>,
    "resource requirement must remain a C ABI");
static_assert(
    std::is_standard_layout_v<turbo_flow_deployment_resource_reference_v1_t>,
    "resource reference must remain a C ABI");
static_assert(
    std::is_standard_layout_v<turbo_flow_deployment_resource_resolver_v1_t>,
    "resource resolver must remain a C ABI");

extern "C" int turbo_flow_deployment_resource_cpp_probe(void) {
  turbo_flow_deployment_resource_reference_v1_t ref =
      TURBO_FLOW_DEPLOYMENT_RESOURCE_REFERENCE_V1_INIT;
  return ref.size == sizeof(ref) ? 0 : 1;
}
