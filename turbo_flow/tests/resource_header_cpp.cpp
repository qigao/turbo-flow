#include "turbo_flow_resource.h"

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_resource_candidate_v1_t>,
              "resource candidate must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_resource_resolver_v1_t>,
              "resource resolver must remain a C ABI");

extern "C" int turbo_flow_resource_header_cpp_probe(void) {
  turbo_flow_resource_candidate_v1_t candidate =
      TURBO_FLOW_RESOURCE_CANDIDATE_V1_INIT;
  turbo_flow_resource_resolver_v1_t resolver =
      TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  return candidate.size == sizeof(candidate) &&
                 resolver.size == sizeof(resolver)
             ? 0
             : 1;
}
