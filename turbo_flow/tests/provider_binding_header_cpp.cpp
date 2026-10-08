#include "turbo_flow_provider_binding.h"

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_provider_candidate_v2_t>,
              "provider candidate must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_provider_resolver_v2_t>,
              "provider resolver must remain a C ABI");

extern "C" int turbo_flow_provider_binding_header_cpp_probe(void) {
  turbo_flow_provider_candidate_v2_t candidate =
      TURBO_FLOW_PROVIDER_CANDIDATE_V2_INIT;
  turbo_flow_provider_resolver_v2_t resolver =
      TURBO_FLOW_PROVIDER_RESOLVER_V2_INIT;
  return candidate.size == sizeof(candidate) &&
                 resolver.size == sizeof(resolver)
             ? 0
             : 1;
}
