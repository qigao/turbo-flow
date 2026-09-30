#include "turbo_flow_provider_resolver.h"

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_provider_export_ref_v1_t>,
              "provider export ref must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_provider_resolver_v1_t>,
              "provider resolver must remain a C ABI");

extern "C" int turbo_flow_provider_resolver_header_cpp_probe(void) {
  turbo_flow_provider_resolver_v1_t resolver =
      TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  return resolver.size == sizeof(resolver) ? 0 : 1;
}
