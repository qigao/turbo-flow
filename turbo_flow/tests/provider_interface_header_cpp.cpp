#include "turbo_flow_provider.h"

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_provider_contract_v1_t>,
              "provider contract must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_provider_instance_v1_t>,
              "provider instance must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_provider_config_view_v1_t>,
              "typed config view must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_provider_resource_view_v1_t>,
              "resource view must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_runtime_owner>,
              "runtime owner Interface handle must remain a C ABI");
static_assert(std::is_standard_layout_v<turbo_flow_provider_factory>,
              "provider factory Interface handle must remain a C ABI");

extern "C" int turbo_flow_provider_header_cpp_probe(void) {
  return cmeta_interface_desc_valid(turbo_flow_provider_factory_interface()) &&
                 cmeta_interface_desc_valid(turbo_flow_runtime_owner_interface())
             ? 0
             : 1;
}
