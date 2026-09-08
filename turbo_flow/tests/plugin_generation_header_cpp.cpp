#include "turbo_flow_plugin_generation.h"

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_plugin_product_owner_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_adapter_provider_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_resource_provider_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_product_catalog_v1_t>);
