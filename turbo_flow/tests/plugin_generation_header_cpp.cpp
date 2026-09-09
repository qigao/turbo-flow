#include "turbo_flow_plugin_generation.h"

#include <cstddef>
#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_plugin_product_owner_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_adapter_provider_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_resource_provider_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_product_catalog_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_generation_config_t>);
static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR == 2u);
static_assert(TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL == (UINT64_C(1) << 6));

static turbo_flow_plugin_product_owner_poll_fn owner_poll = nullptr;
static int (*owner_publish)(turbo_flow_plugin_product_owner_v1_t *,
                            const turbo_flow_plugin_product_owner_v1_t *) =
    turbo_flow_plugin_product_owner_publish;
static int (*generation_poll)(turbo_flow_plugin_generation_t *, uint32_t,
                              turbo_flow_config_error_t *) = turbo_flow_plugin_generation_poll;
