#include "turbo_flow_plugin_generation.h"

#include <cstddef>
#include <type_traits>

struct legacy_product_owner_v1_0 {
  size_t size;
  uint32_t abi_major;
  uint32_t abi_minor;
  turbo_flow_plugin_product_owner_flags_t flags;
  void *ctx;
  turbo_flow_plugin_product_owner_quiesce_fn quiesce;
  turbo_flow_plugin_product_owner_drain_fn drain;
  turbo_flow_plugin_product_owner_shutdown_fn shutdown;
  turbo_flow_plugin_product_owner_destroy_fn destroy;
};

static_assert(std::is_standard_layout_v<turbo_flow_plugin_product_owner_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_adapter_provider_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_resource_provider_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_transactional_product_catalog_v1_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_generation_config_t>);
static_assert(TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_0_SIZE ==
              offsetof(turbo_flow_plugin_product_owner_v1_t, reserved_v1_1));
static_assert(offsetof(turbo_flow_plugin_product_owner_v1_t, poll) >=
              TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_0_SIZE + sizeof(uint64_t));
static_assert(sizeof(legacy_product_owner_v1_0) == TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_0_SIZE);
static_assert(TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL == (UINT64_C(1) << 6));

static turbo_flow_plugin_product_owner_poll_fn owner_poll = nullptr;
static int (*owner_publish)(turbo_flow_plugin_product_owner_v1_t *,
                            const turbo_flow_plugin_product_owner_v1_t *) =
    turbo_flow_plugin_product_owner_publish;
static int (*generation_poll)(turbo_flow_plugin_generation_t *, uint32_t,
                              turbo_flow_config_error_t *) = turbo_flow_plugin_generation_poll;
