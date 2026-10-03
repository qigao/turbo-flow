#include "turbo_flow_plugin_generation.h"

#include <cstddef>
#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_plugin_generation_config_t>);
static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR == 3u);
static_assert(TURBO_FLOW_PLUGIN_CAP_OPERATION == (UINT64_C(1) << 8));
static_assert(TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP == 7);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_operation_v3_t>);
static_assert(std::is_standard_layout_v<turbo_flow_plugin_result_domain_snapshot_v3_t>);
static_assert(std::is_same_v<
              decltype(&turbo_flow_plugin_generation_create),
              int (*)(turbo_flow_plugin_catalog_snapshot_t *, const turbo_flow_resolved_config_t *,
                      turbo_flow_t **, const turbo_flow_plugin_generation_config_t *,
                      turbo_flow_plugin_result_domain_t *, turbo_flow_plugin_generation_t **,
                      turbo_flow_plugin_generation_t **, turbo_flow_config_error_t *)>);

static int (*generation_poll)(turbo_flow_plugin_generation_t *, uint32_t,
                              turbo_flow_config_error_t *) = turbo_flow_plugin_generation_poll;
