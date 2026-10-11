#include "turbo_flow_rulesforge_resource.h"
#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_rulesforge_resource_view_t>);
static_assert(std::is_same_v<decltype(turbo_flow_rulesforge_resource_view_t::knowledge_base),
                             ruleforge_knowledge_base_t>);
constexpr turbo_flow_rulesforge_resource_view_t empty_view = TURBO_FLOW_RULESFORGE_RESOURCE_VIEW_INIT;
static_assert(empty_view.size == sizeof(turbo_flow_rulesforge_resource_view_t));
static_assert(empty_view.knowledge_base == nullptr && empty_view.fact_type == nullptr);
