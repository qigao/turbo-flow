#include "turbo_flow.h"

#include <cstdint>
#include <type_traits>

static_assert(TURBO_FLOW_EXECUTION_PLAN_API_VERSION == 1u,
              "ExecutionPlan diagnostics ABI version");
static_assert(TURBO_FLOW_EXECUTION_PLAN_INDEX_NONE == UINT32_MAX,
              "ExecutionPlan NONE index");
static_assert(std::is_standard_layout<turbo_flow_execution_plan_summary_t>::value,
              "summary must be standard layout");
static_assert(std::is_standard_layout<turbo_flow_execution_stage_view_t>::value,
              "stage view must be standard layout");
static_assert(std::is_standard_layout<turbo_flow_execution_cflow_region_view_t>::value,
              "region view must be standard layout");

extern "C" int turbo_flow_plan_introspection_header_cpp_probe(void) {
  turbo_flow_execution_plan_summary_t summary =
      TURBO_FLOW_EXECUTION_PLAN_SUMMARY_INIT;
  turbo_flow_execution_stage_view_t stage =
      TURBO_FLOW_EXECUTION_STAGE_VIEW_INIT;
  turbo_flow_execution_cflow_region_view_t region =
      TURBO_FLOW_EXECUTION_CFLOW_REGION_VIEW_INIT;

  return summary.size == sizeof(summary) &&
         summary.version == TURBO_FLOW_EXECUTION_PLAN_API_VERSION &&
         stage.size == sizeof(stage) &&
         stage.version == TURBO_FLOW_EXECUTION_PLAN_API_VERSION &&
         stage.cflow_region_index == TURBO_FLOW_EXECUTION_PLAN_INDEX_NONE &&
         region.size == sizeof(region) &&
         region.version == TURBO_FLOW_EXECUTION_PLAN_API_VERSION;
}
