#include "turbo_flow_databind.h"

#include "tf_service.http.h"
#include "tf_service.rpc.h"
#include "tf_service.service_native.h"

#include <type_traits>

static_assert(
    std::is_standard_layout<turbo_flow_databind_service_plan_view_t>::value,
    "DataBind Service plan diagnostics must remain C-compatible");
static_assert(
    std::is_standard_layout<DataBindServiceNativeBinding>::value,
    "generated Service native binding must remain C-compatible");

extern "C" int turbo_flow_databind_service_header_cpp_probe(void) {
  turbo_flow_databind_service_plan_view_t view =
      TURBO_FLOW_DATABIND_SERVICE_PLAN_VIEW_INIT;
  return view.version == TURBO_FLOW_DATABIND_SERVICE_PLAN_API_VERSION &&
         databind_tf_service_http_projection.entry_count != 0u &&
         databind_tf_service_rpc_projection.entry_count != 0u;
}
