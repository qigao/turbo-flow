#include "turbo_flow_domain.h"

#include <cmeta/function.h>

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_operation_port_binding_t>);
static_assert(std::is_standard_layout_v<turbo_flow_reflected_operation_registration_t>);
static_assert(std::is_standard_layout_v<turbo_flow_reflected_operation_view_t>);

int main() {
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
  turbo_flow_reflected_operation_view_t view =
      TURBO_FLOW_REFLECTED_OPERATION_VIEW_INIT;
  return registration.size == sizeof(registration) &&
                 view.size == sizeof(view)
             ? 0
             : 1;
}
