#include "turbo_flow_diagnostic.h"

#include <type_traits>

static_assert(std::is_standard_layout_v<turbo_flow_config_error_t>,
              "TurboFlow diagnostic must remain a C ABI");
static_assert(TURBO_FLOW_DIAGNOSTIC_PATH_MAX == 255u,
              "diagnostic path bound is part of the public contract");
static_assert(TURBO_FLOW_DIAGNOSTIC_MESSAGE_MAX == 255u,
              "diagnostic message bound is part of the public contract");

extern "C" int turbo_flow_diagnostic_cpp_probe(void) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  return error.size == sizeof(error) && error.status == SALTS_OK ? 0 : 1;
}
