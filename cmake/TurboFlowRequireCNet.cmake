function(turbo_flow_require_cnet_stop_drain_contract)
  if(NOT TARGET Salts::CNet)
    message(FATAL_ERROR
            "TurboFlow requires the imported target Salts::CNet")
  endif()

  get_property(_turbo_flow_enabled_languages GLOBAL PROPERTY ENABLED_LANGUAGES)
  set(CMAKE_REQUIRED_LIBRARIES Salts::CNet)
  unset(TURBO_FLOW_HAS_CNET_STOP_DRAIN_CONTRACT_V1 CACHE)
  set(_turbo_flow_cnet_probe [[
#include <cnet/cnet.h>

#if !defined(CNET_STOP_DRAIN_CONTRACT_VERSION) || CNET_STOP_DRAIN_CONTRACT_VERSION < 1u
#error "CNet stop-drain contract v1 is required"
#endif

int main(void) {
  return 0;
}
  ]])

  if("C" IN_LIST _turbo_flow_enabled_languages)
    include(CheckCSourceCompiles)
    check_c_source_compiles(
      "${_turbo_flow_cnet_probe}"
      TURBO_FLOW_HAS_CNET_STOP_DRAIN_CONTRACT_V1)
  elseif("CXX" IN_LIST _turbo_flow_enabled_languages)
    include(CheckCXXSourceCompiles)
    check_cxx_source_compiles(
      "${_turbo_flow_cnet_probe}"
      TURBO_FLOW_HAS_CNET_STOP_DRAIN_CONTRACT_V1)
  else()
    message(FATAL_ERROR
            "TurboFlow package discovery requires the C or CXX language")
  endif()

  if(NOT TURBO_FLOW_HAS_CNET_STOP_DRAIN_CONTRACT_V1)
    message(
      FATAL_ERROR
        "TurboFlow requires Salts::CNet stop-drain contract v1; install a compatible Salts package"
    )
  endif()
endfunction()
