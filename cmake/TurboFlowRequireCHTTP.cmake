function(turbo_flow_require_chttp_deferred_cancel)
  if(NOT TARGET Salts::CHTTP)
    message(FATAL_ERROR
            "TurboFlow requires the imported target Salts::CHTTP")
  endif()

  get_property(_turbo_flow_enabled_languages GLOBAL PROPERTY ENABLED_LANGUAGES)
  set(CMAKE_REQUIRED_LIBRARIES Salts::CHTTP)
  unset(TURBO_FLOW_HAS_CHTTP_SERVER_DEFERRED_CANCEL CACHE)
  set(_turbo_flow_chttp_probe [[
#include <chttp/chttp.h>

int main(void) {
  return chttp_server_deferred_cancel((chttp_server_deferred *)0);
}
  ]])

  if("C" IN_LIST _turbo_flow_enabled_languages)
    include(CheckCSourceCompiles)
    check_c_source_compiles(
      "${_turbo_flow_chttp_probe}"
      TURBO_FLOW_HAS_CHTTP_SERVER_DEFERRED_CANCEL)
  elseif("CXX" IN_LIST _turbo_flow_enabled_languages)
    include(CheckCXXSourceCompiles)
    check_cxx_source_compiles(
      "${_turbo_flow_chttp_probe}"
      TURBO_FLOW_HAS_CHTTP_SERVER_DEFERRED_CANCEL)
  else()
    message(FATAL_ERROR
            "TurboFlow package discovery requires the C or CXX language")
  endif()

  if(NOT TURBO_FLOW_HAS_CHTTP_SERVER_DEFERRED_CANCEL)
    message(
      FATAL_ERROR
        "TurboFlow requires Salts::CHTTP with chttp_server_deferred_cancel; install a compatible Salts package"
    )
  endif()
endfunction()
