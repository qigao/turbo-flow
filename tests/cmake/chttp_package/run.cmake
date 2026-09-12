set(_fixture_dir "${TURBO_FLOW_SOURCE_DIR}/tests/cmake/chttp_package")
set(_active_profile "$ENV{TURBO_FLOW_ACTIVE_PRESET}")
if(_active_profile STREQUAL "win-release-user")
  set(_fixture_profile "fixture-win-release")
elseif(_active_profile STREQUAL "win-dev-user")
  set(_fixture_profile "fixture-win-dev")
elseif(_active_profile STREQUAL "linux-release-user")
  set(_fixture_profile "fixture-linux-release")
elseif(_active_profile STREQUAL "linux-dev-user")
  set(_fixture_profile "fixture-linux-dev")
else()
  message(FATAL_ERROR "Unsupported active host profile for Chttp fixture: ${_active_profile}")
endif()

set(_sdk_preset "${_fixture_profile}-sdk")
set(_sdk_root "${_fixture_dir}/build/${_sdk_preset}/install")
execute_process(COMMAND "${CMAKE_COMMAND}" --fresh --preset "${_sdk_preset}"
                WORKING_DIRECTORY "${_fixture_dir}"
                RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
if(NOT _result EQUAL 0)
  message(FATAL_ERROR "missing-symbol SDK configure failed:\n${_stdout}\n${_stderr}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" --build --preset "${_sdk_preset}"
                WORKING_DIRECTORY "${_fixture_dir}"
                RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
if(NOT _result EQUAL 0)
  message(FATAL_ERROR "missing-symbol SDK build/install failed:\n${_stdout}\n${_stderr}")
endif()

foreach(_language IN ITEMS c cxx)
  set(_positive "${_fixture_profile}-${_language}")
  execute_process(COMMAND "${CMAKE_COMMAND}" --fresh --preset "${_positive}"
                  WORKING_DIRECTORY "${_fixture_dir}"
                  RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
  if(NOT _result EQUAL 0)
    message(FATAL_ERROR "${_positive} configure failed:\n${_stdout}\n${_stderr}")
  endif()
  execute_process(COMMAND "${CMAKE_COMMAND}" --build --preset "${_positive}"
                  WORKING_DIRECTORY "${_fixture_dir}"
                  RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
  if(NOT _result EQUAL 0)
    message(FATAL_ERROR "${_positive} build failed:\n${_stdout}\n${_stderr}")
  endif()
endforeach()

foreach(_state_scenario IN ITEMS preserve-static-try-compile preserve-undefined-try-compile)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "CHTTP_PROBE_SCENARIO=${_state_scenario}"
            "${CMAKE_COMMAND}" --fresh --preset "${_fixture_profile}-c"
    WORKING_DIRECTORY "${_fixture_dir}"
    RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
  if(NOT _result EQUAL 0)
    message(FATAL_ERROR "${_state_scenario} configure failed:\n${_stdout}\n${_stderr}")
  endif()
endforeach()

set(_negative_scenarios
  unset-root empty-root missing-root empty-sdk outside-cache
  preimport-without-provenance preimport-outside-location wrong-config
  missing-deferred-cancel missing-deferred-cancel-static-try-compile)
set(_negative_messages
  "HTTP_SERVICES_ROOT is required"
  "HTTP_SERVICES_ROOT is required"
  "HTTP_SERVICES_ROOT is not a directory"
  "HTTP_SERVICES_ROOT is an empty SDK directory"
  "Chttp_DIR is outside HTTP_SERVICES_ROOT"
  "already imported without verifiable Chttp_DIR provenance"
  "runtime is outside HTTP_SERVICES_ROOT"
  "does not declare requested configuration"
  "CHttp::Server lacks linkable chttp_server_deferred_cancel"
  "CHttp::Server lacks linkable chttp_server_deferred_cancel")
list(LENGTH _negative_scenarios _negative_count)
math(EXPR _negative_last "${_negative_count} - 1")
foreach(_index RANGE ${_negative_last})
  list(GET _negative_scenarios ${_index} _scenario)
  list(GET _negative_messages ${_index} _expected)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "CHTTP_PROBE_SCENARIO=${_scenario}"
            "CHTTP_MISSING_SYMBOL_SDK_ROOT=${_sdk_root}"
            "${CMAKE_COMMAND}" --fresh --preset "${_fixture_profile}-c"
    WORKING_DIRECTORY "${_fixture_dir}"
    RESULT_VARIABLE _result OUTPUT_VARIABLE _stdout ERROR_VARIABLE _stderr)
  if(_result EQUAL 0)
    message(FATAL_ERROR "${_scenario} unexpectedly configured successfully")
  endif()
  string(CONCAT _output "${_stdout}" "${_stderr}")
  string(FIND "${_output}" "${_expected}" _match)
  if(_match EQUAL -1)
    message(FATAL_ERROR "${_scenario} did not report '${_expected}':\n${_output}")
  endif()
  if(_scenario MATCHES "^missing-deferred-cancel")
    set(_configure_log "${_fixture_dir}/build/${_fixture_profile}-c/CMakeFiles/CMakeConfigureLog.yaml")
    file(READ "${_configure_log}" _configure_diagnostics)
    if(_configure_diagnostics MATCHES "Cannot open include file: 'cnet/cnet.h'|cnet/cnet.h: No such file")
      message(FATAL_ERROR "${_scenario} failed on an incomplete native header dependency")
    endif()
    if(NOT _configure_diagnostics MATCHES
       "(LNK2019|undefined reference|unresolved external)[^\n]*chttp_server_deferred_cancel")
      message(FATAL_ERROR "${_scenario} did not record a native linker failure for chttp_server_deferred_cancel")
    endif()
  endif()
endforeach()
