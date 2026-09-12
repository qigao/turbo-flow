set(_fixture_dir "${TURBO_FLOW_SOURCE_DIR}/tests/cmake/chttp_package")

foreach(_positive IN ITEMS c-release cxx-release)
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

set(_negative_scenarios
  unset-root empty-root missing-root empty-sdk outside-cache
  preimport-without-provenance preimport-outside-location wrong-config
  missing-deferred-cancel)
set(_negative_messages
  "HTTP_SERVICES_ROOT is required"
  "HTTP_SERVICES_ROOT is required"
  "HTTP_SERVICES_ROOT is not a directory"
  "HTTP_SERVICES_ROOT is an empty SDK directory"
  "Chttp_DIR is outside HTTP_SERVICES_ROOT"
  "already imported without verifiable Chttp_DIR provenance"
  "runtime is outside HTTP_SERVICES_ROOT"
  "does not declare requested configuration Debug"
  "CHttp::Server lacks linkable chttp_server_deferred_cancel")
list(LENGTH _negative_scenarios _negative_count)
math(EXPR _negative_last "${_negative_count} - 1")
foreach(_index RANGE ${_negative_last})
  list(GET _negative_scenarios ${_index} _scenario)
  list(GET _negative_messages ${_index} _expected)
  execute_process(COMMAND "${CMAKE_COMMAND}" --fresh --preset "${_scenario}"
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
endforeach()
