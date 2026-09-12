foreach(required_variable IN ITEMS TURBO_FLOW_SOURCE_DIR)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

set(fixture_source_dir
    "${TURBO_FLOW_SOURCE_DIR}/tests/cmake/find_tools_without_lemon")
if(NOT DEFINED ENV{TURBO_FLOW_ACTIVE_PRESET} OR
   NOT "$ENV{TURBO_FLOW_ACTIVE_PRESET}" MATCHES "^(win|linux)-(dev|release)-user$")
  message(FATAL_ERROR "A supported TURBO_FLOW_ACTIVE_PRESET is required")
endif()
set(fixture_preset "fixture-$ENV{TURBO_FLOW_ACTIVE_PRESET}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" --fresh --preset "${fixture_preset}"
  WORKING_DIRECTORY "${fixture_source_dir}"
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_stdout
  ERROR_VARIABLE configure_stderr)

if(configure_result EQUAL 0)
  message(FATAL_ERROR
          "FindTools configure unexpectedly succeeded without the project lemon target")
endif()

set(configure_output "${configure_stdout}\n${configure_stderr}")
if(NOT configure_output MATCHES "project-provided lemon target is required")
  message(FATAL_ERROR
          "FindTools failure did not report the missing project lemon target:\n${configure_output}")
endif()
