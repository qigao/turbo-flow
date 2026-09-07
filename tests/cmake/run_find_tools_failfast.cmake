foreach(required_variable IN ITEMS
        TURBO_FLOW_SOURCE_DIR TURBO_FLOW_BINARY_DIR TURBO_FLOW_GENERATOR)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

set(fixture_source_dir
    "${TURBO_FLOW_SOURCE_DIR}/tests/cmake/find_tools_without_lemon")
set(fixture_binary_dir
    "${TURBO_FLOW_BINARY_DIR}/find-tools-without-lemon")

file(REMOVE_RECURSE "${fixture_binary_dir}")

execute_process(
  COMMAND
    "${CMAKE_COMMAND}"
    -S "${fixture_source_dir}"
    -B "${fixture_binary_dir}"
    -G "${TURBO_FLOW_GENERATOR}"
    "-DTURBO_FLOW_SOURCE_DIR=${TURBO_FLOW_SOURCE_DIR}"
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
