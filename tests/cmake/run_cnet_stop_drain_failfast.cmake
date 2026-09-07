foreach(required_variable IN ITEMS
        TURBO_FLOW_SOURCE_DIR TURBO_FLOW_BINARY_DIR TURBO_FLOW_GENERATOR)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

set(fixture_source_dir
    "${TURBO_FLOW_SOURCE_DIR}/tests/cmake/cnet_without_stop_drain")
set(fixture_binary_dir
    "${TURBO_FLOW_BINARY_DIR}/cnet-without-stop-drain")

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
          "CNet capability configure unexpectedly succeeded without stop-drain contract v1")
endif()

set(configure_output "${configure_stdout}\n${configure_stderr}")
if(NOT configure_output MATCHES "Salts::CNet stop-drain contract v1")
  message(FATAL_ERROR
          "CNet capability failure did not report the missing stop-drain contract:\n${configure_output}")
endif()
