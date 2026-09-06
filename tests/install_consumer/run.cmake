cmake_minimum_required(VERSION 3.20)

foreach(required_var IN ITEMS
        TURBO_FLOW_SOURCE_DIR
        TURBO_FLOW_BINARY_DIR
        TURBO_FLOW_GENERATOR
        TURBO_FLOW_CONFIG
        TURBO_FLOW_CTEST_COMMAND
        TURBO_FLOW_SALTS_ROOT
        TURBO_FLOW_SALTS_UTILS_ROOT
        TURBO_FLOW_RULES_FORGE_ROOT)
  if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
    message(FATAL_ERROR "Missing required variable: ${required_var}")
  endif()
endforeach()

file(TO_CMAKE_PATH "${TURBO_FLOW_SALTS_ROOT}" salts_root)
file(TO_CMAKE_PATH "${TURBO_FLOW_SALTS_UTILS_ROOT}" salts_utils_root)
file(TO_CMAKE_PATH "${TURBO_FLOW_RULES_FORGE_ROOT}" rules_forge_root)

set(test_root "${TURBO_FLOW_BINARY_DIR}/install-consumer-test")
set(stage_dir "${test_root}/stage")
set(consumer_build_dir "${test_root}/build")
set(consumer_source_dir "${TURBO_FLOW_SOURCE_DIR}/tests/install_consumer")

file(REMOVE_RECURSE "${test_root}")

function(run_checked operation)
  execute_process(
    COMMAND ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR
            "${operation} failed (${result})\nstdout:\n${output}\nstderr:\n${error}")
  endif()
endfunction()

run_checked(
  "TurboFlow install"
  "${CMAKE_COMMAND}" --install "${TURBO_FLOW_BINARY_DIR}"
  --prefix "${stage_dir}" --config "${TURBO_FLOW_CONFIG}")

set(package_prefixes
    "${stage_dir};${salts_root};${salts_utils_root};${rules_forge_root}")
string(REPLACE ";" "\\;" package_prefixes_arg "${package_prefixes}")
run_checked(
  "consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${consumer_source_dir}" -B "${consumer_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DCMAKE_PREFIX_PATH=${package_prefixes_arg}")

run_checked(
  "consumer build"
  "${CMAKE_COMMAND}" --build "${consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

if(WIN32)
  set(ENV{PATH}
      "${stage_dir}/bin;${salts_root}/bin;${salts_utils_root}/bin;${rules_forge_root}/bin;$ENV{PATH}")
elseif(APPLE)
  set(ENV{DYLD_LIBRARY_PATH}
      "${stage_dir}/lib:${salts_root}/lib:${salts_utils_root}/lib:${rules_forge_root}/lib:$ENV{DYLD_LIBRARY_PATH}")
else()
  set(ENV{LD_LIBRARY_PATH}
      "${stage_dir}/lib:${salts_root}/lib:${salts_utils_root}/lib:${rules_forge_root}/lib:$ENV{LD_LIBRARY_PATH}")
endif()

run_checked(
  "consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)
