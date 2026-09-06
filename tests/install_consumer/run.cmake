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

if(WIN32)
  foreach(required_var IN ITEMS
          TURBO_FLOW_VCPKG_INSTALLED_DIR
          TURBO_FLOW_VCPKG_TARGET_TRIPLET
          TURBO_FLOW_COMPILER_RUNTIME_DIR)
    if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
      message(FATAL_ERROR "Missing required variable: ${required_var}")
    endif()
  endforeach()
endif()

file(TO_CMAKE_PATH "${TURBO_FLOW_SALTS_ROOT}" salts_root)
file(TO_CMAKE_PATH "${TURBO_FLOW_SALTS_UTILS_ROOT}" salts_utils_root)
file(TO_CMAKE_PATH "${TURBO_FLOW_RULES_FORGE_ROOT}" rules_forge_root)

set(test_root "${TURBO_FLOW_BINARY_DIR}/install-consumer-test")
set(stage_dir "${test_root}/stage")
set(consumer_build_dir "${test_root}/build")
set(cxx_consumer_build_dir "${test_root}/cxx-build")
set(rejected_build_dir "${test_root}/rejected-build")
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

function(run_rejected operation expected_text)
  execute_process(
    COMMAND ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if(result EQUAL 0)
    message(FATAL_ERROR
            "${operation} unexpectedly succeeded\nstdout:\n${output}\nstderr:\n${error}")
  endif()
  set(combined_output "${output}\n${error}")
  string(FIND "${combined_output}" "${expected_text}" expected_offset)
  if(expected_offset EQUAL -1)
    message(FATAL_ERROR
            "${operation} failed for the wrong reason\nstdout:\n${output}\nstderr:\n${error}")
  endif()
endfunction()

run_checked(
  "TurboFlow install"
  "${CMAKE_COMMAND}" --install "${TURBO_FLOW_BINARY_DIR}"
  --prefix "${stage_dir}" --config "${TURBO_FLOW_CONFIG}")

set(turbo_flow_dir "${stage_dir}/lib/cmake/TurboFlow")
set(salts_dir "${salts_root}/lib/cmake/Salts")
set(salts_utils_dir "${salts_utils_root}/lib/cmake/SaltsUtils")
set(rules_forge_dir "${rules_forge_root}/lib/cmake/RulesForge")

run_rejected(
  "consumer configure without an exact Salts package directory"
  "Salts_DIR is not a directory"
  "${CMAKE_COMMAND}" -S "${consumer_source_dir}" -B "${rejected_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DCMAKE_PREFIX_PATH=${salts_root}"
  "-DTurboFlow_DIR=${turbo_flow_dir}"
  "-DSalts_DIR=${test_root}/missing-salts"
  "-DSaltsUtils_DIR=${salts_utils_dir}"
  "-DRulesForge_DIR=${rules_forge_dir}")

run_checked(
  "consumer configure"
  "${CMAKE_COMMAND}" -S "${consumer_source_dir}" -B "${consumer_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_dir}"
  "-DSalts_DIR=${salts_dir}"
  "-DSaltsUtils_DIR=${salts_utils_dir}"
  "-DRulesForge_DIR=${rules_forge_dir}")

run_checked(
  "consumer build"
  "${CMAKE_COMMAND}" --build "${consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

run_checked(
  "CXX-only consumer configure"
  "${CMAKE_COMMAND}" -S "${consumer_source_dir}" -B "${cxx_consumer_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTURBO_FLOW_CONSUMER_CXX_ONLY=ON"
  "-DTurboFlow_DIR=${turbo_flow_dir}"
  "-DSalts_DIR=${salts_dir}"
  "-DSaltsUtils_DIR=${salts_utils_dir}"
  "-DRulesForge_DIR=${rules_forge_dir}")

run_checked(
  "CXX-only consumer build"
  "${CMAKE_COMMAND}" --build "${cxx_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

if(WIN32)
  if(TURBO_FLOW_CONFIG STREQUAL "Debug")
    set(vcpkg_runtime_dir
        "${TURBO_FLOW_VCPKG_INSTALLED_DIR}/${TURBO_FLOW_VCPKG_TARGET_TRIPLET}/debug/bin")
  elseif(TURBO_FLOW_CONFIG STREQUAL "Release")
    set(vcpkg_runtime_dir
        "${TURBO_FLOW_VCPKG_INSTALLED_DIR}/${TURBO_FLOW_VCPKG_TARGET_TRIPLET}/bin")
  else()
    message(FATAL_ERROR
            "Unsupported install-consumer runtime configuration: ${TURBO_FLOW_CONFIG}")
  endif()
  if(NOT IS_DIRECTORY "${vcpkg_runtime_dir}")
    message(FATAL_ERROR "vcpkg runtime directory does not exist: ${vcpkg_runtime_dir}")
  endif()
  set(runtime_dirs
      "${vcpkg_runtime_dir}"
      "${TURBO_FLOW_COMPILER_RUNTIME_DIR}"
      "${stage_dir}/bin"
      "${salts_root}/bin"
      "${salts_utils_root}/bin"
      "${rules_forge_root}/bin")
  foreach(runtime_dir IN LISTS runtime_dirs)
    if(NOT IS_DIRECTORY "${runtime_dir}")
      message(FATAL_ERROR "Required runtime directory does not exist: ${runtime_dir}")
    endif()
  endforeach()
  list(JOIN runtime_dirs ";" runtime_path)
  set(ENV{PATH} "${runtime_path}")
elseif(APPLE)
  set(ENV{DYLD_LIBRARY_PATH}
      "${stage_dir}/lib:${salts_root}/lib:${salts_utils_root}/lib:${rules_forge_root}/lib")
else()
  set(ENV{LD_LIBRARY_PATH}
      "${stage_dir}/lib:${salts_root}/lib:${salts_utils_root}/lib:${rules_forge_root}/lib")
endif()

run_checked(
  "consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

run_checked(
  "CXX-only consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${cxx_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)
