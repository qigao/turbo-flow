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
set(full_consumer_build_dir "${test_root}/full-build")
set(full_consumer_source_dir "${TURBO_FLOW_SOURCE_DIR}/tests/install_consumer")
set(component_consumer_source_dir "${full_consumer_source_dir}/component")

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

function(run_expected_failure operation expected_pattern)
  execute_process(
    COMMAND ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if(result EQUAL 0)
    message(FATAL_ERROR "${operation} unexpectedly succeeded")
  endif()
  string(CONCAT diagnostic "${output}" "\n" "${error}")
  if(NOT diagnostic MATCHES "${expected_pattern}")
    message(FATAL_ERROR
            "${operation} failed without '${expected_pattern}'\n${diagnostic}")
  endif()
endfunction()

run_checked(
  "TurboFlow install"
  "${CMAKE_COMMAND}" --install "${TURBO_FLOW_BINARY_DIR}"
  --prefix "${stage_dir}" --config "${TURBO_FLOW_CONFIG}")

set(turbo_flow_package_dir "${stage_dir}/lib/cmake/TurboFlow")
set(salts_package_dir "${salts_root}/lib/cmake/Salts")
set(salts_utils_package_dir "${salts_utils_root}/lib/cmake/SaltsUtils")
set(rules_forge_package_dir "${rules_forge_root}/lib/cmake/RulesForge")

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

set(config_consumer_build_dir "${test_root}/config-build")
run_checked(
  "Config-only consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  --unset=SALTS_UTILS_ROOT
  --unset=RULES_FORGE_ROOT
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${config_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  -DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE
  -DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE
  -DTURBO_FLOW_TEST_COMPONENT=Config)
run_checked(
  "Config-only consumer build"
  "${CMAKE_COMMAND}" --build "${config_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)
run_checked(
  "Config-only consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${config_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

run_checked(
  "full consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${full_consumer_source_dir}"
  -B "${full_consumer_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  -DTURBO_FLOW_TEST_ALL_COMPONENTS=TRUE)

run_checked(
  "full consumer build"
  "${CMAKE_COMMAND}" --build "${full_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

run_checked(
  "full consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${full_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

set(unscoped_consumer_build_dir "${test_root}/unscoped-build")
run_checked(
  "consumer without components configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${full_consumer_source_dir}"
  -B "${unscoped_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}")
run_checked(
  "consumer without components build"
  "${CMAKE_COMMAND}" --build "${unscoped_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)
run_checked(
  "consumer without components execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${unscoped_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

set(graph_consumer_build_dir "${test_root}/graph-build")
run_checked(
  "Graph consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${graph_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  -DTURBO_FLOW_TEST_COMPONENT=Graph)
run_checked(
  "Graph consumer build"
  "${CMAKE_COMMAND}" --build "${graph_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)
run_checked(
  "Graph consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${graph_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

run_expected_failure(
  "Config-only consumer configure without SALTS_ROOT"
  "SALTS_ROOT"
  "${CMAKE_COMMAND}" -E env
  --unset=SALTS_ROOT
  --unset=SALTS_UTILS_ROOT
  --unset=RULES_FORGE_ROOT
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${test_root}/config-missing-root-build"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  -DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE
  -DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE
  -DTURBO_FLOW_TEST_COMPONENT=Config)

run_expected_failure(
  "Graph consumer configure without RULES_FORGE_ROOT"
  "RULES_FORGE_ROOT"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  --unset=RULES_FORGE_ROOT
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${test_root}/graph-missing-dependency-build"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  -DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE
  -DTURBO_FLOW_TEST_COMPONENT=Graph)

run_expected_failure(
  "unknown component configure"
  "MissingComponent"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${test_root}/unknown-component-build"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  -DTURBO_FLOW_TEST_COMPONENT=MissingComponent)
