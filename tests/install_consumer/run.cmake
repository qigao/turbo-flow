cmake_minimum_required(VERSION 3.20)

foreach(required_var IN ITEMS
        TURBO_FLOW_SOURCE_DIR
        TURBO_FLOW_BINARY_DIR
        TURBO_FLOW_GENERATOR
        TURBO_FLOW_CONFIG
        TURBO_FLOW_CTEST_COMMAND
        TURBO_FLOW_SALTS_ROOT
        TURBO_FLOW_SALTS_UTILS_ROOT
        TURBO_FLOW_RULES_FORGE_ROOT
        TURBO_FLOW_HAS_TURBODB_ADAPTER)
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
if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
  if(NOT DEFINED TURBO_FLOW_TURBODB_ROOT OR
     "${TURBO_FLOW_TURBODB_ROOT}" STREQUAL "" OR
     NOT IS_DIRECTORY "${TURBO_FLOW_TURBODB_ROOT}")
    message(FATAL_ERROR
            "TURBO_FLOW_TURBODB_ROOT is required for the installed TurboDb adapter")
  endif()
  file(TO_CMAKE_PATH "${TURBO_FLOW_TURBODB_ROOT}" turbodb_root)
endif()

set(test_root "${TURBO_FLOW_BINARY_DIR}/install-consumer-test")
set(stage_dir "${test_root}/stage")
set(full_consumer_build_dir "${test_root}/full-build")
set(fixture_stage_dir "${test_root}/fixture-stage")
if(WIN32)
  set(installed_operation_fixture
      "${fixture_stage_dir}/bin/turbo_flow_install_operation_fixture.dll")
elseif(APPLE)
  set(installed_operation_fixture
      "${fixture_stage_dir}/lib/libturbo_flow_install_operation_fixture.dylib")
else()
  set(installed_operation_fixture
      "${fixture_stage_dir}/lib/libturbo_flow_install_operation_fixture.so")
endif()
set(cxx_consumer_build_dir "${test_root}/cxx-build")
set(cnet_plugin_consumer_build_dir "${test_root}/cnet-plugin-build")
set(full_consumer_source_dir "${TURBO_FLOW_SOURCE_DIR}/tests/install_consumer")
set(component_consumer_source_dir "${full_consumer_source_dir}/component")
set(cnet_plugin_consumer_source_dir
    "${TURBO_FLOW_SOURCE_DIR}/tests/install_cnet_plugin_consumer")

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

file(GLOB _turbo_flow_legacy_stage_artifacts
     "${stage_dir}/bin/turbo_flow.dll"
     "${stage_dir}/lib/turbo_flow.lib"
     "${stage_dir}/lib/libturbo_flow.so"
     "${stage_dir}/lib/libturbo_flow.dylib"
     "${stage_dir}/include/turbo_flow_config.h")
if(_turbo_flow_legacy_stage_artifacts)
  message(FATAL_ERROR
          "TurboFlow 2.0 install stage contains retired aggregate artifacts: ${_turbo_flow_legacy_stage_artifacts}")
endif()

set(turbo_flow_package_dir "${stage_dir}/lib/cmake/TurboFlow")
set(salts_package_dir "${salts_root}/lib/cmake/Salts")
set(salts_utils_package_dir "${salts_utils_root}/lib/cmake/SaltsUtils")
set(rules_forge_package_dir "${rules_forge_root}/lib/cmake/RulesForge")
if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
  set(orm_package_dir "${turbodb_root}/lib/cmake/Orm")
  set(turbodb_consumer_env "TURBODB_ROOT=${turbodb_root}")
  set(turbodb_consumer_cmake_args
      "-DOrm_DIR=${orm_package_dir}"
      -DTURBO_FLOW_TEST_HAS_TURBODB_ADAPTER=TRUE)
else()
  set(turbodb_consumer_env)
  set(turbodb_consumer_cmake_args
      -DTURBO_FLOW_TEST_HAS_TURBODB_ADAPTER=FALSE)
endif()

# A package requesting the retired aggregate must fail at component selection,
# before a consumer can link the legacy target.
run_expected_failure(
  "removed Flow component"
  "Unsupported TurboFlow component: Flow"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  ${turbodb_consumer_env}
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${test_root}/removed-flow-build" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  ${turbodb_consumer_cmake_args}
  -DTURBO_FLOW_TEST_COMPONENT=Flow)

run_expected_failure(
  "TurboFlow 1.x request"
  "compatible with requested version"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  ${turbodb_consumer_env}
  "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
  -B "${test_root}/version-1-build" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  ${turbodb_consumer_cmake_args}
  -DTURBO_FLOW_TEST_COMPONENT=Config
  -DTURBO_FLOW_TEST_PACKAGE_VERSION=1.0)

run_checked(
  "CXX-only consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  ${turbodb_consumer_env}
  "${CMAKE_COMMAND}" -S "${full_consumer_source_dir}"
  -B "${cxx_consumer_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTURBO_FLOW_CONSUMER_CXX_ONLY=ON"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  ${turbodb_consumer_cmake_args})

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
  if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
    list(APPEND runtime_dirs "${turbodb_root}/bin")
  endif()
  foreach(runtime_dir IN LISTS runtime_dirs)
    if(NOT IS_DIRECTORY "${runtime_dir}")
      message(FATAL_ERROR "Required runtime directory does not exist: ${runtime_dir}")
    endif()
  endforeach()
  list(JOIN runtime_dirs ";" runtime_path)
  set(ENV{PATH} "${runtime_path};$ENV{PATH}")
elseif(APPLE)
  set(ENV{DYLD_LIBRARY_PATH}
      "${stage_dir}/lib:${salts_root}/lib:${salts_utils_root}/lib:${rules_forge_root}/lib:$ENV{DYLD_LIBRARY_PATH}")
  if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
    set(ENV{DYLD_LIBRARY_PATH}
        "${turbodb_root}/lib:$ENV{DYLD_LIBRARY_PATH}")
  endif()
else()
  set(ENV{LD_LIBRARY_PATH}
      "${stage_dir}/lib:${salts_root}/lib:${salts_utils_root}/lib:${rules_forge_root}/lib:$ENV{LD_LIBRARY_PATH}")
  if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
    set(ENV{LD_LIBRARY_PATH}
        "${turbodb_root}/lib:$ENV{LD_LIBRARY_PATH}")
  endif()
endif()

if(WIN32)
  set(installed_cnet_plugin "${stage_dir}/bin/tf_cnet_plugin.dll")
  set(cnet_plugin_consumer_executable
      "${cnet_plugin_consumer_build_dir}/turbo_flow_cnet_plugin_consumer.exe")
elseif(APPLE)
  set(installed_cnet_plugin "${stage_dir}/lib/libtf_cnet_plugin.dylib")
  set(cnet_plugin_consumer_executable
      "${cnet_plugin_consumer_build_dir}/turbo_flow_cnet_plugin_consumer")
else()
  set(installed_cnet_plugin "${stage_dir}/lib/libtf_cnet_plugin.so")
  set(cnet_plugin_consumer_executable
      "${cnet_plugin_consumer_build_dir}/turbo_flow_cnet_plugin_consumer")
endif()
if(NOT EXISTS "${installed_cnet_plugin}")
  message(FATAL_ERROR "Installed CNet provider DLL is missing: ${installed_cnet_plugin}")
endif()

run_checked(
  "CNet plugin Gateway consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${cnet_plugin_consumer_source_dir}"
  -B "${cnet_plugin_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  "-DTURBO_FLOW_CNET_PLUGIN_PATH=${installed_cnet_plugin}")

run_checked(
  "CNet plugin Gateway consumer build"
  "${CMAKE_COMMAND}" --build "${cnet_plugin_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

if(WIN32)
  set(dumpbin "${TURBO_FLOW_COMPILER_RUNTIME_DIR}/dumpbin.exe")
  if(NOT EXISTS "${dumpbin}")
    message(FATAL_ERROR "Required dumpbin executable does not exist: ${dumpbin}")
  endif()
  execute_process(
    COMMAND "${dumpbin}" /nologo /exports "${stage_dir}/bin/turbo_flow_graph.dll"
    RESULT_VARIABLE graph_export_result
    OUTPUT_VARIABLE graph_export_output
    ERROR_VARIABLE graph_export_error)
  if(NOT graph_export_result EQUAL 0)
    message(FATAL_ERROR
      "Graph export inspection failed (${graph_export_result})\n${graph_export_error}")
  endif()
  foreach(retired_export IN ITEMS turbo_flow_adapter_command turbo_flow_rule_register_data_stage turbo_flow_register_stage_with_resources turbo_flow_register_stage_ex)
    if(graph_export_output MATCHES "[ \t]${retired_export}([ \t\r\n]|$)")
      message(FATAL_ERROR "Graph still exports retired API: ${retired_export}")
    endif()
  endforeach()
  foreach(operation_export IN ITEMS turbo_flow_register_operation turbo_flow_register_operation_provider)
    if(NOT graph_export_output MATCHES "[ \t]${operation_export}([ \t\r\n]|$)")
      message(FATAL_ERROR "Graph is missing the operation registration API: ${operation_export}")
    endif()
  endforeach()
  if(NOT graph_export_output MATCHES "[ \t]turbo_flow_rule_register_data_operation([ \t\r\n]|$)")
    message(FATAL_ERROR "Graph is missing the stable Policy data operation API")
  endif()
  if(NOT graph_export_output MATCHES "[ \t]turbo_flow_resource_command([ \t\r\n]|$)")
    message(FATAL_ERROR "Graph is missing the stable resource command API")
  endif()
  execute_process(
    COMMAND "${dumpbin}" /nologo /exports "${installed_cnet_plugin}"
    RESULT_VARIABLE export_result
    OUTPUT_VARIABLE export_output
    ERROR_VARIABLE export_error)
  if(NOT export_result EQUAL 0)
    message(FATAL_ERROR
            "CNet plugin export inspection failed (${export_result})\n${export_output}\n${export_error}")
  endif()
  string(REPLACE "\r\n" "\n" export_output "${export_output}")
  string(REGEX MATCHALL
         "\n[ \t]+[0-9]+[ \t]+[0-9A-Fa-f]+[ \t]+[0-9A-Fa-f]+[ \t]+[^ \t\r\n]+"
         export_rows "${export_output}")
  list(LENGTH export_rows export_count)
  if(export_count EQUAL 1)
    list(GET export_rows 0 export_row)
    string(REGEX MATCH "[^ \t\r\n]+$" export_name "${export_row}")
  endif()
  if(NOT export_count EQUAL 1 OR
     NOT export_name STREQUAL "turbo_flow_plugin_get_api")
    message(FATAL_ERROR
            "CNet plugin must export only turbo_flow_plugin_get_api\n${export_output}")
  endif()

  execute_process(
    COMMAND "${dumpbin}" /nologo /dependents "${installed_cnet_plugin}"
    RESULT_VARIABLE plugin_dependent_result
    OUTPUT_VARIABLE plugin_dependent_output
    ERROR_VARIABLE plugin_dependent_error)
  if(NOT plugin_dependent_result EQUAL 0 OR
     NOT plugin_dependent_output MATCHES "tf_cnet_adapter\\.dll" OR
     NOT plugin_dependent_output MATCHES "salts_cnet\\.dll" OR
     plugin_dependent_output MATCHES "turbo_flow\\.dll")
    message(FATAL_ERROR
            "CNet plugin must not depend on turbo_flow.dll\n${plugin_dependent_output}\n${plugin_dependent_error}")
  endif()
  if(TURBO_FLOW_CONFIG STREQUAL "Debug")
    if(NOT plugin_dependent_output MATCHES "VCRUNTIME140D\\.dll")
      message(FATAL_ERROR "Debug CNet plugin does not use the Debug CRT\n${plugin_dependent_output}")
    endif()
  elseif(plugin_dependent_output MATCHES "VCRUNTIME140D\\.dll|ucrtbased\\.dll")
    message(FATAL_ERROR "Release CNet plugin depends on the Debug CRT\n${plugin_dependent_output}")
  endif()

  execute_process(
    COMMAND "${dumpbin}" /nologo /dependents "${cnet_plugin_consumer_executable}"
    RESULT_VARIABLE consumer_dependent_result
    OUTPUT_VARIABLE consumer_dependent_output
    ERROR_VARIABLE consumer_dependent_error)
  if(NOT consumer_dependent_result EQUAL 0)
    message(FATAL_ERROR
            "Gateway dependency inspection failed (${consumer_dependent_result})\n${consumer_dependent_output}\n${consumer_dependent_error}")
  endif()
  if(consumer_dependent_output MATCHES "tf_cnet_adapter\\.dll|salts_cnet\\.dll|turbo_flow\\.dll")
    message(FATAL_ERROR
            "Gateway consumer must not link the CNet adapter, CNet runtime, or turbo_flow.dll\n${consumer_dependent_output}")
  endif()
endif()

run_checked(
  "CNet plugin Gateway consumer loopback"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${cnet_plugin_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

run_expected_failure(
  "CNet plugin Gateway missing DLL"
  "failed at plugin DLL load"
  "${cnet_plugin_consumer_executable}"
  "${stage_dir}/bin/missing-cnet-plugin.dll")

if(WIN32)
  set(cnet_missing_dependency_dir "${test_root}/cnet-missing-dependency")
  file(MAKE_DIRECTORY "${cnet_missing_dependency_dir}")
  configure_file(
    "${installed_cnet_plugin}"
    "${cnet_missing_dependency_dir}/tf_cnet_plugin.dll"
    COPYONLY)
  run_expected_failure(
    "CNet plugin Gateway missing transitive dependency"
    "Win32 dynamic library error 126"
    "${cnet_plugin_consumer_executable}"
    "${cnet_missing_dependency_dir}/tf_cnet_plugin.dll")
endif()

include("${TURBO_FLOW_SOURCE_DIR}/tests/install_chttp_plugin_consumer/run.cmake")

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
  ${turbodb_consumer_env}
  "${CMAKE_COMMAND}" -S "${full_consumer_source_dir}"
  -B "${full_consumer_build_dir}"
  -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  ${turbodb_consumer_cmake_args}
  "-DTURBO_FLOW_INSTALL_FIXTURE_PATH=${installed_operation_fixture}"
  -DTURBO_FLOW_TEST_ALL_COMPONENTS=TRUE)

run_checked(
  "full consumer build"
  "${CMAKE_COMMAND}" --build "${full_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

run_checked(
  "installed operation fixture staging"
  "${CMAKE_COMMAND}" --install "${full_consumer_build_dir}"
  --prefix "${fixture_stage_dir}" --config "${TURBO_FLOW_CONFIG}")

if(NOT EXISTS "${installed_operation_fixture}")
  message(FATAL_ERROR "Installed operation fixture is missing: ${installed_operation_fixture}")
endif()

if(WIN32)
  execute_process(
    COMMAND "${dumpbin}" /nologo /exports "${installed_operation_fixture}"
    RESULT_VARIABLE operation_export_result
    OUTPUT_VARIABLE operation_export_output
    ERROR_VARIABLE operation_export_error)
  if(NOT operation_export_result EQUAL 0)
    message(FATAL_ERROR
            "Operation fixture export inspection failed (${operation_export_result})\n${operation_export_error}")
  endif()
  string(REPLACE "\r\n" "\n" operation_export_output "${operation_export_output}")
  string(REGEX MATCHALL
         "\n[ \t]+[0-9]+[ \t]+[0-9A-Fa-f]+[ \t]+[0-9A-Fa-f]+[ \t]+[^ \t\r\n]+"
         operation_export_rows "${operation_export_output}")
  list(LENGTH operation_export_rows operation_export_count)
  if(operation_export_count EQUAL 1)
    list(GET operation_export_rows 0 operation_export_row)
    string(REGEX MATCH "[^ \t\r\n]+$" operation_export_name "${operation_export_row}")
  endif()
  if(NOT operation_export_count EQUAL 1 OR
     NOT operation_export_name STREQUAL "turbo_flow_plugin_get_api")
    message(FATAL_ERROR
            "Operation fixture must export only turbo_flow_plugin_get_api\n${operation_export_output}")
  endif()
endif()

if(WIN32)
  execute_process(
    COMMAND "${dumpbin}" /nologo /dependents "${full_consumer_build_dir}/turbo_flow_install_consumer.exe"
    RESULT_VARIABLE full_consumer_dependent_result
    OUTPUT_VARIABLE full_consumer_dependent_output
    ERROR_VARIABLE full_consumer_dependent_error)
  if(NOT full_consumer_dependent_result EQUAL 0 OR
     full_consumer_dependent_output MATCHES "turbo_flow\\.dll")
    message(FATAL_ERROR
            "TurboFlow 2.0 consumer must not depend on turbo_flow.dll\n${full_consumer_dependent_output}\n${full_consumer_dependent_error}")
  endif()
endif()

run_checked(
  "full consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${full_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

run_checked(
  "CXX-only consumer execution"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${cxx_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

set(unscoped_consumer_build_dir "${test_root}/unscoped-build")
run_checked(
  "consumer without components configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  ${turbodb_consumer_env}
  "${CMAKE_COMMAND}" -S "${full_consumer_source_dir}"
  -B "${unscoped_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  ${turbodb_consumer_cmake_args})
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

if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
  set(turbodb_consumer_build_dir "${test_root}/turbodb-build")
  run_checked(
    "TurboDb adapter consumer configure"
    "${CMAKE_COMMAND}" -E env
    "SALTS_ROOT=${salts_root}"
    "SALTS_UTILS_ROOT=${salts_utils_root}"
    "RULES_FORGE_ROOT=${rules_forge_root}"
    "TURBODB_ROOT=${turbodb_root}"
    "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
    -B "${turbodb_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
    "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
    "-DTurboFlow_DIR=${turbo_flow_package_dir}"
    "-DSalts_DIR=${salts_package_dir}"
    "-DSaltsUtils_DIR=${salts_utils_package_dir}"
    "-DRulesForge_DIR=${rules_forge_package_dir}"
    "-DOrm_DIR=${orm_package_dir}"
    -DTURBO_FLOW_TEST_COMPONENT=TurboDbAdapter)
  run_checked(
    "TurboDb adapter consumer build"
    "${CMAKE_COMMAND}" --build "${turbodb_consumer_build_dir}"
    --config "${TURBO_FLOW_CONFIG}" --parallel)
  run_checked(
    "TurboDb adapter consumer execution"
    "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${turbodb_consumer_build_dir}"
    -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

  run_expected_failure(
    "TurboDb adapter consumer configure without TURBODB_ROOT"
    "TURBODB_ROOT"
    "${CMAKE_COMMAND}" -E env
    "SALTS_ROOT=${salts_root}"
    "SALTS_UTILS_ROOT=${salts_utils_root}"
    "RULES_FORGE_ROOT=${rules_forge_root}"
    --unset=TURBODB_ROOT
    "${CMAKE_COMMAND}" -S "${component_consumer_source_dir}"
    -B "${test_root}/turbodb-missing-root-build"
    -G "${TURBO_FLOW_GENERATOR}"
    "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
    "-DTurboFlow_DIR=${turbo_flow_package_dir}"
    "-DSalts_DIR=${salts_package_dir}"
    "-DSaltsUtils_DIR=${salts_utils_package_dir}"
    "-DRulesForge_DIR=${rules_forge_package_dir}"
    -DTURBO_FLOW_TEST_COMPONENT=TurboDbAdapter)
endif()

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
