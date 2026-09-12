cmake_minimum_required(VERSION 3.20)

foreach(required_var IN ITEMS
        TURBO_FLOW_SOURCE_DIR
        TURBO_FLOW_CONFIG
        TURBO_FLOW_HAS_TURBODB_ADAPTER)
  if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
    message(FATAL_ERROR "Missing required variable: ${required_var}")
  endif()
endforeach()

foreach(required_root IN ITEMS SALTS_ROOT SALTS_UTILS_ROOT RULES_FORGE_ROOT)
  if(NOT DEFINED ENV{${required_root}} OR
     "$ENV{${required_root}}" STREQUAL "" OR
     NOT IS_DIRECTORY "$ENV{${required_root}}")
    message(FATAL_ERROR "${required_root} must name an installed SDK prefix")
  endif()
endforeach()
file(TO_CMAKE_PATH "$ENV{SALTS_ROOT}" salts_root)
file(TO_CMAKE_PATH "$ENV{SALTS_UTILS_ROOT}" salts_utils_root)
file(TO_CMAKE_PATH "$ENV{RULES_FORGE_ROOT}" rules_forge_root)
if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
  if(NOT DEFINED ENV{TURBODB_ROOT} OR
     "$ENV{TURBODB_ROOT}" STREQUAL "" OR
     NOT IS_DIRECTORY "$ENV{TURBODB_ROOT}")
    message(FATAL_ERROR
            "TURBODB_ROOT is required for the installed TurboDb adapter")
  endif()
  file(TO_CMAKE_PATH "$ENV{TURBODB_ROOT}" turbodb_root)
endif()

if(NOT DEFINED ENV{TURBO_FLOW_ACTIVE_PRESET} OR
   "$ENV{TURBO_FLOW_ACTIVE_PRESET}" STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_ACTIVE_PRESET is required")
endif()
set(consumer_profile "$ENV{TURBO_FLOW_ACTIVE_PRESET}")
if(NOT consumer_profile MATCHES "^(win|linux)-(dev|release)-user$")
  message(FATAL_ERROR
          "Unsupported install-consumer profile: ${consumer_profile}")
endif()
if(NOT DEFINED ENV{TURBO_FLOW_ROOT} OR
   "$ENV{TURBO_FLOW_ROOT}" STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_ROOT is required")
endif()
file(TO_CMAKE_PATH "$ENV{TURBO_FLOW_ROOT}" stage_dir)
set(test_root "${TURBO_FLOW_SOURCE_DIR}/tests/install_consumer/build")
set(full_consumer_build_dir "${test_root}/full-${consumer_profile}")
set(fixture_stage_dir "${full_consumer_build_dir}/install")
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
set(cxx_consumer_build_dir "${test_root}/cxx-${consumer_profile}")
set(full_consumer_source_dir "${TURBO_FLOW_SOURCE_DIR}/tests/install_consumer")
set(component_consumer_source_dir "${full_consumer_source_dir}/component")
set(cnet_plugin_consumer_source_dir
    "${TURBO_FLOW_SOURCE_DIR}/tests/install_cnet_plugin_consumer")
set(cnet_plugin_consumer_build_dir
    "${cnet_plugin_consumer_source_dir}/build/${consumer_profile}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset "install-${consumer_profile}"
  WORKING_DIRECTORY "${TURBO_FLOW_SOURCE_DIR}"
  COMMAND_ERROR_IS_FATAL ANY)

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

# A package requesting the retired aggregate must fail at component selection,
# before a consumer can link the legacy target.
foreach(failure_case IN ITEMS removed-flow version-1)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" --fresh --preset
            "component-${failure_case}-${consumer_profile}"
    WORKING_DIRECTORY "${component_consumer_source_dir}"
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_output
    ERROR_VARIABLE configure_error)
  if(configure_result EQUAL 0)
    message(FATAL_ERROR "${failure_case} configure unexpectedly succeeded")
  endif()
  string(CONCAT configure_diagnostic "${configure_output}" "\n${configure_error}")
  if(failure_case STREQUAL "removed-flow" AND
     NOT configure_diagnostic MATCHES "Unsupported TurboFlow component: Flow")
    message(FATAL_ERROR "removed Flow failed without the component diagnostic\n${configure_diagnostic}")
  elseif(failure_case STREQUAL "version-1" AND
         NOT configure_diagnostic MATCHES "compatible with requested version")
    message(FATAL_ERROR "version 1 failed without the version diagnostic\n${configure_diagnostic}")
  endif()
endforeach()

execute_process(
  COMMAND "${CMAKE_COMMAND}" --preset "consumer-cxx-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset "consumer-cxx-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

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

execute_process(
  COMMAND "${CMAKE_COMMAND}" --preset
          "cnet-plugin-consumer-${consumer_profile}"
  WORKING_DIRECTORY "${cnet_plugin_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset
          "cnet-plugin-consumer-${consumer_profile}"
  WORKING_DIRECTORY "${cnet_plugin_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

if(WIN32)
  find_program(dumpbin dumpbin.exe REQUIRED)
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

execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset
          "cnet-plugin-consumer-${consumer_profile}"
  WORKING_DIRECTORY "${cnet_plugin_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

execute_process(
  COMMAND "${cnet_plugin_consumer_executable}"
          "${stage_dir}/bin/missing-cnet-plugin.dll"
  RESULT_VARIABLE missing_cnet_result
  OUTPUT_VARIABLE missing_cnet_output
  ERROR_VARIABLE missing_cnet_error)
string(CONCAT missing_cnet_diagnostic "${missing_cnet_output}" "\n${missing_cnet_error}")
if(missing_cnet_result EQUAL 0 OR
   NOT missing_cnet_diagnostic MATCHES "failed at plugin DLL load")
  message(FATAL_ERROR "CNet missing-DLL negative case failed\n${missing_cnet_diagnostic}")
endif()

if(WIN32)
  set(cnet_missing_dependency_dir "${test_root}/cnet-missing-dependency")
  file(MAKE_DIRECTORY "${cnet_missing_dependency_dir}")
  configure_file(
    "${installed_cnet_plugin}"
    "${cnet_missing_dependency_dir}/tf_cnet_plugin.dll"
    COPYONLY)
  execute_process(
    COMMAND "${cnet_plugin_consumer_executable}"
            "${cnet_missing_dependency_dir}/tf_cnet_plugin.dll"
    RESULT_VARIABLE missing_cnet_dependency_result
    OUTPUT_VARIABLE missing_cnet_dependency_output
    ERROR_VARIABLE missing_cnet_dependency_error)
  string(CONCAT missing_cnet_dependency_diagnostic
         "${missing_cnet_dependency_output}" "\n${missing_cnet_dependency_error}")
  if(missing_cnet_dependency_result EQUAL 0 OR
     NOT missing_cnet_dependency_diagnostic MATCHES "Win32 dynamic library error 126")
    message(FATAL_ERROR
            "CNet missing-transitive negative case failed\n${missing_cnet_dependency_diagnostic}")
  endif()
endif()

include("${TURBO_FLOW_SOURCE_DIR}/tests/install_chttp_plugin_consumer/run.cmake")

set(config_consumer_build_dir
    "${component_consumer_source_dir}/build/config-${consumer_profile}")
execute_process(
  COMMAND "${CMAKE_COMMAND}" --preset "component-config-${consumer_profile}"
  WORKING_DIRECTORY "${component_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset "component-config-${consumer_profile}"
  WORKING_DIRECTORY "${component_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset "component-config-${consumer_profile}"
  WORKING_DIRECTORY "${component_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

execute_process(
  COMMAND "${CMAKE_COMMAND}" --preset "consumer-full-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset "consumer-full-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset
          "install-consumer-full-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

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

execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset "consumer-full-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset "consumer-cxx-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

set(unscoped_consumer_build_dir "${test_root}/unscoped-${consumer_profile}")
execute_process(
  COMMAND "${CMAKE_COMMAND}" --preset "consumer-unscoped-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset "consumer-unscoped-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset "consumer-unscoped-${consumer_profile}"
  WORKING_DIRECTORY "${full_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

set(graph_consumer_build_dir
    "${component_consumer_source_dir}/build/graph-${consumer_profile}")
execute_process(
  COMMAND "${CMAKE_COMMAND}" --preset "component-graph-${consumer_profile}"
  WORKING_DIRECTORY "${component_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset "component-graph-${consumer_profile}"
  WORKING_DIRECTORY "${component_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset "component-graph-${consumer_profile}"
  WORKING_DIRECTORY "${component_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
  set(turbodb_consumer_build_dir
      "${component_consumer_source_dir}/build/turbodb-${consumer_profile}")
  execute_process(COMMAND "${CMAKE_COMMAND}" --preset "component-turbodb-${consumer_profile}"
    WORKING_DIRECTORY "${component_consumer_source_dir}" COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${CMAKE_COMMAND}" --build --preset "component-turbodb-${consumer_profile}"
    WORKING_DIRECTORY "${component_consumer_source_dir}" COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${CMAKE_CTEST_COMMAND}" --preset "component-turbodb-${consumer_profile}"
    WORKING_DIRECTORY "${component_consumer_source_dir}" COMMAND_ERROR_IS_FATAL ANY)
endif()

set(component_negative_cases
    config-missing-root graph-missing-root
    chttp-missing-root chttp-empty-root chttp-wrong-root chttp-empty-sdk
    chttp-outside-cache chttp-preimport-no-provenance
    chttp-preimport-outside chttp-wrong-config unknown)
set(component_negative_patterns
    SALTS_ROOT RULES_FORGE_ROOT
    "HTTP_SERVICES_ROOT is required" "HTTP_SERVICES_ROOT is required"
    "Could not find.*Chttp" "HTTP_SERVICES_ROOT is an empty SDK directory"
    "Chttp_DIR is outside HTTP_SERVICES_ROOT"
    "already imported without verifiable Chttp_DIR provenance"
    "runtime is outside HTTP_SERVICES_ROOT"
    "does not declare requested configuration" MissingComponent)
if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
  list(PREPEND component_negative_cases turbodb-missing-root)
  list(PREPEND component_negative_patterns TURBODB_ROOT)
endif()
list(LENGTH component_negative_cases component_negative_count)
math(EXPR component_negative_last "${component_negative_count} - 1")
foreach(component_negative_index RANGE 0 ${component_negative_last})
  list(GET component_negative_cases ${component_negative_index} component_negative_case)
  list(GET component_negative_patterns ${component_negative_index} component_negative_pattern)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" --fresh --preset
            "component-${component_negative_case}-${consumer_profile}"
    WORKING_DIRECTORY "${component_consumer_source_dir}"
    RESULT_VARIABLE component_negative_result
    OUTPUT_VARIABLE component_negative_output
    ERROR_VARIABLE component_negative_error)
  string(CONCAT component_negative_diagnostic
         "${component_negative_output}" "\n${component_negative_error}")
  if(component_negative_result EQUAL 0 OR
     NOT component_negative_diagnostic MATCHES "${component_negative_pattern}")
    message(FATAL_ERROR
            "${component_negative_case} negative case failed\n${component_negative_diagnostic}")
  endif()
endforeach()
