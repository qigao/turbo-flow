set(chttp_plugin_consumer_build_dir "${test_root}/chttp-plugin-build")
set(chttp_plugin_consumer_source_dir "${TURBO_FLOW_SOURCE_DIR}/tests/install_chttp_plugin_consumer")
if(WIN32)
  set(installed_chttp_plugin "${stage_dir}/bin/tf_chttp_plugin.dll")
  set(chttp_plugin_consumer_executable
      "${chttp_plugin_consumer_build_dir}/turbo_flow_chttp_plugin_consumer.exe")
elseif(APPLE)
  set(installed_chttp_plugin "${stage_dir}/lib/libtf_chttp_plugin.dylib")
  set(chttp_plugin_consumer_executable
      "${chttp_plugin_consumer_build_dir}/turbo_flow_chttp_plugin_consumer")
else()
  set(installed_chttp_plugin "${stage_dir}/lib/libtf_chttp_plugin.so")
  set(chttp_plugin_consumer_executable
      "${chttp_plugin_consumer_build_dir}/turbo_flow_chttp_plugin_consumer")
endif()
if(NOT EXISTS "${installed_chttp_plugin}")
  message(FATAL_ERROR "Installed CHTTP provider DLL is missing: ${installed_chttp_plugin}")
endif()

run_checked(
  "CHTTP plugin Gateway consumer configure"
  "${CMAKE_COMMAND}" -E env
  "SALTS_ROOT=${salts_root}"
  "SALTS_UTILS_ROOT=${salts_utils_root}"
  "RULES_FORGE_ROOT=${rules_forge_root}"
  "${CMAKE_COMMAND}" -S "${chttp_plugin_consumer_source_dir}"
  -B "${chttp_plugin_consumer_build_dir}" -G "${TURBO_FLOW_GENERATOR}"
  "-DCMAKE_BUILD_TYPE=${TURBO_FLOW_CONFIG}"
  "-DTurboFlow_DIR=${turbo_flow_package_dir}"
  "-DSalts_DIR=${salts_package_dir}"
  "-DSaltsUtils_DIR=${salts_utils_package_dir}"
  "-DRulesForge_DIR=${rules_forge_package_dir}"
  "-DTURBO_FLOW_CHTTP_PLUGIN_PATH=${installed_chttp_plugin}")

run_checked(
  "CHTTP plugin Gateway consumer build"
  "${CMAKE_COMMAND}" --build "${chttp_plugin_consumer_build_dir}"
  --config "${TURBO_FLOW_CONFIG}" --parallel)

if(WIN32)
  set(dumpbin "${TURBO_FLOW_COMPILER_RUNTIME_DIR}/dumpbin.exe")
  if(NOT EXISTS "${dumpbin}")
    message(FATAL_ERROR "Required dumpbin executable does not exist: ${dumpbin}")
  endif()
  execute_process(
    COMMAND "${dumpbin}" /nologo /exports "${installed_chttp_plugin}"
    RESULT_VARIABLE export_result
    OUTPUT_VARIABLE export_output
    ERROR_VARIABLE export_error)
  if(NOT export_result EQUAL 0)
    message(FATAL_ERROR
            "CHTTP plugin export inspection failed (${export_result})\n${export_output}\n${export_error}")
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
            "CHTTP plugin must export only turbo_flow_plugin_get_api\n${export_output}")
  endif()

  execute_process(
    COMMAND "${dumpbin}" /nologo /dependents "${installed_chttp_plugin}"
    RESULT_VARIABLE plugin_dependent_result
    OUTPUT_VARIABLE plugin_dependent_output
    ERROR_VARIABLE plugin_dependent_error)
  if(NOT plugin_dependent_result EQUAL 0 OR
     NOT plugin_dependent_output MATCHES "tf_chttp_adapter\\.dll" OR
     NOT plugin_dependent_output MATCHES "salts_chttp(-[0-9]+)?\\.dll")
    message(FATAL_ERROR
            "CHTTP plugin dependency inspection failed\n${plugin_dependent_output}\n${plugin_dependent_error}")
  endif()
  if(TURBO_FLOW_CONFIG STREQUAL "Debug")
    if(NOT plugin_dependent_output MATCHES "VCRUNTIME140D\\.dll")
      message(FATAL_ERROR "Debug CHTTP plugin does not use the Debug CRT\n${plugin_dependent_output}")
    endif()
  elseif(plugin_dependent_output MATCHES "VCRUNTIME140D\\.dll|ucrtbased\\.dll")
    message(FATAL_ERROR "Release CHTTP plugin depends on the Debug CRT\n${plugin_dependent_output}")
  endif()

  execute_process(
    COMMAND "${dumpbin}" /nologo /dependents "${chttp_plugin_consumer_executable}"
    RESULT_VARIABLE consumer_dependent_result
    OUTPUT_VARIABLE consumer_dependent_output
    ERROR_VARIABLE consumer_dependent_error)
  if(NOT consumer_dependent_result EQUAL 0)
    message(FATAL_ERROR
            "Gateway dependency inspection failed (${consumer_dependent_result})\n${consumer_dependent_output}\n${consumer_dependent_error}")
  endif()
  if(consumer_dependent_output MATCHES "tf_chttp_adapter\\.dll|salts_chttp(-[0-9]+)?\\.dll")
    message(FATAL_ERROR
            "Gateway consumer must not link the CHTTP adapter or CHTTP runtime\n${consumer_dependent_output}")
  endif()
endif()

run_checked(
  "CHTTP plugin Gateway consumer lifecycle"
  "${TURBO_FLOW_CTEST_COMMAND}" --test-dir "${chttp_plugin_consumer_build_dir}"
  -C "${TURBO_FLOW_CONFIG}" --output-on-failure)

run_expected_failure(
  "CHTTP plugin Gateway missing DLL"
  "failed at plugin DLL load"
  "${chttp_plugin_consumer_executable}"
  "${stage_dir}/bin/missing-chttp-plugin.dll")

if(WIN32)
  set(chttp_missing_dependency_dir "${test_root}/chttp-missing-dependency")
  file(MAKE_DIRECTORY "${chttp_missing_dependency_dir}")
  configure_file(
    "${installed_chttp_plugin}"
    "${chttp_missing_dependency_dir}/tf_chttp_plugin.dll"
    COPYONLY)
  run_expected_failure(
    "CHTTP plugin Gateway missing transitive dependency"
    "Win32 dynamic library error 126"
    "${chttp_plugin_consumer_executable}"
    "${chttp_missing_dependency_dir}/tf_chttp_plugin.dll")
endif()
