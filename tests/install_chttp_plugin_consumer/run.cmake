set(chttp_plugin_consumer_source_dir
    "${TURBO_FLOW_SOURCE_DIR}/tests/install_chttp_plugin_consumer")
set(chttp_plugin_consumer_build_dir
    "${chttp_plugin_consumer_source_dir}/build/${consumer_profile}")
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

execute_process(
  COMMAND "${CMAKE_COMMAND}" --fresh --preset
          "chttp-plugin-consumer-${consumer_profile}"
  WORKING_DIRECTORY "${chttp_plugin_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build --preset
          "chttp-plugin-consumer-${consumer_profile}"
  WORKING_DIRECTORY "${chttp_plugin_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

if(WIN32)
  find_program(dumpbin dumpbin.exe REQUIRED)
  set(native_negative_inputs
      "salts_chttp.dll chttp_client.dll chttp_server.dll"
      "salts_chttp-2.dll chttp_client.dll chttp_server.dll"
      "chttp_clientXdll chttp_client.dll chttp_server.dll"
      "chttp_client.dll"
      "chttp_client.dll chttp_client.dll chttp_server.dll")
  set(native_negative_patterns
      "must not contain a legacy salts_chttp DLL"
      "must not contain a legacy salts_chttp DLL"
      "unknown native import"
      "requires exactly one chttp_server.dll import"
      "requires exactly one chttp_client.dll import")
  foreach(native_negative_index RANGE 0 4)
    list(GET native_negative_inputs ${native_negative_index} native_negative_input)
    list(GET native_negative_patterns ${native_negative_index} native_negative_pattern)
    execute_process(
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=${native_negative_input}"
              -P "${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake"
      RESULT_VARIABLE native_negative_result
      OUTPUT_VARIABLE native_negative_output
      ERROR_VARIABLE native_negative_error)
    string(CONCAT native_negative_diagnostic
           "${native_negative_output}" "\n${native_negative_error}")
    if(native_negative_result EQUAL 0 OR
       NOT native_negative_diagnostic MATCHES "${native_negative_pattern}")
      message(FATAL_ERROR
              "CHTTP native negative case ${native_negative_index} failed\n${native_negative_diagnostic}")
    endif()
  endforeach()
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
            "-DCHTTP_TEST_DEPENDENTS=chttp_client.dll chttp_server.dll"
            -P "${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake"
    COMMAND_ERROR_IS_FATAL ANY)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=gateway
            "-DCHTTP_TEST_DEPENDENTS=KERNEL32.dll"
            -P "${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake"
    COMMAND_ERROR_IS_FATAL ANY)
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
     plugin_dependent_output MATCHES "turbo_flow\\.dll")
    message(FATAL_ERROR
            "CHTTP plugin must not depend on turbo_flow.dll\n${plugin_dependent_output}\n${plugin_dependent_error}")
  endif()
  set(CHTTP_TEST_LAYER provider)
  set(CHTTP_TEST_DEPENDENTS "${plugin_dependent_output}")
  include("${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake")
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
  if(consumer_dependent_output MATCHES "tf_chttp_adapter\\.dll|salts_chttp(-[0-9]+)?\\.dll|turbo_flow\\.dll")
    message(FATAL_ERROR
            "Gateway consumer must not link the CHTTP adapter, CHTTP runtime, or turbo_flow.dll\n${consumer_dependent_output}")
  endif()
  set(CHTTP_TEST_LAYER gateway)
  set(CHTTP_TEST_DEPENDENTS "${consumer_dependent_output}")
  include("${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake")

  execute_process(
    COMMAND "${dumpbin}" /nologo /dependents "${stage_dir}/bin/tf_chttp_adapter.dll"
    RESULT_VARIABLE adapter_dependent_result
    OUTPUT_VARIABLE adapter_dependent_output
    ERROR_VARIABLE adapter_dependent_error)
  if(NOT adapter_dependent_result EQUAL 0)
    message(FATAL_ERROR
            "CHTTP adapter dependency inspection failed (${adapter_dependent_result})\n${adapter_dependent_output}\n${adapter_dependent_error}")
  endif()
  set(CHTTP_TEST_LAYER adapter)
  set(CHTTP_TEST_DEPENDENTS "${adapter_dependent_output}")
  include("${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake")
  set(chttp_closure_roots
      "${stage_dir}" "$ENV{HTTP_SERVICES_ROOT}" "${salts_root}"
      "${salts_utils_root}" "${rules_forge_root}")
  set(chttp_closure_search_dirs
      "${stage_dir}/bin" "$ENV{HTTP_SERVICES_ROOT}/bin" "${salts_root}/bin"
      "${salts_utils_root}/bin" "${rules_forge_root}/bin")
  if(TURBO_FLOW_HAS_TURBODB_ADAPTER)
    list(APPEND chttp_closure_roots "${turbodb_root}")
    list(APPEND chttp_closure_search_dirs "${turbodb_root}/bin")
  endif()
  if(TURBO_FLOW_CONFIG STREQUAL "Debug")
    list(APPEND chttp_closure_roots
         "$ENV{VCPKG_INSTALLED_DIR}/x64-windows/debug")
    list(APPEND chttp_closure_search_dirs
         "$ENV{VCPKG_INSTALLED_DIR}/x64-windows/debug/bin")
  else()
    list(APPEND chttp_closure_roots
         "$ENV{VCPKG_INSTALLED_DIR}/x64-windows")
    list(APPEND chttp_closure_search_dirs
         "$ENV{VCPKG_INSTALLED_DIR}/x64-windows/bin")
  endif()
  string(REPLACE ";" "__CHTTP_LIST__" chttp_closure_roots_arg
         "${chttp_closure_roots}")
  string(REPLACE ";" "__CHTTP_LIST__" chttp_closure_search_dirs_arg
         "${chttp_closure_search_dirs}")
  execute_process(
    COMMAND "${CMAKE_COMMAND}"
            "-DCHTTP_TEST_MODULE=${stage_dir}/bin/tf_chttp_adapter.dll"
            "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_roots_arg}"
            "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_search_dirs_arg}"
            "-DCHTTP_TEST_CONFIG=${TURBO_FLOW_CONFIG}"
            -P "${CMAKE_CURRENT_LIST_DIR}/check_native_abi.cmake"
    COMMAND_ERROR_IS_FATAL ANY)
  if(TURBO_FLOW_CONFIG STREQUAL "Debug")
    if(NOT adapter_dependent_output MATCHES "VCRUNTIME140D\\.dll")
      message(FATAL_ERROR
              "Debug CHTTP adapter does not use the Debug CRT\n${adapter_dependent_output}")
    endif()
  elseif(adapter_dependent_output MATCHES "VCRUNTIME140D\\.dll|ucrtbased\\.dll")
    message(FATAL_ERROR
            "Release CHTTP adapter depends on the Debug CRT\n${adapter_dependent_output}")
  endif()

  foreach(chttp_native_dll IN ITEMS chttp_client.dll chttp_server.dll)
    set(chttp_native_path "$ENV{HTTP_SERVICES_ROOT}/bin/${chttp_native_dll}")
    if(NOT EXISTS "${chttp_native_path}")
      message(FATAL_ERROR "Installed Chttp native DLL is missing: ${chttp_native_path}")
    endif()
    execute_process(
      COMMAND "${dumpbin}" /nologo /dependents "${chttp_native_path}"
      RESULT_VARIABLE native_dependent_result
      OUTPUT_VARIABLE native_dependent_output
      ERROR_VARIABLE native_dependent_error)
    if(NOT native_dependent_result EQUAL 0)
      message(FATAL_ERROR
              "${chttp_native_dll} dependency inspection failed (${native_dependent_result})\n${native_dependent_output}\n${native_dependent_error}")
    endif()
    if(native_dependent_output MATCHES "salts_chttp[^ \t\r\n]*\\.dll")
      message(FATAL_ERROR
              "Standalone Chttp closure contains a legacy salts_chttp DLL\n${native_dependent_output}")
    endif()
    if(TURBO_FLOW_CONFIG STREQUAL "Debug")
      if(NOT native_dependent_output MATCHES "VCRUNTIME140D\\.dll")
        message(FATAL_ERROR
                "Debug ${chttp_native_dll} does not use the Debug CRT\n${native_dependent_output}")
      endif()
    elseif(native_dependent_output MATCHES "VCRUNTIME140D\\.dll|ucrtbased\\.dll")
      message(FATAL_ERROR
              "Release ${chttp_native_dll} depends on the Debug CRT\n${native_dependent_output}")
    endif()
  endforeach()
endif()

execute_process(
  COMMAND "${CMAKE_CTEST_COMMAND}" --preset
          "chttp-plugin-consumer-${consumer_profile}"
  WORKING_DIRECTORY "${chttp_plugin_consumer_source_dir}"
  COMMAND_ERROR_IS_FATAL ANY)

execute_process(
  COMMAND "${chttp_plugin_consumer_executable}"
          "${stage_dir}/bin/missing-chttp-plugin.dll"
  RESULT_VARIABLE missing_chttp_result
  OUTPUT_VARIABLE missing_chttp_output
  ERROR_VARIABLE missing_chttp_error)
string(CONCAT missing_chttp_diagnostic
       "${missing_chttp_output}" "\n${missing_chttp_error}")
if(missing_chttp_result EQUAL 0 OR
   NOT missing_chttp_diagnostic MATCHES "failed at plugin DLL load")
  message(FATAL_ERROR "CHTTP missing-DLL negative case failed\n${missing_chttp_diagnostic}")
endif()

if(WIN32)
  set(chttp_missing_dependency_dir
      "${chttp_plugin_consumer_build_dir}/missing-dependency")
  file(MAKE_DIRECTORY "${chttp_missing_dependency_dir}")
  configure_file(
    "${installed_chttp_plugin}"
    "${chttp_missing_dependency_dir}/tf_chttp_plugin.dll"
    COPYONLY)
  execute_process(
    COMMAND "${chttp_plugin_consumer_executable}"
            "${chttp_missing_dependency_dir}/tf_chttp_plugin.dll"
    RESULT_VARIABLE missing_chttp_dependency_result
    OUTPUT_VARIABLE missing_chttp_dependency_output
    ERROR_VARIABLE missing_chttp_dependency_error)
  string(CONCAT missing_chttp_dependency_diagnostic
         "${missing_chttp_dependency_output}" "\n${missing_chttp_dependency_error}")
  if(missing_chttp_dependency_result EQUAL 0 OR
     NOT missing_chttp_dependency_diagnostic MATCHES "Win32 dynamic library error 126")
    message(FATAL_ERROR
            "CHTTP missing-transitive negative case failed\n${missing_chttp_dependency_diagnostic}")
  endif()
endif()
