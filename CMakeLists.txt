cmake_minimum_required(VERSION 3.20)
if(POLICY CMP0144)
  cmake_policy(SET CMP0144 OLD)
endif()
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()

project(
  TurboFlow
  VERSION 2.0.0
  LANGUAGES C CXX)

# Project configuration - output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Normalize install prefix (remove trailing slashes)
get_filename_component(CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}" ABSOLUTE)

# Project description
set(PROJECT_DESCRIPTION "High-performance data process library")
set(PROJECT_HOMEPAGE_URL "https://github.com/qigao/turboflow")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
# Language standards - unified approach
set(CMAKE_CXX_STANDARD 17) # C++17 for compatibility
set(CMAKE_C_STANDARD 11) # C17 to ensure C11 atomic support
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(MSVC)
  add_compile_options(/experimental:c11atomics)
endif()
# Enable folder organization in IDEs
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set_property(GLOBAL PROPERTY PREDEFINED_TARGETS_FOLDER "cmake")

# Enable testing
include(CTest)
enable_testing()

include(CMakeOptions.cmake)
include(Sanitizers)
include(CmakeUtils)

set(_turbo_flow_required_dependency_roots
    SALTS_ROOT SALTS_UTILS_ROOT DATABIND_ROOT RULES_FORGE_ROOT)
if(TURBO_FLOW_BUILD_TURBODB_ADAPTER)
  list(APPEND _turbo_flow_required_dependency_roots TURBODB_ROOT)
endif()
foreach(_turbo_flow_dependency_root IN LISTS
        _turbo_flow_required_dependency_roots)
  if(NOT DEFINED ENV{${_turbo_flow_dependency_root}} OR
     "$ENV{${_turbo_flow_dependency_root}}" STREQUAL "")
    message(FATAL_ERROR
            "Required dependency root is not defined: ${_turbo_flow_dependency_root}")
  endif()
  if(NOT IS_DIRECTORY "$ENV{${_turbo_flow_dependency_root}}")
    message(FATAL_ERROR
            "${_turbo_flow_dependency_root} is not a directory: $ENV{${_turbo_flow_dependency_root}}")
  endif()
endforeach()

find_package(Salts CONFIG REQUIRED
             PATHS "$ENV{SALTS_ROOT}" NO_DEFAULT_PATH)
find_package(SaltsUtils CONFIG REQUIRED
             PATHS "$ENV{SALTS_UTILS_ROOT}" NO_DEFAULT_PATH)

foreach(_turbo_flow_stale_databind_target IN ITEMS
    Salts::DataBind Salts::DataBindCore Salts::DataBindCMeta
    Salts::DataBindCFlow Salts::TbeSchema)
  if(TARGET ${_turbo_flow_stale_databind_target})
    message(FATAL_ERROR
      "SALTS_UTILS_ROOT still owns DataBind target: ${_turbo_flow_stale_databind_target}")
  endif()
endforeach()

find_package(DataBind 3 CONFIG REQUIRED
             PATHS "$ENV{DATABIND_ROOT}" NO_DEFAULT_PATH)
if(NOT TARGET Salts::DataBind OR NOT TARGET Salts::TbeSchema)
  message(FATAL_ERROR
    "DATABIND_ROOT does not provide the canonical DataBind base targets")
endif()

include(TurboFlowRequireCNet)
turbo_flow_require_cnet_stop_drain_contract()
include(TurboFlowRequireCHTTP)
find_package(xxHash CONFIG REQUIRED)

if(TURBO_FLOW_BUILD_TURBODB_ADAPTER)
  find_package(Orm CONFIG REQUIRED PATHS "$ENV{TURBODB_ROOT}" NO_DEFAULT_PATH)
  file(REAL_PATH "$ENV{TURBODB_ROOT}" TURBO_FLOW_TURBODB_REAL_ROOT)
  file(REAL_PATH "${Orm_DIR}" TURBO_FLOW_ORM_REAL_DIR)
  cmake_path(IS_PREFIX TURBO_FLOW_TURBODB_REAL_ROOT
             "${TURBO_FLOW_ORM_REAL_DIR}" NORMALIZE
             TURBO_FLOW_ORM_IS_WITHIN_TURBODB_ROOT)
  if(NOT TURBO_FLOW_ORM_IS_WITHIN_TURBODB_ROOT)
    message(FATAL_ERROR
            "Orm resolved outside TURBODB_ROOT: ${Orm_DIR}")
  endif()
endif()

if(WIN32)
  find_program(
    TURBO_FLOW_POWERSHELL_EXECUTABLE
    NAMES pwsh.exe powershell.exe
    REQUIRED)

  set(TURBO_FLOW_EXTERNAL_PACKAGE_ROOTS
      "$ENV{SALTS_ROOT}"
      "$ENV{SALTS_UTILS_ROOT}"
      "$ENV{DATABIND_ROOT}"
      "$ENV{RULES_FORGE_ROOT}")
  if(TURBO_FLOW_BUILD_TURBODB_ADAPTER)
    list(APPEND TURBO_FLOW_EXTERNAL_PACKAGE_ROOTS "$ENV{TURBODB_ROOT}")
  endif()
  list(REMOVE_DUPLICATES TURBO_FLOW_EXTERNAL_PACKAGE_ROOTS)

  set(TURBO_FLOW_RUNTIME_COPY_COMMANDS)
  foreach(package_root IN LISTS TURBO_FLOW_EXTERNAL_PACKAGE_ROOTS)
    if(NOT IS_DIRECTORY "${package_root}/bin")
      message(
        FATAL_ERROR
          "External package runtime directory does not exist: ${package_root}/bin")
    endif()
    list(
      APPEND
      TURBO_FLOW_RUNTIME_COPY_COMMANDS
      COMMAND
      "${TURBO_FLOW_POWERSHELL_EXECUTABLE}"
      -NoProfile
      -NonInteractive
      -ExecutionPolicy
      Bypass
      -File
      "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CopyExternalRuntime.ps1"
      -PackageRoot
      "${package_root}"
      -Destination
      "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
  endforeach()

  add_custom_target(
    turbo_flow_external_runtime
    ${TURBO_FLOW_RUNTIME_COPY_COMMANDS}
    COMMENT "Syncing external package DLLs to the runtime output directory"
    VERBATIM)
  set_target_properties(turbo_flow_external_runtime PROPERTIES FOLDER "cmake")
endif()

# Additional components
add_subdirectory(tools)
include(FindTools)
add_subdirectory(vendor)

add_subdirectory(turbo_flow)
add_subdirectory(io/cnet)
add_subdirectory(io/chttp)
if(TURBO_FLOW_BUILD_TURBODB_ADAPTER)
  add_subdirectory(io/turbodb)
endif()
  add_subdirectory(security)
  add_subdirectory(codec)
  add_subdirectory(observe)
  add_subdirectory(schedule)
  add_subdirectory(ingress/protocol)

set(TURBO_FLOW_EXPORT_TARGETS)
foreach(
  target_name
  IN ITEMS
     turbo_flow_config
     turbo_flow_graph
     turbo_flow_product
     turbo_flow_plugin_host
     tf_cnet_adapter
     tf_chttp_adapter
     tf_turbodb_adapter
     tf_protocol
     tf_protocol_inbox_schema
     tf_protocol_inbox
     tf_protocol_mqtt_sn
     tf_protocol_coap
     tf_protocol_lwm2m
     tf_protocol_ocpp
     tf_protocol_gbt32960
     tf_protocol_jtt808
     tf_protocol_business_ocpp201_core
     tf_security_sqlite
     tf_codec
     tf_observe
     tf_schedule)
  if(TARGET ${target_name})
    list(APPEND TURBO_FLOW_EXPORT_TARGETS ${target_name})
  endif()
endforeach()

install(
  TARGETS ${TURBO_FLOW_EXPORT_TARGETS}
  EXPORT TurboFlowTargets
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  INCLUDES DESTINATION include)

# Package Configuration
include(CMakePackageConfigHelpers)

# Generate the config file
set(TURBO_FLOW_HAS_TURBODB_ADAPTER ${TURBO_FLOW_BUILD_TURBODB_ADAPTER})
configure_package_config_file(
  "${CMAKE_SOURCE_DIR}/cmake/TurboFlowConfig.cmake.in"
  "${CMAKE_BINARY_DIR}/TurboFlowConfig.cmake"
  INSTALL_DESTINATION "lib/cmake/TurboFlow")

# Generate the version file
write_basic_package_version_file(
  "${CMAKE_BINARY_DIR}/TurboFlowConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

# Install the config files
install(FILES "${CMAKE_BINARY_DIR}/TurboFlowConfig.cmake"
              "${CMAKE_BINARY_DIR}/TurboFlowConfigVersion.cmake"
              "${CMAKE_SOURCE_DIR}/cmake/TurboFlowRequireCNet.cmake"
              "${CMAKE_SOURCE_DIR}/cmake/TurboFlowRequireCHTTP.cmake"
        DESTINATION "lib/cmake/TurboFlow")

# Install the export set
export(
  EXPORT TurboFlowTargets
  FILE "${CMAKE_BINARY_DIR}/TurboFlowTargets.cmake"
  NAMESPACE TurboFlow::)

install(
  EXPORT TurboFlowTargets
  FILE TurboFlowTargets.cmake
  NAMESPACE TurboFlow::
  DESTINATION "lib/cmake/TurboFlow")

if(BUILD_TESTING AND BUILD_TESTS)
  if(WIN32)
    get_filename_component(TURBO_FLOW_COMPILER_RUNTIME_DIR
                           "${CMAKE_C_COMPILER}" DIRECTORY)
  endif()
  add_test(
    NAME test_turbo_flow_cmake_tools_failfast
    COMMAND
      "${CMAKE_COMMAND}"
      "-DTURBO_FLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
      -P "${CMAKE_SOURCE_DIR}/tests/cmake/run_find_tools_failfast.cmake")
  set_tests_properties(
    test_turbo_flow_cmake_tools_failfast
    PROPERTIES LABELS "cmake;tools" RUN_SERIAL TRUE TIMEOUT 30)

  add_test(
    NAME test_turbo_flow_cnet_stop_drain_failfast
    COMMAND
      "${CMAKE_COMMAND}"
      "-DTURBO_FLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
      -P "${CMAKE_SOURCE_DIR}/tests/cmake/run_cnet_stop_drain_failfast.cmake")
  set_tests_properties(
    test_turbo_flow_cnet_stop_drain_failfast
    PROPERTIES LABELS "cmake;cnet-adapter" RUN_SERIAL TRUE TIMEOUT 30)

  add_test(
    NAME test_turbo_flow_chttp_package
    COMMAND "${CMAKE_COMMAND}"
            "-DTURBO_FLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
            -P "${CMAKE_SOURCE_DIR}/tests/cmake/chttp_package/run.cmake")
  set_tests_properties(test_turbo_flow_chttp_package PROPERTIES
    LABELS "cmake;chttp-adapter" RUN_SERIAL TRUE TIMEOUT 120)

  if(WIN32)
    add_test(NAME test_turbo_flow_chttp_native_adapter
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=chttp_client.dll chttp_server.dll"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_legacy_unversioned
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=salts_chttp.dll chttp_client.dll chttp_server.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=must not contain a legacy salts_chttp DLL"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_legacy_versioned
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=salts_chttp-2.dll chttp_client.dll chttp_server.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=must not contain a legacy salts_chttp DLL"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_wrong_basename
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=chttp_clientXdll chttp_client.dll chttp_server.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=unknown native import"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_missing_server
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=chttp_client.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=requires exactly one chttp_server.dll import"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_duplicate_client
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=adapter
              "-DCHTTP_TEST_DEPENDENTS=chttp_client.dll chttp_client.dll chttp_server.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=requires exactly one chttp_client.dll import"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_gateway_concrete
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=gateway
              "-DCHTTP_TEST_DEPENDENTS=KERNEL32.dll chttp_client.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=Gateway must not link a concrete HTTP implementation"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_native_provider_missing_adapter
      COMMAND "${CMAKE_COMMAND}" -DCHTTP_TEST_LAYER=provider
              "-DCHTTP_TEST_DEPENDENTS=KERNEL32.dll"
              "-DCHTTP_EXPECT_FAILURE_PATTERN=provider requires exactly one tf_chttp_adapter.dll import"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    set_tests_properties(test_turbo_flow_chttp_native_adapter PROPERTIES
      LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(
      test_turbo_flow_chttp_native_legacy_unversioned
      test_turbo_flow_chttp_native_legacy_versioned
      test_turbo_flow_chttp_native_wrong_basename
      test_turbo_flow_chttp_native_missing_server
      test_turbo_flow_chttp_native_duplicate_client
      test_turbo_flow_chttp_native_gateway_concrete
      test_turbo_flow_chttp_native_provider_missing_adapter
      PROPERTIES LABELS "cmake;chttp-adapter" TIMEOUT 30)

    set(chttp_closure_fixture_dir
        "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/closure_fixture")
    set(CHTTP_TEST_POWERSHELL_EXECUTABLE
        "$ENV{WINDIR}/System32/WindowsPowerShell/v1.0/powershell.exe")
    if(NOT EXISTS "${CHTTP_TEST_POWERSHELL_EXECUTABLE}")
      message(FATAL_ERROR
              "Windows CHTTP closure tests require the built-in Windows PowerShell host")
    endif()
    execute_process(
      COMMAND "${CHTTP_TEST_POWERSHELL_EXECUTABLE}" -NoProfile -NonInteractive
              -Command "& { $ErrorActionPreference = 'Stop'; Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Security/Microsoft.PowerShell.Security.psd1'); Get-Command Get-AuthenticodeSignature -ErrorAction Stop | Out-Null }"
      RESULT_VARIABLE CHTTP_TEST_POWERSHELL_CAPABILITY_RESULT)
    if(NOT CHTTP_TEST_POWERSHELL_CAPABILITY_RESULT EQUAL 0)
      message(FATAL_ERROR
              "Windows CHTTP closure tests require the built-in Microsoft.PowerShell.Security module")
    endif()
    find_program(CHTTP_TEST_HOST_DUMPBIN dumpbin REQUIRED NO_CACHE)
    cmake_path(GET CHTTP_TEST_HOST_DUMPBIN PARENT_PATH chttp_signed_fixture_dir)
    file(TO_CMAKE_PATH "$ENV{VCToolsInstallDir}" chttp_test_tools_root)
    cmake_path(IS_PREFIX chttp_test_tools_root "${chttp_signed_fixture_dir}"
               NORMALIZE chttp_fixture_owned_by_tools)
    if(chttp_test_tools_root STREQUAL "" OR NOT chttp_fixture_owned_by_tools OR
       NOT EXISTS "${chttp_signed_fixture_dir}/msobj140.dll")
      message(FATAL_ERROR
              "Windows CHTTP closure tests require msobj140.dll beside the current VsDevCmd dumpbin in VCToolsInstallDir: ${chttp_signed_fixture_dir}")
    endif()
    # A system copy would win native resolution before the explicit fixture directory.
    if(EXISTS "$ENV{WINDIR}/System32/msobj140.dll" OR
       EXISTS "$ENV{WINDIR}/SysWOW64/msobj140.dll")
      message(FATAL_ERROR
              "Windows CHTTP signed non-OS fixture requires no system-directory msobj140.dll shadowing its MSVC source")
    endif()
    string(REPLACE "'" "''" chttp_signed_fixture_argument
           "${chttp_signed_fixture_dir}/msobj140.dll")
    execute_process(
      COMMAND "${CHTTP_TEST_POWERSHELL_EXECUTABLE}" -NoProfile -NonInteractive
              -Command "& { param([string]$Path) $ErrorActionPreference = 'Stop'; Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Security/Microsoft.PowerShell.Security.psd1'); $Signature = Get-AuthenticodeSignature -LiteralPath $Path; if ($Signature.Status -ne 'Valid' -or $Signature.SignerCertificate.Subject -cne 'CN=Microsoft Corporation, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { throw ('CHTTP signed fixture requires a valid Microsoft Corporation signature: ' + $Path) } }"
              "'${chttp_signed_fixture_argument}'"
      RESULT_VARIABLE chttp_signed_fixture_signature_result)
    if(NOT chttp_signed_fixture_signature_result EQUAL 0)
      message(FATAL_ERROR "Windows CHTTP signed non-OS fixture host prerequisite failed")
    endif()
    add_test(NAME test_turbo_flow_chttp_closure_compiler_non_asan_root
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_signed_fixture_dir}/msvcp140.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_signed_fixture_dir}"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_signed_fixture_dir}"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=Compiler-local CRT is not owned by the authenticated ASan runtime"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    set_tests_properties(test_turbo_flow_chttp_closure_compiler_non_asan_root
      PROPERTIES LABELS "cmake;chttp-adapter" TIMEOUT 30)
    foreach(closure_case IN ITEMS asan-outside positive debug-positive legacy wrong-crt wrong-msvcp-crt debug-release-crt unresolved outside arbitrary-outside system-forbidden system-unsigned system-signed-non-os runtime-release runtime-debug asan-positive asan-wrong-crt wrong-vc-suffix wrong-msvcp-suffix debug-vc-suffix debug-msvcp-suffix)
      add_test(NAME test_turbo_flow_chttp_closure_setup_${closure_case}
        COMMAND "${CMAKE_COMMAND}" --workflow --preset "closure-${closure_case}")
      set_tests_properties(test_turbo_flow_chttp_closure_setup_${closure_case}
        PROPERTIES WORKING_DIRECTORY "${chttp_closure_fixture_dir}"
                   FIXTURES_SETUP "chttp-closure-${closure_case}"
                   LABELS "cmake;chttp-adapter" TIMEOUT 120)
    endforeach()
    foreach(closure_case IN ITEMS asan-outside runtime-release runtime-debug asan-positive asan-wrong-crt wrong-vc-suffix wrong-msvcp-suffix debug-vc-suffix debug-msvcp-suffix)
      set(closure_config Release)
      set(closure_expected_failure)
      if(closure_case MATCHES "^(runtime-debug|asan-|debug-)")
        set(closure_config Debug)
      endif()
      if(closure_case MATCHES "^wrong-")
        set(closure_expected_failure "-DCHTTP_EXPECT_FAILURE_PATTERN=Release CHTTP recursive closure contains a Debug CRT")
      elseif(closure_case STREQUAL "asan-outside")
        set(closure_config Release)
        set(closure_expected_failure "-DCHTTP_EXPECT_FAILURE_PATTERN=CHTTP ASan runtime resolved outside the active compiler")
      elseif(closure_case MATCHES "^(debug-|asan-wrong-crt)")
        set(closure_expected_failure "-DCHTTP_EXPECT_FAILURE_PATTERN=Debug CHTTP recursive closure contains a Release CRT")
      endif()
      add_test(NAME test_turbo_flow_chttp_closure_${closure_case}
        COMMAND "${CMAKE_COMMAND}"
                "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/${closure_case}/install/allowed/bin/closure_root.dll"
                "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/${closure_case}/install/allowed"
                "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/${closure_case}/install/allowed/bin"
                "-DCHTTP_TEST_CONFIG=${closure_config}"
                ${closure_expected_failure}
                -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
      set_tests_properties(test_turbo_flow_chttp_closure_${closure_case} PROPERTIES
        FIXTURES_REQUIRED chttp-closure-${closure_case} LABELS "cmake;chttp-adapter" TIMEOUT 30)
    endforeach()
    add_test(NAME test_turbo_flow_chttp_closure_asan_path_shadow
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/asan-positive/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/asan-positive/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/asan-positive/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Debug
              "-DCHTTP_EXPECT_FAILURE_PATTERN=CHTTP ASan runtime PATH provenance does not match the active compiler"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    set_tests_properties(test_turbo_flow_chttp_closure_asan_path_shadow PROPERTIES
      FIXTURES_REQUIRED "chttp-closure-asan-positive;chttp-closure-asan-outside"
      ENVIRONMENT_MODIFICATION "PATH=path_list_prepend:${chttp_closure_fixture_dir}/build/asan-outside/install/allowed/bin"
      LABELS "cmake;chttp-adapter" TIMEOUT 30)
    add_test(NAME test_turbo_flow_chttp_closure_positive
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/positive/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/positive/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/positive/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Release
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_debug_positive
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/debug-positive/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/debug-positive/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/debug-positive/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Debug
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_legacy
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/legacy/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/legacy/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/legacy/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=recursive closure contains a legacy salts_chttp DLL"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_wrong_crt
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/wrong-crt/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/wrong-crt/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/wrong-crt/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=recursive closure contains a Debug CRT"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_wrong_msvcp_crt
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/wrong-msvcp-crt/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/wrong-msvcp-crt/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/wrong-msvcp-crt/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=Release CHTTP recursive closure contains a Debug CRT"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_unresolved
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/unresolved/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/unresolved/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/unresolved/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=closure has unresolved dependencies"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_outside
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/outside/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/outside/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/outside/install/allowed/bin;${chttp_closure_fixture_dir}/build/outside/install/outside/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=resolved outside the active profile roots"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_debug_release_crt
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/debug-release-crt/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/debug-release-crt/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/debug-release-crt/install/allowed/bin"
              -DCHTTP_TEST_CONFIG=Debug
              "-DCHTTP_EXPECT_FAILURE_PATTERN=Debug CHTTP recursive closure contains a Release CRT"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_arbitrary_outside
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/arbitrary-outside/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/arbitrary-outside/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/arbitrary-outside/install/allowed/bin;${chttp_closure_fixture_dir}/build/arbitrary-outside/install/outside/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=non-system dependency resolved outside the active profile roots"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_system_forbidden
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/system-forbidden/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/system-forbidden/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/system-forbidden/install/allowed/bin;${chttp_closure_fixture_dir}/build/system-forbidden/install/system/bin"
              "-DCHTTP_TEST_SYSTEM_ROOTS=${chttp_closure_fixture_dir}/build/system-forbidden/install/system/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=recursive closure contains a legacy salts_chttp DLL"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_system_unsigned
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/system-unsigned/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/system-unsigned/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/system-unsigned/install/allowed/bin;${chttp_closure_fixture_dir}/build/system-unsigned/install/system/bin"
              "-DCHTTP_TEST_SYSTEM_ROOTS=${chttp_closure_fixture_dir}/build/system-unsigned/install/system/bin"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=Status=NotSigned"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    add_test(NAME test_turbo_flow_chttp_closure_system_signed_non_os
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${chttp_closure_fixture_dir}/build/system-signed-non-os/install/allowed/bin/closure_root.dll"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${chttp_closure_fixture_dir}/build/system-signed-non-os/install/allowed"
              "-DCHTTP_TEST_SEARCH_DIRS=${chttp_closure_fixture_dir}/build/system-signed-non-os/install/allowed/bin;${chttp_signed_fixture_dir}"
              "-DCHTTP_TEST_SYSTEM_ROOTS=${chttp_signed_fixture_dir}"
              -DCHTTP_TEST_CONFIG=Release
              "-DCHTTP_EXPECT_FAILURE_PATTERN=msobj140.dll[\r\n ]+Status=Valid[\r\n ]+Publisher=MicrosoftCorporation[\r\n ]+Identity=NonOS"
              -P "${CMAKE_SOURCE_DIR}/tests/install_chttp_plugin_consumer/check_native_abi.cmake")
    set_tests_properties(test_turbo_flow_chttp_closure_legacy PROPERTIES
      FIXTURES_REQUIRED chttp-closure-legacy LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_positive PROPERTIES
      FIXTURES_REQUIRED chttp-closure-positive LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_debug_positive PROPERTIES
      FIXTURES_REQUIRED chttp-closure-debug-positive LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_wrong_crt PROPERTIES
      FIXTURES_REQUIRED chttp-closure-wrong-crt LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_wrong_msvcp_crt PROPERTIES
      FIXTURES_REQUIRED chttp-closure-wrong-msvcp-crt LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_unresolved PROPERTIES
      FIXTURES_REQUIRED chttp-closure-unresolved LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_outside PROPERTIES
      FIXTURES_REQUIRED chttp-closure-outside LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_debug_release_crt PROPERTIES
      FIXTURES_REQUIRED chttp-closure-debug-release-crt LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_arbitrary_outside PROPERTIES
      FIXTURES_REQUIRED chttp-closure-arbitrary-outside LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_system_forbidden PROPERTIES
      FIXTURES_REQUIRED chttp-closure-system-forbidden LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_system_unsigned PROPERTIES
      FIXTURES_REQUIRED chttp-closure-system-unsigned LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_system_signed_non_os PROPERTIES
      FIXTURES_REQUIRED chttp-closure-system-signed-non-os LABELS "cmake;chttp-adapter" TIMEOUT 30)
  endif()

  add_test(
    NAME test_turbo_flow_install_consumer
    COMMAND
      "${CMAKE_COMMAND}"
      "-DTURBO_FLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
      "-DTURBO_FLOW_CONFIG=$<CONFIG>"
      "-DTURBO_FLOW_HAS_TURBODB_ADAPTER=${TURBO_FLOW_BUILD_TURBODB_ADAPTER}"
      -P "${CMAKE_SOURCE_DIR}/tests/install_consumer/run.cmake")
  set_tests_properties(
    test_turbo_flow_install_consumer
    PROPERTIES LABELS "install;package" RUN_SERIAL TRUE TIMEOUT 600)
endif()
