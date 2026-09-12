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
    SALTS_ROOT SALTS_UTILS_ROOT RULES_FORGE_ROOT)
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
  add_subdirectory(sink/mqtt)

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
     tf_protocol_graph
     tf_protocol_mqtt_sn
     tf_protocol_coap
     tf_protocol_lwm2m
     tf_protocol_ocpp
     tf_protocol_gbt32960
     tf_protocol_jtt808
     tf_protocol_business_ocpp201_core
     tf_mqtt_sink
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
      "-DTURBO_FLOW_BINARY_DIR=${CMAKE_BINARY_DIR}"
      "-DTURBO_FLOW_GENERATOR=${CMAKE_GENERATOR}"
      -P "${CMAKE_SOURCE_DIR}/tests/cmake/run_find_tools_failfast.cmake")
  set_tests_properties(
    test_turbo_flow_cmake_tools_failfast
    PROPERTIES LABELS "cmake;tools" RUN_SERIAL TRUE TIMEOUT 30)

  add_test(
    NAME test_turbo_flow_cnet_stop_drain_failfast
    COMMAND
      "${CMAKE_COMMAND}"
      "-DTURBO_FLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
      "-DTURBO_FLOW_BINARY_DIR=${CMAKE_BINARY_DIR}"
      "-DTURBO_FLOW_GENERATOR=${CMAKE_GENERATOR}"
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
    foreach(closure_case IN ITEMS legacy wrong-crt unresolved outside)
      add_test(NAME test_turbo_flow_chttp_closure_setup_${closure_case}
        COMMAND "${CMAKE_COMMAND}" --workflow --preset "closure-${closure_case}")
      set_tests_properties(test_turbo_flow_chttp_closure_setup_${closure_case}
        PROPERTIES WORKING_DIRECTORY "${chttp_closure_fixture_dir}"
                   FIXTURES_SETUP "chttp-closure-${closure_case}"
                   LABELS "cmake;chttp-adapter" TIMEOUT 120)
    endforeach()
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
    set_tests_properties(test_turbo_flow_chttp_closure_legacy PROPERTIES
      FIXTURES_REQUIRED chttp-closure-legacy LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_wrong_crt PROPERTIES
      FIXTURES_REQUIRED chttp-closure-wrong-crt LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_unresolved PROPERTIES
      FIXTURES_REQUIRED chttp-closure-unresolved LABELS "cmake;chttp-adapter" TIMEOUT 30)
    set_tests_properties(test_turbo_flow_chttp_closure_outside PROPERTIES
      FIXTURES_REQUIRED chttp-closure-outside LABELS "cmake;chttp-adapter" TIMEOUT 30)
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
