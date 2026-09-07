cmake_minimum_required(VERSION 3.20)
if(POLICY CMP0144)
  cmake_policy(SET CMP0144 OLD)
endif()
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()

project(
  TurboFlow
  VERSION 1.0.0
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

foreach(_turbo_flow_dependency_root IN ITEMS
        SALTS_ROOT SALTS_UTILS_ROOT RULES_FORGE_ROOT)
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
find_package(xxHash CONFIG REQUIRED)

if(WIN32)
  find_program(
    TURBO_FLOW_POWERSHELL_EXECUTABLE
    NAMES pwsh.exe powershell.exe
    REQUIRED)

  set(TURBO_FLOW_EXTERNAL_PACKAGE_ROOTS
      "$ENV{SALTS_ROOT}"
      "$ENV{SALTS_UTILS_ROOT}"
      "$ENV{RULES_FORGE_ROOT}")
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
     turbo_flow
     tf_cnet_adapter
     tf_chttp_adapter
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
configure_package_config_file(
  "${CMAKE_SOURCE_DIR}/cmake/TurboFlowConfig.cmake.in"
  "${CMAKE_BINARY_DIR}/TurboFlowConfig.cmake"
  INSTALL_DESTINATION "lib/cmake/TurboFlow")

# Generate the version file
write_basic_package_version_file(
  "${CMAKE_BINARY_DIR}/TurboFlowConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY AnyNewerVersion)

# Install the config files
install(FILES "${CMAKE_BINARY_DIR}/TurboFlowConfig.cmake"
              "${CMAKE_BINARY_DIR}/TurboFlowConfigVersion.cmake"
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
    NAME test_turbo_flow_install_consumer
    COMMAND
      "${CMAKE_COMMAND}"
      "-DTURBO_FLOW_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
      "-DTURBO_FLOW_BINARY_DIR=${CMAKE_BINARY_DIR}"
      "-DTURBO_FLOW_GENERATOR=${CMAKE_GENERATOR}"
      "-DTURBO_FLOW_CONFIG=$<CONFIG>"
      "-DTURBO_FLOW_CTEST_COMMAND=${CMAKE_CTEST_COMMAND}"
      "-DTURBO_FLOW_SALTS_ROOT=$ENV{SALTS_ROOT}"
      "-DTURBO_FLOW_SALTS_UTILS_ROOT=$ENV{SALTS_UTILS_ROOT}"
      "-DTURBO_FLOW_RULES_FORGE_ROOT=$ENV{RULES_FORGE_ROOT}"
      -P "${CMAKE_SOURCE_DIR}/tests/install_consumer/run.cmake")
  set_tests_properties(
    test_turbo_flow_install_consumer
    PROPERTIES LABELS "install;package" RUN_SERIAL TRUE TIMEOUT 120)
endif()
