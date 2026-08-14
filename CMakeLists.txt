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

find_package(TurboUtils REQUIRED PATHS ${TURBO_UTILS_ROOT})
find_package(TurboParser CONFIG REQUIRED PATHS "${TURBOPARSER_ROOT}")
find_package(PostgreSQL REQUIRED)
find_package(xxHash CONFIG REQUIRED)

# TurboNet's exported targets reference these dependency targets.
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(TurboNet REQUIRED PATHS ${TURBO_NET_ROOT})

find_package(TurboHttp REQUIRED PATHS ${TURBO_HTTP_ROOT})

if(WIN32)
  find_program(
    TURBO_FLOW_POWERSHELL_EXECUTABLE
    NAMES pwsh.exe powershell.exe
    REQUIRED)

  set(TURBO_FLOW_EXTERNAL_PACKAGE_ROOTS
      "${TURBO_UTILS_ROOT}"
      "${TURBOPARSER_ROOT}"
      "${RulesForge_ROOT}"
      "${TURBO_NET_ROOT}"
      "${TURBO_HTTP_ROOT}")
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
  add_subdirectory(flowstore)
  add_subdirectory(security)
  add_subdirectory(codec)
  add_subdirectory(observe)
  add_subdirectory(schedule)
  add_subdirectory(io)
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
     tf_protocol
     tf_protocol_graph
     tf_protocol_coronet
     tf_protocol_mqtt_sn
     tf_protocol_coap
     tf_protocol_lwm2m
     tf_protocol_ocpp
     tf_protocol_gbt32960
     tf_protocol_jtt808
     tf_protocol_business_ocpp201_core
     tf_mqtt_sink
     tf_storage_backend
     tf_flow_store
     tf_local_storage
     tf_security_sqlite
     tf_codec
     tf_observe
     tf_schedule
     tf_socket
     tf_http_client
     tf_http_server
     tf_http
     tf_rpc
     tf_s3
     tf_redis
     tf_pgsql
     tf_sqlite_storage
     tf_email)
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
