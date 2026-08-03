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
      "${TURBO_UTILS_ROOT}" "${TURBO_NET_ROOT}" "${TURBO_HTTP_ROOT}")
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
add_subdirectory(io/common/storage)
  add_subdirectory(flowstore)
  add_subdirectory(io/local)
  add_subdirectory(security)
  add_subdirectory(flowie)
  add_subdirectory(codec)
  add_subdirectory(observe)
  add_subdirectory(schedule)
  add_subdirectory(io)
  add_subdirectory(gateway)
if(FLOWIE_MQTT_SOAK_TESTS)
  set(_flowie_soak_required_targets
      test_flowie_endpoint
      test_flowie_transport
      test_flowie_session_store_faults
      test_turbo_flow
      test_turbo_flow_redis_live
      test_turbo_flow_pgsql_live)
  foreach(_flowie_soak_target IN LISTS _flowie_soak_required_targets)
    if(NOT TARGET ${_flowie_soak_target})
      message(FATAL_ERROR
              "FLOWIE_MQTT_SOAK_TESTS requires target ${_flowie_soak_target}; enable Flowie, "
              "Redis, PostgreSQL, and tests")
    endif()
  endforeach()
  cmake_add_test(
    test_flowie_mqtt_soak
    SOURCES ${PROJECT_SOURCE_DIR}/flowie/tests/test_flowie_mqtt_soak.c
    LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/flowie/src
    FOLDER "flowie/tests")
  target_compile_definitions(
    test_flowie_mqtt_soak
    PRIVATE FLOWIE_SOAK_ENDPOINT_TEST="$<TARGET_FILE:test_flowie_endpoint>"
            FLOWIE_SOAK_TRANSPORT_TEST="$<TARGET_FILE:test_flowie_transport>"
            FLOWIE_SOAK_STORE_TEST="$<TARGET_FILE:test_flowie_session_store_faults>"
            FLOWIE_SOAK_RUNTIME_TEST="$<TARGET_FILE:test_turbo_flow>"
            FLOWIE_SOAK_REDIS_TEST="$<TARGET_FILE:test_turbo_flow_redis_live>"
            FLOWIE_SOAK_PGSQL_TEST="$<TARGET_FILE:test_turbo_flow_pgsql_live>")
  add_dependencies(test_flowie_mqtt_soak ${_flowie_soak_required_targets})
  set_tests_properties(
    test_flowie_mqtt_soak
    PROPERTIES LABELS "flowie;flowie-release;mqtt-soak;scheduled"
               TIMEOUT 21600)
  if(WIN32)
    target_link_libraries(test_flowie_mqtt_soak PRIVATE psapi ws2_32)
  endif()
  unset(_flowie_soak_required_targets)
  unset(_flowie_soak_target)
endif()
  add_subdirectory(flowmq/protocol)
  add_subdirectory(flowmq)
  add_subdirectory(flowie/cluster)
  add_subdirectory(flowie/server)

if(BUILD_TESTING)
  set(_flowie_release_evidence_targets
      test_flowie_mqtt_protocol_matrix
      test_flowie_mqtt_protocol_corpus
      test_flowie_mqtt_client_fixed_interop
      flowie_mqtt_fixed_mqtt31_capability
      flowie_mqtt_fixed_mqtt31_ws_capability
      test_flowie_mosquitto_interop
      test_flowie_mosquitto_fixed_interop
      test_flowie_session_store_faults
      test_flowie_transport
      test_flowie_mqtt_endurance
      test_flowie_mqtt_soak
      flowie_server_check_redis_session_store
      flowie_server_check_https_auth_provider)
  set(_flowie_release_evidence_dependencies)
  foreach(_flowie_release_target IN LISTS _flowie_release_evidence_targets)
    if(TARGET ${_flowie_release_target})
      list(APPEND _flowie_release_evidence_dependencies ${_flowie_release_target})
    endif()
  endforeach()
  add_custom_target(
    flowie_release_evidence
    COMMAND
      ${CMAKE_COMMAND} "-DCTEST_COMMAND=${CMAKE_CTEST_COMMAND}"
      "-DCMAKE_COMMAND=${CMAKE_COMMAND}" "-DTEST_DIR=${CMAKE_BINARY_DIR}"
      "-DOUTPUT_FILE=${CMAKE_BINARY_DIR}/flowie-release-evidence.json"
      "-DREVISION=${FLOWIE_RELEASE_REVISION}" "-DPROJECT_VERSION=${PROJECT_VERSION}"
      "-DFIXED_BROKER_VERSION=${FLOWIE_MQTT_FIXED_BROKER_NAME}"
      "-DFIXED_SUPPORT_31=${FLOWIE_MQTT_FIXED_SUPPORT_31}"
      "-DFIXED_SUPPORT_31_WS=${FLOWIE_MQTT_FIXED_SUPPORT_31_WS}"
      "-DREDIS_VERSION=${FLOWIE_REDIS_SERVER_VERSION}" "-DCONFIG=$<CONFIG>" -P
      "${PROJECT_SOURCE_DIR}/flowie/cmake/RunReleaseEvidence.cmake"
    DEPENDS ${_flowie_release_evidence_dependencies}
    USES_TERMINAL)
  set_target_properties(flowie_release_evidence PROPERTIES FOLDER "flowie/gates")

  set(_flowie_nightly_dependencies)
  foreach(_flowie_nightly_target IN ITEMS test_flowie_mqtt_protocol_corpus
                                         test_flowie_mqtt_soak
                                         fuzz_flowie_mqtt_protocol)
    if(TARGET ${_flowie_nightly_target})
      list(APPEND _flowie_nightly_dependencies ${_flowie_nightly_target})
    endif()
  endforeach()
  set(_flowie_nightly_sanitizer
      "${CMAKE_BINARY_DIR}/missing-fuzz-flowie-mqtt-protocol")
  if(TARGET fuzz_flowie_mqtt_protocol)
    set(_flowie_nightly_sanitizer "$<TARGET_FILE:fuzz_flowie_mqtt_protocol>")
  endif()
  add_custom_target(
    flowie_nightly_evidence
    COMMAND
      ${CMAKE_COMMAND} "-DCTEST_COMMAND=${CMAKE_CTEST_COMMAND}"
      "-DCMAKE_COMMAND=${CMAKE_COMMAND}" "-DTEST_DIR=${CMAKE_BINARY_DIR}"
      "-DOUTPUT_FILE=${CMAKE_BINARY_DIR}/flowie-nightly-evidence.json"
      "-DREVISION=${FLOWIE_RELEASE_REVISION}" "-DSEED=${FLOWIE_MQTT_NIGHTLY_SEED}"
      "-DFUZZ_RUNS=${FLOWIE_MQTT_FUZZ_RUNS}"
      "-DSOAK_SHORT_MS=${FLOWIE_MQTT_NIGHTLY_SOAK_SHORT_MS}"
      "-DSOAK_LONG_MS=${FLOWIE_MQTT_NIGHTLY_SOAK_LONG_MS}"
      "-DSANITIZER_EXECUTABLE=${_flowie_nightly_sanitizer}" "-DCONFIG=$<CONFIG>"
      -P "${PROJECT_SOURCE_DIR}/flowie/cmake/RunNightlyEvidence.cmake"
    DEPENDS ${_flowie_nightly_dependencies}
    USES_TERMINAL)
  set_target_properties(flowie_nightly_evidence PROPERTIES FOLDER "flowie/gates")
  unset(_flowie_release_evidence_targets)
  unset(_flowie_release_evidence_dependencies)
  unset(_flowie_release_target)
  unset(_flowie_nightly_dependencies)
  unset(_flowie_nightly_target)
  unset(_flowie_nightly_sanitizer)
endif()

set(TURBO_FLOW_EXPORT_TARGETS)
foreach(
  target_name
  IN ITEMS
     turbo_flow
     tf_gateway
     tf_gateway_mqtt_sn
     tf_gateway_coap
     tf_gateway_lwm2m
     tf_gateway_ocpp
     tf_gateway_gbt32960
     tf_gateway_jtt808
     tf_gateway_business_ocpp201_core
     tf_storage_backend
     tf_flow_store
     tf_local_storage
     tf_security_sqlite
     flowie_protocol
     flowie_client
     flowie
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
     tf_email
     tf_fmq)
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
