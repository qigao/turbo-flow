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
find_package(RulesForge REQUIRED PATHS ${RULES_FORGE_ROOT})
if(TURBO_FLOW_BUILD_PGSQL)
  find_package(PostgreSQL REQUIRED)
endif()

set(TURBO_FLOW_NEEDS_TURBONET OFF)
if(TURBO_FLOW_BUILD_FLOWIE
   OR TURBO_FLOW_BUILD_SOCKET
   OR TURBO_FLOW_BUILD_HTTP_CLIENT
   OR TURBO_FLOW_BUILD_HTTP_SERVER
   OR TURBO_FLOW_BUILD_RPC
   OR TURBO_FLOW_BUILD_S3
   OR TURBO_FLOW_BUILD_EMAIL
   OR TURBO_FLOW_BUILD_FMQ
   OR TURBO_FLOW_BUILD_REDIS)
  set(TURBO_FLOW_NEEDS_TURBONET ON)
  # TurboNet's exported CoroNet target references Threads::Threads.
  find_package(Threads REQUIRED)
  if(TURBO_NET_ROOT)
    find_package(TurboNet REQUIRED CONFIG PATHS "${TURBO_NET_ROOT}" NO_DEFAULT_PATH)
  else()
    find_package(TurboNet REQUIRED)
  endif()
endif()

set(TURBO_FLOW_NEEDS_TURBOHTTP OFF)
if(TURBO_FLOW_BUILD_HTTP_CLIENT
   OR TURBO_FLOW_BUILD_HTTP_SERVER
   OR TURBO_FLOW_BUILD_RPC
   OR TURBO_FLOW_BUILD_S3)
  set(TURBO_FLOW_NEEDS_TURBOHTTP ON)
  find_package(TurboHttp REQUIRED PATHS ${TURBO_HTTP_ROOT})
endif()

# Additional components
add_subdirectory(tools)
include(FindTools)
add_subdirectory(vendor)

add_subdirectory(turbo_flow)
if(TURBO_FLOW_BUILD_FLOWIE)
  add_subdirectory(flowie)
endif()
if(TURBO_FLOW_BUILD_STORAGE)
  add_subdirectory(storage)
endif()
if(TURBO_FLOW_BUILD_CODEC)
  add_subdirectory(codec)
endif()
if(TURBO_FLOW_BUILD_OBSERVE)
  add_subdirectory(observe)
endif()
if(TURBO_FLOW_BUILD_SCHEDULE)
  add_subdirectory(schedule)
endif()
if(TURBO_FLOW_BUILD_QUEUE)
  add_subdirectory(queue)
endif()
if(TURBO_FLOW_BUILD_ADAPTERS)
  add_subdirectory(io)
endif()
if(TURBO_FLOW_BUILD_FLOWIE AND TURBO_FLOW_BUILD_QUEUE AND TURBO_FLOW_BUILD_SOCKET)
  add_subdirectory(flowie/app)
endif()

set(TURBO_FLOW_EXPORT_TARGETS)
foreach(
  target_name
  IN ITEMS
     turbo_flow
     flowie_protocol
     flowie_client
     flowie
     turbo_flow_storage
     tf_codec
     tf_observe
     tf_schedule
     tf_queue
     tf_socket
     tf_http_client
     tf_http_server
     tf_http
     tf_rpc
     tf_s3
     tf_redis
     tf_pgsql
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

if(BUILD_TESTING
   AND TURBO_FLOW_BUILD_PROFILE_TESTS
   AND TURBO_FLOW_BUILD_ADAPTERS)
  foreach(profile IN ITEMS core codec_fmq expr_no_jit)
    add_test(
      NAME build_profile_${profile}
      COMMAND
        ${CMAKE_COMMAND} "-DSOURCE_DIR=${CMAKE_SOURCE_DIR}"
        "-DBINARY_DIR=${CMAKE_BINARY_DIR}/profiles/${profile}"
        "-DPROFILE=${profile}" "-DGENERATOR=${CMAKE_GENERATOR}"
        "-DBUILD_TYPE=${CMAKE_BUILD_TYPE}"
        "-DMAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
        "-DTOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
        "-DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}"
        "-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}"
        "-DTURBO_UTILS_ROOT=${TURBO_UTILS_ROOT}"
        "-DRULES_FORGE_ROOT=${RULES_FORGE_ROOT}"
        "-DTURBO_NET_ROOT=${TURBO_NET_ROOT}"
        "-DTURBO_HTTP_ROOT=${TURBO_HTTP_ROOT}" -P
        "${CMAKE_SOURCE_DIR}/cmake/VerifyBuildProfile.cmake")
    set_tests_properties(
      build_profile_${profile} PROPERTIES LABELS "build-profile" RUN_SERIAL
                                          TRUE TIMEOUT 180)
  endforeach()
endif()
