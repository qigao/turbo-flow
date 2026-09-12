cmake_minimum_required(VERSION 3.20)

project(TurboFlowComponentConsumer LANGUAGES C)

if(NOT DEFINED TURBO_FLOW_TEST_COMPONENT OR
   "${TURBO_FLOW_TEST_COMPONENT}" STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_TEST_COMPONENT is required")
endif()

if(NOT DEFINED TURBO_FLOW_TEST_PACKAGE_VERSION)
  set(TURBO_FLOW_TEST_PACKAGE_VERSION 2.0)
endif()

if(TURBO_FLOW_CHTTP_SCENARIO STREQUAL "empty-sdk")
  file(MAKE_DIRECTORY "$ENV{HTTP_SERVICES_ROOT}")
elseif(TURBO_FLOW_CHTTP_SCENARIO STREQUAL "outside-cache")
  set(Chttp_DIR "${CMAKE_CURRENT_BINARY_DIR}/outside" CACHE PATH "" FORCE)
elseif(TURBO_FLOW_CHTTP_SCENARIO STREQUAL "preimport-without-provenance")
  add_library(CHttp::Client SHARED IMPORTED)
  add_library(CHttp::Server SHARED IMPORTED)
  unset(Chttp_DIR)
  unset(Chttp_DIR CACHE)
elseif(TURBO_FLOW_CHTTP_SCENARIO STREQUAL "preimport-outside-location")
  find_package(Chttp CONFIG REQUIRED
               PATHS "$ENV{HTTP_SERVICES_ROOT}" NO_DEFAULT_PATH)
  if(CMAKE_CONFIGURATION_TYPES)
    set(chttp_fixture_configs ${CMAKE_CONFIGURATION_TYPES})
  else()
    set(chttp_fixture_configs "${CMAKE_BUILD_TYPE}")
  endif()
  foreach(chttp_fixture_target IN ITEMS CHttp::Client CHttp::Server)
    foreach(chttp_fixture_config IN LISTS chttp_fixture_configs)
      string(TOUPPER "${chttp_fixture_config}" chttp_fixture_config_upper)
      set_target_properties("${chttp_fixture_target}" PROPERTIES
        "IMPORTED_LOCATION_${chttp_fixture_config_upper}" "${CMAKE_COMMAND}")
      if(WIN32)
        set_target_properties("${chttp_fixture_target}" PROPERTIES
          "IMPORTED_IMPLIB_${chttp_fixture_config_upper}" "${CMAKE_COMMAND}")
      endif()
    endforeach()
  endforeach()
elseif(TURBO_FLOW_CHTTP_SCENARIO STREQUAL "wrong-config")
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_BUILD_TYPE Release)
  else()
    set(CMAKE_BUILD_TYPE Debug)
  endif()
endif()

if(NOT DEFINED ENV{TURBO_FLOW_ROOT} OR "$ENV{TURBO_FLOW_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBO_FLOW_ROOT}")
  message(FATAL_ERROR "TURBO_FLOW_ROOT must name the installed TurboFlow prefix")
endif()
file(REAL_PATH "$ENV{TURBO_FLOW_ROOT}" turbo_flow_root)
if(DEFINED TurboFlow_DIR AND NOT "${TurboFlow_DIR}" STREQUAL "")
  if(NOT IS_DIRECTORY "${TurboFlow_DIR}")
    message(FATAL_ERROR "TurboFlow_DIR is not a directory: ${TurboFlow_DIR}")
  endif()
  file(REAL_PATH "${TurboFlow_DIR}" turbo_flow_dir)
  cmake_path(IS_PREFIX turbo_flow_root "${turbo_flow_dir}" NORMALIZE
             turbo_flow_dir_is_within_root)
  if(NOT turbo_flow_dir_is_within_root)
    message(FATAL_ERROR "TurboFlow_DIR is outside TURBO_FLOW_ROOT: ${TurboFlow_DIR}")
  endif()
endif()
find_package(TurboFlow ${TURBO_FLOW_TEST_PACKAGE_VERSION} CONFIG REQUIRED
             COMPONENTS "${TURBO_FLOW_TEST_COMPONENT}"
             PATHS "$ENV{TURBO_FLOW_ROOT}" NO_DEFAULT_PATH)

add_executable(turbo_flow_component_consumer main.c)
target_link_libraries(
  turbo_flow_component_consumer
  PRIVATE "TurboFlow::${TURBO_FLOW_TEST_COMPONENT}")

if(TURBO_FLOW_TEST_COMPONENT STREQUAL "Config")
  target_compile_definitions(turbo_flow_component_consumer
                             PRIVATE TURBO_FLOW_TEST_CONFIG_COMPONENT=1)
endif()

enable_testing()
add_test(NAME turbo_flow_component_consumer_runs
         COMMAND turbo_flow_component_consumer)
