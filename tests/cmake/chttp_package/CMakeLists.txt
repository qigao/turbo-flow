cmake_minimum_required(VERSION 3.20)

if(CHTTP_PROBE_LANGUAGE STREQUAL "CXX")
  project(TurboFlowCHTTPPackageProbe LANGUAGES CXX)
  set_source_files_properties(probe.c PROPERTIES LANGUAGE CXX)
else()
  project(TurboFlowCHTTPPackageProbe LANGUAGES C)
endif()

if(CHTTP_PROBE_SCENARIO STREQUAL "unset-root")
  unset(ENV{HTTP_SERVICES_ROOT})
elseif(CHTTP_PROBE_SCENARIO STREQUAL "empty-root")
  set(ENV{HTTP_SERVICES_ROOT} "")
elseif(CHTTP_PROBE_SCENARIO STREQUAL "missing-root")
  set(ENV{HTTP_SERVICES_ROOT} "${CMAKE_CURRENT_BINARY_DIR}/does-not-exist")
elseif(CHTTP_PROBE_SCENARIO STREQUAL "empty-sdk")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/empty-sdk")
  set(ENV{HTTP_SERVICES_ROOT} "${CMAKE_CURRENT_BINARY_DIR}/empty-sdk")
elseif(CHTTP_PROBE_SCENARIO STREQUAL "outside-cache")
  set(Chttp_DIR "${CMAKE_CURRENT_BINARY_DIR}/outside" CACHE PATH "" FORCE)
elseif(CHTTP_PROBE_SCENARIO STREQUAL "preimport-without-provenance")
  add_library(CHttp::Client SHARED IMPORTED)
  add_library(CHttp::Server SHARED IMPORTED)
  add_library(CHttp::S3 SHARED IMPORTED)
  unset(Chttp_DIR)
  unset(Chttp_DIR CACHE)
elseif(CHTTP_PROBE_SCENARIO STREQUAL "preimport-outside-location")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/outside")
  file(COPY_FILE "$ENV{HTTP_SERVICES_ROOT}/bin/chttp_client.dll" "${CMAKE_CURRENT_BINARY_DIR}/outside/client.dll")
  file(COPY_FILE "$ENV{HTTP_SERVICES_ROOT}/lib/chttp_client.lib" "${CMAKE_CURRENT_BINARY_DIR}/outside/client.lib")
  add_library(CHttp::Client SHARED IMPORTED)
  add_library(CHttp::Server SHARED IMPORTED)
  add_library(CHttp::S3 SHARED IMPORTED)
  set(Chttp_DIR "$ENV{HTTP_SERVICES_ROOT}/lib/cmake/Chttp")
  set_target_properties(CHttp::Client PROPERTIES IMPORTED_CONFIGURATIONS RELEASE
    IMPORTED_LOCATION_RELEASE "${CMAKE_CURRENT_BINARY_DIR}/outside/client.dll"
    IMPORTED_IMPLIB_RELEASE "${CMAKE_CURRENT_BINARY_DIR}/outside/client.lib"
    INTERFACE_INCLUDE_DIRECTORIES "$ENV{HTTP_SERVICES_ROOT}/include")
  set_target_properties(CHttp::Server PROPERTIES IMPORTED_CONFIGURATIONS RELEASE
    IMPORTED_LOCATION_RELEASE "${CMAKE_CURRENT_BINARY_DIR}/outside/server.dll"
    IMPORTED_IMPLIB_RELEASE "${CMAKE_CURRENT_BINARY_DIR}/outside/server.lib"
    INTERFACE_INCLUDE_DIRECTORIES "$ENV{HTTP_SERVICES_ROOT}/include")
elseif(CHTTP_PROBE_SCENARIO STREQUAL "wrong-config")
  set(CMAKE_BUILD_TYPE Debug)
elseif(CHTTP_PROBE_SCENARIO STREQUAL "missing-deferred-cancel")
  set(_fixture_root "${CMAKE_CURRENT_BINARY_DIR}/missing-symbol-sdk")
  file(MAKE_DIRECTORY "${_fixture_root}/include" "${_fixture_root}/bin"
       "${_fixture_root}/lib" "${_fixture_root}/lib/cmake/Chttp")
  file(COPY "$ENV{HTTP_SERVICES_ROOT}/include/" DESTINATION "${_fixture_root}/include")
  file(COPY_FILE "$ENV{HTTP_SERVICES_ROOT}/bin/chttp_client.dll" "${_fixture_root}/bin/chttp_client.dll")
  file(COPY_FILE "$ENV{HTTP_SERVICES_ROOT}/lib/chttp_client.lib" "${_fixture_root}/lib/chttp_client.lib")
  file(WRITE "${_fixture_root}/lib/cmake/Chttp/ChttpConfig.cmake" "")
  set(ENV{HTTP_SERVICES_ROOT} "${_fixture_root}")
  set(Chttp_DIR "${_fixture_root}/lib/cmake/Chttp")
  add_library(CHttp::Client SHARED IMPORTED)
  add_library(CHttp::Server SHARED IMPORTED)
  add_library(CHttp::S3 SHARED IMPORTED)
  foreach(_fixture_target IN ITEMS CHttp::Client CHttp::Server)
    set_target_properties(${_fixture_target} PROPERTIES
      IMPORTED_CONFIGURATIONS RELEASE
      IMPORTED_LOCATION_RELEASE "${_fixture_root}/bin/chttp_client.dll"
      IMPORTED_IMPLIB_RELEASE "${_fixture_root}/lib/chttp_client.lib"
      INTERFACE_INCLUDE_DIRECTORIES "${_fixture_root}/include")
  endforeach()
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../../cmake")
include(TurboFlowRequireCHTTP)

add_executable(chttp_package_probe probe.c)
target_link_libraries(chttp_package_probe PRIVATE CHttp::Client CHttp::Server)
