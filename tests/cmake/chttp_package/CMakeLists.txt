cmake_minimum_required(VERSION 3.20)

if(DEFINED ENV{CHTTP_PROBE_SCENARIO})
  set(CHTTP_PROBE_SCENARIO "$ENV{CHTTP_PROBE_SCENARIO}")
endif()
if(DEFINED ENV{CHTTP_MISSING_SYMBOL_SDK_ROOT})
  set(CHTTP_MISSING_SYMBOL_SDK_ROOT "$ENV{CHTTP_MISSING_SYMBOL_SDK_ROOT}")
endif()

if(CHTTP_PROBE_LANGUAGE STREQUAL "CXX")
  project(TurboFlowCHTTPPackageProbe LANGUAGES CXX)
  set_source_files_properties(probe.c PROPERTIES LANGUAGE CXX)
else()
  project(TurboFlowCHTTPPackageProbe LANGUAGES C)
endif()

if(CHTTP_PROBE_SCENARIO STREQUAL "build-missing-symbol-sdk")
  find_package(Salts CONFIG REQUIRED PATHS "$ENV{SALTS_ROOT}" NO_DEFAULT_PATH)
  add_library(Client SHARED missing_symbol_client.c)
  add_library(Server SHARED missing_symbol_server.c)
  target_link_libraries(Client PUBLIC Salts::CNet Salts::CMeta Salts::CSerde)
  target_link_libraries(Server PUBLIC Salts::CNet Salts::CMeta Salts::CSerde)
  target_include_directories(Client PUBLIC
    "$<BUILD_INTERFACE:$ENV{HTTP_SERVICES_ROOT}/include>"
    "$<INSTALL_INTERFACE:include>")
  target_include_directories(Server PUBLIC
    "$<BUILD_INTERFACE:$ENV{HTTP_SERVICES_ROOT}/include>"
    "$<INSTALL_INTERFACE:include>")
  install(TARGETS Client Server EXPORT ChttpTargets
    RUNTIME DESTINATION bin LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
  install(EXPORT ChttpTargets NAMESPACE CHttp:: DESTINATION lib/cmake/Chttp)
  install(FILES ChttpConfig.cmake DESTINATION lib/cmake/Chttp)
  install(DIRECTORY "$ENV{HTTP_SERVICES_ROOT}/include/" DESTINATION include)
  return()
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
  set(ENV{HTTP_SERVICES_ROOT} "${CHTTP_MISSING_SYMBOL_SDK_ROOT}")
elseif(CHTTP_PROBE_SCENARIO STREQUAL "missing-deferred-cancel-static-try-compile")
  set(ENV{HTTP_SERVICES_ROOT} "${CHTTP_MISSING_SYMBOL_SDK_ROOT}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
elseif(CHTTP_PROBE_SCENARIO STREQUAL "preserve-static-try-compile")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
elseif(CHTTP_PROBE_SCENARIO STREQUAL "preserve-undefined-try-compile")
  unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../../cmake")
include(TurboFlowRequireCHTTP)

if(CHTTP_PROBE_SCENARIO STREQUAL "preserve-static-try-compile" AND
   NOT CMAKE_TRY_COMPILE_TARGET_TYPE STREQUAL "STATIC_LIBRARY")
  message(FATAL_ERROR "CMAKE_TRY_COMPILE_TARGET_TYPE caller state was not restored")
endif()
if(CHTTP_PROBE_SCENARIO STREQUAL "preserve-undefined-try-compile" AND
   DEFINED CMAKE_TRY_COMPILE_TARGET_TYPE)
  message(FATAL_ERROR "undefined CMAKE_TRY_COMPILE_TARGET_TYPE caller state was not restored")
endif()

add_executable(chttp_package_probe probe.c)
target_link_libraries(chttp_package_probe PRIVATE CHttp::Client CHttp::Server)
