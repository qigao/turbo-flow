if(NOT DEFINED SOURCE_DIR OR NOT DEFINED BINARY_DIR OR NOT DEFINED PROFILE)
  message(FATAL_ERROR "SOURCE_DIR, BINARY_DIR, and PROFILE are required")
endif()

set(configure_args
    --fresh
    -S "${SOURCE_DIR}"
    -B "${BINARY_DIR}"
    "-DBUILD_TESTING=OFF"
    "-DBUILD_TESTS=OFF"
    "-DBUILD_BENCHMARKS=OFF"
    "-DTURBO_FLOW_BUILD_FLOWIE=OFF"
    "-DTURBO_FLOW_BUILD_PROFILE_TESTS=OFF")

if(DEFINED GENERATOR AND NOT GENERATOR STREQUAL "")
  list(APPEND configure_args -G "${GENERATOR}")
endif()
if(DEFINED BUILD_TYPE AND NOT BUILD_TYPE STREQUAL "")
  list(APPEND configure_args "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}")
endif()
if(DEFINED MAKE_PROGRAM AND NOT MAKE_PROGRAM STREQUAL "")
  list(APPEND configure_args "-DCMAKE_MAKE_PROGRAM=${MAKE_PROGRAM}")
endif()
if(DEFINED TOOLCHAIN_FILE AND NOT TOOLCHAIN_FILE STREQUAL "")
  list(APPEND configure_args "-DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE}")
endif()
if(DEFINED VCPKG_INSTALLED_DIR AND NOT VCPKG_INSTALLED_DIR STREQUAL "")
  list(APPEND configure_args "-DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}")
endif()
if(DEFINED VCPKG_OVERLAY_PORTS AND NOT VCPKG_OVERLAY_PORTS STREQUAL "")
  list(APPEND configure_args "-DVCPKG_OVERLAY_PORTS=${VCPKG_OVERLAY_PORTS}")
endif()
if(DEFINED TURBO_UTILS_ROOT AND NOT TURBO_UTILS_ROOT STREQUAL "")
  list(APPEND configure_args "-DTURBO_UTILS_ROOT=${TURBO_UTILS_ROOT}")
endif()
if(DEFINED TURBO_NET_ROOT AND NOT TURBO_NET_ROOT STREQUAL "")
  list(APPEND configure_args "-DTURBO_NET_ROOT=${TURBO_NET_ROOT}")
endif()
if(DEFINED TURBO_HTTP_ROOT AND NOT TURBO_HTTP_ROOT STREQUAL "")
  list(APPEND configure_args "-DTURBO_HTTP_ROOT=${TURBO_HTTP_ROOT}")
endif()

if(PROFILE STREQUAL "core")
  list(APPEND configure_args
       "-DTURBO_FLOW_BUILD_ADAPTERS=OFF"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboNet=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboHttp=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_OpenSSL=TRUE")
  set(build_targets turbo_flow tf_flow_store)
  set(expected_targets TurboFlow::Flow TurboFlow::FlowStore)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie
      TurboFlow::Codec TurboFlow::Socket
      TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Http
      TurboFlow::RPC TurboFlow::S3 TurboFlow::Email TurboFlow::FMQ
      TurboFlow::Observe TurboFlow::Schedule)
elseif(PROFILE STREQUAL "protocol_only")
  list(APPEND configure_args
       "-DTURBO_FLOW_BUILD_ADAPTERS=OFF"
       "-DTURBO_FLOW_BUILD_FMQ_PROTOCOL=ON"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboNet=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboHttp=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_OpenSSL=TRUE")
  set(build_targets flowmq_protocol tf_flow_store)
  set(expected_targets TurboFlow::Flow TurboFlow::FlowStore)
  set(expected_flowmq_targets FlowMQ::Protocol)
  set(absent_flowmq_targets FlowMQ::FlowMQProtocol)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie
      TurboFlow::Codec TurboFlow::Socket
      TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Http
      TurboFlow::RPC TurboFlow::S3 TurboFlow::Email TurboFlow::FMQ
      TurboFlow::Observe TurboFlow::Schedule)
elseif(PROFILE STREQUAL "codec_fmq")
  list(APPEND configure_args
       "-DTURBO_FLOW_BUILD_CODEC=ON"
       "-DTURBO_FLOW_BUILD_SOCKET=OFF"
       "-DTURBO_FLOW_BUILD_HTTP_CLIENT=OFF"
       "-DTURBO_FLOW_BUILD_HTTP_SERVER=OFF"
       "-DTURBO_FLOW_BUILD_RPC=OFF"
       "-DTURBO_FLOW_BUILD_S3=OFF"
       "-DTURBO_FLOW_BUILD_EMAIL=OFF"
       "-DTURBO_FLOW_BUILD_FMQ=ON"
       "-DTURBO_FLOW_BUILD_REDIS=OFF"
       "-DTURBO_FLOW_BUILD_PGSQL=OFF"
       "-DTURBO_FLOW_BUILD_OBSERVE=OFF"
       "-DTURBO_FLOW_BUILD_SCHEDULE=OFF"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboHttp=TRUE")
  set(build_targets turbo_flow tf_flow_store tf_codec flowmq_protocol tf_fmq)
  set(expected_targets TurboFlow::Flow TurboFlow::FlowStore TurboFlow::Codec TurboFlow::FMQ)
  set(expected_flowmq_targets FlowMQ::Protocol)
  set(absent_flowmq_targets FlowMQ::FlowMQProtocol)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie
      TurboFlow::Socket TurboFlow::HttpClient
      TurboFlow::HttpServer TurboFlow::Http TurboFlow::RPC TurboFlow::S3
      TurboFlow::Email TurboFlow::Redis TurboFlow::PostgreSQL TurboFlow::Observe
      TurboFlow::Schedule)
elseif(PROFILE STREQUAL "expr_no_jit")
  list(APPEND configure_args
       "-DBUILD_TESTING=ON"
       "-DBUILD_TESTS=ON"
       "-DTURBO_FLOW_BUILD_ADAPTERS=OFF"
       "-DTURBO_FLOW_EXPR_ENABLE_JIT=OFF"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboNet=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboHttp=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_OpenSSL=TRUE")
  set(build_targets test_flow_expr tf_flow_store)
  set(expected_targets TurboFlow::Flow TurboFlow::FlowStore)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie
      TurboFlow::Codec TurboFlow::Socket
      TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Http
      TurboFlow::RPC TurboFlow::S3 TurboFlow::Email TurboFlow::FMQ
      TurboFlow::Observe TurboFlow::Schedule)
  set(run_test test_flow_expr)
else()
  message(FATAL_ERROR "unknown build profile: ${PROFILE}")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}" ${configure_args}
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_output
  ERROR_VARIABLE configure_error)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR
          "${PROFILE} configure failed\n${configure_output}\n${configure_error}")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}" --build "${BINARY_DIR}" --target ${build_targets}
  RESULT_VARIABLE build_result
  OUTPUT_VARIABLE build_output
  ERROR_VARIABLE build_error)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR "${PROFILE} build failed\n${build_output}\n${build_error}")
endif()

if(DEFINED run_test)
  if(WIN32 AND DEFINED VCPKG_INSTALLED_DIR AND DEFINED VCPKG_TARGET_TRIPLET)
    execute_process(
      COMMAND "${CMAKE_COMMAND}" -E env
              "PATH=${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/bin;$ENV{PATH}"
              "${CMAKE_CTEST_COMMAND}" --test-dir "${BINARY_DIR}"
              -C "${BUILD_TYPE}" -R "^${run_test}$" --output-on-failure
      RESULT_VARIABLE test_result
      OUTPUT_VARIABLE test_output
      ERROR_VARIABLE test_error)
  else()
    execute_process(
      COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${BINARY_DIR}"
              -C "${BUILD_TYPE}" -R "^${run_test}$" --output-on-failure
      RESULT_VARIABLE test_result
      OUTPUT_VARIABLE test_output
      ERROR_VARIABLE test_error)
  endif()
  if(NOT test_result EQUAL 0)
    message(FATAL_ERROR
            "${PROFILE} tests failed\n${test_output}\n${test_error}")
  endif()
endif()

set(targets_file "${BINARY_DIR}/TurboFlowTargets.cmake")
if(NOT EXISTS "${targets_file}")
  message(FATAL_ERROR "${PROFILE} did not generate TurboFlowTargets.cmake")
endif()
file(READ "${targets_file}" targets_text)
foreach(target IN LISTS expected_targets)
  string(FIND "${targets_text}" "${target}" target_index)
  if(target_index EQUAL -1)
    message(FATAL_ERROR "${PROFILE} export is missing ${target}")
  endif()
endforeach()

if(DEFINED expected_flowmq_targets)
  set(flowmq_targets_file "${BINARY_DIR}/flowmq/protocol/FlowMQTargets.cmake")
  if(NOT EXISTS "${flowmq_targets_file}")
    message(FATAL_ERROR "${PROFILE} did not generate FlowMQTargets.cmake")
  endif()
  file(READ "${flowmq_targets_file}" flowmq_targets_text)
  foreach(target IN LISTS expected_flowmq_targets)
    string(FIND "${flowmq_targets_text}" "${target}" target_index)
    if(target_index EQUAL -1)
      message(FATAL_ERROR "${PROFILE} export is missing ${target}")
    endif()
  endforeach()
  foreach(target IN LISTS absent_flowmq_targets)
    string(FIND "${flowmq_targets_text}" "${target}" target_index)
    if(NOT target_index EQUAL -1)
      message(FATAL_ERROR "${PROFILE} export unexpectedly contains ${target}")
    endif()
  endforeach()
endif()
foreach(target IN LISTS absent_targets)
  string(FIND "${targets_text}" "${target}" target_index)
  if(NOT target_index EQUAL -1)
    message(FATAL_ERROR "${PROFILE} export unexpectedly contains ${target}")
  endif()
endforeach()

if(PROFILE STREQUAL "protocol_only")
  set(install_prefix "${BINARY_DIR}/install")
  set(consumer_source_dir "${BINARY_DIR}/consumer-src")
  set(consumer_binary_dir "${BINARY_DIR}/consumer-build")

  execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${BINARY_DIR}" --prefix "${install_prefix}"
            --config "${BUILD_TYPE}" --component FlowMQProtocol
    RESULT_VARIABLE install_result
    OUTPUT_VARIABLE install_output
    ERROR_VARIABLE install_error)
  if(NOT install_result EQUAL 0)
    message(FATAL_ERROR
            "${PROFILE} install failed\n${install_output}\n${install_error}")
  endif()

  file(MAKE_DIRECTORY "${consumer_source_dir}")
  file(WRITE "${consumer_source_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(flowmq_protocol_consumer C)
find_package(FlowMQ CONFIG REQUIRED COMPONENTS Protocol)
add_executable(flowmq_protocol_consumer main.c)
target_link_libraries(flowmq_protocol_consumer PRIVATE FlowMQ::Protocol)
]=])
  file(WRITE "${consumer_source_dir}/main.c" [=[
#include "flowmq_protocol.h"

int main(void) {
  size_t encoded_limit = 0u;
  return flowmq_protocol_encoded_size_limit(1024u, &encoded_limit) == TURBO_OK &&
                 encoded_limit > 0u
             ? 0
             : 1;
}
]=])

  set(consumer_configure_args
      --fresh
      -S "${consumer_source_dir}"
      -B "${consumer_binary_dir}"
      "-DFlowMQ_DIR=${install_prefix}/lib/cmake/FlowMQ"
      "-DTurboUtils_DIR=${TURBO_UTILS_ROOT}/lib/cmake/TurboUtils"
      "-DCMAKE_DISABLE_FIND_PACKAGE_TurboNet=TRUE")
  if(DEFINED GENERATOR AND NOT GENERATOR STREQUAL "")
    list(APPEND consumer_configure_args -G "${GENERATOR}")
  endif()
  if(DEFINED BUILD_TYPE AND NOT BUILD_TYPE STREQUAL "")
    list(APPEND consumer_configure_args "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}")
  endif()
  if(DEFINED MAKE_PROGRAM AND NOT MAKE_PROGRAM STREQUAL "")
    list(APPEND consumer_configure_args "-DCMAKE_MAKE_PROGRAM=${MAKE_PROGRAM}")
  endif()
  if(DEFINED TOOLCHAIN_FILE AND NOT TOOLCHAIN_FILE STREQUAL "")
    list(APPEND consumer_configure_args "-DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE}")
  endif()
  if(DEFINED VCPKG_INSTALLED_DIR AND NOT VCPKG_INSTALLED_DIR STREQUAL "")
    list(APPEND consumer_configure_args "-DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}")
  endif()

  execute_process(
    COMMAND "${CMAKE_COMMAND}" ${consumer_configure_args}
    RESULT_VARIABLE consumer_configure_result
    OUTPUT_VARIABLE consumer_configure_output
    ERROR_VARIABLE consumer_configure_error)
  if(NOT consumer_configure_result EQUAL 0)
    message(FATAL_ERROR
            "${PROFILE} installed consumer configure failed\n"
            "${consumer_configure_output}\n${consumer_configure_error}")
  endif()

  execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${consumer_binary_dir}" --target
            flowmq_protocol_consumer
    RESULT_VARIABLE consumer_build_result
    OUTPUT_VARIABLE consumer_build_output
    ERROR_VARIABLE consumer_build_error)
  if(NOT consumer_build_result EQUAL 0)
    message(FATAL_ERROR
            "${PROFILE} installed consumer build failed\n"
            "${consumer_build_output}\n${consumer_build_error}")
  endif()
endif()
