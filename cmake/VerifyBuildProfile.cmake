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
if(DEFINED TURBO_UTILS_ROOT AND NOT TURBO_UTILS_ROOT STREQUAL "")
  list(APPEND configure_args "-DTURBO_UTILS_ROOT=${TURBO_UTILS_ROOT}")
endif()
if(DEFINED RULES_FORGE_ROOT AND NOT RULES_FORGE_ROOT STREQUAL "")
  list(APPEND configure_args "-DRULES_FORGE_ROOT=${RULES_FORGE_ROOT}")
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
  set(build_targets turbo_flow)
  set(expected_targets TurboFlow::Flow)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie TurboFlow::FlowStorage
      TurboFlow::Codec TurboFlow::Socket
      TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Http
      TurboFlow::RPC TurboFlow::S3 TurboFlow::Email TurboFlow::FMQ
      TurboFlow::Observe TurboFlow::Schedule TurboFlow::Queue)
elseif(PROFILE STREQUAL "codec_fmq")
  list(APPEND configure_args
       "-DTURBO_FLOW_BUILD_STORAGE=OFF"
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
       "-DTURBO_FLOW_BUILD_QUEUE=OFF"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboHttp=TRUE")
  set(build_targets turbo_flow tf_codec tf_fmq)
  set(expected_targets TurboFlow::Flow TurboFlow::Codec TurboFlow::FMQ)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie TurboFlow::FlowStorage
      TurboFlow::Socket TurboFlow::HttpClient
      TurboFlow::HttpServer TurboFlow::Http TurboFlow::RPC TurboFlow::S3
      TurboFlow::Email TurboFlow::Redis TurboFlow::PostgreSQL TurboFlow::Observe
      TurboFlow::Schedule TurboFlow::Queue)
elseif(PROFILE STREQUAL "expr_no_jit")
  list(APPEND configure_args
       "-DBUILD_TESTING=ON"
       "-DBUILD_TESTS=ON"
       "-DTURBO_FLOW_BUILD_ADAPTERS=OFF"
       "-DTURBO_FLOW_EXPR_ENABLE_JIT=OFF"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboNet=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_TurboHttp=TRUE"
       "-DCMAKE_DISABLE_FIND_PACKAGE_OpenSSL=TRUE")
  set(build_targets test_flow_expr)
  set(expected_targets TurboFlow::Flow)
  set(absent_targets
      TurboFlow::FlowieProtocol TurboFlow::Flowie TurboFlow::FlowStorage
      TurboFlow::Codec TurboFlow::Socket
      TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Http
      TurboFlow::RPC TurboFlow::S3 TurboFlow::Email TurboFlow::FMQ
      TurboFlow::Observe TurboFlow::Schedule TurboFlow::Queue)
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
foreach(target IN LISTS absent_targets)
  string(FIND "${targets_text}" "${target}" target_index)
  if(NOT target_index EQUAL -1)
    message(FATAL_ERROR "${PROFILE} export unexpectedly contains ${target}")
  endif()
endforeach()
