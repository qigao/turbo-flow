cmake_minimum_required(VERSION 3.25)

foreach(required_variable IN ITEMS TURBO_FLOW_SOURCE_DIR TURBO_FLOW_TEST_ROOT)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

get_filename_component(TURBO_FLOW_SOURCE_DIR "${TURBO_FLOW_SOURCE_DIR}" ABSOLUTE)
get_filename_component(TURBO_FLOW_TEST_ROOT "${TURBO_FLOW_TEST_ROOT}" ABSOLUTE)
file(REMOVE_RECURSE "${TURBO_FLOW_TEST_ROOT}")
file(MAKE_DIRECTORY "${TURBO_FLOW_TEST_ROOT}")

include(CMakePackageConfigHelpers)

set(salts_root "${TURBO_FLOW_TEST_ROOT}/salts")
set(databind_root "${TURBO_FLOW_TEST_ROOT}/databind")
set(databind_outside_root "${TURBO_FLOW_TEST_ROOT}/databind-outside")
set(turbo_flow_root "${TURBO_FLOW_TEST_ROOT}/turbo-flow")
set(consumer_source_dir "${TURBO_FLOW_TEST_ROOT}/consumer")

foreach(package_dir IN ITEMS
        "${salts_root}/lib/cmake/Salts"
        "${databind_root}/lib/cmake/DataBind"
        "${databind_outside_root}/lib/cmake/DataBind"
        "${turbo_flow_root}/lib/cmake/TurboFlow"
        "${consumer_source_dir}")
  file(MAKE_DIRECTORY "${package_dir}")
endforeach()

file(WRITE "${salts_root}/lib/cmake/Salts/SaltsConfig.cmake" [=[
if(NOT TARGET Salts::Core)
  add_library(Salts::Core INTERFACE IMPORTED)
endif()
if(NOT TARGET Salts::CNet)
  add_library(Salts::CNet INTERFACE IMPORTED)
endif()
]=])

file(WRITE "${databind_root}/lib/cmake/DataBind/DataBindConfig.cmake" [=[
if(NOT TARGET Salts::DataBind)
  add_library(Salts::DataBind INTERFACE IMPORTED)
endif()
if(NOT TARGET Salts::TbeSchema)
  add_library(Salts::TbeSchema INTERFACE IMPORTED)
endif()
]=])
write_basic_package_version_file(
  "${databind_root}/lib/cmake/DataBind/DataBindConfigVersion.cmake"
  VERSION 3.0.0
  COMPATIBILITY SameMajorVersion)

file(COPY
     "${databind_root}/lib/cmake/DataBind/DataBindConfig.cmake"
     "${databind_root}/lib/cmake/DataBind/DataBindConfigVersion.cmake"
     DESTINATION "${databind_outside_root}/lib/cmake/DataBind")

set(TURBO_FLOW_HAS_TURBODB_ADAPTER OFF)
configure_package_config_file(
  "${TURBO_FLOW_SOURCE_DIR}/cmake/TurboFlowConfig.cmake.in"
  "${turbo_flow_root}/lib/cmake/TurboFlow/TurboFlowConfig.cmake"
  INSTALL_DESTINATION "lib/cmake/TurboFlow")
write_basic_package_version_file(
  "${turbo_flow_root}/lib/cmake/TurboFlow/TurboFlowConfigVersion.cmake"
  VERSION 2.0.0
  COMPATIBILITY SameMajorVersion)

file(WRITE "${turbo_flow_root}/lib/cmake/TurboFlow/TurboFlowTargets.cmake" [=[
if(NOT TARGET TurboFlow::Config)
  add_library(TurboFlow::Config INTERFACE IMPORTED)
endif()
if(NOT TARGET TurboFlow::ProtocolIngressInboxSchema)
  add_library(TurboFlow::ProtocolIngressInboxSchema INTERFACE IMPORTED)
endif()
]=])
file(WRITE "${turbo_flow_root}/lib/cmake/TurboFlow/TurboFlowRequireCNet.cmake" [=[
function(turbo_flow_require_cnet_stop_drain_contract)
  if(NOT TARGET Salts::CNet)
    message(FATAL_ERROR "fixture requires Salts::CNet")
  endif()
endfunction()
]=])
file(WRITE "${turbo_flow_root}/lib/cmake/TurboFlow/TurboFlowRequireCHTTP.cmake" "")

file(WRITE "${consumer_source_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.25)
project(TurboFlowDataBindPackageContract LANGUAGES C)

if(TEST_PRELOAD_DATABIND)
  add_library(Salts::DataBind INTERFACE IMPORTED)
  unset(DataBind_DIR)
  unset(DataBind_DIR CACHE)
endif()

if(NOT DEFINED TEST_COMPONENT OR "${TEST_COMPONENT}" STREQUAL "")
  message(FATAL_ERROR "TEST_COMPONENT is required")
endif()

find_package(TurboFlow 2 CONFIG REQUIRED
             COMPONENTS "${TEST_COMPONENT}"
             PATHS "$ENV{TURBO_FLOW_ROOT}"
             NO_DEFAULT_PATH)

if(NOT TARGET "TurboFlow::${TEST_COMPONENT}")
  message(FATAL_ERROR "missing requested TurboFlow target: ${TEST_COMPONENT}")
endif()
]=])

function(run_contract_case case_name expect_success expected_pattern)
  cmake_parse_arguments(CASE "" "" "ENV;CMAKE" ${ARGN})
  set(binary_dir "${TURBO_FLOW_TEST_ROOT}/build/${case_name}")
  execute_process(
    COMMAND
      "${CMAKE_COMMAND}" -E env
      --unset=SALTS_UTILS_ROOT
      --unset=RULES_FORGE_ROOT
      --unset=DATABIND_ROOT
      ${CASE_ENV}
      "SALTS_ROOT=${salts_root}"
      "TURBO_FLOW_ROOT=${turbo_flow_root}"
      "${CMAKE_COMMAND}" -S "${consumer_source_dir}" -B "${binary_dir}"
      -G Ninja
      -DCMAKE_BUILD_TYPE=Release
      ${CASE_CMAKE}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr)
  string(CONCAT diagnostic "${stdout}" "\n" "${stderr}")

  if(expect_success)
    if(NOT result EQUAL 0)
      message(FATAL_ERROR
              "${case_name} unexpectedly failed:\n${diagnostic}")
    endif()
  else()
    if(result EQUAL 0)
      message(FATAL_ERROR "${case_name} unexpectedly succeeded")
    endif()
    if(NOT diagnostic MATCHES "${expected_pattern}")
      message(FATAL_ERROR
              "${case_name} failed without expected diagnostic '${expected_pattern}':\n${diagnostic}")
    endif()
  endif()
endfunction()

run_contract_case(
  schema-without-salts-utils TRUE ""
  ENV "DATABIND_ROOT=${databind_root}"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  config-without-databind TRUE ""
  CMAKE
    "-DTEST_COMPONENT=Config"
    "-DCMAKE_DISABLE_FIND_PACKAGE_DataBind=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  schema-missing-databind-root FALSE
  "DATABIND_ROOT is required for TurboFlow dependency DataBind"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  schema-wrong-databind-dir FALSE
  "DataBind_DIR is outside DATABIND_ROOT"
  ENV "DATABIND_ROOT=${databind_root}"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DDataBind_DIR=${databind_outside_root}/lib/cmake/DataBind"
    "-DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  schema-preloaded-unverifiable FALSE
  "DataBind is already loaded but its package root cannot be verified"
  ENV "DATABIND_ROOT=${databind_root}"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DTEST_PRELOAD_DATABIND=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

file(READ "${TURBO_FLOW_SOURCE_DIR}/CMakeLists.txt" root_cmake)
foreach(required_fragment IN ITEMS
        "SALTS_ROOT SALTS_UTILS_ROOT DATABIND_ROOT RULES_FORGE_ROOT"
        "find_package(DataBind 3 CONFIG REQUIRED"
        "SALTS_UTILS_ROOT still owns DataBind target"
        "DATABIND_ROOT must be physically distinct from SALTS_UTILS_ROOT")
  string(FIND "${root_cmake}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "source package cut missing fragment: ${required_fragment}")
  endif()
endforeach()

file(READ "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/CMakeLists.txt" protocol_cmake)
foreach(required_fragment IN ITEMS
        "DATABIND_HOST_ROOT"
        "find_program("
        "NO_DEFAULT_PATH"
        "tbe_compiler resolved outside the DataBind host root")
  string(FIND "${protocol_cmake}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "compiler provenance cut missing fragment: ${required_fragment}")
  endif()
endforeach()

file(READ "${TURBO_FLOW_SOURCE_DIR}/cmake/TurboFlowConfig.cmake.in" config_template)
string(FIND "${config_template}"
            "_TurboFlow_require_dependency_root(SaltsUtils SALTS_UTILS_ROOT Salts::DataBind)"
            stale_salts_utils_ownership)
if(NOT stale_salts_utils_ownership EQUAL -1)
  message(FATAL_ERROR
          "installed package still treats Salts::DataBind as SaltsUtils provenance")
endif()

file(READ "${TURBO_FLOW_SOURCE_DIR}/CMakeUserPresets.json" user_presets)
foreach(required_fragment IN ITEMS
        "\"DATABIND_ROOT\""
        "\"DATABIND_HOST_ROOT\"")
  string(FIND "${user_presets}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "preset package cut missing fragment: ${required_fragment}")
  endif()
endforeach()
string(FIND "${user_presets}" "\"SALTS_UTILS_HOST_ROOT\"" stale_host_root)
if(NOT stale_host_root EQUAL -1)
  message(FATAL_ERROR "presets still use SALTS_UTILS_HOST_ROOT for tbe_compiler")
endif()
string(FIND "${user_presets}"
            "\"DATABIND_ROOT\": \"$env{PKG_ROOT}/salts-utils"
            stale_databind_profile_root)
if(NOT stale_databind_profile_root EQUAL -1)
  message(FATAL_ERROR "presets still place DataBind under the SaltsUtils root")
endif()
string(FIND "${user_presets}"
            "\"DATABIND_HOST_ROOT\": \"$env{PKG_ROOT}/salts-utils"
            stale_databind_host_root)
if(NOT stale_databind_host_root EQUAL -1)
  message(FATAL_ERROR "presets still source tbe_compiler from the SaltsUtils root")
endif()

message(STATUS "TurboFlow DataBind package-root contract passed")
