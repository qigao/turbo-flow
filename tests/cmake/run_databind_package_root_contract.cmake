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
set(salts_utils_root "${TURBO_FLOW_TEST_ROOT}/salts-utils")
set(salts_utils_outside_root "${TURBO_FLOW_TEST_ROOT}/salts-utils-outside")
set(turbo_flow_root "${TURBO_FLOW_TEST_ROOT}/turbo-flow")
set(consumer_source_dir "${TURBO_FLOW_TEST_ROOT}/consumer")

foreach(package_dir IN ITEMS
        "${salts_root}/lib/cmake/Salts"
        "${salts_utils_root}/lib/cmake/SaltsUtils"
        "${salts_utils_outside_root}/lib/cmake/SaltsUtils"
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
write_basic_package_version_file(
  "${salts_root}/lib/cmake/Salts/SaltsConfigVersion.cmake"
  VERSION 3.0.0
  COMPATIBILITY SameMajorVersion)

file(WRITE "${salts_utils_root}/lib/cmake/SaltsUtils/SaltsUtilsConfig.cmake" [=[
if(NOT TARGET Salts::JsonParser)
  add_library(Salts::JsonParser INTERFACE IMPORTED)
endif()
if(NOT TARGET Salts::DataBind)
  add_library(Salts::DataBind INTERFACE IMPORTED)
endif()
]=])
write_basic_package_version_file(
  "${salts_utils_root}/lib/cmake/SaltsUtils/SaltsUtilsConfigVersion.cmake"
  VERSION 4.1.3
  COMPATIBILITY ExactVersion)

file(COPY
     "${salts_utils_root}/lib/cmake/SaltsUtils/SaltsUtilsConfig.cmake"
     "${salts_utils_root}/lib/cmake/SaltsUtils/SaltsUtilsConfigVersion.cmake"
     DESTINATION "${salts_utils_outside_root}/lib/cmake/SaltsUtils")

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
      --unset=TURBODB_ROOT
      --unset=DATABIND_ROOT
      --unset=DATABIND_HOST_ROOT
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
  schema-with-salts-utils TRUE ""
  ENV "SALTS_UTILS_ROOT=${salts_utils_root}"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  config-without-salts-utils TRUE ""
  CMAKE
    "-DTEST_COMPONENT=Config"
    "-DCMAKE_DISABLE_FIND_PACKAGE_SaltsUtils=TRUE"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  schema-missing-salts-utils-root FALSE
  "SALTS_UTILS_ROOT is required for TurboFlow dependency SaltsUtils"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

run_contract_case(
  schema-wrong-salts-utils-dir FALSE
  "SaltsUtils_DIR is outside SALTS_UTILS_ROOT"
  ENV "SALTS_UTILS_ROOT=${salts_utils_root}"
  CMAKE
    "-DTEST_COMPONENT=ProtocolIngressInboxSchema"
    "-DSaltsUtils_DIR=${salts_utils_outside_root}/lib/cmake/SaltsUtils"
    "-DCMAKE_DISABLE_FIND_PACKAGE_RulesForge=TRUE")

file(READ "${TURBO_FLOW_SOURCE_DIR}/CMakeLists.txt" root_cmake)
foreach(required_fragment IN ITEMS
        "SALTS_ROOT SALTS_UTILS_ROOT"
        "find_package(Salts CONFIG REQUIRED"
        "find_package(SaltsUtils CONFIG REQUIRED"
        "find_package(TurboDB CONFIG REQUIRED"
        "resolved SaltsUtils package is missing required target Salts::DataBind"
        "resolved TurboDB package is missing required target Orm::C")
  string(FIND "${root_cmake}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "source package cut missing fragment: ${required_fragment}")
  endif()
endforeach()
foreach(forbidden_fragment IN ITEMS
        "DATABIND_ROOT"
        "find_package(DataBind"
        "find_package(Orm"
        "find_package(Salts 1."
        "find_package(SaltsUtils 4."
        " EXACT CONFIG REQUIRED"
        "Salts::TbeSchema")
  string(FIND "${root_cmake}" "${forbidden_fragment}" fragment_pos)
  if(NOT fragment_pos EQUAL -1)
    message(FATAL_ERROR "source package cut retains stale fragment: ${forbidden_fragment}")
  endif()
endforeach()

file(READ "${TURBO_FLOW_SOURCE_DIR}/cmake/TurboFlowConfig.cmake.in" config_template)
foreach(required_fragment IN ITEMS
        "find_dependency(Salts CONFIG REQUIRED"
        "find_dependency(SaltsUtils CONFIG REQUIRED"
        "find_dependency(TurboDB CONFIG REQUIRED"
        "resolved SaltsUtils package is missing Salts::DataBind"
        "resolved TurboDB package is missing Orm::C")
  string(FIND "${config_template}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "installed config missing fragment: ${required_fragment}")
  endif()
endforeach()
foreach(forbidden_fragment IN ITEMS
        "DATABIND_ROOT"
        "find_dependency(DataBind"
        "find_dependency(Orm"
        "find_dependency(Salts 1."
        "find_dependency(SaltsUtils 4."
        "find_dependency(RulesForge 0."
        " EXACT CONFIG REQUIRED"
        "Salts::TbeSchema")
  string(FIND "${config_template}" "${forbidden_fragment}" fragment_pos)
  if(NOT fragment_pos EQUAL -1)
    message(FATAL_ERROR "installed config retains stale fragment: ${forbidden_fragment}")
  endif()
endforeach()

file(READ "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/CMakeLists.txt" protocol_cmake)
foreach(required_fragment IN ITEMS
        "SALTS_UTILS_HOST_ROOT"
        "TURBO_FLOW_IDLC_HOST_EXECUTABLE"
        "NAMES salts-idlc salts-idlc.exe"
        "salts-idlc resolved outside the SaltsUtils host root")
  string(FIND "${protocol_cmake}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "compiler provenance cut missing fragment: ${required_fragment}")
  endif()
endforeach()
foreach(forbidden_fragment IN ITEMS
        "DATABIND_HOST_ROOT"
        "tbe_compiler")
  string(FIND "${protocol_cmake}" "${forbidden_fragment}" fragment_pos)
  if(NOT fragment_pos EQUAL -1)
    message(FATAL_ERROR "compiler provenance retains stale fragment: ${forbidden_fragment}")
  endif()
endforeach()

file(READ "${TURBO_FLOW_SOURCE_DIR}/CMakeUserPresets.json" user_presets)
foreach(forbidden_fragment IN ITEMS
        "\"DATABIND_ROOT\""
        "\"DATABIND_HOST_ROOT\"")
  string(FIND "${user_presets}" "${forbidden_fragment}" fragment_pos)
  if(NOT fragment_pos EQUAL -1)
    message(FATAL_ERROR "presets retain stale DataBind root: ${forbidden_fragment}")
  endif()
endforeach()

file(READ "${TURBO_FLOW_SOURCE_DIR}/.github/ci/task4_runtime_assembly.py"
          task4_runtime_assembly)
string(FIND "${task4_runtime_assembly}"
            "SALTS_ROOT SALTS_UTILS_ROOT DATABIND_ROOT"
            stale_task4_root)
if(NOT stale_task4_root EQUAL -1)
  message(FATAL_ERROR "focused Task 4 assembly still requires DATABIND_ROOT")
endif()

file(READ "${TURBO_FLOW_SOURCE_DIR}/.github/workflows/rulesforge-provider.yml"
          rulesforge_workflow)
foreach(required_fragment IN ITEMS
        "restore-native-sdks.ps1 -Rid linux-x64 -WithRulesForge"
        "VCPKG_INSTALL_OPTIONS=--only-binarycaching"
        "TURBO_FLOW_IDLC_HOST_EXECUTABLE"
        "test -z \"\${DATABIND_ROOT:-}\"")
  string(FIND "${rulesforge_workflow}" "${required_fragment}" fragment_pos)
  if(fragment_pos EQUAL -1)
    message(FATAL_ERROR "RulesForge provider gate missing published dependency fragment: ${required_fragment}")
  endif()
endforeach()
foreach(forbidden_fragment IN ITEMS
        "restore-versioned-native-sdks.ps1"
        "Checkout pinned Salts"
        "Checkout pinned SaltsUtils"
        "Checkout pinned RulesForge"
        "/opt/databind")
  string(FIND "${rulesforge_workflow}" "${forbidden_fragment}" fragment_pos)
  if(NOT fragment_pos EQUAL -1)
    message(FATAL_ERROR "RulesForge provider gate retains stale dependency path: ${forbidden_fragment}")
  endif()
endforeach()

message(STATUS "TurboFlow SaltsUtils/DataBind package contract passed")
