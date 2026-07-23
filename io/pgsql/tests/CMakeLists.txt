cmake_add_test(test_turbo_flow_pgsql
               SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_pgsql.c
               LIBS TurboFlow::PostgreSQL TurboFlow::Flow PostgreSQL::PostgreSQL
                    TurboUtils::DataBind TurboUtils::TinyTest
               INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src
               FOLDER "io/pgsql/tests")

cmake_add_test(test_turbo_flow_pgsql_live
               SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_pgsql_live.c
               LIBS TurboFlow::PostgreSQL TurboFlow::Flow PostgreSQL::PostgreSQL
                    TurboUtils::TinyTest
               INCLUDES ${PROJECT_SOURCE_DIR}/flowie/tests
               FOLDER "io/pgsql/tests")

# Keep result conversion internal to the PostgreSQL DLL ABI while testing it directly.
target_sources(test_turbo_flow_pgsql PRIVATE
               ${CMAKE_CURRENT_SOURCE_DIR}/../src/flow_pgsql_result.c)
target_compile_definitions(
  test_turbo_flow_pgsql PRIVATE TURBO_FLOW_PGSQL_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

set_tests_properties(test_turbo_flow_pgsql test_turbo_flow_pgsql_live
                     PROPERTIES LABELS "storage-contract")
set_tests_properties(
  test_turbo_flow_pgsql_live
  PROPERTIES LABELS "storage-contract;integration;pgsql-live;mqtt-persistence;mqtt-soak")
if(NOT TURBO_FLOW_PGSQL_LIVE_TESTS)
  set_tests_properties(test_turbo_flow_pgsql_live PROPERTIES DISABLED TRUE)
endif()

if(WIN32)
  set_tests_properties(
    test_turbo_flow_pgsql test_turbo_flow_pgsql_live
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/bin")
endif()

