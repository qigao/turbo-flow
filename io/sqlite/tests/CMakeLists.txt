cmake_add_test(
  test_turbo_flow_sqlite_storage
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_sqlite_storage.c
  LIBS TurboFlow::SQLiteStorage TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/tests
  FOLDER "io/sqlite/tests")

set_tests_properties(
  test_turbo_flow_sqlite_storage
  PROPERTIES LABELS "io;sqlite;storage-contract;mqtt-persistence")

if(WIN32)
  set_tests_properties(
    test_turbo_flow_sqlite_storage
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/bin")
endif()
