cmake_add_test(
  test_turbo_flow_local_storage
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_local_storage.c
  LIBS TurboFlow::LocalStorage TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowstore/tests
  FOLDER "flowstore/backends/local/tests")

set_tests_properties(
  test_turbo_flow_local_storage
  PROPERTIES LABELS "io;storage-contract;storage-endurance")
