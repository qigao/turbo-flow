cmake_add_test(
  test_turbo_flow_store
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_store.c
  LIBS TurboFlow::FlowStore TurboFlow::FlowStoreDataBind TurboUtils::TinyTest
  FOLDER "flowstore/tests")

set_tests_properties(test_turbo_flow_store PROPERTIES LABELS "flowstore;storage-contract")
