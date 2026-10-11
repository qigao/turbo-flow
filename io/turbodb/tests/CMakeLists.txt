cmake_add_test(
  test_turbodb_adapter
  SOURCES test_turbodb_adapter.c turbodb_adapter_header_cpp.cpp
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_adapter
                     PROPERTIES LABELS "turbodb-adapter;graph")
target_compile_definitions(
  test_turbodb_adapter PRIVATE
  TURBO_FLOW_TEST_SQLITE_DRIVER="${TurboDB_DRIVER_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}turbodb_driver_sqlite${CMAKE_SHARED_LIBRARY_SUFFIX}")

cmake_add_test(
  test_turbodb_outbox_source
  SOURCES test_turbodb_outbox_source.c
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_outbox_source
                     PROPERTIES LABELS "turbodb-adapter;graph;outbox")
