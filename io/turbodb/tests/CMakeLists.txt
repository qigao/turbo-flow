cmake_add_test(
  test_turbodb_adapter
  SOURCES test_turbodb_adapter.c turbodb_adapter_header_cpp.cpp
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_adapter
                     PROPERTIES LABELS "turbodb-adapter;graph")
