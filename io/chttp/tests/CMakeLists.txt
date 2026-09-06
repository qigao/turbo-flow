cmake_add_test(
  test_chttp_adapter
  SOURCES test_chttp_adapter.c chttp_header_cpp.cpp
  LIBS TurboFlow::CHTTPAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/chttp/tests")

set_tests_properties(test_chttp_adapter PROPERTIES LABELS "chttp-adapter;graph")

cmake_add_test(
  test_chttp_server_adapter
  SOURCES test_chttp_server_adapter.c chttp_server_header_cpp.cpp
  LIBS TurboFlow::CHTTPAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/chttp/tests")

set_tests_properties(test_chttp_server_adapter PROPERTIES LABELS "chttp-adapter;graph;server")
