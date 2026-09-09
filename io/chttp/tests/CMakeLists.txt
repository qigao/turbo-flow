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

cmake_add_test(
  test_chttp_websocket_adapter
  SOURCES test_chttp_websocket_adapter.c chttp_server_header_cpp.cpp
  LIBS TurboFlow::CHTTPAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/chttp/tests")

set_tests_properties(
  test_chttp_websocket_adapter
  PROPERTIES LABELS "chttp-adapter;graph;server;websocket")

# Substitute only the native close admission boundary in the isolated test build.
add_library(tf_chttp_websocket_fault_support STATIC chttp_websocket_fault_support.c)
target_compile_definitions(tf_chttp_websocket_fault_support PRIVATE
  chttp_server_websocket_close=chttp_test_websocket_close)
target_include_directories(tf_chttp_websocket_fault_support PUBLIC ../include)
target_link_libraries(tf_chttp_websocket_fault_support PUBLIC TurboFlow::Graph Salts::CHTTP)
cmake_config_target(tf_chttp_websocket_fault_support NO_INSTALL FOLDER "io/chttp/tests")
cmake_add_test(
  test_chttp_websocket_close_fault
  SOURCES test_chttp_websocket_close_fault.c
  LIBS tf_chttp_websocket_fault_support Salts::TinyTest
  FOLDER "io/chttp/tests")
set_tests_properties(test_chttp_websocket_close_fault
  PROPERTIES LABELS "chttp-adapter;graph;server;websocket")
