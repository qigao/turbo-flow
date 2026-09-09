cmake_add_test(test_chttp_plugin
  SOURCES test_chttp_plugin.c
  LIBS TurboFlow::PluginHost Salts::CHTTP Salts::TinyTest
  DEFS TURBO_FLOW_CHTTP_PLUGIN_PATH="$<TARGET_FILE:tf_chttp_plugin>"
  FOLDER "io/chttp/tests")
set_tests_properties(test_chttp_plugin PROPERTIES LABELS "chttp-plugin;plugin;graph")
add_dependencies(test_chttp_plugin tf_chttp_plugin)

foreach(mode RANGE 0 2)
  add_library(tf_chttp_delivery_fixture_${mode} SHARED
    chttp_plugin_delivery_fixture.c ../src/turbo_flow_chttp_plugin_config.c)
  target_compile_definitions(tf_chttp_delivery_fixture_${mode} PRIVATE
    TURBO_FLOW_PLUGIN_BUILD CHTTP_DELIVERY_MODE=${mode})
  target_include_directories(tf_chttp_delivery_fixture_${mode} PRIVATE ../include ../src)
  target_link_libraries(tf_chttp_delivery_fixture_${mode} PRIVATE
    tf_chttp_adapter TurboFlow::Product Salts::CSTL Salts::UriParser)
  if(WIN32)
    target_link_libraries(tf_chttp_delivery_fixture_${mode} PRIVATE ws2_32)
  endif()
  cmake_config_target(tf_chttp_delivery_fixture_${mode} NO_INSTALL FOLDER "io/chttp/tests")
  target_compile_definitions(test_chttp_plugin PRIVATE
    CHTTP_DELIVERY_FIXTURE_${mode}="$<TARGET_FILE:tf_chttp_delivery_fixture_${mode}>")
  add_dependencies(test_chttp_plugin tf_chttp_delivery_fixture_${mode})
endforeach()

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

add_library(tf_chttp_client_fault_support STATIC ../src/turbo_flow_chttp.c)
target_compile_definitions(tf_chttp_client_fault_support PRIVATE
  chttp_async_client_stop=chttp_test_client_stop)
target_include_directories(tf_chttp_client_fault_support PUBLIC ../include)
target_link_libraries(tf_chttp_client_fault_support PUBLIC TurboFlow::Graph Salts::CHTTP)
cmake_config_target(tf_chttp_client_fault_support NO_INSTALL FOLDER "io/chttp/tests")
cmake_add_test(test_chttp_client_stop_fault
  SOURCES test_chttp_client_stop_fault.c chttp_header_cpp.cpp
  LIBS tf_chttp_client_fault_support Salts::TinyTest
  FOLDER "io/chttp/tests")
set_tests_properties(test_chttp_client_stop_fault PROPERTIES LABELS "chttp-adapter;graph")
