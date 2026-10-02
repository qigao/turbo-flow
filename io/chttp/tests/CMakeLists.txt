cmake_add_test(
  test_chttp_typed_contract
  SOURCES test_chttp_typed_contract.c
  LIBS
    TurboFlow::Product
    tf_chttp_provider_support
    Salts::TinyTest
    ${turbo_flow_chttp_provider_config_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${turbo_flow_chttp_provider_config_GENERATED_DIR}
  FOLDER "io/chttp/tests")
add_dependencies(
  test_chttp_typed_contract
  turbo_flow_chttp_provider_config_idl_codegen)
set_tests_properties(test_chttp_typed_contract PROPERTIES
  LABELS "chttp-plugin;provider;config;resource;databind")

add_library(test_chttp_deployment_resource_fixture SHARED
  chttp_deployment_resource_fixture.c)
target_include_directories(test_chttp_deployment_resource_fixture PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_chttp_deployment_resource_fixture PRIVATE
  Salts::Plugin)
set_target_properties(test_chttp_deployment_resource_fixture PROPERTIES
  FOLDER "io/chttp/tests/fixtures"
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

cmake_add_test(
  test_chttp_provider_interface
  SOURCES test_chttp_provider_interface.c
  LIBS
    TurboFlow::Product
    TurboFlow::CHTTPAdapter
    Salts::Plugin
    Salts::TinyTest
    ${turbo_flow_chttp_provider_config_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${turbo_flow_chttp_provider_config_GENERATED_DIR}
  DEFS
    FLOW_CHTTP_PROVIDER_FIXTURE="$<TARGET_FILE:tf_chttp_plugin>"
    FLOW_CHTTP_RESOURCE_FIXTURE="$<TARGET_FILE:test_chttp_deployment_resource_fixture>"
  FOLDER "io/chttp/tests")
add_dependencies(test_chttp_provider_interface
  tf_chttp_plugin
  test_chttp_deployment_resource_fixture
  turbo_flow_chttp_provider_config_idl_codegen)
set_tests_properties(test_chttp_provider_interface PROPERTIES
  LABELS "chttp-plugin;provider;resource;lease;graph")

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

# Substitute only native deferred admission/completion and the publish return
# window in an isolated build of the real server owner.
add_library(tf_chttp_server_defer_fault_support STATIC chttp_server_defer_fault_support.c)
target_compile_definitions(tf_chttp_server_defer_fault_support PRIVATE
  chttp_server_response_defer=chttp_test_server_response_defer
  chttp_server_deferred_reply=chttp_test_server_deferred_reply
  chttp_server_deferred_cancel=chttp_test_server_deferred_cancel
  turbo_flow_publish_async=chttp_test_publish_async)
target_include_directories(tf_chttp_server_defer_fault_support
  PRIVATE $<TARGET_PROPERTY:xxHash::xxhash,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC ../include)
target_link_libraries(tf_chttp_server_defer_fault_support PUBLIC TurboFlow::Graph CHttp::Server)
cmake_config_target(tf_chttp_server_defer_fault_support NO_INSTALL FOLDER "io/chttp/tests")
cmake_add_test(test_chttp_server_defer_fault
  SOURCES test_chttp_server_defer_fault.c
  LIBS tf_chttp_server_defer_fault_support CHttp::Client Salts::TinyTest
  FOLDER "io/chttp/tests")
set_tests_properties(test_chttp_server_defer_fault
  PROPERTIES LABELS "chttp-adapter;graph;server")

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
  chttp_server_websocket_close=chttp_test_websocket_close
  mem_get_buffer=chttp_test_websocket_mem_get_buffer
  turbo_flow_publish_async=chttp_test_websocket_publish_async)
target_include_directories(tf_chttp_websocket_fault_support
  PRIVATE $<TARGET_PROPERTY:xxHash::xxhash,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC ../include)
target_link_libraries(tf_chttp_websocket_fault_support PUBLIC TurboFlow::Graph CHttp::Server)
cmake_config_target(tf_chttp_websocket_fault_support NO_INSTALL FOLDER "io/chttp/tests")
cmake_add_test(
  test_chttp_websocket_close_fault
  SOURCES test_chttp_websocket_close_fault.c
  LIBS tf_chttp_websocket_fault_support CHttp::Client Salts::TinyTest
  FOLDER "io/chttp/tests")
set_tests_properties(test_chttp_websocket_close_fault
  PROPERTIES LABELS "chttp-adapter;graph;server;websocket")

add_library(tf_chttp_client_fault_support STATIC ../src/turbo_flow_chttp.c)
target_compile_definitions(tf_chttp_client_fault_support PRIVATE
  chttp_async_client_stop=chttp_test_client_stop)
target_include_directories(tf_chttp_client_fault_support PUBLIC ../include)
target_link_libraries(tf_chttp_client_fault_support PUBLIC TurboFlow::Graph CHttp::Client)
cmake_config_target(tf_chttp_client_fault_support NO_INSTALL FOLDER "io/chttp/tests")
cmake_add_test(test_chttp_client_stop_fault
  SOURCES test_chttp_client_stop_fault.c chttp_header_cpp.cpp
  LIBS tf_chttp_client_fault_support CHttp::Server Salts::TinyTest
  FOLDER "io/chttp/tests")
set_tests_properties(test_chttp_client_stop_fault PROPERTIES LABELS "chttp-adapter;graph")
