cmake_add_test(
  test_turbodb_adapter
  SOURCES test_turbodb_adapter.c turbodb_adapter_header_cpp.cpp
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_adapter
                     PROPERTIES LABELS "turbodb-adapter;graph")

cmake_add_test(
  test_turbodb_outbox_source
  SOURCES test_turbodb_outbox_source.c
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_outbox_source
                     PROPERTIES LABELS "turbodb-adapter;graph;outbox")

cmake_add_test(
  test_turbodb_inbox
  SOURCES test_turbodb_inbox.c
  LIBS TurboFlow::TurboDbAdapter TurboFlow::ProtocolIngress
       tf_protocol_inbox_schema Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
           ${PROJECT_SOURCE_DIR}/ingress/protocol/inbox/include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_inbox
                     PROPERTIES LABELS "turbodb-adapter;graph;inbox")

cmake_add_test(
  test_turbodb_inbox_source
  SOURCES test_turbodb_inbox_source.c
  LIBS TurboFlow::TurboDbAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/turbodb/tests")

set_tests_properties(test_turbodb_inbox_source
                     PROPERTIES LABELS "turbodb-adapter;graph;inbox")

cmake_add_test(test_turbodb_durable_buffer_plugin
  SOURCES test_turbodb_durable_buffer_plugin.c
  LIBS TurboFlow::PluginHost TurboFlow::TurboDbAdapter Salts::TinyTest
  FOLDER "io/turbodb/tests")
add_dependencies(test_turbodb_durable_buffer_plugin tf_turbodb_plugin)
set_tests_properties(test_turbodb_durable_buffer_plugin PROPERTIES
  LABELS "turbodb-adapter;graph;durable;plugin"
  ENVIRONMENT "TURBO_FLOW_TURBODB_PLUGIN_PATH=$<TARGET_FILE:tf_turbodb_plugin>")
