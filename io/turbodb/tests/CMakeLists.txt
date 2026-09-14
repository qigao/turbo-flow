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
  LIBS TurboFlow::TurboDbAdapter TurboFlow::ProtocolIngressInbox
       tf_protocol_inbox_schema Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
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
