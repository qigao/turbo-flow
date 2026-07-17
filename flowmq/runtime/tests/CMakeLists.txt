cmake_add_test(
  test_flowmq_connect_endpoint
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_connect_endpoint.c
  LIBS flowmq_connect_endpoint_runtime TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

cmake_add_test(
  test_flowmq_pattern
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_pattern.c
  LIBS flowmq_runtime_core TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

cmake_add_test(
  test_flowmq_reconnect
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_reconnect.c
  LIBS flowmq_runtime_core TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

cmake_add_test(
  test_flowmq_peer_session
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_peer_session.c
  LIBS flowmq_runtime_core TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

cmake_add_test(
  test_flowmq_coronet_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_coronet_transport.c
  LIBS flowmq_coronet_transport TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

cmake_add_test(
  test_flowmq_subscription_set
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_subscription_set.c
  LIBS flowmq_runtime_core TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

cmake_add_test(
  test_flowmq_stream_decoder
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowmq_stream_decoder.c
  LIBS flowmq_runtime_core TurboUtils::TinyTest
  FOLDER "flowmq/runtime/tests")

set_tests_properties(test_flowmq_connect_endpoint test_flowmq_pattern test_flowmq_peer_session
                     test_flowmq_stream_decoder test_flowmq_subscription_set
                     test_flowmq_coronet_transport test_flowmq_reconnect
                     PROPERTIES LABELS "fmq-release;flowmq-runtime")
