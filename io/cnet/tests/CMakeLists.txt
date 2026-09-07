cmake_add_test(
  test_cnet_stream_source
  SOURCES test_cnet_stream_source.c cnet_stream_source_header_cpp.cpp
          stream_source_pipe_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_stream_source
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_stream_sink
  SOURCES test_cnet_stream_sink.c cnet_stream_source_header_cpp.cpp
          stream_source_pipe_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_stream_sink
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_datagram_sink
  SOURCES test_cnet_datagram_sink.c cnet_stream_source_header_cpp.cpp
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_datagram_sink
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_listener_source
  SOURCES test_cnet_listener_source.c cnet_stream_source_header_cpp.cpp
          listener_source_tls_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_listener_source
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_packet_source
  SOURCES test_cnet_packet_source.c cnet_stream_source_header_cpp.cpp
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_packet_source
                     PROPERTIES LABELS "cnet-adapter;graph")

add_library(tf_cnet_packet_sink_test_support STATIC
            ../src/turbo_flow_cnet_packet_sink.c
            ../src/turbo_flow_cnet_test_internal.h)
target_compile_definitions(tf_cnet_packet_sink_test_support
                           PRIVATE TURBO_FLOW_CNET_INTERNAL_TESTING)
target_include_directories(
  tf_cnet_packet_sink_test_support
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_link_libraries(
  tf_cnet_packet_sink_test_support
  PUBLIC TurboFlow::Graph Salts::CNet Salts::CFlow Salts::CSTL)
cmake_config_target(tf_cnet_packet_sink_test_support FOLDER "io/cnet/tests")

cmake_add_test(
  test_cnet_packet_sink
  SOURCES test_cnet_packet_sink.c cnet_stream_source_header_cpp.cpp
  LIBS tf_cnet_packet_sink_test_support Salts::TinyTest
  DEFS TURBO_FLOW_CNET_INTERNAL_TESTING
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
           ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_packet_sink
                     PROPERTIES LABELS "cnet-adapter;graph")
