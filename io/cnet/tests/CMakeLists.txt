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
