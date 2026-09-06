cmake_add_test(
  test_cnet_stream_source
  SOURCES test_cnet_stream_source.c cnet_stream_source_header_cpp.cpp
          stream_source_pipe_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_stream_source
                     PROPERTIES LABELS "cnet-adapter;graph")
