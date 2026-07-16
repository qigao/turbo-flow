cmake_add_test(
  test_turbo_flow_codec
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_codec.c
  LIBS TurboFlow::Codec TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "codec/tests")
