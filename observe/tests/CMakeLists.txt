cmake_add_test(
  test_turbo_flow_observe
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_observe.c
  LIBS TurboFlow::Observe TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "observe/tests")
