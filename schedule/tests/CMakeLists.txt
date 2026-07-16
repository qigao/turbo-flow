cmake_add_test(
  test_turbo_flow_schedule
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_schedule.c
  LIBS TurboFlow::Schedule TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "schedule/tests")
