cmake_add_test(
  test_turbo_flow_storage
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_storage.c
  LIBS TurboFlow::FlowStorage TurboFlow::Flow TurboUtils::TinyTest SQLite::SQLite3
  FOLDER "turbo_flow_storage/tests")
