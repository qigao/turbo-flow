cmake_add_test(
  test_turbo_flow_queue
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_queue.c
  LIBS TurboFlow::Queue TurboFlow::Flow TurboUtils::TinyTest SQLite::SQLite3
  FOLDER "flowqueue/tests")

target_compile_definitions(
  test_turbo_flow_queue PRIVATE TURBO_FLOW_QUEUE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

set_tests_properties(test_turbo_flow_queue
                     PROPERTIES LABELS "fmq-release;fmq-chaos;storage-contract")

if(TARGET Flowie::App)
  cmake_add_test(
    test_flowie_queue
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_queue.c
    LIBS TurboFlow::Queue TurboFlow::Socket Flowie::App Flowie::Protocol TurboFlow::Flow
         TurboUtils::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/flowie/tests ${PROJECT_SOURCE_DIR}/flowie/src
    FOLDER "flowqueue/tests")

  set_tests_properties(test_flowie_queue PROPERTIES LABELS "flowie;storage-contract")
  target_compile_definitions(test_flowie_queue
                             PRIVATE FLOWIE_SOURCE_DIR="${PROJECT_SOURCE_DIR}/flowie")

  if(WIN32)
    target_link_libraries(test_flowie_queue PRIVATE ws2_32)
  endif()
endif()
