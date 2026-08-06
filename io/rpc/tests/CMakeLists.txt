cmake_add_test(
  test_turbo_flow_rpc
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_rpc.c
  LIBS TurboFlow::RPC TurboFlow::Flow TurboHttp::TurboHttp TurboUtils::TinyTest
  FOLDER "io/rpc/tests")

set_tests_properties(test_turbo_flow_rpc PROPERTIES LABELS "fmq-release;fmq-chaos")

if(WIN32)
  target_link_libraries(test_turbo_flow_rpc PRIVATE ws2_32)
  set_tests_properties(
    test_turbo_flow_rpc
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::TurboHttp>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::Iris>")
endif()

