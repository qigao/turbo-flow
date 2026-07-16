cmake_add_test(
  test_turbo_flow_http
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_http.c
  LIBS TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Observe TurboFlow::Flow
       TurboUtils::DataBind TurboUtils::TinyTest
  FOLDER "io/http/tests")

set_tests_properties(test_turbo_flow_http PROPERTIES LABELS "fmq-release;fmq-chaos")

if(WIN32)
  target_link_libraries(test_turbo_flow_http PRIVATE ws2_32)
  set_tests_properties(
    test_turbo_flow_http
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::HttpClient>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::Iris>")
endif()

