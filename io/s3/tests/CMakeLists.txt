cmake_add_test(
  test_turbo_flow_s3
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_s3.c
  LIBS TurboFlow::S3 TurboFlow::HttpServer TurboFlow::Observe TurboFlow::Flow
       TurboHttp::Iris TurboUtils::DataBind TurboUtils::TinyTest
  FOLDER "io/s3/tests")

if(WIN32)
  target_link_libraries(test_turbo_flow_s3 PRIVATE ws2_32)
  set_tests_properties(
    test_turbo_flow_s3
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::S3>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::HttpClient>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::Iris>")
endif()

