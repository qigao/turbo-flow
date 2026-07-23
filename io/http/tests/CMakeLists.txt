cmake_add_test(
  test_turbo_flow_http
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_http.c
  LIBS TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Observe TurboFlow::Flow
       TurboUtils::DataBind TurboUtils::TinyTest
  FOLDER "io/http/tests")

target_include_directories(test_turbo_flow_http PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src
                                                        ${PROJECT_SOURCE_DIR}/io/socket/tests)
target_link_libraries(test_turbo_flow_http PRIVATE OpenSSL::SSL OpenSSL::Crypto TurboNet::CoroNet)
cmake_target_enable_boringssl_linkage(test_turbo_flow_http)

set_tests_properties(
  test_turbo_flow_http
  PROPERTIES LABELS "fmq-release;fmq-chaos;mqtt-security;mqtt-negative")

if(WIN32)
  target_link_libraries(test_turbo_flow_http PRIVATE ws2_32)
  set_tests_properties(
    test_turbo_flow_http
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::HttpClient>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboHttp::Iris>")
endif()

