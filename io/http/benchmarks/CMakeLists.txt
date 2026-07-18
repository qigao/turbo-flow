cmake_add_benchmark(
  bench_turbo_flow_http
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_turbo_flow_http.c
  LIBS TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Flow
       TurboUtils::TinyTest
  FOLDER "io/http/benchmarks")

if(WIN32)
  target_link_libraries(bench_turbo_flow_http PRIVATE ws2_32)
  add_custom_command(
    TARGET bench_turbo_flow_http POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:TurboHttp::HttpClient>
            $<TARGET_FILE_DIR:bench_turbo_flow_http>
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:TurboHttp::Iris>
            $<TARGET_FILE_DIR:bench_turbo_flow_http>)
endif()
