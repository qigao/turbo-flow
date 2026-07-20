cmake_add_benchmark(
  bench_turbo_flow_http
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_turbo_flow_http.c
  LIBS TurboFlow::HttpClient TurboFlow::HttpServer TurboFlow::Flow
       TurboUtils::TinyTest
  FOLDER "io/http/benchmarks")

if(WIN32)
  target_link_libraries(bench_turbo_flow_http PRIVATE ws2_32)
endif()
