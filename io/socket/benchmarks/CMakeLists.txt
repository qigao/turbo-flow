cmake_add_benchmark(
  bench_socket
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_socket.c
  LIBS TurboFlow::Socket TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "io/socket/benchmarks")

if(WIN32)
  target_link_libraries(bench_socket PRIVATE ws2_32)
endif()
