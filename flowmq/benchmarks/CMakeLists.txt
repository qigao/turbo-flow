cmake_add_benchmark(
  bench_fmq
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_fmq.c
  LIBS TurboFlow::FMQ TurboUtils::TinyTest
  FOLDER "flowmq/benchmarks")

target_include_directories(bench_fmq
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

if(WIN32)
  target_link_libraries(bench_fmq PRIVATE ws2_32)
endif()
