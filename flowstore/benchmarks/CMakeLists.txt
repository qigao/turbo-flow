cmake_add_benchmark(
  bench_turbo_flow_store_index
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_flow_store_index.c
  LIBS TurboFlow::FlowStore TurboUtils::TinyTest
  FOLDER "flowstore/benchmarks")
