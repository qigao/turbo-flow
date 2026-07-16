cmake_add_source(BENCHMARK_SOURCES DIRS ${CMAKE_CURRENT_SOURCE_DIR}
                 PATTERNS "bench_*.c" "test_*_bench.c")

cmake_add_benchmark(
  bench_turbo_flow
  SOURCES ${BENCHMARK_SOURCES}
  FOLDER "turbo_flow/benchmarks"
  LIBS TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src)
