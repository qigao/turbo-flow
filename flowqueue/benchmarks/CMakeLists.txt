cmake_add_test(
  bench_flowqueue
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_flowqueue.c
  LIBS TurboFlow::Queue TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowqueue/benchmarks")

set_tests_properties(bench_flowqueue PROPERTIES LABELS "benchmark;flowqueue")
