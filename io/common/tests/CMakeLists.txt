foreach(test_name IN ITEMS test_connection test_coronet_actor test_event_time_watermark
                           test_io_policy test_timer)
  cmake_add_test(
    ${test_name}
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/${test_name}.c
    LIBS tf_executor_common TurboFlow::Flow TurboUtils::TinyTest
    FOLDER "io/common/tests")
endforeach()

cmake_add_test(
  test_coronet_runtime
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_coronet_runtime.c
  LIBS tf_coronet_runtime TurboUtils::TinyTest
  FOLDER "io/common/tests")

cmake_add_benchmark(
  bench_event_time_watermark
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_event_time_watermark.c
  LIBS tf_executor_common TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "io/common/benchmarks")

