cmake_add_benchmark(
  bench_redis
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_redis.c
  LIBS TurboFlow::Redis TurboFlow::Flow TurboNet::Redis TurboNet::CoroNet
       TurboUtils::TinyTest
  FOLDER "flowstore/backends/redis/benchmarks")

if(WIN32)
  target_link_libraries(bench_redis PRIVATE ws2_32)
endif()
