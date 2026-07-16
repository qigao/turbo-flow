cmake_add_benchmark(
  bench_flowie
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench_flowie.c
  LIBS Flowie::App TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../src ${CMAKE_CURRENT_SOURCE_DIR}/../tests
  FOLDER "flowie/benchmarks")

if(WIN32)
  target_link_libraries(bench_flowie PRIVATE ws2_32)
endif()
