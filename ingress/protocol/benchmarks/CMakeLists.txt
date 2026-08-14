cmake_add_benchmark(
  bench_protocol_ingress
  SOURCES bench_protocol_ingress.c
  LIBS TurboFlow::ProtocolIngressCoroNet TurboFlow::ProtocolIngressGraph
       TurboUtils::TinyTest
  DEFS FLOW_PROTOCOL_GBT32960_MODULE="$<TARGET_FILE:tf_protocol_gbt32960>"
  FOLDER "ingress/protocol/benchmarks")
add_dependencies(bench_protocol_ingress tf_protocol_gbt32960)

if(WIN32)
  target_link_libraries(bench_protocol_ingress PRIVATE ws2_32)
endif()
