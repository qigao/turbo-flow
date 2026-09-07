add_library(tf_cnet_adapter SHARED
  src/turbo_flow_cnet_datagram_sink.c
  src/turbo_flow_cnet_stream_sink.c
  src/turbo_flow_cnet_listener_source.c
  src/turbo_flow_cnet_packet_source.c
  src/turbo_flow_cnet_stream_source.c
  include/turbo_flow_cnet.h)
cmake_config_target(
  tf_cnet_adapter
  ALIAS TurboFlow::CNetAdapter
  FOLDER "io/cnet"
  EXPORT_NAME CNetAdapter)
target_compile_definitions(tf_cnet_adapter PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_cnet_adapter
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(tf_cnet_adapter PUBLIC TurboFlow::Graph Salts::CNet Salts::CFlow Salts::CSTL)

install(FILES include/turbo_flow_cnet.h DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
