set(TARGET_NAME tf_protocol_graph)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_protocol_graph.c
  include/turbo_flow_protocol_graph.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::ProtocolIngressGraph
  FOLDER "ingress/protocol/graph"
  EXPORT_NAME ProtocolIngressGraph)

target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME}
                      PUBLIC TurboFlow::ProtocolIngress TurboFlow::Graph)

install(FILES include/turbo_flow_protocol_graph.h DESTINATION include)
