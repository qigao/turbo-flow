add_library(
  tf_protocol_network_intake_core STATIC
  src/flow_protocol_network_intake_config.c
  src/flow_protocol_network_intake_sink.c
  src/flow_protocol_network_intake_internal.h)
set_target_properties(
  tf_protocol_network_intake_core
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             FOLDER "ingress/protocol/network/internal")
target_include_directories(
  tf_protocol_network_intake_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  tf_protocol_network_intake_core
  PUBLIC TurboFlow::Graph TurboFlow::ProtocolIngressInbox TurboFlow::PluginHost
  PRIVATE TurboFlow::CNetAdapter xxHash::xxhash)
