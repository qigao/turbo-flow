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

add_library(
  tf_protocol_network_intake SHARED
  src/flow_protocol_network_intake.c
  include/turbo_flow_protocol_network_intake.h)
cmake_config_target(
  tf_protocol_network_intake
  ALIAS TurboFlow::ProtocolNetworkIntake
  FOLDER "ingress/protocol/network"
  EXPORT_NAME ProtocolNetworkIntake)
target_compile_definitions(tf_protocol_network_intake PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_protocol_network_intake
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  tf_protocol_network_intake
  PUBLIC TurboFlow::Graph TurboFlow::PluginHost
  PRIVATE tf_protocol_network_intake_core TurboFlow::CNetAdapter
          TurboFlow::ProtocolIngressInbox xxHash::xxhash)

install(FILES include/turbo_flow_protocol_network_intake.h DESTINATION include)
