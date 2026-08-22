set(TARGET_NAME tf_protocol_coronet)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_protocol_coronet.c
  include/turbo_flow_protocol_coronet.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::ProtocolIngressCoroNet
  FOLDER "ingress/protocol/coronet"
  EXPORT_NAME ProtocolIngressCoroNet)

target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/io/common/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::ProtocolIngress TurboNet::CoroNet
         PRIVATE tf_executor_common tf_coronet_runtime TurboUtils::Core)

install(FILES include/turbo_flow_protocol_coronet.h DESTINATION include)
