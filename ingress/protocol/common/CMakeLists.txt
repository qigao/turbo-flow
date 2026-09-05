set(TARGET_NAME tf_protocol)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_protocol.c
  src/flow_protocol_business.c
  src/flow_protocol_runtime.c
  src/flow_protocol_registry.c
  include/turbo_flow_protocol.h
  include/turbo_flow_protocol_business.h
  include/turbo_flow_protocol_business_plugin.h
  include/turbo_flow_protocol_runtime.h
  include/turbo_flow_protocol_plugin.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::ProtocolIngress
  FOLDER "ingress/protocol/common"
  EXPORT_NAME ProtocolIngress)

target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/turbo_flow/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME}
                      PUBLIC Salts::Core PRIVATE ${CMAKE_DL_LIBS})

install(
  FILES include/turbo_flow_protocol.h
        include/turbo_flow_protocol_business.h
        include/turbo_flow_protocol_business_plugin.h
        include/turbo_flow_protocol_runtime.h
        include/turbo_flow_protocol_plugin.h
  DESTINATION include)
