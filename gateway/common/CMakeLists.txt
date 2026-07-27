set(TARGET_NAME tf_gateway)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_gateway.c
  src/flow_gateway_business.c
  src/flow_gateway_runtime.c
  src/flow_gateway_registry.c
  include/turbo_flow_gateway.h
  include/turbo_flow_gateway_business.h
  include/turbo_flow_gateway_business_plugin.h
  include/turbo_flow_gateway_runtime.h
  include/turbo_flow_gateway_plugin.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::Gateway
  FOLDER "gateway/common"
  EXPORT_NAME Gateway)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME}
                      PUBLIC TurboUtils::Core PRIVATE ${CMAKE_DL_LIBS})

install(
  FILES include/turbo_flow_gateway.h
        include/turbo_flow_gateway_business.h
        include/turbo_flow_gateway_business_plugin.h
        include/turbo_flow_gateway_runtime.h
        include/turbo_flow_gateway_plugin.h
  DESTINATION include)
