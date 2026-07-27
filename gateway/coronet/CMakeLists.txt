set(TARGET_NAME tf_gateway_coronet)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_gateway_coronet.c
  include/turbo_flow_gateway_coronet.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::GatewayCoroNet
  FOLDER "gateway/common"
  EXPORT_NAME GatewayCoroNet)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/io/common/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::Gateway TurboNet::CoroNet
         PRIVATE tf_executor_common tf_coronet_runtime TurboUtils::Core)

install(FILES include/turbo_flow_gateway_coronet.h DESTINATION include)
