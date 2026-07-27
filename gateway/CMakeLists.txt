add_subdirectory(common)
add_subdirectory(coronet)

add_library(
  tf_gateway_plugin_support STATIC
  src/flow_gateway_plugin_support.c
  src/flow_gateway_plugin_support.h)
set_target_properties(tf_gateway_plugin_support PROPERTIES
                      POSITION_INDEPENDENT_CODE ON
                      FOLDER "gateway/internal")
target_include_directories(
  tf_gateway_plugin_support
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tf_gateway_plugin_support PUBLIC TurboFlow::Gateway)

function(turbo_flow_add_gateway target_name export_name source_file)
  add_library(${target_name} SHARED ${source_file})
  cmake_config_target(
    ${target_name}
    FOLDER "gateway/plugins"
    EXPORT_NAME ${export_name})
  target_compile_definitions(${target_name} PRIVATE SHARED_CXX)
  target_include_directories(${target_name}
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries(${target_name}
                        PRIVATE tf_gateway_plugin_support TurboFlow::Gateway)
endfunction()

turbo_flow_add_gateway(
  tf_gateway_mqtt_sn GatewayMqttSn mqtt_sn/flow_gateway_mqtt_sn.c)
turbo_flow_add_gateway(
  tf_gateway_coap GatewayCoap coap/flow_gateway_coap.c)
turbo_flow_add_gateway(
  tf_gateway_lwm2m GatewayLwm2m lwm2m/flow_gateway_lwm2m.c)
turbo_flow_add_gateway(
  tf_gateway_ocpp GatewayOcpp ocpp/flow_gateway_ocpp.c)
target_link_libraries(tf_gateway_ocpp PRIVATE TurboUtils::Parser)
if(NOT WIN32)
  target_link_libraries(tf_gateway_ocpp PRIVATE m)
endif()
turbo_flow_add_gateway(
  tf_gateway_gbt32960 GatewayGbt32960 gbt32960/flow_gateway_gbt32960.c)
turbo_flow_add_gateway(
  tf_gateway_jtt808 GatewayJtt808 jtt808/flow_gateway_jtt808.c)

if(TURBO_FLOW_BUILD_GATEWAY_BUSINESS)
  add_subdirectory(business)
endif()

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
