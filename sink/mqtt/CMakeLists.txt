set(TARGET_NAME tf_mqtt_sink)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_mqtt_sink.c
  include/turbo_flow_mqtt_sink.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::MqttSink
  FOLDER "sink/mqtt"
  EXPORT_NAME MqttSink)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME} PUBLIC TurboFlow::ProtocolIngress)

install(FILES include/turbo_flow_mqtt_sink.h DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
