set(TARGET_NAME flowie_client)

set(FLOWIE_CLIENT_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowie_mqtt_client.h)

add_library(
  ${TARGET_NAME} SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_mqtt_client.c
  ${FLOWIE_CLIENT_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS Flowie::Client
  FOLDER "flowie/client"
  EXPORT_NAME FlowieClient)

target_compile_definitions(${TARGET_NAME} PRIVATE FLOWIE_MQTT_CLIENT_BUILD)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowie/protocol/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PRIVATE Flowie::Protocol TurboNet::CoroNet TurboUtils::Core monocypher)

install(FILES ${FLOWIE_CLIENT_HEADERS} DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
