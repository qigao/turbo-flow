set(TARGET_NAME flowie_protocol)

cmake_add_grammar(FLOWIE_MQTT
  LEXER_RE ${CMAKE_CURRENT_SOURCE_DIR}/parser/flowie_mqtt_lexer.re
  GRAMMAR_Y ${CMAKE_CURRENT_SOURCE_DIR}/parser/flowie_mqtt_parser.y
  FOLDER "flowie/protocol")

if(MSVC)
  set_source_files_properties(${FLOWIE_MQTT_GRAMMAR_GEN} PROPERTIES COMPILE_OPTIONS "/wd4189")
endif()

set(FLOWIE_PROTOCOL_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/include/flowie_mqtt_protocol.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/flowie_mqtt_types.h)

add_library(${TARGET_NAME} SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_mqtt_protocol.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_mqtt_packet_views.c
  ${FLOWIE_MQTT_LEXER_GEN}
  ${FLOWIE_MQTT_GRAMMAR_GEN}
  ${FLOWIE_PROTOCOL_HEADERS})

add_dependencies(${TARGET_NAME}
  ${FLOWIE_MQTT_LEXER_TARGET}
  ${FLOWIE_MQTT_GRAMMAR_TARGET})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS Flowie::Protocol
  FOLDER "flowie/protocol"
  EXPORT_NAME FlowieProtocol)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(${TARGET_NAME} PUBLIC TurboUtils::Core)

install(FILES ${FLOWIE_PROTOCOL_HEADERS} DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
