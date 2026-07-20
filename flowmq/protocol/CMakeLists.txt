set(TARGET_NAME flowmq_protocol)

set(FLOWMQ_PROTOCOL_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flowmq_protocol.h)

add_library(${TARGET_NAME} SHARED
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_protocol.c
            ${FLOWMQ_PROTOCOL_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS FlowMQ::Protocol
  FOLDER "flowmq/protocol"
  EXPORT_NAME Protocol)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME} PUBLIC TurboUtils::Core)

install(
  TARGETS ${TARGET_NAME}
  EXPORT FlowMQTargets
  RUNTIME DESTINATION bin COMPONENT FlowMQProtocol
  LIBRARY DESTINATION lib COMPONENT FlowMQProtocol
  ARCHIVE DESTINATION lib COMPONENT FlowMQProtocol
  INCLUDES DESTINATION include)
install(FILES ${FLOWMQ_PROTOCOL_HEADERS}
        DESTINATION include
        COMPONENT FlowMQProtocol)

include(CMakePackageConfigHelpers)
configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/FlowMQConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfig.cmake"
  INSTALL_DESTINATION "lib/cmake/FlowMQ")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY AnyNewerVersion)

export(
  EXPORT FlowMQTargets
  FILE "${CMAKE_CURRENT_BINARY_DIR}/FlowMQTargets.cmake"
  NAMESPACE FlowMQ::)
install(
  EXPORT FlowMQTargets
  FILE FlowMQTargets.cmake
  NAMESPACE FlowMQ::
  DESTINATION "lib/cmake/FlowMQ"
  COMPONENT FlowMQProtocol)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/FlowMQConfigVersion.cmake"
        DESTINATION "lib/cmake/FlowMQ"
        COMPONENT FlowMQProtocol)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
