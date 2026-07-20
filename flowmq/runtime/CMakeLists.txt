set(TARGET_NAME flowmq_runtime_core)

add_library(${TARGET_NAME} STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_pattern.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_pattern.h
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_reconnect.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_reconnect.h
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_security.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_security.h
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_subscription_set.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_subscription_set.h
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_peer_session.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_peer_session.h
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_stream_decoder.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_stream_decoder.h)

cmake_config_target(${TARGET_NAME} FOLDER "flowmq/runtime")
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/flowmq/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/io/common/include>)
target_link_libraries(${TARGET_NAME} PUBLIC FlowMQ::Protocol TurboFlow::Flow
                                             TurboNet::CoroNet TurboUtils::Core)

add_library(flowmq_coronet_transport STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_coronet_transport.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_coronet_transport.h)
cmake_config_target(flowmq_coronet_transport FOLDER "flowmq/runtime")
target_include_directories(
  flowmq_coronet_transport PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
target_link_libraries(flowmq_coronet_transport
                      PUBLIC flowmq_runtime_core tf_coronet_runtime
                             TurboNet::CoroNet TurboUtils::Core)

add_library(flowmq_connect_endpoint_runtime STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_connect_endpoint.c
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flowmq_connect_endpoint.h)
cmake_config_target(flowmq_connect_endpoint_runtime FOLDER "flowmq/runtime")
target_include_directories(
  flowmq_connect_endpoint_runtime PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
target_link_libraries(flowmq_connect_endpoint_runtime PUBLIC flowmq_coronet_transport)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
