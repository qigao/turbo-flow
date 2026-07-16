set(TARGET_NAME tf_fmq)

set(TURBO_FLOW_FMQ_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_fmq.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_broker.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_broker_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_broker_protocol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_control.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_control_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_control_protocol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_compatibility.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_deployment.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_management.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_management_protocol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_management_schema.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_pattern_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_pubsub.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_pubsub_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_retry.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_retry_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fmq_protocol.c)
set(TURBO_FLOW_FMQ_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_broker.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_broker_protocol.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_compatibility.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_control.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_deployment.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_management.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_management_protocol.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_pubsub.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_fmq_retry.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_FMQ_SOURCES}
                                  ${TURBO_FLOW_FMQ_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS
  TurboFlow::FMQ
  FOLDER
  "io/fmq"
  EXPORT_NAME
  FMQ)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/io/common/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  ${TARGET_NAME} PUBLIC TurboFlow::Flow TurboNet::CoroNet TurboUtils::Core
                 PRIVATE tf_executor_common TurboUtils::Parser)

install(FILES ${TURBO_FLOW_FMQ_HEADERS} DESTINATION include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/examples/fmq.yml
        DESTINATION share/turboflow/examples/fmq)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

