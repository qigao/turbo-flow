set(TARGET_NAME tf_executor_common)

add_library(tf_coronet_runtime STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_coronet_runtime.c
            ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_coronet_runtime.h)
cmake_config_target(tf_coronet_runtime FOLDER "io/common")
target_include_directories(tf_coronet_runtime
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(tf_coronet_runtime
                      PUBLIC TurboNet::CoroNet TurboUtils::Core)

set(TURBO_FLOW_EXECUTOR_COMMON_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_connection.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_io_policy.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_coronet_actor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_coronet_execution.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_event_time_watermark.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_resource_status.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_timer.c)
set(TURBO_FLOW_EXECUTOR_COMMON_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_connection.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_io_policy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_coronet_actor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_coronet_execution.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_event_time_watermark.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_resource_status.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/flow_timer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_coronet_execution.h)

add_library(${TARGET_NAME} STATIC ${TURBO_FLOW_EXECUTOR_COMMON_SOURCES}
                                  ${TURBO_FLOW_EXECUTOR_COMMON_HEADERS})

cmake_config_target(${TARGET_NAME} FOLDER "io/common")

target_include_directories(${TARGET_NAME}
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
                           PRIVATE ${PROJECT_SOURCE_DIR}/turbo_flow/include)
target_link_libraries(${TARGET_NAME}
                      PUBLIC tf_coronet_runtime TurboFlow::Flow TurboNet::CoroNet
                             TurboUtils::Core)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_coronet_execution.h
        DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

