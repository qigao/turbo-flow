set(TARGET_NAME tf_redis)

file(GLOB TURBO_FLOW_REDIS_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_REDIS_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_redis.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_store_redis.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_redis_storage_backend.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_REDIS_SOURCES}
                                ${TURBO_FLOW_REDIS_HEADERS})
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::Redis
  FOLDER "io/redis"
  EXPORT_NAME Redis)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::Flow TurboFlow::FlowStore TurboFlow::StorageBackend
  PRIVATE tf_executor_common TurboNet::Redis TurboNet::CoroNet TurboUtils::Core
          TurboUtils::Parser)
install(FILES ${TURBO_FLOW_REDIS_HEADERS} DESTINATION include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/examples/redis.yml
        DESTINATION share/turboflow/examples/redis)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

