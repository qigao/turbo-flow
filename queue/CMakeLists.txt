set(TARGET_NAME tf_queue)

find_package(SQLite3 REQUIRED)

add_library(${TARGET_NAME} SHARED
  src/flow_queue.c
  src/flow_queue_config.c
  src/flow_queue_internal.h
  include/turbo_flow_queue.h)
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::Queue
  FOLDER "queue"
  EXPORT_NAME Queue)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME}
  PUBLIC TurboFlow::Flow TurboUtils::Core
  PRIVATE SQLite::SQLite3 TurboUtils::Parser)
install(FILES include/turbo_flow_queue.h DESTINATION include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/examples/queue.yml
        DESTINATION share/turboflow/examples/queue)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
