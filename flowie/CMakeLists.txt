add_subdirectory(protocol)
add_subdirectory(client)
if(TURBO_FLOW_BUILD_SECURITY_SQLITE)
  add_subdirectory(control)
endif()

set(TARGET_NAME flowie)
set(FLOWIE_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/flowie.h)

add_library(
  ${TARGET_NAME} SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_config.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_endpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_ingress.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_ingress_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_rule.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_rule_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_security_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_session.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_session_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_topic_index.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_topic_index_internal.h
  ${PROJECT_SOURCE_DIR}/io/common/src/flow_connection.c
  ${PROJECT_SOURCE_DIR}/io/common/src/flow_coronet_execution.c
  ${PROJECT_SOURCE_DIR}/io/common/src/flow_coronet_runtime.c
  ${PROJECT_SOURCE_DIR}/io/common/src/flow_io_policy.c
  ${FLOWIE_HEADERS})
cmake_config_target(
  ${TARGET_NAME}
  ALIAS Flowie::App
  FOLDER "flowie"
  EXPORT_NAME Flowie)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${PROJECT_SOURCE_DIR}/io/common/include)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC Flowie::Protocol TurboFlow::Flow TurboNet::CoroNet TurboUtils::Core)
target_link_libraries(${TARGET_NAME} PRIVATE roaring::roaring)
install(FILES ${FLOWIE_HEADERS} DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
endif()
