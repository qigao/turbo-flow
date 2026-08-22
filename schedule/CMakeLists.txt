set(TARGET_NAME tf_schedule)
set(TURBO_FLOW_SCHEDULE_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/flow_schedule.c)
set(TURBO_FLOW_SCHEDULE_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_schedule.h)

add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_SCHEDULE_SOURCES}
                                  ${TURBO_FLOW_SCHEDULE_HEADERS})
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::Schedule
  FOLDER "schedule"
  EXPORT_NAME Schedule)
target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME} PUBLIC TurboFlow::Flow TurboUtils::Core TurboParser::Cron)
install(FILES ${TURBO_FLOW_SCHEDULE_HEADERS} DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
