set(TARGET_NAME tf_pgsql)
file(GLOB TURBO_FLOW_PGSQL_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_PGSQL_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_pgsql.h)
add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_PGSQL_SOURCES} ${TURBO_FLOW_PGSQL_HEADERS})
cmake_config_target(${TARGET_NAME} ALIAS TurboFlow::PostgreSQL FOLDER "io/pgsql"
                    EXPORT_NAME PostgreSQL)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                                 $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME} PUBLIC TurboFlow::Flow
                                    PRIVATE tf_executor_common PostgreSQL::PostgreSQL TurboUtils::Core
                                            TurboUtils::Parser)
install(FILES ${TURBO_FLOW_PGSQL_HEADERS} DESTINATION include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/examples/pgsql.yml
        DESTINATION share/turboflow/examples/pgsql)
if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

