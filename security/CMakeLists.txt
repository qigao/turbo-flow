set(TARGET_NAME tf_security_sqlite)

find_package(SQLite3 REQUIRED)

add_library(${TARGET_NAME} SHARED
  src/flow_security_sqlite.c
  include/turbo_flow_security_sqlite.h)
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::SecuritySQLite
  FOLDER "security"
  EXPORT_NAME SecuritySQLite)
target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                        $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME}
  PUBLIC TurboFlow::Graph
  PRIVATE SQLite::SQLite3 Salts::Core Salts::JsonParser)
install(FILES include/turbo_flow_security_sqlite.h DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
