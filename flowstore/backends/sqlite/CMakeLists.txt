set(TARGET_NAME tf_sqlite_storage)

find_package(SQLite3 REQUIRED)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_sqlite_config.c
  src/flow_sqlite_record_store.c
  src/flow_sqlite_storage_backend.c
  src/flow_sqlite_storage_internal.h
  include/turbo_flow_sqlite_storage_backend.h)
cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::SQLiteStorage
  FOLDER "flowstore/backends/sqlite"
  EXPORT_NAME SQLiteStorage)
target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC TurboFlow::FlowStore TurboFlow::StorageBackend
  PRIVATE SQLite::SQLite3 TurboUtils::Core TurboParser::Parser)

install(FILES include/turbo_flow_sqlite_storage_backend.h DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
