set(TARGET_NAME turbo_flow_storage)

file(GLOB TURBO_FLOW_STORAGE_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_STORAGE_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_storage.h)

add_library(${TARGET_NAME} SHARED
  ${TURBO_FLOW_STORAGE_SOURCES}
  ${TURBO_FLOW_STORAGE_HEADERS})

find_package(SQLite3 REQUIRED)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::FlowStorage
  FOLDER "turbo_flow_storage"
  EXPORT_NAME FlowStorage)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)

target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)

target_link_libraries(${TARGET_NAME} PUBLIC TurboFlow::Flow TurboUtils::Core
                                      PRIVATE SQLite::SQLite3)

install(FILES ${TURBO_FLOW_STORAGE_HEADERS} DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
