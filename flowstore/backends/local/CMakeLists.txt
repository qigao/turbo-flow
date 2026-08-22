set(TARGET_NAME tf_local_storage)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_local_storage_backend.c
  include/turbo_flow_local_storage_backend.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::LocalStorage
  FOLDER "flowstore/backends/local"
  EXPORT_NAME LocalStorage)

target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME}
                      PUBLIC TurboFlow::FlowStore TurboFlow::StorageBackend
                      PRIVATE TurboUtils::Core)

install(FILES include/turbo_flow_local_storage_backend.h DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
