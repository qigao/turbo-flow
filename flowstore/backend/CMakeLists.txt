set(TARGET_NAME tf_storage_backend)

add_library(
  ${TARGET_NAME} STATIC
  src/flow_storage_backend.c
  include/turbo_flow_storage_backend.h)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::StorageBackend
  FOLDER "flowstore/backend"
  EXPORT_NAME StorageBackend)

target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/turbo_flow/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME} PUBLIC TurboUtils::Core PRIVATE ${CMAKE_DL_LIBS})

install(FILES include/turbo_flow_storage_backend.h DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
