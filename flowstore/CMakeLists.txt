set(TARGET_NAME tf_flow_store)

add_subdirectory(backend)

add_library(
  ${TARGET_NAME} SHARED
  src/flow_store_common.c
  src/flow_record_store.c
  src/flow_store_policy.c
  src/flow_bitmap_index.c
  src/flow_state_store_provider.c
  src/flow_state_store_memory.c
  src/flow_index_store_provider.c
  src/flow_index_store_memory.c
  src/flow_log_store_provider.c
  src/flow_log_store_memory.c
  src/flow_series_store_provider.c
  src/flow_series_store_memory.c)

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::FlowStore
  FOLDER "flowstore"
  EXPORT_NAME FlowStore)

target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(${TARGET_NAME}
                      PUBLIC TurboFlow::Flow TurboFlow::StorageBackend TurboUtils::Core
                      PRIVATE roaring::roaring)

install(
  FILES include/turbo_flow_store.h
        include/turbo_flow_bitmap_index.h
        include/turbo_flow_store_policy.h
        include/turbo_flow_state_store.h
        include/turbo_flow_state_store_provider.h
        include/turbo_flow_index_store.h
        include/turbo_flow_index_store_provider.h
        include/turbo_flow_log_store.h
        include/turbo_flow_log_store_provider.h
        include/turbo_flow_series_store.h
        include/turbo_flow_series_store_provider.h
  DESTINATION include)

add_subdirectory(databind)
add_subdirectory(backends)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
