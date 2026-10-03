salts_idl_target(
  TARGET turbo_flow_durable_memory_provider_config
  IDL "${CMAKE_CURRENT_SOURCE_DIR}/src/turbo_flow_durable_memory_provider_config.schema"
  ARTIFACT_NAME "durable_memory_provider_config"
  ARTIFACTS MESSAGE)

add_library(tf_durable_memory_plugin SHARED
  src/turbo_flow_durable_memory_plugin.c
  src/turbo_flow_durable_memory_config.c
  src/turbo_flow_durable_memory_internal.h)
cmake_config_target(tf_durable_memory_plugin FOLDER "io/durable")
target_compile_definitions(
  tf_durable_memory_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_include_directories(
  tf_durable_memory_plugin
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${turbo_flow_durable_memory_provider_config_GENERATED_DIR}")
target_link_libraries(
  tf_durable_memory_plugin
  PRIVATE
    TurboFlow::Product
    Salts::Plugin
    ${turbo_flow_durable_memory_provider_config_NATIVE_TARGET})
add_dependencies(
  tf_durable_memory_plugin
  turbo_flow_durable_memory_provider_config_idl_codegen)
set_target_properties(
  tf_durable_memory_plugin PROPERTIES
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

install(FILES
  include/turbo_flow_durable_memory_resource.h
  DESTINATION include)
install(TARGETS tf_durable_memory_plugin
  RUNTIME DESTINATION bin COMPONENT DurableMemory
  LIBRARY DESTINATION lib COMPONENT DurableMemory)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
