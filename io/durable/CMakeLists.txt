add_library(tf_durable_memory_plugin SHARED
  src/turbo_flow_durable_memory_plugin.c src/turbo_flow_durable_memory_config.c
  src/turbo_flow_durable_memory_internal.h)
cmake_config_target(tf_durable_memory_plugin FOLDER "io/durable")
target_compile_definitions(tf_durable_memory_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_link_libraries(tf_durable_memory_plugin PRIVATE TurboFlow::Product)
set_target_properties(tf_durable_memory_plugin PROPERTIES C_VISIBILITY_PRESET hidden
                                                        VISIBILITY_INLINES_HIDDEN YES)
install(TARGETS tf_durable_memory_plugin
  RUNTIME DESTINATION bin COMPONENT DurableMemory
  LIBRARY DESTINATION lib COMPONENT DurableMemory)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
