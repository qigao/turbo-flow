add_library(tf_turbodb_adapter SHARED
  src/turbo_flow_turbodb.c
  src/turbo_flow_turbodb_inbox.c
  src/turbo_flow_turbodb_outbox.c
  include/turbo_flow_turbodb.h)
cmake_config_target(
  tf_turbodb_adapter
  ALIAS TurboFlow::TurboDbAdapter
  FOLDER "io/turbodb"
  EXPORT_NAME TurboDbAdapter)
target_compile_definitions(tf_turbodb_adapter PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_turbodb_adapter
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(
  tf_turbodb_adapter
  PUBLIC TurboFlow::Graph Orm::C Salts::CFlow
  PRIVATE Salts::CSTL)

add_library(tf_turbodb_plugin SHARED
  src/turbo_flow_turbodb_durable_plugin.c
  src/turbo_flow_turbodb_durable_config.c
  src/turbo_flow_turbodb_durable_internal.h)
cmake_config_target(tf_turbodb_plugin FOLDER "io/turbodb")
target_compile_definitions(tf_turbodb_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_link_libraries(tf_turbodb_plugin
  PRIVATE TurboFlow::Product TurboFlow::TurboDbAdapter)
set_target_properties(tf_turbodb_plugin PROPERTIES
  C_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)

install(FILES include/turbo_flow_turbodb.h DESTINATION include)
install(TARGETS tf_turbodb_plugin
  RUNTIME DESTINATION bin COMPONENT TurboDbDurable
  LIBRARY DESTINATION lib COMPONENT TurboDbDurable)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
