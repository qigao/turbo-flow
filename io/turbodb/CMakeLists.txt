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

install(FILES include/turbo_flow_turbodb.h DESTINATION include)

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
