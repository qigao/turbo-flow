cmake_minimum_required(VERSION 3.20)
project(TurboFlowCHTTPPluginConsumer LANGUAGES C)

find_package(TurboFlow 2.0 CONFIG REQUIRED COMPONENTS PluginHost Product Graph)
add_executable(turbo_flow_chttp_plugin_consumer main.c)
target_link_libraries(turbo_flow_chttp_plugin_consumer PRIVATE
  TurboFlow::PluginHost TurboFlow::Product TurboFlow::Graph)
if(NOT DEFINED TURBO_FLOW_CHTTP_PLUGIN_PATH OR TURBO_FLOW_CHTTP_PLUGIN_PATH STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_CHTTP_PLUGIN_PATH is required")
endif()
enable_testing()
add_test(NAME turbo_flow_chttp_plugin_consumer_runs
  COMMAND turbo_flow_chttp_plugin_consumer "${TURBO_FLOW_CHTTP_PLUGIN_PATH}")
