cmake_minimum_required(VERSION 3.20)
project(TurboFlowCNetPluginConsumer LANGUAGES C)

find_package(TurboFlow 1.0 CONFIG REQUIRED COMPONENTS PluginHost)

add_executable(turbo_flow_cnet_plugin_consumer main.c)
target_link_libraries(turbo_flow_cnet_plugin_consumer PRIVATE TurboFlow::PluginHost)
if(WIN32)
  target_link_libraries(turbo_flow_cnet_plugin_consumer PRIVATE ws2_32)
endif()

enable_testing()
if(NOT DEFINED TURBO_FLOW_CNET_PLUGIN_PATH OR TURBO_FLOW_CNET_PLUGIN_PATH STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_CNET_PLUGIN_PATH is required")
endif()
add_test(NAME turbo_flow_cnet_plugin_consumer_runs
         COMMAND turbo_flow_cnet_plugin_consumer "${TURBO_FLOW_CNET_PLUGIN_PATH}")
