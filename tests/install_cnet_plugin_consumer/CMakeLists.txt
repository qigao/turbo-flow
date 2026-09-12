cmake_minimum_required(VERSION 3.20)
project(TurboFlowCNetPluginConsumer LANGUAGES C)

if(NOT DEFINED ENV{TURBO_FLOW_ROOT} OR "$ENV{TURBO_FLOW_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBO_FLOW_ROOT}")
  message(FATAL_ERROR "TURBO_FLOW_ROOT must name the installed TurboFlow prefix")
endif()
file(REAL_PATH "$ENV{TURBO_FLOW_ROOT}" turbo_flow_root)
if(DEFINED TurboFlow_DIR AND NOT "${TurboFlow_DIR}" STREQUAL "")
  if(NOT IS_DIRECTORY "${TurboFlow_DIR}")
    message(FATAL_ERROR "TurboFlow_DIR is not a directory: ${TurboFlow_DIR}")
  endif()
  file(REAL_PATH "${TurboFlow_DIR}" turbo_flow_dir)
  cmake_path(IS_PREFIX turbo_flow_root "${turbo_flow_dir}" NORMALIZE
             turbo_flow_dir_is_within_root)
  if(NOT turbo_flow_dir_is_within_root)
    message(FATAL_ERROR "TurboFlow_DIR is outside TURBO_FLOW_ROOT: ${TurboFlow_DIR}")
  endif()
endif()
find_package(TurboFlow 2.0 CONFIG REQUIRED COMPONENTS PluginHost
             PATHS "$ENV{TURBO_FLOW_ROOT}" NO_DEFAULT_PATH)

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
