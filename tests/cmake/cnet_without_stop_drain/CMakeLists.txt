cmake_minimum_required(VERSION 3.20)
project(TurboFlowCNetStopDrainFailFast LANGUAGES C)

if(NOT DEFINED TURBO_FLOW_SOURCE_DIR OR "${TURBO_FLOW_SOURCE_DIR}" STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_SOURCE_DIR is required")
endif()

add_library(Salts::CNet INTERFACE IMPORTED)
set_target_properties(
  Salts::CNet PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/include")

list(APPEND CMAKE_MODULE_PATH "${TURBO_FLOW_SOURCE_DIR}/cmake")
include(TurboFlowRequireCNet)
turbo_flow_require_cnet_stop_drain_contract()
