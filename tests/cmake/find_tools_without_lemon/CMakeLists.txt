cmake_minimum_required(VERSION 3.20)

project(TurboFlowFindToolsWithoutLemon LANGUAGES NONE)

if(NOT DEFINED TURBO_FLOW_SOURCE_DIR)
  message(FATAL_ERROR "TURBO_FLOW_SOURCE_DIR is required")
endif()

list(APPEND CMAKE_MODULE_PATH "${TURBO_FLOW_SOURCE_DIR}/cmake")
include(FindTools)
