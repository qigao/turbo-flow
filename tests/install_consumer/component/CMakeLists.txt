cmake_minimum_required(VERSION 3.20)

project(TurboFlowComponentConsumer LANGUAGES C)

if(NOT DEFINED TURBO_FLOW_TEST_COMPONENT OR
   "${TURBO_FLOW_TEST_COMPONENT}" STREQUAL "")
  message(FATAL_ERROR "TURBO_FLOW_TEST_COMPONENT is required")
endif()

if(NOT DEFINED TURBO_FLOW_TEST_PACKAGE_VERSION)
  set(TURBO_FLOW_TEST_PACKAGE_VERSION 2.0)
endif()

find_package(TurboFlow ${TURBO_FLOW_TEST_PACKAGE_VERSION} CONFIG REQUIRED
             COMPONENTS "${TURBO_FLOW_TEST_COMPONENT}")

add_executable(turbo_flow_component_consumer main.c)
target_link_libraries(
  turbo_flow_component_consumer
  PRIVATE "TurboFlow::${TURBO_FLOW_TEST_COMPONENT}")

if(TURBO_FLOW_TEST_COMPONENT STREQUAL "Config")
  target_compile_definitions(turbo_flow_component_consumer
                             PRIVATE TURBO_FLOW_TEST_CONFIG_COMPONENT=1)
endif()

enable_testing()
add_test(NAME turbo_flow_component_consumer_runs
         COMMAND turbo_flow_component_consumer)
