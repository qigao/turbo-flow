cmake_minimum_required(VERSION 3.20)

if(TURBO_FLOW_CONSUMER_CXX_ONLY)
  project(TurboFlowInstallConsumer LANGUAGES CXX)
  set_source_files_properties(main.c PROPERTIES LANGUAGE CXX)
else()
  project(TurboFlowInstallConsumer LANGUAGES C)
endif()

foreach(required_dir IN ITEMS
        TurboFlow_DIR Salts_DIR SaltsUtils_DIR RulesForge_DIR)
  if(NOT DEFINED ${required_dir} OR "${${required_dir}}" STREQUAL "")
    message(FATAL_ERROR "Missing required package directory: ${required_dir}")
  endif()
  if(NOT IS_DIRECTORY "${${required_dir}}")
    message(FATAL_ERROR
            "${required_dir} is not a directory: ${${required_dir}}")
  endif()
endforeach()

find_package(TurboFlow 1.0 CONFIG REQUIRED
             PATHS "${TurboFlow_DIR}" NO_DEFAULT_PATH)

add_executable(turbo_flow_install_consumer main.c)
target_link_libraries(
  turbo_flow_install_consumer
  PRIVATE TurboFlow::Config
          TurboFlow::Graph
          TurboFlow::CNetAdapter
          TurboFlow::CHTTPAdapter
          TurboFlow::Product
          TurboFlow::Flow
          TurboFlow::ProtocolIngress
          TurboFlow::ProtocolIngressGraph
          TurboFlow::ProtocolMqttSn
          TurboFlow::ProtocolCoap
          TurboFlow::ProtocolLwm2m
          TurboFlow::ProtocolOcpp
          TurboFlow::ProtocolGbt32960
          TurboFlow::ProtocolJtt808
          TurboFlow::ProtocolBusinessOcpp201Core
          TurboFlow::MqttSink
          TurboFlow::SecuritySQLite
          TurboFlow::Codec
          TurboFlow::Observe
          TurboFlow::Schedule)

enable_testing()
add_test(NAME turbo_flow_install_consumer_runs COMMAND turbo_flow_install_consumer)
