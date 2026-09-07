cmake_minimum_required(VERSION 3.20)

if(TURBO_FLOW_CONSUMER_CXX_ONLY)
  project(TurboFlowInstallConsumer LANGUAGES CXX)
  set_source_files_properties(main.c PROPERTIES LANGUAGE CXX)
else()
  project(TurboFlowInstallConsumer LANGUAGES C)
endif()

if(TURBO_FLOW_TEST_ALL_COMPONENTS)
  set(turbo_flow_test_components
      Config
      Graph
      CNetAdapter
      CHTTPAdapter
      Product
      Flow
      ProtocolIngress
      ProtocolIngressGraph
      ProtocolMqttSn
      ProtocolCoap
      ProtocolLwm2m
      ProtocolOcpp
      ProtocolGbt32960
      ProtocolJtt808
      ProtocolBusinessOcpp201Core
      MqttSink
      SecuritySQLite
      Codec
      Observe
      Schedule)
  if(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
    list(APPEND turbo_flow_test_components TurboDbAdapter)
  endif()
  find_package(TurboFlow 1.0 CONFIG REQUIRED
               COMPONENTS ${turbo_flow_test_components})
else()
  find_package(TurboFlow 1.0 CONFIG REQUIRED)
endif()

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
if(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
  target_link_libraries(turbo_flow_install_consumer
                        PRIVATE TurboFlow::TurboDbAdapter)
  target_compile_definitions(turbo_flow_install_consumer
                             PRIVATE TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER=1)
endif()

enable_testing()
add_test(NAME turbo_flow_install_consumer_runs COMMAND turbo_flow_install_consumer)
