cmake_minimum_required(VERSION 3.20)

project(TurboFlowInstallConsumer LANGUAGES C)

if(TURBO_FLOW_TEST_ALL_COMPONENTS)
  find_package(
    TurboFlow 1.0 CONFIG REQUIRED
    COMPONENTS
      Config
      Graph
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
else()
  find_package(TurboFlow 1.0 CONFIG REQUIRED)
endif()

add_executable(turbo_flow_install_consumer main.c)
target_link_libraries(
  turbo_flow_install_consumer
  PRIVATE TurboFlow::Config
          TurboFlow::Graph
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
