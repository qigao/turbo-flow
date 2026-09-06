cmake_minimum_required(VERSION 3.20)

project(TurboFlowInstallConsumer LANGUAGES C)

find_package(TurboFlow 1.0 CONFIG REQUIRED)

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
