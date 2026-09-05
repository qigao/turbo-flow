cmake_minimum_required(VERSION 3.20)

project(TurboFlowInstallConsumer LANGUAGES C)

find_package(TurboFlow 1.0 CONFIG REQUIRED)

add_executable(turbo_flow_install_consumer main.c)
target_link_libraries(turbo_flow_install_consumer PRIVATE TurboFlow::Flow)
