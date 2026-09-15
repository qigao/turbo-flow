cmake_minimum_required(VERSION 3.25)
project(TurboFlowProtocolNetworkCompileContract LANGUAGES C CXX)

if(NOT DEFINED TURBO_FLOW_SOURCE_DIR OR NOT IS_DIRECTORY "${TURBO_FLOW_SOURCE_DIR}")
  message(FATAL_ERROR "TURBO_FLOW_SOURCE_DIR must name the checked-out TurboFlow source tree")
endif()
if(NOT DEFINED SALTS_SOURCE_DIR OR NOT IS_DIRECTORY "${SALTS_SOURCE_DIR}")
  message(FATAL_ERROR "SALTS_SOURCE_DIR must name the pinned Salts source tree")
endif()
if(NOT DEFINED ENV{SALTS_ROOT} OR NOT IS_DIRECTORY "$ENV{SALTS_ROOT}")
  message(FATAL_ERROR "SALTS_ROOT must name the installed pinned Salts SDK")
endif()
if(NOT DEFINED ENV{SALTS_UTILS_ROOT} OR NOT IS_DIRECTORY "$ENV{SALTS_UTILS_ROOT}")
  message(FATAL_ERROR "SALTS_UTILS_ROOT must name the installed pinned SaltsUtils SDK")
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Salts CONFIG REQUIRED PATHS "$ENV{SALTS_ROOT}" NO_DEFAULT_PATH)
find_package(xxHash CONFIG REQUIRED)

set(_tf_include_dirs
    "${TURBO_FLOW_SOURCE_DIR}/turbo_flow/include"
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/include"
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/src"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/common/include"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/inbox/include"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/network/include"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/network/src"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests"
    "$ENV{SALTS_UTILS_ROOT}/include"
    "${SALTS_SOURCE_DIR}/tinytest/include")

set(_tf_c_options
    -Wall -Wextra -Wpedantic
    -Werror=implicit-function-declaration
    -Werror=incompatible-pointer-types
    -fsanitize=address
    -fsanitize=undefined)
set(_tf_cxx_options
    -Wall -Wextra -Wpedantic
    -fsanitize=address
    -fsanitize=undefined)

add_library(protocol_network_production OBJECT
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/src/turbo_flow_cnet_listener_source.c"
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/src/turbo_flow_cnet_plugin.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/network/src/flow_protocol_network_intake_config.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/network/src/flow_protocol_network_intake_sink.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/network/src/flow_protocol_network_intake.c")
target_include_directories(protocol_network_production PRIVATE ${_tf_include_dirs})
target_compile_definitions(protocol_network_production PRIVATE TURBO_FLOW_BUILD TURBO_FLOW_PLUGIN_BUILD)
target_compile_options(protocol_network_production PRIVATE $<$<COMPILE_LANGUAGE:C>:${_tf_c_options}>)
target_link_libraries(protocol_network_production PRIVATE Salts::Core Salts::CFlow Salts::CNet xxHash::xxhash)

add_library(protocol_network_tests OBJECT
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/tests/test_cnet_listener_message_context.c"
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/tests/test_cnet_plugin_source_snapshot.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/protocol_network_source_fixture.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_intake_config.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_intake_core.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_intake_edges.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_intake.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/protocol_network_e2e_fixture.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_jtt808.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/protocol_network_coap_fixture.c"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/test_protocol_network_coap.c")
target_include_directories(protocol_network_tests PRIVATE ${_tf_include_dirs})
target_compile_definitions(protocol_network_tests PRIVATE
    TURBO_FLOW_BUILD TURBO_FLOW_PLUGIN_BUILD
    TURBO_FLOW_CNET_PLUGIN_PATH="unused-cnet-plugin"
    FLOW_PROTOCOL_NETWORK_SOURCE_FIXTURE="unused-source-fixture"
    FLOW_CNET_PLUGIN_MODULE="unused-cnet-plugin"
    FLOW_PROTOCOL_JTT808_MODULE="unused-jtt808-plugin"
    FLOW_PROTOCOL_COAP_MODULE="unused-coap-plugin")
target_compile_options(protocol_network_tests PRIVATE $<$<COMPILE_LANGUAGE:C>:${_tf_c_options}>)
target_link_libraries(protocol_network_tests PRIVATE Salts::Core Salts::CFlow Salts::CNet xxHash::xxhash)

add_library(graph_durable_buffer_contract OBJECT
    "${TURBO_FLOW_SOURCE_DIR}/turbo_flow/tests/flow_plan_type_probe.c"
    "${TURBO_FLOW_SOURCE_DIR}/turbo_flow/tests/test_flow_durable_buffer.c"
    "${TURBO_FLOW_SOURCE_DIR}/turbo_flow/tests/test_flow_durable_identity.c"
    "${TURBO_FLOW_SOURCE_DIR}/turbo_flow/tests/durable_identity_header_cpp.cpp")
target_include_directories(graph_durable_buffer_contract PRIVATE ${_tf_include_dirs})
target_compile_definitions(graph_durable_buffer_contract PRIVATE TURBO_FLOW_BUILD)
target_compile_options(graph_durable_buffer_contract PRIVATE
    $<$<COMPILE_LANGUAGE:C>:${_tf_c_options}>
    $<$<COMPILE_LANGUAGE:CXX>:${_tf_cxx_options}>)
target_link_libraries(graph_durable_buffer_contract PRIVATE Salts::Core Salts::CFlow)

add_library(protocol_network_cpp_contract OBJECT
    "${TURBO_FLOW_SOURCE_DIR}/io/cnet/tests/cnet_stream_source_header_cpp.cpp"
    "${TURBO_FLOW_SOURCE_DIR}/ingress/protocol/tests/protocol_network_intake_header_cpp.cpp")
target_include_directories(protocol_network_cpp_contract PRIVATE ${_tf_include_dirs})
target_compile_options(protocol_network_cpp_contract PRIVATE $<$<COMPILE_LANGUAGE:CXX>:${_tf_cxx_options}>)
target_link_libraries(protocol_network_cpp_contract PRIVATE Salts::Core Salts::CFlow Salts::CNet)
