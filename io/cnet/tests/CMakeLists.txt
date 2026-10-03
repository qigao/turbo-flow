cmake_add_test(
  test_cnet_typed_contract
  SOURCES test_cnet_typed_contract.c
  LIBS
    TurboFlow::Product
    Salts::TinyTest
    ${turbo_flow_cnet_provider_config_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${turbo_flow_cnet_provider_config_GENERATED_DIR}
  FOLDER "io/cnet/tests")
add_dependencies(
  test_cnet_typed_contract
  turbo_flow_cnet_provider_config_idl_codegen)
set_tests_properties(test_cnet_typed_contract PROPERTIES
  LABELS "cnet-plugin;provider;config;resource;databind")

cmake_add_test(
  test_cnet_stream_source
  SOURCES test_cnet_stream_source.c cnet_stream_source_header_cpp.cpp
          stream_source_pipe_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_stream_source
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_stream_sink
  SOURCES test_cnet_stream_sink.c cnet_stream_source_header_cpp.cpp
          stream_source_pipe_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_stream_sink
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_datagram_sink
  SOURCES test_cnet_datagram_sink.c cnet_stream_source_header_cpp.cpp
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_datagram_sink
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_listener_source
  SOURCES test_cnet_listener_source.c cnet_stream_source_header_cpp.cpp
          listener_source_tls_fixture.h
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_listener_source
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_listener_message_context
  SOURCES test_cnet_listener_message_context.c cnet_stream_source_header_cpp.cpp
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_listener_message_context
                     PROPERTIES LABELS "cnet-adapter;graph")

cmake_add_test(
  test_cnet_packet_source
  SOURCES test_cnet_packet_source.c cnet_stream_source_header_cpp.cpp
  LIBS TurboFlow::CNetAdapter Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_packet_source
                     PROPERTIES LABELS "cnet-adapter;graph")

add_library(tf_cnet_packet_sink_test_support STATIC
            ../src/turbo_flow_cnet_packet_sink.c
            ../src/turbo_flow_cnet_test_internal.h)
target_compile_definitions(tf_cnet_packet_sink_test_support
                           PRIVATE TURBO_FLOW_CNET_INTERNAL_TESTING)
target_include_directories(
  tf_cnet_packet_sink_test_support
  PRIVATE $<TARGET_PROPERTY:xxHash::xxhash,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_link_libraries(
  tf_cnet_packet_sink_test_support
  PUBLIC TurboFlow::Graph Salts::CNet Salts::CFlow Salts::CSTL)
cmake_config_target(tf_cnet_packet_sink_test_support FOLDER "io/cnet/tests")

cmake_add_test(
  test_cnet_packet_sink
  SOURCES test_cnet_packet_sink.c cnet_stream_source_header_cpp.cpp
  LIBS tf_cnet_packet_sink_test_support Salts::TinyTest
  DEFS TURBO_FLOW_CNET_INTERNAL_TESTING
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../include
           ${CMAKE_CURRENT_SOURCE_DIR}/../src
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_packet_sink
                     PROPERTIES LABELS "cnet-adapter;graph")

add_library(test_cnet_deployment_resource_fixture SHARED
  cnet_deployment_resource_fixture.c)
target_include_directories(test_cnet_deployment_resource_fixture PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_cnet_deployment_resource_fixture PRIVATE
  Salts::Plugin)
set_target_properties(test_cnet_deployment_resource_fixture PROPERTIES
  FOLDER "io/cnet/tests/fixtures"
  C_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES)

cmake_add_test(
  test_cnet_provider_interface
  SOURCES test_cnet_provider_interface.c
  LIBS
    TurboFlow::Product
    TurboFlow::CNetAdapter
    Salts::Plugin
    Salts::TinyTest
    ${turbo_flow_cnet_provider_config_NATIVE_TARGET}
  INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${turbo_flow_cnet_provider_config_GENERATED_DIR}
  DEFS
    FLOW_CNET_PROVIDER_FIXTURE="$<TARGET_FILE:tf_cnet_plugin>"
    FLOW_CNET_RESOURCE_FIXTURE="$<TARGET_FILE:test_cnet_deployment_resource_fixture>"
  FOLDER "io/cnet/tests")
add_dependencies(test_cnet_provider_interface
  tf_cnet_plugin
  test_cnet_deployment_resource_fixture
  turbo_flow_cnet_provider_config_idl_codegen)
set_tests_properties(test_cnet_provider_interface PROPERTIES
  LABELS "cnet-plugin;provider;resource;lease;graph")

cmake_add_test(
  test_cnet_adapter_benchmark_stats
  SOURCES test_cnet_adapter_benchmark_stats.c
          ../benchmarks/cnet_adapter_benchmark_stats.c
  LIBS Salts::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks
  FOLDER "io/cnet/tests")

set_tests_properties(test_cnet_adapter_benchmark_stats
                     PROPERTIES LABELS "benchmark;cnet-adapter")
