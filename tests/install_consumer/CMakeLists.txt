cmake_minimum_required(VERSION 3.20)

if(TURBO_FLOW_CONSUMER_CXX_ONLY)
  project(TurboFlowInstallConsumer LANGUAGES CXX)
  set_source_files_properties(main.c PROPERTIES LANGUAGE CXX)
else()
  project(TurboFlowInstallConsumer LANGUAGES C)
endif()

if(NOT DEFINED ENV{TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER} OR
   NOT "$ENV{TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER}" MATCHES "^(ON|OFF)$")
  message(FATAL_ERROR
    "TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER environment metadata must be ON or OFF")
endif()
if(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
  set(turbo_flow_test_has_turbodb_adapter_normalized ON)
else()
  set(turbo_flow_test_has_turbodb_adapter_normalized OFF)
endif()
if(NOT turbo_flow_test_has_turbodb_adapter_normalized STREQUAL
       "$ENV{TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER}")
  message(FATAL_ERROR
    "TurboDb consumer capability ${turbo_flow_test_has_turbodb_adapter_normalized} does not match parent metadata $ENV{TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER}")
endif()

if(NOT DEFINED ENV{TURBO_FLOW_ROOT} OR "$ENV{TURBO_FLOW_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBO_FLOW_ROOT}")
  message(FATAL_ERROR "TURBO_FLOW_ROOT must name the installed TurboFlow prefix")
endif()
file(REAL_PATH "$ENV{TURBO_FLOW_ROOT}" turbo_flow_root)
foreach(turbo_flow_removed_protocol_artifact IN ITEMS
        include/turbo_flow_protocol_runtime.h
        include/turbo_flow_protocol_graph.h
        bin/tf_protocol_graph.dll
        lib/tf_protocol_graph.lib
        lib/libtf_protocol_graph.so
        lib/libtf_protocol_graph.dylib
        include/turbo_flow_protocol_inbox.h
        bin/tf_protocol_inbox.dll
        lib/tf_protocol_inbox.lib
        lib/libtf_protocol_inbox.so
        lib/libtf_protocol_inbox.dylib)
  if(EXISTS "${turbo_flow_root}/${turbo_flow_removed_protocol_artifact}")
    message(FATAL_ERROR
      "Removed protocol artifact remains installed: ${turbo_flow_removed_protocol_artifact}")
  endif()
endforeach()
foreach(turbo_flow_protocol_schema_artifact IN ITEMS
        include/turbo_flow_protocol_envelope.h
        include/turbo_flow_protocol_inbox_envelope.h
        share/TurboFlow/protocol/inbox/protocol_inbox.schema
        share/TurboFlow/protocol/inbox/turbo_flow_protocol_inbox_envelope.rfl
        share/TurboFlow/protocol/inbox/turbo_flow_protocol_inbox_envelope.d.ts)
  if(NOT EXISTS "${turbo_flow_root}/${turbo_flow_protocol_schema_artifact}")
    message(FATAL_ERROR
      "Required protocol Inbox schema artifact is missing: ${turbo_flow_protocol_schema_artifact}")
  endif()
endforeach()
if(DEFINED TurboFlow_DIR AND NOT "${TurboFlow_DIR}" STREQUAL "")
  if(NOT IS_DIRECTORY "${TurboFlow_DIR}")
    message(FATAL_ERROR "TurboFlow_DIR is not a directory: ${TurboFlow_DIR}")
  endif()
  file(REAL_PATH "${TurboFlow_DIR}" turbo_flow_dir)
  cmake_path(IS_PREFIX turbo_flow_root "${turbo_flow_dir}" NORMALIZE
             turbo_flow_dir_is_within_root)
  if(NOT turbo_flow_dir_is_within_root)
    message(FATAL_ERROR "TurboFlow_DIR is outside TURBO_FLOW_ROOT: ${TurboFlow_DIR}")
  endif()
endif()

if(TURBO_FLOW_TEST_SCHEMA_ONLY)
  find_package(TurboFlow 2.0 CONFIG REQUIRED
               COMPONENTS ProtocolIngressInboxSchema
               PATHS "$ENV{TURBO_FLOW_ROOT}" NO_DEFAULT_PATH)
elseif(TURBO_FLOW_TEST_ALL_COMPONENTS)
  set(turbo_flow_test_components
      Config
      Graph
      PluginHost
      CNetAdapter
      CHTTPAdapter
      Product
      ProtocolIngress
      ProtocolNetworkIntake
      ProtocolIngressInboxSchema
      ProtocolMqttSn
      ProtocolCoap
      ProtocolLwm2m
      ProtocolOcpp
      ProtocolGbt32960
      ProtocolJtt808
      ProtocolBusinessOcpp201Core
      SecuritySQLite
      Codec
      Observe
      Schedule)
  if(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
    list(APPEND turbo_flow_test_components TurboDbAdapter)
  endif()
  find_package(TurboFlow 2.0 CONFIG REQUIRED COMPONENTS ${turbo_flow_test_components}
               PATHS "$ENV{TURBO_FLOW_ROOT}" NO_DEFAULT_PATH)
else()
  find_package(TurboFlow 2.0 CONFIG REQUIRED
               PATHS "$ENV{TURBO_FLOW_ROOT}" NO_DEFAULT_PATH)
endif()
if(TARGET TurboFlow::MqttSink)
  message(FATAL_ERROR
    "TurboFlow::MqttSink is a removed mapper component; MQTT must be loaded as a provider DLL")
endif()
if(TARGET TurboFlow::ProtocolIngressGraph)
  message(FATAL_ERROR
    "TurboFlow::ProtocolIngressGraph is removed; protocol Sources publish through the generic durable buffer")
endif()
if(TARGET TurboFlow::ProtocolIngressInbox)
  message(FATAL_ERROR
    "TurboFlow::ProtocolIngressInbox is removed; protocol-specific storage runtime must not be exported")
endif()
if(NOT TARGET TurboFlow::ProtocolIngressInboxSchema)
  message(FATAL_ERROR
    "TurboFlow::ProtocolIngressInboxSchema exact generated codec target is required")
endif()
if(NOT TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
  if(TARGET TurboFlow::TurboDbAdapter)
    message(FATAL_ERROR
      "TurboDbAdapter target must not be exported by a TurboDb-disabled package")
  endif()
  if(TARGET Orm::C)
    message(FATAL_ERROR
      "Orm::C must not be required by a TurboDb-disabled TurboFlow consumer")
  endif()
endif()

if(TURBO_FLOW_TEST_SCHEMA_ONLY)
  add_executable(turbo_flow_install_consumer schema_main.c)
  target_link_libraries(turbo_flow_install_consumer
                        PRIVATE TurboFlow::ProtocolIngressInboxSchema)
else()
  add_executable(turbo_flow_install_consumer main.c)
  target_link_libraries(
    turbo_flow_install_consumer
    PRIVATE TurboFlow::Config
            TurboFlow::Graph
            TurboFlow::PluginHost
            TurboFlow::CNetAdapter
            TurboFlow::CHTTPAdapter
            TurboFlow::Product
            TurboFlow::ProtocolIngress
            TurboFlow::ProtocolNetworkIntake
            TurboFlow::ProtocolIngressInboxSchema
            TurboFlow::ProtocolMqttSn
            TurboFlow::ProtocolCoap
            TurboFlow::ProtocolLwm2m
            TurboFlow::ProtocolOcpp
            TurboFlow::ProtocolGbt32960
            TurboFlow::ProtocolJtt808
            TurboFlow::ProtocolBusinessOcpp201Core
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
endif()

enable_testing()
if(TURBO_FLOW_TEST_ALL_COMPONENTS AND NOT TURBO_FLOW_CONSUMER_CXX_ONLY)
  add_library(turbo_flow_install_operation_fixture SHARED
              ../../turbo_flow/tests/operation_schema_fixture.c
              ../../turbo_flow/tests/plugin_operation_fixture.c)
  target_include_directories(turbo_flow_install_operation_fixture PRIVATE ../../turbo_flow/tests)
  target_compile_definitions(turbo_flow_install_operation_fixture
                             PRIVATE TURBO_FLOW_PLUGIN_BUILD)
  target_link_libraries(turbo_flow_install_operation_fixture
                        PRIVATE TurboFlow::PluginHost)
  install(TARGETS turbo_flow_install_operation_fixture
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib)
  if(NOT DEFINED TURBO_FLOW_INSTALL_FIXTURE_PATH OR
     TURBO_FLOW_INSTALL_FIXTURE_PATH STREQUAL "")
    message(FATAL_ERROR "TURBO_FLOW_INSTALL_FIXTURE_PATH is required")
  endif()
  add_test(NAME turbo_flow_install_consumer_runs
           COMMAND turbo_flow_install_consumer
                   "${TURBO_FLOW_INSTALL_FIXTURE_PATH}")
else()
  add_test(NAME turbo_flow_install_consumer_runs COMMAND turbo_flow_install_consumer)
endif()
