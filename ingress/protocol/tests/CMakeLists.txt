function(turbo_flow_add_protocol_catalog_fixture target_name)
  add_library(${target_name} SHARED plugin_catalog_fixture.c)
  target_link_libraries(${target_name} PRIVATE TurboFlow::ProtocolIngress)
  target_compile_definitions(${target_name} PRIVATE TURBO_FLOW_PLUGIN_BUILD)
  set_target_properties(
    ${target_name}
    PROPERTIES C_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN YES
               FOLDER "ingress/protocol/tests/plugin-fixtures")
  if(ARGN)
    target_compile_definitions(${target_name} PRIVATE ${ARGN})
  endif()
endfunction()

turbo_flow_add_protocol_catalog_fixture(test_protocol_catalog_fixture_one)
turbo_flow_add_protocol_catalog_fixture(
  test_protocol_catalog_fixture_two
  FLOW_PROTOCOL_CATALOG_FIXTURE_ID="catalog.fixture.two"
  FLOW_PROTOCOL_CATALOG_FIXTURE_PROTOCOL="catalog-protocol-two"
  FLOW_PROTOCOL_CATALOG_FIXTURE_BUSINESS="catalog-business-two")
turbo_flow_add_protocol_catalog_fixture(
  test_protocol_catalog_fixture_duplicate
  FLOW_PROTOCOL_CATALOG_FIXTURE_ID="catalog.fixture.duplicate")
turbo_flow_add_protocol_catalog_fixture(
  test_protocol_catalog_fixture_mismatch
  FLOW_PROTOCOL_CATALOG_FIXTURE_ID="catalog.fixture.mismatch"
  FLOW_PROTOCOL_CATALOG_FIXTURE_PROTOCOL="catalog-protocol-mismatch"
  FLOW_PROTOCOL_CATALOG_FIXTURE_BUSINESS="catalog-business-mismatch"
  FLOW_PROTOCOL_CATALOG_FIXTURE_MODE=1)

cmake_add_test(
  test_protocol_registry
  SOURCES test_protocol_registry.c
  LIBS TurboFlow::PluginHost Salts::TinyTest
  DEFS FLOW_PROTOCOL_COAP_MODULE="$<TARGET_FILE:tf_protocol_coap>"
       FLOW_PROTOCOL_CATALOG_FIXTURE_ONE="$<TARGET_FILE:test_protocol_catalog_fixture_one>"
       FLOW_PROTOCOL_CATALOG_FIXTURE_TWO="$<TARGET_FILE:test_protocol_catalog_fixture_two>"
       FLOW_PROTOCOL_CATALOG_FIXTURE_DUPLICATE="$<TARGET_FILE:test_protocol_catalog_fixture_duplicate>"
       FLOW_PROTOCOL_CATALOG_FIXTURE_MISMATCH="$<TARGET_FILE:test_protocol_catalog_fixture_mismatch>"
  FOLDER "ingress/protocol/tests")
add_dependencies(
  test_protocol_registry
  tf_protocol_coap
  test_protocol_catalog_fixture_one
  test_protocol_catalog_fixture_two
  test_protocol_catalog_fixture_duplicate
  test_protocol_catalog_fixture_mismatch)

cmake_add_test(
  test_protocol_plugins
  SOURCES test_protocol_plugins.c
  LIBS TurboFlow::PluginHost Salts::TinyTest
  DEFS
    FLOW_PROTOCOL_MQTT_SN_MODULE="$<TARGET_FILE:tf_protocol_mqtt_sn>"
    FLOW_PROTOCOL_COAP_MODULE="$<TARGET_FILE:tf_protocol_coap>"
    FLOW_PROTOCOL_LWM2M_MODULE="$<TARGET_FILE:tf_protocol_lwm2m>"
    FLOW_PROTOCOL_OCPP_MODULE="$<TARGET_FILE:tf_protocol_ocpp>"
    FLOW_PROTOCOL_GBT32960_MODULE="$<TARGET_FILE:tf_protocol_gbt32960>"
    FLOW_PROTOCOL_JTT808_MODULE="$<TARGET_FILE:tf_protocol_jtt808>"
  FOLDER "ingress/protocol/tests")
add_dependencies(
  test_protocol_plugins
  tf_protocol_mqtt_sn
  tf_protocol_coap
  tf_protocol_lwm2m
  tf_protocol_ocpp
  tf_protocol_gbt32960
  tf_protocol_jtt808)
set_tests_properties(test_protocol_registry test_protocol_plugins
                     PROPERTIES LABELS "protocol-ingress")

add_executable(test_protocol_plugin_abi ../../../tests/plugin_host_abi_test.c)
target_link_libraries(test_protocol_plugin_abi
  PRIVATE TurboFlow::PluginHost Salts::TinyTest tf_protocol_coap)
add_test(NAME test_protocol_plugin_abi COMMAND test_protocol_plugin_abi)
set_target_properties(test_protocol_plugin_abi PROPERTIES FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_plugin_abi PROPERTIES LABELS "protocol-ingress;plugin")

cmake_add_test(
  test_protocol_source
  SOURCES test_protocol_source.c
  LIBS TurboFlow::ProtocolIngress Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_source
                     PROPERTIES LABELS "protocol-ingress;protocol-source")

cmake_add_test(
  test_protocol_inbox
  SOURCES test_protocol_inbox.c
  LIBS TurboFlow::ProtocolIngressInbox tf_protocol_inbox_schema Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_inbox
                     PROPERTIES LABELS "protocol-ingress;inbox;databind")

cmake_add_test(
  test_protocol_network_intake_config
  SOURCES test_protocol_network_intake_config.c
  LIBS tf_protocol_network_intake_core Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_network_intake_config
                     PROPERTIES LABELS "protocol-ingress;network-intake;config")

cmake_add_test(
  test_protocol_network_intake_core
  SOURCES test_protocol_network_intake_core.c
  LIBS tf_protocol_network_intake_core Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_network_intake_core
                     PROPERTIES LABELS "protocol-ingress;network-intake;inbox")

cmake_add_test(
  test_protocol_business
  SOURCES test_protocol_business.c
  LIBS TurboFlow::PluginHost Salts::TinyTest
  DEFS FLOW_PROTOCOL_BUSINESS_MODULE="$<TARGET_FILE:tf_protocol_business_ocpp201_core>"
  FOLDER "ingress/protocol/tests")
add_dependencies(test_protocol_business tf_protocol_business_ocpp201_core)
set_tests_properties(test_protocol_business
                     PROPERTIES LABELS "protocol-ingress;protocol-business")

if(TARGET tf_protocol_business_ocpp201_core)
  cmake_add_test(
    test_protocol_business_ocpp201_core
    SOURCES test_protocol_business_ocpp201_core.c
    LIBS TurboFlow::PluginHost Salts::TinyTest
    DEFS
      FLOW_PROTOCOL_BUSINESS_OCPP201_CORE_MODULE="$<TARGET_FILE:tf_protocol_business_ocpp201_core>"
      FLOW_PROTOCOL_OCPP_MODULE="$<TARGET_FILE:tf_protocol_ocpp>"
    FOLDER "ingress/protocol/tests")
  add_dependencies(
    test_protocol_business_ocpp201_core
    tf_protocol_business_ocpp201_core
    tf_protocol_ocpp)
  set_tests_properties(
    test_protocol_business_ocpp201_core
    PROPERTIES LABELS "protocol-ingress;protocol-business;databind")

  cmake_add_test(
    test_protocol_business_ocpp201_core_schema
    SOURCES test_protocol_business_ocpp201_core_schema.cpp
    LIBS tf_protocol_business_ocpp201_core_schema_shared
         Salts::TinyTest
    FOLDER "ingress/protocol/tests")
  target_compile_definitions(
    test_protocol_business_ocpp201_core_schema
    PRIVATE TBE_GENERATED_USE_SHARED)
  set_target_properties(
    test_protocol_business_ocpp201_core_schema
    PROPERTIES CXX_STANDARD 17
               CXX_STANDARD_REQUIRED ON)
  set_tests_properties(
    test_protocol_business_ocpp201_core_schema
    PROPERTIES LABELS "protocol-ingress;protocol-business;databind")
endif()
