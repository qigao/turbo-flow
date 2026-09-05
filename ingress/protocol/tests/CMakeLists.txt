cmake_add_test(
  test_protocol_graph
  SOURCES test_protocol_graph.c
  LIBS TurboFlow::ProtocolIngressGraph Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_graph
                     PROPERTIES LABELS "protocol-ingress;graph")

cmake_add_test(
  test_protocol_registry
  SOURCES test_protocol_registry.c
  LIBS TurboFlow::ProtocolIngress Salts::TinyTest
  FOLDER "ingress/protocol/tests")

cmake_add_test(
  test_protocol_plugins
  SOURCES test_protocol_plugins.c
  LIBS TurboFlow::ProtocolIngress Salts::TinyTest
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

cmake_add_test(
  test_protocol_runtime
  SOURCES test_protocol_runtime.c
  LIBS TurboFlow::ProtocolIngress Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_runtime
                     PROPERTIES LABELS "protocol-ingress;protocol-runtime")

cmake_add_test(
  test_protocol_business
  SOURCES test_protocol_business.c
  LIBS TurboFlow::ProtocolIngress Salts::TinyTest
  FOLDER "ingress/protocol/tests")
set_tests_properties(test_protocol_business
                     PROPERTIES LABELS "protocol-ingress;protocol-business")

if(TARGET tf_protocol_business_ocpp201_core)
  cmake_add_test(
    test_protocol_business_ocpp201_core
    SOURCES test_protocol_business_ocpp201_core.c
    LIBS TurboFlow::ProtocolIngress Salts::TinyTest
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
