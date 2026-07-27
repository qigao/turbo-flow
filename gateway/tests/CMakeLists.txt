cmake_add_test(
  test_gateway_registry
  SOURCES test_gateway_registry.c
  LIBS TurboFlow::Gateway TurboUtils::TinyTest
  FOLDER "gateway/tests")

cmake_add_test(
  test_gateway_plugins
  SOURCES test_gateway_plugins.c
  LIBS TurboFlow::Gateway TurboUtils::TinyTest
  DEFS
    FLOW_GATEWAY_MQTT_SN_MODULE="$<TARGET_FILE:tf_gateway_mqtt_sn>"
    FLOW_GATEWAY_COAP_MODULE="$<TARGET_FILE:tf_gateway_coap>"
    FLOW_GATEWAY_LWM2M_MODULE="$<TARGET_FILE:tf_gateway_lwm2m>"
    FLOW_GATEWAY_OCPP_MODULE="$<TARGET_FILE:tf_gateway_ocpp>"
    FLOW_GATEWAY_GBT32960_MODULE="$<TARGET_FILE:tf_gateway_gbt32960>"
    FLOW_GATEWAY_JTT808_MODULE="$<TARGET_FILE:tf_gateway_jtt808>"
  FOLDER "gateway/tests")
add_dependencies(
  test_gateway_plugins
  tf_gateway_mqtt_sn
  tf_gateway_coap
  tf_gateway_lwm2m
  tf_gateway_ocpp
  tf_gateway_gbt32960
  tf_gateway_jtt808)
set_tests_properties(test_gateway_registry test_gateway_plugins
                     PROPERTIES LABELS "gateway")

cmake_add_test(
  test_gateway_runtime
  SOURCES test_gateway_runtime.c
  LIBS TurboFlow::Gateway TurboUtils::TinyTest
  FOLDER "gateway/tests")
set_tests_properties(test_gateway_runtime
                     PROPERTIES LABELS "gateway;gateway-runtime")

cmake_add_test(
  test_gateway_business
  SOURCES test_gateway_business.c
  LIBS TurboFlow::Gateway TurboUtils::TinyTest
  FOLDER "gateway/tests")
set_tests_properties(test_gateway_business
                     PROPERTIES LABELS "gateway;gateway-business")

if(TARGET tf_gateway_business_ocpp201_core)
  cmake_add_test(
    test_gateway_business_ocpp201_core
    SOURCES test_gateway_business_ocpp201_core.c
    LIBS TurboFlow::Gateway TurboUtils::TinyTest
    DEFS
      FLOW_GATEWAY_BUSINESS_OCPP201_CORE_MODULE="$<TARGET_FILE:tf_gateway_business_ocpp201_core>"
      FLOW_GATEWAY_OCPP_MODULE="$<TARGET_FILE:tf_gateway_ocpp>"
    FOLDER "gateway/tests")
  add_dependencies(
    test_gateway_business_ocpp201_core
    tf_gateway_business_ocpp201_core
    tf_gateway_ocpp)
  set_tests_properties(
    test_gateway_business_ocpp201_core
    PROPERTIES LABELS "gateway;gateway-business;databind")

  cmake_add_test(
    test_gateway_business_ocpp201_core_schema
    SOURCES test_gateway_business_ocpp201_core_schema.cpp
    LIBS tf_gateway_business_ocpp201_core_schema_shared
         TurboUtils::TinyTest
    FOLDER "gateway/tests")
  target_compile_definitions(
    test_gateway_business_ocpp201_core_schema
    PRIVATE TBE_GENERATED_USE_SHARED)
  set_target_properties(
    test_gateway_business_ocpp201_core_schema
    PROPERTIES CXX_STANDARD 17
               CXX_STANDARD_REQUIRED ON)
  set_tests_properties(
    test_gateway_business_ocpp201_core_schema
    PROPERTIES LABELS "gateway;gateway-business;databind")
endif()

cmake_add_test(
  test_gateway_coronet
  SOURCES test_gateway_coronet.c
  INCLUDES ${PROJECT_SOURCE_DIR}/io/socket/tests
  LIBS TurboFlow::GatewayCoroNet TurboFlow::Gateway TurboNet::CoroNet
       TurboUtils::TinyTest
  DEFS
    FLOW_GATEWAY_COAP_MODULE="$<TARGET_FILE:tf_gateway_coap>"
    FLOW_GATEWAY_LWM2M_MODULE="$<TARGET_FILE:tf_gateway_lwm2m>"
    FLOW_GATEWAY_OCPP_MODULE="$<TARGET_FILE:tf_gateway_ocpp>"
    FLOW_GATEWAY_GBT32960_MODULE="$<TARGET_FILE:tf_gateway_gbt32960>"
  FOLDER "gateway/tests")
add_dependencies(
  test_gateway_coronet
  tf_gateway_coap
  tf_gateway_lwm2m
  tf_gateway_ocpp
  tf_gateway_gbt32960)
if(WIN32)
  target_link_libraries(test_gateway_coronet PRIVATE ws2_32)
endif()
set_tests_properties(test_gateway_coronet
                     PROPERTIES LABELS "gateway;gateway-coronet")

if(GATEWAY_TRANSPORT_SOAK_TESTS)
  cmake_add_test(
    test_gateway_coronet_soak
    SOURCES test_gateway_coronet_soak.c
    LIBS TurboUtils::Core TurboUtils::TinyTest
    DEFS
      GATEWAY_CORONET_SOAK_TEST="$<TARGET_FILE:test_gateway_coronet>"
      $<$<BOOL:${ENABLE_SANITIZER_ADDRESS}>:GATEWAY_SOAK_HAS_ASAN=1>
    FOLDER "gateway/tests")
  add_dependencies(test_gateway_coronet_soak test_gateway_coronet)
  set_tests_properties(
    test_gateway_coronet_soak
    PROPERTIES LABELS "gateway;gateway-coronet;gateway-soak;scheduled"
               TIMEOUT 21600)
  if(WIN32)
    target_link_libraries(test_gateway_coronet_soak PRIVATE psapi)
  endif()
endif()
