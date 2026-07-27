cmake_add_test(
  test_flowie_mqtt_client
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_client.c
  LIBS Flowie::Client Flowie::Protocol TurboNet::CoroNet TurboUtils::TinyTest
  FOLDER "flowie/client/tests")

target_include_directories(test_flowie_mqtt_client
                           PRIVATE ${PROJECT_SOURCE_DIR}/flowie/tests
                                   ${PROJECT_SOURCE_DIR}/io/socket/tests)
target_link_libraries(test_flowie_mqtt_client PRIVATE OpenSSL::SSL OpenSSL::Crypto)
cmake_target_enable_boringssl_linkage(test_flowie_mqtt_client)
set_tests_properties(
  test_flowie_mqtt_client
  PROPERTIES LABELS "flowie;flowie-release;mqtt;client;mqtt-security;mqtt-negative")

cmake_add_test(
  test_flowie_mqtt_client_live
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_client_live.c
  LIBS Flowie::Client Flowie::Protocol TurboNet::CoroNet TurboUtils::TinyTest
  FOLDER "flowie/client/tests")

set_tests_properties(
  test_flowie_mqtt_client_live
  PROPERTIES LABELS "flowie;mqtt;client;integration;mqtt-public-live;external-smoke"
             TIMEOUT 120)
if(NOT FLOWIE_MQTT_PUBLIC_LIVE_TESTS)
  set_tests_properties(test_flowie_mqtt_client_live PROPERTIES DISABLED TRUE)
endif()

if(FLOWIE_MQTT_FIXED_INTEROP_TESTS)
  if(NOT FLOWIE_MQTT_FIXED_CA_FILE OR NOT EXISTS "${FLOWIE_MQTT_FIXED_CA_FILE}")
    message(FATAL_ERROR
            "FLOWIE_MQTT_FIXED_INTEROP_TESTS requires FLOWIE_MQTT_FIXED_CA_FILE")
  endif()
  cmake_add_test(
    test_flowie_mqtt_client_fixed_interop
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_client_live.c
    LIBS Flowie::Client Flowie::Protocol TurboNet::CoroNet TurboUtils::TinyTest
    FOLDER "flowie/client/tests")
  target_compile_definitions(
    test_flowie_mqtt_client_fixed_interop
    PRIVATE FLOWIE_MQTT_FIXED_INTEROP=1
            FLOWIE_MQTT_FIXED_BROKER_NAME="${FLOWIE_MQTT_FIXED_BROKER_NAME}"
            FLOWIE_MQTT_FIXED_HOST="${FLOWIE_MQTT_FIXED_HOST}"
            FLOWIE_MQTT_FIXED_TCP_PORT=${FLOWIE_MQTT_FIXED_TCP_PORT}
            FLOWIE_MQTT_FIXED_TLS_PORT=${FLOWIE_MQTT_FIXED_TLS_PORT}
            FLOWIE_MQTT_FIXED_WS_PORT=${FLOWIE_MQTT_FIXED_WS_PORT}
            FLOWIE_MQTT_FIXED_WSS_PORT=${FLOWIE_MQTT_FIXED_WSS_PORT}
            FLOWIE_MQTT_FIXED_WS_PATH="${FLOWIE_MQTT_FIXED_WS_PATH}"
            FLOWIE_MQTT_FIXED_CA_FILE="${FLOWIE_MQTT_FIXED_CA_FILE}"
            FLOWIE_MQTT_FIXED_SUPPORT_31=$<BOOL:${FLOWIE_MQTT_FIXED_SUPPORT_31}>
            FLOWIE_MQTT_FIXED_SUPPORT_31_WS=$<BOOL:${FLOWIE_MQTT_FIXED_SUPPORT_31_WS}>)
  set_tests_properties(
    test_flowie_mqtt_client_fixed_interop
    PROPERTIES LABELS
               "flowie;flowie-release;mqtt-interop;mqtt-fixed-interop;mqtt-persistence"
               TIMEOUT 300)
  add_test(
    NAME flowie_mqtt_fixed_mqtt31_capability
    COMMAND
      ${CMAKE_COMMAND} -E echo
      "$<IF:$<BOOL:${FLOWIE_MQTT_FIXED_SUPPORT_31}>,SUPPORTED: MQTT 3.1 TCP/TLS,SKIP: fixed broker does not declare MQTT 3.1 TCP/TLS>")
  set_tests_properties(
    flowie_mqtt_fixed_mqtt31_capability
    PROPERTIES LABELS "flowie;flowie-release;mqtt-interop;mqtt-fixed-interop"
               SKIP_REGULAR_EXPRESSION "^SKIP: fixed broker")
  add_test(
    NAME flowie_mqtt_fixed_mqtt31_ws_capability
    COMMAND
      ${CMAKE_COMMAND} -E echo
      "$<IF:$<BOOL:${FLOWIE_MQTT_FIXED_SUPPORT_31_WS}>,SUPPORTED: MQTT 3.1 WS/WSS,SKIP: fixed broker does not declare MQTT 3.1 WS/WSS>")
  set_tests_properties(
    flowie_mqtt_fixed_mqtt31_ws_capability
    PROPERTIES LABELS "flowie;flowie-release;mqtt-interop;mqtt-fixed-interop"
               SKIP_REGULAR_EXPRESSION "^SKIP: fixed broker")
  if(WIN32)
    target_link_libraries(test_flowie_mqtt_client_fixed_interop PRIVATE ws2_32)
  endif()
endif()

if(WIN32)
  target_link_libraries(test_flowie_mqtt_client PRIVATE ws2_32)
  target_link_libraries(test_flowie_mqtt_client_live PRIVATE ws2_32)
endif()
