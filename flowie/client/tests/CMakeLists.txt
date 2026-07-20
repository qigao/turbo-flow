cmake_add_test(
  test_flowie_mqtt_client
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_client.c
  LIBS Flowie::Client Flowie::Protocol TurboNet::CoroNet TurboUtils::TinyTest
  FOLDER "flowie/client/tests")

target_include_directories(test_flowie_mqtt_client
                           PRIVATE ${PROJECT_SOURCE_DIR}/flowie/tests
                                   ${PROJECT_SOURCE_DIR}/io/socket/tests)
target_link_libraries(test_flowie_mqtt_client PRIVATE OpenSSL::SSL OpenSSL::Crypto)
set_tests_properties(test_flowie_mqtt_client PROPERTIES LABELS "flowie;flowie-release;mqtt;client")

cmake_add_test(
  test_flowie_mqtt_client_live
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_client_live.c
  LIBS Flowie::Client Flowie::Protocol TurboNet::CoroNet TurboUtils::TinyTest
  FOLDER "flowie/client/tests")

set_tests_properties(
  test_flowie_mqtt_client_live
  PROPERTIES LABELS "flowie;flowie-release;mqtt;client;integration;mqtt-public-live"
             TIMEOUT 120)
if(NOT FLOWIE_MQTT_PUBLIC_LIVE_TESTS)
  set_tests_properties(test_flowie_mqtt_client_live PROPERTIES DISABLED TRUE)
endif()

if(WIN32)
  target_link_libraries(test_flowie_mqtt_client PRIVATE ws2_32)
  target_link_libraries(test_flowie_mqtt_client_live PRIVATE ws2_32)
endif()
