cmake_add_test(
  test_flowie_mqtt_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_protocol.c
  LIBS Flowie::Protocol TurboUtils::TinyTest
  FOLDER "flowie/protocol/tests")

set_tests_properties(test_flowie_mqtt_protocol PROPERTIES LABELS "flowie")
