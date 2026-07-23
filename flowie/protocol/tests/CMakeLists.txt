cmake_add_test(
  test_flowie_mqtt_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_protocol.c
  LIBS Flowie::Protocol TurboUtils::TinyTest
  FOLDER "flowie/protocol/tests")

set_tests_properties(test_flowie_mqtt_protocol PROPERTIES LABELS "flowie;flowie-release")

cmake_add_test(
  test_flowie_mqtt_protocol_matrix
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_protocol_matrix.c
  LIBS Flowie::Protocol TurboUtils::TinyTest
  FOLDER "flowie/protocol/tests")

set_tests_properties(
  test_flowie_mqtt_protocol_matrix
  PROPERTIES LABELS "flowie;flowie-release;mqtt-fast;mqtt-negative" TIMEOUT 20)

cmake_add_test(
  test_flowie_mqtt_protocol_corpus
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_protocol_corpus.c
  LIBS Flowie::Protocol TurboUtils::TinyTest
  FOLDER "flowie/protocol/tests")

set_tests_properties(
  test_flowie_mqtt_protocol_corpus
  PROPERTIES LABELS "flowie;flowie-release;mqtt-fast;mqtt-negative;mqtt-fuzz" TIMEOUT 20)

if(FLOWIE_MQTT_FUZZ_TARGETS)
  if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "FLOWIE_MQTT_FUZZ_TARGETS requires Clang libFuzzer")
  endif()
  add_executable(fuzz_flowie_mqtt_protocol fuzz_flowie_mqtt_protocol.c)
  cmake_config_target(fuzz_flowie_mqtt_protocol NO_INSTALL FOLDER "flowie/protocol/fuzz")
  target_link_libraries(fuzz_flowie_mqtt_protocol PRIVATE Flowie::Protocol)
  target_compile_options(fuzz_flowie_mqtt_protocol
                         PRIVATE -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer)
  target_link_options(fuzz_flowie_mqtt_protocol PRIVATE -fsanitize=fuzzer,address,undefined)
endif()
