cmake_add_test(
  test_mqtt_sink
  SOURCES test_mqtt_sink.c
  LIBS TurboFlow::MqttSink Salts::TinyTest
  FOLDER "sink/mqtt/tests")
set_tests_properties(test_mqtt_sink PROPERTIES LABELS "mqtt-sink;protocol-ingress")
