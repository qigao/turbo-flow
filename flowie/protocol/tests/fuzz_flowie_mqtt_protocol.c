#include "flowie_mqtt_protocol.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static int flowie_fuzz_known_result(int rc) {
  return rc == FLOWIE_MQTT_PARSE_OK || rc == FLOWIE_MQTT_PARSE_NEED_MORE ||
         rc == FLOWIE_MQTT_PARSE_MALFORMED || rc == FLOWIE_MQTT_PARSE_PROTOCOL_ERROR ||
         rc == FLOWIE_MQTT_PARSE_TOO_LARGE || rc == FLOWIE_MQTT_PARSE_NO_MEMORY;
}

static int flowie_fuzz_known_typed_result(int rc) {
  return flowie_fuzz_known_result(rc) || rc == FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
}

static void flowie_fuzz_require(int condition) {
  if (!condition) abort();
}

static void flowie_fuzz_typed_views(const flowie_mqtt_packet_view_t *packet) {
  int rc;
  switch (packet->type) {
  case FLOWIE_MQTT_PACKET_CONNECT: {
    flowie_mqtt_connect_view_t view = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    rc = flowie_mqtt_connect_parse(packet, &view);
    break;
  }
  case FLOWIE_MQTT_PACKET_PUBLISH: {
    flowie_mqtt_publish_view_t view = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    rc = flowie_mqtt_publish_parse(packet, &view);
    break;
  }
  case FLOWIE_MQTT_PACKET_SUBSCRIBE: {
    flowie_mqtt_subscribe_view_t view = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    rc = flowie_mqtt_subscribe_parse(packet, &view);
    break;
  }
  case FLOWIE_MQTT_PACKET_UNSUBSCRIBE: {
    flowie_mqtt_unsubscribe_view_t view = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    rc = flowie_mqtt_unsubscribe_parse(packet, &view);
    break;
  }
  case FLOWIE_MQTT_PACKET_PINGREQ:
    flowie_fuzz_require(packet->remaining_length == 0u && packet->body.size == 0u);
    return;
  default: {
    flowie_mqtt_control_packet_view_t view = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    rc = flowie_mqtt_control_packet_parse(packet, &view);
    break;
  }
  }
  flowie_fuzz_require(flowie_fuzz_known_typed_result(rc));
}

static void flowie_fuzz_property_block(const uint8_t *data, size_t size) {
  flowie_mqtt_property_block_view_t first = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  flowie_mqtt_property_block_view_t second = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  size_t first_consumed = 0u;
  size_t second_consumed = 0u;
  int first_rc = flowie_mqtt_property_block_parse(
      (flowie_mqtt_span_t){data, size}, &first, &first_consumed);
  int second_rc = flowie_mqtt_property_block_parse(
      (flowie_mqtt_span_t){data, size}, &second, &second_consumed);
  flowie_fuzz_require(flowie_fuzz_known_result(first_rc));
  flowie_fuzz_require(first_rc == second_rc && first_consumed == second_consumed);
  if (first_rc == FLOWIE_MQTT_PARSE_OK) {
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    int rc;
    flowie_fuzz_require(first_consumed > 0u && first_consumed <= size &&
                        first.encoded.data == data && first.encoded.size == first_consumed);
    rc = flowie_mqtt_property_iterator_init(&first, &iterator);
    flowie_fuzz_require(rc == FLOWIE_MQTT_PARSE_OK);
    for (;;) {
      flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
      rc = flowie_mqtt_property_iterator_next(&iterator, &property);
      if (rc != FLOWIE_MQTT_PARSE_OK) break;
      flowie_fuzz_require(iterator.cursor >= first.values.data &&
                          iterator.cursor <= first.values.data + first.values.size);
    }
    flowie_fuzz_require(flowie_fuzz_known_result(rc));
  }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static const flowie_mqtt_version_t versions[] = {
      FLOWIE_MQTT_VERSION_UNSPECIFIED,
      FLOWIE_MQTT_VERSION_3_1,
      FLOWIE_MQTT_VERSION_3_1_1,
      FLOWIE_MQTT_VERSION_5,
  };
  for (size_t i = 0u; i < sizeof(versions) / sizeof(versions[0]); ++i) {
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_packet_view_t repeat = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_parse_error_t error = FLOWIE_MQTT_PARSE_ERROR_INIT;
    flowie_mqtt_parse_error_t repeat_error = FLOWIE_MQTT_PARSE_ERROR_INIT;
    size_t consumed = 0u;
    size_t repeat_consumed = 0u;
    int rc;
    int repeat_rc;
    options.version = versions[i];
    rc = flowie_mqtt_packet_parse(data, size, &options, &packet, &consumed, &error);
    repeat_rc =
        flowie_mqtt_packet_parse(data, size, &options, &repeat, &repeat_consumed, &repeat_error);
    flowie_fuzz_require(flowie_fuzz_known_result(rc));
    flowie_fuzz_require(rc == repeat_rc && consumed == repeat_consumed && error.code == rc &&
                        repeat_error.code == repeat_rc);
    if (rc == FLOWIE_MQTT_PARSE_OK) {
      flowie_fuzz_require(consumed > 0u && consumed <= size && packet.packet.data == data &&
                          packet.packet.size == consumed && repeat.type == packet.type);
      flowie_fuzz_typed_views(&packet);
    } else {
      flowie_fuzz_require(consumed == 0u);
    }
  }
  flowie_fuzz_property_block(data, size);
  return 0;
}
