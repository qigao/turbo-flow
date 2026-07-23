#include "flowie_mqtt_protocol.h"

#include "tinytest.h"

#include <stdint.h>
#include <string.h>

typedef struct property_case_s {
  const uint8_t *values;
  size_t value_size;
  int expected;
} property_case_t;

typedef enum property_context_e {
  PROPERTY_CONTEXT_CONNECT = 0,
  PROPERTY_CONTEXT_WILL,
  PROPERTY_CONTEXT_PUBLISH,
  PROPERTY_CONTEXT_SUBSCRIBE,
  PROPERTY_CONTEXT_UNSUBSCRIBE,
  PROPERTY_CONTEXT_CONNACK,
  PROPERTY_CONTEXT_PUBACK,
  PROPERTY_CONTEXT_PUBREC,
  PROPERTY_CONTEXT_PUBREL,
  PROPERTY_CONTEXT_PUBCOMP,
  PROPERTY_CONTEXT_SUBACK,
  PROPERTY_CONTEXT_UNSUBACK,
  PROPERTY_CONTEXT_PINGRESP,
  PROPERTY_CONTEXT_DISCONNECT,
  PROPERTY_CONTEXT_AUTH,
  PROPERTY_CONTEXT_COUNT
} property_context_t;

#define PROPERTY_CONTEXT_BIT(context) (UINT32_C(1) << (context))

typedef struct property_spec_s {
  const uint8_t *values;
  size_t value_size;
  uint32_t allowed_contexts;
} property_spec_t;

typedef struct round_trip_wire_s {
  const uint8_t *bytes;
  size_t size;
} round_trip_wire_t;

enum {
  ROUND_TRIP_BUFFER_CAPACITY = 128,
  ROUND_TRIP_MAX_ENTRIES = 4
};

static flowie_mqtt_span_t literal_span(const char *value) {
  flowie_mqtt_span_t result = {(const uint8_t *)value, strlen(value)};
  return result;
}

static void initialize_packet_sentinel(flowie_mqtt_packet_view_t *packet) {
  memset(packet, 0xa5, sizeof(*packet));
  packet->size = sizeof(*packet);
  packet->abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
}

static void check_packet_result(const uint8_t *bytes, size_t byte_count,
                                flowie_mqtt_version_t version, int expected) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet;
  flowie_mqtt_packet_view_t before;
  flowie_mqtt_parse_error_t error = FLOWIE_MQTT_PARSE_ERROR_INIT;
  size_t consumed = SIZE_MAX;
  int rc;
  options.version = version;
  initialize_packet_sentinel(&packet);
  before = packet;
  rc = flowie_mqtt_packet_parse(bytes, byte_count, &options, &packet, &consumed, &error);
  check_int_eq(rc, expected);
  check_int_eq(error.code, expected);
  if (expected == FLOWIE_MQTT_PARSE_OK) {
    check_size_eq(consumed, packet.packet.size);
    check(packet.packet.data == bytes);
  } else {
    check_size_eq(consumed, 0u);
    check_mem_eq(&packet, &before, sizeof(packet));
  }
}

static size_t build_v5_connect(uint8_t *output, size_t capacity, const uint8_t *properties,
                               size_t property_size) {
  const size_t remaining = 14u + property_size;
  size_t offset = 0u;
  check_not_null(output);
  check(capacity >= remaining + 2u);
  check(property_size < 128u);
  output[offset++] = 0x10u;
  output[offset++] = (uint8_t)remaining;
  output[offset++] = 0x00u;
  output[offset++] = 0x04u;
  output[offset++] = 'M';
  output[offset++] = 'Q';
  output[offset++] = 'T';
  output[offset++] = 'T';
  output[offset++] = 0x05u;
  output[offset++] = 0x02u;
  output[offset++] = 0x00u;
  output[offset++] = 0x3cu;
  output[offset++] = (uint8_t)property_size;
  if (property_size != 0u) {
    memcpy(output + offset, properties, property_size);
    offset += property_size;
  }
  output[offset++] = 0x00u;
  output[offset++] = 0x01u;
  output[offset++] = 'c';
  check_size_eq(offset, remaining + 2u);
  return offset;
}

static size_t build_v5_publish(uint8_t *output, size_t capacity, const uint8_t *properties,
                               size_t property_size) {
  const size_t remaining = 4u + property_size;
  size_t offset = 0u;
  check_not_null(output);
  check(capacity >= remaining + 2u);
  check(property_size < 128u);
  output[offset++] = 0x30u;
  output[offset++] = (uint8_t)remaining;
  output[offset++] = 0x00u;
  output[offset++] = 0x01u;
  output[offset++] = 't';
  output[offset++] = (uint8_t)property_size;
  if (property_size != 0u) {
    memcpy(output + offset, properties, property_size);
    offset += property_size;
  }
  check_size_eq(offset, remaining + 2u);
  return offset;
}

static size_t build_v5_publish_topic(uint8_t *output, size_t capacity, const uint8_t *topic,
                                     size_t topic_size) {
  const size_t remaining = 3u + topic_size;
  size_t offset = 0u;
  check_not_null(output);
  check(topic != NULL || topic_size == 0u);
  check(topic_size <= UINT16_MAX);
  check(remaining < 128u);
  check(capacity >= remaining + 2u);
  output[offset++] = 0x30u;
  output[offset++] = (uint8_t)remaining;
  output[offset++] = (uint8_t)(topic_size >> 8u);
  output[offset++] = (uint8_t)topic_size;
  if (topic_size != 0u) {
    memcpy(output + offset, topic, topic_size);
    offset += topic_size;
  }
  output[offset++] = 0x00u;
  check_size_eq(offset, remaining + 2u);
  return offset;
}

static size_t build_connect_dialect(uint8_t *output, size_t capacity, const char *protocol_name,
                                    size_t protocol_name_size, uint8_t level) {
  const size_t property_prefix_size = level == FLOWIE_MQTT_VERSION_5 ? 1u : 0u;
  const size_t remaining = 2u + protocol_name_size + 4u + property_prefix_size + 3u;
  size_t offset = 0u;
  check_not_null(output);
  check_not_null(protocol_name);
  check(protocol_name_size <= UINT16_MAX);
  check(capacity >= remaining + 2u);
  check(remaining < 128u);
  output[offset++] = 0x10u;
  output[offset++] = (uint8_t)remaining;
  output[offset++] = (uint8_t)(protocol_name_size >> 8u);
  output[offset++] = (uint8_t)protocol_name_size;
  memcpy(output + offset, protocol_name, protocol_name_size);
  offset += protocol_name_size;
  output[offset++] = level;
  output[offset++] = 0x02u;
  output[offset++] = 0x00u;
  output[offset++] = 0x3cu;
  if (level == FLOWIE_MQTT_VERSION_5) output[offset++] = 0x00u;
  output[offset++] = 0x00u;
  output[offset++] = 0x01u;
  output[offset++] = 'c';
  check_size_eq(offset, remaining + 2u);
  return offset;
}

static int byte_is_listed(uint8_t value, const uint8_t *values, size_t value_count) {
  for (size_t i = 0u; i < value_count; ++i)
    if (values[i] == value) return 1;
  return 0;
}

static int mqtt5_reason_valid(flowie_mqtt_packet_type_t type, uint8_t reason) {
  static const uint8_t connack[] = {0x00u, 0x80u, 0x81u, 0x82u, 0x83u, 0x84u, 0x85u, 0x86u,
                                    0x87u, 0x88u, 0x89u, 0x8au, 0x8cu, 0x90u, 0x95u, 0x97u,
                                    0x99u, 0x9au, 0x9bu, 0x9cu, 0x9du, 0x9fu};
  static const uint8_t publish[] = {0x00u, 0x10u, 0x80u, 0x83u, 0x87u,
                                    0x90u, 0x91u, 0x97u, 0x99u};
  static const uint8_t release[] = {0x00u, 0x92u};
  static const uint8_t suback[] = {0x00u, 0x01u, 0x02u, 0x80u, 0x83u, 0x87u,
                                   0x8fu, 0x91u, 0x97u, 0x9eu, 0xa1u, 0xa2u};
  static const uint8_t unsuback[] = {0x00u, 0x11u, 0x80u, 0x83u, 0x87u, 0x8fu, 0x91u};
  static const uint8_t disconnect[] = {
      0x00u, 0x04u, 0x80u, 0x81u, 0x82u, 0x83u, 0x87u, 0x89u, 0x8bu, 0x8cu,
      0x8du, 0x8eu, 0x8fu, 0x90u, 0x93u, 0x94u, 0x95u, 0x96u, 0x97u, 0x98u,
      0x99u, 0x9au, 0x9bu, 0x9cu, 0x9du, 0x9eu, 0x9fu, 0xa0u, 0xa1u, 0xa2u};
  static const uint8_t auth[] = {0x00u, 0x18u, 0x19u};
  const uint8_t *allowed = NULL;
  size_t allowed_count = 0u;
  switch (type) {
  case FLOWIE_MQTT_PACKET_CONNACK:
    allowed = connack;
    allowed_count = sizeof(connack);
    break;
  case FLOWIE_MQTT_PACKET_PUBACK:
  case FLOWIE_MQTT_PACKET_PUBREC:
    allowed = publish;
    allowed_count = sizeof(publish);
    break;
  case FLOWIE_MQTT_PACKET_PUBREL:
  case FLOWIE_MQTT_PACKET_PUBCOMP:
    allowed = release;
    allowed_count = sizeof(release);
    break;
  case FLOWIE_MQTT_PACKET_SUBACK:
    allowed = suback;
    allowed_count = sizeof(suback);
    break;
  case FLOWIE_MQTT_PACKET_UNSUBACK:
    allowed = unsuback;
    allowed_count = sizeof(unsuback);
    break;
  case FLOWIE_MQTT_PACKET_DISCONNECT:
    allowed = disconnect;
    allowed_count = sizeof(disconnect);
    break;
  case FLOWIE_MQTT_PACKET_AUTH:
    allowed = auth;
    allowed_count = sizeof(auth);
    break;
  default:
    return 0;
  }
  return byte_is_listed(reason, allowed, allowed_count);
}

static int encode_property_context(property_context_t context, const uint8_t *values,
                                   size_t value_size, uint8_t *output, size_t output_capacity,
                                   size_t *written) {
  const flowie_mqtt_span_t properties = {values, value_size};
  static const uint8_t success_reason[] = {0x00u};
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_span_t filter = literal_span("t");
  if (context == PROPERTY_CONTEXT_CONNECT || context == PROPERTY_CONTEXT_WILL) {
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.client_id = literal_span("c");
    if (context == PROPERTY_CONTEXT_CONNECT) {
      connect.properties = properties;
    } else {
      connect.has_will = 1u;
      connect.will_properties = properties;
      connect.will_topic = literal_span("t");
    }
    return flowie_mqtt_connect_packet_encode(&connect, output, output_capacity, written);
  }
  if (context == PROPERTY_CONTEXT_PUBLISH) {
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = literal_span("t");
    publish.properties = properties;
    return flowie_mqtt_publish_packet_encode(&publish, output, output_capacity, written);
  }
  if (context == PROPERTY_CONTEXT_SUBSCRIBE) {
    subscription.filter = literal_span("t");
    subscribe.version = FLOWIE_MQTT_VERSION_5;
    subscribe.packet_id = 1u;
    subscribe.properties = properties;
    subscribe.subscriptions = &subscription;
    subscribe.subscription_count = 1u;
    return flowie_mqtt_subscribe_packet_encode(&subscribe, output, output_capacity, written);
  }
  if (context == PROPERTY_CONTEXT_UNSUBSCRIBE) {
    unsubscribe.version = FLOWIE_MQTT_VERSION_5;
    unsubscribe.packet_id = 1u;
    unsubscribe.properties = properties;
    unsubscribe.filters = &filter;
    unsubscribe.filter_count = 1u;
    return flowie_mqtt_unsubscribe_packet_encode(&unsubscribe, output, output_capacity, written);
  }
  control.version = FLOWIE_MQTT_VERSION_5;
  control.properties = properties;
  switch (context) {
  case PROPERTY_CONTEXT_CONNACK:
    control.type = FLOWIE_MQTT_PACKET_CONNACK;
    break;
  case PROPERTY_CONTEXT_PUBACK:
    control.type = FLOWIE_MQTT_PACKET_PUBACK;
    control.packet_id = 1u;
    break;
  case PROPERTY_CONTEXT_PUBREC:
    control.type = FLOWIE_MQTT_PACKET_PUBREC;
    control.packet_id = 1u;
    break;
  case PROPERTY_CONTEXT_PUBREL:
    control.type = FLOWIE_MQTT_PACKET_PUBREL;
    control.packet_id = 1u;
    break;
  case PROPERTY_CONTEXT_PUBCOMP:
    control.type = FLOWIE_MQTT_PACKET_PUBCOMP;
    control.packet_id = 1u;
    break;
  case PROPERTY_CONTEXT_SUBACK:
    control.type = FLOWIE_MQTT_PACKET_SUBACK;
    control.packet_id = 1u;
    control.reason_codes = (flowie_mqtt_span_t){success_reason, sizeof(success_reason)};
    break;
  case PROPERTY_CONTEXT_UNSUBACK:
    control.type = FLOWIE_MQTT_PACKET_UNSUBACK;
    control.packet_id = 1u;
    control.reason_codes = (flowie_mqtt_span_t){success_reason, sizeof(success_reason)};
    break;
  case PROPERTY_CONTEXT_PINGRESP:
    control.type = FLOWIE_MQTT_PACKET_PINGRESP;
    break;
  case PROPERTY_CONTEXT_DISCONNECT:
    control.type = FLOWIE_MQTT_PACKET_DISCONNECT;
    break;
  case PROPERTY_CONTEXT_AUTH:
    control.type = FLOWIE_MQTT_PACKET_AUTH;
    break;
  default:
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  return flowie_mqtt_control_packet_encode(&control, output, output_capacity, written);
}

static int parse_property_context(property_context_t context, const uint8_t *bytes,
                                  size_t byte_count) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  int rc;
  options.version = FLOWIE_MQTT_VERSION_5;
  rc = flowie_mqtt_packet_parse(bytes, byte_count, &options, &packet, NULL, NULL);
  if (rc != FLOWIE_MQTT_PARSE_OK) return rc;
  if (context == PROPERTY_CONTEXT_CONNECT || context == PROPERTY_CONTEXT_WILL) {
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    return flowie_mqtt_connect_parse(&packet, &connect);
  }
  if (context == PROPERTY_CONTEXT_PUBLISH) {
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    return flowie_mqtt_publish_parse(&packet, &publish);
  }
  if (context == PROPERTY_CONTEXT_SUBSCRIBE) {
    flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    return flowie_mqtt_subscribe_parse(&packet, &subscribe);
  }
  if (context == PROPERTY_CONTEXT_UNSUBSCRIBE) {
    flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    return flowie_mqtt_unsubscribe_parse(&packet, &unsubscribe);
  }
  {
    flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    return flowie_mqtt_control_packet_parse(&packet, &control);
  }
}

static void check_canonical_round_trip_pass(const round_trip_wire_t *wire,
                                            flowie_mqtt_version_t version,
                                            unsigned int remaining_passes) {
  uint8_t encoded[ROUND_TRIP_BUFFER_CAPACITY];
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  size_t consumed = 0u;
  size_t written = 0u;
  int rc = FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  check_not_null(wire);
  check(wire->size <= sizeof(encoded));
  options.version = version;
  check_int_eq(flowie_mqtt_packet_parse(wire->bytes, wire->size, &options, &packet, &consumed, NULL),
               FLOWIE_MQTT_PARSE_OK);
  check_size_eq(consumed, wire->size);
  check_int_eq(packet.version, version);

  switch (packet.type) {
  case FLOWIE_MQTT_PACKET_CONNECT: {
    flowie_mqtt_connect_view_t decoded = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_mqtt_connect_packet_t rebuilt = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.version, version);
    rebuilt.version = decoded.version;
    rebuilt.clean_start = decoded.clean_start;
    rebuilt.has_will = decoded.will_topic.data != NULL;
    rebuilt.will_qos = decoded.will_qos;
    rebuilt.will_retain = decoded.will_retain;
    rebuilt.has_username = decoded.username.data != NULL;
    rebuilt.has_password = decoded.password.data != NULL;
    rebuilt.keep_alive = decoded.keep_alive;
    rebuilt.properties = decoded.properties.values;
    rebuilt.client_id = decoded.client_id;
    rebuilt.will_properties = decoded.will_properties.values;
    rebuilt.will_topic = decoded.will_topic;
    rebuilt.will_payload = decoded.will_payload;
    rebuilt.username = decoded.username;
    rebuilt.password = decoded.password;
    rc = flowie_mqtt_connect_packet_encode(&rebuilt, encoded, sizeof(encoded), &written);
    break;
  }
  case FLOWIE_MQTT_PACKET_PUBLISH: {
    flowie_mqtt_publish_view_t decoded = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_mqtt_publish_packet_t rebuilt = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    check_int_eq(flowie_mqtt_publish_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    rebuilt.version = version;
    rebuilt.qos = decoded.qos;
    rebuilt.retain = decoded.retain;
    rebuilt.duplicate = decoded.duplicate;
    rebuilt.packet_id = decoded.packet_id;
    rebuilt.topic = decoded.topic;
    rebuilt.properties = decoded.properties.values;
    rebuilt.payload = decoded.payload;
    rc = flowie_mqtt_publish_packet_encode(&rebuilt, encoded, sizeof(encoded), &written);
    break;
  }
  case FLOWIE_MQTT_PACKET_SUBSCRIBE: {
    flowie_mqtt_subscribe_view_t decoded = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
    flowie_mqtt_subscription_t entries[ROUND_TRIP_MAX_ENTRIES];
    flowie_mqtt_subscribe_packet_t rebuilt = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
    check_int_eq(flowie_mqtt_subscribe_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check(decoded.entry_count <= ROUND_TRIP_MAX_ENTRIES);
    if (decoded.entry_count > ROUND_TRIP_MAX_ENTRIES) return;
    check_int_eq(flowie_mqtt_subscription_iterator_init(&packet, &decoded, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    for (size_t i = 0u; i < decoded.entry_count; ++i)
      check_int_eq(flowie_mqtt_subscription_iterator_next(&iterator, &entries[i]),
                   FLOWIE_MQTT_PARSE_OK);
    rebuilt.version = version;
    rebuilt.packet_id = decoded.packet_id;
    rebuilt.properties = decoded.properties.values;
    rebuilt.subscriptions = entries;
    rebuilt.subscription_count = decoded.entry_count;
    rc = flowie_mqtt_subscribe_packet_encode(&rebuilt, encoded, sizeof(encoded), &written);
    break;
  }
  case FLOWIE_MQTT_PACKET_UNSUBSCRIBE: {
    flowie_mqtt_unsubscribe_view_t decoded = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    flowie_mqtt_topic_filter_iterator_t iterator = FLOWIE_MQTT_TOPIC_FILTER_ITERATOR_INIT;
    flowie_mqtt_span_t filters[ROUND_TRIP_MAX_ENTRIES];
    flowie_mqtt_unsubscribe_packet_t rebuilt = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
    check_int_eq(flowie_mqtt_unsubscribe_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check(decoded.filter_count <= ROUND_TRIP_MAX_ENTRIES);
    if (decoded.filter_count > ROUND_TRIP_MAX_ENTRIES) return;
    check_int_eq(flowie_mqtt_topic_filter_iterator_init(&decoded, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    for (size_t i = 0u; i < decoded.filter_count; ++i)
      check_int_eq(flowie_mqtt_topic_filter_iterator_next(&iterator, &filters[i]),
                   FLOWIE_MQTT_PARSE_OK);
    rebuilt.version = version;
    rebuilt.packet_id = decoded.packet_id;
    rebuilt.properties = decoded.properties.values;
    rebuilt.filters = filters;
    rebuilt.filter_count = decoded.filter_count;
    rc = flowie_mqtt_unsubscribe_packet_encode(&rebuilt, encoded, sizeof(encoded), &written);
    break;
  }
  case FLOWIE_MQTT_PACKET_PINGREQ:
    rc = flowie_mqtt_pingreq_encode(version, encoded, sizeof(encoded), &written);
    break;
  default: {
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_t rebuilt = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    rebuilt.version = decoded.version;
    rebuilt.type = decoded.type;
    rebuilt.session_present = decoded.session_present;
    rebuilt.packet_id = decoded.packet_id;
    rebuilt.reason_code = decoded.reason_code;
    rebuilt.properties = decoded.properties.values;
    rebuilt.reason_codes = decoded.reason_codes;
    rc = flowie_mqtt_control_packet_encode(&rebuilt, encoded, sizeof(encoded), &written);
    break;
  }
  }

  check_int_eq(rc, FLOWIE_MQTT_PARSE_OK);
  check_size_eq(written, wire->size);
  check_mem_eq(encoded, wire->bytes, wire->size);
  if (remaining_passes > 1u) {
    const round_trip_wire_t canonical = {encoded, written};
    check_canonical_round_trip_pass(&canonical, version, remaining_passes - 1u);
  }
}

static void check_canonical_round_trip(const round_trip_wire_t *wire,
                                       flowie_mqtt_version_t version) {
  check_canonical_round_trip_pass(wire, version, 2u);
}

spec("flowie mqtt protocol legality matrix") {
  it("MQTT-PROTO-001 accepts only legal fixed-header flag combinations") {
    static const uint8_t required_flags[16] = {0xffu, 0x00u, 0x00u, 0xffu, 0x00u, 0x00u,
                                                0x02u, 0x00u, 0x02u, 0x00u, 0x02u, 0x00u,
                                                0x00u, 0x00u, 0x00u, 0x00u};
    uint8_t packet[] = {0u, 0u};
    for (uint8_t type = FLOWIE_MQTT_PACKET_CONNECT; type <= FLOWIE_MQTT_PACKET_AUTH; ++type) {
      for (uint8_t flags = 0u; flags < 16u; ++flags) {
        int expected;
        packet[0] = (uint8_t)((type << 4u) | flags);
        if (type == FLOWIE_MQTT_PACKET_PUBLISH) {
          const uint8_t qos = (uint8_t)((flags >> 1u) & 0x03u);
          const uint8_t duplicate = (uint8_t)((flags >> 3u) & 0x01u);
          expected = qos != 3u && !(qos == 0u && duplicate)
                         ? FLOWIE_MQTT_PARSE_OK
                         : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
        } else {
          expected = flags == required_flags[type] ? FLOWIE_MQTT_PARSE_OK
                                                   : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
        }
        check_packet_result(packet, sizeof(packet), FLOWIE_MQTT_VERSION_5, expected);
      }
    }
  }

  it("MQTT-PROTO-005 accepts canonical variable byte integers and rejects malformed encodings") {
    static const uint8_t incomplete[] = {0xc0u};
    static const uint8_t noncanonical[] = {0xc0u, 0x80u, 0x00u};
    static const uint8_t five_byte[] = {0xc0u, 0xffu, 0xffu, 0xffu, 0xffu, 0x01u};
    uint8_t property_127[128];
    uint8_t property_128[130];
    flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    size_t consumed = SIZE_MAX;
    memset(property_127, 0u, sizeof(property_127));
    property_127[0] = 0x7fu;
    check_int_eq(flowie_mqtt_property_block_parse(
                     (flowie_mqtt_span_t){property_127, sizeof(property_127)}, &block, &consumed),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(property_127));
    check_size_eq(block.values.size, 127u);

    memset(property_128, 0u, sizeof(property_128));
    property_128[0] = 0x80u;
    property_128[1] = 0x01u;
    block = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_property_block_parse(
                     (flowie_mqtt_span_t){property_128, sizeof(property_128)}, &block, &consumed),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(property_128));
    check_size_eq(block.values.size, 128u);

    check_packet_result(incomplete, sizeof(incomplete), FLOWIE_MQTT_VERSION_5,
                        FLOWIE_MQTT_PARSE_NEED_MORE);
    check_packet_result(noncanonical, sizeof(noncanonical), FLOWIE_MQTT_VERSION_5,
                        FLOWIE_MQTT_PARSE_MALFORMED);
    check_packet_result(five_byte, sizeof(five_byte), FLOWIE_MQTT_VERSION_5,
                        FLOWIE_MQTT_PARSE_MALFORMED);
  }

  it("MQTT-PROTO-008 enforces packet-size boundaries at 0 1 127 128 and the MQTT maximum") {
    static const uint8_t remaining_0[] = {0xc0u, 0x00u};
    static const uint8_t remaining_1[] = {0x30u, 0x01u, 0x00u};
    static const uint8_t remaining_127_header[] = {0x30u, 0x7fu};
    static const uint8_t remaining_128_header[] = {0x30u, 0x80u, 0x01u};
    static const uint8_t maximum_header[] = {0x30u, 0xffu, 0xffu, 0xffu, 0x7fu};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    size_t consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(remaining_0, sizeof(remaining_0), &options, &packet,
                                          &consumed, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(remaining_0));
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(remaining_1, sizeof(remaining_1), &options, &packet,
                                          &consumed, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(remaining_1));

    options.max_packet_size = sizeof(remaining_127_header) + 127u;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(remaining_127_header, sizeof(remaining_127_header),
                                          &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
    check_size_eq(consumed, 0u);
    --options.max_packet_size;
    check_int_eq(flowie_mqtt_packet_parse(remaining_127_header, sizeof(remaining_127_header),
                                          &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_TOO_LARGE);

    options.max_packet_size = sizeof(remaining_128_header) + 128u;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(remaining_128_header, sizeof(remaining_128_header),
                                          &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
    check_size_eq(consumed, 0u);
    --options.max_packet_size;
    check_int_eq(flowie_mqtt_packet_parse(remaining_128_header, sizeof(remaining_128_header),
                                          &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_TOO_LARGE);

    options.max_packet_size = FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(maximum_header, sizeof(maximum_header), &options, &packet,
                                          &consumed, NULL),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
    check_size_eq(consumed, 0u);
    options.max_packet_size = FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE + 1u;
    check_int_eq(flowie_mqtt_packet_parse(maximum_header, sizeof(maximum_header), &options, &packet,
                                          &consumed, NULL),
                 FLOWIE_MQTT_PARSE_INVALID_ARGUMENT);
  }

  it("MQTT-PROTO-010 accepts only matching MQTT protocol names and levels") {
    static const struct {
      const char *name;
      size_t name_size;
      uint8_t level;
      int expected;
    } cases[] = {{"MQIsdp", 6u, FLOWIE_MQTT_VERSION_3_1, FLOWIE_MQTT_PARSE_OK},
                 {"MQIsdp", 6u, FLOWIE_MQTT_VERSION_3_1_1,
                  FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
                 {"MQIsdp", 6u, FLOWIE_MQTT_VERSION_5, FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
                 {"MQTT", 4u, FLOWIE_MQTT_VERSION_3_1, FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
                 {"MQTT", 4u, FLOWIE_MQTT_VERSION_3_1_1, FLOWIE_MQTT_PARSE_OK},
                 {"MQTT", 4u, FLOWIE_MQTT_VERSION_5, FLOWIE_MQTT_PARSE_OK}};
    uint8_t bytes[32];
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
      flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
      flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
      const size_t size = build_connect_dialect(bytes, sizeof(bytes), cases[i].name,
                                                cases[i].name_size, cases[i].level);
      check_int_eq(flowie_mqtt_packet_parse(bytes, size, &options, &packet, NULL, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      check_int_eq(flowie_mqtt_connect_parse(&packet, &connect), cases[i].expected);
      if (cases[i].expected == FLOWIE_MQTT_PARSE_OK)
        check_int_eq(connect.version, cases[i].level);
    }
  }

  it("MQTT-PROTO-009 round trips every supported packet type in every MQTT level") {
    static const uint8_t connect_v31[] = {0x10u, 0x11u, 0x00u, 0x06u, 'M',   'Q',   'I',
                                          's',   'd',   'p',   0x03u, 0x02u, 0x00u, 0x3cu,
                                          0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t connect_v311[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u,
                                           0x02u, 0x00u, 0x3cu, 0x00u, 0x03u, 'c', 'l', 'i'};
    static const uint8_t connect_v5[] = {0x10u, 0x12u, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x05u,
                                         0x02u, 0x00u, 0x3cu, 0x02u, 0x19u, 0x01u, 0x00u, 0x03u,
                                         'c',   'l',   'i'};
    static const uint8_t connack_v3x[] = {0x20u, 0x02u, 0x00u, 0x00u};
    static const uint8_t publish_v3x[] = {0x32u, 0x06u, 0x00u, 0x01u,
                                          't',   0x00u, 0x01u, 'x'};
    static const uint8_t puback_v3x[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubrec_v3x[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubrel_v3x[] = {0x62u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubcomp_v3x[] = {0x70u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscribe_v3x[] = {0x82u, 0x06u, 0x00u, 0x01u,
                                            0x00u, 0x01u, 't',   0x00u};
    static const uint8_t suback_v3x[] = {0x90u, 0x03u, 0x00u, 0x01u, 0x00u};
    static const uint8_t unsubscribe_v3x[] = {0xa2u, 0x05u, 0x00u, 0x01u,
                                              0x00u, 0x01u, 't'};
    static const uint8_t unsuback_v3x[] = {0xb0u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pingreq[] = {0xc0u, 0x00u};
    static const uint8_t pingresp[] = {0xd0u, 0x00u};
    static const uint8_t disconnect[] = {0xe0u, 0x00u};
    static const uint8_t connack_v5[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
    static const uint8_t publish_v5[] = {0x32u, 0x0bu, 0x00u, 0x01u, 't', 0x00u, 0x01u,
                                         0x04u, 0x03u, 0x00u, 0x01u, 'x', 'p'};
    static const uint8_t puback_v5[] = {0x40u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubrec_v5[] = {0x50u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubrel_v5[] = {0x62u, 0x02u, 0x00u, 0x01u};
    static const uint8_t pubcomp_v5[] = {0x70u, 0x02u, 0x00u, 0x01u};
    static const uint8_t subscribe_v5[] = {0x82u, 0x09u, 0x00u, 0x01u, 0x02u, 0x0bu,
                                           0x01u, 0x00u, 0x01u, 't',   0x00u};
    static const uint8_t suback_v5[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t unsubscribe_v5[] = {0xa2u, 0x0du, 0x00u, 0x01u, 0x07u,
                                             0x26u, 0x00u, 0x01u, 'k',   0x00u,
                                             0x01u, 'v',   0x00u, 0x01u, 't'};
    static const uint8_t unsuback_v5[] = {0xb0u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
    static const uint8_t auth_v5[] = {0xf0u, 0x00u};
    static const round_trip_wire_t common_v3x[] = {
        {connack_v3x, sizeof(connack_v3x)},       {publish_v3x, sizeof(publish_v3x)},
        {puback_v3x, sizeof(puback_v3x)},         {pubrec_v3x, sizeof(pubrec_v3x)},
        {pubrel_v3x, sizeof(pubrel_v3x)},         {pubcomp_v3x, sizeof(pubcomp_v3x)},
        {subscribe_v3x, sizeof(subscribe_v3x)},   {suback_v3x, sizeof(suback_v3x)},
        {unsubscribe_v3x, sizeof(unsubscribe_v3x)},
        {unsuback_v3x, sizeof(unsuback_v3x)},     {pingreq, sizeof(pingreq)},
        {pingresp, sizeof(pingresp)},             {disconnect, sizeof(disconnect)}};
    static const round_trip_wire_t mqtt5[] = {
        {connect_v5, sizeof(connect_v5)},         {connack_v5, sizeof(connack_v5)},
        {publish_v5, sizeof(publish_v5)},         {puback_v5, sizeof(puback_v5)},
        {pubrec_v5, sizeof(pubrec_v5)},           {pubrel_v5, sizeof(pubrel_v5)},
        {pubcomp_v5, sizeof(pubcomp_v5)},         {subscribe_v5, sizeof(subscribe_v5)},
        {suback_v5, sizeof(suback_v5)},           {unsubscribe_v5, sizeof(unsubscribe_v5)},
        {unsuback_v5, sizeof(unsuback_v5)},       {pingreq, sizeof(pingreq)},
        {pingresp, sizeof(pingresp)},             {disconnect, sizeof(disconnect)},
        {auth_v5, sizeof(auth_v5)}};
    static const flowie_mqtt_version_t versions_3x[] = {FLOWIE_MQTT_VERSION_3_1,
                                                        FLOWIE_MQTT_VERSION_3_1_1};
    const round_trip_wire_t connect_3x[] = {{connect_v31, sizeof(connect_v31)},
                                            {connect_v311, sizeof(connect_v311)}};
    for (size_t version_index = 0u;
         version_index < sizeof(versions_3x) / sizeof(versions_3x[0]); ++version_index) {
      check_canonical_round_trip(&connect_3x[version_index], versions_3x[version_index]);
      for (size_t packet_index = 0u;
           packet_index < sizeof(common_v3x) / sizeof(common_v3x[0]); ++packet_index)
        check_canonical_round_trip(&common_v3x[packet_index], versions_3x[version_index]);
    }
    for (size_t packet_index = 0u; packet_index < sizeof(mqtt5) / sizeof(mqtt5[0]); ++packet_index)
      check_canonical_round_trip(&mqtt5[packet_index], FLOWIE_MQTT_VERSION_5);
  }

  it("MQTT-PROTO-008 keeps incomplete frames pending and consumes only the first sticky packet") {
    static const uint8_t complete[] = {0x30u, 0x05u, 0x00u, 0x01u, 't', 0x00u, 'x'};
    static const uint8_t sticky[] = {0x30u, 0x05u, 0x00u, 0x01u, 't', 0x00u, 'x', 0xc0u, 0x00u};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    size_t consumed = SIZE_MAX;
    options.version = FLOWIE_MQTT_VERSION_5;
    for (size_t prefix = 1u; prefix < sizeof(complete); ++prefix) {
      packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
      consumed = SIZE_MAX;
      check_int_eq(flowie_mqtt_packet_parse(complete, prefix, &options, &packet, &consumed, NULL),
                   FLOWIE_MQTT_PARSE_NEED_MORE);
      check_size_eq(consumed, 0u);
    }
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(sticky, sizeof(sticky), &options, &packet, &consumed,
                                          NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, sizeof(complete));
    check_size_eq(packet.packet.size, sizeof(complete));
    check(packet.packet.data == sticky);
  }

  it("MQTT-PROTO-008 distinguishes every configured fragmentation and coalescing boundary") {
    enum { FRAGMENT_PAYLOAD_SIZE = 240, FRAGMENT_WIRE_CAPACITY = 320 };
    static const size_t fixed_boundaries[] = {0u, 1u, 127u, 128u};
    uint8_t payload[FRAGMENT_PAYLOAD_SIZE];
    uint8_t wire[FRAGMENT_WIRE_CAPACITY];
    uint8_t sticky[FRAGMENT_WIRE_CAPACITY + 2u];
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    size_t written = 0u;
    size_t consumed = SIZE_MAX;
    memset(payload, 'x', sizeof(payload));
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = literal_span("fragment/boundary");
    publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload)};
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, wire, sizeof(wire), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check(written > 128u);
    options.version = FLOWIE_MQTT_VERSION_5;
    options.max_packet_size = written;
    for (size_t i = 0u; i < sizeof(fixed_boundaries) / sizeof(fixed_boundaries[0]); ++i) {
      packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
      consumed = SIZE_MAX;
      check(fixed_boundaries[i] < written);
      check_int_eq(flowie_mqtt_packet_parse(wire, fixed_boundaries[i], &options, &packet,
                                            &consumed, NULL),
                   FLOWIE_MQTT_PARSE_NEED_MORE);
      check_size_eq(consumed, 0u);
    }
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(wire, written - 1u, &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
    check_size_eq(consumed, 0u);
    check_int_eq(flowie_mqtt_packet_parse(wire, written, &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, written);

    options.max_packet_size = written - 1u;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(wire, written, &options, &packet, &consumed, NULL),
                 FLOWIE_MQTT_PARSE_TOO_LARGE);
    check_size_eq(consumed, 0u);

    memcpy(sticky, wire, written);
    sticky[written] = 0xc0u;
    sticky[written + 1u] = 0x00u;
    options.max_packet_size = written;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    consumed = SIZE_MAX;
    check_int_eq(flowie_mqtt_packet_parse(sticky, written + 2u, &options, &packet, &consumed,
                                          NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(consumed, written);
    check(packet.packet.data == sticky);
  }

  it("MQTT-PROTO-002/006 rejects unknown properties and truncated property values") {
    static const uint8_t unknown[] = {0x02u, 0x7fu, 0x00u};
    static const uint8_t truncated_utf8[] = {0x05u, 0x03u, 0x00u, 0x02u, 0xc0u, 0x80u};
    static const uint8_t truncated_value[] = {0x04u, 0x03u, 0x00u, 0x02u, 'x'};
    static const uint8_t truncated_binary[] = {0x04u, 0x09u, 0x00u, 0x02u, 'x'};
    flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    check_int_eq(flowie_mqtt_property_block_parse(
                     (flowie_mqtt_span_t){unknown, sizeof(unknown)}, &block, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_init(&block, &iterator), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);

    block = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    iterator = (flowie_mqtt_property_iterator_t)FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    property = (flowie_mqtt_property_view_t)FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    check_int_eq(flowie_mqtt_property_block_parse(
                     (flowie_mqtt_span_t){truncated_utf8, sizeof(truncated_utf8)}, &block, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_init(&block, &iterator), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);

    block = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    iterator = (flowie_mqtt_property_iterator_t)FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    property = (flowie_mqtt_property_view_t)FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    check_int_eq(flowie_mqtt_property_block_parse(
                     (flowie_mqtt_span_t){truncated_value, sizeof(truncated_value)}, &block, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_init(&block, &iterator), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                 FLOWIE_MQTT_PARSE_MALFORMED);

    block = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    iterator = (flowie_mqtt_property_iterator_t)FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    property = (flowie_mqtt_property_view_t)FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    check_int_eq(flowie_mqtt_property_block_parse(
                     (flowie_mqtt_span_t){truncated_binary, sizeof(truncated_binary)}, &block,
                     NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_init(&block, &iterator), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                 FLOWIE_MQTT_PARSE_MALFORMED);
  }

  it("MQTT-PROTO-002/003 enforces the MQTT 5 packet type by property placement matrix") {
    static const uint8_t payload_format[] = {0x01u, 0x00u};
    static const uint8_t message_expiry[] = {0x02u, 0x00u, 0x00u, 0x00u, 0x01u};
    static const uint8_t content_type[] = {0x03u, 0x00u, 0x01u, 'x'};
    static const uint8_t response_topic[] = {0x08u, 0x00u, 0x01u, 't'};
    static const uint8_t correlation_data[] = {0x09u, 0x00u, 0x01u, 'x'};
    static const uint8_t subscription_identifier[] = {0x0bu, 0x01u};
    static const uint8_t session_expiry[] = {0x11u, 0x00u, 0x00u, 0x00u, 0x01u};
    static const uint8_t assigned_client_identifier[] = {0x12u, 0x00u, 0x01u, 'c'};
    static const uint8_t server_keep_alive[] = {0x13u, 0x00u, 0x01u};
    static const uint8_t authentication_method[] = {0x15u, 0x00u, 0x01u, 'm'};
    static const uint8_t authentication_method_data[] = {0x15u, 0x00u, 0x01u, 'm',
                                                         0x16u, 0x00u, 0x01u, 'd'};
    static const uint8_t duplicate_authentication_data[] = {
        0x15u, 0x00u, 0x01u, 'm', 0x16u, 0x00u, 0x01u, 'a',
        0x16u, 0x00u, 0x01u, 'b'};
    static const uint8_t request_problem_information[] = {0x17u, 0x01u};
    static const uint8_t will_delay[] = {0x18u, 0x00u, 0x00u, 0x00u, 0x01u};
    static const uint8_t request_response_information[] = {0x19u, 0x01u};
    static const uint8_t response_information[] = {0x1au, 0x00u, 0x01u, 'r'};
    static const uint8_t server_reference[] = {0x1cu, 0x00u, 0x01u, 's'};
    static const uint8_t reason_string[] = {0x1fu, 0x00u, 0x01u, 'r'};
    static const uint8_t receive_maximum[] = {0x21u, 0x00u, 0x01u};
    static const uint8_t topic_alias_maximum[] = {0x22u, 0x00u, 0x00u};
    static const uint8_t topic_alias[] = {0x23u, 0x00u, 0x01u};
    static const uint8_t maximum_qos[] = {0x24u, 0x01u};
    static const uint8_t retain_available[] = {0x25u, 0x01u};
    static const uint8_t user_property[] = {0x26u, 0x00u, 0x01u, 'k',
                                            0x00u, 0x01u, 'v'};
    static const uint8_t maximum_packet_size[] = {0x27u, 0x00u, 0x00u, 0x00u, 0x01u};
    static const uint8_t wildcard_available[] = {0x28u, 0x01u};
    static const uint8_t subscription_identifier_available[] = {0x29u, 0x01u};
    static const uint8_t shared_available[] = {0x2au, 0x01u};
    const uint32_t ack_contexts =
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBACK) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBREC) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBREL) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBCOMP) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_SUBACK) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_UNSUBACK);
    const uint32_t user_contexts =
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_SUBSCRIBE) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_UNSUBSCRIBE) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK) | ack_contexts |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_DISCONNECT) |
        PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_AUTH);
    const property_spec_t properties[] = {
        {payload_format, sizeof(payload_format),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH)},
        {message_expiry, sizeof(message_expiry),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH)},
        {content_type, sizeof(content_type), PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL) |
                                                     PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH)},
        {response_topic, sizeof(response_topic),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH)},
        {correlation_data, sizeof(correlation_data),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH)},
        {subscription_identifier, sizeof(subscription_identifier),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_SUBSCRIBE)},
        {session_expiry, sizeof(session_expiry),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_DISCONNECT)},
        {assigned_client_identifier, sizeof(assigned_client_identifier),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {server_keep_alive, sizeof(server_keep_alive),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {authentication_method, sizeof(authentication_method),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_AUTH)},
        {authentication_method_data, sizeof(authentication_method_data),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_AUTH)},
        {request_problem_information, sizeof(request_problem_information),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT)},
        {will_delay, sizeof(will_delay), PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_WILL)},
        {request_response_information, sizeof(request_response_information),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT)},
        {response_information, sizeof(response_information),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {server_reference, sizeof(server_reference),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_DISCONNECT)},
        {reason_string, sizeof(reason_string),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK) | ack_contexts |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_DISCONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_AUTH)},
        {receive_maximum, sizeof(receive_maximum),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {topic_alias_maximum, sizeof(topic_alias_maximum),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {topic_alias, sizeof(topic_alias), PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_PUBLISH)},
        {maximum_qos, sizeof(maximum_qos), PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {retain_available, sizeof(retain_available),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {user_property, sizeof(user_property), user_contexts},
        {maximum_packet_size, sizeof(maximum_packet_size),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNECT) |
             PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {wildcard_available, sizeof(wildcard_available),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {subscription_identifier_available, sizeof(subscription_identifier_available),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)},
        {shared_available, sizeof(shared_available),
         PROPERTY_CONTEXT_BIT(PROPERTY_CONTEXT_CONNACK)}};
    uint8_t output[128];
    uint8_t repeated[32];
    for (size_t property_index = 0u;
         property_index < sizeof(properties) / sizeof(properties[0]); ++property_index) {
      for (property_context_t context = PROPERTY_CONTEXT_CONNECT;
           context < PROPERTY_CONTEXT_COUNT; context = (property_context_t)(context + 1)) {
        const int allowed =
            (properties[property_index].allowed_contexts & PROPERTY_CONTEXT_BIT(context)) != 0u;
        size_t written = SIZE_MAX;
        memset(output, 0xa5, sizeof(output));
        check_int_eq(encode_property_context(context, properties[property_index].values,
                                             properties[property_index].value_size, output,
                                             sizeof(output), &written),
                     allowed ? FLOWIE_MQTT_PARSE_OK : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
        if (allowed) {
          const uint8_t identifier = properties[property_index].values[0];
          const int repeatable =
              identifier == FLOWIE_MQTT_PROPERTY_USER_PROPERTY ||
              (identifier == FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER &&
               context == PROPERTY_CONTEXT_PUBLISH);
          check(written > 0u);
          check_int_eq(parse_property_context(context, output, written), FLOWIE_MQTT_PARSE_OK);
          check(properties[property_index].value_size * 2u <= sizeof(repeated));
          memcpy(repeated, properties[property_index].values, properties[property_index].value_size);
          memcpy(repeated + properties[property_index].value_size,
                 properties[property_index].values, properties[property_index].value_size);
          check_int_eq(encode_property_context(context, repeated,
                                               properties[property_index].value_size * 2u, output,
                                               sizeof(output), &written),
                       repeatable ? FLOWIE_MQTT_PARSE_OK : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
          if (repeatable)
            check_int_eq(parse_property_context(context, output, written), FLOWIE_MQTT_PARSE_OK);
          else
            check_size_eq(written, 0u);
        } else {
          check_size_eq(written, 0u);
          for (size_t i = 0u; i < sizeof(output); ++i) check_uint_eq(output[i], 0xa5u);
        }
      }
    }
    {
      static const property_context_t authentication_contexts[] = {
          PROPERTY_CONTEXT_CONNECT, PROPERTY_CONTEXT_CONNACK, PROPERTY_CONTEXT_AUTH};
      for (size_t i = 0u;
           i < sizeof(authentication_contexts) / sizeof(authentication_contexts[0]); ++i) {
        size_t written = SIZE_MAX;
        memset(output, 0xa5, sizeof(output));
        check_int_eq(encode_property_context(authentication_contexts[i],
                                             duplicate_authentication_data,
                                             sizeof(duplicate_authentication_data), output,
                                             sizeof(output), &written),
                     FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
        check_size_eq(written, 0u);
      }
    }
  }

  it("MQTT-PROTO-003 bounds repeated User Property by the packet output limit") {
    static const uint8_t user_property[] = {0x26u, 0x00u, 0x01u, 'k',
                                            0x00u, 0x01u, 'v'};
    uint8_t repeated[64];
    uint8_t output[128];
    size_t written = SIZE_MAX;
    memcpy(repeated, user_property, sizeof(user_property));
    memcpy(repeated + sizeof(user_property), user_property, sizeof(user_property));
    check_int_eq(encode_property_context(PROPERTY_CONTEXT_PUBLISH, repeated,
                                         sizeof(user_property) * 2u, output, sizeof(output),
                                         &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(parse_property_context(PROPERTY_CONTEXT_PUBLISH, output, written),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(encode_property_context(PROPERTY_CONTEXT_PUBLISH, repeated,
                                         sizeof(user_property) * 2u, output, written - 1u,
                                         &written),
                 FLOWIE_MQTT_PARSE_TOO_LARGE);
    check_size_eq(written, 0u);
  }

  it("MQTT-PROTO-006 rejects invalid UTF-8 in every string property type") {
    static const uint8_t string_properties[] = {
        FLOWIE_MQTT_PROPERTY_CONTENT_TYPE,
        FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC,
        FLOWIE_MQTT_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER,
        FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD,
        FLOWIE_MQTT_PROPERTY_RESPONSE_INFORMATION,
        FLOWIE_MQTT_PROPERTY_SERVER_REFERENCE,
        FLOWIE_MQTT_PROPERTY_REASON_STRING};
    uint8_t encoded[] = {0x04u, 0x00u, 0x00u, 0x01u, 0x00u};
    for (size_t i = 0u; i < sizeof(string_properties); ++i) {
      flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
      flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
      flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
      encoded[1] = string_properties[i];
      check_int_eq(flowie_mqtt_property_block_parse(
                       (flowie_mqtt_span_t){encoded, sizeof(encoded)}, &block, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      check_int_eq(flowie_mqtt_property_iterator_init(&block, &iterator), FLOWIE_MQTT_PARSE_OK);
      check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                   FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    }

    {
      static const uint8_t invalid_user_property[] = {0x07u, 0x26u, 0x00u, 0x01u, 'k',
                                                      0x00u, 0x01u, 0x00u};
      flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
      flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
      flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
      check_int_eq(flowie_mqtt_property_block_parse(
                       (flowie_mqtt_span_t){invalid_user_property, sizeof(invalid_user_property)},
                       &block, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      check_int_eq(flowie_mqtt_property_iterator_init(&block, &iterator), FLOWIE_MQTT_PARSE_OK);
      check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                   FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    }
  }

  it("MQTT-PROTO-006 rejects every prohibited UTF-8 class and truncated packet string span") {
    static const uint8_t overlong[] = {0xc0u, 0x80u};
    static const uint8_t truncated_utf8[] = {0xe2u, 0x82u};
    static const uint8_t nul[] = {0x00u};
    static const uint8_t surrogate[] = {0xedu, 0xa0u, 0x80u};
    static const uint8_t bmp_noncharacter[] = {0xefu, 0xb7u, 0x90u};
    static const uint8_t plane_noncharacter[] = {0xf4u, 0x8fu, 0xbfu, 0xbfu};
    static const uint8_t truncated_span[] = {0x30u, 0x03u, 0x00u, 0x02u, 'a'};
    static const struct {
      const uint8_t *bytes;
      size_t size;
    } invalid[] = {{overlong, sizeof(overlong)},
                   {truncated_utf8, sizeof(truncated_utf8)},
                   {nul, sizeof(nul)},
                   {surrogate, sizeof(surrogate)},
                   {bmp_noncharacter, sizeof(bmp_noncharacter)},
                   {plane_noncharacter, sizeof(plane_noncharacter)}};
    static uint8_t utf8_boundary[FLOWIE_MQTT_MAX_UTF8_SIZE + 1u];
    uint8_t wire[32];
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    options.version = FLOWIE_MQTT_VERSION_5;
    for (size_t i = 0u; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
      flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
      flowie_mqtt_publish_view_t publish;
      flowie_mqtt_publish_view_t before;
      const size_t wire_size =
          build_v5_publish_topic(wire, sizeof(wire), invalid[i].bytes, invalid[i].size);
      check_int_eq(flowie_mqtt_packet_parse(wire, wire_size, &options, &packet, NULL, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      memset(&publish, 0xa5, sizeof(publish));
      publish.size = sizeof(publish);
      publish.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
      before = publish;
      check_int_eq(flowie_mqtt_publish_parse(&packet, &publish),
                   FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
      check_mem_eq(&publish, &before, sizeof(publish));
    }
    {
      flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
      flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
      check_int_eq(flowie_mqtt_packet_parse(truncated_span, sizeof(truncated_span), &options,
                                            &packet, NULL, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      check_int_eq(flowie_mqtt_publish_parse(&packet, &publish), FLOWIE_MQTT_PARSE_MALFORMED);
    }
    memset(utf8_boundary, 'a', sizeof(utf8_boundary));
    check_true(flowie_mqtt_topic_name_validate(
        (flowie_mqtt_span_t){utf8_boundary, FLOWIE_MQTT_MAX_UTF8_SIZE}));
    check_false(flowie_mqtt_topic_name_validate(
        (flowie_mqtt_span_t){utf8_boundary, FLOWIE_MQTT_MAX_UTF8_SIZE + 1u}));
  }

  it("MQTT-PROTO-002/003/004 enforces CONNECT property placement repetition and scalar ranges") {
    static const uint8_t receive_maximum[] = {0x21u, 0x00u, 0x01u};
    static const uint8_t duplicate_receive_maximum[] = {0x21u, 0x00u, 0x01u,
                                                         0x21u, 0x00u, 0x02u};
    static const uint8_t zero_receive_maximum[] = {0x21u, 0x00u, 0x00u};
    static const uint8_t zero_maximum_packet_size[] = {0x27u, 0x00u, 0x00u, 0x00u, 0x00u};
    static const uint8_t forbidden_content_type[] = {0x03u, 0x00u, 0x01u, 'x'};
    static const uint8_t repeated_user_property[] = {
        0x26u, 0x00u, 0x01u, 'a', 0x00u, 0x01u, '1',
        0x26u, 0x00u, 0x01u, 'b', 0x00u, 0x01u, '2'};
    static const uint8_t authentication_data_only[] = {0x16u, 0x00u, 0x01u, 'x'};
    static const property_case_t cases[] = {
        {receive_maximum, sizeof(receive_maximum), FLOWIE_MQTT_PARSE_OK},
        {duplicate_receive_maximum, sizeof(duplicate_receive_maximum),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {zero_receive_maximum, sizeof(zero_receive_maximum), FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {zero_maximum_packet_size, sizeof(zero_maximum_packet_size),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {forbidden_content_type, sizeof(forbidden_content_type),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {repeated_user_property, sizeof(repeated_user_property), FLOWIE_MQTT_PARSE_OK},
        {authentication_data_only, sizeof(authentication_data_only),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR}};
    uint8_t bytes[64];
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    options.version = FLOWIE_MQTT_VERSION_5;
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
      flowie_mqtt_connect_view_t connect;
      flowie_mqtt_connect_view_t before;
      size_t size = build_v5_connect(bytes, sizeof(bytes), cases[i].values, cases[i].value_size);
      check_int_eq(flowie_mqtt_packet_parse(bytes, size, &options, &packet, NULL, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      memset(&connect, 0xa5, sizeof(connect));
      connect.size = sizeof(connect);
      connect.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
      before = connect;
      check_int_eq(flowie_mqtt_connect_parse(&packet, &connect), cases[i].expected);
      if (cases[i].expected != FLOWIE_MQTT_PARSE_OK)
        check_mem_eq(&connect, &before, sizeof(connect));
    }
  }

  it("MQTT-PROTO-002/003/004 enforces PUBLISH property placement repetition and scalar ranges") {
    static const uint8_t content_type[] = {0x03u, 0x00u, 0x01u, 'x'};
    static const uint8_t duplicate_content_type[] = {0x03u, 0x00u, 0x01u, 'x',
                                                     0x03u, 0x00u, 0x01u, 'y'};
    static const uint8_t zero_topic_alias[] = {0x23u, 0x00u, 0x00u};
    static const uint8_t invalid_payload_format[] = {0x01u, 0x02u};
    static const uint8_t forbidden_reason_string[] = {0x1fu, 0x00u, 0x01u, 'x'};
    static const uint8_t repeated_user_property[] = {
        0x26u, 0x00u, 0x01u, 'a', 0x00u, 0x01u, '1',
        0x26u, 0x00u, 0x01u, 'b', 0x00u, 0x01u, '2'};
    static const property_case_t cases[] = {
        {content_type, sizeof(content_type), FLOWIE_MQTT_PARSE_OK},
        {duplicate_content_type, sizeof(duplicate_content_type),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {zero_topic_alias, sizeof(zero_topic_alias), FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {invalid_payload_format, sizeof(invalid_payload_format),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {forbidden_reason_string, sizeof(forbidden_reason_string),
         FLOWIE_MQTT_PARSE_PROTOCOL_ERROR},
        {repeated_user_property, sizeof(repeated_user_property), FLOWIE_MQTT_PARSE_OK}};
    uint8_t bytes[64];
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    options.version = FLOWIE_MQTT_VERSION_5;
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
      flowie_mqtt_publish_view_t publish;
      flowie_mqtt_publish_view_t before;
      size_t size = build_v5_publish(bytes, sizeof(bytes), cases[i].values, cases[i].value_size);
      check_int_eq(flowie_mqtt_packet_parse(bytes, size, &options, &packet, NULL, NULL),
                   FLOWIE_MQTT_PARSE_OK);
      memset(&publish, 0xa5, sizeof(publish));
      publish.size = sizeof(publish);
      publish.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
      before = publish;
      check_int_eq(flowie_mqtt_publish_parse(&packet, &publish), cases[i].expected);
      if (cases[i].expected != FLOWIE_MQTT_PARSE_OK)
        check_mem_eq(&publish, &before, sizeof(publish));
    }
  }

  it("MQTT-PROTO-007 accepts exactly the MQTT 5 reason codes allowed for each control packet") {
    static const flowie_mqtt_packet_type_t types[] = {
        FLOWIE_MQTT_PACKET_CONNACK,     FLOWIE_MQTT_PACKET_PUBACK,
        FLOWIE_MQTT_PACKET_PUBREC,      FLOWIE_MQTT_PACKET_PUBREL,
        FLOWIE_MQTT_PACKET_PUBCOMP,     FLOWIE_MQTT_PACKET_SUBACK,
        FLOWIE_MQTT_PACKET_UNSUBACK,    FLOWIE_MQTT_PACKET_DISCONNECT,
        FLOWIE_MQTT_PACKET_AUTH};
    uint8_t output[16];
    for (size_t type_index = 0u; type_index < sizeof(types) / sizeof(types[0]); ++type_index) {
      for (unsigned int candidate = 0u; candidate <= UINT8_MAX; ++candidate) {
        const uint8_t reason = (uint8_t)candidate;
        const int valid = mqtt5_reason_valid(types[type_index], reason);
        flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
        size_t written = SIZE_MAX;
        memset(output, 0xa5, sizeof(output));
        control.version = FLOWIE_MQTT_VERSION_5;
        control.type = types[type_index];
        if (control.type == FLOWIE_MQTT_PACKET_CONNACK ||
            control.type == FLOWIE_MQTT_PACKET_DISCONNECT ||
            control.type == FLOWIE_MQTT_PACKET_AUTH) {
          control.reason_code = reason;
        } else if (control.type == FLOWIE_MQTT_PACKET_SUBACK ||
                   control.type == FLOWIE_MQTT_PACKET_UNSUBACK) {
          control.packet_id = 1u;
          control.reason_codes = (flowie_mqtt_span_t){&reason, 1u};
        } else {
          control.packet_id = 1u;
          control.reason_code = reason;
        }
        check_int_eq(flowie_mqtt_control_packet_encode(&control, output, sizeof(output), &written),
                     valid ? FLOWIE_MQTT_PARSE_OK : FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
        if (valid) {
          check(written > 0u);
        } else {
          check_size_eq(written, 0u);
          for (size_t i = 0u; i < sizeof(output); ++i) check_uint_eq(output[i], 0xa5u);
        }
      }
    }
  }
}
