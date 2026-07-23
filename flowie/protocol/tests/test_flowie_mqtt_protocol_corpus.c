#include "flowie_mqtt_protocol.h"

#include "tinytest.h"

#include <stdint.h>
#include <string.h>

typedef struct corpus_packet_s {
  const uint8_t *bytes;
  size_t size;
  flowie_mqtt_version_t version;
} corpus_packet_t;

static const uint8_t connect_v31[] = {0x10u, 0x11u, 0x00u, 0x06u, 'M',   'Q',   'I',
                                      's',   'd',   'p',   0x03u, 0x02u, 0x00u, 0x3cu,
                                      0x00u, 0x03u, 'c',   'l',   'i'};
static const uint8_t connect_v311[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u,
                                       0x02u, 0x00u, 0x3cu, 0x00u, 0x03u, 'c', 'l', 'i'};
static const uint8_t connect_v5[] = {0x10u, 0x12u, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x05u,
                                     0x02u, 0x00u, 0x3cu, 0x02u, 0x19u, 0x01u, 0x00u, 0x03u,
                                     'c',   'l',   'i'};
static const uint8_t publish_v311[] = {0x30u, 0x04u, 0x00u, 0x01u, 't', 'x'};
static const uint8_t publish_v5[] = {0x30u, 0x05u, 0x00u, 0x01u, 't', 0x00u, 'x'};
static const uint8_t subscribe_v5[] = {0x82u, 0x07u, 0x00u, 0x01u, 0x00u,
                                       0x00u, 0x01u, 't',   0x00u};
static const uint8_t unsubscribe_v5[] = {0xa2u, 0x06u, 0x00u, 0x01u,
                                         0x00u, 0x00u, 0x01u, 't'};
static const uint8_t pingreq_v5[] = {0xc0u, 0x00u};
static const uint8_t puback_v5[] = {0x40u, 0x02u, 0x00u, 0x01u};
static const uint8_t disconnect_v5[] = {0xe0u, 0x00u};
static const uint8_t auth_v5[] = {0xf0u, 0x00u};

static const corpus_packet_t corpus[] = {
    {connect_v31, sizeof(connect_v31), FLOWIE_MQTT_VERSION_UNSPECIFIED},
    {connect_v311, sizeof(connect_v311), FLOWIE_MQTT_VERSION_UNSPECIFIED},
    {connect_v5, sizeof(connect_v5), FLOWIE_MQTT_VERSION_UNSPECIFIED},
    {publish_v311, sizeof(publish_v311), FLOWIE_MQTT_VERSION_3_1_1},
    {publish_v5, sizeof(publish_v5), FLOWIE_MQTT_VERSION_5},
    {subscribe_v5, sizeof(subscribe_v5), FLOWIE_MQTT_VERSION_5},
    {unsubscribe_v5, sizeof(unsubscribe_v5), FLOWIE_MQTT_VERSION_5},
    {pingreq_v5, sizeof(pingreq_v5), FLOWIE_MQTT_VERSION_5},
    {puback_v5, sizeof(puback_v5), FLOWIE_MQTT_VERSION_5},
    {disconnect_v5, sizeof(disconnect_v5), FLOWIE_MQTT_VERSION_5},
    {auth_v5, sizeof(auth_v5), FLOWIE_MQTT_VERSION_5}};

static void initialize_packet_sentinel(flowie_mqtt_packet_view_t *packet) {
  memset(packet, 0xa5, sizeof(*packet));
  packet->size = sizeof(*packet);
  packet->abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
}

static void check_typed_failure_preserves_output(const flowie_mqtt_packet_view_t *packet) {
  int rc;
  if (packet->type == FLOWIE_MQTT_PACKET_CONNECT) {
    flowie_mqtt_connect_view_t value;
    flowie_mqtt_connect_view_t before;
    memset(&value, 0xa5, sizeof(value));
    value.size = sizeof(value);
    value.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
    before = value;
    rc = flowie_mqtt_connect_parse(packet, &value);
    if (rc != FLOWIE_MQTT_PARSE_OK) check_mem_eq(&value, &before, sizeof(value));
  } else if (packet->type == FLOWIE_MQTT_PACKET_PUBLISH) {
    flowie_mqtt_publish_view_t value;
    flowie_mqtt_publish_view_t before;
    memset(&value, 0xa5, sizeof(value));
    value.size = sizeof(value);
    value.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
    before = value;
    rc = flowie_mqtt_publish_parse(packet, &value);
    if (rc != FLOWIE_MQTT_PARSE_OK) check_mem_eq(&value, &before, sizeof(value));
  } else if (packet->type == FLOWIE_MQTT_PACKET_SUBSCRIBE) {
    flowie_mqtt_subscribe_view_t value;
    flowie_mqtt_subscribe_view_t before;
    memset(&value, 0xa5, sizeof(value));
    value.size = sizeof(value);
    value.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
    before = value;
    rc = flowie_mqtt_subscribe_parse(packet, &value);
    if (rc != FLOWIE_MQTT_PARSE_OK) check_mem_eq(&value, &before, sizeof(value));
  } else if (packet->type == FLOWIE_MQTT_PACKET_UNSUBSCRIBE) {
    flowie_mqtt_unsubscribe_view_t value;
    flowie_mqtt_unsubscribe_view_t before;
    memset(&value, 0xa5, sizeof(value));
    value.size = sizeof(value);
    value.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
    before = value;
    rc = flowie_mqtt_unsubscribe_parse(packet, &value);
    if (rc != FLOWIE_MQTT_PARSE_OK) check_mem_eq(&value, &before, sizeof(value));
  } else if (packet->type == FLOWIE_MQTT_PACKET_CONNACK ||
             packet->type == FLOWIE_MQTT_PACKET_PUBACK ||
             packet->type == FLOWIE_MQTT_PACKET_PUBREC ||
             packet->type == FLOWIE_MQTT_PACKET_PUBREL ||
             packet->type == FLOWIE_MQTT_PACKET_PUBCOMP ||
             packet->type == FLOWIE_MQTT_PACKET_SUBACK ||
             packet->type == FLOWIE_MQTT_PACKET_UNSUBACK ||
             packet->type == FLOWIE_MQTT_PACKET_PINGRESP ||
             packet->type == FLOWIE_MQTT_PACKET_DISCONNECT ||
             packet->type == FLOWIE_MQTT_PACKET_AUTH) {
    flowie_mqtt_control_packet_view_t value;
    flowie_mqtt_control_packet_view_t before;
    memset(&value, 0xa5, sizeof(value));
    value.size = sizeof(value);
    value.abi_version = FLOWIE_MQTT_PROTOCOL_ABI_V1;
    before = value;
    rc = flowie_mqtt_control_packet_parse(packet, &value);
    if (rc != FLOWIE_MQTT_PARSE_OK) check_mem_eq(&value, &before, sizeof(value));
  }
}

static int parse_result_known(int rc) {
  return rc == FLOWIE_MQTT_PARSE_OK || rc == FLOWIE_MQTT_PARSE_NEED_MORE ||
         rc == FLOWIE_MQTT_PARSE_MALFORMED || rc == FLOWIE_MQTT_PARSE_PROTOCOL_ERROR ||
         rc == FLOWIE_MQTT_PARSE_TOO_LARGE || rc == FLOWIE_MQTT_PARSE_NO_MEMORY;
}

static int typed_parse_result_known(int rc) {
  return parse_result_known(rc) || rc == FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
}

static void check_success_reencodes(const flowie_mqtt_packet_view_t *packet) {
  enum { REENCODE_CAPACITY = 1024, MAX_ENTRIES = 16 };
  uint8_t encoded[REENCODE_CAPACITY];
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t reparsed = FLOWIE_MQTT_PACKET_VIEW_INIT;
  size_t written = 0u;
  size_t consumed = 0u;
  int rc = FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  if (!packet) return;
  switch (packet->type) {
  case FLOWIE_MQTT_PACKET_CONNECT: {
    flowie_mqtt_connect_view_t decoded = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_mqtt_connect_packet_t rebuilt = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    rc = flowie_mqtt_connect_parse(packet, &decoded);
    check_true(typed_parse_result_known(rc));
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
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
    rc = flowie_mqtt_publish_parse(packet, &decoded);
    check_true(typed_parse_result_known(rc));
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
    rebuilt.version = packet->version;
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
    flowie_mqtt_subscription_t entries[MAX_ENTRIES];
    flowie_mqtt_subscribe_packet_t rebuilt = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
    rc = flowie_mqtt_subscribe_parse(packet, &decoded);
    check_true(typed_parse_result_known(rc));
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
    check_size_le(decoded.entry_count, MAX_ENTRIES);
    if (decoded.entry_count > MAX_ENTRIES) return;
    rc = flowie_mqtt_subscription_iterator_init(packet, &decoded, &iterator);
    check_int_eq(rc, FLOWIE_MQTT_PARSE_OK);
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
    for (size_t i = 0u; i < decoded.entry_count; ++i) {
      rc = flowie_mqtt_subscription_iterator_next(&iterator, &entries[i]);
      check_int_eq(rc, FLOWIE_MQTT_PARSE_OK);
      if (rc != FLOWIE_MQTT_PARSE_OK) return;
    }
    rebuilt.version = packet->version;
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
    flowie_mqtt_span_t filters[MAX_ENTRIES];
    flowie_mqtt_unsubscribe_packet_t rebuilt = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
    rc = flowie_mqtt_unsubscribe_parse(packet, &decoded);
    check_true(typed_parse_result_known(rc));
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
    check_size_le(decoded.filter_count, MAX_ENTRIES);
    if (decoded.filter_count > MAX_ENTRIES) return;
    rc = flowie_mqtt_topic_filter_iterator_init(&decoded, &iterator);
    check_int_eq(rc, FLOWIE_MQTT_PARSE_OK);
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
    for (size_t i = 0u; i < decoded.filter_count; ++i) {
      rc = flowie_mqtt_topic_filter_iterator_next(&iterator, &filters[i]);
      check_int_eq(rc, FLOWIE_MQTT_PARSE_OK);
      if (rc != FLOWIE_MQTT_PARSE_OK) return;
    }
    rebuilt.version = packet->version;
    rebuilt.packet_id = decoded.packet_id;
    rebuilt.properties = decoded.properties.values;
    rebuilt.filters = filters;
    rebuilt.filter_count = decoded.filter_count;
    rc = flowie_mqtt_unsubscribe_packet_encode(&rebuilt, encoded, sizeof(encoded), &written);
    break;
  }
  case FLOWIE_MQTT_PACKET_PINGREQ:
    rc = flowie_mqtt_pingreq_encode(packet->version, encoded, sizeof(encoded), &written);
    break;
  default: {
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_t rebuilt = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    rc = flowie_mqtt_control_packet_parse(packet, &decoded);
    check_true(typed_parse_result_known(rc));
    if (rc != FLOWIE_MQTT_PARSE_OK) return;
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
  if (rc != FLOWIE_MQTT_PARSE_OK) return;
  check(written > 0u);
  options.version = packet->version;
  check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &reparsed, &consumed, NULL),
               FLOWIE_MQTT_PARSE_OK);
  check_size_eq(consumed, written);
  check_int_eq(reparsed.type, packet->type);
}

static void check_parse_invariants(const uint8_t *bytes, size_t size,
                                   flowie_mqtt_version_t version) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet;
  flowie_mqtt_packet_view_t before;
  flowie_mqtt_parse_error_t error = FLOWIE_MQTT_PARSE_ERROR_INIT;
  size_t consumed = SIZE_MAX;
  int rc;
  options.version = version;
  initialize_packet_sentinel(&packet);
  before = packet;
  rc = flowie_mqtt_packet_parse(bytes, size, &options, &packet, &consumed, &error);
  check_true(parse_result_known(rc));
  check_int_eq(error.code, rc);
  if (rc == FLOWIE_MQTT_PARSE_OK) {
    check(consumed > 0u);
    check(consumed <= size);
    check_size_eq(packet.packet.size, consumed);
    check(packet.packet.data == bytes);
    check(packet.body.data >= bytes);
    check(packet.body.data <= bytes + consumed);
    check(packet.body.size <= consumed);
    check_typed_failure_preserves_output(&packet);
    check_success_reencodes(&packet);
  } else {
    check_size_eq(consumed, 0u);
    check_mem_eq(&packet, &before, sizeof(packet));
  }
}

static uint32_t corpus_xorshift32(uint32_t *state) {
  uint32_t value = *state;
  value ^= value << 13u;
  value ^= value >> 17u;
  value ^= value << 5u;
  *state = value;
  return value;
}

spec("flowie mqtt deterministic parser corpus") {
  it("MQTT-FUZZ-003 keeps every corpus prefix incomplete until the full packet") {
    for (size_t corpus_index = 0u; corpus_index < sizeof(corpus) / sizeof(corpus[0]);
         ++corpus_index) {
      for (size_t prefix = 0u; prefix < corpus[corpus_index].size; ++prefix) {
        flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
        flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
        size_t consumed = SIZE_MAX;
        options.version = corpus[corpus_index].version;
        check_int_eq(flowie_mqtt_packet_parse(corpus[corpus_index].bytes, prefix, &options, &packet,
                                              &consumed, NULL),
                     FLOWIE_MQTT_PARSE_NEED_MORE);
        check_size_eq(consumed, 0u);
      }
      check_parse_invariants(corpus[corpus_index].bytes, corpus[corpus_index].size,
                             corpus[corpus_index].version);
    }
  }

  it("MQTT-FUZZ-002 preserves parser invariants under every one-bit AST mutation") {
    uint8_t mutated[64];
    for (size_t corpus_index = 0u; corpus_index < sizeof(corpus) / sizeof(corpus[0]);
         ++corpus_index) {
      check(corpus[corpus_index].size <= sizeof(mutated));
      for (size_t byte_index = 0u; byte_index < corpus[corpus_index].size; ++byte_index) {
        for (uint8_t bit = 0u; bit < 8u; ++bit) {
          memcpy(mutated, corpus[corpus_index].bytes, corpus[corpus_index].size);
          mutated[byte_index] ^= (uint8_t)(1u << bit);
          check_parse_invariants(mutated, corpus[corpus_index].size,
                                 corpus[corpus_index].version);
        }
      }
    }
  }

  it("MQTT-FUZZ-002 mutates encoded AST property length UTF-8 and flag boundaries") {
    enum {
      AST_REMAINING_LENGTH_OFFSET = 1,
      AST_CONNECT_FLAGS_OFFSET = 9,
      AST_PROPERTY_LENGTH_OFFSET = 12,
      AST_PROPERTY_IDENTIFIER_OFFSET = 13,
      AST_CLIENT_ID_OFFSET = 17,
    };
    static const uint8_t properties[] = {FLOWIE_MQTT_PROPERTY_REQUEST_RESPONSE_INFORMATION, 0x01u};
    static const struct {
      size_t offset;
      uint8_t value;
    } mutations[] = {
        {0u, 0x11u},
        {AST_REMAINING_LENGTH_OFFSET, 0x7fu},
        {AST_CONNECT_FLAGS_OFFSET, 0x03u},
        {AST_PROPERTY_LENGTH_OFFSET, 0x7fu},
        {AST_PROPERTY_IDENTIFIER_OFFSET, 0xffu},
        {AST_CLIENT_ID_OFFSET, 0x00u},
    };
    flowie_mqtt_connect_packet_t ast = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    uint8_t encoded[64];
    uint8_t mutated[64];
    size_t written = 0u;
    ast.version = FLOWIE_MQTT_VERSION_5;
    ast.clean_start = 1u;
    ast.keep_alive = 60u;
    ast.properties = (flowie_mqtt_span_t){properties, sizeof(properties)};
    ast.client_id = (flowie_mqtt_span_t){(const uint8_t *)"cli", 3u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&ast, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(connect_v5));
    check_mem_eq(encoded, connect_v5, sizeof(connect_v5));
    check_parse_invariants(encoded, written, FLOWIE_MQTT_VERSION_UNSPECIFIED);
    for (size_t i = 0u; i < sizeof(mutations) / sizeof(mutations[0]); ++i) {
      memcpy(mutated, encoded, written);
      mutated[mutations[i].offset] = mutations[i].value;
      check_parse_invariants(mutated, written, FLOWIE_MQTT_VERSION_UNSPECIFIED);
    }
  }

  it("MQTT-FUZZ-001 deterministically parses generated byte streams within bounds") {
    enum { GENERATED_CAPACITY = 512, GENERATED_CASES = 512 };
    uint8_t generated[GENERATED_CAPACITY];
    uint32_t state = UINT32_C(0x6d717474);
    for (size_t case_index = 0u; case_index < GENERATED_CASES; ++case_index) {
      size_t size = corpus_xorshift32(&state) % (GENERATED_CAPACITY + 1u);
      for (size_t i = 0u; i < size; ++i) generated[i] = (uint8_t)corpus_xorshift32(&state);
      check_parse_invariants(generated, size, FLOWIE_MQTT_VERSION_5);
      {
        flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
        flowie_mqtt_packet_view_t first = FLOWIE_MQTT_PACKET_VIEW_INIT;
        flowie_mqtt_packet_view_t second = FLOWIE_MQTT_PACKET_VIEW_INIT;
        flowie_mqtt_parse_error_t first_error = FLOWIE_MQTT_PARSE_ERROR_INIT;
        flowie_mqtt_parse_error_t second_error = FLOWIE_MQTT_PARSE_ERROR_INIT;
        size_t first_consumed = 0u;
        size_t second_consumed = 0u;
        int first_rc;
        int second_rc;
        options.version = FLOWIE_MQTT_VERSION_5;
        first_rc = flowie_mqtt_packet_parse(generated, size, &options, &first, &first_consumed,
                                           &first_error);
        second_rc = flowie_mqtt_packet_parse(generated, size, &options, &second, &second_consumed,
                                            &second_error);
        check_int_eq(second_rc, first_rc);
        check_size_eq(second_consumed, first_consumed);
        check_int_eq(second_error.code, first_error.code);
        if (first_rc == FLOWIE_MQTT_PARSE_OK) {
          check_int_eq(second.type, first.type);
          check_size_eq(second.packet.size, first.packet.size);
        }
      }
    }
  }

  it("MQTT-FUZZ-003 matches coalesced and seeded chunked packet streams") {
    uint8_t stream[256];
    size_t offsets[sizeof(corpus) / sizeof(corpus[0]) + 1u];
    size_t stream_size = 0u;
    size_t packet_count = sizeof(corpus) / sizeof(corpus[0]);
    uint32_t seed = UINT32_C(0x43484b53);
    offsets[0] = 0u;
    for (size_t i = 0u; i < packet_count; ++i) {
      check(corpus[i].size <= sizeof(stream) - stream_size);
      memcpy(stream + stream_size, corpus[i].bytes, corpus[i].size);
      stream_size += corpus[i].size;
      offsets[i + 1u] = stream_size;
    }
    for (size_t round = 0u; round < 128u; ++round) {
      uint8_t assembled[256];
      size_t assembled_size = 0u;
      size_t source_offset = 0u;
      while (source_offset < stream_size) {
        size_t chunk = 1u + corpus_xorshift32(&seed) % 23u;
        if (chunk > stream_size - source_offset) chunk = stream_size - source_offset;
        memcpy(assembled + assembled_size, stream + source_offset, chunk);
        assembled_size += chunk;
        source_offset += chunk;
      }
      check_size_eq(assembled_size, stream_size);
      check_mem_eq(assembled, stream, stream_size);
      for (size_t i = 0u; i < packet_count; ++i)
        check_parse_invariants(assembled + offsets[i], offsets[i + 1u] - offsets[i],
                               corpus[i].version);
    }
  }
}
