#include "flowie_mqtt_protocol.h"

#include "tinytest.h"

#include <string.h>

static flowie_mqtt_span_t span(const char *value) {
  flowie_mqtt_span_t result = {(const uint8_t *)value, strlen(value)};
  return result;
}

static void check_span(flowie_mqtt_span_t value, const char *expected) {
  check_size_eq(value.size, strlen(expected));
  check_int_eq(memcmp(value.data, expected, value.size), 0);
}

spec("flowie mqtt protocol") {
  it("parses one zero-copy packet and reports trailing bytes") {
    static const uint8_t bytes[] = {0x30u, 0x05u, 'a', '/', 'b', 0x00u, 0x01u, 0xd0u, 0x00u};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_parse_error_t error = FLOWIE_MQTT_PARSE_ERROR_INIT;
    size_t consumed = 0u;
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(
        flowie_mqtt_packet_parse(bytes, sizeof(bytes), &options, &packet, &consumed, &error),
        FLOWIE_MQTT_PARSE_OK);
    check_int_eq(packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
    check_int_eq(packet.flags, 0u);
    check_uint_eq(packet.remaining_length, 5u);
    check_size_eq(packet.fixed_header_size, 2u);
    check_size_eq(packet.body.size, 5u);
    check_size_eq(consumed, 7u);
    check(packet.packet.data == bytes);
  }

  it("distinguishes incomplete malformed oversized and version-invalid packets") {
    static const uint8_t incomplete[] = {0x30u, 0x05u, 'a'};
    static const uint8_t overlong[] = {0xc0u, 0x80u, 0x00u};
    static const uint8_t bad_flags[] = {0x80u, 0x00u};
    static const uint8_t auth[] = {0xf0u, 0x00u};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_parse_error_t error = FLOWIE_MQTT_PARSE_ERROR_INIT;
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(
        flowie_mqtt_packet_parse(incomplete, sizeof(incomplete), &options, &packet, NULL, &error),
        FLOWIE_MQTT_PARSE_NEED_MORE);
    check_int_eq(
        flowie_mqtt_packet_parse(overlong, sizeof(overlong), &options, &packet, NULL, &error),
        FLOWIE_MQTT_PARSE_MALFORMED);
    check_int_eq(
        flowie_mqtt_packet_parse(bad_flags, sizeof(bad_flags), &options, &packet, NULL, &error),
        FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    options.max_packet_size = 4u;
    check_int_eq(
        flowie_mqtt_packet_parse(incomplete, sizeof(incomplete), &options, &packet, NULL, &error),
        FLOWIE_MQTT_PARSE_TOO_LARGE);
    options.max_packet_size = FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
    options.version = FLOWIE_MQTT_VERSION_3_1_1;
    check_int_eq(flowie_mqtt_packet_parse(auth, sizeof(auth), &options, &packet, NULL, &error),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
  }

  it("validates UTF-8 topic names and normal or shared filters") {
    static const uint8_t invalid_utf8[] = {0xc0u, 0x80u};
    static const uint8_t truncated_utf8[] = {0xe2u, 0x82u};
    static const uint8_t surrogate[] = {0xedu, 0xa0u, 0x80u};
    static const uint8_t nul[] = {0x00u};
    static const uint8_t bmp_noncharacter[] = {0xefu, 0xb7u, 0x90u};
    static const uint8_t plane_noncharacter[] = {0xf4u, 0x8fu, 0xbfu, 0xbfu};
    check_true(flowie_mqtt_utf8_validate(span("sensors/温度")));
    check_false(
        flowie_mqtt_utf8_validate((flowie_mqtt_span_t){invalid_utf8, sizeof(invalid_utf8)}));
    check_false(
        flowie_mqtt_utf8_validate((flowie_mqtt_span_t){truncated_utf8, sizeof(truncated_utf8)}));
    check_false(flowie_mqtt_utf8_validate((flowie_mqtt_span_t){surrogate, sizeof(surrogate)}));
    check_false(flowie_mqtt_utf8_validate((flowie_mqtt_span_t){nul, sizeof(nul)}));
    check_false(flowie_mqtt_utf8_validate(
        (flowie_mqtt_span_t){bmp_noncharacter, sizeof(bmp_noncharacter)}));
    check_false(flowie_mqtt_utf8_validate(
        (flowie_mqtt_span_t){plane_noncharacter, sizeof(plane_noncharacter)}));
    check(flowie_mqtt_topic_name_validate(span("$SYS/broker/clients")));
    check(!flowie_mqtt_topic_name_validate(span("sensors/+/value")));
    check(flowie_mqtt_topic_filter_validate(span("sensors/+/value")));
    check(flowie_mqtt_topic_filter_validate(span("$share/workers/jobs/#")));
    check(!flowie_mqtt_topic_filter_validate(span("$share/+/jobs/#")));
    check(!flowie_mqtt_topic_filter_validate(span("a/#/b")));
  }

  it("matches shared filters and applies the system-topic wildcard rule") {
    int matched = 0;
    check_int_eq(
        flowie_mqtt_topic_matches(span("$share/workers/jobs/+"), span("jobs/42"), &matched),
        FLOWIE_MQTT_PARSE_OK);
    check(matched);
    check_int_eq(flowie_mqtt_topic_matches(span("sensors/#"), span("sensors"), &matched),
                 FLOWIE_MQTT_PARSE_OK);
    check(matched);
    check_int_eq(flowie_mqtt_topic_matches(span("#"), span("$SYS/status"), &matched),
                 FLOWIE_MQTT_PARSE_OK);
    check(!matched);
    check_int_eq(flowie_mqtt_topic_matches(span("$SYS/#"), span("$SYS/status"), &matched),
                 FLOWIE_MQTT_PARSE_OK);
    check(matched);
  }

  it("parses ACL text as borrowed views without importing plugin runtime") {
    static const char rule_text[] =
        "allow:readwrite:device:domain:user-1:client.1:$share/workers/root/+/events/#\n";
    static const char invalid_text[] = "allow:execute:*:*:*:*:events/#";
    flowie_mqtt_acl_rule_view_t rule = FLOWIE_MQTT_ACL_RULE_VIEW_INIT;
    check_int_eq(flowie_mqtt_acl_parse_line(rule_text, sizeof(rule_text) - 1u, &rule),
                 FLOWIE_MQTT_ACL_PARSE_OK);
    check_int_eq(rule.effect, FLOWIE_MQTT_ACL_ALLOW);
    check_int_eq(rule.permission, FLOWIE_MQTT_ACL_READ_WRITE);
    check_span(rule.role, "device");
    check_span(rule.scope, "domain");
    check_span(rule.username, "user-1");
    check_span(rule.client_id, "client.1");
    check_span(rule.topic_filter, "$share/workers/root/+/events/#");
    check_int_eq(flowie_mqtt_acl_parse_line(invalid_text, sizeof(invalid_text) - 1u, &rule),
                 FLOWIE_MQTT_ACL_PARSE_INVALID_PERMISSION);
    check_int_eq(flowie_mqtt_acl_parse_line(" # comment", sizeof(" # comment") - 1u, &rule),
                 FLOWIE_MQTT_ACL_PARSE_SKIP);
  }

  it("parses CONNECT fields and MQTT 5 properties as borrowed views") {
    static const uint8_t bytes[] = {0x10u, 0x17u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T', 0x05u,
                                    0x02u, 0x00u, 0x3cu, 0x07u, 0x15u, 0x00u, 0x04u, 'n', 'o',
                                    'n',   'e',   0x00u, 0x03u, 'c',   'l',   'i'};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    options.version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
    check_int_eq(flowie_mqtt_packet_parse(bytes, sizeof(bytes), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(packet.size, sizeof(packet));
    check_uint_eq(packet.abi_version, FLOWIE_MQTT_PROTOCOL_ABI_V1);
    check_int_eq(packet.type, FLOWIE_MQTT_PACKET_CONNECT);
    check_size_eq(connect.size, sizeof(connect));
    check_not_null(packet.body.data);
    check_size_eq(packet.body.size, packet.remaining_length);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &connect), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(connect.version, FLOWIE_MQTT_VERSION_5);
    check(connect.clean_start);
    check_uint_eq(connect.keep_alive, 60u);
    check_span(connect.client_id, "cli");
    check_int_eq(flowie_mqtt_property_iterator_init(&connect.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(property.identifier, FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD);
    check_span(property.value, "none");
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property),
                 FLOWIE_MQTT_PARSE_NEED_MORE);
  }

  it("parses PUBLISH request-response properties without copying payload") {
    static const uint8_t bytes[] = {0x32u, 0x17u, 0x00u, 0x03u, 'r', 'e', 'q', 0x00u, 0x2au,
                                    0x0du, 0x08u, 0x00u, 0x05u, 'r', 'e', 'p', 'l',   'y',
                                    0x09u, 0x00u, 0x02u, 'i',   'd', 'o', 'k'};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    uint8_t malformed[sizeof(bytes)];
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(bytes, sizeof(bytes), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(publish.size, sizeof(publish));
    check_int_eq(packet.type, FLOWIE_MQTT_PACKET_PUBLISH);
    check_int_eq(flowie_mqtt_publish_parse(&packet, &publish), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(publish.qos, 1);
    check_uint_eq(publish.packet_id, 42u);
    check_span(publish.topic, "req");
    check_span(publish.payload, "ok");
    check(publish.payload.data == bytes + sizeof(bytes) - 2u);
    check_int_eq(flowie_mqtt_property_iterator_init(&publish.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(property.identifier, FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC);
    check_span(property.value, "reply");
    property = (flowie_mqtt_property_view_t)FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(property.identifier, FLOWIE_MQTT_PROPERTY_CORRELATION_DATA);
    check_span(property.value, "id");

    memcpy(malformed, bytes, sizeof(bytes));
    malformed[9] = 0x20u;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    publish = (flowie_mqtt_publish_view_t)FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    check_int_eq(
        flowie_mqtt_packet_parse(malformed, sizeof(malformed), &options, &packet, NULL, NULL),
        FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_publish_parse(&packet, &publish), FLOWIE_MQTT_PARSE_MALFORMED);
  }

  it("parses shared subscriptions and rejects shared no-local") {
    static const uint8_t bytes[] = {0x82u, 0x17u, 0x00u, 0x07u, 0x02u, 0x0bu, 0x01u, 0x00u, 0x0fu,
                                    '$',   's',   'h',   'a',   'r',   'e',   '/',   'g',   '/',
                                    'j',   'o',   'b',   's',   '/',   '+',   0x09u};
    uint8_t invalid[sizeof(bytes)];
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_subscribe_view_t subscribe = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
    flowie_mqtt_subscription_view_t entry;
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(bytes, sizeof(bytes), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(subscribe.size, sizeof(subscribe));
    check_int_eq(packet.type, FLOWIE_MQTT_PACKET_SUBSCRIBE);
    check_int_eq(flowie_mqtt_subscribe_parse(&packet, &subscribe), FLOWIE_MQTT_PARSE_OK);
    check_uint_eq(subscribe.packet_id, 7u);
    check_size_eq(subscribe.entry_count, 1u);
    check_int_eq(flowie_mqtt_subscription_iterator_init(&packet, &subscribe, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscription_iterator_next(&iterator, &entry), FLOWIE_MQTT_PARSE_OK);
    check_span(entry.filter, "$share/g/jobs/+");
    check_int_eq(entry.qos, 1);
    check(entry.retain_as_published);

    memcpy(invalid, bytes, sizeof(bytes));
    invalid[sizeof(invalid) - 1u] = 0x05u;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    subscribe = (flowie_mqtt_subscribe_view_t)FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(invalid, sizeof(invalid), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscribe_parse(&packet, &subscribe),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
  }

  it("encodes MQTT 3.1, MQTT 3.1.1, and MQTT 5 CONNECT wire packets") {
    static const uint8_t auth_method[] = {0x15u, 0x00u, 0x04u, 'n', 'o', 'n', 'e'};
    static const uint8_t expected_v31[] = {0x10u, 0x11u, 0x00u, 0x06u, 'M',   'Q',   'I',
                                           's',   'd',   'p',   0x03u, 0x02u, 0x00u, 0x3cu,
                                           0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t expected_v311[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q', 'T', 'T', 0x04u,
                                            0x02u, 0x00u, 0x3cu, 0x00u, 0x03u, 'c', 'l', 'i'};
    static const uint8_t expected_v5[] = {
        0x10u, 0x17u, 0x00u, 0x04u, 'M', 'Q', 'T', 'T',   0x05u, 0x02u, 0x00u, 0x3cu, 0x07u,
        0x15u, 0x00u, 0x04u, 'n',   'o', 'n', 'e', 0x00u, 0x03u, 'c',   'l',   'i'};
    uint8_t encoded[32];
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t decoded = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    size_t written = 0u;
    connect.version = FLOWIE_MQTT_VERSION_3_1;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.client_id = span("cli");
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v31));
    check_mem_eq(encoded, expected_v31, sizeof(expected_v31));
    options.version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.version, FLOWIE_MQTT_VERSION_3_1);
    check_span(decoded.client_id, "cli");

    connect.version = FLOWIE_MQTT_VERSION_3_1_1;
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v311));
    check_mem_eq(encoded, expected_v311, sizeof(expected_v311));
    options.version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.version, FLOWIE_MQTT_VERSION_3_1_1);
    check_span(decoded.client_id, "cli");

    connect = (flowie_mqtt_connect_packet_t)FLOWIE_MQTT_CONNECT_PACKET_INIT;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.keep_alive = 60u;
    connect.properties = (flowie_mqtt_span_t){auth_method, sizeof(auth_method)};
    connect.client_id = span("cli");
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v5));
    check_mem_eq(encoded, expected_v5, sizeof(expected_v5));
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    decoded = (flowie_mqtt_connect_view_t)FLOWIE_MQTT_CONNECT_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.version, FLOWIE_MQTT_VERSION_5);
    check_size_eq(decoded.properties.values.size, sizeof(auth_method));
  }

  it("enforces MQTT 3.1 client identifiers and legacy acknowledgement limits") {
    static const uint8_t mismatched_name[] = {0x10u, 0x0fu, 0x00u, 0x04u, 'M',   'Q',
                                              'T',   'T',   0x03u, 0x02u, 0x00u, 0x3cu,
                                              0x00u, 0x03u, 'c',   'l',   'i'};
    static const uint8_t mismatched_legacy_name[] = {
        0x10u, 0x11u, 0x00u, 0x06u, 'M',   'Q',   'I', 's', 'd', 'p',
        0x04u, 0x02u, 0x00u, 0x3cu, 0x00u, 0x03u, 'c', 'l', 'i'};
    static const uint8_t too_long_id[] = "abcdefghijklmnopqrstuvwx";
    static const uint8_t suback_ok[] = {0x90u, 0x03u, 0x00u, 0x07u, 0x02u};
    uint8_t encoded[32];
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t decoded = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    size_t written = 0u;
    connect.version = FLOWIE_MQTT_VERSION_3_1;
    connect.clean_start = 1u;
    connect.client_id = span("");
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    connect.client_id = (flowie_mqtt_span_t){too_long_id, sizeof(too_long_id) - 1u};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    check_int_eq(flowie_mqtt_packet_parse(mismatched_name, sizeof(mismatched_name), &options,
                                          &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    decoded = (flowie_mqtt_connect_view_t)FLOWIE_MQTT_CONNECT_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(mismatched_legacy_name, sizeof(mismatched_legacy_name),
                                          &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);

    control.version = FLOWIE_MQTT_VERSION_3_1;
    control.type = FLOWIE_MQTT_PACKET_CONNACK;
    control.session_present = 1u;
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    control.session_present = 0u;
    control.type = FLOWIE_MQTT_PACKET_SUBACK;
    control.packet_id = 7u;
    control.reason_codes = (flowie_mqtt_span_t){(const uint8_t *)"\x80", 1u};
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    control.reason_codes = (flowie_mqtt_span_t){suback_ok + 4u, 1u};
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(suback_ok));
    check_mem_eq(encoded, suback_ok, sizeof(suback_ok));
  }

  it("round-trips optional CONNECT payload fields") {
    static const uint8_t will_payload[] = {0x00u, 0xffu};
    static const uint8_t password[] = {0x01u, 0x00u, 0xfeu};
    uint8_t encoded[128];
    flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_connect_view_t decoded = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    size_t written = 0u;
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.clean_start = 1u;
    connect.has_will = 1u;
    connect.will_qos = 1u;
    connect.will_retain = 1u;
    connect.has_username = 1u;
    connect.has_password = 1u;
    connect.keep_alive = 10u;
    connect.client_id = span("client-1");
    connect.will_topic = span("status/client-1");
    connect.will_payload = (flowie_mqtt_span_t){will_payload, sizeof(will_payload)};
    connect.username = span("user");
    connect.password = (flowie_mqtt_span_t){password, sizeof(password)};
    check_int_eq(flowie_mqtt_connect_packet_encode(&connect, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    options.version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_connect_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_true(decoded.clean_start);
    check_uint_eq(decoded.will_qos, 1u);
    check_true(decoded.will_retain);
    check_span(decoded.will_topic, "status/client-1");
    check_mem_eq(decoded.will_payload.data, will_payload, sizeof(will_payload));
    check_span(decoded.username, "user");
    check_size_eq(decoded.password.size, sizeof(password));
    check_mem_eq(decoded.password.data, password, sizeof(password));
  }

  it("encodes and parses MQTT 3.1.1 and MQTT 5 PUBLISH packets") {
    static const uint8_t properties[] = {0x08u, 0x00u, 0x05u, 'r',   'e', 'p', 'l',
                                         'y',   0x09u, 0x00u, 0x02u, 'i', 'd'};
    static const uint8_t expected_v311[] = {0x32u, 0x09u, 0x00u, 0x03u, 'r', 'e',
                                            'q',   0x00u, 0x2au, 'o',   'k'};
    static const uint8_t expected_v5[] = {0x32u, 0x17u, 0x00u, 0x03u, 'r', 'e', 'q', 0x00u, 0x2au,
                                          0x0du, 0x08u, 0x00u, 0x05u, 'r', 'e', 'p', 'l',   'y',
                                          0x09u, 0x00u, 0x02u, 'i',   'd', 'o', 'k'};
    uint8_t encoded[32];
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_publish_view_t decoded = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    size_t written = 0u;
    publish.version = FLOWIE_MQTT_VERSION_3_1_1;
    publish.qos = 1u;
    publish.packet_id = 42u;
    publish.topic = span("req");
    publish.payload = span("ok");
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v311));
    check_mem_eq(encoded, expected_v311, sizeof(expected_v311));
    options.version = FLOWIE_MQTT_VERSION_3_1_1;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_publish_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_span(decoded.payload, "ok");

    publish = (flowie_mqtt_publish_packet_t)FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.qos = 1u;
    publish.packet_id = 42u;
    publish.topic = span("req");
    publish.properties = (flowie_mqtt_span_t){properties, sizeof(properties)};
    publish.payload = span("ok");
    check_int_eq(flowie_mqtt_publish_packet_encode(&publish, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v5));
    check_mem_eq(encoded, expected_v5, sizeof(expected_v5));
    options.version = FLOWIE_MQTT_VERSION_5;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    decoded = (flowie_mqtt_publish_view_t)FLOWIE_MQTT_PUBLISH_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_publish_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_size_eq(decoded.properties.values.size, sizeof(properties));
  }

  it("encodes and parses MQTT 3.1.1 and MQTT 5 SUBSCRIBE packets") {
    static const uint8_t subscribe_properties[] = {0x0bu, 0x01u};
    static const uint8_t expected_v311[] = {0x82u, 0x0bu, 0x00u, 0x07u, 0x00u, 0x06u, 'j',
                                            'o',   'b',   's',   '/',   '+',   0x01u};
    static const uint8_t expected_v5[] = {
        0x82u, 0x17u, 0x00u, 0x07u, 0x02u, 0x0bu, 0x01u, 0x00u, 0x0fu, '$', 's', 'h',  'a',
        'r',   'e',   '/',   'g',   '/',   'j',   'o',   'b',   's',   '/', '+', 0x09u};
    uint8_t encoded[32];
    flowie_mqtt_subscription_t entry = {0};
    flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_subscribe_view_t decoded = FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT;
    flowie_mqtt_subscription_iterator_t iterator = FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT;
    flowie_mqtt_subscription_view_t decoded_entry;
    size_t written = 0u;
    entry.filter = span("jobs/+");
    entry.qos = 1u;
    subscribe.version = FLOWIE_MQTT_VERSION_3_1_1;
    subscribe.packet_id = 7u;
    subscribe.subscriptions = &entry;
    subscribe.subscription_count = 1u;
    check_int_eq(
        flowie_mqtt_subscribe_packet_encode(&subscribe, encoded, sizeof(encoded), &written),
        FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v311));
    check_mem_eq(encoded, expected_v311, sizeof(expected_v311));

    entry = (flowie_mqtt_subscription_t){0};
    entry.filter = span("$share/g/jobs/+");
    entry.qos = 1u;
    entry.retain_as_published = 1u;
    subscribe = (flowie_mqtt_subscribe_packet_t)FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
    subscribe.version = FLOWIE_MQTT_VERSION_5;
    subscribe.packet_id = 7u;
    subscribe.properties = (flowie_mqtt_span_t){subscribe_properties, sizeof(subscribe_properties)};
    subscribe.subscriptions = &entry;
    subscribe.subscription_count = 1u;
    check_int_eq(
        flowie_mqtt_subscribe_packet_encode(&subscribe, encoded, sizeof(encoded), &written),
        FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v5));
    check_mem_eq(encoded, expected_v5, sizeof(expected_v5));
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscribe_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscription_iterator_init(&packet, &decoded, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_subscription_iterator_next(&iterator, &decoded_entry),
                 FLOWIE_MQTT_PARSE_OK);
    check_span(decoded_entry.filter, "$share/g/jobs/+");
    check_true(decoded_entry.retain_as_published);
  }

  it("encodes and parses MQTT 3.1.1 and MQTT 5 UNSUBSCRIBE packets") {
    static const uint8_t user_property[] = {0x26u, 0x00u, 0x01u, 'k', 0x00u, 0x01u, 'v'};
    static const uint8_t expected_v311[] = {0xa2u, 0x07u, 0x00u, 0x07u, 0x00u,
                                            0x03u, 'a',   '/',   '+'};
    static const uint8_t expected_v5[] = {0xa2u, 0x14u, 0x00u, 0x07u, 0x07u, 0x26u, 0x00u, 0x01u,
                                          'k',   0x00u, 0x01u, 'v',   0x00u, 0x03u, 'a',   '/',
                                          '+',   0x00u, 0x03u, 'b',   '/',   '#'};
    flowie_mqtt_span_t filters[] = {span("a/+"), span("b/#")};
    uint8_t encoded[32];
    flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_unsubscribe_view_t decoded = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    size_t written = 0u;
    unsubscribe.version = FLOWIE_MQTT_VERSION_3_1_1;
    unsubscribe.packet_id = 7u;
    unsubscribe.filters = filters;
    unsubscribe.filter_count = 1u;
    check_int_eq(
        flowie_mqtt_unsubscribe_packet_encode(&unsubscribe, encoded, sizeof(encoded), &written),
        FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v311));
    check_mem_eq(encoded, expected_v311, sizeof(expected_v311));

    unsubscribe = (flowie_mqtt_unsubscribe_packet_t)FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
    unsubscribe.version = FLOWIE_MQTT_VERSION_5;
    unsubscribe.packet_id = 7u;
    unsubscribe.properties = (flowie_mqtt_span_t){user_property, sizeof(user_property)};
    unsubscribe.filters = filters;
    unsubscribe.filter_count = 2u;
    check_int_eq(
        flowie_mqtt_unsubscribe_packet_encode(&unsubscribe, encoded, sizeof(encoded), &written),
        FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v5));
    check_mem_eq(encoded, expected_v5, sizeof(expected_v5));
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_unsubscribe_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_size_eq(decoded.filter_count, 2u);
  }

  it("encodes PINGREQ and rejects client packets before modifying output") {
    static const uint8_t invalid_publish_property[] = {0x1fu, 0x00u, 0x02u, 'n', 'o'};
    static const uint8_t expected_ping[] = {0xc0u, 0x00u};
    static const uint8_t invalid_ping[] = {0xc0u, 0x03u, 0x08u, 0x00u, 0x00u};
    uint8_t unchanged[16];
    flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    size_t written = 99u;
    memset(unchanged, 0xa5, sizeof(unchanged));
    check_int_eq(flowie_mqtt_pingreq_encode(FLOWIE_MQTT_VERSION_3_1_1, unchanged, sizeof(unchanged),
                                            &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_ping));
    check_mem_eq(unchanged, expected_ping, sizeof(expected_ping));
    check_int_eq(flowie_mqtt_packet_parse(invalid_ping, sizeof(invalid_ping), &options, &packet,
                                          NULL, NULL),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);

    memset(unchanged, 0xa5, sizeof(unchanged));
    check_int_eq(flowie_mqtt_pingreq_encode(FLOWIE_MQTT_VERSION_5, unchanged, 1u, &written),
                 FLOWIE_MQTT_PARSE_TOO_LARGE);
    check_size_eq(written, 0u);
    for (size_t i = 0u; i < sizeof(unchanged); ++i)
      check_uint_eq(unchanged[i], 0xa5u);

    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = span("a");
    publish.duplicate = 1u;
    check_int_eq(
        flowie_mqtt_publish_packet_encode(&publish, unchanged, sizeof(unchanged), &written),
        FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    check_size_eq(written, 0u);
    for (size_t i = 0u; i < sizeof(unchanged); ++i)
      check_uint_eq(unchanged[i], 0xa5u);

    publish = (flowie_mqtt_publish_packet_t)FLOWIE_MQTT_PUBLISH_PACKET_INIT;
    publish.version = FLOWIE_MQTT_VERSION_5;
    publish.topic = span("a");
    publish.properties =
        (flowie_mqtt_span_t){invalid_publish_property, sizeof(invalid_publish_property)};
    check_int_eq(
        flowie_mqtt_publish_packet_encode(&publish, unchanged, sizeof(unchanged), &written),
        FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
    check_size_eq(written, 0u);
    for (size_t i = 0u; i < sizeof(unchanged); ++i)
      check_uint_eq(unchanged[i], 0xa5u);
  }

  it("encodes and decodes bounded MQTT 3.1.1 server acknowledgements") {
    static const uint8_t expected_connack[] = {0x20u, 0x02u, 0x01u, 0x00u};
    static const uint8_t expected_puback[] = {0x40u, 0x02u, 0x12u, 0x34u};
    static const uint8_t suback_codes[] = {0x01u, 0x80u};
    static const uint8_t expected_suback[] = {0x90u, 0x04u, 0x00u, 0x07u, 0x01u, 0x80u};
    uint8_t encoded[16];
    flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    size_t written = 0u;
    control.version = FLOWIE_MQTT_VERSION_3_1_1;
    control.type = FLOWIE_MQTT_PACKET_CONNACK;
    control.session_present = 1u;
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_connack));
    check_mem_eq(encoded, expected_connack, sizeof(expected_connack));

    control = (flowie_mqtt_control_packet_t)FLOWIE_MQTT_CONTROL_PACKET_INIT;
    control.version = FLOWIE_MQTT_VERSION_3_1_1;
    control.type = FLOWIE_MQTT_PACKET_PUBACK;
    control.packet_id = 0x1234u;
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_mem_eq(encoded, expected_puback, sizeof(expected_puback));

    control.type = FLOWIE_MQTT_PACKET_SUBACK;
    control.packet_id = 7u;
    control.reason_codes = (flowie_mqtt_span_t){suback_codes, sizeof(suback_codes)};
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_mem_eq(encoded, expected_suback, sizeof(expected_suback));
    options.version = FLOWIE_MQTT_VERSION_3_1_1;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.type, FLOWIE_MQTT_PACKET_SUBACK);
    check_uint_eq(decoded.packet_id, 7u);
    check_size_eq(decoded.reason_codes.size, sizeof(suback_codes));
    check_mem_eq(decoded.reason_codes.data, suback_codes, sizeof(suback_codes));
  }

  it("encodes MQTT 5 acknowledgement properties and fails before overflowing output") {
    static const uint8_t properties[] = {0x1fu, 0x00u, 0x02u, 'o', 'k'};
    static const uint8_t expected[] = {0x50u, 0x09u, 0x00u, 0x2au, 0x10u, 0x05u,
                                       0x1fu, 0x00u, 0x02u, 'o',   'k'};
    uint8_t encoded[16];
    uint8_t unchanged[sizeof(expected)];
    flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    size_t written = 99u;
    memset(unchanged, 0xa5, sizeof(unchanged));
    control.version = FLOWIE_MQTT_VERSION_5;
    control.type = FLOWIE_MQTT_PACKET_PUBREC;
    control.packet_id = 42u;
    control.reason_code = 0x10u;
    control.properties = (flowie_mqtt_span_t){properties, sizeof(properties)};
    check_int_eq(
        flowie_mqtt_control_packet_encode(&control, unchanged, sizeof(expected) - 1u, &written),
        FLOWIE_MQTT_PARSE_TOO_LARGE);
    check_size_eq(written, 0u);
    for (size_t i = 0u; i < sizeof(unchanged); ++i)
      check_uint_eq(unchanged[i], 0xa5u);

    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected));
    check_mem_eq(encoded, expected, sizeof(expected));
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.type, FLOWIE_MQTT_PACKET_PUBREC);
    check_uint_eq(decoded.packet_id, 42u);
    check_uint_eq(decoded.reason_code, 0x10u);
    check_int_eq(flowie_mqtt_property_iterator_init(&decoded.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(property.identifier, FLOWIE_MQTT_PROPERTY_REASON_STRING);
    check_span(property.value, "ok");

    control.packet_id = 0u;
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
  }

  it("decodes PUBREL packet identifiers and rejects MQTT 3.1.1 reason bytes") {
    static const uint8_t pubrel[] = {0x62u, 0x02u, 0x00u, 0x2au};
    static const uint8_t invalid_puback[] = {0x40u, 0x03u, 0x00u, 0x2au, 0x00u};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(pubrel, sizeof(pubrel), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.type, FLOWIE_MQTT_PACKET_PUBREL);
    check_uint_eq(decoded.packet_id, 42u);
    options.version = FLOWIE_MQTT_VERSION_3_1_1;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    decoded = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(invalid_puback, sizeof(invalid_puback), &options, &packet,
                                          NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
  }

  it("parses bounded UNSUBSCRIBE filters and rejects an empty payload") {
    static const uint8_t bytes[] = {0xa2u, 0x14u, 0x00u, 0x07u, 0x07u, 0x26u, 0x00u, 0x01u,
                                    'k',   0x00u, 0x01u, 'v',   0x00u, 0x03u, 'a',   '/',
                                    '+',   0x00u, 0x03u, 'b',   '/',   '#'};
    static const uint8_t empty[] = {0xa2u, 0x03u, 0x00u, 0x07u, 0x00u};
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_unsubscribe_view_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    flowie_mqtt_topic_filter_iterator_t iterator = FLOWIE_MQTT_TOPIC_FILTER_ITERATOR_INIT;
    flowie_mqtt_span_t filter;
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(bytes, sizeof(bytes), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_unsubscribe_parse(&packet, &unsubscribe), FLOWIE_MQTT_PARSE_OK);
    check_uint_eq(unsubscribe.packet_id, 7u);
    check_size_eq(unsubscribe.filter_count, 2u);
    check_int_eq(flowie_mqtt_topic_filter_iterator_init(&unsubscribe, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_topic_filter_iterator_next(&iterator, &filter), FLOWIE_MQTT_PARSE_OK);
    check_span(filter, "a/+");
    check_int_eq(flowie_mqtt_topic_filter_iterator_next(&iterator, &filter), FLOWIE_MQTT_PARSE_OK);
    check_span(filter, "b/#");
    check_int_eq(flowie_mqtt_topic_filter_iterator_next(&iterator, &filter),
                 FLOWIE_MQTT_PARSE_NEED_MORE);

    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    unsubscribe = (flowie_mqtt_unsubscribe_view_t)FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(empty, sizeof(empty), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_unsubscribe_parse(&packet, &unsubscribe),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
  }

  it("round-trips MQTT 3.1.1 and MQTT 5 UNSUBACK wire contracts") {
    static const uint8_t reasons[] = {0x00u, 0x11u, 0x8fu};
    static const uint8_t expected_v5[] = {0xb0u, 0x06u, 0x00u, 0x07u, 0x00u, 0x00u, 0x11u, 0x8fu};
    static const uint8_t expected_v311[] = {0xb0u, 0x02u, 0x00u, 0x07u};
    uint8_t encoded[16];
    flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    size_t written = 0u;
    control.version = FLOWIE_MQTT_VERSION_5;
    control.type = FLOWIE_MQTT_PACKET_UNSUBACK;
    control.packet_id = 7u;
    control.reason_codes = (flowie_mqtt_span_t){reasons, sizeof(reasons)};
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v5));
    check_mem_eq(encoded, expected_v5, sizeof(expected_v5));
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_size_eq(decoded.reason_codes.size, sizeof(reasons));
    check_mem_eq(decoded.reason_codes.data, reasons, sizeof(reasons));

    control = (flowie_mqtt_control_packet_t)FLOWIE_MQTT_CONTROL_PACKET_INIT;
    control.version = FLOWIE_MQTT_VERSION_3_1_1;
    control.type = FLOWIE_MQTT_PACKET_UNSUBACK;
    control.packet_id = 7u;
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_v311));
    check_mem_eq(encoded, expected_v311, sizeof(expected_v311));
  }

  it("encodes and decodes MQTT 5 DISCONNECT and AUTH reason properties") {
    static const uint8_t reason_string[] = {0x1fu, 0x00u, 0x02u, 'o', 'k'};
    static const uint8_t expected_disconnect[] = {0xe0u, 0x07u, 0x8eu, 0x05u, 0x1fu,
                                                  0x00u, 0x02u, 'o',   'k'};
    static const uint8_t auth[] = {0xf0u, 0x0au, 0x18u, 0x08u, 0x15u, 0x00u,
                                   0x05u, 's',   'c',   'r',   'a',   'm'};
    uint8_t invalid_auth[sizeof(auth)];
    uint8_t encoded[16];
    flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
    flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_view_t decoded = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
    flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
    size_t written = 0u;
    control.version = FLOWIE_MQTT_VERSION_5;
    control.type = FLOWIE_MQTT_PACKET_DISCONNECT;
    control.reason_code = 0x8eu;
    control.properties = (flowie_mqtt_span_t){reason_string, sizeof(reason_string)};
    check_int_eq(flowie_mqtt_control_packet_encode(&control, encoded, sizeof(encoded), &written),
                 FLOWIE_MQTT_PARSE_OK);
    check_size_eq(written, sizeof(expected_disconnect));
    check_mem_eq(encoded, expected_disconnect, sizeof(expected_disconnect));
    options.version = FLOWIE_MQTT_VERSION_5;
    check_int_eq(flowie_mqtt_packet_parse(encoded, written, &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_uint_eq(decoded.reason_code, 0x8eu);

    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    decoded = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    check_int_eq(flowie_mqtt_packet_parse(auth, sizeof(auth), &options, &packet, NULL, NULL),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(decoded.type, FLOWIE_MQTT_PACKET_AUTH);
    check_uint_eq(decoded.reason_code, 0x18u);
    check_int_eq(flowie_mqtt_property_iterator_init(&decoded.properties, &iterator),
                 FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_property_iterator_next(&iterator, &property), FLOWIE_MQTT_PARSE_OK);
    check_int_eq(property.identifier, FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD);
    check_span(property.value, "scram");

    memcpy(invalid_auth, auth, sizeof(auth));
    invalid_auth[2] = 0x01u;
    packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
    decoded = (flowie_mqtt_control_packet_view_t)FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    check_int_eq(
        flowie_mqtt_packet_parse(invalid_auth, sizeof(invalid_auth), &options, &packet, NULL, NULL),
        FLOWIE_MQTT_PARSE_OK);
    check_int_eq(flowie_mqtt_control_packet_parse(&packet, &decoded),
                 FLOWIE_MQTT_PARSE_PROTOCOL_ERROR);
  }
}
